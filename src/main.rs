//! `coop` command-line entry point.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use serde_json::{json, Value};

use cooperators::{client, mcp, paths, server};

mod host_cli;

/// Environment variable read when `--token` is absent.
const TOKEN_ENV: &str = "COOP_TOKEN";

#[derive(Debug, Parser)]
#[command(
    name = "coop",
    version,
    about = "Localhost inter-agent communication broker"
)]
struct Cli {
    /// Bus root directory. Overridden by COOPERATORS_HOME when that is set.
    #[arg(long, global = true, value_name = "DIR")]
    root: Option<PathBuf>,

    /// Agent token. Defaults to $COOP_TOKEN when omitted.
    #[arg(long, global = true, value_name = "TOKEN")]
    token: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the daemon
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    /// Run an MCP server over stdin/stdout, forwarding tool calls to the daemon
    McpStdio {
        /// Dispatch against the bus root in-process instead of a daemon socket
        /// (tests and single-process debugging; the daemon must not be running)
        #[arg(long)]
        embedded: bool,
        /// Reuse COOP_TOKEN/--token instead of enrolling a fresh instance
        #[arg(long)]
        resume: bool,
        /// Kind shared by instances of the same agent application
        #[arg(long, default_value = "agent")]
        kind: String,
        /// Repeatable human label; routing identity remains unique
        #[arg(long, default_value = "agent")]
        display_name: String,
        /// Enable arrivals when dispatching against an embedded root
        #[arg(long, requires = "embedded")]
        enable_arrivals: bool,
        /// Private IPC socket of this managed session's host bridge
        #[arg(long, requires_all = ["resume", "host_bus_id", "host_agent_id"], conflicts_with = "embedded")]
        host_control: Option<PathBuf>,
        #[arg(long, requires = "host_control")]
        host_bus_id: Option<String>,
        #[arg(long, requires = "host_control")]
        host_agent_id: Option<String>,
    },
    /// Register a new agent name (or rotate an existing token)
    Register {
        /// Display name, unique per bus
        name: String,
        /// Agent kind (codex, claude, agent, ...)
        #[arg(long, default_value = "agent")]
        kind: String,
        /// Capability label; repeat for multiple labels
        #[arg(long = "capability")]
        capabilities: Vec<String>,
    },
    /// Send a direct message and wait for the durable append
    Send {
        /// Recipient agent name or id
        #[arg(long)]
        to: String,
        /// Message kind (request, response, event, ...)
        #[arg(long, default_value = "text")]
        kind: String,
        /// Message text
        #[arg(long)]
        text: String,
        /// Correlation id of the message being answered
        #[arg(long)]
        reply_to: Option<String>,
        /// Dedupe key scoped to the authenticated sender
        #[arg(long)]
        idempotency_key: Option<String>,
        /// Message TTL in seconds
        #[arg(long)]
        ttl: Option<u64>,
        /// Accepted for compatibility; identity always comes from the token
        #[arg(long)]
        from: Option<String>,
    },
    /// Long-poll the authenticated agent's inbox (peek; does not ack)
    Wait {
        /// Opaque cursor from a previous wait; defaults to the acked cursor
        #[arg(long)]
        cursor: Option<String>,
        /// Long-poll timeout in seconds
        #[arg(long, default_value_t = 30)]
        timeout: u64,
    },
    /// Acknowledge messages through a cursor
    Ack {
        /// Cursor returned by wait
        #[arg(long)]
        cursor: String,
    },
    /// Manage the separate durable document/task arrival feed
    Arrivals {
        #[command(subcommand)]
        command: ArrivalCommand,
    },
    /// Run and inspect a managed agent activation bridge
    Host {
        #[command(subcommand)]
        command: host_cli::HostCommand,
    },
    /// Record a presence heartbeat
    Heartbeat,
    /// List registered agents
    Agents {
        /// Include stale and gone agents
        #[arg(long)]
        include_stale: bool,
    },
    /// Print daemon status as JSON
    Status,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    /// Run the broker daemon in the foreground
    Run {
        /// Streamable-HTTP MCP listen address; "off" disables it
        /// (default: 127.0.0.1:7420, or $COOP_HTTP_ADDR)
        #[arg(long, value_name = "ADDR")]
        http_addr: Option<String>,
        /// Upgrade this root to support durable arrival notifications
        #[arg(long)]
        enable_arrivals: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ArrivalCommand {
    /// Subscribe to one exact document or queue name
    Subscribe {
        #[arg(long)]
        client_key: String,
        #[arg(long, conflicts_with = "queue", required_unless_present = "queue")]
        document: Option<String>,
        #[arg(
            long,
            conflicts_with = "document",
            required_unless_present = "document"
        )]
        queue: Option<String>,
        /// Omit the initial current-state catch-up
        #[arg(long)]
        future_only: bool,
    },
    /// Stop future notifications from a subscription
    Unsubscribe {
        #[arg(long)]
        subscription_id: String,
    },
    /// List subscriptions and the stable bus identity
    List,
    /// Peek at arrivals without acknowledging them
    Wait {
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long, default_value_t = 30)]
        timeout: u64,
        #[arg(long, default_value_t = 64)]
        limit: usize,
    },
    /// Durably acknowledge arrivals through a returned cursor
    Ack {
        #[arg(long)]
        cursor: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let root = paths::resolve_root(cli.root.as_deref());

    match cli.command {
        Command::Daemon {
            command:
                DaemonCommand::Run {
                    http_addr,
                    enable_arrivals,
                },
        } => {
            let http_addr = match resolve_http_addr(http_addr) {
                Ok(address) => address,
                Err(message) => {
                    eprintln!("coop daemon: {message}");
                    return ExitCode::from(2);
                }
            };
            let options = server::ServerOptions {
                http_addr,
                enable_arrivals,
            };
            match server::run_with_options(&root, options).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("coop daemon: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::McpStdio {
            embedded,
            resume,
            kind,
            display_name,
            enable_arrivals,
            host_control,
            host_bus_id,
            host_agent_id,
        } => {
            let token = token_option(&cli.token);
            let backend = if embedded {
                let mut config = server::DaemonConfig::new(root.clone());
                config.enable_arrivals = enable_arrivals;
                match server::DaemonState::owned(config) {
                    Ok(state) => mcp::Backend::embedded(Arc::new(state)),
                    Err(error) => {
                        eprintln!("coop mcp-stdio: initialize bus root: {error}");
                        return ExitCode::FAILURE;
                    }
                }
            } else {
                mcp::Backend::uds(paths::socket_path(&root))
            };
            let handler = if let Some(socket) = host_control {
                let Some(token) = token else {
                    eprintln!("coop mcp-stdio: managed mode requires COOP_TOKEN");
                    return ExitCode::from(2);
                };
                let secret = match std::env::var("COOP_HOST_CONTROL_SECRET") {
                    Ok(secret) if !secret.is_empty() => secret,
                    _ => {
                        eprintln!("coop mcp-stdio: managed mode requires COOP_HOST_CONTROL_SECRET");
                        return ExitCode::from(2);
                    }
                };
                let control = cooperators::host_control::HostControlClient::new(
                    socket,
                    secret,
                    host_bus_id.expect("Clap requires bus id"),
                    host_agent_id.expect("Clap requires agent id"),
                );
                match mcp::CoopMcpHandler::managed_stdio(backend, token, control).await {
                    Ok(handler) => handler,
                    Err(error) => {
                        eprintln!("coop mcp-stdio: managed binding failed: {}", error.message);
                        return ExitCode::FAILURE;
                    }
                }
            } else if resume {
                mcp::CoopMcpHandler::stdio(backend, token)
            } else {
                match mcp::CoopMcpHandler::fresh_stdio(backend, kind, display_name, Vec::new())
                    .await
                {
                    Ok(handler) => handler,
                    Err(error) => {
                        eprintln!("coop mcp-stdio: enrollment failed: {}", error.message);
                        return ExitCode::FAILURE;
                    }
                }
            };
            match mcp::serve_stdio(handler).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("coop mcp-stdio: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Status => match client::call(&root, "daemon.status", json!({})).await {
            Ok(value) => print_json(&value),
            Err(error) => report("status", &error),
        },
        Command::Host { command } => host_cli::run(root, cli.token.clone(), command).await,
        Command::Register {
            name,
            kind,
            capabilities,
        } => {
            let mut params = json!({
                "name": name,
                "kind": kind,
                "capabilities": capabilities,
            });
            if let Some(token) = token_option(&cli.token) {
                params["token"] = Value::String(token);
            }
            call_and_print(&root, "register", "register_agent", params).await
        }
        Command::Send {
            to,
            kind,
            text,
            reply_to,
            idempotency_key,
            ttl,
            from,
        } => {
            if from.is_some() {
                eprintln!("coop send: --from is ignored; sender identity comes from the token");
            }
            let token = match require_token(&cli.token) {
                Ok(token) => token,
                Err(code) => return code,
            };
            let mut params = json!({
                "token": token,
                "to": to,
                "kind": kind,
                "body": {"mime": "text/plain", "text": text},
            });
            if let Some(reply_to) = reply_to {
                params["reply_to"] = Value::String(reply_to);
            }
            if let Some(key) = idempotency_key {
                params["idempotency_key"] = Value::String(key);
            }
            if let Some(ttl) = ttl {
                params["ttl_s"] = json!(ttl);
            }
            call_and_print(&root, "send", "send_message", params).await
        }
        Command::Wait { cursor, timeout } => {
            let token = match require_token(&cli.token) {
                Ok(token) => token,
                Err(code) => return code,
            };
            let mut params = json!({"token": token, "timeout_s": timeout});
            if let Some(cursor) = cursor {
                params["cursor"] = Value::String(cursor);
            }
            let deadline = Duration::from_secs(timeout.saturating_add(15).max(5));
            match client::call_with_timeout(&root, "wait_message", params, deadline).await {
                Ok(value) => print_json(&value),
                Err(error) => report("wait", &error),
            }
        }
        Command::Ack { cursor } => {
            let token = match require_token(&cli.token) {
                Ok(token) => token,
                Err(code) => return code,
            };
            call_and_print(
                &root,
                "ack",
                "ack",
                json!({"token": token, "cursor": cursor}),
            )
            .await
        }
        Command::Arrivals { command } => {
            let token = match require_token(&cli.token) {
                Ok(token) => token,
                Err(code) => return code,
            };
            let (method, mut params, timeout) = match command {
                ArrivalCommand::Subscribe {
                    client_key,
                    document,
                    queue,
                    future_only,
                } => {
                    let (resource, kinds) = match (document, queue) {
                        (Some(name), None) => (
                            json!({"type":"document","name":name}),
                            json!(["doc.published", "doc.updated"]),
                        ),
                        (None, Some(name)) => (
                            json!({"type":"queue","name":name}),
                            json!(["queue.task_available"]),
                        ),
                        _ => {
                            eprintln!("coop arrivals: select exactly one --document or --queue");
                            return ExitCode::from(2);
                        }
                    };
                    (
                        "subscribe_arrivals",
                        json!({"client_key":client_key,"resource":resource,"event_kinds":kinds,"catch_up":!future_only}),
                        30,
                    )
                }
                ArrivalCommand::Unsubscribe { subscription_id } => (
                    "unsubscribe_arrivals",
                    json!({"subscription_id":subscription_id}),
                    30,
                ),
                ArrivalCommand::List => ("list_arrival_subscriptions", json!({}), 30),
                ArrivalCommand::Wait {
                    cursor,
                    timeout,
                    limit,
                } => (
                    "wait_arrivals",
                    json!({"cursor":cursor,"timeout_s":timeout,"limit":limit}),
                    timeout.saturating_add(15),
                ),
                ArrivalCommand::Ack { cursor } => ("ack_arrivals", json!({"cursor":cursor}), 30),
            };
            params["token"] = Value::String(token);
            match client::call_with_timeout(&root, method, params, Duration::from_secs(timeout))
                .await
            {
                Ok(value) => print_json(&value),
                Err(error) => report("arrivals", &error),
            }
        }
        Command::Heartbeat => {
            let token = match require_token(&cli.token) {
                Ok(token) => token,
                Err(code) => return code,
            };
            call_and_print(&root, "heartbeat", "heartbeat", json!({"token": token})).await
        }
        Command::Agents { include_stale } => {
            let token = match require_token(&cli.token) {
                Ok(token) => token,
                Err(code) => return code,
            };
            call_and_print(
                &root,
                "agents",
                "list_agents",
                json!({"token": token, "include_stale": include_stale}),
            )
            .await
        }
    }
}

async fn call_and_print(
    root: &std::path::Path,
    label: &str,
    method: &str,
    params: Value,
) -> ExitCode {
    match client::call(root, method, params).await {
        Ok(value) => print_json(&value),
        Err(error) => report(label, &error),
    }
}

fn print_json(value: &Value) -> ExitCode {
    match serde_json::to_string_pretty(value) {
        Ok(rendered) => {
            println!("{rendered}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("coop: {error}");
            ExitCode::FAILURE
        }
    }
}

fn report(command: &str, error: &client::ClientError) -> ExitCode {
    let code = error
        .error_code()
        .map(|code| code.as_str())
        .unwrap_or("UNKNOWN");
    eprintln!("coop {command}: {error} [{code}]");
    ExitCode::FAILURE
}

fn token_option(flag: &Option<String>) -> Option<String> {
    flag.clone()
        .or_else(|| std::env::var(TOKEN_ENV).ok())
        .filter(|token| !token.is_empty())
}

fn require_token(flag: &Option<String>) -> Result<String, ExitCode> {
    token_option(flag).ok_or_else(|| {
        eprintln!("coop: missing agent token; pass --token or set {TOKEN_ENV}");
        ExitCode::from(2)
    })
}

/// Resolves the streamable-HTTP listen address from `--http-addr`, then
/// `$COOP_HTTP_ADDR`, then the design default. `off`/`none` disables it.
fn resolve_http_addr(flag: Option<String>) -> Result<Option<std::net::SocketAddr>, String> {
    let raw = flag
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("COOP_HTTP_ADDR")
                .ok()
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| mcp::DEFAULT_HTTP_ADDR.to_string());
    if raw.eq_ignore_ascii_case("off") || raw.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    raw.parse()
        .map(Some)
        .map_err(|error| format!("invalid HTTP address {raw:?}: {error}"))
}
