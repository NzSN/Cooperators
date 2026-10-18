//! `coop` command-line entry point.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use serde_json::{json, Value};

use cooperators::{client, paths, server};

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
    Run,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let root = paths::resolve_root(cli.root.as_deref());

    match cli.command {
        Command::Daemon {
            command: DaemonCommand::Run,
        } => match server::run(&root).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("coop daemon: {error}");
                ExitCode::FAILURE
            }
        },
        Command::Status => match client::call(&root, "daemon.status", json!({})).await {
            Ok(value) => print_json(&value),
            Err(error) => report("status", &error),
        },
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
