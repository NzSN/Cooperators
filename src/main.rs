//! `coop` command-line entry point.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde_json::json;

use cooperators::{client, paths, server};

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

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run or inspect the daemon
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    /// Send a direct message (wave T2)
    Send {
        /// Recipient agent name or ID
        #[arg(long)]
        to: String,
        /// Message kind (request, response, event, ...)
        #[arg(long, default_value = "text")]
        kind: String,
        /// Message text
        #[arg(long)]
        text: String,
        /// Sender name
        #[arg(long)]
        from: Option<String>,
    },
    /// Wait for new inbox messages (wave T2)
    Wait {
        /// Opaque cursor from a previous wait/ack
        #[arg(long)]
        cursor: Option<String>,
        /// Long-poll timeout in seconds
        #[arg(long, default_value_t = 30)]
        timeout: u64,
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
            Ok(value) => match serde_json::to_string_pretty(&value) {
                Ok(rendered) => {
                    println!("{rendered}");
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("coop status: {error}");
                    ExitCode::FAILURE
                }
            },
            Err(error) => {
                let code = error
                    .error_code()
                    .map(|code| code.as_str())
                    .unwrap_or("UNKNOWN");
                eprintln!("coop status: {error} [{code}]");
                ExitCode::FAILURE
            }
        },
        Command::Send {
            to,
            kind,
            text,
            from,
        } => {
            let _ = (&to, &kind, &text, &from);
            eprintln!("coop send: not implemented (wave T2)");
            ExitCode::from(2)
        }
        Command::Wait { cursor, timeout } => {
            let _ = (&cursor, &timeout);
            eprintln!("coop wait: not implemented (wave T2)");
            ExitCode::from(2)
        }
    }
}
