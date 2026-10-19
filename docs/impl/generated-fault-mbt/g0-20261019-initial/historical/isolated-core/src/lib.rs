#[path = "/home/nzsn/Repos/Cooperators/src/storage.rs"]
pub mod storage;
#[path = "/home/nzsn/Repos/Cooperators/src/id.rs"]
pub mod id;
#[path = "/home/nzsn/Repos/Cooperators/src/rpc.rs"]
pub mod rpc;
#[path = "/home/nzsn/Repos/Cooperators/src/status.rs"]
pub mod status;
#[path = "/home/nzsn/Repos/Cooperators/src/error.rs"]
pub mod error;
#[path = "/home/nzsn/Repos/Cooperators/src/client.rs"]
pub mod client;
#[path = "/home/nzsn/Repos/Cooperators/src/envelope.rs"]
pub mod envelope;
#[path = "/home/nzsn/Repos/Cooperators/src/queues.rs"]
pub mod queues;
#[path = "/home/nzsn/Repos/Cooperators/src/channels.rs"]
pub mod channels;
#[path = "/home/nzsn/Repos/Cooperators/src/registry.rs"]
pub mod registry;
#[path = "/home/nzsn/Repos/Cooperators/src/docs.rs"]
pub mod docs;
#[path = "/home/nzsn/Repos/Cooperators/src/presence.rs"]
pub mod presence;
#[path = "/home/nzsn/Repos/Cooperators/src/paths.rs"]
pub mod paths;
#[path = "/home/nzsn/Repos/Cooperators/src/server.rs"]
pub mod server;
#[path = "/home/nzsn/Repos/Cooperators/src/messaging.rs"]
pub mod messaging;
#[path = "/home/nzsn/Repos/Cooperators/src/leases.rs"]
pub mod leases;
#[path = "/home/nzsn/Repos/Cooperators/src/metrics.rs"]
pub mod metrics;
pub const VERSION: &str = "0.1.0";
pub mod mcp {
    pub const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:7420";
    pub async fn serve_http(_: tokio::net::TcpListener, _: std::sync::Arc<crate::server::DaemonState>) -> std::io::Result<()> {
        Err(std::io::Error::other("HTTP excluded from isolated core probe"))
    }
}
