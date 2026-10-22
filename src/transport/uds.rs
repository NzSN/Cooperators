use std::path::Path;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::client::ClientError;

use super::{decode, encode, MAX_RPC_BYTES};

pub(super) async fn call(socket: &Path, method: &str, params: Value) -> Result<Value, ClientError> {
    let mut encoded = encode(method, params)?;
    encoded.push(b'\n');
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(ClientError::Unreachable)?;
    stream
        .write_all(&encoded)
        .await
        .map_err(ClientError::Unreachable)?;
    stream.flush().await.map_err(ClientError::Unreachable)?;
    let mut reader = BufReader::new(stream).take((MAX_RPC_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    let count = reader
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(ClientError::Unreachable)?;
    if count == 0 {
        return Err(ClientError::Unreachable(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "daemon closed the connection without a response",
        )));
    }
    if bytes.len() > MAX_RPC_BYTES {
        return Err(ClientError::Protocol(
            "broker response exceeds 4 MiB".into(),
        ));
    }
    decode(&bytes)
}
