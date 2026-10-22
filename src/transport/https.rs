use std::time::Duration;

use serde_json::Value;

use crate::client::ClientError;

use super::{decode, encode, MAX_RPC_BYTES};

/// TLS trust additions and an explicitly selected proxy. No insecure TLS mode.
#[derive(Default)]
pub struct HttpsOptions {
    pub ca_pem: Option<Vec<u8>>,
    pub identity_pem: Option<Vec<u8>>,
    pub proxy: Option<String>,
}

pub(super) fn build(options: HttpsOptions) -> Result<reqwest::Client, ClientError> {
    let mut builder = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .min_tls_version(reqwest::tls::Version::TLS_1_2)
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(8);
    if let Some(pem) = options.ca_pem {
        let certificate = reqwest::Certificate::from_pem(&pem)
            .map_err(|_| ClientError::Protocol("invalid broker CA certificate".into()))?;
        builder = builder.add_root_certificate(certificate);
    }
    if let Some(pem) = options.identity_pem {
        let identity = reqwest::Identity::from_pem(&pem)
            .map_err(|_| ClientError::Protocol("invalid client TLS identity".into()))?;
        builder = builder.identity(identity);
    }
    if let Some(proxy) = options.proxy {
        builder = builder.proxy(
            reqwest::Proxy::https(proxy)
                .map_err(|_| ClientError::Protocol("invalid HTTPS proxy configuration".into()))?,
        );
    }
    builder
        .build()
        .map_err(|_| ClientError::Protocol("cannot initialize HTTPS client".into()))
}

pub(super) async fn call(
    client: &reqwest::Client,
    origin: &reqwest::Url,
    method: &str,
    mut params: Value,
    deadline: Duration,
    expected_bus_id: Option<&str>,
) -> Result<Value, ClientError> {
    let token = match params.as_object_mut().and_then(|p| p.remove("token")) {
        None | Some(Value::Null) => None,
        Some(Value::String(token)) if !token.is_empty() => Some(token),
        _ => return Err(ClientError::Protocol("invalid broker credential".into())),
    };
    let encoded = encode(method, params)?;
    if let Some(expected) = expected_bus_id {
        verify_bus(client, origin, token.as_deref(), expected, deadline).await?;
    }
    let url = origin
        .join(if method == "enroll_agent" {
            "v1/enroll"
        } else {
            "v1/rpc"
        })
        .map_err(|_| ClientError::Protocol("invalid broker RPC URL".into()))?;
    let mut request = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .timeout(deadline)
        .body(encoded);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let mut response = request.send().await.map_err(|error| {
        if error.is_connect() {
            ClientError::RemoteTransport(error.without_url().to_string())
        } else {
            uncertain(
                method,
                "HTTPS exchange failed before a response was received",
            )
        }
    })?;
    let status = response.status();
    if status.is_server_error() {
        return Err(uncertain(
            method,
            "HTTPS upstream failed; the request may have committed",
        ));
    }
    if !status.is_success() && !status.is_client_error() {
        return Err(ClientError::HttpStatus(status.as_u16()));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RPC_BYTES as u64)
    {
        if status.is_client_error() {
            return Err(ClientError::HttpStatus(status.as_u16()));
        }
        return Err(uncertain(method, "broker response exceeds 4 MiB"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| {
        if status.is_client_error() {
            ClientError::HttpStatus(status.as_u16())
        } else {
            uncertain(method, "HTTPS response was interrupted")
        }
    })? {
        if bytes.len().saturating_add(chunk.len()) > MAX_RPC_BYTES {
            if status.is_client_error() {
                return Err(ClientError::HttpStatus(status.as_u16()));
            }
            return Err(uncertain(method, "broker response exceeds 4 MiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if status.is_client_error() {
        // Preserve the broker's typed denial without treating an HTTP error
        // page or a mismatched JSON-RPC response as a trusted outcome.
        return match decode(&bytes) {
            Err(ClientError::Rpc(error)) => Err(ClientError::Rpc(error)),
            _ => Err(ClientError::HttpStatus(status.as_u16())),
        };
    }
    match decode(&bytes) {
        Err(ClientError::Protocol(_)) => Err(uncertain(method, "invalid broker JSON-RPC response")),
        result => result,
    }
}

pub(super) fn uncertain(method: &str, detail: &str) -> ClientError {
    if crate::network::is_read(method) {
        ClientError::RemoteTransport(detail.to_string())
    } else {
        ClientError::OutcomeUnknown(detail.to_string())
    }
}

async fn verify_bus(
    client: &reqwest::Client,
    origin: &reqwest::Url,
    token: Option<&str>,
    expected: &str,
    deadline: Duration,
) -> Result<(), ClientError> {
    let mut request = client
        .get(origin.join("v1/info").expect("validated origin"))
        .timeout(deadline);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let mut response = request
        .send()
        .await
        .map_err(|_| ClientError::RemoteTransport("broker identity query failed".into()))?;
    if !response.status().is_success() {
        return Err(ClientError::HttpStatus(response.status().as_u16()));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ClientError::RemoteTransport("broker identity response interrupted".into()))?
    {
        if bytes.len() + chunk.len() > 64 * 1024 {
            return Err(ClientError::Protocol(
                "broker identity response exceeds limit".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let info: Value = serde_json::from_slice(&bytes)
        .map_err(|_| ClientError::Protocol("invalid broker identity response".into()))?;
    if info["bus_id"].as_str() != Some(expected) {
        return Err(ClientError::Protocol(
            "broker identity does not match selected profile".into(),
        ));
    }
    Ok(())
}
