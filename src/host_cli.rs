//! Managed host launcher. Credentials live only in private files/environment;
//! public output is an operator NDJSON stream with explicit approval requests.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch, Mutex};

use cooperators::client;
use cooperators::host::adapters::{
    ClaudeAdapterConfig, ClaudeHostAdapter, CodexAdapterConfig, CodexAppServerAdapter,
};
use cooperators::host::{
    run_receipt_loop, Binding, BridgeLimits, BridgeRunner, HostAdapter, HostBridge, Readiness,
    SharedHostBridge, UdsArrivalClient,
};
use cooperators::host_control::{HostControlClient, HostControlServer};
use cooperators::{id, registry};

type Result<T> = std::result::Result<T, String>;
const MAX_OPERATOR_BYTES: usize = 1024 * 1024;
static OUTPUT_SECRETS: OnceLock<std::sync::Mutex<Vec<String>>> = OnceLock::new();

fn remember_secret(value: &str) {
    OUTPUT_SECRETS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .push(value.into());
}

#[derive(Debug, Subcommand)]
pub enum HostCommand {
    /// Enroll one new identity and manage a new exact host session
    Run(HostRun),
    /// Resume the credential, journal and exact session in a private config
    Resume {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        shutdown_after: Option<u64>,
    },
    /// Query the live controller without opening another journal writer
    Status {
        #[arg(long)]
        config: PathBuf,
    },
    /// Explicitly release pending work for another bounded activation attempt
    Retry {
        #[arg(long)]
        config: PathBuf,
    },
    /// Answer one outstanding approval with an explicit JSON response
    Approval {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        response: String,
    },
    /// Stop the controller and reap its managed child
    Shutdown {
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Codex,
    Claude,
}

#[derive(Debug, Args)]
pub struct HostRun {
    #[arg(long, value_enum)]
    provider: Provider,
    #[arg(long, default_value = ".")]
    workspace: PathBuf,
    /// Private state outside the broker root
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// New private resume file; defaults to a unique file in --state-dir
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, default_value = "agent")]
    display_name: String,
    #[arg(long = "queue")]
    queues: Vec<String>,
    #[arg(long = "document")]
    documents: Vec<String>,
    /// Override the provider executable, including a local protocol fake
    #[arg(long)]
    executable: Option<PathBuf>,
    /// Private child environment entry KEY=VALUE; repeat as needed
    #[arg(long = "env")]
    environment: Vec<String>,
    /// Explicit Codex configuration override; does not edit global settings
    #[arg(long = "config-override")]
    config_overrides: Vec<String>,
    #[arg(long, default_value_t = 30)]
    request_timeout_s: u64,
    /// Stop after this many seconds; useful for bounded operator qualification
    #[arg(long)]
    shutdown_after: Option<u64>,
}

// Deliberately no Debug: these records contain credentials.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderOptions {
    kind: Provider,
    executable: PathBuf,
    env: BTreeMap<String, String>,
    config_overrides: Vec<String>,
    request_timeout_s: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LaunchConfig {
    v: u16,
    token: String,
    broker_root: PathBuf,
    state_base: PathBuf,
    binding: Binding,
    provider: ProviderOptions,
    documents: Vec<String>,
    queues: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveLaunch {
    v: u16,
    control_socket: PathBuf,
    control_secret: String,
    operator_socket: PathBuf,
    operator_secret: String,
}

struct PrivateLaunch(PathBuf);
impl PrivateLaunch {
    fn create() -> Result<Self> {
        let path = PathBuf::from("/tmp").join(format!("coop-host-{}", secret()?));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(err)?;
        Ok(Self(path))
    }
}
impl Drop for PrivateLaunch {
    fn drop(&mut self) {
        // This randomly named directory was created exclusively by this launch.
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn secret() -> Result<String> {
    let value = registry::new_token().map_err(err)?;
    remember_secret(&value);
    Ok(value)
}

fn uuid() -> Result<String> {
    let mut bytes = id::random_bytes::<16>().map_err(err)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}

fn private_dir(path: &Path) -> Result<PathBuf> {
    if !path.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .map_err(err)?;
    }
    let metadata = fs::symlink_metadata(path).map_err(err)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err("host state/config parent must be a private directory with mode 0700".into());
    }
    fs::canonicalize(path).map_err(err)
}

fn private_read<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let metadata = fs::symlink_metadata(path).map_err(err)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err("host configuration must be a regular private file with mode 0600".into());
    }
    let parent = path
        .parent()
        .ok_or("host configuration needs a private parent")?;
    let parent_meta = fs::metadata(parent).map_err(err)?;
    if parent_meta.uid() != metadata.uid() || parent_meta.permissions().mode() & 0o777 != 0o700 {
        return Err("host configuration parent must have the same owner and mode 0700".into());
    }
    let bytes = fs::read(path).map_err(err)?;
    serde_json::from_slice(&bytes).map_err(|_| "invalid private host configuration".into())
}

fn private_write(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().ok_or("host configuration needs a parent")?;
    private_dir(parent)?;
    if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink() || !m.is_file()) {
        return Err("host configuration destination is not a regular file".into());
    }
    let temporary = parent.join(format!(".host-{}.tmp", secret()?));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(err)?;
    let bytes = serde_json::to_vec_pretty(value).map_err(err)?;
    if let Err(error) = file
        .write_all(&bytes)
        .and_then(|_| file.sync_all())
        .and_then(|_| fs::rename(&temporary, path))
    {
        let _ = fs::remove_file(&temporary);
        return Err(err(error));
    }
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(err)
}

fn active_path(config: &Path) -> PathBuf {
    config.with_extension("active.json")
}

fn remember_config(config: &LaunchConfig) {
    remember_secret(&config.token);
    for (key, value) in &config.provider.env {
        let upper = key.to_ascii_uppercase();
        if ["TOKEN", "SECRET", "PASSWORD", "KEY", "CREDENTIAL"]
            .iter()
            .any(|part| upper.contains(part))
        {
            remember_secret(value);
        }
    }
}

fn redact(value: &mut Value, secrets: &[String]) {
    match value {
        Value::String(text) => {
            for secret in secrets.iter().filter(|value| !value.is_empty()) {
                *text = text.replace(secret, "[redacted]");
            }
        }
        Value::Array(values) => values.iter_mut().for_each(|value| redact(value, secrets)),
        Value::Object(values) => values.values_mut().for_each(|value| redact(value, secrets)),
        _ => {}
    }
}

fn emit(mut value: Value, secrets: &[String]) {
    redact(&mut value, secrets);
    if let Some(known) = OUTPUT_SECRETS.get() {
        redact(&mut value, &known.lock().unwrap());
    }
    let mut stdout = std::io::stdout().lock();
    let _ = serde_json::to_writer(&mut stdout, &value);
    let _ = stdout.write_all(b"\n");
    let _ = stdout.flush();
}

async fn broker(root: &Path, method: &str, params: Value) -> Result<Value> {
    client::call(root, method, params).await.map_err(err)
}

async fn validate_credential(config: &LaunchConfig) -> Result<()> {
    let metadata = broker(
        &config.broker_root,
        "instance_metadata",
        json!({"token":config.token}),
    )
    .await?;
    let subscriptions = broker(
        &config.broker_root,
        "list_arrival_subscriptions",
        json!({"token":config.token}),
    )
    .await?;
    if metadata["agent_id"] != config.binding.agent_id
        || subscriptions["agent_id"] != config.binding.agent_id
        || subscriptions["bus_id"] != config.binding.bus_id
    {
        return Err("resume credential does not belong to the persisted bus and agent".into());
    }
    if config.v != 1
        || config.binding.generation == 0
        || config.binding.exact_host_session_id.is_empty()
    {
        return Err("unsupported or incomplete host resume configuration".into());
    }
    let workspace = fs::canonicalize(&config.binding.workspace).map_err(err)?;
    if workspace.to_str() != Some(&config.binding.workspace) || !workspace.is_dir() {
        return Err("resume workspace differs from the exact canonical binding".into());
    }
    Ok(())
}

fn mcp_args(config: &LaunchConfig, active: &ActiveLaunch) -> Vec<String> {
    vec![
        "--root".into(),
        config.broker_root.to_string_lossy().into_owned(),
        "mcp-stdio".into(),
        "--resume".into(),
        "--host-control".into(),
        active.control_socket.to_string_lossy().into_owned(),
        "--host-bus-id".into(),
        config.binding.bus_id.clone(),
        "--host-agent-id".into(),
        config.binding.agent_id.clone(),
    ]
}

fn codex_options(config: &LaunchConfig, active: &ActiveLaunch) -> Result<CodexAdapterConfig> {
    let mut env = config.provider.env.clone();
    env.insert("COOP_TOKEN".into(), config.token.clone());
    env.insert(
        "COOP_HOST_CONTROL_SECRET".into(),
        active.control_secret.clone(),
    );
    // The explicit root argument wins over any inherited service root.
    env.insert(
        "COOPERATORS_HOME".into(),
        config.broker_root.to_string_lossy().into_owned(),
    );
    let executable = std::env::current_exe().map_err(err)?;
    let mut overrides = config.provider.config_overrides.clone();
    overrides.extend([
        format!("mcp_servers.cooperators.command={}", json!(executable)),
        format!(
            "mcp_servers.cooperators.args={}",
            json!(mcp_args(config, active))
        ),
        format!(
            "mcp_servers.cooperators.env_vars={}",
            json!(["COOP_TOKEN", "COOP_HOST_CONTROL_SECRET", "COOPERATORS_HOME"])
        ),
    ]);
    Ok(CodexAdapterConfig {
        executable: config.provider.executable.clone(),
        env,
        config_overrides: overrides,
        request_timeout: Duration::from_secs(config.provider.request_timeout_s),
    })
}

async fn adapter(
    config: &LaunchConfig,
    active: &ActiveLaunch,
    launch: &PrivateLaunch,
    resume: bool,
) -> Result<Box<dyn HostAdapter>> {
    match config.provider.kind {
        Provider::Codex => Ok(Box::new(
            CodexAppServerAdapter::spawn(codex_options(config, active)?, &config.binding)
                .await
                .map_err(err)?,
        )),
        Provider::Claude => {
            let mut env = config.provider.env.clone();
            env.insert("COOP_TOKEN".into(), config.token.clone());
            env.insert(
                "COOP_HOST_CONTROL_SECRET".into(),
                active.control_secret.clone(),
            );
            env.insert(
                "COOPERATORS_HOME".into(),
                config.broker_root.to_string_lossy().into_owned(),
            );
            let mcp_config = launch.0.join("mcp.json");
            private_write(
                &mcp_config,
                &json!({"mcpServers":{"cooperators":{"command":std::env::current_exe().map_err(err)?,"args":mcp_args(config,active),"env":{"COOP_TOKEN":config.token,"COOP_HOST_CONTROL_SECRET":active.control_secret,"COOPERATORS_HOME":config.broker_root}}}}),
            )?;
            Ok(Box::new(
                ClaudeHostAdapter::spawn(
                    ClaudeAdapterConfig {
                        executable: config.provider.executable.clone(),
                        env,
                        mcp_config: Some(mcp_config),
                        resume_existing: resume,
                        request_timeout: Duration::from_secs(config.provider.request_timeout_s),
                    },
                    &config.binding,
                )
                .await
                .map_err(err)?,
            ))
        }
    }
}

struct OperatorRequest {
    value: Value,
    response: Option<oneshot::Sender<Value>>,
    delivered: Option<oneshot::Receiver<()>>,
}

async fn operator_server(
    listener: UnixListener,
    secret: String,
    sender: mpsc::Sender<OperatorRequest>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut children = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            result = children.join_next(), if !children.is_empty() => { let _ = result; },
            connection = listener.accept() => {
                let Ok((stream, _)) = connection else { break; };
                if children.len() >= 16 { drop(stream); continue; }
                let secret = secret.clone();
                let sender = sender.clone();
                children.spawn(async move {
                    let _ = tokio::time::timeout(Duration::from_secs(40), async {
                        let mut reader = BufReader::new(stream);
                        let mut bytes = Vec::new();
                        // read_until is capped by Take so malformed clients cannot
                        // retain unbounded bytes in the controller.
                        let count = tokio::io::AsyncReadExt::take(&mut reader, MAX_OPERATOR_BYTES as u64 + 1).read_until(b'\n', &mut bytes).await?;
                        if count == 0 || bytes.len() > MAX_OPERATOR_BYTES || bytes.last() != Some(&b'\n') { return Ok::<(),std::io::Error>(()); }
                        let request: Value = match serde_json::from_slice(&bytes) { Ok(value) => value, Err(_) => return Ok(()) };
                        if request["secret"].as_str() != Some(&secret) { return Ok(()); }
                        let (tx,rx) = oneshot::channel();
                        let (delivered_tx,delivered_rx) = oneshot::channel();
                        if sender.send(OperatorRequest {value:request["command"].clone(),response:Some(tx),delivered:Some(delivered_rx)}).await.is_err() { return Ok(()); }
                        if let Ok(response) = rx.await {
                            let mut bytes = serde_json::to_vec(&response)?;
                            bytes.push(b'\n');
                            reader.get_mut().write_all(&bytes).await?;
                            let _ = delivered_tx.send(());
                        }
                        Ok(())
                    }).await;
                });
            }
        }
    }
    children.abort_all();
    while children.join_next().await.is_some() {}
}

fn operator_stdin(sender: mpsc::Sender<OperatorRequest>) {
    // A detached OS input thread does not retain the Tokio runtime at shutdown.
    // It owns no child, socket, journal or secret; EOF leaves IPC control alive.
    std::thread::spawn(move || {
        let mut input = std::io::BufReader::new(std::io::stdin());
        loop {
            let mut line = String::new();
            match (&mut input)
                .take(MAX_OPERATOR_BYTES as u64 + 1)
                .read_line(&mut line)
            {
                Ok(0) | Err(_) => break,
                Ok(_) if line.len() > MAX_OPERATOR_BYTES => break,
                Ok(_) => {}
            }
            let value = serde_json::from_str(&line).unwrap_or_else(|_| json!({"type":"invalid"}));
            if sender
                .blocking_send(OperatorRequest {
                    value,
                    response: None,
                    delivered: None,
                })
                .is_err()
            {
                break;
            }
        }
    });
}

async fn operator_call(path: &Path, command: Value) -> Result<Value> {
    let active: ActiveLaunch = private_read(&active_path(path))?;
    remember_secret(&active.control_secret);
    remember_secret(&active.operator_secret);
    let result = tokio::time::timeout(Duration::from_secs(40), async {
        let stream = UnixStream::connect(&active.operator_socket)
            .await
            .map_err(err)?;
        let mut reader = BufReader::new(stream);
        let mut bytes =
            serde_json::to_vec(&json!({"secret":active.operator_secret,"command":command}))
                .map_err(err)?;
        bytes.push(b'\n');
        reader.get_mut().write_all(&bytes).await.map_err(err)?;
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::take(&mut reader, MAX_OPERATOR_BYTES as u64 + 1)
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(err)?;
        if bytes.len() > MAX_OPERATOR_BYTES {
            return Err("operator response exceeds limit".into());
        }
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| "invalid operator response".to_string())?;
        if value.get("error").is_some() {
            return Err("operator rejected command; check the live host output".into());
        }
        Ok(value)
    })
    .await
    .map_err(|_| "host controller request timed out".to_string())?;
    result
}

async fn subscribe(config: &LaunchConfig) -> Result<()> {
    for (kind, names, events) in [
        (
            "document",
            &config.documents,
            json!(["doc.published", "doc.updated"]),
        ),
        ("queue", &config.queues, json!(["queue.task_available"])),
    ] {
        for name in names {
            broker(&config.broker_root,"subscribe_arrivals",json!({"token":config.token,"client_key":format!("host-v1-{kind}-{name}"),"resource":{"type":kind,"name":name},"event_kinds":events,"catch_up":true})).await?;
        }
    }
    Ok(())
}

async fn managed(
    config: LaunchConfig,
    path: PathBuf,
    launch: PrivateLaunch,
    active: ActiveLaunch,
    resume: bool,
    shutdown_after: Option<u64>,
) -> Result<()> {
    remember_config(&config);
    remember_secret(&active.control_secret);
    remember_secret(&active.operator_secret);
    validate_credential(&config).await?;
    let bridge = HostBridge::open(
        &config.state_base,
        &config.broker_root,
        config.binding.clone(),
        BridgeLimits::default(),
    )
    .map_err(err)?;
    let shared: SharedHostBridge = Arc::new(Mutex::new(bridge));
    let control = HostControlServer::start(
        &active.control_socket,
        active.control_secret.clone(),
        Arc::clone(&shared),
    )
    .await
    .map_err(err)?;
    let host = match adapter(&config, &active, &launch, resume).await {
        Ok(host) => host,
        Err(error) => {
            let _ = control.shutdown().await;
            return Err(error);
        }
    };
    let mut runner = BridgeRunner::new(Arc::clone(&shared), host);
    let listener = match UnixListener::bind(&active.operator_socket) {
        Ok(listener) => listener,
        Err(error) => {
            let _ = runner.shutdown().await;
            let _ = control.shutdown().await;
            return Err(err(error));
        }
    };
    let (stop, receiver) = watch::channel(false);
    let (commands, mut requests) = mpsc::channel(32);
    let operators = tokio::spawn(operator_server(
        listener,
        active.operator_secret.clone(),
        commands.clone(),
        receiver.clone(),
    ));
    let mut secrets = vec![
        config.token.clone(),
        active.control_secret.clone(),
        active.operator_secret.clone(),
    ];
    for (key, value) in &config.provider.env {
        let upper = key.to_ascii_uppercase();
        if ["TOKEN", "SECRET", "PASSWORD", "KEY", "CREDENTIAL"]
            .iter()
            .any(|part| upper.contains(part))
        {
            secrets.push(value.clone());
        }
    }
    let setup = async {
        private_write(&active_path(&path), &active)?;
        subscribe(&config).await?;
        Ok::<(), String>(())
    }
    .await;
    if let Err(error) = setup {
        let _ = stop.send(true);
        let _ = operators.await;
        let _ = runner.shutdown().await;
        let _ = control.shutdown().await;
        return Err(error);
    }
    let broker_client = Arc::new(UdsArrivalClient::new(
        config.broker_root.clone(),
        config.token.clone(),
        config.binding.bus_id.clone(),
        config.binding.agent_id.clone(),
    ));
    let mut receipts = tokio::spawn(run_receipt_loop(
        Arc::clone(&shared),
        broker_client,
        receiver,
    ));
    operator_stdin(commands);
    emit(
        json!({"type":"host_ready","config":path,"status":shared.lock().await.status()}),
        &secrets,
    );
    let deadline = async move {
        match shutdown_after {
            Some(seconds) => tokio::time::sleep(Duration::from_secs(seconds)).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(deadline);
    let mut interval = tokio::time::interval(Duration::from_millis(200));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut receipt_done = false;
    let mut last_status = Value::Null;
    let mut result = Ok(());
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            _=&mut deadline=>break,
            done=&mut receipts, if !receipt_done => {
                receipt_done=true;
                match done { Ok(Ok(()))=>{}, Ok(Err(error))=>emit(json!({"type":"receipt_error","error":error.to_string()}),&secrets), Err(_)=>emit(json!({"type":"receipt_error","error":"receipt task stopped"}),&secrets) }
            }
            request=requests.recv()=>{
                let Some(request)=request else { break; };
                let operation=match request.value["type"].as_str() {
                    Some("status")=>Ok(json!({"type":"status","status":shared.lock().await.status()})),
                    Some("retry")=>shared.lock().await.retry_pending(id::unix_millis()).map(|_|json!({"type":"retry","scheduled":true})).map_err(err),
                    Some("approval")=>{
                        if request.value.get("request_id").is_none() || request.value.get("response").is_none() { Err("approval needs request_id and explicit response".into()) }
                        else { runner.answer_approval(&request.value["request_id"],request.value["response"].clone()).await.map(|_|json!({"type":"approval_answered","request_id":request.value["request_id"]})).map_err(err) }
                    }
                    Some("shutdown")=>Ok(json!({"type":"shutdown","accepted":true})),
                    _=>Err("expected status, retry, approval or shutdown operator command".into()),
                };
                let mut response=operation.unwrap_or_else(|error|json!({"type":"operator_error","error":error}));
                redact(&mut response,&secrets);
                emit(response.clone(),&secrets);
                if let Some(sender)=request.response { let _=sender.send(response); }
                if request.value["type"]=="shutdown" {
                    if let Some(delivered)=request.delivered { let _=tokio::time::timeout(Duration::from_secs(1),delivered).await; }
                    break;
                }
            }
            _=interval.tick()=>{
                match runner.tick(id::unix_millis()).await {
                    Ok(events)=>for event in events { emit(json!({"type":"host_event","event":event}),&secrets); },
                    Err(error)=>{ let _=shared.lock().await.attention(error.to_string()); emit(json!({"type":"host_error","error":error.to_string()}),&secrets); }
                }
                let status=serde_json::to_value(shared.lock().await.status()).map_err(err)?;
                if status!=last_status { emit(json!({"type":"status","status":status}),&secrets); last_status=status; }
            }
        }
    }
    let _ = stop.send(true);
    if !receipt_done {
        if tokio::time::timeout(Duration::from_secs(2), &mut receipts)
            .await
            .is_err()
        {
            receipts.abort();
            let _ = receipts.await;
        }
    }
    if let Err(error) = runner.shutdown().await {
        result = Err(err(error));
    }
    if operators.await.is_err() {
        result = Err("operator server stopped unexpectedly".into());
    }
    if let Err(error) = control.shutdown().await {
        result = Err(err(error));
    }
    emit(
        json!({"type":"host_stopped","config":path,"status":shared.lock().await.status()}),
        &secrets,
    );
    drop(runner);
    drop(shared);
    drop(launch);
    result
}

fn default_state_dir() -> PathBuf {
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".local/state")
        })
        .join("cooperators/hosts")
}

async fn new_host(root: PathBuf, options: HostRun) -> Result<()> {
    if options.request_timeout_s == 0 {
        return Err("request timeout must be positive".into());
    }
    let root = fs::canonicalize(root).map_err(err)?;
    let workspace = fs::canonicalize(options.workspace).map_err(err)?;
    if !workspace.is_dir() {
        return Err("host workspace must be a directory".into());
    }
    let base = private_dir(&options.state_dir.unwrap_or_else(default_state_dir))?;
    if base.starts_with(&root) {
        return Err("host state must be outside the broker root".into());
    }
    let path = options.config.unwrap_or_else(|| {
        base.join(format!(
            "{}.json",
            id::new_ulid().unwrap_or_else(|_| "launch".into())
        ))
    });
    let parent = private_dir(path.parent().ok_or("resume config needs a parent")?)?;
    let path = parent.join(path.file_name().ok_or("resume config needs a filename")?);
    if path.starts_with(&root) || path.exists() {
        return Err("new host config must be a new file outside the broker root".into());
    }
    let mut environment = BTreeMap::new();
    for item in options.environment {
        let (key, value) = item.split_once('=').ok_or("--env requires KEY=VALUE")?;
        if key.is_empty()
            || key.contains('\0')
            || value.contains('\0')
            || [
                "COOP_TOKEN",
                "COOP_HOST_CONTROL_SECRET",
                "COOPERATORS_HOME",
                "COOP_SOCKET",
            ]
            .contains(&key)
        {
            return Err("invalid or reserved managed host environment key".into());
        }
        let upper = key.to_ascii_uppercase();
        if ["TOKEN", "SECRET", "PASSWORD", "KEY", "CREDENTIAL"]
            .iter()
            .any(|part| upper.contains(part))
        {
            remember_secret(value);
        }
        environment.insert(key.into(), value.into());
    }
    let token = secret()?;
    let kind = match options.provider {
        Provider::Codex => "codex",
        Provider::Claude => "claude",
    };
    let enrolled = broker(
        &root,
        "enroll_agent",
        json!({"launch_key":token,"kind":kind,"display_name":options.display_name}),
    )
    .await?;
    if enrolled["token"] != token {
        return Err("enrollment did not preserve the launch credential".into());
    }
    let agent_id = enrolled["agent_id"]
        .as_str()
        .ok_or("enrollment omitted agent identity")?
        .to_string();
    let subscriptions = broker(&root, "list_arrival_subscriptions", json!({"token":token})).await?;
    if subscriptions["agent_id"] != agent_id {
        return Err("subscription identity differs from enrollment".into());
    }
    let bus_id = subscriptions["bus_id"]
        .as_str()
        .ok_or("subscription listing omitted bus identity")?
        .to_string();
    let launch = PrivateLaunch::create()?;
    let active = ActiveLaunch {
        v: 1,
        control_socket: launch.0.join("control.sock"),
        control_secret: secret()?,
        operator_socket: launch.0.join("operator.sock"),
        operator_secret: secret()?,
    };
    let mut config = LaunchConfig {
        v: 1,
        token,
        broker_root: root,
        state_base: base,
        binding: Binding {
            binding_id: id::new_ulid().map_err(err)?,
            generation: 1,
            bus_id,
            agent_id,
            adapter_kind: match options.provider {
                Provider::Codex => "codex_app_server",
                Provider::Claude => "claude_host",
            }
            .into(),
            exact_host_session_id: uuid()?,
            workspace: workspace.to_str().ok_or("workspace must be UTF-8")?.into(),
            capability_status: Readiness::Configured,
        },
        provider: ProviderOptions {
            kind: options.provider,
            executable: options.executable.unwrap_or_else(|| PathBuf::from(kind)),
            env: environment,
            config_overrides: options.config_overrides,
            request_timeout_s: options.request_timeout_s,
        },
        documents: options.documents,
        queues: options.queues,
    };
    if matches!(config.provider.kind, Provider::Codex) {
        config.binding.exact_host_session_id =
            CodexAppServerAdapter::create_session(&codex_options(&config, &active)?, &workspace)
                .await
                .map_err(err)?;
    }
    private_write(&path, &config)?;
    // Emit the recovery path before spawning the provider, including on an
    // adapter startup failure. The file itself is never printed.
    emit(
        json!({"type":"host_configured","config":path,"binding":config.binding}),
        &[config.token.clone(), active.control_secret.clone()],
    );
    managed(config, path, launch, active, false, options.shutdown_after).await
}

async fn execute(root: PathBuf, token: Option<String>, command: HostCommand) -> Result<()> {
    match command {
        HostCommand::Run(options) => new_host(root, options).await,
        HostCommand::Resume {
            config: path,
            shutdown_after,
        } => {
            let config: LaunchConfig = private_read(&path)?;
            remember_config(&config);
            if token.is_some_and(|token| token != config.token) {
                return Err(
                    "explicit credential differs from the private resume configuration".into(),
                );
            }
            validate_credential(&config).await?;
            let launch = PrivateLaunch::create()?;
            let active = ActiveLaunch {
                v: 1,
                control_socket: launch.0.join("control.sock"),
                control_secret: secret()?,
                operator_socket: launch.0.join("operator.sock"),
                operator_secret: secret()?,
            };
            managed(config, path, launch, active, true, shutdown_after).await
        }
        HostCommand::Status { config: path } => {
            let config: LaunchConfig = private_read(&path)?;
            remember_config(&config);
            let active: ActiveLaunch = private_read(&active_path(&path))?;
            let client = HostControlClient::new(
                active.control_socket,
                active.control_secret.clone(),
                config.binding.bus_id,
                config.binding.agent_id,
            );
            let status = client.call("status", json!({})).await.map_err(err)?;
            emit(
                json!({"type":"status","status":status}),
                &[config.token, active.control_secret, active.operator_secret],
            );
            Ok(())
        }
        HostCommand::Retry { config } => {
            emit(operator_call(&config, json!({"type":"retry"})).await?, &[]);
            Ok(())
        }
        HostCommand::Shutdown { config } => {
            emit(
                operator_call(&config, json!({"type":"shutdown"})).await?,
                &[],
            );
            Ok(())
        }
        HostCommand::Approval {
            config,
            request_id,
            response,
        } => {
            let request_id =
                serde_json::from_str::<Value>(&request_id).unwrap_or(Value::String(request_id));
            let response = serde_json::from_str::<Value>(&response)
                .map_err(|_| "--response must be explicit JSON".to_string())?;
            emit(
                operator_call(
                    &config,
                    json!({"type":"approval","request_id":request_id,"response":response}),
                )
                .await?,
                &[],
            );
            Ok(())
        }
    }
}

pub async fn run(root: PathBuf, token: Option<String>, command: HostCommand) -> ExitCode {
    match execute(root, token, command).await {
        Ok(()) => ExitCode::SUCCESS,
        // Internal error details are emitted inside the credential-aware
        // controller. Startup errors contain only local protocol diagnostics.
        Err(error) => {
            emit(json!({"type":"host_error","error":error}), &[]);
            ExitCode::from(1)
        }
    }
}
