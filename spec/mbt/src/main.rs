//! Cooperators MBT harness.
//!
//! Replays Apalache witness traces of `spec/MBCooperators.tla` against the
//! real `DaemonState` (in-process, no sockets) through the ModelMirrors
//! protocol. Oracle boundary: each action the model enabled must succeed in
//! the implementation, and the reconstructed abstract state (log + ack
//! cursors) must match the trace exactly at every step. Rejection paths are
//! proven by TLC on spec/Cooperators.tla, not exercised here.
//!
//! Usage: MIRROR_BIN=/path/to/ModelMirrors cargo run --offline [--skip-gen]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use cooperators::envelope::Body;
use cooperators::messaging::SendRequest;
use cooperators::registry::AgentIdentity;
use cooperators::server::{DaemonConfig, DaemonState};

use mirrorrust::{
    as_int, as_str, get_param, run_client_with_traces, ApalacheConfig, State, StateComputer, Value,
};
use num_bigint::BigInt;

const AGENTS: [&str; 2] = ["a1", "a2"];
const KEYS: [&str; 2] = ["k1", "k2"];
const MAX_ATTEMPTS: u64 = 2;
const QUEUE: &str = "q0";
const DOC: &str = "d0";
const LEASE_TTL_S: u64 = 3600; // far future: expiry happens only via the forced hook

fn vstr(s: &str) -> Value {
    Value::Str(s.to_string())
}
fn vint(n: u64) -> Value {
    Value::Int(BigInt::from(n))
}

/// One flattened abstract log record, field order matching the ITF encoding
/// (BTreeMap keeps keys sorted: base, fence, from, holder, key, res, to, type).
fn rec(ty: &str, from: &str, to: &str, key: &str, res: &str, holder: &str, fence: u64, base: u64) -> Value {
    let mut m = State::new();
    m.insert("type".to_string(), vstr(ty));
    m.insert("from".to_string(), vstr(from));
    m.insert("to".to_string(), vstr(to));
    m.insert("key".to_string(), vstr(key));
    m.insert("res".to_string(), vstr(res));
    m.insert("holder".to_string(), vstr(holder));
    m.insert("fence".to_string(), vint(fence));
    m.insert("base".to_string(), vint(base));
    Value::Record(m)
}

fn parse_seq(s: &str) -> u64 {
    s.strip_prefix("seq:").unwrap_or(s).parse().unwrap_or(0)
}

struct CoopSut {
    rt: tokio::runtime::Runtime,
    _tmp: tempfile::TempDir,
    root: PathBuf,
    state: DaemonState,
    ids: BTreeMap<String, String>,
    tokens: BTreeMap<String, String>,
    log: Vec<Value>,
    /// (lease_id, concrete fence, abstract fence = position of claim in `log`)
    q_lease: Option<(String, u64, u64)>,
    d_lease: Option<(String, u64, u64)>,
    doc_version: u64,
}

impl CoopSut {
    fn new() -> Self {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("coop-root");
        let state = DaemonState::with_config(DaemonConfig::new(&root)).expect("daemon state");
        let mut sut = CoopSut {
            rt,
            _tmp: tmp,
            root,
            state,
            ids: BTreeMap::new(),
            tokens: BTreeMap::new(),
            log: Vec::new(),
            q_lease: None,
            d_lease: None,
            doc_version: 0,
        };
        sut.bootstrap();
        sut
    }

    fn bootstrap(&mut self) {
        for name in AGENTS {
            let reg = self
                .rt
                .block_on(self.state.register_agent(
                    name.to_string(),
                    Some("mbt".to_string()),
                    None,
                    None,
                ))
                .expect("register agent");
            self.ids.insert(name.to_string(), reg.agent_id);
            self.tokens.insert(name.to_string(), reg.token);
        }
        self.rt.block_on(self.state.create_queue(QUEUE)).expect("create queue");
        self.rt
            .block_on(self.state.enqueue(QUEUE, serde_json::json!("task0"), Some(MAX_ATTEMPTS)))
            .expect("enqueue task0");
    }

    fn reopen_after_crash(&mut self) {
        self.state = DaemonState::with_config(DaemonConfig::new(&self.root)).expect("reopen");
        // Tokens survive in registry.json; refresh identities.
        for name in AGENTS {
            let token = self.tokens[name].clone();
            self.rt
                .block_on(self.state.authenticate(&token))
                .expect("re-authenticate after crash");
        }
        // Leases survive via log replay: keep stored (lease_id, fence) pairs.
    }

    fn identity(&self, name: &str) -> AgentIdentity {
        let token = &self.tokens[name];
        self.rt
            .block_on(self.state.authenticate(token))
            .expect("authenticate")
    }

    /// Authoritative read-back of an agent's acknowledged cursor.
    fn acked(&self, name: &str) -> u64 {
        let id = &self.ids[name];
        self.rt
            .block_on(self.state.wait_message(id, None, Duration::from_millis(0)))
            .map(|b| parse_seq(&b.acked_cursor))
            .unwrap_or(0)
    }

    fn report(&self) -> State {
        let mut m = State::new();
        m.insert("log".to_string(), Value::Seq(self.log.clone()));
        let ack_map: Vec<(Value, Value)> = AGENTS
            .iter()
            .map(|a| (vstr(a), vint(self.acked(a))))
            .collect();
        m.insert("ackCursor".to_string(), Value::Map(ack_map));
        // The replay comparison covers exactly the spec's non-meta
        // variables: log and ackCursor. Constants are not compared.
        m
    }

    fn param<'a>(params: &'a State, field: &str) -> Option<&'a Value> {
        get_param(params, "parameters").and_then(|r| r.get(field))
    }
    fn pstr(params: &State, field: &str) -> String {
        Self::param(params, field)
            .and_then(as_str)
            .unwrap_or("-")
            .to_string()
    }
    fn pint(params: &State, field: &str) -> u64 {
        Self::param(params, field)
            .and_then(as_int)
            .and_then(|b| b.to_string().parse().ok())
            .unwrap_or(0)
    }
}

impl StateComputer for CoopSut {
    fn compute(&mut self, action: &str, params: &State, _prev: &State) -> State {
        match action {
            "init" => {} // bootstrap() already ran
            "send" => {
                let (f, t, k) = (Self::pstr(params, "from"), Self::pstr(params, "to"), Self::pstr(params, "key"));
                let from = self.identity(&f);
                let req = SendRequest {
                    to: self.ids[&t].clone(),
                    kind: "mbt".to_string(),
                    body: Body { mime: "text/plain".to_string(), text: "mbt".to_string() },
                    reply_to: None,
                    idempotency_key: Some(k.clone()),
                    ttl: None,
                };
                if self.rt.block_on(self.state.send_message(&from, req)).is_ok() {
                    self.log.push(rec("msg", &f, &t, &k, "-", "-", 0, 0));
                }
            }
            "ack" => {
                let a = Self::pstr(params, "holder");
                let c = Self::pint(params, "cursor");
                let id = self.ids[&a].clone();
                let _ = self.rt.block_on(self.state.ack(&id, &c.to_string()));
            }
            "claim" => {
                let res = Self::pstr(params, "res");
                let h = Self::pstr(params, "holder");
                let who = self.identity(&h);
                let abs_fence = (self.log.len() as u64) + 1;
                if res == QUEUE {
                    if let Ok(r) = self.rt.block_on(self.state.claim_task(&who, QUEUE, LEASE_TTL_S)) {
                        self.q_lease = Some((r.lease_id, r.fence, abs_fence));
                        self.log.push(rec("claim", "-", "-", "-", &res, &h, 0, 0));
                    }
                } else if let Ok(r) = self.rt.block_on(self.state.claim_doc(&who, DOC, LEASE_TTL_S)) {
                    self.d_lease = Some((r.lease_id, r.fence, abs_fence));
                    self.log.push(rec("claim", "-", "-", "-", &res, &h, 0, 0));
                }
            }
            "expire" => {
                let res = Self::pstr(params, "res");
                if res == QUEUE {
                    let fence = self.q_lease.as_ref().map(|(_, _, af)| *af).unwrap_or(0);
                    let n = self.rt.block_on(self.state.debug_force_expire_queue(QUEUE)).unwrap_or(0);
                    if n > 0 {
                        self.q_lease = None;
                        self.log.push(rec("expire", "-", "-", "-", &res, "-", fence, 0));
                    }
                } else {
                    let fence = self.d_lease.as_ref().map(|(_, _, af)| *af).unwrap_or(0);
                    let ok = self.rt.block_on(self.state.debug_force_expire_doc(DOC)).unwrap_or(false);
                    if ok {
                        self.d_lease = None;
                        self.log.push(rec("expire", "-", "-", "-", &res, "-", fence, 0));
                    }
                }
            }
            "complete" => {
                let h = Self::pstr(params, "holder");
                if let Some((lease_id, fence, abs_fence)) = self.q_lease.take() {
                    let who = self.identity(&h);
                    if self
                        .rt
                        .block_on(self.state.complete_task(&who, &lease_id, fence, serde_json::json!("done"), None))
                        .is_ok()
                    {
                        self.log.push(rec("complete", "-", "-", "-", QUEUE, &h, abs_fence, 0));
                    } else {
                        self.q_lease = Some((lease_id, fence, abs_fence));
                    }
                }
            }
            "fail" => {
                let h = Self::pstr(params, "holder");
                if let Some((lease_id, fence, abs_fence)) = self.q_lease.take() {
                    let who = self.identity(&h);
                    if self
                        .rt
                        .block_on(self.state.fail_task(&who, &lease_id, fence, "mbt-fail"))
                        .is_ok()
                    {
                        self.log.push(rec("fail", "-", "-", "-", QUEUE, &h, abs_fence, 0));
                    } else {
                        self.q_lease = Some((lease_id, fence, abs_fence));
                    }
                }
            }
            "release" => {
                let h = Self::pstr(params, "holder");
                if let Some((lease_id, fence, abs_fence)) = self.d_lease.take() {
                    let who = self.identity(&h);
                    if self
                        .rt
                        .block_on(self.state.release_doc(&who, &lease_id, fence))
                        .is_ok()
                    {
                        self.log.push(rec("release", "-", "-", "-", DOC, &h, abs_fence, 0));
                    } else {
                        self.d_lease = Some((lease_id, fence, abs_fence));
                    }
                }
            }
            "publish" => {
                let h = Self::pstr(params, "holder");
                let who = self.identity(&h);
                if self
                    .rt
                    .block_on(self.state.publish_doc(
                        &who,
                        DOC,
                        "text/plain",
                        Body { mime: "text/plain".to_string(), text: "mbt doc".to_string() },
                        None,
                    ))
                    .is_ok()
                {
                    self.doc_version = 1;
                    self.log.push(rec("docput", "-", "-", "-", DOC, &h, 0, 0));
                }
            }
            "update" => {
                let h = Self::pstr(params, "holder");
                let who = self.identity(&h);
                let base = self.doc_version;
                let abs_fence = self.d_lease.as_ref().map(|(_, _, af)| *af).unwrap_or(0);
                let lease_owned: Option<(String, u64)> =
                    self.d_lease.as_ref().map(|(lid, f, _)| (lid.clone(), *f));
                let lease = lease_owned.as_ref().map(|(lid, f)| (lid.as_str(), *f));
                if self
                    .rt
                    .block_on(self.state.update_doc(
                        &who,
                        DOC,
                        base,
                        Body { mime: "text/plain".to_string(), text: "mbt update".to_string() },
                        None,
                        lease,
                    ))
                    .is_ok()
                {
                    self.doc_version += 1;
                    self.log.push(rec("docput", "-", "-", "-", DOC, &h, abs_fence, base));
                }
            }
            "crash" => self.reopen_after_crash(),
            other => eprintln!("mbt: unknown action {other:?}"),
        }
        self.report()
    }
}

fn spec_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../MBCooperators.tla")
}

fn apalache_cfg() -> ApalacheConfig {
    ApalacheConfig {
        spec_path: "MBCooperators.tla".to_string(),
        init_predicate: None,
        next_predicate: None,
        const_init: Some("CInit".to_string()),
        invariant: "TraceComplete".to_string(),
        length_bound: 12,
        param_vars: Some("parameters".to_string()),
    }
}

fn main() -> ExitCode {
    let bin = match std::env::var("MIRROR_BIN") {
        Ok(b) if !b.is_empty() => b,
        _ => {
            eprintln!("MIRROR_BIN not set; skipping MBT (set it to the ModelMirrors binary)");
            return ExitCode::SUCCESS;
        }
    };
    let skip_gen = std::env::args().any(|a| a == "--skip-gen");
    let spec_abs = spec_path();
    let traces_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("traces");

    if !skip_gen {
        // Trace generation runs Apalache directly. The ModelMirrors gen
        // protocol inlines all traces in one response line, which overflows
        // MirrorRust's 64 KiB protocol-line cap at >3 Cooperators-sized
        // traces; direct CLI generation has no such limit.
        let invariants = [
            ("TraceComplete", 12),
            ("NoAck", 2),
            ("NoCrash", 2),
            ("NoRelease", 2),
            ("NoExpire", 2),
            ("NoComplete", 2),
            ("NoDead", 2),
            ("NoFail", 2),
        ];
        let out_root = spec_abs.parent().unwrap().join("_apalache-out");
        let _ = std::fs::create_dir_all(&traces_dir);
        for (inv, num) in invariants {
            let marker = std::time::SystemTime::now();
            let status = std::process::Command::new("apalache-mc")
                .args([
                    "check",
                    &format!("--inv={inv}"),
                    "--length=12",
                    &format!("--max-error={num}"),
                    "--cinit=CInit",
                    "--view=View",
                ])
                .arg(&spec_abs)
                .current_dir(spec_abs.parent().unwrap())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .expect("spawn apalache-mc");
            // Apalache exits 12 when a counterexample (= our witness) exists.
            if status.code() != Some(12) {
                panic!("apalache found no witness for {inv} (exit {status})");
            }
            // Collect the violation*.itf.json of the run directory created
            // after our start marker.
            let spec_out = out_root.join("MBCooperators.tla");
            let mut runs: Vec<PathBuf> = std::fs::read_dir(&spec_out)
                .expect("apalache out dir")
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.is_dir()
                        && p.metadata()
                            .and_then(|m| m.modified())
                            .map(|t| t >= marker)
                            .unwrap_or(false)
                })
                .collect();
            runs.sort();
            let latest = runs.last().unwrap_or_else(|| panic!("no apalache run dir for {inv}"));
            let mut found = 0usize;
            for entry in std::fs::read_dir(latest).expect("run dir").filter_map(|e| e.ok()) {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with("violation") && name.ends_with(".itf.json") {
                    let dest = traces_dir.join(format!("{inv}__{name}"));
                    std::fs::copy(entry.path(), &dest).expect("copy trace");
                    found += 1;
                }
            }
            eprintln!("gen {inv}: {found} traces");
        }
    }

    let mut traces: Vec<PathBuf> = std::fs::read_dir(&traces_dir)
        .expect("traces dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().is_some_and(|x| x == "json"))
        .collect();
    traces.sort();
    if traces.is_empty() {
        eprintln!("no traces in {}", traces_dir.display());
        return ExitCode::FAILURE;
    }

    let mut failures = 0;
    for trace in &traces {
        let sut = CoopSut::new();
        let path = trace.to_string_lossy().into_owned();
        match run_client_with_traces(&bin, apalache_cfg(), vec![path.clone()], sut) {
            Ok(()) => eprintln!("PASS {path}"),
            Err(e) => {
                failures += 1;
                eprintln!("FAIL {path}: {e}");
            }
        }
    }
    println!("mbt: {} traces, {} failed", traces.len(), failures);
    if failures > 0 { ExitCode::FAILURE } else { ExitCode::SUCCESS }
}
