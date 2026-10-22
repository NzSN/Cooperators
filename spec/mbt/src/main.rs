//! Cooperators MBT harness — generated-adapter edition.
//!
//! The SUT adapter is the Mirrors model-interface compiler's `mirrorrust-v2`
//! output (`generated/MBCooperatorsMirror.generated.rs`, semantic digest in
//! `Cooperators.mirror-interface.lock.json`); this file implements the
//! generated typed port (`MBCooperatorsPort`) against the real daemon and
//! replays the protocol and root-ownership batteries through ModelMirrors.
//! Root-ownership traces use real daemon subprocesses and disk observations.
//!
//! Oracle boundary and limits: see README.md.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use cooperators::envelope::Body;
use cooperators::messaging::SendRequest;
use cooperators::registry::AgentIdentity;
use cooperators::server::{DaemonConfig, DaemonState};

use mirrorrust::{run_client_with_traces, ApalacheConfig, BindingError, FallibleStateComputer};
use num_bigint::BigInt;

mod fault_support;
mod faults_identity;
mod faults_rotation;
mod faults_runtime;
mod faults_storage;
mod faults_writer;
mod writers;

#[path = "../generated/MBCooperatorsMirror.generated.rs"]
#[allow(dead_code)]
mod generated;
use generated::{
    AckInput, ClaimInput, CompleteInput, ExpireInput, FailInput, MBCooperatorsObservation,
    MBCooperatorsPort, MiTypeO1Item, MirrorMap, MirrorSeq, PublishInput, ReleaseInput, SendInput,
    UpdateInput,
};

const AGENTS: [&str; 2] = ["a1", "a2"];
const MAX_ATTEMPTS: u64 = 2;
const QUEUE: &str = "q0";
const DOC: &str = "d0";
const LEASE_TTL_S: u64 = 3600; // far future: expiry happens only via the forced hook
const ADAPTER_ID: &str = "cooperators.mbc/v1";
const TARGET_PROFILE: &str = "mirrorrust-v2";

fn to_bigint(v: u64) -> BigInt {
    BigInt::from(v)
}

struct CoopSut {
    rt: tokio::runtime::Runtime,
    _tmp: tempfile::TempDir,
    root: PathBuf,
    state: DaemonState,
    ids: BTreeMap<String, String>,
    tokens: BTreeMap<String, String>,
    log: Vec<MiTypeO1Item>,
    /// (lease_id, concrete fence, abstract fence = position of claim in `log`)
    q_lease: Option<(String, u64, u64)>,
    d_lease: Option<(String, u64, u64)>,
    doc_version: u64,
}

fn log_rec(
    ty: &str,
    from: &str,
    to: &str,
    key: &str,
    res: &str,
    holder: &str,
    fence: u64,
    base: u64,
) -> MiTypeO1Item {
    MiTypeO1Item {
        field_0: to_bigint(base),
        field_1: to_bigint(fence),
        field_2: from.to_string(),
        field_3: holder.to_string(),
        field_4: key.to_string(),
        field_5: res.to_string(),
        field_6: to.to_string(),
        field_7: ty.to_string(),
    }
}

fn parse_seq(s: &str) -> u64 {
    s.strip_prefix("seq:").unwrap_or(s).parse().unwrap_or(0)
}

fn daemon_err(e: impl std::fmt::Debug) -> BindingError {
    BindingError::new("daemon_rejected", format!("{e:?}"))
}

impl CoopSut {
    fn new() -> Self {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("coop-root");
        let state = DaemonState::with_config(DaemonConfig::new(&root)).expect("daemon state");
        CoopSut {
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
        }
    }

    fn identity(&self, name: &str) -> Result<AgentIdentity, BindingError> {
        let token = self
            .tokens
            .get(name)
            .ok_or_else(|| BindingError::new("unknown_agent", name))?;
        self.rt
            .block_on(self.state.authenticate(token))
            .map_err(daemon_err)
    }

    fn acked(&self, name: &str) -> u64 {
        let Some(id) = self.ids.get(name) else {
            return 0;
        };
        self.rt
            .block_on(self.state.wait_message(id, None, Duration::from_millis(0)))
            .map(|b| parse_seq(&b.acked_cursor))
            .unwrap_or(0)
    }
}

impl MBCooperatorsPort for CoopSut {
    fn initialize(&mut self) -> Result<(), BindingError> {
        for name in AGENTS {
            let reg = self
                .rt
                .block_on(self.state.register_agent(
                    name.to_string(),
                    Some("mbt".to_string()),
                    None,
                    None,
                ))
                .map_err(daemon_err)?;
            self.ids.insert(name.to_string(), reg.agent_id);
            self.tokens.insert(name.to_string(), reg.token);
        }
        self.rt
            .block_on(self.state.create_queue(QUEUE))
            .map_err(daemon_err)?;
        self.rt
            .block_on(
                self.state
                    .enqueue(QUEUE, serde_json::json!("task0"), Some(MAX_ATTEMPTS)),
            )
            .map_err(daemon_err)?;
        Ok(())
    }

    fn send(&mut self, input: SendInput) -> Result<(), BindingError> {
        let from = self.identity(&input.from)?;
        let req = SendRequest {
            to: self.ids[&input.to].clone(),
            kind: "mbt".to_string(),
            body: Body {
                mime: "text/plain".to_string(),
                text: "mbt".to_string(),
            },
            reply_to: None,
            idempotency_key: Some(input.key.clone()),
            ttl: None,
        };
        self.rt
            .block_on(self.state.send_message(&from, req))
            .map_err(daemon_err)?;
        self.log.push(log_rec(
            "msg",
            &input.from,
            &input.to,
            &input.key,
            "-",
            "-",
            0,
            0,
        ));
        Ok(())
    }

    fn ack(&mut self, input: AckInput) -> Result<(), BindingError> {
        let id = self.ids[&input.holder].clone();
        let cursor = input.cursor.to_string();
        self.rt
            .block_on(self.state.ack(&id, &cursor))
            .map_err(daemon_err)?;
        Ok(())
    }

    fn claim(&mut self, input: ClaimInput) -> Result<(), BindingError> {
        let who = self.identity(&input.holder)?;
        let abs_fence = (self.log.len() as u64) + 1;
        if input.res == QUEUE {
            let r = self
                .rt
                .block_on(self.state.claim_task(&who, QUEUE, LEASE_TTL_S))
                .map_err(daemon_err)?;
            self.q_lease = Some((r.lease_id, r.fence, abs_fence));
        } else {
            let r = self
                .rt
                .block_on(self.state.claim_doc(&who, DOC, LEASE_TTL_S))
                .map_err(daemon_err)?;
            self.d_lease = Some((r.lease_id, r.fence, abs_fence));
        }
        self.log.push(log_rec(
            "claim",
            "-",
            "-",
            "-",
            &input.res,
            &input.holder,
            0,
            0,
        ));
        Ok(())
    }

    fn expire(&mut self, input: ExpireInput) -> Result<(), BindingError> {
        if input.res == QUEUE {
            let fence = self.q_lease.as_ref().map(|(_, _, af)| *af).unwrap_or(0);
            let n = self
                .rt
                .block_on(self.state.debug_force_expire_queue(QUEUE))
                .map_err(daemon_err)?;
            if n == 0 {
                return Err(BindingError::new(
                    "daemon_rejected",
                    "no queue lease to expire",
                ));
            }
            self.q_lease = None;
            self.log
                .push(log_rec("expire", "-", "-", "-", &input.res, "-", fence, 0));
        } else {
            let fence = self.d_lease.as_ref().map(|(_, _, af)| *af).unwrap_or(0);
            let ok = self
                .rt
                .block_on(self.state.debug_force_expire_doc(DOC))
                .map_err(daemon_err)?;
            if !ok {
                return Err(BindingError::new(
                    "daemon_rejected",
                    "no doc claim to expire",
                ));
            }
            self.d_lease = None;
            self.log
                .push(log_rec("expire", "-", "-", "-", &input.res, "-", fence, 0));
        }
        Ok(())
    }

    fn complete(&mut self, input: CompleteInput) -> Result<(), BindingError> {
        let (lease_id, fence, abs_fence) = self
            .q_lease
            .take()
            .ok_or_else(|| BindingError::new("adapter_state", "no active q lease"))?;
        let who = self.identity(&input.holder)?;
        self.rt
            .block_on(self.state.complete_task(
                &who,
                &lease_id,
                fence,
                serde_json::json!("done"),
                None,
            ))
            .map_err(daemon_err)?;
        self.log.push(log_rec(
            "complete",
            "-",
            "-",
            "-",
            QUEUE,
            &input.holder,
            abs_fence,
            0,
        ));
        Ok(())
    }

    fn fail(&mut self, input: FailInput) -> Result<(), BindingError> {
        let (lease_id, fence, abs_fence) = self
            .q_lease
            .take()
            .ok_or_else(|| BindingError::new("adapter_state", "no active q lease"))?;
        let who = self.identity(&input.holder)?;
        self.rt
            .block_on(self.state.fail_task(&who, &lease_id, fence, "mbt-fail"))
            .map_err(daemon_err)?;
        self.log.push(log_rec(
            "fail",
            "-",
            "-",
            "-",
            QUEUE,
            &input.holder,
            abs_fence,
            0,
        ));
        Ok(())
    }

    fn release(&mut self, input: ReleaseInput) -> Result<(), BindingError> {
        let (lease_id, fence, abs_fence) = self
            .d_lease
            .take()
            .ok_or_else(|| BindingError::new("adapter_state", "no active d lease"))?;
        let who = self.identity(&input.holder)?;
        self.rt
            .block_on(self.state.release_doc(&who, &lease_id, fence))
            .map_err(daemon_err)?;
        self.log.push(log_rec(
            "release",
            "-",
            "-",
            "-",
            DOC,
            &input.holder,
            abs_fence,
            0,
        ));
        Ok(())
    }

    fn publish(&mut self, input: PublishInput) -> Result<(), BindingError> {
        let who = self.identity(&input.holder)?;
        self.rt
            .block_on(self.state.publish_doc(
                &who,
                DOC,
                "text/plain",
                Body {
                    mime: "text/plain".to_string(),
                    text: "mbt doc".to_string(),
                },
                None,
            ))
            .map_err(daemon_err)?;
        self.doc_version = 1;
        self.log
            .push(log_rec("docput", "-", "-", "-", DOC, &input.holder, 0, 0));
        Ok(())
    }

    fn update(&mut self, input: UpdateInput) -> Result<(), BindingError> {
        let who = self.identity(&input.holder)?;
        let base = self.doc_version;
        let abs_fence = self.d_lease.as_ref().map(|(_, _, af)| *af).unwrap_or(0);
        let lease_owned: Option<(String, u64)> =
            self.d_lease.as_ref().map(|(lid, f, _)| (lid.clone(), *f));
        let lease = lease_owned.as_ref().map(|(lid, f)| (lid.as_str(), *f));
        self.rt
            .block_on(self.state.update_doc(
                &who,
                DOC,
                base,
                Body {
                    mime: "text/plain".to_string(),
                    text: "mbt update".to_string(),
                },
                None,
                lease,
            ))
            .map_err(daemon_err)?;
        self.doc_version += 1;
        self.log.push(log_rec(
            "docput",
            "-",
            "-",
            "-",
            DOC,
            &input.holder,
            abs_fence,
            base,
        ));
        Ok(())
    }

    fn crash(&mut self) -> Result<(), BindingError> {
        self.state = DaemonState::with_config(DaemonConfig::new(&self.root)).map_err(daemon_err)?;
        for name in AGENTS {
            let token = self.tokens[name].clone();
            self.rt
                .block_on(self.state.authenticate(&token))
                .map_err(daemon_err)?;
        }
        // Leases survive via log replay: stored (lease_id, fence) stay valid.
        Ok(())
    }

    fn observe(&mut self) -> Result<MBCooperatorsObservation, BindingError> {
        let mut ack = BTreeMap::new();
        for name in AGENTS {
            ack.insert(name.to_string(), to_bigint(self.acked(name)));
        }
        Ok(MBCooperatorsObservation {
            ack_cursor: MirrorMap(ack),
            log: MirrorSeq(self.log.clone()),
        })
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

fn replay_one(bin: &str, path: &Path) -> Result<(), String> {
    // The generated binding is the FallibleStateComputer; wrap it for the
    // infallible StateComputer signature. A daemon rejection or observation
    // codec error poisons the binding and fails the replay immediately.
    let mut binding = generated::bind_m_b_cooperators(CoopSut::new(), &apalache_cfg())
        .map_err(|e| format!("bind: {e}"))?;
    let compute = move |action: &str, params: &mirrorrust::State, prev: &mirrorrust::State| {
        FallibleStateComputer::compute(&mut binding, action, params, prev)
            .unwrap_or_else(|e| panic!("generated binding failed at {action}: {e}"))
    };
    // NOTE: the negotiated client path (run_client_with_traces_negotiated)
    // requires server support for mirrors.model-interface-negotiation/v1,
    // which this ModelMirrors 0.1.1.0 build lacks; replay uses the plain
    // trace path. The semantic digest/lock still binds adapter to spec via
    // `model_interface_gen check`.
    run_client_with_traces(
        bin,
        apalache_cfg(),
        vec![path.to_string_lossy().into_owned()],
        compute,
    )
    .map_err(|e| e.to_string())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args
        .first()
        .is_some_and(|arg| arg == "--fault-rotation-child")
    {
        let result = if args.len() == 3 {
            args[2]
                .parse::<u64>()
                .map_err(|e| e.to_string())
                .and_then(|units| faults_rotation::child(Path::new(&args[1]), units))
        } else {
            Err("rotation child requires ROOT UNITS".into())
        };
        return match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("rotation child failed: {error}");
                ExitCode::FAILURE
            }
        };
    }
    if args
        .first()
        .is_some_and(|arg| arg == "--runtime-fixture-child")
    {
        return if args.len() == 3 {
            faults_runtime::child_main(&args[1], Path::new(&args[2]))
        } else {
            eprintln!("runtime fixture child requires PROFILE ROOT");
            ExitCode::FAILURE
        };
    }
    if args.first().is_some_and(|arg| arg == "--writer-child") {
        return if args.len() == 2 {
            writers::child_main(Path::new(&args[1]))
        } else {
            eprintln!("internal writer child requires a root path");
            ExitCode::FAILURE
        };
    }
    let mut suite = "all";
    let mut skip_gen = false;
    let mut scenario: Option<&str> = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--skip-gen" => skip_gen = true,
            "--suite" if index + 1 < args.len() => {
                index += 1;
                suite = &args[index];
            }
            "--scenario" if index + 1 < args.len() => {
                index += 1;
                scenario = Some(&args[index]);
            }
            "--help" | "-h" => {
                println!("cooperators-mbt [--skip-gen] [--suite all|protocol|writers|faults] [--scenario PROFILE]");
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown or incomplete argument: {other}");
                return ExitCode::FAILURE;
            }
        }
        index += 1;
    }
    if !matches!(suite, "all" | "protocol" | "writers" | "faults") {
        eprintln!("suite must be all, protocol, writers, or faults");
        return ExitCode::FAILURE;
    }
    let bin = match std::env::var("MIRROR_BIN") {
        Ok(b) if !b.is_empty() => b,
        _ => {
            eprintln!("MIRROR_BIN not set; set it to the mirror binary (formerly ModelMirrors)");
            return ExitCode::FAILURE;
        }
    };
    let spec_abs = spec_path();
    let home = Path::new(env!("CARGO_MANIFEST_DIR"));
    let traces_dir = home.join("traces");

    if suite == "faults" {
        return match run_fault_profiles(&bin, home, scenario) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("fault MBT gate failed: {error}");
                ExitCode::FAILURE
            }
        };
    }
    if scenario.is_some() {
        eprintln!("--scenario requires --suite faults");
        return ExitCode::FAILURE;
    }

    if !skip_gen && suite != "writers" {
        // Trace generation runs Apalache directly: the ModelMirrors gen
        // protocol inlines all traces in one response line, which overflows
        // MirrorRust's 64 KiB protocol-line cap beyond ~3 Cooperators-sized
        // traces. Direct CLI generation has no such limit.
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
            if status.code() != Some(12) {
                panic!("apalache found no witness for {inv} (exit {status})");
            }
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
            let latest = runs
                .last()
                .unwrap_or_else(|| panic!("no apalache run dir for {inv}"));
            let mut found = 0usize;
            for entry in std::fs::read_dir(latest)
                .expect("run dir")
                .filter_map(|e| e.ok())
            {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with("violation") && name.ends_with(".itf.json") {
                    std::fs::copy(entry.path(), traces_dir.join(format!("{inv}__{name}")))
                        .expect("copy trace");
                    found += 1;
                }
            }
            eprintln!("gen {inv}: {found} traces");
        }
    }

    if !skip_gen && suite != "protocol" {
        match std::process::Command::new("python3")
            .arg(home.join("generate_writers.py"))
            .status()
        {
            Ok(status) if status.success() => {}
            other => {
                eprintln!("writer trace generation failed: {other:?}");
                return ExitCode::FAILURE;
            }
        }
    }

    let mut traces = Vec::new();
    for (name, dir) in [
        ("protocol", traces_dir),
        ("writers", home.join("writers/traces")),
    ] {
        if suite != "all" && suite != name {
            continue;
        }
        if name == "writers" {
            for required in writers::REQUIRED_TRACES {
                if !dir.join(required).is_file() {
                    eprintln!("missing required writer trace: {required}");
                    return ExitCode::FAILURE;
                }
            }
        }
        let mut paths: Vec<_> = match std::fs::read_dir(&dir) {
            Ok(entries) => entries
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| path.is_file() && path.extension().is_some_and(|x| x == "json"))
                .collect(),
            Err(error) => {
                eprintln!("read {} traces at {}: {error}", name, dir.display());
                return ExitCode::FAILURE;
            }
        };
        paths.sort();
        if paths.is_empty() {
            eprintln!("no {name} traces in {}", dir.display());
            return ExitCode::FAILURE;
        }
        traces.extend(paths.into_iter().map(|path| (name, path)));
    }

    let mut failures = 0;
    for (name, trace) in &traces {
        let result = if *name == "writers" {
            writers::replay(&bin, trace)
        } else {
            replay_one(&bin, trace)
        };
        match result {
            Ok(()) => eprintln!("PASS {}", trace.display()),
            Err(e) => {
                failures += 1;
                eprintln!("FAIL {}: {e}", trace.display());
            }
        }
    }
    eprintln!("mbt: {} traces, {} failed", traces.len(), failures);
    if suite == "all" {
        if let Err(error) = run_fault_profiles(&bin, home, None) {
            eprintln!("fault MBT gate failed: {error}");
            failures += 1;
        }
    }
    if failures > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn run_fault_profiles(bin: &str, home: &Path, selected: Option<&str>) -> Result<(), String> {
    let profiles = [
        "writer",
        "storage",
        "inbox",
        "deadletter",
        "document",
        "status",
        "rotation",
        "identity",
    ];
    if selected.is_some_and(|name| !profiles.contains(&name)) {
        eprintln!("unknown fault profile");
        return Err("unknown fault profile".into());
    }
    let mut check = std::process::Command::new("python3");
    check
        .arg(home.join("generate_fault_interfaces.py"))
        .arg("--check");
    if let Some(compiler) = std::env::var_os("MODEL_INTERFACE_GEN") {
        check.arg("--compiler").arg(compiler);
    }
    if let Some(profile) = selected {
        check.arg("--profile").arg(profile);
    }
    let checked = check.status().map_err(|e| e.to_string())?;
    if !checked.success() {
        return Err("compiler-generated fault interface check failed".into());
    }
    let mut total = 0;
    let mut failed = 0;
    for profile in profiles {
        if selected.is_some_and(|name| name != profile) {
            continue;
        }
        let dir = home.join("fault_interfaces").join(profile).join("traces");
        let mut traces = std::fs::read_dir(&dir)
            .map_err(|e| e.to_string())?
            .map(|entry| entry.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        traces.retain(|path| path.extension().is_some_and(|ext| ext == "json"));
        traces.sort();
        if traces.is_empty() {
            return Err(format!("missing required {profile} traces"));
        }
        for trace in traces {
            total += 1;
            let result = match profile {
                "storage" => faults_storage::replay(bin, &trace),
                "writer" => faults_writer::replay(bin, &trace),
                "identity" => faults_identity::replay(bin, &trace),
                "rotation" => faults_rotation::replay(bin, &trace),
                "inbox" | "deadletter" | "document" | "status" => {
                    faults_runtime::replay(profile, bin, &trace)
                }
                _ => Err(format!("fault profile {profile} not wired")),
            };
            match result {
                Ok(()) => eprintln!("PASS fault-{profile} {}", trace.display()),
                Err(error) => {
                    failed += 1;
                    eprintln!("FAIL fault-{profile} {}: {error}", trace.display());
                }
            }
        }
        if profile == "rotation" {
            match faults_rotation::check_crash_boundaries() {
                Ok(()) => eprintln!("PASS rotation crash boundaries"),
                Err(error) => {
                    failed += 1;
                    eprintln!("FAIL rotation crash boundaries: {error}");
                }
            }
        }
    }
    eprintln!("fault MBT: {total} traces, {failed} failures");
    if failed == 0 {
        Ok(())
    } else {
        Err(format!("{failed} fault traces failed"))
    }
}
