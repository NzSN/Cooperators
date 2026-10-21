//! Bounded generated traces against real broker and bridge journals.
//!
//! Enumeration seed 0x0004_0001 fixes the operation alphabet/order. Each group
//! executes every ordered pair of eight operations, surrounded by explicit
//! setup and fair recovery steps. This is finite conformance evidence, not a
//! proof of unbounded liveness or provider/model behavior. Replay one trace
//! with COOP_MODEL_CASE=broker:17 (or bridge:17) and --nocapture.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use cooperators::arrivals::{
    ArrivalKind, ArrivalNotice, ArrivalReference, ObligationCoordinate, SourceCoordinate,
};
use cooperators::docs::DocEvent;
use cooperators::host::*;
use cooperators::rpc::{RpcError, RpcRequest};
use cooperators::server::{self, ConnectionSession, DaemonConfig, DaemonState};
use cooperators::storage::{hash_record, LogFile, GENESIS_HASH};
use serde_json::{json, Value};

const SEED: u64 = 0x0004_0001;
const PAIRS: usize = 64;
const BROKER_OPS: [&str; 8] = [
    "toggle_a",
    "toggle_b",
    "mutate",
    "restart",
    "drain_ack_a",
    "drain_ack_b",
    "reject_future_ack",
    "reject_cross_identity",
];
const BRIDGE_OPS: [&str; 8] = [
    "arrival",
    "restart",
    "rebind",
    "schedule",
    "begin_claim",
    "finish_empty",
    "finish_adopted",
    "resolve",
];

fn selected(group: &str, case: usize) -> bool {
    std::env::var("COOP_MODEL_CASE").map_or(true, |filter| {
        let (selected_group, selected_case) = filter
            .split_once(':')
            .expect("COOP_MODEL_CASE=broker:17 or bridge:17");
        assert!(
            matches!(selected_group, "broker" | "bridge"),
            "unknown model group"
        );
        let selected_case: usize = selected_case.parse().expect("numeric model case");
        assert!(
            selected_case < PAIRS,
            "model case must be less than {PAIRS}"
        );
        selected_group == group && selected_case == case
    })
}

fn trace(group: &str, case: usize, alphabet: &[&str; 8]) -> String {
    let description = format!(
        "{group}:{case} seed={SEED} pair=[{},{}]",
        alphabet[case / 8],
        alphabet[case % 8]
    );
    println!(
        "MODEL_TRACE {}",
        json!({"group":group,"case":case,"seed":SEED,"pair":[alphabet[case/8],alphabet[case%8]]})
    );
    description
}

async fn call(state: &Arc<DaemonState>, method: &str, params: Value) -> Result<Value, RpcError> {
    server::dispatch(
        state,
        &mut ConnectionSession::default(),
        RpcRequest {
            id: Some(json!(1)),
            method: method.into(),
            params,
            is_notification: false,
        },
    )
    .await
    .expect("RPC response")
    .into_result()
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Due {
    version: u64,
    origin: String,
    subscription: String,
}

// A reference model records the recipient set at publication time. It has no
// projector, production cursor parser, hash-chain replay, or routing calls.
struct BrokerModel {
    root: common::TempDir,
    state: Option<Arc<DaemonState>>,
    projecting: bool,
    actors: [Value; 3],
    bus: String,
    active: [Option<String>; 2],
    keys: [usize; 2],
    sources: Vec<[Option<String>; 2]>,
    due: [Vec<Due>; 2],
    acked: [usize; 2],
    context: String,
}

impl BrokerModel {
    async fn new(context: String) -> Self {
        let root = common::TempDir::new("arrival-generated-model");
        let mut config = DaemonConfig::new(root.path());
        config.enable_arrivals = true;
        // Projection is deliberately deferred until an explicit fair step.
        let state = Arc::new(DaemonState::with_config(config).unwrap());
        let mut actors = Vec::new();
        for name in ["model-author", "model-a", "model-b"] {
            actors.push(
                call(&state, "register_agent", json!({"name":name,"kind":"test"}))
                    .await
                    .unwrap(),
            );
        }
        let metadata: Value =
            serde_json::from_slice(&fs::read(root.path().join("notifications/meta.json")).unwrap())
                .unwrap();
        Self {
            root,
            state: Some(state),
            projecting: false,
            actors: actors.try_into().unwrap(),
            bus: metadata["bus_id"].as_str().unwrap().into(),
            active: [None, None],
            keys: [0, 0],
            sources: Vec::new(),
            due: [Vec::new(), Vec::new()],
            acked: [0, 0],
            context,
        }
    }

    fn state(&self) -> &Arc<DaemonState> {
        self.state.as_ref().unwrap()
    }
    fn id(&self, actor: usize) -> &str {
        self.actors[actor + 1]["agent_id"].as_str().unwrap()
    }
    fn cursor(&self, actor: usize, seq: usize) -> String {
        format!("arrival:{}:{}:{seq}", self.bus, self.id(actor))
    }

    async fn reopen(&mut self, project: bool) {
        let state = self.state.take().unwrap();
        state.shutdown_arrivals().await.unwrap();
        drop(state);
        let mut config = DaemonConfig::new(self.root.path());
        config.enable_arrivals = true;
        let state = if project {
            DaemonState::owned(config)
        } else {
            DaemonState::with_config(config)
        };
        self.state = Some(Arc::new(state.unwrap()));
        self.projecting = project;
        let metadata: Value = serde_json::from_slice(
            &fs::read(self.root.path().join("notifications/meta.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            metadata["bus_id"], self.bus,
            "{} stable bus after restart",
            self.context
        );
    }

    async fn toggle(&mut self, actor: usize) {
        if let Some(subscription) = self.active[actor].take() {
            call(
                self.state(),
                "unsubscribe_arrivals",
                json!({"token":self.actors[actor+1]["token"],"subscription_id":subscription}),
            )
            .await
            .unwrap();
        } else {
            let key = format!("model-{actor}-{}", self.keys[actor]);
            self.keys[actor] += 1;
            let receipt = call(
                self.state(),
                "subscribe_arrivals",
                json!({
                    "token":self.actors[actor+1]["token"],"client_key":key,
                    "resource":{"type":"document","name":"model-plan"},
                    "event_kinds":["doc.published","doc.updated"],"catch_up":true
                }),
            )
            .await
            .unwrap();
            let id = receipt["subscription_id"].as_str().unwrap().to_string();
            if !self.sources.is_empty() {
                self.due[actor].push(Due {
                    version: self.sources.len() as u64,
                    origin: "subscription".into(),
                    subscription: id.clone(),
                });
            }
            self.active[actor] = Some(id);
        }
    }

    async fn mutate(&mut self) {
        let version = self.sources.len() as u64 + 1;
        let method = if version == 1 {
            "publish_doc"
        } else {
            "update_doc"
        };
        let mut params = json!({"token":self.actors[0]["token"],"name":"model-plan","text":format!("version-{version}")});
        if version > 1 {
            params["base_version"] = json!(version - 1);
        }
        let response = call(self.state(), method, params).await.unwrap();
        assert_eq!(
            response["version"], version,
            "{} mutation receipt",
            self.context
        );
        self.sources.push(self.active.clone());
        for actor in 0..2 {
            if let Some(subscription) = &self.active[actor] {
                self.due[actor].push(Due {
                    version,
                    origin: "document".into(),
                    subscription: subscription.clone(),
                });
            }
        }
    }

    fn source_invariant(&self) {
        if self.sources.is_empty() {
            return;
        }
        let events =
            LogFile::<DocEvent>::read_segments(self.root.path().join("docs/model-plan.jsonl"))
                .unwrap()
                .records;
        assert_eq!(
            events.len(),
            self.sources.len(),
            "{} source cardinality",
            self.context
        );
        for (index, event) in events.iter().enumerate() {
            let expected: BTreeMap<_, _> = self.sources[index]
                .iter()
                .enumerate()
                .filter_map(|(actor, subscription)| {
                    subscription
                        .as_ref()
                        .map(|id| (self.id(actor).to_string(), vec![id.clone()]))
                })
                .collect();
            let actual: BTreeMap<_, _> =
                event
                    .notification_intent
                    .as_ref()
                    .map_or_else(BTreeMap::new, |intent| {
                        intent
                            .routes
                            .iter()
                            .map(|route| (route.agent_id.clone(), route.subscription_ids.clone()))
                            .collect()
                    });
            assert_eq!(
                actual,
                expected,
                "{} immutable routes for version {}",
                self.context,
                index + 1
            );
            assert_eq!(
                event.version,
                Some(index as u64 + 1),
                "{} source version",
                self.context
            );
        }
    }

    async fn cursor_invariant(&self, actor: usize) {
        let page = call(
            self.state(),
            "wait_arrivals",
            json!({"token":self.actors[actor+1]["token"],"timeout_s":0,"limit":64}),
        )
        .await
        .unwrap();
        assert_eq!(
            page["agent_id"],
            self.id(actor),
            "{} feed identity",
            self.context
        );
        assert_eq!(page["bus_id"], self.bus, "{} feed bus", self.context);
        assert_eq!(
            page["acked_cursor"],
            self.cursor(actor, self.acked[actor]),
            "{} durable ACK cursor",
            self.context
        );
        let ack_path = self
            .root
            .path()
            .join(format!("agents/{}/arrivals-acks.jsonl", self.id(actor)));
        let persisted = fs::read_to_string(ack_path).unwrap_or_default();
        let through = persisted
            .lines()
            .last()
            .map(|line| {
                serde_json::from_str::<Value>(line).unwrap()["through"]
                    .as_u64()
                    .unwrap()
            })
            .unwrap_or(0);
        assert_eq!(
            through, self.acked[actor] as u64,
            "{} persisted ACK head",
            self.context
        );
    }

    async fn drain_ack(&mut self, actor: usize) {
        if !self.projecting {
            self.reopen(true).await;
        }
        let target = self.due[actor].len();
        let mut cursor = self.cursor(actor, self.acked[actor]);
        let mut received = 0;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while self.acked[actor]+received < target {
                let page = call(self.state(),"wait_arrivals",json!({"token":self.actors[actor+1]["token"],"cursor":cursor,"timeout_s":1,"limit":64})).await.unwrap();
                let notices = page["notices"].as_array().unwrap();
                for notice in notices {
                    received += 1;
                    assert_eq!(notice["seq"],(self.acked[actor]+received) as u64,"{} dense feed sequence",self.context);
                    assert_eq!(notice["recipient_agent_id"],self.id(actor),"{} no cross-identity notice",self.context);
                }
                assert!(self.acked[actor]+received <= target,"{} unexpected delivery",self.context);
                cursor = page["next_cursor"].as_str().unwrap().into();
            }
        }).await.unwrap_or_else(|_|panic!("{} fair recovery did not deliver recipient {actor}",self.context));
        let path = self
            .root
            .path()
            .join(format!("agents/{}/arrivals.jsonl", self.id(actor)));
        let notices = LogFile::<ArrivalNotice>::read_segments(path)
            .unwrap()
            .records;
        let mut actual = Vec::new();
        let source_events =
            LogFile::<DocEvent>::read_segments(self.root.path().join("docs/model-plan.jsonl"))
                .unwrap()
                .records;
        let mut deliveries = BTreeSet::new();
        for (index, notice) in notices.iter().enumerate() {
            assert_eq!(
                notice.seq,
                index as u64 + 1,
                "{} persisted dense feed",
                self.context
            );
            assert!(
                deliveries.insert(&notice.delivery_id),
                "{} no duplicate delivery identity",
                self.context
            );
            assert_eq!(
                notice.recipient_agent_id,
                self.id(actor),
                "{} persisted identity isolation",
                self.context
            );
            let version = match notice.resource {
                ArrivalReference::Document { version, .. } => version,
                _ => panic!("unexpected resource"),
            };
            assert_eq!(
                notice.source.hash,
                hash_record(&source_events[version as usize - 1]).unwrap(),
                "{} source anchor",
                self.context
            );
            assert_eq!(
                notice.subscription_ids.len(),
                1,
                "{} captured subscription",
                self.context
            );
            actual.push(Due {
                version,
                origin: notice.obligation.journal_type.clone(),
                subscription: notice.subscription_ids[0].clone(),
            });
        }
        let mut expected = self.due[actor].clone();
        actual.sort();
        expected.sort();
        assert_eq!(
            actual, expected,
            "{} committed intent eventually matches persisted feed for actor {actor}",
            self.context
        );
        call(
            self.state(),
            "ack_arrivals",
            json!({"token":self.actors[actor+1]["token"],"cursor":cursor}),
        )
        .await
        .unwrap();
        self.acked[actor] = target;
        self.cursor_invariant(actor).await;
        let pending = call(
            self.state(),
            "wait_arrivals",
            json!({"token":self.actors[actor+1]["token"],"timeout_s":0}),
        )
        .await
        .unwrap();
        assert!(
            pending["notices"].as_array().unwrap().is_empty(),
            "{} ACK removes pending delivery only",
            self.context
        );
    }

    async fn reject_future(&self) {
        assert!(
            call(
                self.state(),
                "ack_arrivals",
                json!({"token":self.actors[1]["token"],"cursor":self.cursor(0,10_000)})
            )
            .await
            .is_err(),
            "{} future ACK rejected",
            self.context
        );
        self.cursor_invariant(0).await;
    }

    async fn reject_cross_identity(&self) {
        for method in ["ack_arrivals", "wait_arrivals"] {
            assert!(
                call(
                    self.state(),
                    method,
                    json!({"token":self.actors[2]["token"],"cursor":self.cursor(0,0),"timeout_s":0})
                )
                .await
                .is_err(),
                "{} cross-identity {method} rejected",
                self.context
            );
        }
        if let Some(subscription) = &self.active[1] {
            assert!(
                call(
                    self.state(),
                    "unsubscribe_arrivals",
                    json!({"token":self.actors[1]["token"],"subscription_id":subscription})
                )
                .await
                .is_err(),
                "{} cross-identity unsubscribe rejected",
                self.context
            );
        }
        self.cursor_invariant(0).await;
        self.cursor_invariant(1).await;
    }
}

#[tokio::test]
async fn generated_broker_pairs_preserve_intent_feeds_and_ack_boundaries() {
    let mut executed = 0;
    for case in 0..PAIRS {
        if !selected("broker", case) {
            continue;
        }
        executed += 1;
        let mut model = BrokerModel::new(trace("broker", case, &BROKER_OPS)).await;
        model.toggle(0).await;
        model.toggle(1).await;
        model.mutate().await;
        for (step, operation) in [case / 8, case % 8].into_iter().enumerate() {
            model
                .context
                .push_str(&format!(" step{step}={}", BROKER_OPS[operation]));
            match operation {
                0 => model.toggle(0).await,
                1 => model.toggle(1).await,
                2 => model.mutate().await,
                3 => model.reopen(false).await,
                4 => model.drain_ack(0).await,
                5 => model.drain_ack(1).await,
                6 => model.reject_future().await,
                7 => model.reject_cross_identity().await,
                _ => unreachable!(),
            }
            model.source_invariant();
        }
        // Explicit fair recovery: all captured recipients stay live, recovery
        // runs the real projector, and both identities consume their own feed.
        model.reopen(true).await;
        model.drain_ack(0).await;
        model.drain_ack(1).await;
        model.reject_future().await;
        model.reject_cross_identity().await;
        model.reopen(true).await;
        model.drain_ack(0).await;
        model.drain_ack(1).await;
        model.source_invariant();
        model
            .state
            .take()
            .unwrap()
            .shutdown_arrivals()
            .await
            .unwrap();
    }
    println!("MODEL_COVERAGE group=broker executed={executed} alphabet=8 ordered_pairs=64 explicit_fair_recovery=true");
}

#[derive(Clone)]
struct ModelClaim {
    context: ClaimAttempt,
    boundary: u64,
    epoch: u64,
}

struct ModelWake {
    wake: Wake,
    seqs: BTreeSet<u64>,
    claims: BTreeMap<String, ModelClaim>,
    epoch: u64,
    empty: u64,
}

struct BridgeModel {
    root: common::TempDir,
    bridge: Option<HostBridge>,
    binding: Binding,
    pending: BTreeSet<u64>,
    notices: Vec<ArrivalNotice>,
    active: Option<ModelWake>,
    now: u64,
    claim_sequence: usize,
    context: String,
}

impl BridgeModel {
    fn new(case: usize, context: String) -> Self {
        let root = common::TempDir::new("arrival-generated-bridge");
        fs::create_dir(root.path().join("broker")).unwrap();
        let binding = Binding {
            binding_id: "model-binding".into(),
            generation: 1,
            bus_id: format!("model-bus-{SEED}-{case}"),
            agent_id: "model-agent".into(),
            adapter_kind: "model".into(),
            exact_host_session_id: "model-session".into(),
            workspace: root.path().to_string_lossy().into_owned(),
            capability_status: Readiness::Configured,
        };
        let bridge = Self::open(root.path(), binding.clone());
        Self {
            root,
            bridge: Some(bridge),
            binding,
            pending: BTreeSet::new(),
            notices: Vec::new(),
            active: None,
            now: 1,
            claim_sequence: 0,
            context,
        }
    }
    fn open(root: &Path, binding: Binding) -> HostBridge {
        HostBridge::open(
            &root.join("state"),
            &root.join("broker"),
            binding,
            BridgeLimits {
                coalesce_ms: 0,
                wake_notices: 2,
                observation_timeout_ms: 100_000,
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn bridge(&mut self) -> &mut HostBridge {
        self.bridge.as_mut().unwrap()
    }
    fn check(&mut self) {
        let expected_wake = self
            .active
            .as_ref()
            .map(|active| active.wake.wake_id.clone());
        let expected_claims = self.active.as_ref().map_or(0, |active| active.claims.len());
        let status = self.bridge().status();
        assert_eq!(
            status.pending_notices,
            self.pending.len(),
            "{} pending-set cardinality",
            self.context
        );
        assert_eq!(
            status.received_seq,
            self.notices.len() as u64,
            "{} received head",
            self.context
        );
        assert_eq!(
            status.binding.generation, self.binding.generation,
            "{} binding generation",
            self.context
        );
        assert_eq!(
            status.active_wake_id, expected_wake,
            "{} active wake",
            self.context
        );
        assert_eq!(
            status.in_flight_claims, expected_claims,
            "{} persisted claim contexts",
            self.context
        );
        if !self.notices.is_empty() {
            assert_eq!(
                status.received_cursor,
                Some(format!(
                    "arrival:{}:{}:{}",
                    self.binding.bus_id,
                    self.binding.agent_id,
                    self.notices.len()
                )),
                "{} received cursor",
                self.context
            );
        }
        if let Some(active) = &self.active {
            let actual = self.bridge.as_ref().unwrap().active_wake().unwrap();
            assert_eq!(
                actual, &active.wake,
                "{} immutable wake payload",
                self.context
            );
        }
    }
    fn receive(&mut self) {
        let seq = self.notices.len() as u64 + 1;
        let previous = self.notices.last();
        let notice = ArrivalNotice {
            v: 1,
            seq,
            prev_hash: previous.map_or_else(
                || GENESIS_HASH.into(),
                |notice| hash_record(notice).unwrap(),
            ),
            delivery_id: format!("model-delivery-{seq}"),
            recipient_agent_id: self.binding.agent_id.clone(),
            kind: ArrivalKind::QueueTaskAvailable,
            obligation: ObligationCoordinate {
                id: format!("model-obligation-{seq}"),
                journal_type: "queue".into(),
                journal_name: "model-jobs".into(),
                seq,
                hash: format!("source-{seq}"),
                item_index: 0,
            },
            source: SourceCoordinate {
                stream: "queue".into(),
                name: "model-jobs".into(),
                seq,
                hash: format!("source-{seq}"),
            },
            resource: ArrivalReference::Queue {
                name: "model-jobs".into(),
                cause: "enqueued".into(),
                task_id: Some(format!("task-{seq}")),
                ready_count: None,
            },
            subscription_ids: vec!["model-subscription".into()],
        };
        let binding = self.binding.clone();
        let now = self.now;
        let receipt = self
            .bridge()
            .receive(
                &binding.bus_id,
                &binding.agent_id,
                vec![notice.clone()],
                format!("arrival:{}:{}:{seq}", binding.bus_id, binding.agent_id),
                now,
            )
            .unwrap();
        assert_eq!(
            receipt.added_notices, 1,
            "{} receive appends once",
            self.context
        );
        self.notices.push(notice);
        self.pending.insert(seq);
        self.check();
    }
    fn reopen(&mut self) {
        drop(self.bridge.take());
        self.bridge = Some(Self::open(self.root.path(), self.binding.clone()));
        if let Some(active) = &mut self.active {
            active.empty = 0;
        }
        self.check();
    }
    fn schedule(&mut self) {
        let now = self.now;
        if self.active.is_none() && !self.pending.is_empty() {
            self.bridge().retry_pending(now).unwrap();
            self.bridge()
                .set_session_state(SessionState::Idle, now)
                .unwrap();
            let BridgeAction::Submit(wake) = self
                .bridge()
                .next_action(now)
                .unwrap()
                .expect("fair idle wake")
            else {
                panic!("expected fresh Submit")
            };
            let expected: BTreeSet<_> = self.pending.iter().copied().take(2).collect();
            assert_eq!(
                wake.notices
                    .iter()
                    .map(|notice| notice.seq)
                    .collect::<BTreeSet<_>>(),
                expected,
                "{} bounded oldest pending wake",
                self.context
            );
            assert_eq!(
                wake.generation, self.binding.generation,
                "{} exact wake generation",
                self.context
            );
            self.bridge()
                .confirm_activation(
                    ConfirmActivationRequest {
                        binding_id: wake.binding_id.clone(),
                        generation: wake.generation,
                        nonce: wake.observation_nonce.clone(),
                        wake_id: Some(wake.wake_id.clone()),
                    },
                    now,
                )
                .unwrap();
            self.active = Some(ModelWake {
                wake,
                seqs: expected,
                claims: BTreeMap::new(),
                epoch: 0,
                empty: 0,
            });
        } else if self.active.is_none() {
            assert!(
                self.bridge().next_action(now).unwrap().is_none(),
                "{} no wake without pending input",
                self.context
            );
        }
        self.check();
    }
    fn begin(&mut self) {
        let Some(active) = &self.active else {
            return;
        };
        let id = active.wake.wake_id.clone();
        let generation = self.binding.generation;
        let key = format!("claim-{}", self.claim_sequence);
        self.claim_sequence += 1;
        let context = self
            .bridge()
            .begin_claim_with_key(&id, generation, "model-jobs", &key)
            .unwrap();
        let active = self.active.as_mut().unwrap();
        assert_eq!(
            context.queue_generation,
            self.notices.len() as u64,
            "{} captured claim boundary",
            self.context
        );
        assert_eq!(
            context.adoption_epoch, active.epoch,
            "{} captured adoption epoch",
            self.context
        );
        active.claims.insert(
            key,
            ModelClaim {
                context,
                boundary: self.notices.len() as u64,
                epoch: active.epoch,
            },
        );
        self.check();
    }
    fn finish(&mut self, result: ClaimResult) {
        let Some(claim) = self
            .active
            .as_ref()
            .and_then(|active| {
                if matches!(result, ClaimResult::Adopted { .. }) {
                    active.claims.values().next_back()
                } else {
                    active.claims.values().next()
                }
            })
            .cloned()
        else {
            return;
        };
        self.bridge()
            .finish_claim_context(&claim.context, result.clone())
            .unwrap();
        let active = self.active.as_mut().unwrap();
        active.claims.remove(&claim.context.attempt_id);
        match result {
            ClaimResult::Empty if claim.epoch == active.epoch => active.empty = claim.boundary,
            ClaimResult::Empty => {}
            ClaimResult::Adopted { .. } | ClaimResult::Unproven => {
                active.epoch += 1;
                active.empty = 0;
            }
        }
        self.check();
    }
    fn resolution(&self) -> Option<ResolveArrivalsRequest> {
        self.active.as_ref().map(|active| ResolveArrivalsRequest {
            wake_id: active.wake.wake_id.clone(),
            generation: active.wake.generation,
            decisions: active
                .wake
                .notices
                .iter()
                .map(|notice| ResolutionDecision {
                    delivery_id: notice.delivery_id.clone(),
                    disposition: Disposition::QueueDrained,
                })
                .collect(),
            continuation: false,
        })
    }
    fn resolve(&mut self) -> Option<ResolveArrivalsRequest> {
        let request = self.resolution()?;
        let active = self.active.as_ref().unwrap();
        let allowed =
            active.claims.is_empty() && active.seqs.iter().all(|seq| *seq <= active.empty);
        let seqs = active.seqs.clone();
        let result = self.bridge().resolve(request.clone());
        assert_eq!(
            result.is_ok(),
            allowed,
            "{} resolution requires completed fresh empty evidence",
            self.context
        );
        if let Ok(receipt) = result {
            for seq in seqs {
                self.pending.remove(&seq);
            }
            assert_eq!(
                receipt.pending_notices,
                self.pending.len(),
                "{} resolution leaves newer arrivals",
                self.context
            );
            assert_eq!(
                receipt.continuation,
                !self.pending.is_empty(),
                "{} continuation follows unresolved notices",
                self.context
            );
            self.active = None;
        }
        self.check();
        Some(request)
    }
    fn rebind(&mut self) {
        let blocked = self
            .active
            .as_ref()
            .is_some_and(|active| !active.claims.is_empty());
        let mut next = self.binding.clone();
        next.generation += 1;
        let result = self.bridge().rebind(next.clone(), true);
        assert_eq!(
            result.is_err(),
            blocked,
            "{} rebind cannot lose outstanding broker claims",
            self.context
        );
        if result.is_ok() {
            self.binding = next;
            self.active = None;
        }
        self.check();
    }
}

#[test]
fn generated_bridge_pairs_preserve_generations_claims_and_new_arrivals() {
    let mut executed = 0;
    for case in 0..PAIRS {
        if !selected("bridge", case) {
            continue;
        }
        executed += 1;
        let mut model = BridgeModel::new(case, trace("bridge", case, &BRIDGE_OPS));
        model.receive();
        model.schedule();
        model.begin();
        for (step, operation) in [case / 8, case % 8].into_iter().enumerate() {
            model.now += 1;
            model
                .context
                .push_str(&format!(" step{step}={}", BRIDGE_OPS[operation]));
            match operation {
                0 => model.receive(),
                1 => model.reopen(),
                2 => model.rebind(),
                3 => model.schedule(),
                4 => model.begin(),
                5 => model.finish(ClaimResult::Empty),
                6 => model.finish(ClaimResult::Adopted {
                    task_id: "model-adoption".into(),
                }),
                7 => {
                    model.resolve();
                }
                _ => unreachable!(),
            }
            model.check();
        }
        // Fair suffix closes every retained claim, then places a new notice
        // after an empty-claim invocation but before its reply. The old wake's
        // proof can resolve its original batch while newer input remains due.
        while model
            .active
            .as_ref()
            .is_some_and(|active| !active.claims.is_empty())
        {
            model.finish(ClaimResult::Unproven);
        }
        if model.pending.is_empty() {
            model.receive();
        }
        model.schedule();
        // A late empty result that began before another claim adopted work
        // cannot prove the queue drained. Exercise both contexts against the
        // real journal before permitting a new, fresh empty observation.
        model.begin();
        model.begin();
        model.finish(ClaimResult::Adopted {
            task_id: "suffix-adoption".into(),
        });
        model.finish(ClaimResult::Empty);
        let before = model.pending.clone();
        model.resolve();
        assert_eq!(
            model.pending, before,
            "{} earlier empty cannot cover a later adoption",
            model.context
        );
        assert!(
            model.active.is_some(),
            "{} stale empty did not resolve the wake",
            model.context
        );
        model.begin();
        let old_context = model
            .active
            .as_ref()
            .unwrap()
            .claims
            .values()
            .next()
            .unwrap()
            .context
            .clone();
        model.receive();
        model.finish(ClaimResult::Empty);
        let old_resolution = model.resolve().unwrap();
        assert!(
            model.active.is_none(),
            "{} valid old batch resolves",
            model.context
        );
        assert!(
            !model.pending.is_empty(),
            "{} arrival after claim remains pending",
            model.context
        );
        model.rebind();
        let before = model.pending.clone();
        assert!(
            model.bridge().resolve(old_resolution).is_err(),
            "{} old resolution cannot clear new generation",
            model.context
        );
        assert!(
            model
                .bridge()
                .finish_claim_context(&old_context, ClaimResult::Empty)
                .is_err(),
            "{} old claim context fenced",
            model.context
        );
        assert_eq!(model.pending, before);
        model.check();
        model.reopen();
        while !model.pending.is_empty() {
            model.now += 1;
            model.schedule();
            model.begin();
            model.finish(ClaimResult::Empty);
            model.resolve();
        }
        model.reopen();
        model.check();
        assert_eq!(
            model.bridge().status().in_flight_claims,
            0,
            "{} all claims reconciled",
            model.context
        );
    }
    println!("MODEL_COVERAGE group=bridge executed={executed} alphabet=8 ordered_pairs=64 explicit_fair_recovery=true");
}
