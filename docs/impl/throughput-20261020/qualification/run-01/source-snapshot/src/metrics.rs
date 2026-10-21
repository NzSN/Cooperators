//! Prometheus-style metrics for the daemon (design §12).
//!
//! `/metrics` is rendered on demand from replay-derived state. Counters that
//! belong to a log are derived from the log's durable sequence head, so a
//! daemon restart resumes the same values instead of resetting to zero. HTTP
//! request counters are process-local and reset on restart.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::id;
use crate::queues::TaskPhase;
use crate::server::DaemonState;
use crate::VERSION;

/// Process-local counters shared by the HTTP surface.
#[derive(Debug, Default)]
pub struct Metrics {
    http_requests: Mutex<HashMap<(String, u16), u64>>,
    size_rejections: AtomicU64,
}

impl Metrics {
    /// Records one completed HTTP response.
    pub(crate) fn record_http(&self, path: &str, status: u16) {
        if let Ok(mut counters) = self.http_requests.lock() {
            *counters.entry((path.to_string(), status)).or_insert(0) += 1;
        }
    }

    /// Records a stdio message rejected by the size cap.
    pub(crate) fn record_size_rejection(&self) {
        self.size_rejections.fetch_add(1, Ordering::Relaxed);
    }

    fn http_snapshot(&self) -> Vec<((String, u16), u64)> {
        let mut rows: Vec<_> = self
            .http_requests
            .lock()
            .map(|counters| {
                counters
                    .iter()
                    .map(|(key, value)| (key.clone(), *value))
                    .collect()
            })
            .unwrap_or_default();
        rows.sort();
        rows
    }
}

/// Renders the current metrics snapshot in Prometheus text exposition format.
pub async fn render(state: &Arc<DaemonState>) -> String {
    let now_ms = id::unix_millis();
    let stale_after_ms = u64::try_from(state.config.stale_after.as_millis()).unwrap_or(u64::MAX);
    let gone_after_ms = u64::try_from(state.config.gone_after.as_millis()).unwrap_or(u64::MAX);

    let (registered, names) = {
        let registry = state.registry.read().await;
        let mut names = HashMap::new();
        for (name, record) in registry.records_with_names() {
            names.insert(record.agent_id.clone(), name.to_string());
        }
        (registry.len(), names)
    };

    let presence = {
        let presence = state.presence.lock().await;
        presence.counts(now_ms, stale_after_ms, gone_after_ms)
    };

    let mut inboxes: Vec<(String, usize, u64)> = {
        state
            .inboxes
            .backlog_snapshot()
            .await
            .into_iter()
            .map(|(agent_id, messages, bytes)| {
                let label = names.get(&agent_id).cloned().unwrap_or(agent_id);
                (label, messages, bytes)
            })
            .collect()
    };
    inboxes.sort();

    let mut queues: Vec<(String, usize, usize, usize)> = {
        let states = state.queue_states.lock().await;
        states
            .iter()
            .map(|(queue, runtime)| {
                let mut pending = 0usize;
                let mut leased = 0usize;
                let mut active = 0usize;
                for task in runtime.tasks.values() {
                    match task.phase {
                        TaskPhase::Pending => pending += 1,
                        TaskPhase::Leased => {
                            leased += 1;
                            if task
                                .lease
                                .as_ref()
                                .is_some_and(|lease| lease.expires_at_ms > now_ms)
                            {
                                active += 1;
                            }
                        }
                        TaskPhase::Completed | TaskPhase::Dead => {}
                    }
                }
                (queue.clone(), pending, leased, active)
            })
            .collect()
    };
    queues.sort();

    let active_doc_leases = {
        let states = state.doc_states.lock().await;
        states
            .values()
            .filter(|runtime| {
                runtime
                    .claim
                    .as_ref()
                    .is_some_and(|claim| claim.expires_at_ms > now_ms)
            })
            .count()
    };
    let active_leases: usize = queues
        .iter()
        .map(|(_, _, _, active)| *active)
        .sum::<usize>()
        + active_doc_leases;

    let http_requests = state.metrics.http_snapshot();
    let size_rejections = state.metrics.size_rejections.load(Ordering::Relaxed);

    // Filesystem scanning is blocking; keep it off the async worker.
    let scan_state = Arc::clone(state);
    let heads = tokio::task::spawn_blocking(move || scan_state.status().seq_heads)
        .await
        .unwrap_or_default();

    let mut output = String::new();
    let _ = writeln!(output, "# HELP coop_info Daemon version.");
    let _ = writeln!(output, "# TYPE coop_info gauge");
    let _ = writeln!(output, "coop_info{{version=\"{}\"}} 1", escape(VERSION));
    let _ = writeln!(
        output,
        "# HELP coop_uptime_seconds Seconds since daemon state was constructed."
    );
    let _ = writeln!(output, "# TYPE coop_uptime_seconds gauge");
    let _ = writeln!(output, "coop_uptime_seconds {}", state.uptime_s());
    let _ = writeln!(
        output,
        "# HELP coop_http_listener Whether the streamable HTTP listener is serving."
    );
    let _ = writeln!(output, "# TYPE coop_http_listener gauge");
    let _ = writeln!(
        output,
        "coop_http_listener {}",
        u8::from(state.http_listener())
    );
    let _ = writeln!(
        output,
        "# HELP coop_agents_registered Registered agent names."
    );
    let _ = writeln!(output, "# TYPE coop_agents_registered gauge");
    let _ = writeln!(output, "coop_agents_registered {registered}");
    let _ = writeln!(
        output,
        "# HELP coop_agents Registered agents by effective presence state."
    );
    let _ = writeln!(output, "# TYPE coop_agents gauge");
    let _ = writeln!(
        output,
        "coop_agents{{state=\"active\"}} {}",
        presence.active
    );
    let _ = writeln!(output, "coop_agents{{state=\"stale\"}} {}", presence.stale);
    let _ = writeln!(output, "coop_agents{{state=\"gone\"}} {}", presence.gone);
    let _ = writeln!(
        output,
        "# HELP coop_agents_stale Agents currently stale (alias for coop_agents state=stale)."
    );
    let _ = writeln!(output, "# TYPE coop_agents_stale gauge");
    let _ = writeln!(output, "coop_agents_stale {}", presence.stale);

    let _ = writeln!(
        output,
        "# HELP coop_messages_appended_total Records appended to message logs (inbox and channel)."
    );
    let _ = writeln!(output, "# TYPE coop_messages_appended_total counter");
    for (log, head) in &heads {
        if is_message_log(log) {
            let _ = writeln!(
                output,
                "coop_messages_appended_total{{log=\"{}\"}} {}",
                escape(log),
                head.seq
            );
        }
    }

    let _ = writeln!(
        output,
        "# HELP coop_inbox_pending_messages Unacknowledged messages per inbox."
    );
    let _ = writeln!(output, "# TYPE coop_inbox_pending_messages gauge");
    for (agent, messages, _) in &inboxes {
        let _ = writeln!(
            output,
            "coop_inbox_pending_messages{{agent=\"{}\"}} {messages}",
            escape(agent)
        );
    }
    let _ = writeln!(
        output,
        "# HELP coop_inbox_pending_bytes Unacknowledged serialized bytes per inbox."
    );
    let _ = writeln!(output, "# TYPE coop_inbox_pending_bytes gauge");
    for (agent, _, bytes) in &inboxes {
        let _ = writeln!(
            output,
            "coop_inbox_pending_bytes{{agent=\"{}\"}} {bytes}",
            escape(agent)
        );
    }

    let _ = writeln!(
        output,
        "# HELP coop_queue_pending Tasks waiting for a claim."
    );
    let _ = writeln!(output, "# TYPE coop_queue_pending gauge");
    for (queue, pending, _, _) in &queues {
        let _ = writeln!(
            output,
            "coop_queue_pending{{queue=\"{}\"}} {pending}",
            escape(queue)
        );
    }
    let _ = writeln!(
        output,
        "# HELP coop_queue_depth Tasks waiting for a claim (alias for coop_queue_pending)."
    );
    let _ = writeln!(output, "# TYPE coop_queue_depth gauge");
    for (queue, pending, _, _) in &queues {
        let _ = writeln!(
            output,
            "coop_queue_depth{{queue=\"{}\"}} {pending}",
            escape(queue)
        );
    }
    let _ = writeln!(output, "# HELP coop_queue_leased Tasks holding a lease.");
    let _ = writeln!(output, "# TYPE coop_queue_leased gauge");
    for (queue, _, leased, _) in &queues {
        let _ = writeln!(
            output,
            "coop_queue_leased{{queue=\"{}\"}} {leased}",
            escape(queue)
        );
    }
    let _ = writeln!(
        output,
        "# HELP coop_active_leases Unexpired queue and document leases."
    );
    let _ = writeln!(output, "# TYPE coop_active_leases gauge");
    let _ = writeln!(output, "coop_active_leases {active_leases}");

    let _ = writeln!(
        output,
        "# HELP coop_http_requests_total Completed HTTP responses by path and status."
    );
    let _ = writeln!(output, "# TYPE coop_http_requests_total counter");
    for ((path, status), count) in &http_requests {
        let _ = writeln!(
            output,
            "coop_http_requests_total{{path=\"{}\",status=\"{}\"}} {count}",
            escape(path),
            status
        );
    }
    let _ = writeln!(
        output,
        "# HELP coop_stdio_size_rejections_total Stdio messages rejected by the size cap."
    );
    let _ = writeln!(output, "# TYPE coop_stdio_size_rejections_total counter");
    let _ = writeln!(output, "coop_stdio_size_rejections_total {size_rejections}");
    output
}

fn is_message_log(path: &str) -> bool {
    path.starts_with("channels/") || (path.starts_with("agents/") && path.ends_with("/inbox.jsonl"))
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
