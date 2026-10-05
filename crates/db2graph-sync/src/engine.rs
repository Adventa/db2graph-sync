//! The shared engine used by embedded and standalone operation.
//!
//! Task layout (all spawned on the caller's Tokio runtime):
//!
//! ```text
//!   supervisor ── capture loop: source → normalize/policy → journal (fsync) → ack source
//!       │
//!       ├── applier: journal (ordered) → plan → graph batch (atomic, receipts, fence) → checkpoint
//!       ├── vector worker: graph outbox → render/embed → vector sink → complete task
//!       ├── audit forwarder: encrypted spool → audit sinks
//!       └── status poller: backlog gauges
//! ```
//!
//! Any terminal error stops every task, is recorded in status, and is
//! returned from `SyncHandle::wait`/`failed`. Retryable errors are retried
//! with capped exponential backoff and counted.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde_json::json;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::audit::{AuditRecord, AuditSink};
use crate::capability::SnapshotConsistency;
use crate::config::{Config, Profile, SnapshotConfig, SourceConfig};
use crate::error::{Error, ErrorKind, Result};
use crate::handle::{Phase, Shared, ShutdownReport};
use crate::mapping::MappingPlan;
use crate::plan::{GraphTx, PlanContext, plan_transaction};
use crate::policy::{Decision, Policy};
use crate::schema::SourceSchema;
use crate::sink::{ApplyContext, ApplyStatus, GraphSink};
use crate::source::{ChangeStream, SnapshotProgress, SourceConnector, SourceEvent, StartRequest};
use crate::state::{FaultInjector, StateStore, fault};
use crate::types::{CapturedTransaction, TxItem};
use crate::vector::{VectorRuntime, build_points};

pub(crate) async fn blocking<T: Send + 'static>(
    state: &Arc<StateStore>,
    f: impl FnOnce(&StateStore) -> Result<T> + Send + 'static,
) -> Result<T> {
    let s = state.clone();
    tokio::task::spawn_blocking(move || f(&s)).await.map_err(|e| Error::internal(format!("state task failed: {e}")))?
}

/// Audit pipeline: encrypted spool, then forwarding.
pub(crate) struct AuditPipe {
    pub state: Arc<StateStore>,
    pub sinks: Vec<Arc<dyn AuditSink>>,
    pub max: u64,
    pub notify: Notify,
}

impl AuditPipe {
    pub async fn record(&self, category: &str, action: &str, detail: serde_json::Value) -> Result<()> {
        let rec = AuditRecord::new(category, action, detail);
        let max = self.max;
        let spool_only = self.sinks.is_empty();
        blocking(&self.state, move |s| {
            if spool_only && s.audit_spool_len()? >= max {
                // No external destination configured: the spool is a bounded
                // local retention buffer; drop the oldest records.
                let oldest = s.audit_spooled(1)?;
                if let Some((k, _)) = oldest.first() {
                    s.audit_ack(*k)?;
                }
            }
            s.spool_audit(&[rec], max)
        })
        .await?;
        self.notify.notify_one();
        Ok(())
    }

    pub async fn forward_once(&self) -> Result<usize> {
        if self.sinks.is_empty() {
            return Ok(0);
        }
        let batch = blocking(&self.state, |s| s.audit_spooled(500)).await?;
        if batch.is_empty() {
            return Ok(0);
        }
        let records: Vec<AuditRecord> = batch.iter().map(|(_, r)| r.clone()).collect();
        for sink in &self.sinks {
            sink.deliver(&records).await?;
        }
        let upto = batch.last().map(|(k, _)| *k).unwrap_or(0);
        blocking(&self.state, move |s| s.audit_ack(upto)).await?;
        Ok(records.len())
    }
}

pub(crate) struct Engine {
    pub cfg: Arc<Config>,
    pub pipeline: String,
    pub state: Arc<StateStore>,
    pub source: Arc<dyn SourceConnector>,
    pub sink: Arc<dyn GraphSink>,
    pub policy: Arc<RwLock<Policy>>,
    pub vector: Option<VectorRuntime>,
    pub audit: Arc<AuditPipe>,
    pub shared: Arc<Shared>,
    pub faults: Option<FaultInjector>,
    pub apply_notify: Arc<Notify>,
    pub plan: RwLock<Option<Arc<MappingPlan>>>,
    pub schema: RwLock<Option<SourceSchema>>,
    pub fence: AtomicI64,
    /// Highest generation the graph has activated (from the pipeline marker).
    pub graph_generation: AtomicU64,
    pub source_id: RwLock<String>,
    pub vector_stats: tokio::sync::Mutex<crate::vector::VectorStats>,
}

fn check_rollback(graph_epoch: u64, local_epoch: u64) -> Result<()> {
    if graph_epoch > local_epoch {
        return Err(Error::new(
            ErrorKind::StateRollback,
            format!(
                "graph target recorded state epoch {graph_epoch} but local state is at epoch {local_epoch}: the state directory was \
                 restored, replaced or lost. Use the authorized recovery procedure (docs/src/operations/recovery.md)."
            ),
        ));
    }
    Ok(())
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn backoff(attempt: u32, max_ms: u64) -> Duration {
    let ms = 100u64.saturating_mul(1u64 << attempt.min(16)).min(max_ms);
    Duration::from_millis(ms)
}

impl Engine {
    fn snapshot_cfg(&self) -> SnapshotConfig {
        match &self.cfg.source {
            SourceConfig::Postgres(pg) => pg.snapshot.clone(),
            _ => SnapshotConfig::default(),
        }
    }

    fn schema_refresh(&self) -> Duration {
        match &self.cfg.source {
            SourceConfig::Postgres(pg) => Duration::from_secs(pg.schema_refresh_interval_secs),
            _ => Duration::from_secs(300),
        }
    }

    fn current_plan(&self) -> Option<Arc<MappingPlan>> {
        self.plan.read().expect("poisoned").clone()
    }

    fn vector_relations(&self) -> BTreeSet<String> {
        self.vector.as_ref().map(|v| v.templates.keys().cloned().collect()).unwrap_or_default()
    }

    fn ctx(&self, epoch: u64) -> ApplyContext {
        ApplyContext { pipeline: self.pipeline.clone(), fence_token: self.fence.load(Ordering::SeqCst), state_epoch: epoch }
    }

    /// Entry point of the supervisor task.
    ///
    /// Completion is reported only after every reference to the state store
    /// is gone, so "done" implies the state directory is released and a new
    /// engine may take ownership immediately.
    pub async fn run(self: Arc<Self>) {
        let result = self.clone().run_inner().await;
        let outcome = match result {
            Ok(mut report) => {
                let _ = self.audit.record("operation", "stopped", json!({"drained": report.drained})).await;
                let _ = self.audit.forward_once().await;
                self.shared.phase(Phase::Stopped);
                report.final_status = self.shared.status.borrow().clone();
                Ok(report)
            }
            Err(e) => {
                tracing::error!(pipeline = %self.pipeline, kind = %e.kind(), "db2graph-sync engine failed: {}", e.message());
                let _ = self.audit.record("failure", "engine_failed", json!({"kind": e.kind(), "message": e.message()})).await;
                let _ = self.audit.forward_once().await;
                self.shared.update(|s| {
                    s.failure = Some(crate::handle::FailureInfo { kind: e.kind(), message: e.message().to_owned() });
                    s.phase = Phase::Failed;
                    s.ready = false;
                });
                Err(e)
            }
        };
        let shared = self.shared.clone();
        let state = Arc::downgrade(&self.state);
        drop(self);
        // In-flight blocking state operations may still hold the store briefly.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while state.strong_count() > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        shared.done.send_replace(Some(outcome));
    }

    async fn cancellable<T>(&self, fut: impl std::future::Future<Output = T>) -> Option<T> {
        tokio::select! {
            biased;
            _ = self.shared.cancel.cancelled() => None,
            v = fut => Some(v),
        }
    }

    async fn run_inner(self: Arc<Self>) -> Result<ShutdownReport> {
        self.shared.phase(Phase::Preflight);
        // Drain any spool left by a previous run (e.g. a forwarding outage)
        // before adding records; if the destination is still down and the
        // spool is full, the record below fails closed with audit_unavailable.
        while matches!(self.audit.forward_once().await, Ok(n) if n > 0) {}
        self.audit
            .record(
                "operation",
                "start",
                json!({
                    "pipeline": self.pipeline,
                    "profile": self.cfg.profile(),
                    "crypto_provider": self.shared.status.borrow().crypto_provider,
                    "crypto_validation": self.shared.status.borrow().crypto_validation,
                    "delivery_guarantee": self.shared.status.borrow().delivery_guarantee,
                }),
            )
            .await?;

        let Some(report) = self.retrying("source preflight", || self.source.preflight()).await else {
            return self.stopped_early().await;
        };
        let report = report?;
        self.audit.record("operation", "source_preflight", serde_json::to_value(&report).unwrap_or_default()).await?;
        if !report.passed() {
            let f: Vec<String> = report.failures().iter().map(|c| format!("{}: {}", c.name, c.detail)).collect();
            return Err(Error::new(ErrorKind::Preflight, format!("source preflight failed: {}", f.join("; "))));
        }

        let Some(identity) = self.retrying("source identity", || self.source.identity()).await else {
            return self.stopped_early().await;
        };
        let identity = identity?;
        self.check_identity(&identity).await?;

        if !self.refresh_schema().await? {
            return self.stopped_early().await;
        }

        let plan = self.current_plan();
        let Some(greport) = self.retrying("graph preflight", || self.sink.preflight(plan.as_deref())).await else {
            return self.stopped_early().await;
        };
        let greport = greport?;
        self.audit.record("operation", "graph_preflight", serde_json::to_value(&greport).unwrap_or_default()).await?;
        if !greport.passed() {
            let f: Vec<String> = greport.failures().iter().map(|c| format!("{}: {}", c.name, c.detail)).collect();
            return Err(Error::new(ErrorKind::Preflight, format!("graph preflight failed: {}", f.join("; "))));
        }

        // Rollback evidence is checked BEFORE taking ownership, so an instance
        // with stale state can never fence out a healthy owner.
        let Some(observed) = self.retrying("pipeline marker", || self.sink.marker(&self.pipeline)).await else {
            return self.stopped_early().await;
        };
        let observed = observed?.unwrap_or_default();
        let local_epoch = blocking(&self.state, |s| s.epoch()).await?;
        check_rollback(observed.state_epoch, local_epoch)?;
        if let Some(anchor) = self.cfg.state.anchor_file.clone() {
            blocking(&self.state, move |s| s.check_anchor(&anchor)).await?;
        }
        let Some(fenced) = self.retrying("fence acquisition", || self.sink.acquire_fence(&self.pipeline)).await else {
            return self.stopped_early().await;
        };
        let (token, marker) = fenced?;
        // Re-check: another owner may have committed in between.
        check_rollback(marker.state_epoch, local_epoch)?;
        self.fence.store(token, Ordering::SeqCst);
        self.graph_generation.store(marker.active_generation.unwrap_or(0), Ordering::SeqCst);
        self.shared.update(|s| {
            s.fence_token = Some(token);
            s.state_epoch = local_epoch;
        });
        self.audit.record("operation", "fence_acquired", json!({"token": token, "state_epoch": local_epoch})).await?;

        // Background tasks.
        self.shared.phase(Phase::Recovering);
        let stop = CancellationToken::new();
        let drain = CancellationToken::new();
        let mut tasks: JoinSet<Result<()>> = JoinSet::new();
        tasks.spawn(self.clone().applier(drain.clone(), stop.clone()));
        tasks.spawn(self.clone().audit_forwarder(stop.clone()));
        tasks.spawn(self.clone().status_poller(stop.clone()));
        if let Some(v) = self.vector.clone() {
            tasks.spawn(self.clone().vector_worker(v, stop.clone()));
        }

        // Capture runs in this task; a failing background task aborts it.
        let capture = self.clone().capture_loop();
        tokio::pin!(capture);
        let capture_result = loop {
            tokio::select! {
                r = &mut capture => break r,
                Some(joined) = tasks.join_next() => {
                    match joined {
                        Ok(Ok(())) => continue,
                        Ok(Err(e)) => break Err(e),
                        Err(e) => break Err(Error::internal(format!("engine task panicked: {e}"))),
                    }
                }
            }
        };

        if let Err(e) = capture_result {
            stop.cancel();
            while tasks.join_next().await.is_some() {}
            return Err(e);
        }

        // Orderly shutdown: drain journaled work to the graph.
        self.shared.phase(Phase::Draining);
        fault(&self.faults, "shutdown.before_drain")?;
        drain.cancel();
        let mut failure = None;
        // Wait for the applier (the first task to finish after drain) while
        // other tasks keep running, then stop the rest.
        let deadline = self.shared.drain_deadline.read().expect("poisoned").unwrap_or_else(tokio::time::Instant::now);
        let drained_in_time = tokio::time::timeout_at(deadline + Duration::from_millis(50), async {
            loop {
                let cp = blocking(&self.state, |s| s.checkpoint()).await?;
                if cp.applied_seq >= cp.last_seq {
                    return Ok::<_, Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        stop.cancel();
        while let Some(j) = tasks.join_next().await {
            if let Ok(Err(e)) = j {
                failure.get_or_insert(e);
            }
        }
        if let Some(e) = failure {
            return Err(e);
        }
        let cp = blocking(&self.state, |s| s.checkpoint()).await?;
        let remaining = cp.last_seq - cp.applied_seq;
        let _ = self.audit.forward_once().await;
        Ok(ShutdownReport {
            drained: matches!(drained_in_time, Ok(Ok(()))) && remaining == 0,
            remaining_journal_units: remaining,
            final_status: self.shared.status.borrow().clone(),
        })
    }

    /// Run `f` until it succeeds or fails non-retryably. `None` if cancelled.
    async fn retrying<T, F, Fut>(&self, what: &str, mut f: F) -> Option<Result<T>>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0u32;
        loop {
            match self.cancellable(f()).await? {
                Ok(v) => return Some(Ok(v)),
                Err(e) if e.is_retryable() => {
                    attempt += 1;
                    tracing::warn!(pipeline = %self.pipeline, attempt, "{what} failed, retrying: {e}");
                    self.shared.update(|s| {
                        s.warnings.retain(|w| !w.starts_with(what));
                        s.warnings.push(format!("{what}: {e} (attempt {attempt})"));
                    });
                    self.cancellable(tokio::time::sleep(backoff(attempt, self.cfg.engine.retry_max_backoff_ms))).await?;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn stopped_early(&self) -> Result<ShutdownReport> {
        Ok(ShutdownReport { drained: true, remaining_journal_units: 0, final_status: self.shared.status.borrow().clone() })
    }

    async fn check_identity(&self, identity: &crate::source::SourceIdentity) -> Result<()> {
        let stored: Option<crate::source::SourceIdentity> =
            blocking(&self.state, |s| s.get_meta::<Option<_>>("source_identity")).await?.flatten();
        match stored {
            None => {
                let id = identity.clone();
                blocking(&self.state, move |s| s.put_meta("source_identity", &id)).await?;
            }
            Some(prev) if prev.source_id != identity.source_id => {
                return Err(Error::new(
                    ErrorKind::SourceIdentityChanged,
                    format!(
                        "source identity changed from {} to {}; positions are not comparable. Authorized recovery is required.",
                        prev.source_id, identity.source_id
                    ),
                ));
            }
            Some(prev) if prev.history != identity.history => {
                self.audit.record("recovery", "source_history_changed", json!({"from": prev.history, "to": identity.history})).await?;
                self.shared.update(|s| s.warnings.push(format!("source history changed ({} -> {})", prev.history, identity.history)));
                let id = identity.clone();
                blocking(&self.state, move |s| s.put_meta("source_identity", &id)).await?;
            }
            Some(_) => {}
        }
        *self.source_id.write().expect("poisoned") = identity.source_id.clone();
        self.shared.update(|s| s.source_id = Some(identity.source_id.clone()));
        Ok(())
    }

    /// Discover schema, regenerate the mapping plan and enforce approval.
    /// Returns false if cancelled while waiting for approval.
    async fn refresh_schema(&self) -> Result<bool> {
        let schema = self.source.discover_schema().await?;
        let version = schema.version();
        let unchanged = self.schema.read().expect("poisoned").as_ref().map(|s| s.version()) == Some(version.clone());
        if unchanged && self.current_plan().is_some() {
            return Ok(true);
        }
        let previous = blocking(&self.state, |s| s.latest_schema()).await?;
        let changes = previous.as_ref().map(|(_, p)| crate::schema::diff(p, &schema)).unwrap_or_default();
        if previous.as_ref().map(|(_, p)| p.version()) != Some(version.clone()) {
            let next = previous.as_ref().map(|(v, _)| v + 1).unwrap_or(1);
            let s2 = schema.clone();
            blocking(&self.state, move |s| s.append_schema(next, &s2)).await?;
            self.audit.record("sync", "schema_version", json!({"version": version, "changes": changes})).await?;
        }
        let policy = self.policy.read().expect("poisoned").clone();
        let plan = MappingPlan::generate(&schema, &self.cfg.mapping, &policy)?;
        let digest = plan.digest();
        self.audit
            .record(
                "policy",
                "mapping_plan",
                json!({"digest": digest, "policy_version": policy.version(), "excluded": plan.excluded, "relations": plan.relations.keys().collect::<Vec<_>>()}),
            )
            .await?;
        loop {
            let approved = self.cfg.profile() == Profile::Development
                || self.shared.approved_digest.read().expect("poisoned").as_deref() == Some(digest.as_str());
            if approved {
                break;
            }
            let reason = format!("mapping plan {digest} is not approved (schema {version}); approve it to continue");
            self.audit.record("policy", "mapping_awaiting_approval", json!({"digest": digest})).await?;
            self.shared.paused.send_replace(true);
            self.shared.update(|s| {
                s.pause_reason = Some(reason.clone());
                s.mapping_digest = Some(digest.clone());
                s.mapping_approved = false;
            });
            let prev_phase = self.shared.status.borrow().phase;
            self.shared.phase(Phase::Paused);
            if !self.wait_unpaused().await {
                return Ok(false);
            }
            self.shared.phase(prev_phase);
        }
        self.shared.update(|s| {
            s.schema_version = Some(version.clone());
            s.mapping_digest = Some(digest.clone());
            s.mapping_approved = true;
        });
        *self.plan.write().expect("poisoned") = Some(Arc::new(plan));
        *self.schema.write().expect("poisoned") = Some(schema);
        Ok(true)
    }

    /// Wait while paused. Returns false if cancelled.
    async fn wait_unpaused(&self) -> bool {
        let mut rx = self.shared.paused.subscribe();
        loop {
            if !*rx.borrow_and_update() {
                return true;
            }
            tokio::select! {
                _ = self.shared.cancel.cancelled() => return false,
                r = rx.changed() => if r.is_err() { return false },
            }
        }
    }

    async fn honor_pause(&self) -> bool {
        if !*self.shared.paused.borrow() {
            return true;
        }
        let prev = self.shared.status.borrow().phase;
        self.shared.phase(Phase::Paused);
        let _ = self.audit.record("operation", "paused", json!({})).await;
        let ok = self.wait_unpaused().await;
        if ok {
            let _ = self.audit.record("operation", "resumed", json!({})).await;
            self.shared.phase(if prev == Phase::Paused { Phase::Streaming } else { prev });
        }
        ok
    }

    async fn start_request(&self) -> Result<StartRequest> {
        // Values may be explicitly cleared (null) by authorized recovery.
        let resume: Option<crate::types::SourcePosition> =
            blocking(&self.state, |s| s.get_meta::<Option<_>>("ack_position")).await?.flatten();
        let done: Option<u64> = blocking(&self.state, |s| s.get_meta::<Option<u64>>("snapshot_done")).await?.flatten();
        let progress: Option<SnapshotProgress> = blocking(&self.state, |s| s.get_meta::<Option<_>>("snapshot")).await?.flatten();
        let epoch: u32 = blocking(&self.state, |s| s.get_meta::<Option<u32>>("source_epoch")).await?.flatten().unwrap_or(0);
        let snap = self.snapshot_cfg();
        let snapshot = if done.is_none() && snap.enabled {
            // A new snapshot always gets a generation above anything the graph
            // has activated, so its unit ids never collide with old receipts.
            let next = self.graph_generation.load(Ordering::SeqCst) + 1;
            Some(progress.unwrap_or(SnapshotProgress { generation: next, ..Default::default() }))
        } else {
            None
        };
        let plan = self.current_plan().ok_or_else(|| Error::internal("no mapping plan"))?;
        let snapshot_relations = plan.relations.keys().chain(plan.join_tables.keys()).cloned().collect();
        Ok(StartRequest { resume, snapshot, snapshot_relations, snapshot_chunk_rows: snap.chunk_rows, epoch })
    }

    async fn capture_loop(self: Arc<Self>) -> Result<()> {
        let max = self.cfg.engine.retry_max_backoff_ms;
        let mut attempt = 0u32;
        loop {
            if self.shared.cancel.is_cancelled() {
                return Ok(());
            }
            let req = self.start_request().await?;
            let snapshotting = req.snapshot.is_some();
            let resume = req.resume.clone();
            let started = match self.cancellable(self.source.start(req)).await {
                None => return Ok(()),
                Some(r) => r,
            };
            let mut stream = match started {
                Ok(s) => s,
                Err(e) if e.is_retryable() => {
                    attempt += 1;
                    self.note_reconnect(&e, attempt).await;
                    if self.cancellable(tokio::time::sleep(backoff(attempt, max))).await.is_none() {
                        return Ok(());
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            // Re-confirm the durable resume position: a crash between journal
            // commit and the source acknowledgement must not leave the
            // source retaining changes that are already recoverable.
            if let Some(r) = &resume {
                stream.ack(r).await?;
            }
            self.shared.phase(if snapshotting { Phase::Snapshotting } else { Phase::Streaming });
            match self.consume(stream.as_mut(), &mut attempt).await {
                Ok(()) => return Ok(()),
                Err(e) if e.is_retryable() => {
                    attempt += 1;
                    self.note_reconnect(&e, attempt).await;
                    drop(stream);
                    if self.cancellable(tokio::time::sleep(backoff(attempt, max))).await.is_none() {
                        return Ok(());
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn note_reconnect(&self, e: &Error, attempt: u32) {
        tracing::warn!(pipeline = %self.pipeline, attempt, "source error, reconnecting: {e}");
        self.shared.update(|s| {
            s.counters.source_reconnects += 1;
            s.ready = false;
        });
    }

    async fn consume(&self, stream: &mut dyn ChangeStream, attempt: &mut u32) -> Result<()> {
        let refresh_every = self.schema_refresh();
        let mut next_refresh = tokio::time::Instant::now() + refresh_every;
        loop {
            if !self.honor_pause().await {
                return Ok(());
            }
            if tokio::time::Instant::now() >= next_refresh {
                if !self.refresh_schema().await? {
                    return Ok(());
                }
                next_refresh = tokio::time::Instant::now() + refresh_every;
            }
            let ev = tokio::select! {
                biased;
                _ = self.shared.cancel.cancelled() => return Ok(()),
                ev = stream.next() => ev?,
            };
            *attempt = 0;
            self.shared.update(|s| s.freshness.last_source_activity = Some(now_rfc3339()));
            match ev {
                SourceEvent::Unit(tx, progress) => self.capture_unit(*tx, progress, stream).await?,
                SourceEvent::SnapshotComplete(progress) => {
                    if !self.complete_snapshot(progress).await? {
                        return Ok(());
                    }
                }
                SourceEvent::SchemaSignal(rel) => {
                    tracing::info!(pipeline = %self.pipeline, relation = %rel, "schema signal; refreshing");
                    if !self.refresh_schema().await? {
                        return Ok(());
                    }
                    next_refresh = tokio::time::Instant::now() + refresh_every;
                }
                SourceEvent::Keepalive(pos) => {
                    if let Some(p) = pos {
                        stream.ack(&p).await?;
                    }
                    if self.shared.status.borrow().phase == Phase::Recovering {
                        self.shared.phase(Phase::Streaming);
                    }
                }
            }
        }
    }

    /// Strip denied fields (keeping identity/reference columns, which are
    /// needed to compute identities and are never projected unless allowed)
    /// and drop unmapped relations.
    fn normalize(&self, mut tx: CapturedTransaction) -> Result<CapturedTransaction> {
        let plan = self.current_plan().ok_or_else(|| Error::internal("no mapping plan"))?;
        let policy = self.policy.read().expect("poisoned").clone();
        tx.items.retain_mut(|item| match item {
            TxItem::Truncate(t) => {
                t.relations.retain(|r| plan.relations.contains_key(r) || plan.join_tables.contains_key(r));
                !t.relations.is_empty()
            }
            TxItem::Change(c) => {
                let rel = &c.relation;
                let identity_cols: BTreeSet<String> = if let Some(rp) = plan.relations.get(rel) {
                    rp.key_columns
                        .iter()
                        .chain(rp.edges.iter().flat_map(|e| e.columns.iter()))
                        .chain(rp.unique_keys.iter().flat_map(|u| u.columns.iter()))
                        .cloned()
                        .collect()
                } else if let Some(jp) = plan.join_tables.get(rel) {
                    jp.key_columns.iter().chain(jp.from.columns.iter()).chain(jp.to.columns.iter()).cloned().collect()
                } else {
                    return false;
                };
                let tenant_col = policy.dataset(&rel.0).and_then(|d| d.tenant_column.clone());
                let keep = |col: &String| {
                    identity_cols.contains(col) || tenant_col.as_ref() == Some(col) || policy.field(&rel.0, col) == Decision::Allow
                };
                for img in [&mut c.after, &mut c.before, &mut c.old_key].into_iter().flatten() {
                    img.retain(|k, _| keep(k));
                }
                true
            }
        });
        tx.mapping_version = plan.digest();
        tx.policy_version = policy.version().to_owned();
        Ok(tx)
    }

    async fn capture_unit(&self, tx: CapturedTransaction, progress: Option<SnapshotProgress>, stream: &mut dyn ChangeStream) -> Result<()> {
        // Backpressure on journal size.
        let limit = self.cfg.state.max_journal_units;
        loop {
            let len = blocking(&self.state, |s| s.journal_len()).await?;
            if len < limit {
                break;
            }
            if self.cancellable(tokio::time::sleep(Duration::from_millis(50))).await.is_none() {
                return Ok(());
            }
        }
        let tx = self.normalize(tx)?;
        let ack = tx.ack_position.clone();
        let mut updates = vec![("ack_position", serde_json::to_value(&ack).map_err(|e| Error::internal(e.to_string()))?)];
        if let Some(p) = &progress {
            updates.push(("snapshot", serde_json::to_value(p).map_err(|e| Error::internal(e.to_string()))?));
        }
        let snapshot = progress.clone();
        let (_, epoch) = blocking(&self.state, move |s| {
            let refs: Vec<(&str, serde_json::Value)> = updates.iter().map(|(k, v)| (*k, v.clone())).collect();
            s.append_journal(std::slice::from_ref(&tx), &refs)
        })
        .await?;
        self.shared.update(|s| {
            s.counters.units_captured += 1;
            s.acked_position = Some(ack.native.clone());
            s.state_epoch = epoch;
            if snapshot.is_some() {
                s.snapshot = snapshot;
            }
        });
        // Only now is the unit recoverable; acknowledge to the source.
        stream.ack(&ack).await?;
        self.apply_notify.notify_one();
        Ok(())
    }

    /// Drain the snapshot into the graph, validate, and activate the
    /// generation. Returns false if cancelled.
    async fn complete_snapshot(&self, progress: SnapshotProgress) -> Result<bool> {
        self.shared.phase(Phase::Validating);
        loop {
            let cp = blocking(&self.state, |s| s.checkpoint()).await?;
            if cp.applied_seq >= cp.last_seq {
                break;
            }
            self.apply_notify.notify_one();
            if self.cancellable(tokio::time::sleep(Duration::from_millis(20))).await.is_none() {
                return Ok(false);
            }
        }
        let counts = self.sink.counts(&self.pipeline).await?;
        let mut mismatches = Vec::new();
        let plan = self.current_plan().ok_or_else(|| Error::internal("no plan"))?;
        let consistency = progress.consistency.unwrap_or(SnapshotConsistency::Convergent);
        let expected: &BTreeMap<String, u64> = &progress.relation_rows;
        let policy_now = self.policy.read().expect("poisoned").clone();
        let mut not_comparable = Vec::new();
        for (rel, rows) in expected {
            if !plan.relations.contains_key(&crate::types::RelationName(rel.clone())) {
                continue;
            }
            // Rows of other tenants are read but not projected; an exact
            // count comparison is not meaningful for row-level tenancy.
            if policy_now.dataset(rel).is_some_and(|d| d.tenant_column.is_some()) {
                not_comparable.push(rel.clone());
                continue;
            }
            let have = counts.get(rel).copied().unwrap_or(0);
            if have != *rows {
                mismatches.push(json!({"relation": rel, "snapshot_rows": rows, "graph_entities": have}));
            }
        }
        self.audit
            .record(
                "sync",
                "snapshot_validation",
                json!({
                    "generation": progress.generation,
                    "consistency": consistency,
                    "rows": progress.rows,
                    "mismatches": mismatches,
                    "count_not_comparable": not_comparable,
                }),
            )
            .await?;
        if !mismatches.is_empty() {
            if consistency == SnapshotConsistency::TransactionConsistent {
                return Err(Error::new(
                    ErrorKind::ConsistencyIncident,
                    format!("snapshot generation {} failed validation: {}", progress.generation, serde_json::Value::from(mismatches)),
                ));
            }
            self.shared.update(|s| {
                s.warnings.push(format!(
                    "snapshot generation {} is convergent; entity counts will converge as streaming catches up",
                    progress.generation
                ))
            });
        }
        let epoch = blocking(&self.state, |s| s.epoch()).await?;
        self.sink.set_active_generation(&self.ctx(epoch), progress.generation).await?;
        let generation = progress.generation;
        let mut p = progress.clone();
        p.consistency = Some(consistency);
        blocking(&self.state, move |s| {
            s.put_meta("snapshot", &p)?;
            s.put_meta("snapshot_done", &generation)
        })
        .await?;
        self.audit.record("sync", "generation_activated", json!({"generation": generation})).await?;
        self.shared.update(|s| s.snapshot = Some(progress));
        self.shared.phase(Phase::Streaming);
        Ok(true)
    }

    async fn applier(self: Arc<Self>, drain: CancellationToken, stop: CancellationToken) -> Result<()> {
        loop {
            if stop.is_cancelled() {
                return Ok(());
            }
            if !drain.is_cancelled() && *self.shared.paused.borrow() {
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    _ = drain.cancelled() => {},
                    _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                }
            }
            let applied = self.apply_available(&stop).await?;
            if applied == 0 {
                if drain.is_cancelled() {
                    return Ok(());
                }
                tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    _ = drain.cancelled() => {},
                    _ = self.apply_notify.notified() => {},
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {},
                }
            } else if drain.is_cancelled() {
                let deadline = *self.shared.drain_deadline.read().expect("poisoned");
                if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                    return Ok(());
                }
            }
        }
    }

    async fn apply_available(&self, stop: &CancellationToken) -> Result<usize> {
        let cp = blocking(&self.state, |s| s.checkpoint()).await?;
        if cp.applied_seq >= cp.last_seq {
            return Ok(0);
        }
        let max_tx = self.cfg.engine.max_batch_transactions;
        let entries = blocking(&self.state, move |s| s.journal_after(cp.applied_seq, max_tx)).await?;
        if entries.is_empty() {
            return Ok(0);
        }
        let Some(plan) = self.current_plan() else { return Ok(0) };
        let policy = self.policy.read().expect("poisoned").clone();
        let source_id = self.source_id.read().expect("poisoned").clone();
        let vector_relations = self.vector_relations();
        let ctx = PlanContext {
            tenant: &self.cfg.pipeline.tenant,
            source_id: &source_id,
            mapping: &plan,
            policy: &policy,
            vector_relations: &vector_relations,
        };
        let mut batch: Vec<GraphTx> = Vec::new();
        let mut last_seq = cp.applied_seq;
        let mut changes = 0usize;
        let mut last_commit_time = None;
        for (seq, unit) in &entries {
            let n = unit.items.len();
            if !batch.is_empty() && changes + n > self.cfg.engine.max_batch_changes {
                break;
            }
            let gtx = match plan_transaction(unit, &ctx) {
                Ok(g) => g,
                Err(e) if matches!(e.kind(), ErrorKind::SchemaIncompatible | ErrorKind::Unsupported) => {
                    if !batch.is_empty() {
                        break; // apply what precedes the problem first
                    }
                    // Cannot skip or reorder: pause until an operator acts.
                    let reason = format!("unit {} cannot be applied: {}", unit.unit_id, e.message());
                    self.audit.record("failure", "apply_paused", json!({"unit": unit.unit_id, "reason": reason})).await?;
                    self.shared.paused.send_replace(true);
                    self.shared.update(|s| s.pause_reason = Some(reason));
                    return Ok(0);
                }
                Err(e) => return Err(e),
            };
            changes += n;
            batch.push(gtx);
            last_seq = *seq;
            if let Some(t) = &unit.commit_time {
                last_commit_time = Some(t.clone());
            }
        }
        let count = batch.len();
        let epoch = blocking(&self.state, |s| s.epoch()).await?;
        let ctx = self.ctx(epoch);
        let mut attempt = 0u32;
        let outcomes = loop {
            fault(&self.faults, "graph.before_commit")?;
            match self.sink.apply(&ctx, &batch).await {
                Ok(o) => break o,
                Err(e) if e.is_retryable() => {
                    attempt += 1;
                    self.shared.update(|s| s.counters.graph_retries += 1);
                    tracing::warn!(pipeline = %self.pipeline, attempt, "graph apply failed, retrying: {e}");
                    tokio::select! {
                        _ = stop.cancelled() => return Ok(0),
                        _ = tokio::time::sleep(backoff(attempt, self.cfg.engine.retry_max_backoff_ms)) => {}
                    }
                }
                Err(e) => {
                    if e.kind() == ErrorKind::ConsistencyIncident {
                        let _ = self.audit.record("failure", "consistency_incident", json!({"message": e.message()})).await;
                    }
                    return Err(e);
                }
            }
        };
        fault(&self.faults, "graph.after_commit")?;
        let new_epoch = blocking(&self.state, move |s| s.advance_checkpoint(last_seq)).await?;
        if let Some(anchor) = self.cfg.state.anchor_file.clone() {
            blocking(&self.state, move |s| s.write_anchor(&anchor, new_epoch)).await?;
        }
        let duplicates = outcomes.iter().filter(|o| o.status == ApplyStatus::Duplicate).count() as u64;
        let last = batch.last().map(|g| (g.unit_id.clone(), g.version.clone()));
        let now = chrono::Utc::now();
        let lag = last_commit_time
            .as_deref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|t| (now - t.with_timezone(&chrono::Utc)).num_milliseconds());
        self.shared.update(|s| {
            s.counters.units_applied += count as u64;
            s.counters.duplicates += duplicates;
            s.counters.changes_applied += changes as u64;
            s.counters.batches += 1;
            s.state_epoch = new_epoch;
            if let Some((u, v)) = &last {
                s.freshness.last_applied_unit = Some(u.clone());
                s.freshness.last_applied_version = Some(v.clone());
            }
            if last_commit_time.is_some() {
                s.freshness.last_commit_time = last_commit_time.clone();
                s.freshness.last_lag_ms = lag;
            }
            s.freshness.last_applied_at = Some(now_rfc3339());
            // Readiness is owned by the capture loop and snapshot activation;
            // applying a unit (possibly a snapshot chunk) never implies it.
        });
        self.audit
            .record(
                "sync",
                "batch_applied",
                json!({
                    "units": batch.iter().map(|g| json!({"unit": g.unit_id, "expected_digest": g.content_digest})).collect::<Vec<_>>(),
                    "duplicates": duplicates,
                    "fence": ctx.fence_token,
                    "state_epoch": new_epoch,
                    "digest_kind": "expected",
                }),
            )
            .await?;
        Ok(count)
    }

    async fn audit_forwarder(self: Arc<Self>, stop: CancellationToken) -> Result<()> {
        let mut failures = 0u32;
        loop {
            match self.audit.forward_once().await {
                Ok(n) if n > 0 => {
                    failures = 0;
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    failures += 1;
                    tracing::warn!(pipeline = %self.pipeline, "audit forwarding failed (records remain spooled): {e}");
                }
            }
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = self.audit.notify.notified() => {},
                _ = tokio::time::sleep(backoff(failures, 10_000).max(Duration::from_millis(500))) => {},
            }
        }
    }

    async fn status_poller(self: Arc<Self>, stop: CancellationToken) -> Result<()> {
        loop {
            let journal = blocking(&self.state, |s| s.journal_len()).await.unwrap_or(0);
            let spool = blocking(&self.state, |s| s.audit_spool_len()).await.unwrap_or(0);
            let vector = if self.vector.is_some() { self.sink.vector_backlog(&self.pipeline).await.ok() } else { Some(0) };
            let pending = self.sink.pending_edges(&self.pipeline).await.ok();
            let vstats = if self.vector.is_some() { Some(self.vector_stats.lock().await.clone()) } else { None };
            self.shared.update(|s| {
                s.backlog.journal_units = journal;
                s.backlog.audit_spool = spool;
                if let Some(v) = vector {
                    s.backlog.vector_tasks = v;
                }
                if let Some(p) = pending {
                    s.backlog.pending_edges = p;
                }
                s.vector = vstats.map(|mut v| {
                    v.backlog = vector.unwrap_or(0);
                    v
                });
            });
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    async fn vector_worker(self: Arc<Self>, rt: VectorRuntime, stop: CancellationToken) -> Result<()> {
        let min_interval = Duration::from_millis(60_000 / u64::from(rt.max_requests_per_minute.max(1)));
        let mut last_call = tokio::time::Instant::now() - min_interval;
        let mut failures = 0u32;
        loop {
            if stop.is_cancelled() {
                return Ok(());
            }
            if *self.shared.paused.borrow() {
                tokio::select! { _ = stop.cancelled() => return Ok(()), _ = tokio::time::sleep(Duration::from_millis(200)) => continue }
            }
            let tasks = match self.sink.pending_vector_tasks(&self.pipeline, rt.batch_size).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(pipeline = %self.pipeline, "vector outbox read failed: {e}");
                    Vec::new()
                }
            };
            if tasks.is_empty() {
                tokio::select! { _ = stop.cancelled() => return Ok(()), _ = tokio::time::sleep(Duration::from_millis(300)) => {} }
                continue;
            }
            let mut progressed = false;
            for t in tasks {
                if stop.is_cancelled() {
                    return Ok(());
                }
                let wait = (last_call + min_interval).saturating_duration_since(tokio::time::Instant::now());
                if !wait.is_zero() {
                    tokio::select! { _ = stop.cancelled() => return Ok(()), _ = tokio::time::sleep(wait) => {} }
                }
                last_call = tokio::time::Instant::now();
                match self.process_vector_task(&rt, &t).await {
                    Ok(p) => {
                        failures = 0;
                        progressed |= p;
                    }
                    Err(e) if e.kind() == ErrorKind::Security => return Err(e),
                    Err(e) => {
                        failures += 1;
                        self.vector_stats.lock().await.failures += 1;
                        tracing::warn!(pipeline = %self.pipeline, entity = %t.entity_id, "vector task failed (will retry): {e}");
                        tokio::select! {
                            _ = stop.cancelled() => return Ok(()),
                            _ = tokio::time::sleep(backoff(failures, self.cfg.engine.retry_max_backoff_ms)) => {}
                        }
                        break;
                    }
                }
            }
            if !progressed {
                tokio::select! { _ = stop.cancelled() => return Ok(()), _ = tokio::time::sleep(Duration::from_millis(200)) => {} }
            }
        }
    }

    /// Returns whether the task was completed.
    async fn process_vector_task(&self, rt: &VectorRuntime, t: &crate::sink::VectorTask) -> Result<bool> {
        if t.delete {
            rt.sink.delete_entity(&self.pipeline, &t.entity_id, None).await?;
            let done = self.sink.complete_vector_task(&self.pipeline, t).await?;
            let mut st = self.vector_stats.lock().await;
            st.deleted += 1;
            return Ok(done);
        }
        let found = self.sink.lookup(&self.pipeline, std::slice::from_ref(&t.entity_id)).await?;
        let Some(entity) = found.into_iter().find(|e| !e.deleted) else {
            rt.sink.delete_entity(&self.pipeline, &t.entity_id, None).await?;
            return self.sink.complete_vector_task(&self.pipeline, t).await;
        };
        if entity.version != t.version {
            // A newer graph change superseded this task; its own task follows.
            self.vector_stats.lock().await.superseded += 1;
            return Ok(false);
        }
        let Some(template) = rt.templates.get(&entity.relation) else {
            return self.sink.complete_vector_task(&self.pipeline, t).await;
        };
        let policy = self.policy.read().expect("poisoned").clone();
        let caps = rt.embedder.capabilities();
        if caps.allows_protected && self.cfg.profile() == Profile::Restricted {
            return Err(Error::new(ErrorKind::Security, "embedding provider is authorized for protected data in the restricted profile"));
        }
        let (points, _rendered) = build_points(&self.pipeline, &entity, template, &policy, rt.embedder.as_ref()).await?;
        let report = rt.sink.upsert(&points).await?;
        rt.sink.delete_entity(&self.pipeline, &entity.id, Some(&entity.version)).await?;
        fault(&self.faults, "vector.after_delivery")?;
        let done = self.sink.complete_vector_task(&self.pipeline, t).await?;
        let mut st = self.vector_stats.lock().await;
        st.delivered += 1;
        match report.visibility {
            crate::capability::Visibility::Verified => st.verified_visible += 1,
            crate::capability::Visibility::Unknown => st.visibility_unknown += 1,
        }
        if !done {
            st.superseded += 1;
        }
        Ok(done)
    }
}
