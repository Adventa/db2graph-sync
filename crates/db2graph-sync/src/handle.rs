//! The [`SyncHandle`] returned by [`crate::start`].
//!
//! # What startup success means
//!
//! `start(config).await` returning `Ok(handle)` means:
//!
//! * the configuration validated for its profile,
//! * the cryptographic provider passed its self-tests (and, for the restricted
//!   profile, reports a validated module),
//! * secrets resolved and the encrypted state store opened and is
//!   exclusively owned by this engine,
//! * adapters were constructed and the supervisor task is running on the
//!   caller's Tokio runtime.
//!
//! It does **not** mean the source or graph is reachable, that the snapshot
//! completed, or that replication is live. Use [`SyncHandle::ready`] with a
//! deadline for that, and [`SyncHandle::failed`] (or [`SyncHandle::status`])
//! to observe terminal failures.
//!
//! # Dropping the handle
//!
//! Dropping the handle requests cancellation but does **not** wait for
//! in-flight work, flushing, or checkpointing. Recovery on the next start is
//! safe (work is journaled and receipts make replay idempotent), but call
//! [`SyncHandle::shutdown`] and await it for an orderly stop.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::capability::DeliveryGuarantee;
use crate::config::Profile;
use crate::crypto::Validation;
use crate::error::{Error, ErrorKind, Result};
use crate::retrieval::RetrievalValidator;
use crate::source::SnapshotProgress;
use crate::vector::VectorStats;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Starting,
    Preflight,
    Recovering,
    Snapshotting,
    Validating,
    Streaming,
    Paused,
    Draining,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FailureInfo {
    pub kind: ErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Freshness {
    pub last_applied_unit: Option<String>,
    pub last_applied_version: Option<String>,
    /// Source commit time of the last applied transaction (RFC 3339).
    pub last_commit_time: Option<String>,
    /// Wall-clock time the last batch committed in the graph (RFC 3339).
    pub last_applied_at: Option<String>,
    /// Commit-to-graph lag of the last applied transaction, when the source
    /// reports commit time.
    pub last_lag_ms: Option<i64>,
    /// Last time the source produced any event (including keepalives).
    pub last_source_activity: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Backlog {
    pub journal_units: u64,
    pub vector_tasks: u64,
    pub pending_edges: u64,
    pub audit_spool: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Counters {
    pub units_captured: u64,
    pub units_applied: u64,
    pub duplicates: u64,
    pub changes_applied: u64,
    pub graph_retries: u64,
    pub source_reconnects: u64,
    pub batches: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Status {
    pub pipeline: String,
    pub profile: Profile,
    pub phase: Phase,
    /// Live replication is established and the initial generation is active.
    pub ready: bool,
    pub pause_reason: Option<String>,
    pub failure: Option<FailureInfo>,
    pub delivery_guarantee: DeliveryGuarantee,
    pub crypto_provider: String,
    pub crypto_validation: Validation,
    pub source_id: Option<String>,
    pub acked_position: Option<String>,
    pub fence_token: Option<i64>,
    pub state_epoch: u64,
    pub schema_version: Option<String>,
    pub mapping_digest: Option<String>,
    pub mapping_approved: bool,
    pub snapshot: Option<SnapshotProgress>,
    pub freshness: Freshness,
    pub backlog: Backlog,
    pub counters: Counters,
    pub vector: Option<VectorStats>,
    pub warnings: Vec<String>,
}

/// Final report from an awaited shutdown.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShutdownReport {
    /// Whether all journaled work was applied before stopping.
    pub drained: bool,
    pub remaining_journal_units: u64,
    pub final_status: Status,
}

/// State shared between the handle and engine tasks.
pub(crate) struct Shared {
    pub status: watch::Sender<Status>,
    pub cancel: CancellationToken,
    pub paused: watch::Sender<bool>,
    pub approved_digest: RwLock<Option<String>>,
    pub done: watch::Sender<Option<Result<ShutdownReport, Error>>>,
    pub drain_deadline: RwLock<Option<tokio::time::Instant>>,
}

impl Shared {
    pub fn update(&self, f: impl FnOnce(&mut Status)) {
        self.status.send_modify(f);
    }
    pub fn phase(&self, p: Phase) {
        self.update(|s| {
            s.phase = p;
            s.ready = p == Phase::Streaming;
        });
    }
}

/// Handle to a running engine. See the module documentation.
pub struct SyncHandle {
    pub(crate) shared: Arc<Shared>,
    pub(crate) validator: RetrievalValidator,
    pub(crate) policy: Arc<RwLock<crate::policy::Policy>>,
    pub(crate) profile: Profile,
    pub(crate) finished: bool,
}

impl std::fmt::Debug for SyncHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncHandle").field("pipeline", &self.shared.status.borrow().pipeline).finish()
    }
}

impl SyncHandle {
    /// Current status snapshot.
    pub fn status(&self) -> Status {
        self.shared.status.borrow().clone()
    }

    /// Watch status changes.
    pub fn subscribe(&self) -> watch::Receiver<Status> {
        self.shared.status.subscribe()
    }

    /// Wait until live replication is established and the initial generation
    /// is active, or fail with the terminal error, or with `Timeout`.
    pub async fn ready(&self, timeout: Duration) -> Result<()> {
        let mut rx = self.shared.status.subscribe();
        let wait = async {
            loop {
                {
                    let s = rx.borrow_and_update();
                    if let Some(f) = &s.failure {
                        return Err(Error::new(f.kind, f.message.clone()));
                    }
                    if s.ready {
                        return Ok(());
                    }
                    if s.phase == Phase::Stopped {
                        return Err(Error::new(ErrorKind::Shutdown, "engine stopped before becoming ready"));
                    }
                }
                if rx.changed().await.is_err() {
                    return Err(Error::new(ErrorKind::Shutdown, "engine stopped"));
                }
            }
        };
        match tokio::time::timeout(timeout, wait).await {
            Ok(r) => r,
            Err(_) => Err(Error::new(ErrorKind::Timeout, format!("not ready within {timeout:?} (phase {:?})", self.status().phase))),
        }
    }

    /// Resolves with the terminal error if the engine fails. Never resolves
    /// if the engine stops cleanly (use [`SyncHandle::wait`] for completion).
    pub async fn failed(&self) -> Error {
        let mut rx = self.shared.status.subscribe();
        loop {
            if let Some(f) = rx.borrow_and_update().failure.clone() {
                return Error::new(f.kind, f.message);
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Pause capture and application. Idempotent.
    pub fn pause(&self) {
        self.shared.paused.send_replace(true);
    }

    /// Resume after `pause` or after an operator-resolvable pause. Idempotent.
    pub fn resume(&self) {
        self.shared.paused.send_replace(false);
        self.shared.update(|s| s.pause_reason = None);
    }

    /// Approve a mapping plan digest at runtime (e.g. after a schema change
    /// paused the pipeline) and resume. The digest must match the plan the
    /// engine computed; otherwise the pipeline stays paused.
    pub fn approve_mapping(&self, digest: &str) {
        *self.shared.approved_digest.write().expect("poisoned") = Some(digest.to_owned());
        self.resume();
    }

    /// Replace the data policy. Takes effect immediately at the retrieval
    /// boundary and for newly captured work. Already-projected properties are
    /// not retroactively removed; run reconciliation for that.
    pub fn update_policy(&self, cfg: &crate::policy::PolicyConfig) -> Result<String> {
        let issues = cfg.findings(self.profile);
        if !issues.is_empty() {
            return Err(Error::new(ErrorKind::InvalidRequest, issues.join("; ")));
        }
        let compiled = crate::policy::Policy::compile(cfg, self.profile);
        let version = compiled.version().to_owned();
        *self.policy.write().expect("poisoned") = compiled;
        Ok(version)
    }

    /// Validator for the authorized retrieval boundary.
    pub fn retrieval_validator(&self) -> RetrievalValidator {
        self.validator.clone()
    }

    /// A cloneable control/observation handle (no ownership: dropping it does
    /// nothing). Used by the management router and host health checks.
    pub fn control(&self) -> Control {
        Control { shared: self.shared.clone(), validator: self.validator.clone(), policy: self.policy.clone(), profile: self.profile }
    }

    /// Request an orderly stop: stop capture, apply journaled work until
    /// `drain_timeout`, checkpoint, flush audit, release ownership. Awaits
    /// completion (bounded by `drain_timeout` plus a fixed grace period).
    pub async fn shutdown(mut self, drain_timeout: Duration) -> Result<ShutdownReport> {
        *self.shared.drain_deadline.write().expect("poisoned") = Some(tokio::time::Instant::now() + drain_timeout);
        self.shared.cancel.cancel();
        let r = self.wait_inner(drain_timeout + Duration::from_secs(10)).await;
        self.finished = true;
        r
    }

    /// Wait for the engine to finish (after a failure or shutdown).
    pub async fn wait(mut self) -> Result<ShutdownReport> {
        let r = self.wait_inner(Duration::MAX).await;
        self.finished = true;
        r
    }

    async fn wait_inner(&self, limit: Duration) -> Result<ShutdownReport> {
        let mut rx = self.shared.done.subscribe();
        let fut = async {
            loop {
                if let Some(r) = rx.borrow_and_update().clone() {
                    return r;
                }
                if rx.changed().await.is_err() {
                    return Err(Error::internal("engine task ended without reporting"));
                }
            }
        };
        if limit == Duration::MAX {
            fut.await
        } else {
            tokio::time::timeout(limit, fut)
                .await
                .unwrap_or_else(|_| Err(Error::new(ErrorKind::Timeout, "shutdown did not complete in time")))
        }
    }
}

/// Cloneable control and observation handle. See [`SyncHandle::control`].
#[derive(Clone)]
pub struct Control {
    shared: Arc<Shared>,
    validator: RetrievalValidator,
    policy: Arc<RwLock<crate::policy::Policy>>,
    profile: Profile,
}

impl Control {
    pub fn status(&self) -> Status {
        self.shared.status.borrow().clone()
    }
    pub fn subscribe(&self) -> watch::Receiver<Status> {
        self.shared.status.subscribe()
    }
    pub fn pause(&self) {
        self.shared.paused.send_replace(true);
    }
    pub fn resume(&self) {
        self.shared.paused.send_replace(false);
        self.shared.update(|s| s.pause_reason = None);
    }
    pub fn approve_mapping(&self, digest: &str) {
        *self.shared.approved_digest.write().expect("poisoned") = Some(digest.to_owned());
        self.resume();
    }
    pub fn retrieval_validator(&self) -> RetrievalValidator {
        self.validator.clone()
    }
    pub fn profile(&self) -> Profile {
        self.profile
    }
    pub fn policy_version(&self) -> String {
        self.policy.read().expect("poisoned").version().to_owned()
    }
    /// Request an orderly stop (non-blocking). Await `SyncHandle::wait` for completion.
    pub fn request_shutdown(&self, drain_timeout: Duration) {
        *self.shared.drain_deadline.write().expect("poisoned") = Some(tokio::time::Instant::now() + drain_timeout);
        self.shared.cancel.cancel();
    }
}

impl Drop for SyncHandle {
    fn drop(&mut self) {
        if !self.finished {
            // Best-effort stop; no flush is implied. See module docs.
            self.shared.cancel.cancel();
            tracing::debug!("SyncHandle dropped without awaited shutdown; cancellation requested");
        }
    }
}
