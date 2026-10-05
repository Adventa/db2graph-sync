//! Capability descriptors.
//!
//! Every extension declares its capabilities. The engine reports the
//! *effective* guarantee for a pipeline as the weakest guarantee among its
//! components; implementing a trait never grants a stronger guarantee than
//! the implementation declares, and declarations by third-party components
//! are reported as `qualified: false` until they pass the conformance suite
//! (`db2graph_sync::sink::conformance`) in a qualification run.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    /// No duplicate suppression or ordering guarantee.
    BestEffort,
    /// Every unit is applied at least once; duplicates may be visible.
    AtLeastOnce,
    /// At-least-once transport with effectively-once graph effects: receipts,
    /// guarded versions and fencing make replays have no effect.
    EffectivelyOnceEffects,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingScopeKind {
    /// Total commit order across the whole source.
    TotalCommitOrder,
    /// Order only within a partition (shard, partition key).
    PerPartition,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotConsistency {
    /// The snapshot reflects exactly one source commit point.
    TransactionConsistent,
    /// The snapshot is assembled from reads at different times and converges
    /// via version-guarded overlap reconciliation with the change stream.
    Convergent,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCapabilities {
    pub connector: String,
    /// Whether source transaction boundaries are delivered.
    pub transaction_boundaries: bool,
    pub ordering: OrderingScopeKind,
    pub snapshot: SnapshotConsistency,
    /// Snapshot consistency after a resumed (interrupted) snapshot.
    pub resumed_snapshot: SnapshotConsistency,
    pub before_images: &'static str,
    pub truncate: bool,
    pub schema_change_signal: bool,
    /// Engine versions this connector was qualified against (by tests that
    /// actually ran). Empty means unqualified.
    pub qualified_versions: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSinkCapabilities {
    pub sink: String,
    /// All operations of a batch commit atomically.
    pub atomic_batches: bool,
    /// Receipts are stored in the target, in the same transaction.
    pub receipts_in_target: bool,
    /// Version checks are protected by locking/isolation proven for this target.
    pub guarded_versions: bool,
    /// Writes are rejected when the fence token is stale.
    pub fencing: bool,
    pub tombstones: bool,
    /// Vector work is created atomically with graph effects.
    pub atomic_vector_outbox: bool,
    /// Whether this implementation is part of this project's qualification.
    pub qualified: bool,
    pub notes: Vec<String>,
}

impl GraphSinkCapabilities {
    /// Strongest guarantee these capabilities can support.
    pub fn guarantee(&self) -> DeliveryGuarantee {
        if self.atomic_batches && self.receipts_in_target && self.guarded_versions && self.fencing && self.tombstones {
            DeliveryGuarantee::EffectivelyOnceEffects
        } else {
            DeliveryGuarantee::AtLeastOnce
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// The sink confirmed the write is visible to search.
    Verified,
    /// Accepted, but search visibility cannot be verified by this sink.
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSinkCapabilities {
    pub sink: String,
    /// Visibility reported after a successful upsert.
    pub visibility: Visibility,
    pub delete_by_filter: bool,
    pub qualified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingCapabilities {
    pub provider: String,
    pub model: String,
    /// Versioned model identifier recorded with every vector.
    pub model_version: String,
    pub dimensions: usize,
    /// Whether the provider is authorized to receive protected plaintext.
    pub allows_protected: bool,
    /// Whether text leaves the process (network provider).
    pub remote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PreflightReport {
    pub component: String,
    pub checks: Vec<PreflightCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightCheck {
    pub name: String,
    pub status: CheckStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
    /// The check could not be executed (missing privilege, unsupported version).
    NotExecuted,
}

impl PreflightReport {
    pub fn new(component: impl Into<String>) -> Self {
        Self { component: component.into(), checks: Vec::new() }
    }
    pub fn push(&mut self, name: &str, status: CheckStatus, detail: impl Into<String>) {
        self.checks.push(PreflightCheck { name: name.into(), status, detail: detail.into() });
    }
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|c| c.status != CheckStatus::Fail)
    }
    pub fn failures(&self) -> Vec<&PreflightCheck> {
        self.checks.iter().filter(|c| c.status == CheckStatus::Fail).collect()
    }
}
