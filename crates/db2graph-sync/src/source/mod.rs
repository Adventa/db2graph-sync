//! Source connector interface.
//!
//! A connector turns native change capture into an ordered stream of
//! [`SourceEvent`]s. Connectors never scan business tables to *detect*
//! changes; table reads are permitted only for the initial snapshot and for
//! metadata discovery.
//!
//! Acknowledgement contract: the engine calls [`ChangeStream::ack`] only
//! after every unit up to that position is durably journaled. A connector
//! must never confirm a position to the source on its own.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::capability::{PreflightReport, SnapshotConsistency, SourceCapabilities};
use crate::error::Result;
use crate::schema::SourceSchema;
pub use crate::types::Image;
use crate::types::{CapturedTransaction, RelationName, SourcePosition};

#[cfg(feature = "postgres")]
pub mod postgres;
pub mod scripted;

/// Identity of the connected source system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentity {
    /// Stable id used in entity identities, e.g. `pg:<system_identifier>`.
    pub source_id: String,
    /// Detail that changes on failover to a different history (timeline etc.).
    pub history: String,
    pub server_version: String,
}

/// Persisted snapshot progress (encrypted in the state store).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SnapshotProgress {
    pub generation: u64,
    /// Boundary position: changes after it are streamed.
    pub boundary: Option<SourcePosition>,
    /// Relations fully loaded.
    pub completed: Vec<RelationName>,
    /// Relation in progress and the last key loaded (keyset pagination).
    pub current: Option<(RelationName, Option<Image>)>,
    pub chunks_emitted: u64,
    /// Consistency achieved so far. A resumed snapshot downgrades to
    /// `Convergent`.
    pub consistency: Option<SnapshotConsistency>,
    pub rows: u64,
    /// Rows loaded per relation (used to validate the generation).
    #[serde(default)]
    pub relation_rows: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct StartRequest {
    /// Last acknowledged position (resume point). `None` on first start.
    pub resume: Option<SourcePosition>,
    /// Snapshot to perform (or resume) before streaming.
    pub snapshot: Option<SnapshotProgress>,
    pub snapshot_relations: Vec<RelationName>,
    pub snapshot_chunk_rows: u32,
    /// Current source epoch (incremented by authorized recovery).
    pub epoch: u32,
}

#[derive(Debug, Clone)]
pub enum SourceEvent {
    /// A complete unit (transaction or snapshot chunk), with updated snapshot
    /// progress for chunks.
    Unit(Box<CapturedTransaction>, Option<SnapshotProgress>),
    /// The snapshot finished; streaming follows.
    SnapshotComplete(SnapshotProgress),
    /// The source reported a relation definition that differs from the
    /// active schema; the engine refreshes schema before continuing.
    SchemaSignal(RelationName),
    /// Liveness; the position may be acknowledged if nothing is pending.
    Keepalive(Option<SourcePosition>),
}

#[async_trait]
pub trait ChangeStream: Send {
    async fn next(&mut self) -> Result<SourceEvent>;
    /// Confirm that everything up to `pos` is durably recoverable.
    async fn ack(&mut self, pos: &SourcePosition) -> Result<()>;
}

#[async_trait]
pub trait SourceConnector: Send + Sync {
    fn capabilities(&self) -> SourceCapabilities;
    async fn preflight(&self) -> Result<PreflightReport>;
    async fn identity(&self) -> Result<SourceIdentity>;
    async fn discover_schema(&self) -> Result<SourceSchema>;
    async fn start(&self, req: StartRequest) -> Result<Box<dyn ChangeStream>>;
    /// Statements an operator would run to enable capture. Never executed by
    /// the library.
    fn setup_statements(&self) -> Vec<String> {
        Vec::new()
    }

    /// Operator-invoked reconciliation only (never used to detect changes):
    /// current row count of a relation.
    async fn row_count(&self, _relation: &RelationName) -> Result<u64> {
        Err(crate::error::Error::new(crate::error::ErrorKind::Unsupported, "row counts are not supported by this connector"))
    }

    /// Operator-invoked reconciliation only: up to `limit` current rows of a
    /// relation in key order, as full images.
    async fn sample_rows(&self, _relation: &RelationName, _limit: u32) -> Result<Vec<Image>> {
        Err(crate::error::Error::new(crate::error::ErrorKind::Unsupported, "row sampling is not supported by this connector"))
    }
}
