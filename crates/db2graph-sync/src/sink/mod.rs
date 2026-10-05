//! Graph sink interface.
//!
//! A sink applies batches of [`GraphTx`] (each one source unit). The
//! contract a sink must implement to declare
//! [`DeliveryGuarantee::EffectivelyOnceEffects`](crate::capability::DeliveryGuarantee):
//!
//! 1. A batch commits atomically, including receipts, tombstones, pending
//!    relationship records, vector outbox entries and the pipeline marker.
//! 2. Before applying a unit, the sink checks for its receipt *while holding
//!    the pipeline lock*: same digest → `Duplicate` (no effect); different
//!    digest → `ConsistencyIncident` error and the batch is rolled back.
//! 3. Entity writes are version guarded under a lock (a read followed by an
//!    unguarded write is not sufficient).
//! 4. Every batch verifies the fence token under the same lock and fails
//!    with `FencedOut` if a newer owner exists.
//! 5. Deletes leave version state (tombstones) so stale events cannot
//!    resurrect entities; deletes never remove relationships the pipeline
//!    does not own.
//!
//! [`conformance`] contains the executable form of this contract.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::capability::{GraphSinkCapabilities, PreflightReport};
use crate::error::Result;
use crate::mapping::MappingPlan;
use crate::plan::GraphTx;
use crate::types::Value;

pub mod conformance;
pub mod memory;
#[cfg(feature = "neo4j")]
pub mod neo4j;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyContext {
    pub pipeline: String,
    pub fence_token: i64,
    /// Local state epoch at the time of the batch (rollback evidence).
    pub state_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyStatus {
    Applied,
    /// The receipt already existed with the same digest; nothing changed.
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyOutcome {
    pub unit_id: String,
    pub status: ApplyStatus,
}

/// Pipeline marker stored in the target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PipelineMarker {
    pub fence_token: i64,
    /// Highest local state epoch recorded by any committed batch.
    pub state_epoch: u64,
    pub last_unit: Option<String>,
    pub last_version: Option<String>,
    pub active_generation: Option<u64>,
}

/// Current target-side state of an entity (for retrieval validation and
/// vector rendering).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityState {
    pub id: String,
    pub relation: String,
    pub tenant: String,
    pub version: String,
    pub deleted: bool,
    pub labels_json: String,
    pub properties: BTreeMap<String, Value>,
}

/// Durable vector work stored in the target's outbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorTask {
    pub entity_id: String,
    pub relation: String,
    pub version: String,
    pub delete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Incident {
    pub unit_id: String,
    pub expected_digest: String,
    pub found_digest: String,
}

#[async_trait]
pub trait GraphSink: Send + Sync {
    fn capabilities(&self) -> GraphSinkCapabilities;
    async fn preflight(&self, plan: Option<&MappingPlan>) -> Result<PreflightReport>;
    /// Create constraints and indexes required by `plan`. Called only by the
    /// explicit bootstrap command/API.
    async fn bootstrap(&self, plan: &MappingPlan) -> Result<()>;
    /// Take ownership of `pipeline`, returning a strictly larger fence token
    /// than any previous owner received, and the marker as found.
    async fn acquire_fence(&self, pipeline: &str) -> Result<(i64, PipelineMarker)>;
    async fn marker(&self, pipeline: &str) -> Result<Option<PipelineMarker>>;
    /// Apply a batch atomically, in order.
    async fn apply(&self, ctx: &ApplyContext, batch: &[GraphTx]) -> Result<Vec<ApplyOutcome>>;
    async fn set_active_generation(&self, ctx: &ApplyContext, generation: u64) -> Result<()>;
    /// Entity counts per relation owned by the pipeline (non-deleted).
    async fn counts(&self, pipeline: &str) -> Result<BTreeMap<String, u64>>;
    async fn lookup(&self, pipeline: &str, ids: &[String]) -> Result<Vec<EntityState>>;
    async fn pending_vector_tasks(&self, pipeline: &str, limit: usize) -> Result<Vec<VectorTask>>;
    async fn vector_backlog(&self, pipeline: &str) -> Result<u64>;
    /// Remove the task if its version is still `task.version`. Returns false
    /// if a newer task superseded it.
    async fn complete_vector_task(&self, pipeline: &str, task: &VectorTask) -> Result<bool>;
    /// Unresolved relationship records awaiting their endpoint.
    async fn pending_edges(&self, pipeline: &str) -> Result<u64>;
    /// Owned outgoing relationships of an entity as (type, target id), sorted.
    async fn outgoing(&self, pipeline: &str, id: &str) -> Result<Vec<(String, String)>>;
}
