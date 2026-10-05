//! A custom graph sink. It wraps the in-memory reference sink and counts
//! applied units. It declares its capabilities honestly: because it is not
//! part of this project's qualification it reports `qualified: false`, and
//! implementing the trait does not by itself grant a stronger guarantee.
//!
//! Run the conformance suite against your own sink before relying on it:
//! `db2graph_sync::sink::conformance::run_all(&sink, "qual").await`.
//!
//! `cargo run --example custom_graph_sink`

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use db2graph_sync::capability::{GraphSinkCapabilities, PreflightReport};
use db2graph_sync::mapping::MappingPlan;
use db2graph_sync::plan::GraphTx;
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::sink::{ApplyContext, ApplyOutcome, EntityState, GraphSink, PipelineMarker, VectorTask};
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::{Result, StartOptions};

struct CountingSink {
    inner: MemoryGraph,
    units: AtomicU64,
}

#[async_trait]
impl GraphSink for CountingSink {
    fn capabilities(&self) -> GraphSinkCapabilities {
        let mut c = self.inner.capabilities();
        c.sink = "counting-example".into();
        c.qualified = false;
        c
    }
    async fn preflight(&self, plan: Option<&MappingPlan>) -> Result<PreflightReport> {
        self.inner.preflight(plan).await
    }
    async fn bootstrap(&self, plan: &MappingPlan) -> Result<()> {
        self.inner.bootstrap(plan).await
    }
    async fn acquire_fence(&self, pipeline: &str) -> Result<(i64, PipelineMarker)> {
        self.inner.acquire_fence(pipeline).await
    }
    async fn marker(&self, pipeline: &str) -> Result<Option<PipelineMarker>> {
        self.inner.marker(pipeline).await
    }
    async fn apply(&self, ctx: &ApplyContext, batch: &[GraphTx]) -> Result<Vec<ApplyOutcome>> {
        let out = self.inner.apply(ctx, batch).await?;
        self.units.fetch_add(out.len() as u64, Ordering::Relaxed);
        Ok(out)
    }
    async fn set_active_generation(&self, ctx: &ApplyContext, generation: u64) -> Result<()> {
        self.inner.set_active_generation(ctx, generation).await
    }
    async fn counts(&self, pipeline: &str) -> Result<BTreeMap<String, u64>> {
        self.inner.counts(pipeline).await
    }
    async fn lookup(&self, pipeline: &str, ids: &[String]) -> Result<Vec<EntityState>> {
        self.inner.lookup(pipeline, ids).await
    }
    async fn pending_vector_tasks(&self, pipeline: &str, limit: usize) -> Result<Vec<VectorTask>> {
        self.inner.pending_vector_tasks(pipeline, limit).await
    }
    async fn vector_backlog(&self, pipeline: &str) -> Result<u64> {
        self.inner.vector_backlog(pipeline).await
    }
    async fn complete_vector_task(&self, pipeline: &str, task: &VectorTask) -> Result<bool> {
        self.inner.complete_vector_task(pipeline, task).await
    }
    async fn pending_edges(&self, pipeline: &str) -> Result<u64> {
        self.inner.pending_edges(pipeline).await
    }
    async fn outgoing(&self, pipeline: &str, id: &str) -> Result<Vec<(String, String)>> {
        self.inner.outgoing(pipeline, id).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let sink = Arc::new(CountingSink { inner: MemoryGraph::new(), units: AtomicU64::new(0) });
    // Qualify the sink with the shared conformance suite first.
    db2graph_sync::sink::conformance::run_all(sink.as_ref(), "qualify").await?;
    println!("conformance suite passed");

    let dir = tempfile::tempdir().map_err(|e| db2graph_sync::Error::internal(e.to_string()))?;
    let source = ScriptedSource::new(common::shop_schema());
    let sync = db2graph_sync::start_with(
        common::dev_config(dir.path()),
        StartOptions { source: Some(Arc::new(source.clone())), graph_sink: Some(sink.clone()), ..Default::default() },
    )
    .await?;
    sync.ready(Duration::from_secs(10)).await?;
    println!("warnings: {:?}", sync.status().warnings);
    source.commit(source.unit(1, vec![common::insert("public.customers", 0, &[("id", 1i64.into()), ("name", "Ada".into())])]));
    common::wait(|| sync.status().counters.units_applied >= 1).await;
    sync.shutdown(Duration::from_secs(10)).await?;
    println!("units seen by custom sink: {}", sink.units.load(Ordering::Relaxed));
    Ok(())
}
