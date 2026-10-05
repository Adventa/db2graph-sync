//! Optional Qdrant indexing with the authorized retrieval boundary.
//! Needs a Qdrant endpoint (deploy/test/docker-compose.yml provides one):
//!
//! `D2G_QDRANT_URL=http://127.0.0.1:56333 cargo run --example qdrant_indexing --features vector-qdrant`
//!
//! Uses the deterministic hashing embedder (not semantic) so the example
//! needs no external embedding provider; switch to `type = "http"` for a
//! real model.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use db2graph_sync::config::{EmbeddingConfig, VectorConfig, VectorSinkConfig, VectorTemplateConfig};
use db2graph_sync::policy::Principal;
use db2graph_sync::retrieval::{Hit, VersionRule};
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::vector::qdrant::QdrantSink;
use db2graph_sync::vector::{EmbeddingProvider, HashingEmbedder};
use db2graph_sync::{Result, StartOptions};

#[tokio::main]
async fn main() -> Result<()> {
    let Ok(url) = std::env::var("D2G_QDRANT_URL") else {
        eprintln!("set D2G_QDRANT_URL to run this example");
        return Ok(());
    };
    let dir = tempfile::tempdir().map_err(|e| db2graph_sync::Error::internal(e.to_string()))?;
    let mut cfg = common::dev_config(dir.path());
    cfg.engine.allow_plaintext = url.starts_with("http://");
    cfg.vector = Some(VectorConfig {
        sink: VectorSinkConfig::Qdrant { url: url.clone(), collection: "d2g_example".into(), api_key: None },
        embedding: EmbeddingConfig::Hashing { dimensions: 64 },
        templates: vec![VectorTemplateConfig {
            relation: "public.customers".into(),
            template: "Customer {name}: {notes}".into(),
            chunk_chars: 1000,
        }],
        max_embedding_requests_per_minute: 600,
        batch_size: 16,
    });
    // Explicit operator step: create the collection.
    db2graph_sync::admin::bootstrap_vector(&cfg).await?;

    let source = ScriptedSource::new(common::shop_schema());
    let graph = MemoryGraph::new();
    let sync = db2graph_sync::start_with(
        cfg,
        StartOptions { source: Some(Arc::new(source.clone())), graph_sink: Some(Arc::new(graph)), ..Default::default() },
    )
    .await?;
    sync.ready(Duration::from_secs(20)).await?;
    source.commit(source.unit(
        1,
        vec![common::insert("public.customers", 0, &[("id", 1i64.into()), ("name", "Ada".into()), ("notes", "analytical engines".into())])],
    ));
    common::wait(|| sync.status().vector.as_ref().is_some_and(|v| v.delivered >= 1)).await;
    println!("vector status: {:?}", sync.status().vector);

    let q = HashingEmbedder { dimensions: 64 }.embed(&["analytical engines".into()]).await?.remove(0);
    let qdrant = QdrantSink::new(&url, "d2g_example", None)?;
    let hits: Vec<Hit> = qdrant
        .search(&q, 5, "demo")
        .await?
        .into_iter()
        .map(|(_, p)| Hit {
            entity_id: p["entity_id"].as_str().unwrap_or_default().into(),
            version: p["version"].as_str().unwrap_or_default().into(),
        })
        .collect();
    let verdicts = sync
        .retrieval_validator()
        .validate(&Principal { tenant: "demo".into(), ..Default::default() }, &hits, VersionRule::ExactVersion)
        .await?;
    println!("validated hits: {verdicts:?}");
    sync.shutdown(Duration::from_secs(10)).await?;
    Ok(())
}
