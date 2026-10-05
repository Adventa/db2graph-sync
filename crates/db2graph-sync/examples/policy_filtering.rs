//! Policy filtering and the authorized retrieval boundary:
//! denied fields are never journaled, projected or embedded; protected
//! fields are not sent to the embedder without authorization; retrieval
//! re-checks tenant and labels against the *current* graph and policy.
//!
//! `cargo run --example policy_filtering`

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use db2graph_sync::config::{EmbeddingConfig, VectorConfig, VectorSinkConfig, VectorTemplateConfig};
use db2graph_sync::policy::{PolicyConfig, Principal};
use db2graph_sync::retrieval::{Hit, Verdict, VersionRule};
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::vector::{EmbeddingProvider, HashingEmbedder, MemoryVectorSink};
use db2graph_sync::{Result, StartOptions};

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir().map_err(|e| db2graph_sync::Error::internal(e.to_string()))?;
    let mut cfg = common::dev_config(dir.path());
    cfg.policy = toml::from_str(
        r#"
[[datasets]]
relation = "public.customers"
deny_fields = ["ssn"]
embed_fields = ["name", "notes", "ssn"]
[datasets.field_labels.notes]
classification = "CONFIDENTIAL"
[[datasets]]
relation = "public.orders"
"#,
    )
    .map_err(|e| db2graph_sync::Error::config(e.to_string()))?;
    cfg.vector = Some(VectorConfig {
        sink: VectorSinkConfig::Custom(Default::default()),
        embedding: EmbeddingConfig::Hashing { dimensions: 32 },
        templates: vec![VectorTemplateConfig {
            relation: "public.customers".into(),
            template: "{name}. {notes}. {ssn}".into(),
            chunk_chars: 500,
        }],
        max_embedding_requests_per_minute: 6000,
        batch_size: 8,
    });

    let source = ScriptedSource::new(common::shop_schema());
    let graph = MemoryGraph::new();
    let vectors = MemoryVectorSink::new();
    let sync = db2graph_sync::start_with(
        cfg,
        StartOptions {
            source: Some(Arc::new(source.clone())),
            graph_sink: Some(Arc::new(graph.clone())),
            vector_sink: Some(Arc::new(vectors.clone())),
            ..Default::default()
        },
    )
    .await?;
    sync.ready(Duration::from_secs(10)).await?;
    source.commit(source.unit(
        1,
        vec![common::insert(
            "public.customers",
            0,
            &[("id", 1i64.into()), ("name", "Ada".into()), ("notes", "prefers email".into()), ("ssn", "000-00-0000".into())],
        )],
    ));
    common::wait(|| sync.status().vector.as_ref().is_some_and(|v| v.delivered >= 1)).await;

    let node = graph.state("example").nodes.values().next().cloned().expect("entity");
    println!("projected properties: {:?}", node.props.keys().collect::<Vec<_>>());
    assert!(!node.props.contains_key("ssn"));

    // Search, then validate through the retrieval boundary.
    let q = HashingEmbedder { dimensions: 32 }.embed(&["Ada".into()]).await?.remove(0);
    let hits: Vec<Hit> =
        vectors.search(&q, 3).into_iter().map(|(_, p)| Hit { entity_id: p.payload.entity_id, version: p.payload.version }).collect();
    let validator = sync.retrieval_validator();
    // The entity carries the CONFIDENTIAL marking of its `notes` field.
    let same_tenant = Principal { tenant: "demo".into(), classifications: ["CONFIDENTIAL".to_string()].into(), ..Default::default() };
    let other_tenant = Principal { tenant: "other".into(), ..Default::default() };
    for (who, p) in [("demo", &same_tenant), ("other", &other_tenant)] {
        for v in validator.validate(p, &hits, VersionRule::ExactVersion).await? {
            println!("principal {who}: {}", if v.is_eligible() { "eligible".to_owned() } else { format!("{v:?}") });
        }
    }

    // Newly restricted: ineligible immediately, before any vector cleanup.
    let mut restricted: PolicyConfig = toml::from_str(
        "[[datasets]]\nrelation = \"public.customers\"\ndeny_fields = [\"ssn\"]\n[datasets.labels]\nclassification = \"SECRET\"\n",
    )
    .map_err(|e| db2graph_sync::Error::config(e.to_string()))?;
    restricted.datasets.push(toml::from_str("relation = \"public.orders\"").map_err(|e| db2graph_sync::Error::config(e.to_string()))?);
    sync.update_policy(&restricted)?;
    let v = validator.validate(&same_tenant, &hits, VersionRule::ExactVersion).await?;
    assert!(matches!(v[0], Verdict::Ineligible { .. }));
    println!("after restriction: {:?}", v[0]);
    sync.shutdown(Duration::from_secs(10)).await?;
    Ok(())
}
