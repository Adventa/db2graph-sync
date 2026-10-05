//! Vector indexing against a real Qdrant (graph: in-memory reference sink,
//! source: scripted). Requires `D2G_IT_QDRANT_URL`; prints `UNEXECUTED:`
//! otherwise (treated as failure by the qualification scripts).
#![cfg(feature = "vector-qdrant")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use db2graph_sync::config::{EmbeddingConfig, Profile, VectorConfig, VectorSinkConfig, VectorTemplateConfig};
use db2graph_sync::policy::{Ineligible, Principal};
use db2graph_sync::retrieval::{Hit, Verdict, VersionRule};
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::vector::qdrant::QdrantSink;
use db2graph_sync::vector::{EmbeddingProvider, HashingEmbedder, VectorSink};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qdrant_indexing_and_authorized_retrieval() {
    let Ok(url) = std::env::var("D2G_IT_QDRANT_URL") else {
        println!("UNEXECUTED: qdrant_indexing_and_authorized_retrieval (D2G_IT_QDRANT_URL not set)");
        return;
    };
    let collection = format!("d2g_it_{}", std::process::id());
    let qdrant = QdrantSink::new(&url, &collection, None).unwrap();
    qdrant.ensure_collection(64).await.unwrap();
    assert!(qdrant.preflight(64).await.unwrap().passed());

    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let mut cfg = config(root.path(), Profile::Development);
    cfg.engine.allow_plaintext = true;
    cfg.policy = toml::from_str(
        "[[datasets]]\nrelation = \"public.customers\"\ndeny_fields = [\"ssn\"]\nembed_fields = [\"name\", \"ssn\"]\n\
         [[datasets]]\nrelation = \"public.orders\"\n",
    )
    .unwrap();
    cfg.vector = Some(VectorConfig {
        sink: VectorSinkConfig::Qdrant { url: url.clone(), collection: collection.clone(), api_key: None },
        embedding: EmbeddingConfig::Hashing { dimensions: 64 },
        templates: vec![VectorTemplateConfig {
            relation: "public.customers".into(),
            template: "Customer {name} ssn {ssn}".into(),
            chunk_chars: 2000,
        }],
        max_embedding_requests_per_minute: 6000,
        batch_size: 16,
    });
    let mut o = opts(&src, &graph);
    o.vector_sink = Some(Arc::new(QdrantSink::new(&url, &collection, None).unwrap()));
    let sync = db2graph_sync::start_with(cfg, o).await.unwrap();
    sync.ready(Duration::from_secs(20)).await.unwrap();

    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada")), ins("public.customers", 1, &customer(2, "Grace"))]));
    wait_for("vectors delivered", || sync.status().vector.as_ref().is_some_and(|v| v.delivered >= 2 && v.verified_visible >= 2)).await;

    let embedder = HashingEmbedder { dimensions: 64 };
    let q = embedder.embed(&["Customer Ada".to_string()]).await.unwrap().remove(0);
    let found = qdrant.search(&q, 5, "acme").await.unwrap();
    assert_eq!(found.len(), 2);
    for (_, payload) in &found {
        assert_eq!(payload["tenant"], "acme");
        assert!(payload["model_version"].as_str().unwrap().starts_with("feature-hash-v1"));
    }
    let hits: Vec<Hit> = found
        .iter()
        .map(|(_, p)| Hit { entity_id: p["entity_id"].as_str().unwrap().into(), version: p["version"].as_str().unwrap().into() })
        .collect();
    let principal = Principal { tenant: "acme".into(), ..Default::default() };
    let validator = sync.retrieval_validator();
    assert!(validator.validate(&principal, &hits, VersionRule::ExactVersion).await.unwrap().iter().all(Verdict::is_eligible));

    // Delete Ada in the source: ineligible at the boundary immediately, even
    // if the vector delete has not been processed yet.
    sync.pause();
    src.commit(src.unit(101, vec![del("public.customers", 0, 1)]));
    sync.resume();
    let ada = entity("public.customers", 1);
    let start = std::time::Instant::now();
    loop {
        let v = validator.validate(&principal, &hits, VersionRule::ExactVersion).await.unwrap();
        let ada_v = v
            .iter()
            .find(|x| matches!(x, Verdict::Ineligible { hit, .. } | Verdict::Eligible { hit, .. } if hit.entity_id == ada))
            .unwrap();
        if matches!(ada_v, Verdict::Ineligible { reason: Ineligible::NotFound | Ineligible::Deleted, .. }) {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(20));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_for("vector cleanup", || sync.status().vector.as_ref().is_some_and(|v| v.deleted >= 1)).await;
    let after = qdrant.search(&q, 5, "acme").await.unwrap();
    assert!(after.iter().all(|(_, p)| p["entity_id"] != ada.as_str()), "deleted entity's vectors removed");

    // Newly restricted dataset: ineligible immediately.
    sync.update_policy(
        &toml::from_str(
            "[[datasets]]\nrelation = \"public.customers\"\ndeny_fields = [\"ssn\"]\n[datasets.labels]\nclassification = \"CUI\"\n",
        )
        .unwrap(),
    )
    .unwrap();
    let grace = hits.iter().find(|h| h.entity_id == entity("public.customers", 2)).unwrap().clone();
    let v = validator.validate(&principal, &[grace], VersionRule::ExactVersion).await.unwrap();
    assert!(matches!(v[0], Verdict::Ineligible { reason: Ineligible::Classification, .. }));

    // The denied field never reached the vector payload or text.
    for (_, payload) in &after {
        assert!(!payload.to_string().contains("000-00-0000"));
    }
    sync.shutdown(Duration::from_secs(10)).await.unwrap();
}
