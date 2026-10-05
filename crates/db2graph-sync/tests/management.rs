//! Management API: authentication, scopes, correlation ids, idempotency,
//! invalid requests not affecting synchronization, and OpenAPI agreement.
#![cfg(feature = "service")]

mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::*;
use db2graph_sync::config::{ManagementToken, Profile, Scope};
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use sha2::{Digest, Sha256};
use tower::ServiceExt;

fn token(name: &str, secret: &str, scopes: Vec<Scope>) -> ManagementToken {
    ManagementToken { name: name.into(), sha256: hex::encode(Sha256::digest(secret.as_bytes())), scopes }
}

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: &str,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let mut b = Request::builder().method(method).uri(path).header("x-correlation-id", "test-cid-1").header("idempotency-key", "k1");
    if let Some(t) = bearer {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let resp = app.clone().oneshot(b.body(Body::from(body.to_owned())).unwrap()).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (status, headers, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn management_api_contract() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let sync = db2graph_sync::start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    sync.ready(Duration::from_secs(10)).await.unwrap();
    let app = db2graph_sync::management::router(
        sync.control(),
        vec![token("reader", "r-secret", vec![Scope::Read]), token("operator", "o-secret", vec![Scope::Read, Scope::Operate])],
    );

    let (s, h, body) = call(&app, "GET", "/v1/about", None, "").await;
    assert_eq!(s, StatusCode::OK);
    assert!(body.contains("an open-source project by Adventa"));
    assert_eq!(h["x-correlation-id"], "test-cid-1");

    assert_eq!(call(&app, "GET", "/v1/health/ready", None, "").await.0, StatusCode::OK);
    let (s, _, body) = call(&app, "GET", "/v1/status", None, "").await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert!(body.contains("\"code\":\"unauthorized\"") && body.contains("test-cid-1"));
    assert_eq!(call(&app, "GET", "/v1/status", Some("wrong"), "").await.0, StatusCode::UNAUTHORIZED);
    let (s, _, body) = call(&app, "GET", "/v1/status", Some("r-secret"), "").await;
    assert_eq!(s, StatusCode::OK);
    assert!(body.contains("\"phase\":\"streaming\""));
    assert_eq!(call(&app, "POST", "/v1/pause", Some("r-secret"), "").await.0, StatusCode::FORBIDDEN, "scope enforced");

    // Idempotent operations.
    for _ in 0..2 {
        let (s, h, _) = call(&app, "POST", "/v1/pause", Some("o-secret"), "").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(h["idempotency-key"], "k1");
    }
    wait_for("paused", || sync.status().phase == db2graph_sync::Phase::Paused).await;
    for _ in 0..2 {
        assert_eq!(call(&app, "POST", "/v1/resume", Some("o-secret"), "").await.0, StatusCode::OK);
    }

    // Invalid requests are rejected and do not affect synchronization.
    let (s, _, body) = call(&app, "POST", "/v1/mapping/approve", Some("o-secret"), "{not json").await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid_request"));
    assert_eq!(call(&app, "POST", "/v1/mapping/approve", Some("o-secret"), r#"{"digest":"zz"}"#).await.0, StatusCode::BAD_REQUEST);
    let huge = format!(r#"{{"principal":{{"tenant":"acme"}},"hits":[{}]}}"#, vec![r#"{"entity_id":"x","version":"v"}"#; 1001].join(","));
    assert_eq!(call(&app, "POST", "/v1/retrieval/validate", Some("r-secret"), &huge).await.0, StatusCode::BAD_REQUEST);
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    wait_for("still syncing", || sync.status().counters.units_applied >= 1).await;
    assert!(sync.status().failure.is_none());

    let id = entity("public.customers", 1);
    let ver = graph.state("test-pipeline").nodes[&id].version.clone();
    let req = format!(
        r#"{{"principal":{{"tenant":"acme"}},"hits":[{{"entity_id":"{id}","version":"{ver}"}},{{"entity_id":"nope","version":"x"}}]}}"#
    );
    let (s, _, body) = call(&app, "POST", "/v1/retrieval/validate", Some("r-secret"), &req).await;
    assert_eq!(s, StatusCode::OK);
    assert!(body.contains("\"verdict\":\"eligible\"") && body.contains("\"reason\":\"not_found\""), "{body}");

    let (s, _, body) = call(&app, "GET", "/v1/metrics", Some("r-secret"), "").await;
    assert_eq!(s, StatusCode::OK);
    assert!(body.contains("d2g_units_applied_total{pipeline=\"test-pipeline\"}"));

    sync.shutdown(Duration::from_secs(10)).await.unwrap();
}

#[test]
fn openapi_documents_exactly_the_served_routes() {
    let spec: serde_json::Value = serde_json::from_str(include_str!("../api/openapi.json")).expect("openapi.json is valid JSON");
    assert_eq!(spec["info"]["version"], "v1");
    let mut documented: Vec<(String, String)> = Vec::new();
    for (path, ops) in spec["paths"].as_object().unwrap() {
        for m in ops.as_object().unwrap().keys() {
            documented.push((m.to_uppercase(), path.clone()));
        }
    }
    documented.sort();
    let mut served = vec![
        ("GET", "/v1/about"),
        ("GET", "/v1/health/live"),
        ("GET", "/v1/health/ready"),
        ("GET", "/v1/status"),
        ("GET", "/v1/metrics"),
        ("POST", "/v1/pause"),
        ("POST", "/v1/resume"),
        ("POST", "/v1/mapping/approve"),
        ("POST", "/v1/retrieval/validate"),
    ]
    .into_iter()
    .map(|(m, p)| (m.to_owned(), p.to_owned()))
    .collect::<Vec<_>>();
    served.sort();
    assert_eq!(documented, served);
    // Every ErrorKind is listed in the spec.
    let codes = spec["components"]["schemas"]["Error"]["properties"]["code"]["enum"].as_array().unwrap().len();
    assert_eq!(codes, 27);
}
