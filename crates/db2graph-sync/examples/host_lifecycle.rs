//! Host application lifecycle: start the engine once in the application's
//! startup hook (never per request), expose readiness through the host's own
//! health endpoint, observe failures, and shut down in order.
//!
//! Runs without external services (scripted source, in-memory graph).
//! `cargo run --example host_lifecycle --features service`

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::{Control, StartOptions};

#[derive(Clone)]
struct AppState {
    sync: Control,
}

async fn health(State(s): State<AppState>) -> (StatusCode, String) {
    let st = s.sync.status();
    match (&st.failure, st.ready) {
        (Some(f), _) => (StatusCode::SERVICE_UNAVAILABLE, format!("sync failed: {}", f.kind)),
        (None, true) => (StatusCode::OK, "ok".into()),
        (None, false) => (StatusCode::SERVICE_UNAVAILABLE, format!("sync {:?}", st.phase)),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let source = ScriptedSource::new(common::shop_schema());
    let graph = MemoryGraph::new();

    // --- application startup hook: exactly one engine per process/state dir.
    let sync = db2graph_sync::start_with(
        common::dev_config(dir.path()),
        StartOptions { source: Some(Arc::new(source.clone())), graph_sink: Some(Arc::new(graph.clone())), ..Default::default() },
    )
    .await?;
    sync.ready(Duration::from_secs(10)).await?;

    // The application router. The management router is optional and mounted
    // under the host's own server and TLS; the library binds nothing.
    let app = Router::new()
        .route("/healthz", get(health))
        .with_state(AppState { sync: sync.control() })
        .nest("/sync", db2graph_sync::management::router(sync.control(), vec![]));

    // Normal application traffic (simulated): requests never start engines.
    source.commit(source.unit(1, vec![common::insert("public.customers", 0, &[("id", 1i64.into()), ("name", "Ada".into())])]));
    common::wait(|| sync.status().counters.units_applied >= 1).await;
    let resp = tower::ServiceExt::oneshot(app.clone(), axum::http::Request::get("/healthz").body(axum::body::Body::empty())?).await?;
    println!("GET /healthz -> {}", resp.status());

    // --- application shutdown hook: stop accepting work, then drain sync.
    let report = sync.shutdown(Duration::from_secs(10)).await?;
    println!("sync stopped: drained={} entities={}", report.drained, graph.state("example").nodes.len());
    Ok(())
}
