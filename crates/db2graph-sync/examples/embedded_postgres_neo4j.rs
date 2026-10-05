//! Embedded PostgreSQL -> Neo4j, using the one-line startup.
//!
//! Prerequisites (operator steps, not done by the library):
//!   * PostgreSQL with `wal_level = logical`, a role with REPLICATION and
//!     SELECT, and the publication (`db2graph-sync source-setup -c <cfg>`).
//!   * Neo4j 5 with the constraints created by `db2graph-sync bootstrap -c <cfg>`.
//!   * Secrets available to the referenced locations (env vars here).
//!
//! Cargo.toml of the host application:
//! ```toml
//! [dependencies]
//! db2graph-sync = "0.1"   # pending publication; use { path = "..." } until then
//! tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
//! ```
//!
//! Run (against deploy/test/docker-compose.yml):
//!   D2G_PG_PASSWORD=d2g-test-only D2G_NEO4J_PASSWORD=d2g-test-only \
//!   D2G_STATE_KEY=$(db2graph-sync keygen) \
//!   cargo run --example embedded_postgres_neo4j -- examples/config/postgres-neo4j.dev.toml

use std::time::Duration;

use db2graph_sync::{Config, ErrorKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The host owns logging. The library only emits `tracing` events.
    let path = std::env::args().nth(1).unwrap_or_else(|| "examples/config/postgres-neo4j.dev.toml".into());
    let config = Config::from_file(&path)?;

    // One-line startup: validates, self-tests crypto, resolves secrets, opens
    // encrypted state, and starts supervision on *this* runtime.
    let sync = db2graph_sync::start(config).await?;

    // Startup success is not readiness. Wait (with a deadline) for the
    // snapshot to activate and live replication to begin.
    match sync.ready(Duration::from_secs(120)).await {
        Ok(()) => println!("db2graph-sync ready: {:?}", sync.status().phase),
        Err(e) if e.kind() == ErrorKind::Timeout => {
            // The host decides: keep serving with a degraded health check, or stop.
            eprintln!("not ready yet: {e}; continuing and observing");
        }
        Err(e) => {
            eprintln!("synchronization failed during startup: {e}");
            let _ = sync.wait().await;
            return Err(e.into());
        }
    }

    // Observe terminal failures alongside the application's own work.
    tokio::select! {
        err = sync.failed() => {
            // Security failures (tampered state, rollback, fenced out) stop
            // protected synchronization; the host owns the shutdown policy.
            eprintln!("synchronization stopped: [{}] {}", err.kind(), err.message());
            let _ = sync.wait().await;
            Err(err.into())
        }
        _ = tokio::signal::ctrl_c() => {
            let report = sync.shutdown(Duration::from_secs(30)).await?;
            println!("stopped; drained={} remaining={}", report.drained, report.remaining_journal_units);
            Ok(())
        }
    }
}
