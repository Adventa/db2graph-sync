//! Restart and replay: a crash after the graph commit but before the local
//! checkpoint is replayed on restart, and the receipt stored in the graph
//! makes the replay a no-op (counted as a duplicate).
//!
//! `cargo run --example restart_replay`

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::{Error, ErrorKind, Result, StartOptions};

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir().map_err(|e| Error::internal(e.to_string()))?;
    let source = ScriptedSource::new(common::shop_schema());
    let graph = MemoryGraph::new();

    // First run: simulate a crash right after the graph commit.
    let crashed = Arc::new(AtomicBool::new(false));
    let c2 = crashed.clone();
    let opts = StartOptions {
        source: Some(Arc::new(source.clone())),
        graph_sink: Some(Arc::new(graph.clone())),
        fault_injector: Some(Arc::new(move |point| {
            if point == "graph.after_commit" && !c2.swap(true, Ordering::SeqCst) {
                return Err(Error::new(ErrorKind::Internal, "simulated crash after graph commit"));
            }
            Ok(())
        })),
        ..Default::default()
    };
    let sync = db2graph_sync::start_with(common::dev_config(dir.path()), opts).await?;
    source.commit(source.unit(
        1,
        vec![
            common::insert("public.customers", 0, &[("id", 1i64.into()), ("name", "Ada".into())]),
            common::insert("public.orders", 1, &[("id", 10i64.into()), ("customer_id", 1i64.into())]),
        ],
    ));
    let err = sync.failed().await;
    println!("first run stopped: {err}");
    let _ = sync.wait().await;
    println!("graph after crash: {} entities, {} receipts", graph.state("example").nodes.len(), graph.state("example").receipts.len());

    // Restart on the same state: the journaled unit is replayed.
    let sync = db2graph_sync::start_with(
        common::dev_config(dir.path()),
        StartOptions { source: Some(Arc::new(source.clone())), graph_sink: Some(Arc::new(graph.clone())), ..Default::default() },
    )
    .await?;
    sync.ready(Duration::from_secs(10)).await?;
    common::wait(|| sync.status().counters.duplicates >= 1).await;
    let st = sync.status();
    sync.shutdown(Duration::from_secs(10)).await?;
    println!(
        "after restart: duplicates={} entities={} relationships={}",
        st.counters.duplicates,
        graph.state("example").nodes.len(),
        graph.state("example").edges.len()
    );
    assert_eq!(graph.state("example").nodes.len(), 2);
    Ok(())
}
