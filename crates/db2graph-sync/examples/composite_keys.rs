//! Composite keys and relationships: a composite primary key, a composite
//! foreign key whose columns are declared in a different order than the
//! referenced key, a self reference, and a relationship that arrives before
//! its endpoint (retained as pending, then resolved).
//!
//! `cargo run --example composite_keys`

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use db2graph_sync::schema::{ForeignKey, ImageMode, Relation, SourceSchema};
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::types::{RelationName, Value};
use db2graph_sync::{Result, StartOptions};

fn schema() -> SourceSchema {
    let mut s = SourceSchema { source_id: "example:warehouse".into(), relations: BTreeMap::new() };
    for r in [
        Relation {
            name: RelationName("wh.bins".into()),
            columns: vec![
                common::col("site", "text"),
                common::col("bin", "int4"),
                common::col("parent_site", "text"),
                common::col("parent_bin", "int4"),
            ],
            primary_key: vec!["site".into(), "bin".into()],
            unique_constraints: vec![],
            foreign_keys: vec![ForeignKey {
                name: "bins_parent_fkey".into(),
                columns: vec!["parent_site".into(), "parent_bin".into()],
                target: RelationName("wh.bins".into()),
                target_columns: vec!["site".into(), "bin".into()],
                target_constraint: String::new(),
            }],
            image_mode: ImageMode::KeyOnly,
        },
        Relation {
            name: RelationName("wh.stock".into()),
            columns: vec![
                common::col("sku", "text"),
                common::col("bin_no", "int4"),
                common::col("bin_site", "text"),
                common::col("qty", "int4"),
            ],
            primary_key: vec!["sku".into(), "bin_site".into(), "bin_no".into()],
            unique_constraints: vec![],
            // Declared (bin_no, bin_site) -> (bin, site): reordered by the plan.
            foreign_keys: vec![ForeignKey {
                name: "stock_bin_fkey".into(),
                columns: vec!["bin_no".into(), "bin_site".into()],
                target: RelationName("wh.bins".into()),
                target_columns: vec!["bin".into(), "site".into()],
                target_constraint: String::new(),
            }],
            image_mode: ImageMode::KeyOnly,
        },
    ] {
        s.relations.insert(r.name.clone(), r);
    }
    s
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir().map_err(|e| db2graph_sync::Error::internal(e.to_string()))?;
    let source = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let sync = db2graph_sync::start_with(
        common::dev_config(dir.path()),
        StartOptions { source: Some(Arc::new(source.clone())), graph_sink: Some(Arc::new(graph.clone())), ..Default::default() },
    )
    .await?;
    sync.ready(Duration::from_secs(10)).await?;
    let t = |s: &str| Value::Text(s.into());
    // Stock arrives before its bin (e.g. deferred constraint): pending.
    source.commit(source.unit(
        1,
        vec![common::insert("wh.stock", 0, &[("sku", t("A-1")), ("bin_no", 7i64.into()), ("bin_site", t("SYD")), ("qty", 3i64.into())])],
    ));
    common::wait(|| graph.state("example").pending.len() == 1).await;
    println!("pending relationships: {}", graph.state("example").pending.len());
    source.commit(source.unit(
        2,
        vec![
            common::insert(
                "wh.bins",
                0,
                &[("site", t("SYD")), ("bin", 1i64.into()), ("parent_site", Value::Null), ("parent_bin", Value::Null)],
            ),
            common::insert(
                "wh.bins",
                1,
                &[("site", t("SYD")), ("bin", 7i64.into()), ("parent_site", t("SYD")), ("parent_bin", 1i64.into())],
            ),
        ],
    ));
    common::wait(|| graph.state("example").edges.len() == 2).await;
    let g = graph.state("example");
    println!("entities: {}  relationships: {}  pending: {}", g.nodes.len(), g.edges.len(), g.pending.len());
    for e in g.edges.values() {
        println!("  ({}) -[:{}]-> ({})", &e.from_id[..8], e.rel_type, &e.to_id[..8]);
    }
    assert!(g.pending.is_empty());
    sync.shutdown(Duration::from_secs(10)).await?;
    Ok(())
}
