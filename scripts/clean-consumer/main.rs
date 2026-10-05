//! Independent consumer of the packaged crate: start, sync one unit, stop.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use db2graph_sync::schema::{Column, ImageMode, Relation, SourceSchema};
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::types::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let about = db2graph_sync::about();
    assert_eq!(about.license, "Apache-2.0");
    println!("{}", about.project);

    let dir = std::env::temp_dir().join(format!("d2g-consumer-state-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("key"), "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=")?;
    let config = db2graph_sync::Config::from_toml_str(&format!(
        "[pipeline]\nid = \"consumer\"\ntenant = \"t\"\n[source]\ntype = \"custom\"\n[graph]\ntype = \"custom\"\n\
         [state]\ndir = '{}'\nactive_key = 1\nkeys = [{{ id = 1, key = 'file:{}' }}]\n",
        dir.join("state").display(),
        dir.join("key").display()
    ))?;
    let mut relations = BTreeMap::new();
    let rel = Relation {
        name: RelationName("app.items".into()),
        columns: vec![Column { name: "id".into(), type_name: "int8".into(), type_id: None, nullable: false }],
        primary_key: vec!["id".into()],
        unique_constraints: vec![],
        foreign_keys: vec![],
        image_mode: ImageMode::KeyOnly,
    };
    relations.insert(rel.name.clone(), rel);
    let source = ScriptedSource::new(SourceSchema { source_id: "consumer:1".into(), relations });
    let graph = MemoryGraph::new();
    let sync = db2graph_sync::start_with(
        config,
        db2graph_sync::StartOptions { source: Some(Arc::new(source.clone())), graph_sink: Some(Arc::new(graph.clone())), ..Default::default() },
    )
    .await?;
    sync.ready(Duration::from_secs(20)).await?;
    source.commit(source.unit(
        1,
        vec![TxItem::Change(Change {
            relation: RelationName("app.items".into()),
            op: ChangeOp::Insert,
            event_ordinal: 0,
            old_key: None,
            before: None,
            after: Some([("id".to_string(), FieldState::Present(Value::Int(1)))].into()),
            completeness: Completeness::FullAfterImage,
            schema_version: String::new(),
        })],
    ));
    let start = std::time::Instant::now();
    while graph.state("consumer").nodes.is_empty() {
        assert!(start.elapsed() < Duration::from_secs(20), "unit not applied");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let report = sync.shutdown(Duration::from_secs(10)).await?;
    assert!(report.drained);
    let _ = std::fs::remove_dir_all(&dir);
    println!("consumer synced 1 entity and shut down cleanly");
    Ok(())
}
