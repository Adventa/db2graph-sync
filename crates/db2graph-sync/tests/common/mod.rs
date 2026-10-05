//! Shared helpers for engine-level tests (synthetic data only).
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use db2graph_sync::config::*;
use db2graph_sync::schema::{Column, ForeignKey, ImageMode, Relation, SourceSchema, UniqueConstraint};
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::types::*;

pub const KEY_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

pub fn col(n: &str, t: &str) -> Column {
    Column { name: n.into(), type_name: t.into(), type_id: None, nullable: true }
}

pub fn schema() -> SourceSchema {
    let mut s = SourceSchema { source_id: "scripted:1".into(), relations: BTreeMap::new() };
    let customers = Relation {
        name: RelationName("public.customers".into()),
        columns: vec![col("id", "int8"), col("name", "text"), col("email", "text"), col("ssn", "text")],
        primary_key: vec!["id".into()],
        unique_constraints: vec![UniqueConstraint { name: "customers_email_key".into(), columns: vec!["email".into()] }],
        foreign_keys: vec![],
        image_mode: ImageMode::KeyOnly,
    };
    let orders = Relation {
        name: RelationName("public.orders".into()),
        columns: vec![col("id", "int8"), col("customer_id", "int8"), col("total", "numeric")],
        primary_key: vec!["id".into()],
        unique_constraints: vec![],
        foreign_keys: vec![ForeignKey {
            name: "orders_customer_id_fkey".into(),
            columns: vec!["customer_id".into()],
            target: RelationName("public.customers".into()),
            target_columns: vec!["id".into()],
            target_constraint: String::new(),
        }],
        image_mode: ImageMode::KeyOnly,
    };
    s.relations.insert(customers.name.clone(), customers);
    s.relations.insert(orders.name.clone(), orders);
    s
}

/// Config rooted at `root`: state in `root/state`, key in `root/state.key`
/// (referenced as a `file:` secret, exactly like a mounted secret).
pub fn config(root: &Path, profile: Profile) -> Config {
    let key_path = root.join("state.key");
    if !key_path.exists() {
        std::fs::write(&key_path, KEY_B64).expect("write key");
    }
    let toml = format!(
        r#"
[pipeline]
id = "test-pipeline"
tenant = "acme"
profile = "{profile}"

[source]
type = "custom"

[graph]
type = "custom"

[state]
dir = '{dir}'
active_key = 1
keys = [{{ id = 1, key = 'file:{key}' }}]

[engine]
max_batch_transactions = 8
retry_max_backoff_ms = 200
"#,
        profile = match profile {
            Profile::Development => "development",
            Profile::Enterprise => "enterprise",
            Profile::Restricted => "restricted",
        },
        dir = root.join("state").display(),
        key = key_path.display()
    );
    Config::from_toml_str(&toml).expect("config parses")
}

pub fn img(pairs: &[(&str, Value)]) -> Image {
    pairs.iter().map(|(k, v)| (k.to_string(), FieldState::Present(v.clone()))).collect()
}

pub fn ins(rel: &str, ord: u32, pairs: &[(&str, Value)]) -> TxItem {
    TxItem::Change(Change {
        relation: RelationName(rel.into()),
        op: ChangeOp::Insert,
        event_ordinal: ord,
        old_key: None,
        before: None,
        after: Some(img(pairs)),
        completeness: Completeness::FullAfterImage,
        schema_version: String::new(),
    })
}

pub fn read(rel: &str, pairs: &[(&str, Value)]) -> TxItem {
    TxItem::Change(Change {
        relation: RelationName(rel.into()),
        op: ChangeOp::Read,
        event_ordinal: 0,
        old_key: None,
        before: None,
        after: Some(img(pairs)),
        completeness: Completeness::FullAfterImage,
        schema_version: String::new(),
    })
}

pub fn del(rel: &str, ord: u32, id: i64) -> TxItem {
    TxItem::Change(Change {
        relation: RelationName(rel.into()),
        op: ChangeOp::Delete,
        event_ordinal: ord,
        old_key: Some(img(&[("id", Value::Int(id))])),
        before: None,
        after: None,
        completeness: Completeness::KeyOnly,
        schema_version: String::new(),
    })
}

pub fn snapshot_chunk(src: &ScriptedSource, rel: &str, items: Vec<TxItem>) -> CapturedTransaction {
    let mut u = src.unit(5, items);
    u.kind = UnitKind::SnapshotChunk { generation: 1, relation: RelationName(rel.into()), chunk: 0 };
    u
}

pub fn customer(id: i64, name: &str) -> Vec<(&'static str, Value)> {
    vec![
        ("id", Value::Int(id)),
        ("name", Value::Text(name.into())),
        ("email", Value::Text(format!("c{id}@example.test"))),
        ("ssn", Value::Text("000-00-0000".into())),
    ]
}

pub fn order(id: i64, customer: i64) -> Vec<(&'static str, Value)> {
    vec![("id", Value::Int(id)), ("customer_id", Value::Int(customer)), ("total", Value::Decimal("10.50".into()))]
}

pub fn entity(rel: &str, id: i64) -> String {
    db2graph_sync::identity::entity_id(
        db2graph_sync::identity::IdentityScope { tenant: "acme", source_id: "scripted:1", relation: rel },
        &[&Value::Int(id)],
    )
}

pub async fn wait_for<F: Fn() -> bool>(what: &str, f: F) {
    let start = std::time::Instant::now();
    while !f() {
        assert!(start.elapsed() < std::time::Duration::from_secs(15), "timed out waiting for {what}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

pub fn opts(src: &ScriptedSource, graph: &db2graph_sync::sink::memory::MemoryGraph) -> db2graph_sync::StartOptions {
    db2graph_sync::StartOptions { source: Some(Arc::new(src.clone())), graph_sink: Some(Arc::new(graph.clone())), ..Default::default() }
}
