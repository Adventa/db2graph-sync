//! Synthetic data shared by the runnable examples (no real data, no services).
#![allow(dead_code)]

use std::collections::BTreeMap;

use db2graph_sync::schema::{Column, ForeignKey, ImageMode, Relation, SourceSchema};
use db2graph_sync::types::*;

pub fn col(n: &str, t: &str) -> Column {
    Column { name: n.into(), type_name: t.into(), type_id: None, nullable: true }
}

/// customers(id) <- orders(id, customer_id)
pub fn shop_schema() -> SourceSchema {
    let mut s = SourceSchema { source_id: "example:shop".into(), relations: BTreeMap::new() };
    for r in [
        Relation {
            name: RelationName("public.customers".into()),
            columns: vec![col("id", "int8"), col("name", "text"), col("notes", "text"), col("ssn", "text")],
            primary_key: vec!["id".into()],
            unique_constraints: vec![],
            foreign_keys: vec![],
            image_mode: ImageMode::KeyOnly,
        },
        Relation {
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
        },
    ] {
        s.relations.insert(r.name.clone(), r);
    }
    s
}

pub fn insert(rel: &str, ordinal: u32, fields: &[(&str, Value)]) -> TxItem {
    TxItem::Change(Change {
        relation: RelationName(rel.into()),
        op: ChangeOp::Insert,
        event_ordinal: ordinal,
        old_key: None,
        before: None,
        after: Some(fields.iter().map(|(k, v)| (k.to_string(), FieldState::Present(v.clone()))).collect()),
        completeness: Completeness::FullAfterImage,
        schema_version: String::new(),
    })
}

/// A development config using in-code components, with state under `dir`
/// and the key read from `dir/state.key` (a `file:` secret reference).
pub fn dev_config(dir: &std::path::Path) -> db2graph_sync::Config {
    let key = dir.join("state.key");
    if !key.exists() {
        // Example-only key. Real deployments use `db2graph-sync keygen` and a secret manager.
        std::fs::write(&key, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
    }
    db2graph_sync::Config::from_toml_str(&format!(
        "[pipeline]\nid = \"example\"\ntenant = \"demo\"\nprofile = \"development\"\n\
         [source]\ntype = \"custom\"\n[graph]\ntype = \"custom\"\n\
         [state]\ndir = '{}'\nactive_key = 1\nkeys = [{{ id = 1, key = 'file:{}' }}]\n",
        dir.join("state").display(),
        key.display()
    ))
    .expect("example config")
}

pub async fn wait<F: Fn() -> bool>(f: F) {
    let start = std::time::Instant::now();
    while !f() {
        assert!(start.elapsed() < std::time::Duration::from_secs(20), "example timed out");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
