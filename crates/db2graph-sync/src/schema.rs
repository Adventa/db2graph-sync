//! Discovered source schema: relations, columns, real keys and constraints.
//!
//! Schemas are versioned by a content digest. Mappings are generated from a
//! schema version, and a change in the digest is what triggers mapping
//! re-evaluation.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::identity::content_digest;
use crate::types::RelationName;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    /// Native type name (e.g. `int8`, `numeric`, `timestamptz`).
    pub type_name: String,
    /// Native type identifier (e.g. PostgreSQL type OID) when available.
    pub type_id: Option<u32>,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueConstraint {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKey {
    pub name: String,
    /// Referencing columns, in constraint order.
    pub columns: Vec<String>,
    pub target: RelationName,
    /// Referenced columns, positionally matched to `columns`.
    pub target_columns: Vec<String>,
    /// Name of the referenced constraint: empty for the primary key, otherwise
    /// the unique constraint name.
    pub target_constraint: String,
}

/// How much of the old row the source emits on update/delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageMode {
    /// Old key columns only (PostgreSQL REPLICA IDENTITY DEFAULT/INDEX).
    KeyOnly,
    /// Entire old row (PostgreSQL REPLICA IDENTITY FULL, MySQL binlog_row_image=FULL).
    Full,
    /// No old image (PostgreSQL REPLICA IDENTITY NOTHING) — deletes cannot be captured.
    Nothing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    pub name: RelationName,
    pub columns: Vec<Column>,
    /// Primary key columns in key order. Empty when the relation has no
    /// primary key (such relations are rejected unless mapped with an
    /// explicit unique key).
    pub primary_key: Vec<String>,
    pub unique_constraints: Vec<UniqueConstraint>,
    pub foreign_keys: Vec<ForeignKey>,
    pub image_mode: ImageMode,
}

impl Relation {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SourceSchema {
    pub source_id: String,
    pub relations: BTreeMap<RelationName, Relation>,
}

impl SourceSchema {
    /// Deterministic digest of the schema content.
    pub fn version(&self) -> String {
        let bytes = serde_json::to_vec(&self.relations).expect("schema serializes");
        content_digest(&[self.source_id.as_bytes(), &bytes])[..16].to_owned()
    }
}

/// Classification of a schema difference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaChange {
    RelationAdded(RelationName),
    RelationRemoved(RelationName),
    ColumnAdded { relation: RelationName, column: String },
    ColumnRemoved { relation: RelationName, column: String },
    ColumnTypeChanged { relation: RelationName, column: String, from: String, to: String },
    KeyChanged { relation: RelationName },
    ConstraintsChanged { relation: RelationName },
    ImageModeChanged { relation: RelationName },
}

impl SchemaChange {
    /// Whether the change can be applied without operator approval. Only
    /// purely additive column changes are considered compatible; whether the
    /// new column is *projected* is decided by policy (unknown fields are
    /// denied in the restricted profile).
    pub fn is_additive(&self) -> bool {
        matches!(self, SchemaChange::ColumnAdded { .. } | SchemaChange::RelationAdded(_))
    }
}

/// Compute the differences between two schema versions.
pub fn diff(old: &SourceSchema, new: &SourceSchema) -> Vec<SchemaChange> {
    let mut out = Vec::new();
    for (name, o) in &old.relations {
        match new.relations.get(name) {
            None => out.push(SchemaChange::RelationRemoved(name.clone())),
            Some(n) => {
                for c in &o.columns {
                    match n.column(&c.name) {
                        None => out.push(SchemaChange::ColumnRemoved { relation: name.clone(), column: c.name.clone() }),
                        Some(nc) if nc.type_name != c.type_name => out.push(SchemaChange::ColumnTypeChanged {
                            relation: name.clone(),
                            column: c.name.clone(),
                            from: c.type_name.clone(),
                            to: nc.type_name.clone(),
                        }),
                        _ => {}
                    }
                }
                for c in &n.columns {
                    if o.column(&c.name).is_none() {
                        out.push(SchemaChange::ColumnAdded { relation: name.clone(), column: c.name.clone() });
                    }
                }
                if o.primary_key != n.primary_key {
                    out.push(SchemaChange::KeyChanged { relation: name.clone() });
                }
                if o.unique_constraints != n.unique_constraints || o.foreign_keys != n.foreign_keys {
                    out.push(SchemaChange::ConstraintsChanged { relation: name.clone() });
                }
                if o.image_mode != n.image_mode {
                    out.push(SchemaChange::ImageModeChanged { relation: name.clone() });
                }
            }
        }
    }
    for name in new.relations.keys() {
        if !old.relations.contains_key(name) {
            out.push(SchemaChange::RelationAdded(name.clone()));
        }
    }
    out
}
