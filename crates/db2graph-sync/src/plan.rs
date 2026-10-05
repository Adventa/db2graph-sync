//! Translation of captured transactions into backend-neutral graph operations.
//!
//! A [`GraphTx`] contains the operations for exactly one source unit, in
//! source order. Sinks must apply a `GraphTx` atomically and must apply the
//! `GraphTx`s of a batch in order.
//!
//! Field-state semantics when upserting an entity:
//!
//! | State         | Effect                                                        |
//! |---------------|---------------------------------------------------------------|
//! | `Present(v)`  | property set (`Null` removes it and records it in `_d2g_nulls`) |
//! | `Unchanged`   | property left as is; if the entity is new, recorded in `_d2g_incomplete` |
//! | `Missing`     | property left as is                                           |
//! | `Unavailable` | property left as is; recorded in `_d2g_incomplete`            |
//! | `Removed`     | property removed                                              |

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::error::{Error, ErrorKind, Result};
use crate::identity::{IdentityScope, content_digest, entity_id, key_digest};
use crate::mapping::{EdgePlan, MappingPlan, RelationPlan};
use crate::policy::{Decision, Labels, Policy};
use crate::types::{CapturedTransaction, Change, ChangeOp, FieldState, Image, RelationName, TxItem, UnitKind, Value, Version};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum GraphOp {
    UpsertEntity(EntityUpsert),
    DeleteEntity {
        id: String,
        relation: RelationName,
        labels: Vec<String>,
        version: String,
        vector: bool,
    },
    /// Replace the owned edge for (`from_id`, `constraint`). `target = None`
    /// removes it (the reference became NULL). Applied only if the owning
    /// entity's current version equals `version` (i.e. the upsert that
    /// produced this op was not stale).
    SetEdge {
        from_id: String,
        constraint: String,
        rel_type: String,
        target: Option<EdgeRef>,
        version: String,
    },
    UpsertJoinEdge {
        id: String,
        relation: RelationName,
        rel_type: String,
        from: EdgeRef,
        to: EdgeRef,
        props: BTreeMap<String, Value>,
        version: String,
    },
    DeleteJoinEdge {
        id: String,
        relation: RelationName,
        version: String,
    },
    /// Remove every join-table relationship of `relation` older than
    /// `version` and record a watermark so stale events cannot recreate them.
    TruncateJoinRelation {
        relation: RelationName,
        version: String,
    },
    /// Remove every entity of a relation owned by this pipeline whose version
    /// is older than `version`, and record a relation-level watermark so
    /// stale events cannot recreate them.
    TruncateRelation {
        relation: RelationName,
        labels: Vec<String>,
        version: String,
        vector: bool,
    },
}

/// Reference to an edge endpoint by key property and digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeRef {
    pub property: String,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityUpsert {
    pub id: String,
    pub relation: RelationName,
    pub labels: Vec<String>,
    pub version: String,
    /// Properties to set (non-null present values).
    pub set: BTreeMap<String, Value>,
    /// Properties to remove (present-null and removed fields).
    pub remove: Vec<String>,
    /// Fields explicitly null in this image.
    pub nulls: Vec<String>,
    /// Fields whose value is not known to the graph (unchanged/unavailable on
    /// an entity that may not have them yet).
    pub incomplete: Vec<String>,
    /// Unique-key lookup properties (property -> digest).
    pub key_props: BTreeMap<String, String>,
    pub tenant: String,
    /// JSON-serialized [`Labels`].
    pub labels_json: String,
    /// Create durable vector work for this entity in the same transaction.
    pub vector: bool,
    /// Digest of (id, version, set, remove): an entity seen again at the same
    /// version with a different digest is a consistency incident.
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphTx {
    pub unit_id: String,
    pub content_digest: String,
    /// Highest version produced by this unit (pipeline progress marker).
    pub version: String,
    pub ops: Vec<GraphOp>,
}

/// Context needed to plan transactions.
pub struct PlanContext<'a> {
    pub tenant: &'a str,
    pub source_id: &'a str,
    pub mapping: &'a MappingPlan,
    pub policy: &'a Policy,
    /// Relations that have a vector template.
    pub vector_relations: &'a BTreeSet<String>,
}

impl PlanContext<'_> {
    fn scope<'b>(&'b self, relation: &'b str) -> IdentityScope<'b> {
        IdentityScope { tenant: self.tenant, source_id: self.source_id, relation }
    }
}

/// Plan one captured unit.
pub fn plan_transaction(tx: &CapturedTransaction, ctx: &PlanContext<'_>) -> Result<GraphTx> {
    let mut ops = Vec::new();
    let mut max_version = tx.base_version;
    for item in &tx.items {
        match item {
            TxItem::Change(c) => {
                let version = item_version(tx, c.event_ordinal);
                max_version = max_version.max(version);
                plan_change(c, version, ctx, &mut ops)?;
            }
            TxItem::Truncate(t) => {
                let version = item_version(tx, t.event_ordinal);
                max_version = max_version.max(version);
                for r in &t.relations {
                    if let Some(rp) = ctx.mapping.relations.get(r) {
                        ops.push(GraphOp::TruncateRelation {
                            relation: r.clone(),
                            labels: rp.labels.clone(),
                            version: version.encode(),
                            vector: ctx.vector_relations.contains(&r.0),
                        });
                    } else if ctx.mapping.join_tables.contains_key(r) {
                        ops.push(GraphOp::TruncateJoinRelation { relation: r.clone(), version: version.encode() });
                    }
                }
            }
        }
    }
    // The receipt digest covers the captured unit (what the source said), not
    // the planned operations, so replaying a unit after an approved mapping or
    // policy change is recognized as a duplicate rather than an incident.
    let bytes = serde_json::to_vec(&(&tx.items, &tx.base_version, &tx.kind)).map_err(|e| Error::internal(e.to_string()))?;
    Ok(GraphTx {
        unit_id: tx.unit_id.clone(),
        content_digest: content_digest(&[tx.unit_id.as_bytes(), &bytes]),
        version: max_version.encode(),
        ops,
    })
}

fn item_version(tx: &CapturedTransaction, ordinal: u32) -> Version {
    match tx.kind {
        UnitKind::SnapshotChunk { .. } => tx.base_version,
        UnitKind::Transaction => Version { ordinal: ordinal.saturating_add(1), ..tx.base_version },
    }
}

/// Key values for `cols` from `img`. Returns `Ok(None)` when any key column
/// is SQL NULL (no identity / no reference), and an error when a key value is
/// not available at all.
fn key_values<'a>(img: &'a Image, cols: &[String], what: &str) -> Result<Option<Vec<&'a Value>>> {
    let mut out = Vec::with_capacity(cols.len());
    for c in cols {
        match img.get(c) {
            Some(FieldState::Present(Value::Null)) => return Ok(None),
            Some(FieldState::Present(v)) => out.push(v),
            other => {
                return Err(Error::new(
                    ErrorKind::SchemaIncompatible,
                    format!("{what}: key column `{c}` not available in image ({other:?})"),
                ));
            }
        }
    }
    Ok(Some(out))
}

/// Whether every column in `cols` is unchanged/missing in `img` (so a
/// reference built from them did not change).
fn all_unchanged(img: &Image, cols: &[String]) -> bool {
    cols.iter().all(|c| matches!(img.get(c), Some(FieldState::Unchanged) | Some(FieldState::Missing) | None))
}

fn tenant_allowed(c: &Change, img: &Image, ctx: &PlanContext<'_>) -> bool {
    match ctx.policy.dataset(&c.relation.0).and_then(|d| d.tenant_column.as_deref()) {
        None => true,
        Some(col) => crate::policy::tenant_matches(img.get(col), ctx.tenant),
    }
}

fn plan_change(c: &Change, version: Version, ctx: &PlanContext<'_>, ops: &mut Vec<GraphOp>) -> Result<()> {
    if let Some(jp) = ctx.mapping.join_tables.get(&c.relation) {
        return plan_join_change(c, version, jp, ctx, ops);
    }
    let Some(rp) = ctx.mapping.relations.get(&c.relation) else {
        // Not mapped (excluded by policy or mapping): ignored by design.
        return Ok(());
    };
    let rel = c.relation.0.as_str();
    let v = version.encode();
    let vector = ctx.vector_relations.contains(rel);

    let old_id = match &c.old_key {
        Some(k) => key_values(k, &rp.key_columns, rel)?.map(|vals| entity_id(ctx.scope(rel), &vals)),
        None => None,
    };

    match c.op {
        ChangeOp::Delete => {
            let id = match old_id {
                Some(id) => id,
                None => {
                    let before = c
                        .before
                        .as_ref()
                        .ok_or_else(|| Error::new(ErrorKind::SchemaIncompatible, format!("{rel}: delete without key image")))?;
                    let vals = key_values(before, &rp.key_columns, rel)?
                        .ok_or_else(|| Error::new(ErrorKind::SchemaIncompatible, format!("{rel}: delete with NULL key")))?;
                    entity_id(ctx.scope(rel), &vals)
                }
            };
            ops.push(GraphOp::DeleteEntity { id, relation: c.relation.clone(), labels: rp.labels.clone(), version: v, vector });
        }
        ChangeOp::Insert | ChangeOp::Update | ChangeOp::Read => {
            let after =
                c.after.as_ref().ok_or_else(|| Error::new(ErrorKind::Internal, format!("{rel}: {:?} without after image", c.op)))?;
            let vals = key_values(after, &rp.key_columns, rel)?
                .ok_or_else(|| Error::new(ErrorKind::SchemaIncompatible, format!("{rel}: NULL primary key")))?;
            let id = entity_id(ctx.scope(rel), &vals);

            if !tenant_allowed(c, after, ctx) {
                // Row is (now) outside this tenant: ensure it is not projected.
                ops.push(GraphOp::DeleteEntity { id, relation: c.relation.clone(), labels: rp.labels.clone(), version: v, vector });
                return Ok(());
            }

            // Primary-key change: the old identity is deleted in the same unit.
            if let Some(old) = &old_id
                && *old != id
            {
                ops.push(GraphOp::DeleteEntity {
                    id: old.clone(),
                    relation: c.relation.clone(),
                    labels: rp.labels.clone(),
                    version: v.clone(),
                    vector,
                });
            }
            let key_changed = old_id.as_ref().is_some_and(|o| *o != id);
            ops.push(GraphOp::UpsertEntity(build_upsert(c, after, id.clone(), &v, rp, ctx, vector)?));
            for e in &rp.edges {
                if !key_changed && c.op == ChangeOp::Update && all_unchanged(after, &e.columns) {
                    continue;
                }
                ops.push(edge_op(&id, e, after, &v, ctx)?);
            }
        }
    }
    Ok(())
}

fn edge_op(from_id: &str, e: &EdgePlan, img: &Image, version: &str, ctx: &PlanContext<'_>) -> Result<GraphOp> {
    let target_rel = e.target.relation.0.as_str();
    let target = key_values(img, &e.columns, &e.constraint)?
        .map(|vals| EdgeRef { property: e.target.property.clone(), digest: key_digest(ctx.scope(target_rel), &e.target.key_name, &vals) });
    Ok(GraphOp::SetEdge {
        from_id: from_id.to_owned(),
        constraint: e.constraint.clone(),
        rel_type: e.rel_type.clone(),
        target,
        version: version.to_owned(),
    })
}

fn build_upsert(
    c: &Change,
    after: &Image,
    id: String,
    version: &str,
    rp: &RelationPlan,
    ctx: &PlanContext<'_>,
    vector: bool,
) -> Result<EntityUpsert> {
    let rel = c.relation.0.as_str();
    let mut set = BTreeMap::new();
    let mut remove = Vec::new();
    let mut nulls = Vec::new();
    let mut incomplete = Vec::new();
    for (col, state) in after {
        if ctx.policy.field(rel, col) == Decision::Deny {
            continue;
        }
        if col.starts_with("_d2g") {
            return Err(Error::new(ErrorKind::SchemaIncompatible, format!("{rel}: column `{col}` uses the reserved _d2g prefix")));
        }
        match state {
            FieldState::Present(Value::Null) => {
                remove.push(col.clone());
                nulls.push(col.clone());
            }
            FieldState::Present(v) => {
                set.insert(col.clone(), v.clone());
            }
            FieldState::Removed => remove.push(col.clone()),
            FieldState::Unchanged | FieldState::Unavailable => incomplete.push(col.clone()),
            FieldState::Missing => {}
        }
    }
    let mut key_props = BTreeMap::new();
    for uk in &rp.unique_keys {
        if let Some(vals) = key_values(after, &uk.columns, &uk.constraint).ok().flatten() {
            key_props.insert(uk.property.clone(), key_digest(ctx.scope(rel), &uk.constraint, &vals));
        }
    }
    let labels: Labels = ctx.policy.entity_labels(rel, set.keys().map(String::as_str));
    let body = serde_json::to_vec(&(&set, &remove)).map_err(|e| Error::internal(e.to_string()))?;
    let digest = content_digest(&[id.as_bytes(), version.as_bytes(), &body]);
    Ok(EntityUpsert {
        digest,
        id,
        relation: c.relation.clone(),
        labels: rp.labels.clone(),
        version: version.to_owned(),
        set,
        remove,
        nulls,
        incomplete,
        key_props,
        tenant: ctx.tenant.to_owned(),
        labels_json: serde_json::to_string(&labels).map_err(|e| Error::internal(e.to_string()))?,
        vector,
    })
}

fn plan_join_change(
    c: &Change,
    version: Version,
    jp: &crate::mapping::JoinPlan,
    ctx: &PlanContext<'_>,
    ops: &mut Vec<GraphOp>,
) -> Result<()> {
    let rel = c.relation.0.as_str();
    let v = version.encode();
    let old_id = match &c.old_key {
        Some(k) => key_values(k, &jp.key_columns, rel)?.map(|vals| entity_id(ctx.scope(rel), &vals)),
        None => None,
    };
    match c.op {
        ChangeOp::Delete => {
            let img = c
                .old_key
                .as_ref()
                .or(c.before.as_ref())
                .ok_or_else(|| Error::new(ErrorKind::SchemaIncompatible, format!("{rel}: delete without key image")))?;
            let vals = key_values(img, &jp.key_columns, rel)?
                .ok_or_else(|| Error::new(ErrorKind::SchemaIncompatible, format!("{rel}: NULL key")))?;
            ops.push(GraphOp::DeleteJoinEdge { id: entity_id(ctx.scope(rel), &vals), relation: c.relation.clone(), version: v });
        }
        _ => {
            let after = c.after.as_ref().ok_or_else(|| Error::internal(format!("{rel}: missing after image")))?;
            let vals = key_values(after, &jp.key_columns, rel)?
                .ok_or_else(|| Error::new(ErrorKind::SchemaIncompatible, format!("{rel}: NULL key")))?;
            let id = entity_id(ctx.scope(rel), &vals);
            if let Some(old) = old_id.filter(|o| *o != id) {
                ops.push(GraphOp::DeleteJoinEdge { id: old, relation: c.relation.clone(), version: v.clone() });
            }
            let end = |e: &EdgePlan| -> Result<Option<EdgeRef>> {
                Ok(key_values(after, &e.columns, &e.constraint)?.map(|vals| EdgeRef {
                    property: e.target.property.clone(),
                    digest: key_digest(ctx.scope(&e.target.relation.0), &e.target.key_name, &vals),
                }))
            };
            match (end(&jp.from)?, end(&jp.to)?) {
                (Some(from), Some(to)) => {
                    let mut props = BTreeMap::new();
                    let fk_cols: BTreeSet<&String> = jp.from.columns.iter().chain(jp.to.columns.iter()).collect();
                    for (col, st) in after {
                        if fk_cols.contains(col) || ctx.policy.field(rel, col) == Decision::Deny {
                            continue;
                        }
                        if let FieldState::Present(val) = st
                            && *val != Value::Null
                        {
                            props.insert(col.clone(), val.clone());
                        }
                    }
                    ops.push(GraphOp::UpsertJoinEdge {
                        id,
                        relation: c.relation.clone(),
                        rel_type: jp.rel_type.clone(),
                        from,
                        to,
                        props,
                        version: v,
                    });
                }
                // A NULL endpoint means no relationship.
                _ => ops.push(GraphOp::DeleteJoinEdge { id, relation: c.relation.clone(), version: v }),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Profile;
    use crate::mapping::MappingConfig;
    use crate::policy::PolicyConfig;
    use crate::types::{Completeness, OrderingScope, SourcePosition};

    fn ctx_parts() -> (MappingPlan, Policy) {
        let schema = crate::mapping::tests_support::sample_schema();
        let policy = Policy::compile(&PolicyConfig::default(), Profile::Development);
        let plan = MappingPlan::generate(&schema, &MappingConfig::default(), &policy).unwrap();
        (plan, policy)
    }

    fn img(pairs: &[(&str, FieldState)]) -> Image {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn tx(items: Vec<TxItem>) -> CapturedTransaction {
        let scope = OrderingScope { source_id: "pg:1".into(), epoch: 0, partition: String::new() };
        CapturedTransaction {
            unit_id: "tx:1".into(),
            kind: UnitKind::Transaction,
            source_id: "pg:1".into(),
            scope: scope.clone(),
            ack_position: SourcePosition { scope, ordinal: Some(100), native: "0/64".into() },
            base_version: Version { epoch: 0, position: 100, ordinal: 0 },
            native_tx_id: Some("42".into()),
            commit_time: None,
            items,
            mapping_version: "m".into(),
            policy_version: "p".into(),
        }
    }

    fn change(rel: &str, op: ChangeOp, ord: u32, old_key: Option<Image>, after: Option<Image>) -> TxItem {
        TxItem::Change(Change {
            relation: RelationName(rel.into()),
            op,
            event_ordinal: ord,
            old_key,
            before: None,
            after,
            completeness: Completeness::FullAfterImage,
            schema_version: "s".into(),
        })
    }

    #[test]
    fn insert_creates_entity_and_edges_with_null_fk_removing_edge() {
        let (plan, policy) = ctx_parts();
        let vr = BTreeSet::new();
        let ctx = PlanContext { tenant: "t", source_id: "pg:1", mapping: &plan, policy: &policy, vector_relations: &vr };
        let t = tx(vec![change(
            "public.orders",
            ChangeOp::Insert,
            0,
            None,
            Some(img(&[
                ("id", FieldState::Present(Value::Int(1))),
                ("customer_id", FieldState::Present(Value::Int(7))),
                ("billing_email", FieldState::Present(Value::Null)),
            ])),
        )]);
        let g = plan_transaction(&t, &ctx).unwrap();
        assert_eq!(g.ops.len(), 3);
        assert!(matches!(&g.ops[0], GraphOp::UpsertEntity(u) if u.nulls == vec!["billing_email".to_string()]));
        assert!(matches!(&g.ops[1], GraphOp::SetEdge { target: Some(_), .. }));
        assert!(matches!(&g.ops[2], GraphOp::SetEdge { target: None, .. }));
        // The PK reference digest equals the customer's entity id.
        let cust = entity_id(IdentityScope { tenant: "t", source_id: "pg:1", relation: "public.customers" }, &[&Value::Int(7)]);
        assert!(matches!(&g.ops[1], GraphOp::SetEdge { target: Some(r), .. } if r.digest == cust));
    }

    #[test]
    fn pk_change_deletes_old_identity_first() {
        let (plan, policy) = ctx_parts();
        let vr = BTreeSet::new();
        let ctx = PlanContext { tenant: "t", source_id: "pg:1", mapping: &plan, policy: &policy, vector_relations: &vr };
        let t = tx(vec![change(
            "public.customers",
            ChangeOp::Update,
            0,
            Some(img(&[("id", FieldState::Present(Value::Int(1)))])),
            Some(img(&[("id", FieldState::Present(Value::Int(2))), ("email", FieldState::Unchanged)])),
        )]);
        let g = plan_transaction(&t, &ctx).unwrap();
        assert!(matches!(&g.ops[0], GraphOp::DeleteEntity { .. }));
        assert!(matches!(&g.ops[1], GraphOp::UpsertEntity(u) if u.incomplete == vec!["email".to_string()]));
    }

    #[test]
    fn unchanged_fk_columns_do_not_touch_edges() {
        let (plan, policy) = ctx_parts();
        let vr = BTreeSet::new();
        let ctx = PlanContext { tenant: "t", source_id: "pg:1", mapping: &plan, policy: &policy, vector_relations: &vr };
        let t = tx(vec![change(
            "public.orders",
            ChangeOp::Update,
            0,
            None,
            Some(img(&[
                ("id", FieldState::Present(Value::Int(1))),
                ("customer_id", FieldState::Unchanged),
                ("billing_email", FieldState::Unchanged),
            ])),
        )]);
        let g = plan_transaction(&t, &ctx).unwrap();
        assert_eq!(g.ops.len(), 1);
    }

    #[test]
    fn identical_input_gives_identical_digest() {
        let (plan, policy) = ctx_parts();
        let vr = BTreeSet::new();
        let ctx = PlanContext { tenant: "t", source_id: "pg:1", mapping: &plan, policy: &policy, vector_relations: &vr };
        let t = tx(vec![change(
            "public.customers",
            ChangeOp::Insert,
            0,
            None,
            Some(img(&[("id", FieldState::Present(Value::Int(1))), ("email", FieldState::Present(Value::Text("a@x.test".into())))])),
        )]);
        assert_eq!(plan_transaction(&t, &ctx).unwrap().content_digest, plan_transaction(&t, &ctx).unwrap().content_digest);
    }
}
