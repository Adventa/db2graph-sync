//! Deterministic mapping from a discovered schema to a graph plan.
//!
//! * Every captured relation becomes a node label (PascalCase of the table
//!   name; schema-qualified on collision).
//! * Every *actual* foreign-key constraint becomes an owned relationship type
//!   `FK_<CONSTRAINT_NAME>`. Semantic names such as `PLACED_BY` are only used
//!   when an operator supplies them in `[[mapping.relations.edges]]`.
//! * Join tables become relationships only when declared.
//!
//! The plan is digested. Outside the development profile the engine refuses
//! to run a plan whose digest is not equal to `mapping.approved_digest`, so a
//! schema change that alters the plan pauses work until an operator approves
//! it. Suggested mappings (including AI-generated ones) are inert until their
//! digest is approved.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::error::{Error, ErrorKind, Result};
use crate::identity::content_digest;
use crate::policy::Policy;
use crate::schema::{Relation, SourceSchema};
use crate::types::RelationName;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MappingConfig {
    /// Digest printed by `db2graph-sync plan`. Required outside development.
    #[serde(default)]
    pub approved_digest: Option<String>,
    #[serde(default)]
    pub relations: Vec<RelationOverride>,
    #[serde(default)]
    pub join_tables: Vec<JoinTableConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RelationOverride {
    pub relation: String,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
    #[serde(default)]
    pub edges: Vec<EdgeOverride>,
    /// Use this unique constraint as the identity key when the relation has
    /// no primary key.
    #[serde(default)]
    pub key_constraint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeOverride {
    /// Source foreign-key constraint name.
    pub constraint: String,
    /// Approved relationship type, e.g. `PLACED_BY`.
    #[serde(rename = "type")]
    pub rel_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinTableConfig {
    pub relation: String,
    /// FK constraint pointing at the relationship's start node.
    pub from_constraint: String,
    /// FK constraint pointing at the relationship's end node.
    pub to_constraint: String,
    #[serde(rename = "type")]
    pub rel_type: String,
}

impl MappingConfig {
    pub fn findings(&self) -> Vec<String> {
        let mut out = Vec::new();
        for r in &self.relations {
            for l in r.labels.iter().flatten() {
                if !valid_graph_ident(l) {
                    out.push(format!("label `{l}` is not a valid identifier ([A-Za-z_][A-Za-z0-9_]{{0,63}})"));
                }
            }
            for e in &r.edges {
                if !valid_graph_ident(&e.rel_type) {
                    out.push(format!("relationship type `{}` is not a valid identifier", e.rel_type));
                }
            }
        }
        for j in &self.join_tables {
            if !valid_graph_ident(&j.rel_type) {
                out.push(format!("relationship type `{}` is not a valid identifier", j.rel_type));
            }
        }
        out
    }
}

/// Graph identifiers are interpolated into Cypher (labels and relationship
/// types cannot be parameters on every supported server), so they are
/// restricted to a safe alphabet. Values are always parameters.
pub fn valid_graph_ident(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && s.len() <= 64
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueKeyPlan {
    pub constraint: String,
    pub columns: Vec<String>,
    /// Node property holding the key digest (indexed at bootstrap).
    pub property: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeTarget {
    pub relation: RelationName,
    /// Empty for the primary key, else the referenced unique constraint.
    pub key_name: String,
    /// Node property matched on the target (`_d2g_id` for primary keys).
    pub property: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgePlan {
    pub constraint: String,
    pub columns: Vec<String>,
    pub target: EdgeTarget,
    pub rel_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationPlan {
    pub relation: RelationName,
    pub labels: Vec<String>,
    pub key_columns: Vec<String>,
    /// Unique constraints referenced by some foreign key (need lookup digests).
    pub unique_keys: Vec<UniqueKeyPlan>,
    pub edges: Vec<EdgePlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinPlan {
    pub relation: RelationName,
    pub key_columns: Vec<String>,
    pub from: EdgePlan,
    pub to: EdgePlan,
    pub rel_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MappingPlan {
    pub schema_version: String,
    pub relations: BTreeMap<RelationName, RelationPlan>,
    pub join_tables: BTreeMap<RelationName, JoinPlan>,
    /// Relations present in the schema but excluded, with the reason.
    pub excluded: BTreeMap<RelationName, String>,
}

impl MappingPlan {
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("plan serializes");
        content_digest(&[&bytes])[..32].to_owned()
    }

    /// Generate the plan for `schema` under `cfg` and `policy`.
    pub fn generate(schema: &SourceSchema, cfg: &MappingConfig, policy: &Policy) -> Result<MappingPlan> {
        let overrides: BTreeMap<&str, &RelationOverride> = cfg.relations.iter().map(|r| (r.relation.as_str(), r)).collect();
        let joins: BTreeMap<&str, &JoinTableConfig> = cfg.join_tables.iter().map(|j| (j.relation.as_str(), j)).collect();
        let mut excluded = BTreeMap::new();

        // Captured relations, in deterministic order.
        let captured: Vec<&Relation> = schema
            .relations
            .values()
            .filter(|r| {
                if policy.relation_allowed(&r.name.0) {
                    true
                } else {
                    excluded.insert(r.name.clone(), "not in dataset allowlist".to_owned());
                    false
                }
            })
            .collect();

        // Default labels with collision handling.
        let mut by_label: BTreeMap<String, Vec<&RelationName>> = BTreeMap::new();
        for r in &captured {
            by_label.entry(pascal(table_part(&r.name.0))).or_default().push(&r.name);
        }

        // Key resolution for every captured relation.
        let mut keys: BTreeMap<RelationName, Vec<String>> = BTreeMap::new();
        for r in &captured {
            let key = match overrides.get(r.name.0.as_str()).and_then(|o| o.key_constraint.as_ref()) {
                Some(uc) => r
                    .unique_constraints
                    .iter()
                    .find(|u| &u.name == uc)
                    .map(|u| u.columns.clone())
                    .ok_or_else(|| Error::config(format!("{}: key_constraint `{uc}` not found", r.name)))?,
                None => r.primary_key.clone(),
            };
            if key.is_empty() {
                excluded.insert(r.name.clone(), "no primary key and no key_constraint override".to_owned());
                continue;
            }
            keys.insert(r.name.clone(), key);
        }

        // Unique constraints referenced by any FK among captured relations.
        let mut referenced_uks: BTreeMap<RelationName, BTreeSet<String>> = BTreeMap::new();
        for r in &captured {
            for fk in &r.foreign_keys {
                if !fk.target_constraint.is_empty() {
                    referenced_uks.entry(fk.target.clone()).or_default().insert(fk.target_constraint.clone());
                }
            }
        }

        let mut relations = BTreeMap::new();
        let mut join_tables = BTreeMap::new();
        for r in &captured {
            let Some(key_columns) = keys.get(&r.name).cloned() else { continue };
            let ov = overrides.get(r.name.0.as_str());
            let edge_override: BTreeMap<&str, &str> =
                ov.map(|o| o.edges.iter().map(|e| (e.constraint.as_str(), e.rel_type.as_str())).collect()).unwrap_or_default();

            let mut edges = Vec::new();
            for fk in &r.foreign_keys {
                if !keys.contains_key(&fk.target) {
                    // Target not captured: no edge (recorded so plans are explicit).
                    continue;
                }
                let target_key_name = resolve_target_key(fk, schema)?;
                let property = if target_key_name.is_empty() { "_d2g_id".to_owned() } else { uk_property(&target_key_name) };
                // Reorder referencing columns into the target key's column
                // order so the digest equals the target's identity digest.
                let target_rel = &schema.relations[&fk.target];
                let target_key_cols: Vec<String> = if target_key_name.is_empty() {
                    keys[&fk.target].clone()
                } else {
                    target_rel
                        .unique_constraints
                        .iter()
                        .find(|u| u.name == target_key_name)
                        .map(|u| u.columns.clone())
                        .ok_or_else(|| Error::config(format!("{}: unknown unique constraint", fk.name)))?
                };
                let columns = target_key_cols
                    .iter()
                    .map(|tc| {
                        fk.target_columns
                            .iter()
                            .position(|c| c == tc)
                            .map(|i| fk.columns[i].clone())
                            .ok_or_else(|| Error::config(format!("{}: referenced column `{tc}` missing", fk.name)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                edges.push(EdgePlan {
                    constraint: fk.name.clone(),
                    columns,
                    target: EdgeTarget { relation: fk.target.clone(), key_name: target_key_name, property },
                    rel_type: edge_override.get(fk.name.as_str()).map(|s| s.to_string()).unwrap_or_else(|| default_rel_type(&fk.name)),
                });
            }

            if let Some(j) = joins.get(r.name.0.as_str()) {
                let find = |c: &str| {
                    edges
                        .iter()
                        .find(|e| e.constraint == c)
                        .cloned()
                        .ok_or_else(|| Error::config(format!("{}: join constraint `{c}` is not a captured foreign key", r.name)))
                };
                join_tables.insert(
                    r.name.clone(),
                    JoinPlan {
                        relation: r.name.clone(),
                        key_columns,
                        from: find(&j.from_constraint)?,
                        to: find(&j.to_constraint)?,
                        rel_type: j.rel_type.clone(),
                    },
                );
                continue;
            }

            let labels = match ov.and_then(|o| o.labels.clone()) {
                Some(l) => l,
                None => {
                    let base = pascal(table_part(&r.name.0));
                    if by_label.get(&base).map(|v| v.len()).unwrap_or(0) > 1 {
                        vec![pascal(&r.name.0.replace('.', "_"))]
                    } else {
                        vec![base]
                    }
                }
            };
            for l in &labels {
                if !valid_graph_ident(l) || l.starts_with("_D2G") {
                    return Err(Error::new(ErrorKind::Config, format!("{}: invalid label `{l}`", r.name)));
                }
            }
            let unique_keys = referenced_uks
                .get(&r.name)
                .into_iter()
                .flatten()
                .filter_map(|uc| {
                    r.unique_constraints.iter().find(|u| &u.name == uc).map(|u| UniqueKeyPlan {
                        constraint: u.name.clone(),
                        columns: u.columns.clone(),
                        property: uk_property(&u.name),
                    })
                })
                .collect();
            relations.insert(r.name.clone(), RelationPlan { relation: r.name.clone(), labels, key_columns, unique_keys, edges });
        }

        for o in &cfg.relations {
            if !schema.relations.contains_key(&RelationName(o.relation.clone())) {
                return Err(Error::config(format!("mapping override for unknown relation `{}`", o.relation)));
            }
        }

        Ok(MappingPlan { schema_version: schema.version(), relations, join_tables, excluded })
    }
}

fn resolve_target_key(fk: &crate::schema::ForeignKey, schema: &SourceSchema) -> Result<String> {
    if !fk.target_constraint.is_empty() {
        return Ok(fk.target_constraint.clone());
    }
    // Referenced columns equal the target primary key (in some order); the
    // caller reorders them into key order.
    let target = schema
        .relations
        .get(&fk.target)
        .ok_or_else(|| Error::config(format!("foreign key `{}` targets unknown relation {}", fk.name, fk.target)))?;
    let a: BTreeSet<&String> = fk.target_columns.iter().collect();
    let b: BTreeSet<&String> = target.primary_key.iter().collect();
    if a == b {
        Ok(String::new())
    } else {
        target
            .unique_constraints
            .iter()
            .find(|u| u.columns.iter().collect::<BTreeSet<_>>() == a)
            .map(|u| u.name.clone())
            .ok_or_else(|| Error::config(format!("foreign key `{}` references columns that are not a known key", fk.name)))
    }
}

pub fn uk_property(constraint: &str) -> String {
    format!("_d2g_uk_{}", sanitize_lower(constraint))
}

fn default_rel_type(constraint: &str) -> String {
    let s: String = constraint.chars().map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_uppercase() } else { '_' }).collect();
    let s = format!("FK_{s}");
    s.chars().take(64).collect()
}

fn sanitize_lower(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '_' }).collect()
}

fn table_part(rel: &str) -> &str {
    rel.rsplit('.').next().unwrap_or(rel)
}

fn pascal(s: &str) -> String {
    let mut out = String::new();
    let mut up = true;
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            if up {
                out.push(c.to_ascii_uppercase());
            } else {
                out.push(c);
            }
            up = false;
        } else {
            up = true;
        }
    }
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, 'R');
    }
    out.chars().take(64).collect()
}

#[cfg(test)]
pub(crate) mod tests_support {
    use super::*;
    use crate::schema::{Column, ForeignKey, ImageMode, UniqueConstraint};

    fn col(n: &str) -> Column {
        Column { name: n.into(), type_name: "int8".into(), type_id: Some(20), nullable: true }
    }

    pub(crate) fn sample_schema() -> SourceSchema {
        let mut s = SourceSchema { source_id: "pg:1".into(), relations: BTreeMap::new() };
        let customers = Relation {
            name: RelationName("public.customers".into()),
            columns: vec![col("id"), col("email")],
            primary_key: vec!["id".into()],
            unique_constraints: vec![UniqueConstraint { name: "customers_email_key".into(), columns: vec!["email".into()] }],
            foreign_keys: vec![],
            image_mode: ImageMode::KeyOnly,
        };
        let orders = Relation {
            name: RelationName("public.orders".into()),
            columns: vec![col("id"), col("customer_id"), col("billing_email")],
            primary_key: vec!["id".into()],
            unique_constraints: vec![],
            foreign_keys: vec![
                ForeignKey {
                    name: "orders_customer_id_fkey".into(),
                    columns: vec!["customer_id".into()],
                    target: RelationName("public.customers".into()),
                    target_columns: vec!["id".into()],
                    target_constraint: String::new(),
                },
                ForeignKey {
                    name: "orders_billing_email_fkey".into(),
                    columns: vec!["billing_email".into()],
                    target: RelationName("public.customers".into()),
                    target_columns: vec!["email".into()],
                    target_constraint: "customers_email_key".into(),
                },
            ],
            image_mode: ImageMode::KeyOnly,
        };
        s.relations.insert(customers.name.clone(), customers);
        s.relations.insert(orders.name.clone(), orders);
        s
    }
}

#[cfg(test)]
mod tests {
    use super::tests_support::sample_schema;
    use super::*;
    use crate::config::Profile;
    use crate::policy::PolicyConfig;

    #[test]
    fn plan_is_deterministic_and_uses_real_constraints() {
        let schema = sample_schema();
        let policy = Policy::compile(&PolicyConfig::default(), Profile::Development);
        let p1 = MappingPlan::generate(&schema, &MappingConfig::default(), &policy).unwrap();
        let p2 = MappingPlan::generate(&schema, &MappingConfig::default(), &policy).unwrap();
        assert_eq!(p1.digest(), p2.digest());
        let orders = &p1.relations[&RelationName("public.orders".into())];
        assert_eq!(orders.labels, vec!["Orders"]);
        assert_eq!(orders.edges.len(), 2, "multiple FKs between the same tables are distinct edges");
        assert_eq!(orders.edges[0].rel_type, "FK_ORDERS_CUSTOMER_ID_FKEY");
        assert_eq!(orders.edges[1].target.property, "_d2g_uk_customers_email_key");
        let customers = &p1.relations[&RelationName("public.customers".into())];
        assert_eq!(customers.unique_keys.len(), 1);
    }

    #[test]
    fn semantic_names_change_the_digest() {
        let schema = sample_schema();
        let policy = Policy::compile(&PolicyConfig::default(), Profile::Development);
        let base = MappingPlan::generate(&schema, &MappingConfig::default(), &policy).unwrap();
        let cfg = MappingConfig {
            relations: vec![RelationOverride {
                relation: "public.orders".into(),
                edges: vec![EdgeOverride { constraint: "orders_customer_id_fkey".into(), rel_type: "PLACED_BY".into() }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let named = MappingPlan::generate(&schema, &cfg, &policy).unwrap();
        assert_ne!(base.digest(), named.digest());
        assert_eq!(named.relations[&RelationName("public.orders".into())].edges[0].rel_type, "PLACED_BY");
    }

    #[test]
    fn rejects_injection_in_identifiers() {
        assert!(!valid_graph_ident("Order`) DETACH DELETE n //"));
        assert!(valid_graph_ident("PLACED_BY"));
    }
}
