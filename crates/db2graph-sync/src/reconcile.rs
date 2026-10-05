//! Operator-invoked reconciliation.
//!
//! Two kinds of hashes appear in evidence and must not be confused:
//!
//! * **Expected destination hash** — the content digest of what the engine
//!   *intended* to write, recorded in receipts and audit (`batch_applied`).
//! * **Verified readback hash** — computed here by reading the *current*
//!   graph entity back and hashing its properties, compared with the hash of
//!   the policy-filtered *current* source row.
//!
//! Reconciliation reads business tables (counts and a bounded sample in key
//! order). It is an explicit operator action and is never used to detect
//! changes. Rows changing while it runs produce transient differences;
//! re-run to confirm.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value as Json;

use crate::error::Result;
use crate::identity::{IdentityScope, entity_id};
use crate::mapping::MappingPlan;
use crate::policy::{Decision, Policy};
use crate::sink::GraphSink;
use crate::source::SourceConnector;
use crate::types::{FieldState, Image, Value};

#[derive(Debug, Clone, Serialize)]
pub struct RelationReport {
    pub relation: String,
    /// `None` when the connector cannot count rows.
    pub source_rows: Option<u64>,
    pub graph_entities: u64,
    pub sampled: u64,
    /// Sampled entities whose verified readback hash matched the source.
    pub verified: u64,
    pub missing_in_graph: Vec<String>,
    pub mismatched: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReconcileReport {
    pub pipeline: String,
    pub generated_at: String,
    pub plan_digest: String,
    pub policy_version: String,
    pub hash_kind: &'static str,
    pub relations: Vec<RelationReport>,
    /// True when every count matched and every sampled entity verified.
    pub consistent: bool,
}

/// Graph representation of a value (what a sink stores), as JSON.
pub fn graph_repr(v: &Value) -> Option<Json> {
    use base64::Engine;
    Some(match v {
        Value::Null => return None,
        Value::Bool(b) => Json::Bool(*b),
        Value::Int(i) => Json::from(*i),
        Value::Float(f) if f.is_finite() => Json::from(*f),
        Value::Float(f) => Json::String(f.to_string()),
        Value::Bytes(b) => Json::String(format!("b64:{}", base64::engine::general_purpose::STANDARD.encode(b))),
        Value::Array(a) => Json::Array(a.iter().filter_map(graph_repr).collect()),
        Value::Unknown { text, .. } => Json::String(text.clone()),
        Value::BigInt(s)
        | Value::Decimal(s)
        | Value::Text(s)
        | Value::Uuid(s)
        | Value::Date(s)
        | Value::Time(s)
        | Value::Timestamp(s)
        | Value::TimestampTz(s)
        | Value::Json(s) => Json::String(s.clone()),
    })
}

/// Canonical hash of a property map in graph representation.
pub fn readback_hash(props: &BTreeMap<String, Json>) -> String {
    crate::identity::content_digest(&[serde_json::to_string(props).unwrap_or_default().as_bytes()])
}

fn expected_props(relation: &str, row: &Image, policy: &Policy) -> BTreeMap<String, Json> {
    row.iter()
        .filter(|(k, _)| policy.field(relation, k) == Decision::Allow)
        .filter_map(|(k, s)| match s {
            FieldState::Present(v) => graph_repr(v).map(|j| (k.clone(), j)),
            _ => None,
        })
        .collect()
}

pub async fn reconcile(
    pipeline: &str,
    tenant: &str,
    source: &dyn SourceConnector,
    sink: &dyn GraphSink,
    plan: &MappingPlan,
    policy: &Policy,
    sample: u32,
) -> Result<ReconcileReport> {
    let source_id = source.identity().await?.source_id;
    let counts = sink.counts(pipeline).await?;
    let mut reports = Vec::new();
    let mut consistent = true;
    for (rel, rp) in &plan.relations {
        let graph_entities = counts.get(&rel.0).copied().unwrap_or(0);
        let tenant_col = policy.dataset(&rel.0).and_then(|d| d.tenant_column.clone());
        // A whole-table count includes other tenants' rows and is not
        // comparable with this tenant's projection; report it as unknown.
        let source_rows = if tenant_col.is_some() { None } else { source.row_count(rel).await.ok() };
        let rows = source.sample_rows(rel, sample).await.unwrap_or_default();
        let mut ids = Vec::new();
        let mut expected = BTreeMap::new();
        for row in &rows {
            if let Some(col) = &tenant_col
                && !crate::policy::tenant_matches(row.get(col), tenant)
            {
                continue;
            }
            let keys: Option<Vec<&Value>> = rp
                .key_columns
                .iter()
                .map(|k| match row.get(k) {
                    Some(FieldState::Present(v)) if *v != Value::Null => Some(v),
                    _ => None,
                })
                .collect();
            let Some(keys) = keys else { continue };
            let id = entity_id(IdentityScope { tenant, source_id: &source_id, relation: &rel.0 }, &keys);
            expected.insert(id.clone(), readback_hash(&expected_props(&rel.0, row, policy)));
            ids.push(id);
        }
        let found: BTreeMap<String, String> = sink
            .lookup(pipeline, &ids)
            .await?
            .into_iter()
            .filter(|e| !e.deleted)
            .map(|e| {
                let props: BTreeMap<String, Json> =
                    e.properties.iter().filter_map(|(k, v)| graph_repr(v).map(|j| (k.clone(), j))).collect();
                (e.id, readback_hash(&props))
            })
            .collect();
        let mut missing = Vec::new();
        let mut mismatched = Vec::new();
        let mut verified = 0;
        for (id, h) in &expected {
            match found.get(id) {
                None => missing.push(id.clone()),
                Some(g) if g == h => verified += 1,
                Some(_) => mismatched.push(id.clone()),
            }
        }
        if source_rows.is_some_and(|n| n != graph_entities) || !missing.is_empty() || !mismatched.is_empty() {
            consistent = false;
        }
        reports.push(RelationReport {
            relation: rel.0.clone(),
            source_rows,
            graph_entities,
            sampled: expected.len() as u64,
            verified,
            missing_in_graph: missing,
            mismatched,
        });
    }
    Ok(ReconcileReport {
        pipeline: pipeline.to_owned(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        plan_digest: plan.digest(),
        policy_version: policy.version().to_owned(),
        hash_kind: "verified_readback",
        relations: reports,
        consistent,
    })
}
