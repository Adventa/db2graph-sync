//! In-memory reference graph sink.
//!
//! This sink is the executable specification of the sink contract. It is
//! used by unit tests, by the conformance suite, and by examples. It is
//! **not durable**: its contents live only as long as the process (or the
//! `MemoryGraph` value shared between engine instances in a test).
//!
//! Atomicity: a batch is applied to a copy of the pipeline state, which
//! replaces the original only if every unit succeeds.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{ApplyContext, ApplyOutcome, ApplyStatus, EntityState, GraphSink, PipelineMarker, VectorTask};
use crate::capability::{CheckStatus, GraphSinkCapabilities, PreflightReport};
use crate::error::{Error, ErrorKind, Result};
use crate::mapping::MappingPlan;
use crate::plan::{EdgeRef, EntityUpsert, GraphOp, GraphTx};
use crate::types::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub relation: String,
    pub labels: Vec<String>,
    pub tenant: String,
    pub version: String,
    pub digest: String,
    pub props: BTreeMap<String, Value>,
    pub key_props: BTreeMap<String, String>,
    pub nulls: Vec<String>,
    pub incomplete: Vec<String>,
    pub labels_json: String,
    /// Stripped placeholder kept because non-owned relationships exist.
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EdgeKey {
    Fk { from_id: String, constraint: String },
    Join { id: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
    pub rel_type: String,
    pub from_id: String,
    pub to_id: String,
    pub from_ref: EdgeRef,
    pub to_ref: EdgeRef,
    pub props: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pending {
    pub rel_type: String,
    pub from_ref: EdgeRef,
    pub to_ref: EdgeRef,
    pub props: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default)]
pub struct PipelineState {
    pub marker: PipelineMarker,
    pub receipts: HashMap<String, String>,
    pub nodes: BTreeMap<String, Node>,
    pub tombstones: HashMap<String, String>,
    pub watermarks: HashMap<String, String>,
    pub edges: BTreeMap<EdgeKey, Edge>,
    pub join_versions: HashMap<String, String>,
    /// Join edge id -> join-table relation (for truncate).
    pub join_relations: HashMap<String, String>,
    pub pending: BTreeMap<EdgeKey, Pending>,
    pub vector_tasks: BTreeMap<String, VectorTask>,
    /// Count of relationships on a node that this pipeline does not own
    /// (simulates relationships created by other applications).
    pub foreign_rels: HashMap<String, u32>,
}

/// Shared in-memory graph. Clone it to share between engine instances.
#[derive(Debug, Clone, Default)]
pub struct MemoryGraph {
    inner: Arc<Mutex<HashMap<String, PipelineState>>>,
    /// Test hook: fail the next N applies with a retryable error.
    fail_next: Arc<Mutex<u32>>,
}

impl MemoryGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of a pipeline's state (for assertions).
    pub fn state(&self, pipeline: &str) -> PipelineState {
        self.inner.lock().expect("poisoned").get(pipeline).cloned().unwrap_or_default()
    }

    /// Record a relationship owned by another application on `node_id`.
    pub fn add_foreign_relationship(&self, pipeline: &str, node_id: &str) {
        let mut g = self.inner.lock().expect("poisoned");
        *g.entry(pipeline.to_owned()).or_default().foreign_rels.entry(node_id.to_owned()).or_default() += 1;
    }

    /// Make the next `n` apply calls fail with a retryable graph error.
    pub fn fail_next_applies(&self, n: u32) {
        *self.fail_next.lock().expect("poisoned") = n;
    }
}

fn node_matches(n: &Node, id: &str, r: &EdgeRef) -> bool {
    !n.deleted && if r.property == "_d2g_id" { id == r.digest } else { n.key_props.get(&r.property) == Some(&r.digest) }
}

fn find_node<'a>(s: &'a PipelineState, r: &'a EdgeRef) -> Option<&'a str> {
    if r.property == "_d2g_id" {
        return s.nodes.get(&r.digest).filter(|n| !n.deleted).map(|_| r.digest.as_str());
    }
    s.nodes.iter().find(|(id, n)| node_matches(n, id, r)).map(|(id, _)| id.as_str())
}

fn floor(s: &PipelineState, id: &str, relation: &str) -> Option<String> {
    let cur = s.nodes.get(id).filter(|n| !n.deleted).map(|n| n.version.clone());
    [cur, s.tombstones.get(id).cloned(), s.watermarks.get(relation).cloned()].into_iter().flatten().max()
}

fn upsert(s: &mut PipelineState, u: &EntityUpsert) -> Result<()> {
    let existing = s.nodes.get(&u.id).filter(|n| !n.deleted);
    if let Some(n) = existing
        && n.version == u.version
    {
        if n.digest != u.digest {
            return Err(Error::new(
                ErrorKind::ConsistencyIncident,
                format!("entity {} at version {} seen with different content", u.id, u.version),
            ));
        }
        return Ok(());
    }
    if let Some(f) = floor(s, &u.id, &u.relation.0)
        && u.version <= f
    {
        return Ok(()); // stale
    }
    let mut node = match s.nodes.get(&u.id) {
        Some(n) if !n.deleted => n.clone(),
        _ => Node {
            relation: u.relation.0.clone(),
            labels: u.labels.clone(),
            tenant: u.tenant.clone(),
            version: String::new(),
            digest: String::new(),
            props: BTreeMap::new(),
            key_props: BTreeMap::new(),
            nulls: Vec::new(),
            incomplete: u.incomplete.clone(),
            labels_json: String::new(),
            deleted: false,
        },
    };
    for (k, v) in &u.set {
        node.props.insert(k.clone(), v.clone());
    }
    for k in &u.remove {
        node.props.remove(k);
    }
    node.nulls.retain(|f| !u.set.contains_key(f) && !u.remove.contains(f));
    node.nulls.extend(u.nulls.iter().cloned());
    node.nulls.sort();
    node.nulls.dedup();
    node.incomplete.retain(|f| !u.set.contains_key(f) && !u.remove.contains(f));
    node.incomplete.sort();
    node.incomplete.dedup();
    node.key_props = u.key_props.clone();
    node.version = u.version.clone();
    node.digest = u.digest.clone();
    node.labels = u.labels.clone();
    node.labels_json = u.labels_json.clone();
    node.tenant = u.tenant.clone();
    node.deleted = false;
    s.tombstones.remove(&u.id);
    s.nodes.insert(u.id.clone(), node);
    resolve_pending(s, &u.id);
    if u.vector {
        set_vector_task(s, &u.id, &u.relation.0, &u.version, false);
    }
    Ok(())
}

fn set_vector_task(s: &mut PipelineState, id: &str, relation: &str, version: &str, delete: bool) {
    let newer = s.vector_tasks.get(id).is_none_or(|t| t.version.as_str() < version);
    if newer {
        s.vector_tasks.insert(
            id.to_owned(),
            VectorTask { entity_id: id.to_owned(), relation: relation.to_owned(), version: version.to_owned(), delete },
        );
    }
}

fn resolve_pending(s: &mut PipelineState, _new_id: &str) {
    let keys: Vec<EdgeKey> = s.pending.keys().cloned().collect();
    for k in keys {
        let p = s.pending[&k].clone();
        if let (Some(from), Some(to)) = (find_node(s, &p.from_ref).map(String::from), find_node(s, &p.to_ref).map(String::from)) {
            s.pending.remove(&k);
            s.edges
                .insert(k, Edge { rel_type: p.rel_type, from_id: from, to_id: to, from_ref: p.from_ref, to_ref: p.to_ref, props: p.props });
        }
    }
}

/// Remove a node's owned relationships; incoming ones become pending again.
fn detach_owned(s: &mut PipelineState, id: &str) {
    let keys: Vec<EdgeKey> = s.edges.iter().filter(|(_, e)| e.from_id == id || e.to_id == id).map(|(k, _)| k.clone()).collect();
    for k in keys {
        let e = s.edges.remove(&k).expect("present");
        let is_owner = matches!(&k, EdgeKey::Fk { from_id, .. } if from_id == id);
        if !is_owner {
            s.pending.insert(k, Pending { rel_type: e.rel_type, from_ref: e.from_ref, to_ref: e.to_ref, props: e.props });
        }
    }
    s.pending.retain(|k, _| !matches!(k, EdgeKey::Fk { from_id, .. } if from_id == id));
}

fn remove_node(s: &mut PipelineState, id: &str) {
    detach_owned(s, id);
    if s.foreign_rels.get(id).copied().unwrap_or(0) > 0 {
        if let Some(n) = s.nodes.get_mut(id) {
            n.deleted = true;
            n.props.clear();
            n.labels.clear();
            n.key_props.clear();
        }
    } else {
        s.nodes.remove(id);
    }
}

fn delete(s: &mut PipelineState, id: &str, relation: &str, version: &str, vector: bool) {
    if floor(s, id, relation).is_some_and(|f| version <= f.as_str()) {
        return;
    }
    if s.nodes.contains_key(id) {
        remove_node(s, id);
    }
    s.tombstones.insert(id.to_owned(), version.to_owned());
    if vector {
        set_vector_task(s, id, relation, version, true);
    }
}

fn set_edge(s: &mut PipelineState, from_id: &str, constraint: &str, rel_type: &str, target: &Option<EdgeRef>, version: &str) {
    match s.nodes.get(from_id) {
        Some(n) if !n.deleted && n.version == version => {}
        _ => return, // owning upsert was stale or entity absent
    }
    let key = EdgeKey::Fk { from_id: from_id.to_owned(), constraint: constraint.to_owned() };
    s.edges.remove(&key);
    s.pending.remove(&key);
    if let Some(t) = target {
        let from_ref = EdgeRef { property: "_d2g_id".into(), digest: from_id.to_owned() };
        match find_node(s, t).map(String::from) {
            Some(to) => {
                s.edges.insert(
                    key,
                    Edge {
                        rel_type: rel_type.to_owned(),
                        from_id: from_id.to_owned(),
                        to_id: to,
                        from_ref,
                        to_ref: t.clone(),
                        props: BTreeMap::new(),
                    },
                );
            }
            None => {
                s.pending.insert(key, Pending { rel_type: rel_type.to_owned(), from_ref, to_ref: t.clone(), props: BTreeMap::new() });
            }
        }
    }
}

fn apply_op(s: &mut PipelineState, op: &GraphOp) -> Result<()> {
    match op {
        GraphOp::UpsertEntity(u) => upsert(s, u)?,
        GraphOp::DeleteEntity { id, relation, version, vector, .. } => delete(s, id, &relation.0, version, *vector),
        GraphOp::SetEdge { from_id, constraint, rel_type, target, version } => set_edge(s, from_id, constraint, rel_type, target, version),
        GraphOp::UpsertJoinEdge { id, relation, rel_type, from, to, props, version } => {
            let cur = [s.join_versions.get(id).cloned(), s.tombstones.get(id).cloned(), s.watermarks.get(&relation.0).cloned()]
                .into_iter()
                .flatten()
                .max();
            if cur.is_some_and(|c| version.as_str() <= c.as_str()) {
                return Ok(());
            }
            let key = EdgeKey::Join { id: id.clone() };
            s.edges.remove(&key);
            s.pending.remove(&key);
            s.tombstones.remove(id);
            s.join_versions.insert(id.clone(), version.clone());
            s.join_relations.insert(id.clone(), relation.0.clone());
            s.pending.insert(key, Pending { rel_type: rel_type.clone(), from_ref: from.clone(), to_ref: to.clone(), props: props.clone() });
            resolve_pending(s, id);
        }
        GraphOp::DeleteJoinEdge { id, relation, version } => {
            let cur = [s.join_versions.get(id).cloned(), s.tombstones.get(id).cloned(), s.watermarks.get(&relation.0).cloned()]
                .into_iter()
                .flatten()
                .max();
            if cur.is_some_and(|c| version.as_str() <= c.as_str()) {
                return Ok(());
            }
            let key = EdgeKey::Join { id: id.clone() };
            s.edges.remove(&key);
            s.pending.remove(&key);
            s.join_versions.remove(id);
            s.join_relations.remove(id);
            s.tombstones.insert(id.clone(), version.clone());
        }
        GraphOp::TruncateJoinRelation { relation, version } => {
            if s.watermarks.get(&relation.0).is_some_and(|w| version.as_str() <= w.as_str()) {
                return Ok(());
            }
            s.watermarks.insert(relation.0.clone(), version.clone());
            let ids: Vec<String> = s
                .join_relations
                .iter()
                .filter(|(id, r)| **r == relation.0 && s.join_versions.get(*id).is_some_and(|v| v.as_str() < version.as_str()))
                .map(|(id, _)| id.clone())
                .collect();
            for id in ids {
                let key = EdgeKey::Join { id: id.clone() };
                s.edges.remove(&key);
                s.pending.remove(&key);
                s.join_versions.remove(&id);
                s.join_relations.remove(&id);
            }
        }
        GraphOp::TruncateRelation { relation, version, vector, .. } => {
            if s.watermarks.get(&relation.0).is_some_and(|w| version.as_str() <= w.as_str()) {
                return Ok(());
            }
            s.watermarks.insert(relation.0.clone(), version.clone());
            let ids: Vec<String> = s
                .nodes
                .iter()
                .filter(|(_, n)| n.relation == relation.0 && !n.deleted && n.version.as_str() < version.as_str())
                .map(|(id, _)| id.clone())
                .collect();
            for id in ids {
                remove_node(s, &id);
                if *vector {
                    set_vector_task(s, &id, &relation.0, version, true);
                }
            }
        }
    }
    Ok(())
}

#[async_trait]
impl GraphSink for MemoryGraph {
    fn capabilities(&self) -> GraphSinkCapabilities {
        GraphSinkCapabilities {
            sink: "memory".into(),
            atomic_batches: true,
            receipts_in_target: true,
            guarded_versions: true,
            fencing: true,
            tombstones: true,
            atomic_vector_outbox: true,
            qualified: true,
            notes: vec!["not durable: contents are lost when the process exits".into()],
        }
    }

    async fn preflight(&self, _plan: Option<&MappingPlan>) -> Result<PreflightReport> {
        let mut r = PreflightReport::new("graph:memory");
        r.push("durability", CheckStatus::Warn, "in-memory sink is not durable");
        Ok(r)
    }

    async fn bootstrap(&self, _plan: &MappingPlan) -> Result<()> {
        Ok(())
    }

    async fn acquire_fence(&self, pipeline: &str) -> Result<(i64, PipelineMarker)> {
        let mut g = self.inner.lock().expect("poisoned");
        let s = g.entry(pipeline.to_owned()).or_default();
        let before = s.marker.clone();
        s.marker.fence_token += 1;
        Ok((s.marker.fence_token, before))
    }

    async fn marker(&self, pipeline: &str) -> Result<Option<PipelineMarker>> {
        Ok(self.inner.lock().expect("poisoned").get(pipeline).map(|s| s.marker.clone()))
    }

    async fn apply(&self, ctx: &ApplyContext, batch: &[GraphTx]) -> Result<Vec<ApplyOutcome>> {
        {
            let mut f = self.fail_next.lock().expect("poisoned");
            if *f > 0 {
                *f -= 1;
                return Err(Error::graph("injected transient graph failure"));
            }
        }
        let mut g = self.inner.lock().expect("poisoned");
        let current = g.entry(ctx.pipeline.clone()).or_default();
        if current.marker.fence_token != ctx.fence_token {
            return Err(Error::new(
                ErrorKind::FencedOut,
                format!("fence token {} superseded by {}", ctx.fence_token, current.marker.fence_token),
            ));
        }
        let mut s = current.clone();
        let mut out = Vec::with_capacity(batch.len());
        for tx in batch {
            match s.receipts.get(&tx.unit_id) {
                Some(d) if *d == tx.content_digest => {
                    out.push(ApplyOutcome { unit_id: tx.unit_id.clone(), status: ApplyStatus::Duplicate });
                    continue;
                }
                Some(d) => {
                    return Err(Error::new(
                        ErrorKind::ConsistencyIncident,
                        format!("receipt mismatch for unit {}: target has {d}, replay has {}", tx.unit_id, tx.content_digest),
                    ));
                }
                None => {}
            }
            for op in &tx.ops {
                apply_op(&mut s, op)?;
            }
            s.receipts.insert(tx.unit_id.clone(), tx.content_digest.clone());
            s.marker.last_unit = Some(tx.unit_id.clone());
            s.marker.last_version = Some(tx.version.clone()).max(s.marker.last_version.clone());
            out.push(ApplyOutcome { unit_id: tx.unit_id.clone(), status: ApplyStatus::Applied });
        }
        s.marker.state_epoch = s.marker.state_epoch.max(ctx.state_epoch);
        *current = s;
        Ok(out)
    }

    async fn set_active_generation(&self, ctx: &ApplyContext, generation: u64) -> Result<()> {
        let mut g = self.inner.lock().expect("poisoned");
        let s = g.entry(ctx.pipeline.clone()).or_default();
        if s.marker.fence_token != ctx.fence_token {
            return Err(Error::new(ErrorKind::FencedOut, "fence token superseded"));
        }
        s.marker.active_generation = Some(generation);
        Ok(())
    }

    async fn counts(&self, pipeline: &str) -> Result<BTreeMap<String, u64>> {
        let s = self.state(pipeline);
        let mut m = BTreeMap::new();
        for n in s.nodes.values().filter(|n| !n.deleted) {
            *m.entry(n.relation.clone()).or_default() += 1;
        }
        Ok(m)
    }

    async fn lookup(&self, pipeline: &str, ids: &[String]) -> Result<Vec<EntityState>> {
        let g = self.inner.lock().expect("poisoned");
        let Some(s) = g.get(pipeline) else { return Ok(vec![]) };
        Ok(ids
            .iter()
            .filter_map(|id| {
                s.nodes.get(id).map(|n| EntityState {
                    id: id.clone(),
                    relation: n.relation.clone(),
                    tenant: n.tenant.clone(),
                    version: n.version.clone(),
                    deleted: n.deleted,
                    labels_json: n.labels_json.clone(),
                    properties: n.props.clone(),
                })
            })
            .collect())
    }

    async fn pending_vector_tasks(&self, pipeline: &str, limit: usize) -> Result<Vec<VectorTask>> {
        Ok(self.state(pipeline).vector_tasks.values().take(limit).cloned().collect())
    }

    async fn vector_backlog(&self, pipeline: &str) -> Result<u64> {
        Ok(self.state(pipeline).vector_tasks.len() as u64)
    }

    async fn complete_vector_task(&self, pipeline: &str, task: &VectorTask) -> Result<bool> {
        let mut g = self.inner.lock().expect("poisoned");
        let Some(s) = g.get_mut(pipeline) else { return Ok(false) };
        match s.vector_tasks.get(&task.entity_id) {
            Some(t) if t.version == task.version && t.delete == task.delete => {
                s.vector_tasks.remove(&task.entity_id);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn pending_edges(&self, pipeline: &str) -> Result<u64> {
        Ok(self.state(pipeline).pending.len() as u64)
    }

    async fn outgoing(&self, pipeline: &str, id: &str) -> Result<Vec<(String, String)>> {
        let mut v: Vec<(String, String)> =
            self.state(pipeline).edges.values().filter(|e| e.from_id == id).map(|e| (e.rel_type.clone(), e.to_id.clone())).collect();
        v.sort();
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn memory_sink_conforms() {
        let g = super::MemoryGraph::new();
        super::super::conformance::run_all(&g, "conf").await.unwrap();
    }
}
