//! Graph sink conformance suite.
//!
//! The executable form of the sink contract (see the module docs of
//! [`crate::sink`]). Run it against a sink with
//! [`run_all`]; every check uses its own pipeline id derived from `prefix`,
//! so it can run against a shared target. A custom sink that passes this
//! suite in a qualification run may be reported as qualified.

use std::collections::BTreeMap;

use super::{ApplyContext, ApplyStatus, GraphSink};
use crate::error::{Error, ErrorKind, Result};
use crate::identity::{IdentityScope, entity_id, key_digest};
use crate::plan::{EdgeRef, EntityUpsert, GraphOp, GraphTx};
use crate::types::{RelationName, Value, Version};

fn v(pos: u64, ord: u32) -> String {
    Version { epoch: 0, position: pos, ordinal: ord }.encode()
}

fn id(rel: &str, k: i64) -> String {
    entity_id(IdentityScope { tenant: "t", source_id: "conf", relation: rel }, &[&Value::Int(k)])
}

fn upsert(rel: &str, k: i64, version: &str, props: &[(&str, Value)]) -> GraphOp {
    let set: BTreeMap<String, Value> = props.iter().map(|(a, b)| (a.to_string(), b.clone())).collect();
    let eid = id(rel, k);
    let digest = crate::identity::content_digest(&[eid.as_bytes(), version.as_bytes(), &serde_json::to_vec(&set).unwrap()]);
    GraphOp::UpsertEntity(EntityUpsert {
        id: eid,
        relation: RelationName(rel.into()),
        labels: vec![label(rel)],
        version: version.into(),
        set,
        remove: vec![],
        nulls: vec![],
        incomplete: vec![],
        key_props: BTreeMap::new(),
        tenant: "t".into(),
        labels_json: "{}".into(),
        vector: true,
        digest,
    })
}

fn label(rel: &str) -> String {
    if rel.ends_with("customers") { "ConfCustomer".into() } else { "ConfOrder".into() }
}

fn delete(rel: &str, k: i64, version: &str) -> GraphOp {
    GraphOp::DeleteEntity {
        id: id(rel, k),
        relation: RelationName(rel.into()),
        labels: vec![label(rel)],
        version: version.into(),
        vector: true,
    }
}

fn edge(from: i64, to: Option<i64>, version: &str) -> GraphOp {
    GraphOp::SetEdge {
        from_id: id("c.orders", from),
        constraint: "orders_customer_fk".into(),
        rel_type: "PLACED_BY".into(),
        target: to.map(|t| EdgeRef { property: "_d2g_id".into(), digest: id("c.customers", t) }),
        version: version.into(),
    }
}

fn tx(unit: &str, ops: Vec<GraphOp>) -> GraphTx {
    let bytes = serde_json::to_vec(&ops).unwrap();
    GraphTx { unit_id: unit.into(), content_digest: crate::identity::content_digest(&[unit.as_bytes(), &bytes]), version: v(0, 0), ops }
}

struct Ctx<'a> {
    sink: &'a dyn GraphSink,
    ctx: ApplyContext,
}

impl<'a> Ctx<'a> {
    async fn new(sink: &'a dyn GraphSink, pipeline: String) -> Result<Ctx<'a>> {
        let (token, _) = sink.acquire_fence(&pipeline).await?;
        Ok(Ctx { sink, ctx: ApplyContext { pipeline, fence_token: token, state_epoch: 1 } })
    }
    async fn apply(&self, txs: Vec<GraphTx>) -> Result<Vec<ApplyStatus>> {
        Ok(self.sink.apply(&self.ctx, &txs).await?.into_iter().map(|o| o.status).collect())
    }
    async fn prop(&self, rel: &str, k: i64, p: &str) -> Result<Option<Value>> {
        let e = self.sink.lookup(&self.ctx.pipeline, &[id(rel, k)]).await?;
        Ok(e.into_iter().find(|e| !e.deleted).and_then(|e| e.properties.get(p).cloned()))
    }
    async fn exists(&self, rel: &str, k: i64) -> Result<bool> {
        Ok(self.sink.lookup(&self.ctx.pipeline, &[id(rel, k)]).await?.iter().any(|e| !e.deleted))
    }
    async fn out(&self, k: i64) -> Result<Vec<(String, String)>> {
        self.sink.outgoing(&self.ctx.pipeline, &id("c.orders", k)).await
    }
}

fn check(cond: bool, what: &str) -> Result<()> {
    if cond { Ok(()) } else { Err(Error::new(ErrorKind::Internal, format!("conformance: {what}"))) }
}

/// Run every conformance check. `prefix` namespaces pipeline ids.
pub async fn run_all(sink: &dyn GraphSink, prefix: &str) -> Result<()> {
    duplicate_replay(sink, &format!("{prefix}-dup")).await?;
    receipt_mismatch_is_incident_and_atomic(sink, &format!("{prefix}-mismatch")).await?;
    stale_and_tombstones(sink, &format!("{prefix}-stale")).await?;
    same_version_different_content(sink, &format!("{prefix}-samever")).await?;
    relationships(sink, &format!("{prefix}-rel")).await?;
    unique_key_and_join_edges(sink, &format!("{prefix}-uk")).await?;
    truncate_watermark(sink, &format!("{prefix}-trunc")).await?;
    fencing(sink, &format!("{prefix}-fence")).await?;
    vector_outbox(sink, &format!("{prefix}-vec")).await?;
    Ok(())
}

async fn duplicate_replay(sink: &dyn GraphSink, p: &str) -> Result<()> {
    let c = Ctx::new(sink, p.into()).await?;
    let t = tx("u1", vec![upsert("c.customers", 1, &v(10, 1), &[("name", Value::Text("Ada".into()))])]);
    check(c.apply(vec![t.clone()]).await? == vec![ApplyStatus::Applied], "first apply")?;
    check(c.apply(vec![t]).await? == vec![ApplyStatus::Duplicate], "replay is duplicate")?;
    check(c.prop("c.customers", 1, "name").await? == Some(Value::Text("Ada".into())), "value kept")?;
    Ok(())
}

async fn receipt_mismatch_is_incident_and_atomic(sink: &dyn GraphSink, p: &str) -> Result<()> {
    let c = Ctx::new(sink, p.into()).await?;
    c.apply(vec![tx("u1", vec![upsert("c.customers", 1, &v(10, 1), &[("name", Value::Text("A".into()))])])]).await?;
    let ok = tx("u2", vec![upsert("c.customers", 2, &v(11, 1), &[])]);
    let bad = tx("u1", vec![upsert("c.customers", 1, &v(10, 1), &[("name", Value::Text("B".into()))])]);
    let e = c.apply(vec![ok, bad]).await.err().ok_or_else(|| Error::internal("conformance: mismatch accepted"))?;
    check(e.kind() == ErrorKind::ConsistencyIncident, "mismatch kind")?;
    check(!c.exists("c.customers", 2).await?, "batch rolled back atomically")?;
    Ok(())
}

async fn stale_and_tombstones(sink: &dyn GraphSink, p: &str) -> Result<()> {
    let c = Ctx::new(sink, p.into()).await?;
    c.apply(vec![tx("u1", vec![upsert("c.customers", 1, &v(20, 1), &[("n", Value::Int(2))])])]).await?;
    c.apply(vec![tx("u0", vec![upsert("c.customers", 1, &v(10, 1), &[("n", Value::Int(1))])])]).await?;
    check(c.prop("c.customers", 1, "n").await? == Some(Value::Int(2)), "stale update ignored")?;
    c.apply(vec![tx("u2", vec![delete("c.customers", 1, &v(30, 1))])]).await?;
    check(!c.exists("c.customers", 1).await?, "deleted")?;
    c.apply(vec![tx("u1b", vec![upsert("c.customers", 1, &v(25, 1), &[("n", Value::Int(9))])])]).await?;
    check(!c.exists("c.customers", 1).await?, "tombstone blocks stale resurrection")?;
    c.apply(vec![tx("u3", vec![upsert("c.customers", 1, &v(40, 1), &[("n", Value::Int(4))])])]).await?;
    check(c.prop("c.customers", 1, "n").await? == Some(Value::Int(4)), "newer insert recreates")?;
    Ok(())
}

async fn same_version_different_content(sink: &dyn GraphSink, p: &str) -> Result<()> {
    let c = Ctx::new(sink, p.into()).await?;
    c.apply(vec![tx("a", vec![upsert("c.customers", 1, &v(10, 1), &[("n", Value::Int(1))])])]).await?;
    let e = c.apply(vec![tx("b", vec![upsert("c.customers", 1, &v(10, 1), &[("n", Value::Int(2))])])]).await;
    check(matches!(e, Err(ref e) if e.kind() == ErrorKind::ConsistencyIncident), "same version, different content")?;
    Ok(())
}

async fn relationships(sink: &dyn GraphSink, p: &str) -> Result<()> {
    let c = Ctx::new(sink, p.into()).await?;
    // Order before its customer exists: relationship is retained as pending.
    c.apply(vec![tx("o1", vec![upsert("c.orders", 1, &v(10, 1), &[]), edge(1, Some(7), &v(10, 1))])]).await?;
    check(c.out(1).await?.is_empty(), "no edge without endpoint")?;
    check(sink.pending_edges(p).await? == 1, "pending retained")?;
    c.apply(vec![tx("c7", vec![upsert("c.customers", 7, &v(11, 1), &[])])]).await?;
    check(c.out(1).await? == vec![("PLACED_BY".into(), id("c.customers", 7))], "pending resolved on endpoint arrival")?;
    check(sink.pending_edges(p).await? == 0, "pending cleared")?;
    // FK change removes the obsolete relationship.
    c.apply(vec![tx("c8", vec![upsert("c.customers", 8, &v(12, 1), &[])])]).await?;
    c.apply(vec![tx("o1b", vec![upsert("c.orders", 1, &v(13, 1), &[("x", Value::Int(1))]), edge(1, Some(8), &v(13, 1))])]).await?;
    check(c.out(1).await? == vec![("PLACED_BY".into(), id("c.customers", 8))], "FK change replaces edge")?;
    // Stale edge op (owning upsert stale) has no effect.
    c.apply(vec![tx("o1old", vec![upsert("c.orders", 1, &v(12, 5), &[]), edge(1, Some(7), &v(12, 5))])]).await?;
    check(c.out(1).await? == vec![("PLACED_BY".into(), id("c.customers", 8))], "stale edge ignored")?;
    // Deleting the target turns the relationship back into pending work.
    c.apply(vec![tx("d8", vec![delete("c.customers", 8, &v(14, 1))])]).await?;
    check(c.out(1).await?.is_empty() && sink.pending_edges(p).await? == 1, "target delete -> pending")?;
    // FK set to NULL removes the relationship and the pending record.
    c.apply(vec![tx("o1n", vec![upsert("c.orders", 1, &v(15, 1), &[("x", Value::Int(2))]), edge(1, None, &v(15, 1))])]).await?;
    check(sink.pending_edges(p).await? == 0, "NULL FK clears pending")?;
    // Self reference (cycle) works.
    c.apply(vec![tx(
        "self",
        vec![
            upsert("c.orders", 5, &v(16, 1), &[]),
            GraphOp::SetEdge {
                from_id: id("c.orders", 5),
                constraint: "orders_parent_fk".into(),
                rel_type: "PARENT".into(),
                target: Some(EdgeRef { property: "_d2g_id".into(), digest: id("c.orders", 5) }),
                version: v(16, 1),
            },
        ],
    )])
    .await?;
    check(c.out(5).await? == vec![("PARENT".into(), id("c.orders", 5))], "self reference")?;
    Ok(())
}

async fn unique_key_and_join_edges(sink: &dyn GraphSink, p: &str) -> Result<()> {
    let c = Ctx::new(sink, p.into()).await?;
    let email = Value::Text("a@example.test".into());
    let uk = key_digest(IdentityScope { tenant: "t", source_id: "conf", relation: "c.customers" }, "customers_email_key", &[&email]);
    let mut cust = upsert("c.customers", 3, &v(10, 1), &[("email", email.clone())]);
    if let GraphOp::UpsertEntity(u) = &mut cust {
        u.key_props.insert("_d2g_uk_customers_email_key".into(), uk.clone());
    }
    c.apply(vec![tx(
        "uk",
        vec![
            cust,
            upsert("c.orders", 9, &v(10, 2), &[]),
            GraphOp::SetEdge {
                from_id: id("c.orders", 9),
                constraint: "orders_email_fk".into(),
                rel_type: "BILLED_TO".into(),
                target: Some(EdgeRef { property: "_d2g_uk_customers_email_key".into(), digest: uk }),
                version: v(10, 2),
            },
        ],
    )])
    .await?;
    check(c.out(9).await? == vec![("BILLED_TO".into(), id("c.customers", 3))], "unique-key reference")?;
    // Join-table edge whose end node does not exist yet.
    let join = |ver: String| GraphOp::UpsertJoinEdge {
        id: id("c.order_tags", 1),
        relation: RelationName("c.order_tags".into()),
        rel_type: "TAGGED".into(),
        from: EdgeRef { property: "_d2g_id".into(), digest: id("c.orders", 9) },
        to: EdgeRef { property: "_d2g_id".into(), digest: id("c.customers", 4) },
        props: BTreeMap::new(),
        version: ver,
    };
    c.apply(vec![tx("j1", vec![join(v(11, 1))])]).await?;
    check(sink.pending_edges(p).await? == 1, "join edge pending")?;
    c.apply(vec![tx("c4", vec![upsert("c.customers", 4, &v(12, 1), &[])])]).await?;
    let out = c.out(9).await?;
    check(out.contains(&("TAGGED".into(), id("c.customers", 4))), "join edge resolved")?;
    c.apply(vec![tx(
        "j1d",
        vec![GraphOp::DeleteJoinEdge { id: id("c.order_tags", 1), relation: RelationName("c.order_tags".into()), version: v(13, 1) }],
    )])
    .await?;
    check(!c.out(9).await?.iter().any(|(t, _)| t == "TAGGED"), "join edge deleted")?;
    c.apply(vec![tx("j1old", vec![join(v(12, 9))])]).await?;
    check(!c.out(9).await?.iter().any(|(t, _)| t == "TAGGED"), "stale join edge ignored")?;
    // Join-table truncate removes join relationships and blocks stale ones.
    let join2 = |ver: String| GraphOp::UpsertJoinEdge {
        id: id("c.order_tags", 2),
        relation: RelationName("c.order_tags".into()),
        rel_type: "TAGGED".into(),
        from: EdgeRef { property: "_d2g_id".into(), digest: id("c.orders", 9) },
        to: EdgeRef { property: "_d2g_id".into(), digest: id("c.customers", 4) },
        props: BTreeMap::new(),
        version: ver,
    };
    c.apply(vec![tx("j2", vec![join2(v(14, 1))])]).await?;
    check(c.out(9).await?.iter().any(|(t, _)| t == "TAGGED"), "second join edge created")?;
    c.apply(vec![tx("jt", vec![GraphOp::TruncateJoinRelation { relation: RelationName("c.order_tags".into()), version: v(15, 1) }])])
        .await?;
    check(!c.out(9).await?.iter().any(|(t, _)| t == "TAGGED"), "join truncate removed relationships")?;
    c.apply(vec![tx("j2old", vec![join2(v(14, 5))])]).await?;
    check(!c.out(9).await?.iter().any(|(t, _)| t == "TAGGED"), "watermark blocks stale join edge")?;
    c.apply(vec![tx("j2new", vec![join2(v(16, 1))])]).await?;
    check(c.out(9).await?.iter().any(|(t, _)| t == "TAGGED"), "post-truncate join edge")?;
    Ok(())
}

async fn truncate_watermark(sink: &dyn GraphSink, p: &str) -> Result<()> {
    let c = Ctx::new(sink, p.into()).await?;
    c.apply(vec![tx("a", vec![upsert("c.customers", 1, &v(10, 1), &[]), upsert("c.customers", 2, &v(10, 2), &[])])]).await?;
    c.apply(vec![tx(
        "t",
        vec![GraphOp::TruncateRelation {
            relation: RelationName("c.customers".into()),
            labels: vec!["ConfCustomer".into()],
            version: v(20, 1),
            vector: true,
        }],
    )])
    .await?;
    check(!c.exists("c.customers", 1).await? && !c.exists("c.customers", 2).await?, "truncated")?;
    c.apply(vec![tx("stale", vec![upsert("c.customers", 1, &v(15, 1), &[])])]).await?;
    check(!c.exists("c.customers", 1).await?, "watermark blocks stale")?;
    c.apply(vec![tx("new", vec![upsert("c.customers", 1, &v(21, 1), &[])])]).await?;
    check(c.exists("c.customers", 1).await?, "post-truncate insert")?;
    Ok(())
}

async fn fencing(sink: &dyn GraphSink, p: &str) -> Result<()> {
    let old = Ctx::new(sink, p.into()).await?;
    let new = Ctx::new(sink, p.into()).await?;
    check(new.ctx.fence_token > old.ctx.fence_token, "fence tokens increase")?;
    let e = old.apply(vec![tx("x", vec![upsert("c.customers", 1, &v(1, 1), &[])])]).await;
    check(matches!(e, Err(ref e) if e.kind() == ErrorKind::FencedOut), "stale owner fenced out")?;
    check(!new.exists("c.customers", 1).await?, "fenced write had no effect")?;
    new.apply(vec![tx("y", vec![upsert("c.customers", 1, &v(2, 1), &[])])]).await?;
    let m = sink.marker(p).await?.ok_or_else(|| Error::internal("marker"))?;
    check(m.state_epoch >= 1 && m.last_unit.as_deref() == Some("y"), "marker updated atomically")?;
    Ok(())
}

async fn vector_outbox(sink: &dyn GraphSink, p: &str) -> Result<()> {
    let c = Ctx::new(sink, p.into()).await?;
    c.apply(vec![tx("a", vec![upsert("c.customers", 1, &v(10, 1), &[])])]).await?;
    let tasks = sink.pending_vector_tasks(p, 10).await?;
    check(tasks.len() == 1 && tasks[0].version == v(10, 1) && !tasks[0].delete, "task created with graph change")?;
    let stale = tasks[0].clone();
    c.apply(vec![tx("b", vec![upsert("c.customers", 1, &v(11, 1), &[("n", Value::Int(1))])])]).await?;
    check(!sink.complete_vector_task(p, &stale).await?, "superseded task not completed")?;
    let t = sink.pending_vector_tasks(p, 10).await?;
    check(t.len() == 1 && t[0].version == v(11, 1), "task superseded by newer version")?;
    check(sink.complete_vector_task(p, &t[0]).await?, "current task completes")?;
    check(sink.vector_backlog(p).await? == 0, "backlog drained")?;
    c.apply(vec![tx("c", vec![delete("c.customers", 1, &v(12, 1))])]).await?;
    let t = sink.pending_vector_tasks(p, 10).await?;
    check(t.len() == 1 && t[0].delete, "delete creates vector delete task")?;
    Ok(())
}
