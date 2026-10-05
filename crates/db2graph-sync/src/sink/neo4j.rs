//! Neo4j graph sink (Bolt via `neo4rs`).
//!
//! Every batch runs in one explicit transaction:
//!
//! 1. Write-lock the pipeline node (`SET p._d2g_lock = true REMOVE p._d2g_lock`)
//!    and verify the fence token. Neo4j holds write locks until commit
//!    ("Concurrent data access", Neo4j Operations Manual), so every later read
//!    in the transaction is protected against concurrent writers of this
//!    pipeline: all writers of a pipeline serialize on this lock.
//! 2. Per unit: receipt check, then operations (version-guarded entity
//!    writes, tombstones, truncate watermarks, owned relationships, pending
//!    relationship records, vector outbox), then the receipt.
//! 3. Update the pipeline marker (state epoch evidence) and commit.
//!
//! All values are parameters. Labels and relationship types are interpolated
//! only after validation against `[A-Za-z_][A-Za-z0-9_]{0,63}` and are
//! backtick-quoted.
//!
//! Pipeline-owned metadata uses labels prefixed `_D2G_`; entity nodes carry
//! `_D2G_Entity` plus mapped labels, and a `_d2g_key = "<pipeline>|<id>"`
//! uniqueness constraint created by `bootstrap`.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use async_trait::async_trait;
use neo4rs::{BoltType, Graph, Query, Row, Txn, query};

use super::{ApplyContext, ApplyOutcome, ApplyStatus, EntityState, GraphSink, PipelineMarker, VectorTask};
use crate::capability::{CheckStatus, GraphSinkCapabilities, PreflightReport};
use crate::config::Neo4jConfig;
use crate::error::{Error, ErrorKind, Result};
use crate::mapping::{MappingPlan, valid_graph_ident};
use crate::plan::{EdgeRef, EntityUpsert, GraphOp, GraphTx};
use crate::secret::Secret;
use crate::types::Value;

const CONSTRAINTS: &[(&str, &str)] = &[
    ("d2g_entity_key", "_D2G_Entity"),
    ("d2g_tombstone_key", "_D2G_Tombstone"),
    ("d2g_receipt_key", "_D2G_Receipt"),
    ("d2g_watermark_key", "_D2G_Watermark"),
    ("d2g_pending_key", "_D2G_PendingEdge"),
    ("d2g_vector_task_key", "_D2G_VectorTask"),
    ("d2g_join_state_key", "_D2G_JoinState"),
];

pub struct Neo4jSink {
    graph: Graph,
    db: Option<String>,
}

fn map_err(e: neo4rs::Error) -> Error {
    match &e {
        neo4rs::Error::Neo4j(n) => match n.kind() {
            neo4rs::Neo4jErrorKind::Transient => Error::graph(format!("neo4j transient {}: {}", n.code(), n.message())),
            neo4rs::Neo4jErrorKind::Client(neo4rs::Neo4jClientErrorKind::Security(_)) => {
                Error::new(ErrorKind::Security, format!("neo4j {}: {}", n.code(), n.message()))
            }
            neo4rs::Neo4jErrorKind::Client(neo4rs::Neo4jClientErrorKind::SessionExpired) => {
                Error::graph(format!("neo4j {}: {}", n.code(), n.message()))
            }
            _ => Error::new(ErrorKind::Graph, format!("neo4j {}: {}", n.code(), n.message())),
        },
        neo4rs::Error::AuthenticationError(m) => Error::new(ErrorKind::Security, format!("neo4j authentication failed: {m}")),
        neo4rs::Error::IOError { .. } | neo4rs::Error::ConnectionError => Error::graph(format!("neo4j connection: {e}")),
        _ => Error::graph(format!("neo4j: {e}")),
    }
}

fn ident(s: &str) -> Result<String> {
    if valid_graph_ident(s) {
        Ok(format!("`{s}`"))
    } else {
        Err(Error::new(ErrorKind::Security, format!("refusing unsafe graph identifier `{s}`")))
    }
}

fn bolt(v: &Value) -> BoltType {
    match v {
        Value::Null => Option::<i64>::None.into(),
        Value::Bool(b) => (*b).into(),
        Value::Int(i) => (*i).into(),
        Value::Float(f) => (*f).into(),
        Value::Bytes(b) => b.clone().into(),
        Value::Array(items) => {
            let first = items.first().map(|x| x.type_tag());
            if items.iter().all(|x| Some(x.type_tag()) == first && !matches!(x, Value::Array(_) | Value::Null)) {
                items.iter().map(bolt).collect::<Vec<BoltType>>().into()
            } else {
                serde_json::to_string(items).unwrap_or_default().into()
            }
        }
        Value::Unknown { text, .. } => text.clone().into(),
        Value::BigInt(s)
        | Value::Decimal(s)
        | Value::Text(s)
        | Value::Uuid(s)
        | Value::Date(s)
        | Value::Time(s)
        | Value::Timestamp(s)
        | Value::TimestampTz(s)
        | Value::Json(s) => s.clone().into(),
    }
}

fn from_bolt(b: &BoltType) -> Value {
    match b {
        BoltType::String(s) => Value::Text(s.value.clone()),
        BoltType::Boolean(x) => Value::Bool(x.value),
        BoltType::Integer(i) => Value::Int(i.value),
        BoltType::Float(f) => Value::Float(f.value),
        BoltType::Bytes(x) => Value::Bytes(x.value.to_vec()),
        BoltType::List(l) => Value::Array(l.value.iter().map(from_bolt).collect()),
        BoltType::Null(_) => Value::Null,
        other => Value::Unknown { type_name: "bolt".into(), text: other.to_string() },
    }
}

fn key(pipeline: &str, id: &str) -> String {
    format!("{pipeline}|{id}")
}

fn fk_edge_key(from_id: &str, constraint: &str) -> String {
    format!("fk|{from_id}|{constraint}")
}

fn join_edge_key(id: &str) -> String {
    format!("join|{id}")
}

async fn rows(txn: &mut Txn, q: Query) -> Result<Vec<Row>> {
    let mut s = txn.execute(q).await.map_err(map_err)?;
    let mut out = Vec::new();
    while let Some(r) = s.next(txn.handle()).await.map_err(map_err)? {
        out.push(r);
    }
    Ok(out)
}

fn get<T: serde::de::DeserializeOwned>(r: &Row, k: &str) -> Result<T> {
    r.get::<T>(k).map_err(|e| Error::new(ErrorKind::Graph, format!("neo4j result field `{k}`: {e}")))
}

/// Current guard state of an entity.
struct Guard {
    exists: bool,
    deleted: bool,
    version: Option<String>,
    digest: Option<String>,
    tomb: Option<String>,
    watermark: Option<String>,
    props: Vec<String>,
    nulls: Vec<String>,
    incomplete: Vec<String>,
}

impl Guard {
    fn floor(&self) -> Option<String> {
        let cur = if self.exists && !self.deleted { self.version.clone() } else { None };
        [cur, self.tomb.clone(), self.watermark.clone()].into_iter().flatten().max()
    }
}

impl Neo4jSink {
    pub async fn connect(cfg: Neo4jConfig, password: Secret) -> Result<Self> {
        let mut b = neo4rs::ConfigBuilder::default()
            .uri(&cfg.uri)
            .user(&cfg.user)
            .password(password.expose_str()?)
            .max_connections(cfg.max_connections);
        if let Some(db) = &cfg.database {
            b = b.db(db.as_str());
        }
        if let Some(ca) = &cfg.ca_file {
            b = b.with_client_certificate(ca);
        }
        let config = b.build().map_err(map_err)?;
        let graph = Graph::connect(config).await.map_err(map_err)?;
        Ok(Self { graph, db: cfg.database })
    }

    async fn txn(&self) -> Result<Txn> {
        match &self.db {
            Some(db) => self.graph.start_txn_on(db.as_str()).await.map_err(map_err),
            None => self.graph.start_txn().await.map_err(map_err),
        }
    }

    async fn read(&self, q: Query) -> Result<Vec<Row>> {
        let mut s = match &self.db {
            Some(db) => self.graph.execute_on(db.as_str(), q).await.map_err(map_err)?,
            None => self.graph.execute(q).await.map_err(map_err)?,
        };
        let mut out = Vec::new();
        while let Some(r) = s.next().await.map_err(map_err)? {
            out.push(r);
        }
        Ok(out)
    }

    /// Run `f` inside a transaction; commit on success, roll back otherwise.
    async fn in_txn<T>(&self, f: impl AsyncFnOnce(&mut Txn) -> Result<T>) -> Result<T> {
        let mut txn = self.txn().await?;
        match f(&mut txn).await {
            Ok(v) => {
                txn.commit().await.map_err(map_err)?;
                Ok(v)
            }
            Err(e) => {
                let _ = txn.rollback().await;
                Err(e)
            }
        }
    }
}

async fn lock_and_fence(txn: &mut Txn, ctx: &ApplyContext) -> Result<()> {
    let r = rows(
        txn,
        query("MATCH (p:_D2G_Pipeline {id: $p}) SET p._d2g_lock = true REMOVE p._d2g_lock RETURN p.fence AS fence")
            .param("p", ctx.pipeline.as_str()),
    )
    .await?;
    let fence: i64 = match r.first() {
        Some(row) => get(row, "fence")?,
        None => return Err(Error::new(ErrorKind::FencedOut, "pipeline marker missing; ownership was never acquired")),
    };
    if fence != ctx.fence_token {
        return Err(Error::new(ErrorKind::FencedOut, format!("fence token {} superseded by {fence}", ctx.fence_token)));
    }
    Ok(())
}

async fn guard(txn: &mut Txn, p: &str, id: &str, relation: &str) -> Result<Guard> {
    let r = rows(
        txn,
        query(
            "OPTIONAL MATCH (n:_D2G_Entity {_d2g_key: $k}) \
             OPTIONAL MATCH (t:_D2G_Tombstone {_d2g_key: $k}) \
             OPTIONAL MATCH (w:_D2G_Watermark {_d2g_key: $wk}) \
             RETURN n IS NOT NULL AS exists, coalesce(n._d2g_deleted, false) AS deleted, n._d2g_ver AS ver, \
                    n._d2g_digest AS digest, t.ver AS tver, w.ver AS wver, \
                    coalesce(n._d2g_props, []) AS props, coalesce(n._d2g_nulls, []) AS nulls, \
                    coalesce(n._d2g_incomplete, []) AS incomplete",
        )
        .param("k", key(p, id))
        .param("wk", key(p, relation)),
    )
    .await?;
    let row = r.first().ok_or_else(|| Error::internal("guard query returned no row"))?;
    Ok(Guard {
        exists: get(row, "exists")?,
        deleted: get(row, "deleted")?,
        version: get(row, "ver")?,
        digest: get(row, "digest")?,
        tomb: get(row, "tver")?,
        watermark: get(row, "wver")?,
        props: get(row, "props")?,
        nulls: get(row, "nulls")?,
        incomplete: get(row, "incomplete")?,
    })
}

/// Find a live entity by reference. Returns its `_d2g_key`.
async fn find(txn: &mut Txn, p: &str, r: &EdgeRef) -> Result<Option<String>> {
    let q = if r.property == "_d2g_id" {
        query("MATCH (n:_D2G_Entity {_d2g_key: $k}) WHERE NOT coalesce(n._d2g_deleted, false) RETURN n._d2g_key AS k LIMIT 1")
            .param("k", key(p, &r.digest))
    } else {
        let prop = ident(&r.property)?;
        query(&format!(
            "MATCH (n:_D2G_Entity {{_d2g_pipeline: $p, {prop}: $d}}) WHERE NOT coalesce(n._d2g_deleted, false) \
             RETURN n._d2g_key AS k LIMIT 1"
        ))
        .param("p", p)
        .param("d", r.digest.as_str())
    };
    let r = rows(txn, q).await?;
    r.first().map(|row| get(row, "k")).transpose()
}

struct EdgeSpec<'a> {
    edge_key: String,
    rel_type: &'a str,
    from: EdgeRef,
    to: EdgeRef,
    props: BTreeMap<String, Value>,
    /// Entity id owning the record (FK edges), empty for join edges.
    owner: String,
}

async fn create_edge(txn: &mut Txn, p: &str, e: &EdgeSpec<'_>, a: &str, b: &str) -> Result<()> {
    let t = ident(e.rel_type)?;
    let props: HashMap<String, BoltType> = e.props.iter().map(|(k, v)| (k.clone(), bolt(v))).collect();
    txn.run(
        query(&format!(
            "MATCH (a:_D2G_Entity {{_d2g_key: $a}}), (b:_D2G_Entity {{_d2g_key: $b}}) \
             CREATE (a)-[r:{t}]->(b) SET r += $props, r._d2g_owned = true, r._d2g_pipeline = $p, r._d2g_edge = $ek, \
             r._d2g_from_prop = $fp, r._d2g_from_digest = $fd, r._d2g_to_prop = $tp, r._d2g_to_digest = $td, r._d2g_owner = $owner"
        ))
        .param("a", a)
        .param("b", b)
        .param("props", props)
        .param("p", p)
        .param("ek", e.edge_key.as_str())
        .param("fp", e.from.property.as_str())
        .param("fd", e.from.digest.as_str())
        .param("tp", e.to.property.as_str())
        .param("td", e.to.digest.as_str())
        .param("owner", e.owner.as_str()),
    )
    .await
    .map_err(map_err)
}

async fn put_pending(txn: &mut Txn, p: &str, e: &EdgeSpec<'_>) -> Result<()> {
    txn.run(
        query(
            "MERGE (pe:_D2G_PendingEdge {_d2g_key: $k}) SET pe._d2g_pipeline = $p, pe.edge_key = $ek, pe.rel_type = $t, \
             pe.from_prop = $fp, pe.from_digest = $fd, pe.to_prop = $tp, pe.to_digest = $td, pe.props = $props, pe.owner = $owner",
        )
        .param("k", key(p, &e.edge_key))
        .param("p", p)
        .param("ek", e.edge_key.as_str())
        .param("t", e.rel_type)
        .param("fp", e.from.property.as_str())
        .param("fd", e.from.digest.as_str())
        .param("tp", e.to.property.as_str())
        .param("td", e.to.digest.as_str())
        .param("props", serde_json::to_string(&e.props).unwrap_or_default())
        .param("owner", e.owner.as_str()),
    )
    .await
    .map_err(map_err)
}

/// Create the edge if both endpoints exist, otherwise retain it as pending.
async fn place_edge(txn: &mut Txn, p: &str, e: &EdgeSpec<'_>) -> Result<()> {
    match (find(txn, p, &e.from).await?, find(txn, p, &e.to).await?) {
        (Some(a), Some(b)) => create_edge(txn, p, e, &a, &b).await,
        _ => put_pending(txn, p, e).await,
    }
}

/// Remove the owned edge with `edge_key` (found via its start node) and any
/// pending record for it.
async fn remove_edge(txn: &mut Txn, p: &str, edge_key: &str, from: &EdgeRef) -> Result<()> {
    if let Some(a) = find(txn, p, from).await? {
        txn.run(
            query("MATCH (a:_D2G_Entity {_d2g_key: $a})-[r]->() WHERE r._d2g_edge = $ek AND r._d2g_pipeline = $p DELETE r")
                .param("a", a)
                .param("ek", edge_key)
                .param("p", p),
        )
        .await
        .map_err(map_err)?;
    }
    txn.run(query("MATCH (pe:_D2G_PendingEdge {_d2g_key: $k}) DELETE pe").param("k", key(p, edge_key))).await.map_err(map_err)
}

/// Resolve pending relationships that reference the entity `id`.
async fn resolve_pending(txn: &mut Txn, p: &str, id: &str, key_props: &BTreeMap<String, String>) -> Result<()> {
    let mut refs: Vec<HashMap<String, BoltType>> =
        vec![HashMap::from([("prop".to_owned(), BoltType::from("_d2g_id")), ("d".to_owned(), BoltType::from(id))])];
    for (prop, d) in key_props {
        refs.push(HashMap::from([("prop".to_owned(), BoltType::from(prop.as_str())), ("d".to_owned(), BoltType::from(d.as_str()))]));
    }
    let pending = rows(
        txn,
        query(
            "UNWIND $refs AS ref MATCH (pe:_D2G_PendingEdge {_d2g_pipeline: $p}) \
             WHERE (pe.to_prop = ref.prop AND pe.to_digest = ref.d) OR (pe.from_prop = ref.prop AND pe.from_digest = ref.d) \
             RETURN DISTINCT pe.edge_key AS ek, pe.rel_type AS t, pe.from_prop AS fp, pe.from_digest AS fd, \
                    pe.to_prop AS tp, pe.to_digest AS td, pe.props AS props, pe.owner AS owner",
        )
        .param("refs", refs)
        .param("p", p),
    )
    .await?;
    for row in pending {
        let props: String = get(&row, "props")?;
        let t: String = get(&row, "t")?;
        let spec = EdgeSpec {
            edge_key: get(&row, "ek")?,
            rel_type: &t,
            from: EdgeRef { property: get(&row, "fp")?, digest: get(&row, "fd")? },
            to: EdgeRef { property: get(&row, "tp")?, digest: get(&row, "td")? },
            props: serde_json::from_str(&props).unwrap_or_default(),
            owner: get(&row, "owner")?,
        };
        if let (Some(a), Some(b)) = (find(txn, p, &spec.from).await?, find(txn, p, &spec.to).await?) {
            create_edge(txn, p, &spec, &a, &b).await?;
            txn.run(query("MATCH (pe:_D2G_PendingEdge {_d2g_key: $k}) DELETE pe").param("k", key(p, &spec.edge_key)))
                .await
                .map_err(map_err)?;
        }
    }
    Ok(())
}

async fn set_vector_task(txn: &mut Txn, p: &str, id: &str, relation: &str, version: &str, delete: bool) -> Result<()> {
    txn.run(
        query(
            "MERGE (v:_D2G_VectorTask {_d2g_key: $k}) ON CREATE SET v._d2g_pipeline = $p, v.entity_id = $id \
             WITH v WHERE coalesce(v.ver, '') < $ver SET v.ver = $ver, v.delete = $del, v.relation = $rel",
        )
        .param("k", key(p, id))
        .param("p", p)
        .param("id", id)
        .param("ver", version)
        .param("del", delete)
        .param("rel", relation),
    )
    .await
    .map_err(map_err)
}

/// Remove an entity node's owned relationships and the node itself (or strip
/// it to a placeholder when non-owned relationships exist).
async fn remove_node(txn: &mut Txn, p: &str, id: &str, labels: &[String], props: &[String]) -> Result<()> {
    let k = key(p, id);
    let own_prefix = format!("fk|{id}|");
    let touching = rows(
        txn,
        query(
            "MATCH (n:_D2G_Entity {_d2g_key: $k})-[r]-() WHERE r._d2g_owned = true AND r._d2g_pipeline = $p \
             RETURN DISTINCT elementId(r) AS rid, r._d2g_edge AS ek, type(r) AS t, r._d2g_from_prop AS fp, r._d2g_from_digest AS fd, \
                    r._d2g_to_prop AS tp, r._d2g_to_digest AS td, r._d2g_owner AS owner, properties(r) AS props",
        )
        .param("k", k.as_str())
        .param("p", p),
    )
    .await?;
    for row in touching {
        let ek: String = get(&row, "ek")?;
        let rid: String = get(&row, "rid")?;
        if !ek.starts_with(&own_prefix) {
            // Someone else's reference to this entity: keep it as pending work.
            let t: String = get(&row, "t")?;
            let all: HashMap<String, BoltType> = get(&row, "props")?;
            let props = all.iter().filter(|(k, _)| !k.starts_with("_d2g")).map(|(k, v)| (k.clone(), from_bolt(v))).collect();
            let spec = EdgeSpec {
                edge_key: ek,
                rel_type: &t,
                from: EdgeRef { property: get(&row, "fp")?, digest: get(&row, "fd")? },
                to: EdgeRef { property: get(&row, "tp")?, digest: get(&row, "td")? },
                props,
                owner: get(&row, "owner")?,
            };
            put_pending(txn, p, &spec).await?;
        }
        txn.run(query("MATCH ()-[r]->() WHERE elementId(r) = $rid DELETE r").param("rid", rid)).await.map_err(map_err)?;
    }
    txn.run(query("MATCH (pe:_D2G_PendingEdge {_d2g_pipeline: $p, owner: $id}) DELETE pe").param("p", p).param("id", id))
        .await
        .map_err(map_err)?;
    let foreign: i64 = {
        let r =
            rows(txn, query("MATCH (n:_D2G_Entity {_d2g_key: $k}) OPTIONAL MATCH (n)-[r]-() RETURN count(r) AS c").param("k", k.as_str()))
                .await?;
        r.first().map(|row| get(row, "c")).transpose()?.unwrap_or(0)
    };
    if foreign > 0 {
        let mut remove: HashMap<String, BoltType> = props.iter().map(|k| (k.clone(), Option::<i64>::None.into())).collect();
        for k in ["_d2g_props", "_d2g_nulls", "_d2g_incomplete", "_d2g_labels"] {
            remove.insert(k.to_owned(), Option::<i64>::None.into());
        }
        // Unique-key lookup properties must not resolve to a deleted entity.
        let ks = rows(
            txn,
            query("MATCH (n:_D2G_Entity {_d2g_key: $k}) RETURN [x IN keys(n) WHERE x STARTS WITH '_d2g_uk_'] AS ks").param("k", k.as_str()),
        )
        .await?;
        if let Some(row) = ks.first() {
            for uk in get::<Vec<String>>(row, "ks")? {
                remove.insert(uk, Option::<i64>::None.into());
            }
        }
        let mut label_clause = String::new();
        for l in labels {
            label_clause.push_str(&format!(" REMOVE n:{}", ident(l)?));
        }
        txn.run(
            query(&format!("MATCH (n:_D2G_Entity {{_d2g_key: $k}}) SET n += $rm, n._d2g_deleted = true{label_clause}"))
                .param("k", k.as_str())
                .param("rm", remove),
        )
        .await
        .map_err(map_err)?;
    } else {
        txn.run(query("MATCH (n:_D2G_Entity {_d2g_key: $k}) DELETE n").param("k", k.as_str())).await.map_err(map_err)?;
    }
    Ok(())
}

async fn upsert(txn: &mut Txn, p: &str, u: &EntityUpsert) -> Result<()> {
    let g = guard(txn, p, &u.id, &u.relation.0).await?;
    if g.exists && !g.deleted && g.version.as_deref() == Some(u.version.as_str()) {
        if g.digest.as_deref() != Some(u.digest.as_str()) {
            return Err(Error::new(
                ErrorKind::ConsistencyIncident,
                format!("entity {} at version {} seen with different content", u.id, u.version),
            ));
        }
        return Ok(());
    }
    if g.floor().is_some_and(|f| u.version.as_str() <= f.as_str()) {
        return Ok(()); // stale
    }
    let live = g.exists && !g.deleted;
    let removed: BTreeSet<&String> = u.remove.iter().collect();
    let mut owned: BTreeSet<String> = if live { g.props.into_iter().collect() } else { BTreeSet::new() };
    owned.extend(u.set.keys().cloned());
    owned.retain(|k| !removed.contains(k));
    let mut nulls: BTreeSet<String> = if live { g.nulls.into_iter().collect() } else { BTreeSet::new() };
    nulls.retain(|f| !u.set.contains_key(f) && !removed.contains(f));
    nulls.extend(u.nulls.iter().cloned());
    let mut incomplete: BTreeSet<String> = if live { g.incomplete.into_iter().collect() } else { u.incomplete.iter().cloned().collect() };
    incomplete.retain(|f| !u.set.contains_key(f) && !removed.contains(f));

    let mut set: HashMap<String, BoltType> = u.set.iter().map(|(k, v)| (k.clone(), bolt(v))).collect();
    for k in &u.remove {
        set.insert(k.clone(), Option::<i64>::None.into());
    }
    for (k, v) in &u.key_props {
        set.insert(k.clone(), v.as_str().into());
    }
    let mut labels = String::from(":_D2G_Entity");
    for l in &u.labels {
        labels.push(':');
        labels.push_str(&ident(l)?);
    }
    txn.run(
        query(&format!(
            "MERGE (n:_D2G_Entity {{_d2g_key: $k}}) ON CREATE SET n._d2g_id = $id, n._d2g_pipeline = $p \
             SET n{labels}, n += $set, n._d2g_ver = $ver, n._d2g_digest = $digest, n._d2g_relation = $rel, \
             n._d2g_tenant = $tenant, n._d2g_labels = $lj, n._d2g_props = $owned, n._d2g_nulls = $nulls, \
             n._d2g_incomplete = $incomplete, n._d2g_deleted = false \
             WITH n OPTIONAL MATCH (t:_D2G_Tombstone {{_d2g_key: $k}}) DELETE t"
        ))
        .param("k", key(p, &u.id))
        .param("id", u.id.as_str())
        .param("p", p)
        .param("set", set)
        .param("ver", u.version.as_str())
        .param("digest", u.digest.as_str())
        .param("rel", u.relation.0.as_str())
        .param("tenant", u.tenant.as_str())
        .param("lj", u.labels_json.as_str())
        .param("owned", owned.into_iter().collect::<Vec<_>>())
        .param("nulls", nulls.into_iter().collect::<Vec<_>>())
        .param("incomplete", incomplete.into_iter().collect::<Vec<_>>()),
    )
    .await
    .map_err(map_err)?;
    resolve_pending(txn, p, &u.id, &u.key_props).await?;
    if u.vector {
        set_vector_task(txn, p, &u.id, &u.relation.0, &u.version, false).await?;
    }
    Ok(())
}

async fn apply_op(txn: &mut Txn, p: &str, op: &GraphOp) -> Result<()> {
    match op {
        GraphOp::UpsertEntity(u) => upsert(txn, p, u).await,
        GraphOp::DeleteEntity { id, relation, labels, version, vector } => {
            let g = guard(txn, p, id, &relation.0).await?;
            if g.floor().is_some_and(|f| version.as_str() <= f.as_str()) {
                return Ok(());
            }
            if g.exists && !g.deleted {
                remove_node(txn, p, id, labels, &g.props).await?;
            }
            txn.run(
                query("MERGE (t:_D2G_Tombstone {_d2g_key: $k}) SET t._d2g_pipeline = $p, t.ver = $ver")
                    .param("k", key(p, id))
                    .param("p", p)
                    .param("ver", version.as_str()),
            )
            .await
            .map_err(map_err)?;
            if *vector {
                set_vector_task(txn, p, id, &relation.0, version, true).await?;
            }
            Ok(())
        }
        GraphOp::SetEdge { from_id, constraint, rel_type, target, version } => {
            let r = rows(
                txn,
                query("MATCH (n:_D2G_Entity {_d2g_key: $k}) WHERE NOT coalesce(n._d2g_deleted, false) RETURN n._d2g_ver AS ver")
                    .param("k", key(p, from_id)),
            )
            .await?;
            let current: Option<String> = r.first().map(|row| get::<Option<String>>(row, "ver")).transpose()?.flatten();
            if current.as_deref() != Some(version.as_str()) {
                return Ok(()); // owning upsert was stale
            }
            let from = EdgeRef { property: "_d2g_id".into(), digest: from_id.clone() };
            let ek = fk_edge_key(from_id, constraint);
            remove_edge(txn, p, &ek, &from).await?;
            if let Some(t) = target {
                place_edge(
                    txn,
                    p,
                    &EdgeSpec { edge_key: ek, rel_type, from, to: t.clone(), props: BTreeMap::new(), owner: from_id.clone() },
                )
                .await?;
            }
            Ok(())
        }
        GraphOp::UpsertJoinEdge { id, relation, rel_type, from, to, props, version } => {
            let (cur, prev) = join_state(txn, p, id, &relation.0).await?;
            if cur.is_some_and(|c| version.as_str() <= c.as_str()) {
                return Ok(());
            }
            let ek = join_edge_key(id);
            if let Some(prev_from) = prev {
                remove_edge(txn, p, &ek, &prev_from).await?;
            }
            txn.run(
                query(
                    "MERGE (j:_D2G_JoinState {_d2g_key: $k}) SET j._d2g_pipeline = $p, j.relation = $rel, j.ver = $ver, j.from_prop = $fp, j.from_digest = $fd \
                     WITH j OPTIONAL MATCH (t:_D2G_Tombstone {_d2g_key: $k}) DELETE t",
                )
                .param("k", key(p, id))
                .param("p", p)
                .param("ver", version.as_str())
                .param("rel", relation.0.as_str())
                .param("fp", from.property.as_str())
                .param("fd", from.digest.as_str()),
            )
            .await
            .map_err(map_err)?;
            place_edge(
                txn,
                p,
                &EdgeSpec { edge_key: ek, rel_type, from: from.clone(), to: to.clone(), props: props.clone(), owner: String::new() },
            )
            .await
        }
        GraphOp::DeleteJoinEdge { id, relation, version } => {
            let (cur, prev) = join_state(txn, p, id, &relation.0).await?;
            if cur.is_some_and(|c| version.as_str() <= c.as_str()) {
                return Ok(());
            }
            if let Some(prev_from) = prev {
                remove_edge(txn, p, &join_edge_key(id), &prev_from).await?;
            }
            txn.run(
                query(
                    "OPTIONAL MATCH (j:_D2G_JoinState {_d2g_key: $k}) DELETE j \
                     MERGE (t:_D2G_Tombstone {_d2g_key: $k}) SET t._d2g_pipeline = $p, t.ver = $ver",
                )
                .param("k", key(p, id))
                .param("p", p)
                .param("ver", version.as_str()),
            )
            .await
            .map_err(map_err)
        }
        GraphOp::TruncateJoinRelation { relation, version } => {
            let wk = key(p, &relation.0);
            let r = rows(txn, query("OPTIONAL MATCH (w:_D2G_Watermark {_d2g_key: $k}) RETURN w.ver AS v").param("k", wk.as_str())).await?;
            let cur: Option<String> = r.first().map(|row| get::<Option<String>>(row, "v")).transpose()?.flatten();
            if cur.is_some_and(|c| version.as_str() <= c.as_str()) {
                return Ok(());
            }
            txn.run(
                query("MERGE (w:_D2G_Watermark {_d2g_key: $k}) SET w._d2g_pipeline = $p, w.ver = $ver")
                    .param("k", wk.as_str())
                    .param("p", p)
                    .param("ver", version.as_str()),
            )
            .await
            .map_err(map_err)?;
            let joins = rows(
                txn,
                query(
                    "MATCH (j:_D2G_JoinState {_d2g_pipeline: $p, relation: $rel}) WHERE j.ver < $ver \
                     RETURN j._d2g_key AS k, j.from_prop AS fp, j.from_digest AS fd",
                )
                .param("p", p)
                .param("rel", relation.0.as_str())
                .param("ver", version.as_str()),
            )
            .await?;
            let prefix = format!("{p}|");
            for row in joins {
                let k: String = get(&row, "k")?;
                let jid = k.strip_prefix(&prefix).unwrap_or(&k).to_owned();
                let from = EdgeRef { property: get(&row, "fp")?, digest: get(&row, "fd")? };
                remove_edge(txn, p, &join_edge_key(&jid), &from).await?;
                txn.run(query("MATCH (j:_D2G_JoinState {_d2g_key: $k}) DELETE j").param("k", k.as_str())).await.map_err(map_err)?;
            }
            Ok(())
        }
        GraphOp::TruncateRelation { relation, labels, version, vector } => {
            let wk = key(p, &relation.0);
            let r = rows(txn, query("OPTIONAL MATCH (w:_D2G_Watermark {_d2g_key: $k}) RETURN w.ver AS v").param("k", wk.as_str())).await?;
            let cur: Option<String> = r.first().map(|row| get::<Option<String>>(row, "v")).transpose()?.flatten();
            if cur.is_some_and(|c| version.as_str() <= c.as_str()) {
                return Ok(());
            }
            txn.run(
                query("MERGE (w:_D2G_Watermark {_d2g_key: $k}) SET w._d2g_pipeline = $p, w.ver = $ver")
                    .param("k", wk.as_str())
                    .param("p", p)
                    .param("ver", version.as_str()),
            )
            .await
            .map_err(map_err)?;
            let victims = rows(
                txn,
                query(
                    "MATCH (n:_D2G_Entity {_d2g_pipeline: $p, _d2g_relation: $rel}) \
                     WHERE NOT coalesce(n._d2g_deleted, false) AND n._d2g_ver < $ver \
                     RETURN n._d2g_id AS id, coalesce(n._d2g_props, []) AS props",
                )
                .param("p", p)
                .param("rel", relation.0.as_str())
                .param("ver", version.as_str()),
            )
            .await?;
            for row in victims {
                let id: String = get(&row, "id")?;
                let props: Vec<String> = get(&row, "props")?;
                remove_node(txn, p, &id, labels, &props).await?;
                if *vector {
                    set_vector_task(txn, p, &id, &relation.0, version, true).await?;
                }
            }
            Ok(())
        }
    }
}

/// (current version incl. tombstone, previous from-reference)
/// (current version incl. tombstone and relation truncate watermark,
/// previous from-reference)
async fn join_state(txn: &mut Txn, p: &str, id: &str, relation: &str) -> Result<(Option<String>, Option<EdgeRef>)> {
    let r = rows(
        txn,
        query(
            "OPTIONAL MATCH (j:_D2G_JoinState {_d2g_key: $k}) OPTIONAL MATCH (t:_D2G_Tombstone {_d2g_key: $k}) \
             OPTIONAL MATCH (w:_D2G_Watermark {_d2g_key: $wk}) \
             RETURN j.ver AS jv, t.ver AS tv, w.ver AS wv, j.from_prop AS fp, j.from_digest AS fd",
        )
        .param("k", key(p, id))
        .param("wk", key(p, relation)),
    )
    .await?;
    let row = r.first().ok_or_else(|| Error::internal("join state query returned no row"))?;
    let jv: Option<String> = get(row, "jv")?;
    let tv: Option<String> = get(row, "tv")?;
    let wv: Option<String> = get(row, "wv")?;
    let fp: Option<String> = get(row, "fp")?;
    let fd: Option<String> = get(row, "fd")?;
    Ok(([jv, tv, wv].into_iter().flatten().max(), fp.zip(fd).map(|(property, digest)| EdgeRef { property, digest })))
}

#[async_trait]
impl GraphSink for Neo4jSink {
    fn capabilities(&self) -> GraphSinkCapabilities {
        GraphSinkCapabilities {
            sink: "neo4j".into(),
            atomic_batches: true,
            receipts_in_target: true,
            guarded_versions: true,
            fencing: true,
            tombstones: true,
            atomic_vector_outbox: true,
            // Set to true only after the live-server qualification suite has run for a release.
            qualified: false,
            notes: vec![
                "single-member connections only (bolt+s://); the bundled driver has no cluster routing".into(),
                "all writes of a pipeline serialize on the pipeline node lock".into(),
            ],
        }
    }

    async fn preflight(&self, plan: Option<&MappingPlan>) -> Result<PreflightReport> {
        let mut r = PreflightReport::new("graph:neo4j");
        let comp = self.read(query("CALL dbms.components() YIELD name, versions, edition RETURN versions[0] AS v, edition AS e")).await?;
        if let Some(row) = comp.first() {
            let v: String = get(row, "v")?;
            let e: String = get(row, "e")?;
            r.push("engine_version", if v.starts_with('5') { CheckStatus::Pass } else { CheckStatus::Warn }, format!("Neo4j {v} {e}"));
        }
        let names: BTreeSet<String> = self
            .read(query("SHOW CONSTRAINTS YIELD name RETURN name"))
            .await?
            .iter()
            .filter_map(|row| row.get::<String>("name").ok())
            .collect();
        let missing: Vec<&str> = CONSTRAINTS.iter().map(|(n, _)| *n).filter(|n| !names.contains(*n)).collect();
        r.push(
            "constraints",
            if missing.is_empty() { CheckStatus::Pass } else { CheckStatus::Fail },
            if missing.is_empty() {
                "required uniqueness constraints present".to_owned()
            } else {
                format!("missing {missing:?}; run `db2graph-sync bootstrap`")
            },
        );
        if let Some(plan) = plan {
            let idx: BTreeSet<String> = self
                .read(query("SHOW INDEXES YIELD name RETURN name"))
                .await?
                .iter()
                .filter_map(|row| row.get::<String>("name").ok())
                .collect();
            let need: Vec<String> = uk_index_names(plan).into_iter().filter(|n| !idx.contains(n)).collect();
            r.push(
                "indexes",
                if need.is_empty() { CheckStatus::Pass } else { CheckStatus::Fail },
                if need.is_empty() {
                    "lookup indexes present".to_owned()
                } else {
                    format!("missing {need:?}; run `db2graph-sync bootstrap`")
                },
            );
        }
        Ok(r)
    }

    async fn bootstrap(&self, plan: &MappingPlan) -> Result<()> {
        for (name, label) in CONSTRAINTS {
            self.read(query(&format!("CREATE CONSTRAINT {name} IF NOT EXISTS FOR (n:{label}) REQUIRE n._d2g_key IS UNIQUE"))).await?;
        }
        self.read(query("CREATE CONSTRAINT d2g_pipeline_id IF NOT EXISTS FOR (n:_D2G_Pipeline) REQUIRE n.id IS UNIQUE")).await?;
        for stmt in [
            "CREATE INDEX d2g_entity_relation IF NOT EXISTS FOR (n:_D2G_Entity) ON (n._d2g_pipeline, n._d2g_relation)",
            "CREATE INDEX d2g_pending_to IF NOT EXISTS FOR (n:_D2G_PendingEdge) ON (n._d2g_pipeline, n.to_prop, n.to_digest)",
            "CREATE INDEX d2g_pending_from IF NOT EXISTS FOR (n:_D2G_PendingEdge) ON (n._d2g_pipeline, n.from_prop, n.from_digest)",
            "CREATE INDEX d2g_pending_owner IF NOT EXISTS FOR (n:_D2G_PendingEdge) ON (n._d2g_pipeline, n.owner)",
            "CREATE INDEX d2g_vector_task IF NOT EXISTS FOR (n:_D2G_VectorTask) ON (n._d2g_pipeline, n.ver)",
            "CREATE INDEX d2g_join_relation IF NOT EXISTS FOR (n:_D2G_JoinState) ON (n._d2g_pipeline, n.relation)",
        ] {
            self.read(query(stmt)).await?;
        }
        for (name, prop) in uk_indexes(plan) {
            let p = ident(&prop)?;
            self.read(query(&format!("CREATE INDEX {name} IF NOT EXISTS FOR (n:_D2G_Entity) ON (n._d2g_pipeline, n.{p})"))).await?;
        }
        self.read(query("CALL db.awaitIndexes(300)")).await?;
        Ok(())
    }

    async fn acquire_fence(&self, pipeline: &str) -> Result<(i64, PipelineMarker)> {
        let pipeline = pipeline.to_owned();
        self.in_txn(async |txn: &mut Txn| {
            let r = rows(
                txn,
                query(
                    "MERGE (p:_D2G_Pipeline {id: $p}) ON CREATE SET p.fence = 0, p.state_epoch = 0 \
                     SET p.fence = p.fence + 1 \
                     RETURN p.fence AS fence, p.state_epoch AS epoch, p.last_unit AS lu, p.last_version AS lv, p.active_generation AS g",
                )
                .param("p", pipeline.as_str()),
            )
            .await?;
            let row = r.first().ok_or_else(|| Error::internal("fence query returned no row"))?;
            let fence: i64 = get(row, "fence")?;
            let marker = PipelineMarker {
                fence_token: fence - 1,
                state_epoch: get::<i64>(row, "epoch")?.max(0) as u64,
                last_unit: get(row, "lu")?,
                last_version: get(row, "lv")?,
                active_generation: get::<Option<i64>>(row, "g")?.map(|g| g as u64),
            };
            Ok((fence, marker))
        })
        .await
    }

    async fn marker(&self, pipeline: &str) -> Result<Option<PipelineMarker>> {
        let r = self
            .read(
                query(
                    "MATCH (p:_D2G_Pipeline {id: $p}) RETURN p.fence AS fence, p.state_epoch AS epoch, p.last_unit AS lu, \
                     p.last_version AS lv, p.active_generation AS g",
                )
                .param("p", pipeline),
            )
            .await?;
        r.first()
            .map(|row| {
                Ok(PipelineMarker {
                    fence_token: get(row, "fence")?,
                    state_epoch: get::<i64>(row, "epoch")?.max(0) as u64,
                    last_unit: get(row, "lu")?,
                    last_version: get(row, "lv")?,
                    active_generation: get::<Option<i64>>(row, "g")?.map(|g| g as u64),
                })
            })
            .transpose()
    }

    async fn apply(&self, ctx: &ApplyContext, batch: &[GraphTx]) -> Result<Vec<ApplyOutcome>> {
        let ctx = ctx.clone();
        self.in_txn(async |txn: &mut Txn| {
            lock_and_fence(txn, &ctx).await?;
            let p = ctx.pipeline.as_str();
            let mut out = Vec::with_capacity(batch.len());
            let mut last: Option<&GraphTx> = None;
            for tx in batch {
                let r = rows(
                    txn,
                    query("OPTIONAL MATCH (r:_D2G_Receipt {_d2g_key: $k}) RETURN r.digest AS d").param("k", key(p, &tx.unit_id)),
                )
                .await?;
                let existing: Option<String> = r.first().map(|row| get(row, "d")).transpose()?.flatten();
                match existing {
                    Some(d) if d == tx.content_digest => {
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
                    apply_op(txn, p, op).await?;
                }
                txn.run(
                    query(
                        "CREATE (r:_D2G_Receipt {_d2g_key: $k, _d2g_pipeline: $p, unit: $u, digest: $d, version: $v, applied_at: datetime()})",
                    )
                    .param("k", key(p, &tx.unit_id))
                    .param("p", p)
                    .param("u", tx.unit_id.as_str())
                    .param("d", tx.content_digest.as_str())
                    .param("v", tx.version.as_str()),
                )
                .await
                .map_err(map_err)?;
                out.push(ApplyOutcome { unit_id: tx.unit_id.clone(), status: ApplyStatus::Applied });
                last = Some(tx);
            }
            let (lu, lv) = last.map(|t| (Some(t.unit_id.clone()), Some(t.version.clone()))).unwrap_or((None, None));
            txn.run(
                query(
                    "MATCH (p:_D2G_Pipeline {id: $p}) \
                     SET p.state_epoch = CASE WHEN coalesce(p.state_epoch, 0) < $e THEN $e ELSE p.state_epoch END, \
                         p.last_unit = coalesce($lu, p.last_unit), \
                         p.last_version = CASE WHEN $lv IS NOT NULL AND coalesce(p.last_version, '') < $lv THEN $lv ELSE p.last_version END",
                )
                .param("p", p)
                .param("e", ctx.state_epoch as i64)
                .param("lu", lu)
                .param("lv", lv),
            )
            .await
            .map_err(map_err)?;
            Ok(out)
        })
        .await
    }

    async fn set_active_generation(&self, ctx: &ApplyContext, generation: u64) -> Result<()> {
        let ctx = ctx.clone();
        self.in_txn(async |txn: &mut Txn| {
            lock_and_fence(txn, &ctx).await?;
            txn.run(
                query("MATCH (p:_D2G_Pipeline {id: $p}) SET p.active_generation = $g")
                    .param("p", ctx.pipeline.as_str())
                    .param("g", generation as i64),
            )
            .await
            .map_err(map_err)
        })
        .await
    }

    async fn counts(&self, pipeline: &str) -> Result<BTreeMap<String, u64>> {
        let r = self
            .read(
                query(
                    "MATCH (n:_D2G_Entity {_d2g_pipeline: $p}) WHERE NOT coalesce(n._d2g_deleted, false) \
                     RETURN n._d2g_relation AS r, count(*) AS c",
                )
                .param("p", pipeline),
            )
            .await?;
        r.iter().map(|row| Ok((get::<String>(row, "r")?, get::<i64>(row, "c")? as u64))).collect()
    }

    async fn lookup(&self, pipeline: &str, ids: &[String]) -> Result<Vec<EntityState>> {
        let keys: Vec<String> = ids.iter().map(|i| key(pipeline, i)).collect();
        let r = self
            .read(
                query(
                    "UNWIND $keys AS k MATCH (n:_D2G_Entity {_d2g_key: k}) \
                     RETURN n._d2g_id AS id, coalesce(n._d2g_relation, '') AS rel, coalesce(n._d2g_tenant, '') AS tenant, \
                            coalesce(n._d2g_ver, '') AS ver, coalesce(n._d2g_deleted, false) AS deleted, \
                            coalesce(n._d2g_labels, '{}') AS labels, properties(n) AS props",
                )
                .param("keys", keys),
            )
            .await?;
        r.iter()
            .map(|row| {
                let props: HashMap<String, BoltType> = get(row, "props")?;
                Ok(EntityState {
                    id: get(row, "id")?,
                    relation: get(row, "rel")?,
                    tenant: get(row, "tenant")?,
                    version: get(row, "ver")?,
                    deleted: get(row, "deleted")?,
                    labels_json: get(row, "labels")?,
                    properties: props.iter().filter(|(k, _)| !k.starts_with("_d2g")).map(|(k, v)| (k.clone(), from_bolt(v))).collect(),
                })
            })
            .collect()
    }

    async fn pending_vector_tasks(&self, pipeline: &str, limit: usize) -> Result<Vec<VectorTask>> {
        let r = self
            .read(
                query(
                    "MATCH (v:_D2G_VectorTask {_d2g_pipeline: $p}) RETURN v.entity_id AS id, v.relation AS rel, v.ver AS ver, \
                     v.delete AS del ORDER BY v.ver LIMIT $n",
                )
                .param("p", pipeline)
                .param("n", limit as i64),
            )
            .await?;
        r.iter()
            .map(|row| {
                Ok(VectorTask {
                    entity_id: get(row, "id")?,
                    relation: get(row, "rel")?,
                    version: get(row, "ver")?,
                    delete: get(row, "del")?,
                })
            })
            .collect()
    }

    async fn vector_backlog(&self, pipeline: &str) -> Result<u64> {
        let r = self.read(query("MATCH (v:_D2G_VectorTask {_d2g_pipeline: $p}) RETURN count(v) AS c").param("p", pipeline)).await?;
        Ok(r.first().map(|row| get::<i64>(row, "c")).transpose()?.unwrap_or(0) as u64)
    }

    async fn complete_vector_task(&self, pipeline: &str, task: &VectorTask) -> Result<bool> {
        let k = key(pipeline, &task.entity_id);
        let task = task.clone();
        self.in_txn(async |txn: &mut Txn| {
            // Lock first, then read: protects against a concurrent newer task.
            let r = rows(
                txn,
                query("MATCH (v:_D2G_VectorTask {_d2g_key: $k}) SET v._d2g_lock = true REMOVE v._d2g_lock RETURN count(v) AS c")
                    .param("k", k.as_str()),
            )
            .await?;
            if r.first().map(|row| get::<i64>(row, "c")).transpose()?.unwrap_or(0) == 0 {
                return Ok(false);
            }
            let r = rows(
                txn,
                query("MATCH (v:_D2G_VectorTask {_d2g_key: $k}) WHERE v.ver = $ver AND v.delete = $del DELETE v RETURN count(*) AS c")
                    .param("k", k.as_str())
                    .param("ver", task.version.as_str())
                    .param("del", task.delete),
            )
            .await?;
            Ok(r.first().map(|row| get::<i64>(row, "c")).transpose()?.unwrap_or(0) > 0)
        })
        .await
    }

    async fn pending_edges(&self, pipeline: &str) -> Result<u64> {
        let r = self.read(query("MATCH (pe:_D2G_PendingEdge {_d2g_pipeline: $p}) RETURN count(pe) AS c").param("p", pipeline)).await?;
        Ok(r.first().map(|row| get::<i64>(row, "c")).transpose()?.unwrap_or(0) as u64)
    }

    async fn outgoing(&self, pipeline: &str, id: &str) -> Result<Vec<(String, String)>> {
        let r = self
            .read(
                query(
                    "MATCH (a:_D2G_Entity {_d2g_key: $k})-[r]->(b:_D2G_Entity) WHERE r._d2g_owned = true AND r._d2g_pipeline = $p \
                     RETURN type(r) AS t, b._d2g_id AS b ORDER BY t, b",
                )
                .param("k", key(pipeline, id))
                .param("p", pipeline),
            )
            .await?;
        r.iter().map(|row| Ok((get(row, "t")?, get(row, "b")?))).collect()
    }
}

fn uk_indexes(plan: &MappingPlan) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = plan
        .relations
        .values()
        .flat_map(|r| r.unique_keys.iter().map(|u| (format!("d2g_uk_{}", u.property.trim_start_matches("_d2g_uk_")), u.property.clone())))
        .collect();
    v.sort();
    v.dedup();
    v
}

fn uk_index_names(plan: &MappingPlan) -> Vec<String> {
    uk_indexes(plan).into_iter().map(|(n, _)| n).collect()
}
