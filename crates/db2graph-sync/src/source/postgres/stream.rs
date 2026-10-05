//! PostgreSQL change stream: optional snapshot phase, then pgoutput streaming.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Buf;

use super::pgoutput::{self, Msg, RelationMsg};
use super::wire::{self, Conn, format_lsn, parse_lsn, quote_ident, quote_literal};
use super::{Inner, simple_rows};
use crate::capability::SnapshotConsistency;
use crate::error::{Error, ErrorKind, Result};
use crate::schema::{Relation, SourceSchema};
use crate::source::{ChangeStream, SnapshotProgress, SourceEvent, StartRequest};
use crate::types::{
    CapturedTransaction, Change, ChangeOp, Completeness, FieldState, Image, OrderingScope, RelationName, SourcePosition, Truncate, TxItem,
    UnitKind, Value, Version,
};

struct Snapshot {
    client: tokio_postgres::Client,
    progress: SnapshotProgress,
    remaining: VecDeque<RelationName>,
    schema: SourceSchema,
    boundary: u64,
    chunk_rows: u32,
}

struct TxBuf {
    xid: u32,
    commit_ts: i64,
    items: Vec<TxItem>,
    ordinal: u32,
}

pub(crate) struct PgStream {
    inner: Arc<Inner>,
    repl: Option<Conn>,
    streaming: bool,
    snapshot: Option<Snapshot>,
    start_lsn: u64,
    acked: u64,
    last_status: Instant,
    status_every: Duration,
    epoch: u32,
    source_id: String,
    relations: HashMap<u32, RelationMsg>,
    tx: Option<TxBuf>,
    pending: VecDeque<SourceEvent>,
    max_changes: usize,
}

fn position(scope: &OrderingScope, lsn: u64) -> SourcePosition {
    SourcePosition { scope: scope.clone(), ordinal: Some(lsn), native: format_lsn(lsn) }
}

impl PgStream {
    pub async fn open(inner: Arc<Inner>, req: StartRequest) -> Result<PgStream> {
        let mut repl = inner.replication_conn().await?;
        let id = repl.simple_query("IDENTIFY_SYSTEM").await?;
        let sysid = id.first().and_then(|r| r.first().cloned().flatten()).ok_or_else(|| Error::source("IDENTIFY_SYSTEM failed"))?;
        let source_id = format!("pg:{sysid}");
        let known = inner.source_id.read().expect("poisoned").clone();
        if let Some(k) = known
            && k != source_id
        {
            return Err(Error::new(
                ErrorKind::SourceIdentityChanged,
                format!("connected to {source_id} but the pipeline follows {k}; authorized recovery is required"),
            ));
        }
        *inner.source_id.write().expect("poisoned") = Some(source_id.clone());

        let client = inner.client().await?;
        let slot_row = client
            .query_opt(
                "SELECT confirmed_flush_lsn::text, active, wal_status::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&inner.cfg.slot],
            )
            .await
            .map_err(|e| Error::source(e.to_string()))?;
        let scope = OrderingScope { source_id: source_id.clone(), epoch: req.epoch, partition: String::new() };
        let resume_lsn = req.resume.as_ref().and_then(|p| p.ordinal);

        let mut snapshot_consistency = SnapshotConsistency::Convergent;
        let mut exported: Option<String> = None;
        let start_lsn = match slot_row {
            Some(row) => {
                let active: bool = row.get(1);
                if active {
                    // Retryable: after a network interruption the server can
                    // still consider our previous connection active until
                    // wal_sender_timeout. A genuinely competing consumer keeps
                    // this retrying (visible in status warnings and reconnect
                    // counters); the state lock and graph fence still prevent
                    // two writers.
                    return Err(Error::retryable(
                        ErrorKind::OwnershipConflict,
                        format!("replication slot `{}` is in use by another connection", inner.cfg.slot),
                    ));
                }
                if row.get::<_, Option<String>>(2).as_deref() == Some("lost") {
                    return Err(Error::new(
                        ErrorKind::ResumeHistoryLost,
                        format!("replication slot `{}` was invalidated (wal_status=lost)", inner.cfg.slot),
                    ));
                }
                // The server starts from max(requested, confirmed_flush_lsn).
                let confirmed = row.get::<_, Option<String>>(0).and_then(|s| parse_lsn(&s)).unwrap_or(0);
                resume_lsn.unwrap_or(confirmed)
            }
            None => {
                if resume_lsn.is_some() {
                    return Err(Error::new(
                        ErrorKind::ResumeHistoryLost,
                        format!(
                            "replication slot `{}` no longer exists; changes since the last checkpoint cannot be recovered",
                            inner.cfg.slot
                        ),
                    ));
                }
                let want_export = req.snapshot.as_ref().is_some_and(|p| p.boundary.is_none());
                let sql = format!(
                    "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput {}",
                    quote_ident(&inner.cfg.slot),
                    if want_export { "EXPORT_SNAPSHOT" } else { "NOEXPORT_SNAPSHOT" }
                );
                let rows = repl.simple_query(&sql).await?;
                let row = rows.first().ok_or_else(|| Error::source("CREATE_REPLICATION_SLOT returned nothing"))?;
                let cp = row.get(1).cloned().flatten().and_then(|s| parse_lsn(&s)).ok_or_else(|| Error::source("no consistent point"))?;
                if want_export {
                    exported = row.get(2).cloned().flatten();
                    snapshot_consistency = SnapshotConsistency::TransactionConsistent;
                }
                tracing::info!(slot = %inner.cfg.slot, consistent_point = %format_lsn(cp), "created replication slot");
                cp
            }
        };

        let snapshot = match req.snapshot {
            None => None,
            Some(mut progress) => {
                let boundary = progress.boundary.as_ref().and_then(|b| b.ordinal).unwrap_or(start_lsn);
                if progress.boundary.is_none() {
                    progress.boundary = Some(position(&scope, boundary));
                    progress.consistency = Some(snapshot_consistency);
                } else {
                    // Resumed after interruption: fresh reads, convergent only.
                    progress.consistency = Some(SnapshotConsistency::Convergent);
                }
                match &exported {
                    Some(name) => {
                        client
                            .batch_execute(&format!(
                                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT {}",
                                quote_literal(name)
                            ))
                            .await
                            .map_err(|e| Error::source(format!("importing exported snapshot: {e}")))?;
                    }
                    None => {
                        client
                            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                            .await
                            .map_err(|e| Error::source(e.to_string()))?;
                    }
                }
                let schema = inner.discover(&client).await?;
                let remaining: VecDeque<RelationName> = req
                    .snapshot_relations
                    .iter()
                    .filter(|r| !progress.completed.contains(r) && schema.relations.contains_key(r))
                    .cloned()
                    .collect();
                Some(Snapshot { client, progress, remaining, schema, boundary, chunk_rows: req.snapshot_chunk_rows.max(1) })
            }
        };

        Ok(PgStream {
            inner: inner.clone(),
            repl: Some(repl),
            streaming: false,
            snapshot,
            start_lsn,
            acked: resume_lsn.unwrap_or(0),
            last_status: Instant::now(),
            status_every: Duration::from_secs(inner.cfg.status_interval_secs.max(1)),
            epoch: req.epoch,
            source_id,
            relations: HashMap::new(),
            tx: None,
            pending: VecDeque::new(),
            max_changes: inner.cfg.max_transaction_changes,
        })
    }

    fn scope(&self) -> OrderingScope {
        OrderingScope { source_id: self.source_id.clone(), epoch: self.epoch, partition: String::new() }
    }

    async fn next_snapshot_event(&mut self) -> Result<Option<SourceEvent>> {
        let scope = self.scope();
        let source_id = self.source_id.clone();
        let epoch = self.epoch;
        let Some(s) = self.snapshot.as_mut() else { return Ok(None) };
        loop {
            let (rel_name, after) = match s.progress.current.clone() {
                Some(c) => c,
                None => match s.remaining.pop_front() {
                    Some(r) => (r, None),
                    None => {
                        let _ = s.client.batch_execute("COMMIT").await;
                        let progress = s.progress.clone();
                        self.snapshot = None;
                        return Ok(Some(SourceEvent::SnapshotComplete(progress)));
                    }
                },
            };
            let rel = s
                .schema
                .relations
                .get(&rel_name)
                .ok_or_else(|| Error::new(ErrorKind::SchemaIncompatible, format!("{rel_name} disappeared during snapshot")))?
                .clone();
            if rel.primary_key.is_empty() {
                s.progress.completed.push(rel_name.clone());
                s.progress.current = None;
                continue;
            }
            let sql = chunk_query(&rel, after.as_ref(), s.chunk_rows)?;
            let rows = simple_rows(&s.client, &sql).await?;
            if rows.is_empty() {
                s.progress.completed.push(rel_name.clone());
                s.progress.current = None;
                continue;
            }
            let mut items = Vec::with_capacity(rows.len());
            for row in &rows {
                let mut img = Image::new();
                for (c, v) in rel.columns.iter().zip(row) {
                    let val = match v {
                        None => Value::Null,
                        Some(t) => pgoutput::text_to_value(c.type_id.unwrap_or(0), t, Some(&c.type_name)),
                    };
                    img.insert(c.name.clone(), FieldState::Present(val));
                }
                items.push(TxItem::Change(Change {
                    relation: rel_name.clone(),
                    op: ChangeOp::Read,
                    event_ordinal: 0,
                    old_key: None,
                    before: None,
                    after: Some(img),
                    completeness: Completeness::FullAfterImage,
                    schema_version: s.schema.version(),
                }));
            }
            // Raw key text is needed for keyset pagination; keep the last row's text.
            let last_text: Image = rel
                .primary_key
                .iter()
                .map(|k| {
                    let idx = rel.columns.iter().position(|c| &c.name == k).expect("pk column exists");
                    (k.clone(), FieldState::Present(Value::Text(rows.last().expect("non-empty")[idx].clone().unwrap_or_default())))
                })
                .collect();
            s.progress.chunks_emitted += 1;
            s.progress.rows += rows.len() as u64;
            *s.progress.relation_rows.entry(rel_name.0.clone()).or_default() += rows.len() as u64;
            s.progress.current = Some((rel_name.clone(), Some(last_text)));
            let unit = CapturedTransaction {
                unit_id: format!("{source_id}:e{epoch}:snapshot:{}:{}:{}", s.progress.generation, rel_name, s.progress.chunks_emitted),
                kind: UnitKind::SnapshotChunk { generation: s.progress.generation, relation: rel_name, chunk: s.progress.chunks_emitted },
                source_id: source_id.clone(),
                scope: scope.clone(),
                ack_position: position(&scope, s.boundary),
                base_version: Version { epoch, position: s.boundary, ordinal: 0 },
                native_tx_id: None,
                commit_time: None,
                items,
                mapping_version: String::new(),
                policy_version: String::new(),
            };
            return Ok(Some(SourceEvent::Unit(Box::new(unit), Some(s.progress.clone()))));
        }
    }

    async fn start_streaming(&mut self) -> Result<()> {
        let repl = self.repl.as_mut().ok_or_else(|| Error::internal("replication connection missing"))?;
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names {})",
            quote_ident(&self.inner.cfg.slot),
            format_lsn(self.start_lsn),
            quote_literal(&quote_ident(&self.inner.cfg.publication)),
        );
        repl.start_copy_both(&sql).await?;
        self.streaming = true;
        tracing::info!(slot = %self.inner.cfg.slot, from = %format_lsn(self.start_lsn), "logical replication started");
        Ok(())
    }

    fn relation(&self, oid: u32) -> Result<&RelationMsg> {
        self.relations.get(&oid).ok_or_else(|| Error::source(format!("change for unknown relation oid {oid} (no Relation message)")))
    }

    fn push_change(&mut self, item: TxItem) -> Result<()> {
        let tx = self.tx.as_mut().ok_or_else(|| Error::source("change outside of a transaction"))?;
        tx.items.push(item);
        tx.ordinal += 1;
        if tx.items.len() > self.max_changes {
            return Err(Error::new(
                ErrorKind::LimitExceeded,
                format!(
                    "source transaction {} exceeds max_transaction_changes ({}); raise the limit or split the transaction (see LIMITATIONS.md)",
                    tx.xid, self.max_changes
                ),
            ));
        }
        Ok(())
    }

    /// Detect a Relation message that disagrees with the discovered schema.
    fn schema_differs(&self, r: &RelationMsg) -> bool {
        let known = self.inner.known_schema.read().expect("poisoned");
        let Some(schema) = known.as_ref() else { return false };
        match schema.relations.get(&RelationName(r.qualified())) {
            None => true,
            Some(rel) => {
                rel.columns.len() != r.columns.len()
                    || rel.columns.iter().zip(&r.columns).any(|(a, b)| a.name != b.name || a.type_id != Some(b.type_oid))
            }
        }
    }

    fn handle(&mut self, msg: Msg) -> Result<Option<SourceEvent>> {
        match msg {
            Msg::Begin { commit_ts, xid, .. } => {
                self.tx = Some(TxBuf { xid, commit_ts, items: Vec::new(), ordinal: 0 });
            }
            Msg::Relation(r) => {
                let differs = self.schema_differs(&r);
                let name = RelationName(r.qualified());
                self.relations.insert(r.oid, r);
                if differs {
                    return Ok(Some(SourceEvent::SchemaSignal(name)));
                }
            }
            Msg::Insert { rel, new } => {
                let r = self.relation(rel)?.clone();
                let ord = self.tx.as_ref().map(|t| t.ordinal).unwrap_or(0);
                self.push_change(TxItem::Change(Change {
                    relation: RelationName(r.qualified()),
                    op: ChangeOp::Insert,
                    event_ordinal: ord,
                    old_key: None,
                    before: None,
                    after: Some(pgoutput::image(&r, &new, false)?),
                    completeness: Completeness::FullAfterImage,
                    schema_version: String::new(),
                }))?;
            }
            Msg::Update { rel, key, old, new } => {
                let r = self.relation(rel)?.clone();
                let ord = self.tx.as_ref().map(|t| t.ordinal).unwrap_or(0);
                let before = old.as_ref().map(|o| pgoutput::image(&r, o, false)).transpose()?;
                let old_key = match (&key, &before) {
                    (Some(k), _) => Some(pgoutput::image(&r, k, true)?),
                    (None, Some(b)) => Some(b.clone()),
                    _ => None,
                };
                self.push_change(TxItem::Change(Change {
                    relation: RelationName(r.qualified()),
                    op: ChangeOp::Update,
                    event_ordinal: ord,
                    old_key,
                    before,
                    after: Some(pgoutput::image(&r, &new, false)?),
                    completeness: Completeness::FullAfterImage,
                    schema_version: String::new(),
                }))?;
            }
            Msg::Delete { rel, key, old } => {
                let r = self.relation(rel)?.clone();
                let ord = self.tx.as_ref().map(|t| t.ordinal).unwrap_or(0);
                let before = old.as_ref().map(|o| pgoutput::image(&r, o, false)).transpose()?;
                let old_key = match (&key, &before) {
                    (Some(k), _) => Some(pgoutput::image(&r, k, true)?),
                    (None, Some(b)) => Some(b.clone()),
                    _ => None,
                };
                self.push_change(TxItem::Change(Change {
                    relation: RelationName(r.qualified()),
                    op: ChangeOp::Delete,
                    event_ordinal: ord,
                    old_key,
                    before,
                    after: None,
                    completeness: Completeness::KeyOnly,
                    schema_version: String::new(),
                }))?;
            }
            Msg::Truncate { rels, cascade } => {
                let relations = rels.iter().map(|o| self.relation(*o).map(|r| RelationName(r.qualified()))).collect::<Result<Vec<_>>>()?;
                let ord = self.tx.as_ref().map(|t| t.ordinal).unwrap_or(0);
                self.push_change(TxItem::Truncate(Truncate { relations, event_ordinal: ord, cascade }))?;
            }
            Msg::Commit { end_lsn, commit_ts, .. } => {
                let tx = self.tx.take().ok_or_else(|| Error::source("commit without begin"))?;
                let scope = self.scope();
                let pos = position(&scope, end_lsn);
                if tx.items.is_empty() {
                    return Ok(Some(SourceEvent::Keepalive(Some(pos))));
                }
                let commit_time = wire::pg_time_rfc3339(if commit_ts != 0 { commit_ts } else { tx.commit_ts });
                return Ok(Some(SourceEvent::Unit(
                    Box::new(CapturedTransaction {
                        // Epoch included: after an authorized source change LSNs may repeat.
                        unit_id: format!("{}:e{}:{:016x}", self.source_id, self.epoch, end_lsn),
                        kind: UnitKind::Transaction,
                        source_id: self.source_id.clone(),
                        scope,
                        ack_position: pos,
                        base_version: Version { epoch: self.epoch, position: end_lsn, ordinal: 0 },
                        native_tx_id: Some(tx.xid.to_string()),
                        commit_time: Some(commit_time),
                        items: tx.items,
                        mapping_version: String::new(),
                        policy_version: String::new(),
                    }),
                    None,
                )));
            }
            Msg::Origin | Msg::Type | Msg::Message => {}
        }
        Ok(None)
    }

    async fn maybe_status(&mut self, force: bool) -> Result<()> {
        if !self.streaming {
            return Ok(());
        }
        if force || self.last_status.elapsed() >= self.status_every {
            let acked = self.acked;
            if let Some(repl) = self.repl.as_mut() {
                repl.send_status(acked, false).await?;
            }
            self.last_status = Instant::now();
        }
        Ok(())
    }
}

#[async_trait]
impl ChangeStream for PgStream {
    async fn next(&mut self) -> Result<SourceEvent> {
        if let Some(e) = self.pending.pop_front() {
            return Ok(e);
        }
        if self.snapshot.is_some()
            && let Some(ev) = self.next_snapshot_event().await?
        {
            return Ok(ev);
        }
        if !self.streaming {
            self.start_streaming().await?;
        }
        loop {
            self.maybe_status(false).await?;
            let repl = self.repl.as_mut().ok_or_else(|| Error::internal("replication connection missing"))?;
            let (tag, mut body) = repl.read_msg().await?;
            match tag {
                b'd' => {
                    if !body.has_remaining() {
                        continue;
                    }
                    match body.get_u8() {
                        b'w' => {
                            if body.remaining() < 24 {
                                return Err(Error::source("short XLogData"));
                            }
                            let _start = body.get_u64();
                            let _end = body.get_u64();
                            let _ts = body.get_i64();
                            let msg = pgoutput::decode(body)?;
                            if let Some(ev) = self.handle(msg)? {
                                return Ok(ev);
                            }
                        }
                        b'k' => {
                            if body.remaining() < 17 {
                                return Err(Error::source("short keepalive"));
                            }
                            let wal_end = body.get_u64();
                            let _ts = body.get_i64();
                            let reply = body.get_u8() == 1;
                            if reply {
                                self.maybe_status(true).await?;
                            }
                            if self.tx.is_none() {
                                // No transaction is open: everything decoded up to
                                // wal_end has been delivered.
                                return Ok(SourceEvent::Keepalive(Some(position(&self.scope(), wal_end))));
                            }
                        }
                        other => return Err(Error::source(format!("unexpected CopyData sub-message `{}`", other as char))),
                    }
                }
                b'E' => {
                    return Err(Error::source(format!("server error during replication: {}", String::from_utf8_lossy(&body))));
                }
                b'c' => return Err(Error::source("server ended replication (CopyDone)")),
                _ => {}
            }
        }
    }

    async fn ack(&mut self, pos: &SourcePosition) -> Result<()> {
        if let Some(l) = pos.ordinal
            && l > self.acked
        {
            self.acked = l;
        }
        self.maybe_status(false).await
    }
}

/// First `limit` rows in key order (reconciliation sampling).
pub(crate) fn chunk_query_pub(rel: &Relation, limit: u32) -> Result<String> {
    chunk_query(rel, None, limit)
}

/// Keyset-paginated snapshot query with literals (simple-query protocol, so
/// values come back in exactly the type output format pgoutput uses).
fn chunk_query(rel: &Relation, after: Option<&Image>, limit: u32) -> Result<String> {
    let (schema, table) = rel.name.0.split_once('.').ok_or_else(|| Error::internal("unqualified relation"))?;
    let cols = rel.columns.iter().map(|c| quote_ident(&c.name)).collect::<Vec<_>>().join(", ");
    let pk = rel.primary_key.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let mut sql = format!("SELECT {cols} FROM {}.{}", quote_ident(schema), quote_ident(table));
    if let Some(after) = after {
        let vals = rel
            .primary_key
            .iter()
            .map(|k| {
                let col = rel.column(k).ok_or_else(|| Error::internal("pk column missing"))?;
                match after.get(k) {
                    Some(FieldState::Present(Value::Text(t))) => Ok(format!("{}::{}", quote_literal(t), col.type_name)),
                    _ => Err(Error::internal("snapshot resume key missing")),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        sql.push_str(&format!(" WHERE ({pk}) > ({})", vals.join(", ")));
    }
    sql.push_str(&format!(" ORDER BY {pk} LIMIT {limit}"));
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, ImageMode};

    #[test]
    fn chunk_query_is_quoted_and_keyset_paginated() {
        let rel = Relation {
            name: RelationName("public.order items".into()),
            columns: vec![
                Column { name: "a".into(), type_name: "integer".into(), type_id: Some(23), nullable: false },
                Column { name: "b\"x".into(), type_name: "text".into(), type_id: Some(25), nullable: false },
            ],
            primary_key: vec!["a".into(), "b\"x".into()],
            unique_constraints: vec![],
            foreign_keys: vec![],
            image_mode: ImageMode::KeyOnly,
        };
        assert_eq!(
            chunk_query(&rel, None, 10).unwrap(),
            "SELECT \"a\", \"b\"\"x\" FROM \"public\".\"order items\" ORDER BY \"a\", \"b\"\"x\" LIMIT 10"
        );
        let after: Image = [
            ("a".to_string(), FieldState::Present(Value::Text("5".into()))),
            ("b\"x".to_string(), FieldState::Present(Value::Text("it's".into()))),
        ]
        .into();
        let q = chunk_query(&rel, Some(&after), 10).unwrap();
        assert!(q.contains("WHERE (\"a\", \"b\"\"x\") > ('5'::integer, 'it''s'::text)"), "{q}");
    }
}
