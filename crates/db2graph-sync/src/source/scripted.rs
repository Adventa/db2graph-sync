//! Scripted source connector for tests, examples and custom-connector
//! templates.
//!
//! It is not a database connector. It emulates the parts of real source
//! semantics the engine relies on: a fixed schema, an ordered log of
//! committed units with comparable positions, and resumption *after* the last
//! acknowledged position (anything not acknowledged is redelivered).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::Notify;

use super::{ChangeStream, SnapshotProgress, SourceConnector, SourceEvent, SourceIdentity, StartRequest};
use crate::capability::{CheckStatus, OrderingScopeKind, PreflightReport, SnapshotConsistency, SourceCapabilities};
use crate::error::{Error, Result};
use crate::schema::SourceSchema;
use crate::types::{CapturedTransaction, OrderingScope, SourcePosition, UnitKind, Version};

#[derive(Default)]
struct Log {
    units: Vec<CapturedTransaction>,
    /// Snapshot rows (already-existing records) delivered before streaming.
    snapshot: Vec<CapturedTransaction>,
    acked: Option<u64>,
    fail_next_reads: u32,
    /// Current rows per relation, as declared by the test (for reconciliation).
    tables: std::collections::BTreeMap<crate::types::RelationName, Vec<crate::types::Image>>,
}

/// See module docs.
#[derive(Clone)]
pub struct ScriptedSource {
    schema: SourceSchema,
    log: Arc<Mutex<Log>>,
    notify: Arc<Notify>,
}

impl ScriptedSource {
    pub fn new(schema: SourceSchema) -> Self {
        Self { schema, log: Arc::new(Mutex::new(Log::default())), notify: Arc::new(Notify::new()) }
    }

    pub fn scope(&self) -> OrderingScope {
        OrderingScope { source_id: self.schema.source_id.clone(), epoch: 0, partition: String::new() }
    }

    /// Append a committed unit. Its position must be greater than all
    /// previous ones (enforced).
    pub fn commit(&self, mut tx: CapturedTransaction) {
        let mut log = self.log.lock().expect("poisoned");
        let pos = tx.ack_position.ordinal.expect("scripted units need ordinal positions");
        if let Some(last) = log.units.last() {
            assert!(pos > last.ack_position.ordinal.unwrap_or(0), "positions must increase");
        }
        tx.kind = UnitKind::Transaction;
        log.units.push(tx);
        drop(log);
        self.notify.notify_waiters();
    }

    /// Provide pre-existing rows delivered as snapshot chunks.
    pub fn set_snapshot(&self, chunks: Vec<CapturedTransaction>) {
        self.log.lock().expect("poisoned").snapshot = chunks;
    }

    /// Highest acknowledged position.
    pub fn acked(&self) -> Option<u64> {
        self.log.lock().expect("poisoned").acked
    }

    /// Declare the current rows of a relation (used by reconciliation).
    pub fn set_current_rows(&self, relation: &str, rows: Vec<crate::types::Image>) {
        self.log.lock().expect("poisoned").tables.insert(crate::types::RelationName(relation.into()), rows);
    }

    /// Make the next `n` reads fail with a retryable source error.
    pub fn fail_next_reads(&self, n: u32) {
        self.log.lock().expect("poisoned").fail_next_reads = n;
    }

    /// Convenience: build a transaction unit at `position`.
    pub fn unit(&self, position: u64, items: Vec<crate::types::TxItem>) -> CapturedTransaction {
        let scope = self.scope();
        CapturedTransaction {
            unit_id: format!("{}:{position}", self.schema.source_id),
            kind: UnitKind::Transaction,
            source_id: self.schema.source_id.clone(),
            scope: scope.clone(),
            ack_position: SourcePosition { scope, ordinal: Some(position), native: position.to_string() },
            base_version: Version { epoch: 0, position, ordinal: 0 },
            native_tx_id: Some(position.to_string()),
            commit_time: Some(chrono::Utc::now().to_rfc3339()),
            items,
            mapping_version: String::new(),
            policy_version: String::new(),
        }
    }
}

struct Stream {
    src: ScriptedSource,
    cursor: u64,
    snapshot: Option<(SnapshotProgress, Vec<CapturedTransaction>)>,
}

#[async_trait]
impl ChangeStream for Stream {
    async fn next(&mut self) -> Result<SourceEvent> {
        if let Some((mut progress, mut chunks)) = self.snapshot.take() {
            if chunks.is_empty() {
                progress.consistency.get_or_insert(SnapshotConsistency::TransactionConsistent);
                return Ok(SourceEvent::SnapshotComplete(progress));
            }
            let mut c = chunks.remove(0);
            let rel = match &c.kind {
                UnitKind::SnapshotChunk { relation, .. } => relation.clone(),
                _ => crate::types::RelationName("scripted".into()),
            };
            progress.chunks_emitted += 1;
            for item in &c.items {
                if let crate::types::TxItem::Change(ch) = item {
                    progress.rows += 1;
                    *progress.relation_rows.entry(ch.relation.0.clone()).or_default() += 1;
                }
            }
            c.kind = UnitKind::SnapshotChunk { generation: progress.generation, relation: rel, chunk: progress.chunks_emitted };
            c.unit_id = format!("snapshot:{}:{}", progress.generation, progress.chunks_emitted);
            self.snapshot = Some((progress.clone(), chunks));
            return Ok(SourceEvent::Unit(Box::new(c), Some(progress)));
        }
        loop {
            let notified = self.src.notify.notified();
            {
                let mut log = self.src.log.lock().expect("poisoned");
                if log.fail_next_reads > 0 {
                    log.fail_next_reads -= 1;
                    return Err(Error::source("injected source read failure"));
                }
                if let Some(u) = log.units.iter().find(|u| u.ack_position.ordinal.unwrap_or(0) > self.cursor) {
                    self.cursor = u.ack_position.ordinal.unwrap_or(0);
                    return Ok(SourceEvent::Unit(Box::new(u.clone()), None));
                }
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {
                    return Ok(SourceEvent::Keepalive(None));
                }
            }
        }
    }

    async fn ack(&mut self, pos: &SourcePosition) -> Result<()> {
        let mut log = self.src.log.lock().expect("poisoned");
        let p = pos.ordinal.unwrap_or(0);
        if log.acked.is_none_or(|a| p > a) {
            log.acked = Some(p);
        }
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for ScriptedSource {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            connector: "scripted".into(),
            transaction_boundaries: true,
            ordering: OrderingScopeKind::TotalCommitOrder,
            snapshot: SnapshotConsistency::TransactionConsistent,
            resumed_snapshot: SnapshotConsistency::Convergent,
            before_images: "as scripted",
            truncate: true,
            schema_change_signal: false,
            qualified_versions: vec![],
            notes: vec!["test/development connector; not a database".into()],
        }
    }
    async fn preflight(&self) -> Result<PreflightReport> {
        let mut r = PreflightReport::new("source:scripted");
        r.push("connector", CheckStatus::Pass, "scripted source ready");
        Ok(r)
    }
    async fn identity(&self) -> Result<SourceIdentity> {
        Ok(SourceIdentity { source_id: self.schema.source_id.clone(), history: "1".into(), server_version: "scripted".into() })
    }
    async fn discover_schema(&self) -> Result<SourceSchema> {
        Ok(self.schema.clone())
    }
    async fn start(&self, req: StartRequest) -> Result<Box<dyn ChangeStream>> {
        let cursor = req.resume.and_then(|p| p.ordinal).unwrap_or(0);
        let snapshot = match req.snapshot {
            Some(mut progress) => {
                let all = self.log.lock().expect("poisoned").snapshot.clone();
                let skip = progress.chunks_emitted as usize;
                if skip > 0 {
                    // Resumed snapshot: remaining chunks only; no longer a
                    // single consistent read.
                    progress.consistency = Some(SnapshotConsistency::Convergent);
                }
                Some((progress, all.into_iter().skip(skip).collect()))
            }
            None => None,
        };
        Ok(Box::new(Stream { src: self.clone(), cursor, snapshot }))
    }
    async fn row_count(&self, relation: &crate::types::RelationName) -> Result<u64> {
        Ok(self.log.lock().expect("poisoned").tables.get(relation).map(|t| t.len() as u64).unwrap_or(0))
    }
    async fn sample_rows(&self, relation: &crate::types::RelationName, limit: u32) -> Result<Vec<crate::types::Image>> {
        Ok(self
            .log
            .lock()
            .expect("poisoned")
            .tables
            .get(relation)
            .map(|t| t.iter().take(limit as usize).cloned().collect())
            .unwrap_or_default())
    }
}
