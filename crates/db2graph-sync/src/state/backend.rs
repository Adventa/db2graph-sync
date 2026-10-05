//! Storage backends for durable state.
//!
//! A [`StateBackend`] is a small ordered key-value store with four tables and
//! atomic, durable batch commits. It only ever sees **sealed** (encrypted and
//! authenticated) values; encryption, epochs, contiguity and rollback checks
//! live in [`super::StateStore`] so a custom backend cannot weaken them.
//!
//! Contract for implementations:
//!
//! * `commit` applies every operation of the batch or none of them, and is
//!   durable (survives process and OS crash) when it returns `Ok`.
//! * Reads observe every previously committed batch.
//! * Keys of the u64 tables are 8-byte big-endian, so byte order is numeric order.
//! * Exclusive ownership (one engine per store) is the backend's
//!   responsibility; declare it with [`StateBackend::exclusive`].
//! * Errors indicating exhausted storage must be returned, never ignored.

use std::fs::{File, OpenOptions};
use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use crate::error::{Error, ErrorKind, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Table {
    Meta,
    Journal,
    Schemas,
    Audit,
}

impl Table {
    pub fn name(self) -> &'static str {
        match self {
            Table::Meta => "meta",
            Table::Journal => "journal",
            Table::Schemas => "schema_history",
            Table::Audit => "audit_spool",
        }
    }
}

/// One write: `value = None` deletes the key.
#[derive(Debug, Clone)]
pub struct WriteOp {
    pub table: Table,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    pub ops: Vec<WriteOp>,
}

impl WriteBatch {
    pub fn put(&mut self, table: Table, key: impl Into<Vec<u8>>, value: Vec<u8>) {
        self.ops.push(WriteOp { table, key: key.into(), value: Some(value) });
    }
    pub fn delete(&mut self, table: Table, key: impl Into<Vec<u8>>) {
        self.ops.push(WriteOp { table, key: key.into(), value: None });
    }
}

pub trait StateBackend: Send + Sync {
    fn name(&self) -> &str;
    /// Whether committed batches survive process/OS crashes.
    fn durable(&self) -> bool;
    /// Whether the backend guarantees a single owner (e.g. file lock).
    fn exclusive(&self) -> bool;
    fn commit(&self, batch: WriteBatch) -> Result<()>;
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Entries with `start <= key < end` (unbounded when `end` is `None`) in
    /// key order, at most `limit`.
    fn range(&self, table: Table, start: &[u8], end: Option<&[u8]>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn last(&self, table: Table) -> Result<Option<(Vec<u8>, Vec<u8>)>>;
    fn len(&self, table: Table) -> Result<u64>;
}

const META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("meta");
const JOURNAL: TableDefinition<&[u8], &[u8]> = TableDefinition::new("journal");
const SCHEMAS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("schema_history");
const AUDIT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("audit_spool");

fn def(t: Table) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    match t {
        Table::Meta => META,
        Table::Journal => JOURNAL,
        Table::Schemas => SCHEMAS,
        Table::Audit => AUDIT,
    }
}

/// Default backend: [redb](https://www.redb.org) (copy-on-write B-trees,
/// fsync on commit) in `<dir>/state.redb`, guarded by an OS file lock on
/// `<dir>/LOCK`.
pub struct RedbBackend {
    db: Database,
    _lock: File,
}

impl RedbBackend {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::state_io(format!("cannot create state dir {}: {e}", dir.display())))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("LOCK"))
            .map_err(|e| Error::state_io(format!("cannot open lock file: {e}")))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(Error::new(
                    ErrorKind::OwnershipConflict,
                    format!("state directory {} is owned by another running engine", dir.display()),
                ));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(Error::state_io(format!("cannot lock state dir: {e}"))),
        }
        let db = Database::create(dir.join("state.redb"))?;
        let w = db.begin_write()?;
        for t in [Table::Meta, Table::Journal, Table::Schemas, Table::Audit] {
            w.open_table(def(t))?;
        }
        w.commit()?;
        Ok(Self { db, _lock: lock })
    }
}

impl StateBackend for RedbBackend {
    fn name(&self) -> &str {
        "redb"
    }
    fn durable(&self) -> bool {
        true
    }
    fn exclusive(&self) -> bool {
        true
    }
    fn commit(&self, batch: WriteBatch) -> Result<()> {
        let w = self.db.begin_write()?;
        for op in &batch.ops {
            let mut t = w.open_table(def(op.table))?;
            match &op.value {
                Some(v) => {
                    t.insert(op.key.as_slice(), v.as_slice())?;
                }
                None => {
                    t.remove(op.key.as_slice())?;
                }
            }
        }
        w.commit()?;
        Ok(())
    }
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(def(table))?;
        Ok(t.get(key)?.map(|g| g.value().to_vec()))
    }
    fn range(&self, table: Table, start: &[u8], end: Option<&[u8]>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(def(table))?;
        let mut out = Vec::new();
        let iter = match end {
            Some(e) => t.range::<&[u8]>(start..e)?,
            None => t.range::<&[u8]>(start..)?,
        };
        for item in iter {
            let (k, v) = item?;
            out.push((k.value().to_vec(), v.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
    fn last(&self, table: Table) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(def(table))?;
        Ok(t.last()?.map(|(k, v)| (k.value().to_vec(), v.value().to_vec())))
    }
    fn len(&self, table: Table) -> Result<u64> {
        let r = self.db.begin_read()?;
        Ok(r.open_table(def(table))?.len()?)
    }
}
