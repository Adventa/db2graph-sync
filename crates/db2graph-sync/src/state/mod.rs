//! Encrypted durable state.
//!
//! Every value is sealed with the configured data-encryption key
//! ([`crate::crypto::Sealer`]); the AAD binds each record to its store, table
//! and key, so records cannot be moved or substituted without detection.
//! Storage is delegated to a [`StateBackend`] (default: [`RedbBackend`]),
//! which only ever sees ciphertext.
//!
//! Stored here: captured work (journal), checkpoints, acknowledged source
//! positions, snapshot progress, schema history, audit spool and the state
//! *epoch*. The epoch increases with every committed write and is the basis
//! of rollback detection: the graph pipeline marker and the optional anchor
//! file record the epoch they observed, and a store whose epoch is lower than
//! that evidence has been restored from an older copy.
//!
//! Tombstones, receipts, unresolved relationships and vector tasks are kept
//! **in the graph target**, inside the same transaction as the effects they
//! describe (see `docs/architecture/durability.md`).

mod backend;

use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde::de::DeserializeOwned;

pub use backend::{RedbBackend, StateBackend, Table, WriteBatch, WriteOp};

use crate::crypto::Sealer;
use crate::error::{Error, ErrorKind, Result};
use crate::types::CapturedTransaction;

const FORMAT: u32 = 1;
/// Random 96-bit nonces: stay far below the 2^32 invocation bound per key.
const MAX_SEALS_PER_KEY: u64 = 1 << 31;

/// Named points at which tests can inject failures that emulate a crash.
pub type FaultInjector = Arc<dyn Fn(&'static str) -> Result<()> + Send + Sync>;

pub(crate) fn fault(f: &Option<FaultInjector>, point: &'static str) -> Result<()> {
    match f {
        Some(f) => f(point),
        None => Ok(()),
    }
}

fn k64(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn from_k64(k: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(k.try_into().map_err(|_| Error::new(ErrorKind::StateTampered, "invalid sequence key"))?))
}

pub struct StateStore {
    backend: Arc<dyn StateBackend>,
    sealer: Sealer,
    store_id: String,
    /// Serializes read-modify-write sequences (epoch, checkpoint).
    write: Mutex<()>,
    faults: Option<FaultInjector>,
}

impl std::fmt::Debug for StateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateStore").field("backend", &self.backend.name()).field("store_id", &self.store_id).finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, Default)]
pub struct Checkpoint {
    /// Highest journal sequence whose graph effects are committed. All lower
    /// sequences are also committed (contiguity is enforced).
    pub applied_seq: u64,
    /// Highest journal sequence ever assigned.
    pub last_seq: u64,
}

impl StateStore {
    /// Open (or create) the default redb-backed store in `dir`.
    pub fn open(dir: &Path, sealer: Sealer, faults: Option<FaultInjector>) -> Result<StateStore> {
        Self::with_backend(Arc::new(RedbBackend::open(dir)?), sealer, faults)
    }

    /// Open a store over a custom backend.
    pub fn with_backend(backend: Arc<dyn StateBackend>, sealer: Sealer, faults: Option<FaultInjector>) -> Result<StateStore> {
        let mut store = StateStore { backend, sealer, store_id: String::new(), write: Mutex::new(()), faults };
        store.init()?;
        Ok(store)
    }

    pub fn backend(&self) -> &Arc<dyn StateBackend> {
        &self.backend
    }

    fn init(&mut self) -> Result<()> {
        match self.backend.get(Table::Meta, b"store_id")? {
            Some(raw) => {
                let (_, id) = self.sealer.open(b"meta/store_id", &raw)?;
                self.store_id = String::from_utf8(id).map_err(|_| Error::new(ErrorKind::StateTampered, "store id"))?;
                let raw = self
                    .backend
                    .get(Table::Meta, b"format")?
                    .ok_or_else(|| Error::new(ErrorKind::StateTampered, "format marker missing"))?;
                let fmt: u32 = self.open_json("meta", "format", &raw)?;
                if fmt != FORMAT {
                    return Err(Error::new(ErrorKind::StateIo, format!("unsupported state format {fmt}; expected {FORMAT}")));
                }
            }
            None => {
                let mut rnd = [0u8; 16];
                self.sealer.provider().random(&mut rnd)?;
                self.store_id = hex::encode(rnd);
                let mut b = WriteBatch::default();
                b.put(Table::Meta, b"store_id".to_vec(), self.sealer.seal(b"meta/store_id", self.store_id.as_bytes())?);
                b.put(Table::Meta, b"format".to_vec(), self.seal_json("meta", "format", &FORMAT)?);
                self.backend.commit(b)?;
            }
        }
        Ok(())
    }

    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    fn ctx(&self, table: &str, key: &str) -> Vec<u8> {
        format!("{}/{table}/{key}", self.store_id).into_bytes()
    }

    fn seal_json<T: Serialize + ?Sized>(&self, table: &str, key: &str, v: &T) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(v).map_err(|e| Error::internal(e.to_string()))?;
        self.sealer.seal(&self.ctx(table, key), &bytes)
    }

    fn open_json<T: DeserializeOwned>(&self, table: &str, key: &str, raw: &[u8]) -> Result<T> {
        let (_, pt) = self.sealer.open(&self.ctx(table, key), raw)?;
        serde_json::from_slice(&pt).map_err(|e| Error::new(ErrorKind::StateTampered, format!("{table}/{key}: {e}")))
    }

    fn meta_get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.backend.get(Table::Meta, key.as_bytes())? {
            Some(raw) => Ok(Some(self.open_json("meta", key, &raw)?)),
            None => Ok(None),
        }
    }

    fn meta_put<T: Serialize + ?Sized>(&self, b: &mut WriteBatch, key: &str, v: &T) -> Result<()> {
        let sealed = self.seal_json("meta", key, v)?;
        b.put(Table::Meta, key.as_bytes().to_vec(), sealed);
        Ok(())
    }

    /// Add epoch and seal-counter updates to a batch. Caller holds `write`.
    fn bump(&self, b: &mut WriteBatch, seals: u64) -> Result<u64> {
        let epoch: u64 = self.meta_get("epoch")?.unwrap_or(0) + 1;
        self.meta_put(b, "epoch", &epoch)?;
        let ck = format!("seals:{}", self.sealer.active_key_id());
        let count: u64 = self.meta_get(&ck)?.unwrap_or(0) + seals + 2;
        if count > MAX_SEALS_PER_KEY {
            return Err(Error::new(
                ErrorKind::Security,
                "the active state key reached its encryption limit; rotate state keys (`db2graph-sync rotate-keys`)",
            ));
        }
        self.meta_put(b, &ck, &count)?;
        Ok(epoch)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn epoch(&self) -> Result<u64> {
        Ok(self.meta_get("epoch")?.unwrap_or(0))
    }

    pub fn get_meta<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.meta_get(key)
    }

    pub fn put_meta<T: Serialize>(&self, key: &str, v: &T) -> Result<u64> {
        let _g = self.lock();
        let mut b = WriteBatch::default();
        self.meta_put(&mut b, key, v)?;
        let epoch = self.bump(&mut b, 1)?;
        self.backend.commit(b)?;
        Ok(epoch)
    }

    /// Authorized recovery only: move the epoch past `evidence` in one commit,
    /// recording `note` under the `recovery` key.
    pub fn advance_epoch_past(&self, evidence: u64, note: &serde_json::Value) -> Result<u64> {
        let _g = self.lock();
        let next = self.epoch()?.max(evidence) + 1;
        let mut b = WriteBatch::default();
        self.meta_put(&mut b, "epoch", &next)?;
        self.meta_put(&mut b, "recovery", note)?;
        self.backend.commit(b)?;
        Ok(next)
    }

    pub fn checkpoint(&self) -> Result<Checkpoint> {
        Ok(self.meta_get("checkpoint")?.unwrap_or_default())
    }

    /// Durably append units to the journal and, in the same commit, apply
    /// metadata updates (acknowledgeable source position, snapshot progress).
    /// Returns the assigned sequence numbers and the new epoch.
    pub fn append_journal(&self, units: &[CapturedTransaction], meta_updates: &[(&str, serde_json::Value)]) -> Result<(Vec<u64>, u64)> {
        fault(&self.faults, "journal.before_commit")?;
        let _g = self.lock();
        let mut b = WriteBatch::default();
        let mut cp = self.checkpoint()?;
        let mut seqs = Vec::with_capacity(units.len());
        for u in units {
            cp.last_seq += 1;
            b.put(Table::Journal, k64(cp.last_seq).to_vec(), self.seal_json("journal", &cp.last_seq.to_string(), u)?);
            seqs.push(cp.last_seq);
        }
        self.meta_put(&mut b, "checkpoint", &cp)?;
        for (k, v) in meta_updates {
            self.meta_put(&mut b, k, v)?;
        }
        let epoch = self.bump(&mut b, units.len() as u64 + meta_updates.len() as u64 + 1)?;
        self.backend.commit(b)?;
        drop(_g);
        fault(&self.faults, "journal.after_commit")?;
        Ok((seqs, epoch))
    }

    /// Read up to `limit` journal entries with sequence > `after`.
    pub fn journal_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, CapturedTransaction)>> {
        self.backend
            .range(Table::Journal, &k64(after.saturating_add(1)), None, limit)?
            .into_iter()
            .map(|(k, v)| {
                let seq = from_k64(&k)?;
                Ok((seq, self.open_json("journal", &seq.to_string(), &v)?))
            })
            .collect()
    }

    pub fn journal_len(&self) -> Result<u64> {
        self.backend.len(Table::Journal)
    }

    /// Advance the applied checkpoint to `applied_seq` and compact the journal.
    /// Fails if this would create a gap (progress must be contiguous).
    pub fn advance_checkpoint(&self, applied_seq: u64) -> Result<u64> {
        fault(&self.faults, "checkpoint.before_commit")?;
        let _g = self.lock();
        let mut cp = self.checkpoint()?;
        if applied_seq <= cp.applied_seq {
            return self.epoch();
        }
        if applied_seq > cp.last_seq {
            return Err(Error::internal(format!("checkpoint {applied_seq} beyond last journal seq {}", cp.last_seq)));
        }
        let entries = self.backend.range(Table::Journal, &k64(cp.applied_seq + 1), Some(&k64(applied_seq + 1)), usize::MAX)?;
        let mut expected = cp.applied_seq + 1;
        let mut b = WriteBatch::default();
        for (k, _) in &entries {
            let seq = from_k64(k)?;
            if seq != expected {
                return Err(Error::internal(format!("checkpoint gap: expected seq {expected}, found {seq}")));
            }
            b.delete(Table::Journal, k.clone());
            expected += 1;
        }
        if expected != applied_seq + 1 {
            return Err(Error::internal(format!("checkpoint gap: journal ends at {}", expected - 1)));
        }
        cp.applied_seq = applied_seq;
        self.meta_put(&mut b, "checkpoint", &cp)?;
        let epoch = self.bump(&mut b, 1)?;
        self.backend.commit(b)?;
        Ok(epoch)
    }

    pub fn append_schema(&self, version: u64, schema: &crate::schema::SourceSchema) -> Result<()> {
        let _g = self.lock();
        let mut b = WriteBatch::default();
        b.put(Table::Schemas, k64(version).to_vec(), self.seal_json("schema_history", &version.to_string(), schema)?);
        self.bump(&mut b, 1)?;
        self.backend.commit(b)
    }

    pub fn latest_schema(&self) -> Result<Option<(u64, crate::schema::SourceSchema)>> {
        match self.backend.last(Table::Schemas)? {
            Some((k, v)) => {
                let ver = from_k64(&k)?;
                Ok(Some((ver, self.open_json("schema_history", &ver.to_string(), &v)?)))
            }
            None => Ok(None),
        }
    }

    /// Append audit records to the encrypted spool. Fails with
    /// `AuditUnavailable` when the spool is full.
    pub fn spool_audit(&self, records: &[crate::audit::AuditRecord], max: u64) -> Result<()> {
        let _g = self.lock();
        if self.backend.len(Table::Audit)? + records.len() as u64 > max {
            return Err(Error::new(ErrorKind::AuditUnavailable, "audit spool limit reached"));
        }
        let mut next = match self.backend.last(Table::Audit)? {
            Some((k, _)) => from_k64(&k)? + 1,
            None => self.meta_get::<u64>("audit_next")?.unwrap_or(1),
        };
        let mut b = WriteBatch::default();
        for r in records {
            b.put(Table::Audit, k64(next).to_vec(), self.seal_json("audit_spool", &next.to_string(), r)?);
            next += 1;
        }
        self.meta_put(&mut b, "audit_next", &next)?;
        self.bump(&mut b, records.len() as u64 + 1)?;
        self.backend.commit(b)
    }

    pub fn audit_spooled(&self, limit: usize) -> Result<Vec<(u64, crate::audit::AuditRecord)>> {
        self.backend
            .range(Table::Audit, &k64(0), None, limit)?
            .into_iter()
            .map(|(k, v)| {
                let seq = from_k64(&k)?;
                Ok((seq, self.open_json("audit_spool", &seq.to_string(), &v)?))
            })
            .collect()
    }

    pub fn audit_spool_len(&self) -> Result<u64> {
        self.backend.len(Table::Audit)
    }

    pub fn audit_ack(&self, upto: u64) -> Result<()> {
        let _g = self.lock();
        let mut b = WriteBatch::default();
        for (k, _) in self.backend.range(Table::Audit, &k64(0), Some(&k64(upto.saturating_add(1))), usize::MAX)? {
            b.delete(Table::Audit, k);
        }
        if b.ops.is_empty() {
            return Ok(());
        }
        self.backend.commit(b)
    }

    /// Re-encrypt every record under the active key. Returns records rewritten.
    pub fn rotate_keys(&self) -> Result<u64> {
        let _g = self.lock();
        let active = self.sealer.active_key_id();
        let mut b = WriteBatch::default();
        for (k, raw) in self.backend.range(Table::Meta, &[], None, usize::MAX)? {
            let key = String::from_utf8(k.clone()).map_err(|_| Error::new(ErrorKind::StateTampered, "meta key"))?;
            let ctx: Vec<u8> = if key == "store_id" { b"meta/store_id".to_vec() } else { self.ctx("meta", &key) };
            let (kid, pt) = self.sealer.open(&ctx, &raw)?;
            if kid != active {
                b.put(Table::Meta, k, self.sealer.seal(&ctx, &pt)?);
            }
        }
        for t in [Table::Journal, Table::Schemas, Table::Audit] {
            for (k, raw) in self.backend.range(t, &[], None, usize::MAX)? {
                let ctx = self.ctx(t.name(), &from_k64(&k)?.to_string());
                let (kid, pt) = self.sealer.open(&ctx, &raw)?;
                if kid != active {
                    b.put(t, k, self.sealer.seal(&ctx, &pt)?);
                }
            }
        }
        let n = b.ops.len() as u64;
        self.bump(&mut b, n)?;
        self.backend.commit(b)?;
        Ok(n)
    }

    /// Write the external anchor (rollback evidence) atomically: temp file,
    /// fsync, rename.
    pub fn write_anchor(&self, path: &Path, epoch: u64) -> Result<()> {
        // The key id is part of the authenticated body so the anchor still
        // verifies after `rotate-keys` (old keys remain in the keyring).
        let kid = self.sealer.active_key_id();
        let body = format!("{}:{epoch}:{kid}", self.store_id);
        let mac = hex::encode(self.sealer.mac_with(kid, b"anchor", body.as_bytes())?);
        let tmp = path.with_extension("tmp");
        {
            use std::io::Write;
            let mut f = File::create(&tmp)?;
            f.write_all(format!("{body}:{mac}\n").as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Verify the anchor: the store epoch must be >= the anchored epoch.
    pub fn check_anchor(&self, path: &Path) -> Result<()> {
        let s = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let parts: Vec<&str> = s.trim().split(':').collect();
        if parts.len() != 4 {
            return Err(Error::new(ErrorKind::StateTampered, "anchor file is malformed"));
        }
        let kid: u32 = parts[2].parse().map_err(|_| Error::new(ErrorKind::StateTampered, "anchor key id"))?;
        let body = format!("{}:{}:{kid}", parts[0], parts[1]);
        let mac = hex::encode(self.sealer.mac_with(kid, b"anchor", body.as_bytes())?);
        if !bool::from(subtle::ConstantTimeEq::ct_eq(mac.as_bytes(), parts[3].as_bytes())) {
            return Err(Error::new(ErrorKind::StateTampered, "anchor file failed authentication"));
        }
        if parts[0] != self.store_id {
            return Err(Error::new(ErrorKind::StateRollback, "anchor belongs to a different state store"));
        }
        let anchored: u64 = parts[1].parse().map_err(|_| Error::new(ErrorKind::StateTampered, "anchor epoch"))?;
        let epoch = self.epoch()?;
        if epoch < anchored {
            return Err(Error::new(
                ErrorKind::StateRollback,
                format!("state epoch {epoch} is older than anchored epoch {anchored}: the state directory was restored or rolled back"),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{Keyring, RustCryptoProvider};
    use crate::types::*;

    pub(crate) fn sealer() -> Sealer {
        let mut kr = Keyring::new(1);
        kr.add_base64(1, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
        Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr))
    }

    fn unit(n: u64) -> CapturedTransaction {
        let scope = OrderingScope { source_id: "s".into(), epoch: 0, partition: String::new() };
        CapturedTransaction {
            unit_id: format!("u{n}"),
            kind: UnitKind::Transaction,
            source_id: "s".into(),
            scope: scope.clone(),
            ack_position: SourcePosition { scope, ordinal: Some(n), native: n.to_string() },
            base_version: Version { epoch: 0, position: n, ordinal: 0 },
            native_tx_id: None,
            commit_time: None,
            items: vec![],
            mapping_version: String::new(),
            policy_version: String::new(),
        }
    }

    #[test]
    fn journal_checkpoint_contiguity_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = StateStore::open(dir.path(), sealer(), None).unwrap();
            let (seqs, _) = s.append_journal(&[unit(1), unit(2), unit(3)], &[("ack_position", serde_json::json!("ack3"))]).unwrap();
            assert_eq!(seqs, vec![1, 2, 3]);
            s.advance_checkpoint(2).unwrap();
            assert_eq!(s.checkpoint().unwrap(), Checkpoint { applied_seq: 2, last_seq: 3 });
            s.advance_checkpoint(1).unwrap();
            assert!(s.advance_checkpoint(9).is_err());
        }
        let s = StateStore::open(dir.path(), sealer(), None).unwrap();
        let rest = s.journal_after(s.checkpoint().unwrap().applied_seq, 10).unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].1.unit_id, "u3");
        assert_eq!(s.get_meta::<String>("ack_position").unwrap().as_deref(), Some("ack3"));
    }

    #[test]
    fn checkpoint_gap_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let s = StateStore::open(dir.path(), sealer(), None).unwrap();
        s.append_journal(&[unit(1), unit(2), unit(3)], &[]).unwrap();
        // Remove seq 2 behind the store's back: advancing past it must fail.
        let mut b = WriteBatch::default();
        b.delete(Table::Journal, k64(2).to_vec());
        s.backend.commit(b).unwrap();
        assert!(s.advance_checkpoint(3).unwrap_err().message().contains("gap"));
        assert_eq!(s.checkpoint().unwrap().applied_seq, 0, "no partial progress recorded");
    }

    #[test]
    fn second_open_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let _a = StateStore::open(dir.path(), sealer(), None).unwrap();
        let e = StateStore::open(dir.path(), sealer(), None).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::OwnershipConflict);
    }

    #[test]
    fn wrong_key_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        {
            StateStore::open(dir.path(), sealer(), None).unwrap();
        }
        let mut kr = Keyring::new(1);
        kr.add_base64(1, "HxwdHh8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo=").unwrap();
        let other = Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr));
        let e = StateStore::open(dir.path(), other, None).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::StateTampered);
    }

    #[test]
    fn rollback_detected_via_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let backup = tempfile::tempdir().unwrap();
        let anchor = tempfile::tempdir().unwrap();
        let anchor_path = anchor.path().join("anchor");
        {
            let s = StateStore::open(dir.path(), sealer(), None).unwrap();
            s.append_journal(&[unit(1)], &[]).unwrap();
        }
        std::fs::copy(dir.path().join("state.redb"), backup.path().join("state.redb")).unwrap();
        {
            let s = StateStore::open(dir.path(), sealer(), None).unwrap();
            let (_, e) = s.append_journal(&[unit(2)], &[]).unwrap();
            s.write_anchor(&anchor_path, e).unwrap();
            s.check_anchor(&anchor_path).unwrap();
        }
        std::fs::copy(backup.path().join("state.redb"), dir.path().join("state.redb")).unwrap();
        let s = StateStore::open(dir.path(), sealer(), None).unwrap();
        assert_eq!(s.check_anchor(&anchor_path).unwrap_err().kind(), ErrorKind::StateRollback);
    }

    #[test]
    fn anchor_survives_key_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let anchor = dir.path().join("anchor");
        {
            let s = StateStore::open(&dir.path().join("s"), sealer(), None).unwrap();
            let (_, e) = s.append_journal(&[unit(1)], &[]).unwrap();
            s.write_anchor(&anchor, e).unwrap();
        }
        let mut kr = Keyring::new(2);
        kr.add_base64(1, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
        kr.add_base64(2, "HxwdHh8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo=").unwrap();
        let s = StateStore::open(&dir.path().join("s"), Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr)), None).unwrap();
        s.rotate_keys().unwrap();
        s.check_anchor(&anchor).unwrap();
        // Tampering with the key id is detected.
        let text = std::fs::read_to_string(&anchor).unwrap();
        let mut parts: Vec<&str> = text.trim().split(':').collect();
        parts[2] = "2";
        std::fs::write(&anchor, parts.join(":")).unwrap();
        assert_eq!(s.check_anchor(&anchor).unwrap_err().kind(), ErrorKind::StateTampered);
    }

    #[test]
    fn substituted_record_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let s = StateStore::open(dir.path(), sealer(), None).unwrap();
        s.append_journal(&[unit(1)], &[]).unwrap();
        // Move a validly sealed record to another key: AAD binding catches it.
        let raw = s.backend.get(Table::Journal, &k64(1)).unwrap().unwrap();
        let mut b = WriteBatch::default();
        b.put(Table::Journal, k64(2).to_vec(), raw);
        s.backend.commit(b).unwrap();
        assert_eq!(s.journal_after(1, 10).unwrap_err().kind(), ErrorKind::StateTampered);
    }

    #[test]
    fn corrupted_bytes_are_detected() {
        let dir = tempfile::tempdir().unwrap();
        let s = StateStore::open(dir.path(), sealer(), None).unwrap();
        s.append_journal(&[unit(1)], &[]).unwrap();
        let mut raw = s.backend.get(Table::Journal, &k64(1)).unwrap().unwrap();
        let n = raw.len() / 2;
        raw[n] ^= 0x01;
        let mut b = WriteBatch::default();
        b.put(Table::Journal, k64(1).to_vec(), raw);
        s.backend.commit(b).unwrap();
        assert_eq!(s.journal_after(0, 10).unwrap_err().kind(), ErrorKind::StateTampered);
    }

    #[test]
    fn audit_spool_limit_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let s = StateStore::open(dir.path(), sealer(), None).unwrap();
        let r = crate::audit::AuditRecord::new("test", "x", serde_json::json!({}));
        s.spool_audit(&[r.clone(), r.clone()], 2).unwrap();
        assert_eq!(s.spool_audit(&[r], 2).unwrap_err().kind(), ErrorKind::AuditUnavailable);
    }

    #[test]
    fn key_rotation_rewrites_records() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = StateStore::open(dir.path(), sealer(), None).unwrap();
            s.append_journal(&[unit(1)], &[]).unwrap();
        }
        let mut kr = Keyring::new(2);
        kr.add_base64(1, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
        kr.add_base64(2, "HxwdHh8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo=").unwrap();
        let both = Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr));
        {
            let s = StateStore::open(dir.path(), both, None).unwrap();
            assert!(s.rotate_keys().unwrap() > 0);
        }
        let mut kr = Keyring::new(2);
        kr.add_base64(2, "HxwdHh8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo=").unwrap();
        let only_new = Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr));
        let s = StateStore::open(dir.path(), only_new, None).unwrap();
        assert_eq!(s.journal_after(0, 10).unwrap().len(), 1);
    }
}
