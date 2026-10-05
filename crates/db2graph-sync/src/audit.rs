//! Audit records and sinks.
//!
//! Audit records are first written to the encrypted state spool (same
//! durability as the journal), then forwarded to the configured
//! [`AuditSink`]s and acknowledged. If the spool reaches
//! `audit.spool_max_records` (for example because forwarding is failing),
//! protected work stops with `AuditUnavailable` instead of proceeding
//! unaudited.
//!
//! The built-in [`FileAuditSink`] writes JSON lines, each carrying an HMAC
//! that chains to the previous record, so deletion, reordering or edits are
//! detectable with `db2graph-sync audit-verify`.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::crypto::Sealer;
use crate::error::{Error, ErrorKind, Result};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    /// RFC 3339 timestamp.
    pub time: String,
    /// Category: `sync`, `policy`, `operation`, `failure`, `recovery`, `security`.
    pub category: String,
    pub action: String,
    pub detail: serde_json::Value,
}

impl AuditRecord {
    pub fn new(category: &str, action: &str, detail: serde_json::Value) -> Self {
        Self {
            time: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            category: category.to_owned(),
            action: action.to_owned(),
            detail,
        }
    }
}

/// Destination for audit records. `deliver` must be durable when it returns
/// `Ok` (records are removed from the spool afterwards).
#[async_trait]
pub trait AuditSink: Send + Sync {
    fn name(&self) -> &str;
    /// Whether delivery is authenticated/integrity protected end to end.
    fn authenticated(&self) -> bool;
    async fn deliver(&self, records: &[AuditRecord]) -> Result<()>;
}

/// Append-only, HMAC-chained JSON-lines file.
pub struct FileAuditSink {
    path: PathBuf,
    sealer: Sealer,
    state: tokio::sync::Mutex<Option<String>>,
}

#[derive(Serialize, Deserialize)]
struct ChainedLine {
    #[serde(flatten)]
    record: AuditRecord,
    prev: String,
    /// State key version that produced `mac` (bound into the MAC input).
    kid: u32,
    mac: String,
}

fn mac_context(prev: &str, kid: u32) -> Vec<u8> {
    format!("{prev}|kid={kid}").into_bytes()
}

impl FileAuditSink {
    pub fn new(path: impl Into<PathBuf>, sealer: Sealer) -> Self {
        Self { path: path.into(), sealer, state: tokio::sync::Mutex::new(None) }
    }

    fn last_mac(path: &Path) -> Result<String> {
        match std::fs::read_to_string(path) {
            Ok(s) => Ok(s
                .lines()
                .rev()
                .find(|l| !l.trim().is_empty())
                .and_then(|l| serde_json::from_str::<ChainedLine>(l).ok())
                .map(|l| l.mac)
                .unwrap_or_else(|| "genesis".to_owned())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok("genesis".to_owned()),
            Err(e) => Err(Error::new(ErrorKind::AuditUnavailable, format!("audit file: {e}"))),
        }
    }

    /// Verify the full chain. Returns the number of records.
    pub fn verify(path: &Path, sealer: &Sealer) -> Result<u64> {
        let s = std::fs::read_to_string(path).map_err(|e| Error::new(ErrorKind::AuditUnavailable, e.to_string()))?;
        let mut prev = "genesis".to_owned();
        let mut n = 0;
        for (i, line) in s.lines().filter(|l| !l.trim().is_empty()).enumerate() {
            let l: ChainedLine =
                serde_json::from_str(line).map_err(|e| Error::new(ErrorKind::StateTampered, format!("audit line {}: {e}", i + 1)))?;
            if l.prev != prev {
                return Err(Error::new(ErrorKind::StateTampered, format!("audit chain broken at line {}", i + 1)));
            }
            let body = serde_json::to_vec(&l.record).map_err(|e| Error::internal(e.to_string()))?;
            let mac = hex::encode(sealer.mac_with(l.kid, &mac_context(&prev, l.kid), &body)?);
            if mac != l.mac {
                return Err(Error::new(ErrorKind::StateTampered, format!("audit MAC mismatch at line {}", i + 1)));
            }
            prev = l.mac;
            n += 1;
        }
        Ok(n)
    }
}

#[async_trait]
impl AuditSink for FileAuditSink {
    fn name(&self) -> &str {
        "file"
    }
    fn authenticated(&self) -> bool {
        true
    }
    async fn deliver(&self, records: &[AuditRecord]) -> Result<()> {
        let mut guard = self.state.lock().await;
        let path = self.path.clone();
        let mut prev = match guard.clone() {
            Some(p) => p,
            None => {
                let p = path.clone();
                tokio::task::spawn_blocking(move || Self::last_mac(&p)).await.map_err(|e| Error::internal(e.to_string()))??
            }
        };
        let mut buf = String::new();
        for r in records {
            let body = serde_json::to_vec(r).map_err(|e| Error::internal(e.to_string()))?;
            let kid = self.sealer.active_key_id();
            let mac = hex::encode(self.sealer.mac_with(kid, &mac_context(&prev, kid), &body)?);
            let line = ChainedLine { record: r.clone(), prev: prev.clone(), kid, mac: mac.clone() };
            buf.push_str(&serde_json::to_string(&line).map_err(|e| Error::internal(e.to_string()))?);
            buf.push('\n');
            prev = mac;
        }
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
            f.write_all(buf.as_bytes())?;
            f.sync_data()
        })
        .await
        .map_err(|e| Error::internal(e.to_string()))?
        .map_err(|e| Error::new(ErrorKind::AuditUnavailable, format!("audit file write failed: {e}")))?;
        *guard = Some(prev);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{Keyring, RustCryptoProvider};
    use std::sync::Arc;

    fn sealer() -> Sealer {
        let mut kr = Keyring::new(1);
        kr.add_base64(1, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
        Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr))
    }

    #[tokio::test]
    async fn chain_verifies_and_detects_edits() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("audit.jsonl");
        let sink = FileAuditSink::new(&p, sealer());
        sink.deliver(&[AuditRecord::new("sync", "a", serde_json::json!({"n":1}))]).await.unwrap();
        // A new sink instance continues the existing chain.
        let sink2 = FileAuditSink::new(&p, sealer());
        sink2.deliver(&[AuditRecord::new("sync", "b", serde_json::json!({"n":2}))]).await.unwrap();
        assert_eq!(FileAuditSink::verify(&p, &sealer()).unwrap(), 2);
        let s = std::fs::read_to_string(&p).unwrap().replace("\"n\":1", "\"n\":9");
        std::fs::write(&p, s).unwrap();
        assert_eq!(FileAuditSink::verify(&p, &sealer()).unwrap_err().kind(), ErrorKind::StateTampered);
    }

    #[tokio::test]
    async fn chain_survives_key_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("audit.jsonl");
        FileAuditSink::new(&p, sealer()).deliver(&[AuditRecord::new("sync", "before", serde_json::json!({}))]).await.unwrap();
        let mut kr = Keyring::new(2);
        kr.add_base64(1, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
        kr.add_base64(2, "HxwdHh8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo=").unwrap();
        let rotated = Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr));
        FileAuditSink::new(&p, rotated.clone()).deliver(&[AuditRecord::new("sync", "after", serde_json::json!({}))]).await.unwrap();
        assert_eq!(FileAuditSink::verify(&p, &rotated).unwrap(), 2);
        // Relabelling a line with a different key id breaks verification.
        let s = std::fs::read_to_string(&p).unwrap().replacen("\"kid\":1", "\"kid\":2", 1);
        std::fs::write(&p, s).unwrap();
        assert!(FileAuditSink::verify(&p, &rotated).is_err());
    }

    #[tokio::test]
    async fn deleted_line_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("audit.jsonl");
        let sink = FileAuditSink::new(&p, sealer());
        for i in 0..3 {
            sink.deliver(&[AuditRecord::new("sync", "x", serde_json::json!({"i":i}))]).await.unwrap();
        }
        let lines: Vec<String> = std::fs::read_to_string(&p).unwrap().lines().map(String::from).collect();
        std::fs::write(&p, format!("{}\n{}\n", lines[0], lines[2])).unwrap();
        assert!(FileAuditSink::verify(&p, &sealer()).is_err());
    }
}
