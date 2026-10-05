//! A custom durable state backend: all tables in one file, replaced
//! atomically (write temp file, fsync, rename) on each commit, so a batch is
//! all-or-nothing. It is simple and slow; it demonstrates the backend
//! contract, not a production store. The backend only ever sees ciphertext.
//!
//! `cargo run --example custom_state_backend`

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::state::{StateBackend, Table, WriteBatch};
use db2graph_sync::{Error, ErrorKind, Result, StartOptions};

type Tables = BTreeMap<Table, BTreeMap<Vec<u8>, Vec<u8>>>;

struct FileBackend {
    dir: PathBuf,
    tables: Mutex<Tables>,
}

fn io(e: std::io::Error) -> Error {
    Error::new(ErrorKind::StateIo, e.to_string())
}

impl FileBackend {
    const TABLES: [Table; 4] = [Table::Meta, Table::Journal, Table::Schemas, Table::Audit];

    fn open(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir).map_err(io)?;
        let on_disk: BTreeMap<String, BTreeMap<String, String>> = match std::fs::read(dir.join("state.json")) {
            Ok(b) => serde_json::from_slice(&b).map_err(|e| Error::new(ErrorKind::StateTampered, e.to_string()))?,
            Err(_) => BTreeMap::new(),
        };
        let mut tables = Tables::new();
        for t in Self::TABLES {
            let decoded = on_disk
                .get(t.name())
                .into_iter()
                .flatten()
                .map(|(k, v)| {
                    Ok((
                        STANDARD.decode(k).map_err(|e| Error::internal(e.to_string()))?,
                        STANDARD.decode(v).map_err(|e| Error::internal(e.to_string()))?,
                    ))
                })
                .collect::<Result<_>>()?;
            tables.insert(t, decoded);
        }
        Ok(Self { dir, tables: Mutex::new(tables) })
    }

    /// Persist every table in one file, replaced atomically.
    fn persist(&self, all: &Tables) -> Result<()> {
        use std::io::Write;
        let enc: BTreeMap<&str, BTreeMap<String, String>> =
            all.iter().map(|(t, data)| (t.name(), data.iter().map(|(k, v)| (STANDARD.encode(k), STANDARD.encode(v))).collect())).collect();
        let path = self.dir.join("state.json");
        let tmp = path.with_extension("tmp");
        let mut f = std::fs::File::create(&tmp).map_err(io)?;
        f.write_all(&serde_json::to_vec(&enc).map_err(|e| Error::internal(e.to_string()))?).map_err(io)?;
        f.sync_all().map_err(io)?;
        std::fs::rename(&tmp, &path).map_err(io)?;
        Ok(())
    }
}

impl StateBackend for FileBackend {
    fn name(&self) -> &str {
        "example-files"
    }
    fn durable(&self) -> bool {
        true
    }
    fn exclusive(&self) -> bool {
        false // a real backend would lock; the host must ensure a single owner
    }
    fn commit(&self, batch: WriteBatch) -> Result<()> {
        let mut g = self.tables.lock().expect("poisoned");
        // Apply to a copy, persist atomically, then publish in memory.
        let mut next = g.clone();
        for op in batch.ops {
            let t = next.entry(op.table).or_default();
            match op.value {
                Some(v) => t.insert(op.key, v),
                None => t.remove(&op.key),
            };
        }
        self.persist(&next)?;
        *g = next;
        Ok(())
    }
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.tables.lock().expect("poisoned").get(&table).and_then(|t| t.get(key).cloned()))
    }
    fn range(&self, table: Table, start: &[u8], end: Option<&[u8]>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let g = self.tables.lock().expect("poisoned");
        Ok(g.get(&table)
            .map(|t| {
                t.range(start.to_vec()..)
                    .take_while(|(k, _)| end.is_none_or(|e| k.as_slice() < e))
                    .take(limit)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default())
    }
    fn last(&self, table: Table) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(self.tables.lock().expect("poisoned").get(&table).and_then(|t| t.last_key_value().map(|(k, v)| (k.clone(), v.clone()))))
    }
    fn len(&self, table: Table) -> Result<u64> {
        Ok(self.tables.lock().expect("poisoned").get(&table).map(|t| t.len() as u64).unwrap_or(0))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir().map_err(io)?;
    let source = ScriptedSource::new(common::shop_schema());
    let graph = MemoryGraph::new();
    for run in 1..=2 {
        let backend = Arc::new(FileBackend::open(dir.path().join("files"))?);
        let sync = db2graph_sync::start_with(
            common::dev_config(dir.path()),
            StartOptions {
                source: Some(Arc::new(source.clone())),
                graph_sink: Some(Arc::new(graph.clone())),
                state_backend: Some(backend),
                ..Default::default()
            },
        )
        .await?;
        sync.ready(Duration::from_secs(10)).await?;
        source.commit(source.unit(run, vec![common::insert("public.customers", 0, &[("id", (run as i64).into()), ("name", "x".into())])]));
        common::wait(|| graph.state("example").nodes.len() as u64 >= run).await;
        let st = sync.status();
        sync.shutdown(Duration::from_secs(10)).await?;
        println!("run {run}: state epoch {} acked {:?}", st.state_epoch, st.acked_position);
    }
    // Ciphertext only: the plaintext name never appears in the files.
    let on_disk = std::fs::read_to_string(dir.path().join("files/state.json")).map_err(io)?;
    assert!(!on_disk.contains("customers"));
    println!("files contain only sealed records");
    Ok(())
}
