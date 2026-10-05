//! Engine acceptance tests with the scripted source and in-memory graph.
//! Synthetic data only. These need no external services.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::*;
use db2graph_sync::config::Profile;
use db2graph_sync::error::{Error, ErrorKind};
use db2graph_sync::sink::memory::MemoryGraph;
use db2graph_sync::source::scripted::ScriptedSource;
use db2graph_sync::types::Value;
use db2graph_sync::{Phase, start_with};

const T: Duration = Duration::from_secs(10);

fn applied(handle: &db2graph_sync::SyncHandle) -> u64 {
    handle.status().counters.units_applied
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_snapshot_then_stream_then_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    src.set_snapshot(vec![
        snapshot_chunk(&src, "public.customers", vec![read("public.customers", &customer(1, "Ada"))]),
        snapshot_chunk(&src, "public.orders", vec![read("public.orders", &order(10, 1))]),
    ]);
    let graph = MemoryGraph::new();

    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    sync.ready(T).await.unwrap();
    let st = sync.status();
    assert_eq!(st.phase, Phase::Streaming);
    assert_eq!(st.snapshot.as_ref().unwrap().rows, 2);

    // Order for a customer that does not exist yet, then the customer.
    src.commit(src.unit(100, vec![ins("public.orders", 0, &order(11, 2))]));
    src.commit(src.unit(101, vec![ins("public.customers", 0, &customer(2, "Grace"))]));
    wait_for("stream applied", || applied(&sync) >= 4).await;

    let g = graph.state("test-pipeline");
    assert_eq!(g.nodes.len(), 4);
    assert!(g.pending.is_empty(), "pending relationship resolved when its endpoint arrived");
    let o11 = entity("public.orders", 11);
    assert!(g.edges.values().any(|e| e.from_id == o11 && e.to_id == entity("public.customers", 2)));
    // Denied by default? Development has no dataset policy, so ssn is projected
    // here; see `policy_filtering_*` for the enterprise behaviour.
    assert_eq!(g.marker.active_generation, Some(1));

    let report = sync.shutdown(T).await.unwrap();
    assert!(report.drained);
    assert_eq!(report.remaining_journal_units, 0);
    assert_eq!(report.final_status.phase, Phase::Stopped);
    assert_eq!(src.acked(), Some(101), "source acknowledged only after journaling");
}

/// Fault injector that fails once at `point`.
fn fail_once(point: &'static str) -> (Arc<AtomicU32>, db2graph_sync::state::FaultInjector) {
    let hits = Arc::new(AtomicU32::new(0));
    let h = hits.clone();
    let f: db2graph_sync::state::FaultInjector = Arc::new(move |p| {
        if p == point && h.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(Error::new(ErrorKind::Internal, format!("injected crash at {p}")));
        }
        Ok(())
    });
    (hits, f)
}

async fn run_crash_case(point: &'static str) -> (MemoryGraph, ScriptedSource, db2graph_sync::Status) {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let (_hits, injector) = fail_once(point);

    let mut o = opts(&src, &graph);
    o.fault_injector = Some(injector);
    let mut cfg = config(root.path(), Profile::Development);
    cfg.engine.max_batch_transactions = 1;
    let sync = start_with(cfg, o).await.unwrap();
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada")), ins("public.orders", 1, &order(10, 1))]));
    let err = tokio::time::timeout(T, sync.failed()).await.expect("failure is observable");
    assert!(err.message().contains("injected crash"), "{err}");
    let done = sync.wait().await;
    assert!(done.is_err(), "wait reports the terminal error");

    // "Restart" on the same state directory and graph.
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    sync.ready(T).await.unwrap();
    // Wait for the replay itself, not just for nodes that may already exist.
    wait_for("recovered", || graph.state("test-pipeline").nodes.len() == 2 && sync.status().counters.units_applied >= 1).await;
    let st = sync.status();
    sync.shutdown(T).await.unwrap();
    (graph, src, st)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_graph_commit_before_checkpoint_replays_as_duplicate() {
    let (graph, _src, st) = run_crash_case("graph.after_commit").await;
    assert!(st.counters.duplicates >= 1, "replayed unit recognized by its receipt: {:?}", st.counters);
    let g = graph.state("test-pipeline");
    assert_eq!(g.receipts.len(), 1);
    assert_eq!(g.edges.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_graph_commit_exposes_nothing_then_applies_once() {
    let (graph, _src, st) = run_crash_case("graph.before_commit").await;
    assert_eq!(st.counters.duplicates, 0);
    assert_eq!(graph.state("test-pipeline").receipts.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_journal_commit_is_redelivered_by_source() {
    let (graph, src, _st) = run_crash_case("journal.before_commit").await;
    assert_eq!(graph.state("test-pipeline").receipts.len(), 1);
    assert_eq!(src.acked(), Some(100));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_at_checkpoint_commit_recovers() {
    let (graph, _src, st) = run_crash_case("checkpoint.before_commit").await;
    assert!(st.counters.duplicates >= 1);
    assert_eq!(graph.state("test-pipeline").nodes.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_failures_are_retried_not_terminal() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    sync.ready(T).await.unwrap();
    graph.fail_next_applies(3);
    src.fail_next_reads(2);
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    wait_for("applied after retries", || applied(&sync) >= 1).await;
    let st = sync.status();
    assert!(st.failure.is_none());
    assert!(st.counters.graph_retries >= 3, "{:?}", st.counters);
    assert!(st.counters.source_reconnects >= 1, "{:?}", st.counters);
    sync.shutdown(T).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_engine_on_same_state_dir_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let a = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    let e = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::OwnershipConflict);
    a.shutdown(T).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_brain_older_owner_is_fenced_out() {
    let root_a = tempfile::tempdir().unwrap();
    let root_b = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let a = start_with(config(root_a.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    a.ready(T).await.unwrap();
    // A second instance (different state dir, same pipeline) takes over.
    let b = start_with(config(root_b.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    b.ready(T).await.unwrap();
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    let err = tokio::time::timeout(T, a.failed()).await.expect("old owner fails");
    assert_eq!(err.kind(), ErrorKind::FencedOut);
    wait_for("new owner applies", || applied(&b) >= 1).await;
    assert_eq!(graph.state("test-pipeline").nodes.len(), 1);
    b.shutdown(T).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_loss_is_detected_as_rollback() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    // A snapshot is present so recovery must re-snapshot under a new
    // generation without colliding with the first generation's receipts.
    src.set_snapshot(vec![snapshot_chunk(&src, "public.customers", vec![read("public.customers", &customer(9, "Snap"))])]);
    let graph = MemoryGraph::new();
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    sync.ready(T).await.unwrap();
    wait_for("applied", || applied(&sync) >= 2).await;
    sync.shutdown(T).await.unwrap();
    assert_eq!(graph.state("test-pipeline").marker.active_generation, Some(1), "first snapshot activated");
    // Replace state with a fresh, valid but older (empty) store.
    std::fs::remove_dir_all(root.path().join("state")).unwrap();
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    let e = sync.ready(T).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::StateRollback);
    let _ = sync.wait().await;
    // The source now holds both rows, so the re-snapshot reads both (as a
    // real source would after the streamed insert).
    src.set_snapshot(vec![snapshot_chunk(
        &src,
        "public.customers",
        vec![read("public.customers", &customer(1, "Ada")), read("public.customers", &customer(9, "Snap"))],
    )]);
    // Authorized recovery accepts the target's evidence; the engine then runs.
    let mut o = opts(&src, &graph);
    o.source = None;
    db2graph_sync::admin::accept_state_reset(&config(root.path(), Profile::Development), o, "test: state volume lost").await.unwrap();
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    sync.ready(T).await.unwrap();
    let st = sync.status();
    assert!(st.failure.is_none());
    assert_eq!(st.snapshot.as_ref().map(|s| s.generation), Some(2), "re-snapshot uses a new generation");
    sync.shutdown(T).await.unwrap();
    assert_eq!(graph.state("test-pipeline").marker.active_generation, Some(2));
    assert_eq!(graph.state("test-pipeline").nodes.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_instance_cannot_fence_out_healthy_owner() {
    let root_a = tempfile::tempdir().unwrap();
    let root_b = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let a = start_with(config(root_a.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    wait_for("applied", || applied(&a) >= 1).await;
    // B has empty (older) state: it must fail on rollback evidence WITHOUT
    // taking the fence from A.
    let b = start_with(config(root_b.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    assert_eq!(b.ready(T).await.unwrap_err().kind(), ErrorKind::StateRollback);
    let _ = b.wait().await;
    src.commit(src.unit(101, vec![ins("public.customers", 0, &customer(2, "Grace"))]));
    wait_for("healthy owner keeps applying", || applied(&a) >= 2).await;
    assert!(a.status().failure.is_none());
    a.shutdown(T).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_identity_change_requires_and_accepts_authorized_recovery() {
    let root = tempfile::tempdir().unwrap();
    let graph = MemoryGraph::new();
    let src = ScriptedSource::new(schema());
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    wait_for("applied", || applied(&sync) >= 1).await;
    sync.shutdown(T).await.unwrap();

    // The source is restored onto a different system.
    let mut other = schema();
    other.source_id = "scripted:2".into();
    let src2 = ScriptedSource::new(other);
    let sync = start_with(config(root.path(), Profile::Development), opts(&src2, &graph)).await.unwrap();
    assert_eq!(sync.ready(T).await.unwrap_err().kind(), ErrorKind::SourceIdentityChanged);
    let _ = sync.wait().await;

    let epoch = db2graph_sync::admin::accept_source_change(
        &config(root.path(), Profile::Development),
        db2graph_sync::StartOptions::default(),
        "test: restored to new cluster",
    )
    .await
    .unwrap();
    assert_eq!(epoch, 1);
    let sync = start_with(config(root.path(), Profile::Development), opts(&src2, &graph)).await.unwrap();
    sync.ready(T).await.unwrap();
    // Positions restart in the new history; epoch 1 versions supersede epoch 0.
    let mut u = src2.unit(5, vec![ins("public.customers", 0, &customer(1, "Ada (restored)"))]);
    u.base_version.epoch = 1;
    u.scope.epoch = 1;
    src2.commit(u);
    wait_for("applied in new epoch", || applied(&sync) >= 1).await;
    sync.shutdown(T).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_journal_commit_before_source_ack_replays() {
    let (graph, src, _st) = run_crash_case("journal.after_commit").await;
    assert_eq!(graph.state("test-pipeline").receipts.len(), 1, "applied exactly once");
    assert_eq!(src.acked(), Some(100));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_profile_refuses_builtin_vector_components() {
    use db2graph_sync::config::{EmbeddingConfig, VectorConfig, VectorSinkConfig};
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let mut cfg = config(root.path(), Profile::Restricted);
    cfg.policy = toml::from_str("[[datasets]]\nrelation = \"public.customers\"\nallow_fields = [\"id\"]\n").unwrap();
    cfg.mapping.approved_digest = Some("x".into());
    cfg.audit.file = Some(root.path().join("audit.jsonl"));
    cfg.vector = Some(VectorConfig {
        sink: VectorSinkConfig::Qdrant { url: "https://q.example.test".into(), collection: "c".into(), api_key: None },
        embedding: EmbeddingConfig::Custom(Default::default()),
        templates: vec![],
        max_embedding_requests_per_minute: 1,
        batch_size: 1,
    });
    // A provider claiming validation, and custom source/sink: still refused
    // because the vector sink would be the built-in one.
    struct Claimed;
    impl db2graph_sync::crypto::CryptoProvider for Claimed {
        fn name(&self) -> &str {
            "test-claimed"
        }
        fn validation(&self) -> db2graph_sync::crypto::Validation {
            db2graph_sync::crypto::Validation::Fips140_3 { module: "test".into(), certificate: "none".into() }
        }
        fn self_test(&self) -> db2graph_sync::Result<()> {
            db2graph_sync::crypto::run_known_answer_tests(self)
        }
        fn aead_seal(&self, k: &[u8; 32], n: &[u8; 12], a: &[u8], p: &[u8]) -> db2graph_sync::Result<Vec<u8>> {
            db2graph_sync::crypto::RustCryptoProvider.aead_seal(k, n, a, p)
        }
        fn aead_open(&self, k: &[u8; 32], n: &[u8; 12], a: &[u8], c: &[u8]) -> db2graph_sync::Result<Vec<u8>> {
            db2graph_sync::crypto::RustCryptoProvider.aead_open(k, n, a, c)
        }
        fn random(&self, b: &mut [u8]) -> db2graph_sync::Result<()> {
            db2graph_sync::crypto::RustCryptoProvider.random(b)
        }
        fn sha256(&self, d: &[u8]) -> [u8; 32] {
            db2graph_sync::crypto::RustCryptoProvider.sha256(d)
        }
        fn hmac_sha256(&self, k: &[u8], d: &[u8]) -> [u8; 32] {
            db2graph_sync::crypto::RustCryptoProvider.hmac_sha256(k, d)
        }
    }
    let mut o = opts(&src, &graph);
    o.crypto_provider = Some(Arc::new(Claimed));
    let e = start_with(cfg, o).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::RestrictedProviderUnavailable, "{e}");
    assert!(e.message().contains("vector"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_and_duplicate_source_replay_has_no_effect() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    src.commit(src.unit(
        101,
        vec![db2graph_sync::types::TxItem::Change(db2graph_sync::types::Change {
            relation: db2graph_sync::types::RelationName("public.customers".into()),
            op: db2graph_sync::types::ChangeOp::Update,
            event_ordinal: 0,
            old_key: None,
            before: None,
            after: Some(img(&[("id", Value::Int(1)), ("name", Value::Text("Ada L.".into()))])),
            completeness: db2graph_sync::types::Completeness::Delta,
            schema_version: String::new(),
        })],
    ));
    src.commit(src.unit(102, vec![del("public.customers", 0, 1)]));
    wait_for("applied", || applied(&sync) >= 3).await;
    sync.shutdown(T).await.unwrap();
    assert!(graph.state("test-pipeline").nodes.is_empty());
    assert_eq!(graph.state("test-pipeline").tombstones.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enterprise_requires_mapping_approval_and_dataset_allowlist() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    // Without allowlist or approval: validation fails before anything starts.
    let e = start_with(config(root.path(), Profile::Enterprise), opts(&src, &graph)).await.unwrap_err();
    assert!(matches!(e.kind(), ErrorKind::MappingNotApproved | ErrorKind::Config), "{e}");

    let mut cfg = config(root.path(), Profile::Enterprise);
    cfg.policy = toml::from_str(
        r#"
[[datasets]]
relation = "public.customers"
deny_fields = ["ssn"]
[[datasets]]
relation = "public.orders"
"#,
    )
    .unwrap();
    cfg.mapping.approved_digest = Some("not-the-right-digest".into());
    let sync = start_with(cfg.clone(), opts(&src, &graph)).await.unwrap();
    // The computed plan differs from the approved digest: paused, not guessing.
    wait_for("paused for approval", || sync.status().pause_reason.is_some()).await;
    let st = sync.status();
    assert!(!st.ready && !st.mapping_approved);
    let digest = st.mapping_digest.clone().unwrap();
    sync.approve_mapping(&digest);
    sync.ready(T).await.unwrap();

    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    wait_for("applied", || applied(&sync) >= 1).await;
    let node = graph.state("test-pipeline").nodes.values().next().cloned().unwrap();
    assert!(!node.props.contains_key("ssn"), "denied field never projected");
    assert_eq!(node.props.get("name"), Some(&Value::Text("Ada".into())));
    sync.shutdown(T).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_fields_are_never_journaled() {
    use db2graph_sync::crypto::{Keyring, RustCryptoProvider, Sealer};
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let mut cfg = config(root.path(), Profile::Development);
    cfg.policy = toml::from_str("[[datasets]]\nrelation = \"public.customers\"\ndeny_fields = [\"ssn\"]\n").unwrap();
    // Keep the unit in the journal: the graph never commits.
    let mut o = opts(&src, &graph);
    o.fault_injector = Some(Arc::new(|p| if p == "graph.before_commit" { Err(Error::new(ErrorKind::Internal, "hold")) } else { Ok(()) }));
    let sync = start_with(cfg, o).await.unwrap();
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    let _ = tokio::time::timeout(T, sync.failed()).await.unwrap();
    let _ = sync.wait().await;

    let mut kr = Keyring::new(1);
    kr.add_base64(1, KEY_B64).unwrap();
    let store =
        db2graph_sync::state::StateStore::open(&root.path().join("state"), Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr)), None)
            .unwrap();
    let journal = store.journal_after(0, 10).unwrap();
    assert_eq!(journal.len(), 1);
    let json = serde_json::to_string(&journal[0].1).unwrap();
    assert!(json.contains("Ada") && !json.contains("000-00-0000"), "ssn must not be persisted: {json}");
    drop(store);
    // And the raw file never contains plaintext.
    let raw = std::fs::read(root.path().join("state").join("state.redb")).unwrap();
    assert!(!raw.windows(3).any(|w| w == b"Ada"), "journal is encrypted at rest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_profile_fails_clearly_without_validated_provider() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let mut cfg = config(root.path(), Profile::Restricted);
    cfg.policy = toml::from_str("[[datasets]]\nrelation = \"public.customers\"\nallow_fields = [\"id\"]\n").unwrap();
    cfg.mapping.approved_digest = Some("x".into());
    cfg.audit.file = Some(root.path().join("audit.jsonl"));
    let e = start_with(cfg, opts(&src, &graph)).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::RestrictedProviderUnavailable, "{e}");
    assert!(!root.path().join("state").exists(), "nothing was started or created");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_safety_no_global_logger_no_panic_bounded_shutdown() {
    assert!(!tracing::dispatcher::has_been_set());
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    sync.ready(T).await.unwrap();
    assert!(!tracing::dispatcher::has_been_set(), "library must not install a global subscriber");
    let started = std::time::Instant::now();
    let r = sync.shutdown(Duration::from_secs(2)).await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(r.drained);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_handle_without_shutdown_does_not_hang_or_panic() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    {
        let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
        sync.ready(T).await.unwrap();
    }
    // The engine releases the state directory once it observes cancellation.
    let start = std::time::Instant::now();
    loop {
        match start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await {
            Ok(s) => {
                s.shutdown(T).await.unwrap();
                break;
            }
            Err(e) if e.kind() == ErrorKind::OwnershipConflict && start.elapsed() < T => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("{e}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconciliation_verifies_readback_and_detects_drift() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let cfg = config(root.path(), Profile::Development);
    let sync = start_with(cfg.clone(), opts(&src, &graph)).await.unwrap();
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada")), ins("public.customers", 1, &customer(2, "Grace"))]));
    wait_for("applied", || applied(&sync) >= 1).await;
    sync.shutdown(T).await.unwrap();

    src.set_current_rows("public.customers", vec![img(&customer(1, "Ada")), img(&customer(2, "Grace"))]);
    let r = db2graph_sync::admin::reconcile(&cfg, opts(&src, &graph), 10).await.unwrap();
    let c = r.relations.iter().find(|x| x.relation == "public.customers").unwrap();
    assert_eq!((c.source_rows, c.graph_entities, c.sampled, c.verified), (Some(2), 2, 2, 2));
    assert!(r.consistent);
    assert_eq!(r.hash_kind, "verified_readback");

    // Drift that bypassed CDC (e.g. restored source table) is detected.
    src.set_current_rows("public.customers", vec![img(&customer(1, "Ada Lovelace")), img(&customer(2, "Grace"))]);
    let r = db2graph_sync::admin::reconcile(&cfg, opts(&src, &graph), 10).await.unwrap();
    assert!(!r.consistent);
    let c = r.relations.iter().find(|x| x.relation == "public.customers").unwrap();
    assert_eq!(c.mismatched, vec![entity("public.customers", 1)]);

    src.set_current_rows("public.customers", vec![img(&customer(1, "Ada")), img(&customer(2, "Grace")), img(&customer(3, "Missing"))]);
    let r = db2graph_sync::admin::reconcile(&cfg, opts(&src, &graph), 10).await.unwrap();
    let c = r.relations.iter().find(|x| x.relation == "public.customers").unwrap();
    assert_eq!(c.missing_in_graph, vec![entity("public.customers", 3)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_and_resume_are_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    sync.ready(T).await.unwrap();
    sync.pause();
    sync.pause();
    wait_for("paused", || sync.status().phase == Phase::Paused).await;
    src.commit(src.unit(100, vec![ins("public.customers", 0, &customer(1, "Ada"))]));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(applied(&sync), 0, "nothing applied while paused");
    sync.resume();
    sync.resume();
    wait_for("applied after resume", || applied(&sync) >= 1).await;
    sync.shutdown(T).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_and_composite_relationships() {
    let root = tempfile::tempdir().unwrap();
    let src = ScriptedSource::new(schema());
    let graph = MemoryGraph::new();
    let sync = start_with(config(root.path(), Profile::Development), opts(&src, &graph)).await.unwrap();
    src.commit(src.unit(
        100,
        vec![
            ins("public.customers", 0, &customer(1, "Ada")),
            ins("public.orders", 1, &order(10, 1)),
            ins("public.orders", 2, &order(11, 1)),
        ],
    ));
    src.commit(src.unit(
        101,
        vec![db2graph_sync::types::TxItem::Truncate(db2graph_sync::types::Truncate {
            relations: vec![db2graph_sync::types::RelationName("public.orders".into())],
            event_ordinal: 0,
            cascade: false,
        })],
    ));
    wait_for("applied", || applied(&sync) >= 2).await;
    let g = graph.state("test-pipeline");
    assert_eq!(g.nodes.len(), 1, "orders truncated, customer kept");
    assert!(g.edges.is_empty());
    sync.shutdown(T).await.unwrap();
}
