# Changelog

All notable changes to db2graph-sync are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is `0.x`, minor releases may contain breaking changes; they
are called out here with migration notes.

db2graph-sync — an open-source project by Adventa. (<https://www.adventa.com.au>)

## [Unreleased]

Nothing has been released or published yet. Workspace version: `0.1.0`.
MSRV: Rust 1.90. Developed and tested on Windows 11 x86_64 (MSVC) with
Rust 1.99.0; Linux and macOS are intended but not yet tested (no CI run has
executed).

### Added

#### Library (`db2graph-sync`)

- One-call embedding API `db2graph_sync::start(config)` and
  `start_with(config, StartOptions)` for custom sources, graph sinks, vector
  sinks, embedders, secret providers, crypto providers, audit sinks and state
  backends. The engine runs on the caller's Tokio runtime.
- `SyncHandle` with `status`, `subscribe`, `ready`, `failed`, `pause`,
  `resume`, `approve_mapping`, `update_policy`, `retrieval_validator`,
  `control`, bounded `shutdown(drain_timeout)` returning a `ShutdownReport`,
  and `wait`.
- Status model with phases, freshness, backlog, counters, vector statistics,
  delivery guarantee, crypto provider/validation status and warnings.
- Stable snake_case error kinds with documented CLI exit codes.
- Engine: encrypted, fsync'd journal; ordered application in atomic graph
  batches of whole source transactions; contiguous checkpoints; retry with
  capped exponential backoff; journal backpressure; periodic schema refresh;
  pause on unplannable units instead of skipping.
- Delivery contract: at-least-once transport with effectively-once graph
  effects when the graph sink declares atomic batches, receipts in the
  target, guarded versions, fencing and tombstones. Graph and vector stores
  converge asynchronously.
- Deterministic entity identity (SHA-256 over tenant, source, relation and
  typed key values) and fixed-width versions (source epoch, position,
  ordinal). The source epoch is incremented only by the authorized recovery
  `accept_source_change`.
- Receipt digests cover the captured unit content (items, base version,
  kind), not the planned operations, so replay after an approved mapping or
  policy change is a duplicate.
- Snapshot generations: a new snapshot uses the graph marker's active
  generation + 1, so a re-snapshot after state reset or source change does
  not collide with earlier snapshot receipts.
- Graph semantics: receipts and duplicate detection, consistency incidents,
  stale-version guards, tombstones, truncate watermarks, foreign-key edges
  with pending relationships, join tables, composite and unique-key
  references, fencing tokens, non-destructive deletes for nodes with foreign
  relationships.
- In-memory graph sink (executable specification) and an executable graph sink
  conformance suite (`sink::conformance::run_all`).
- Scripted source for tests and examples.
- **PostgreSQL source connector** (pgoutput v1, own replication client,
  verify-full TLS, SCRAM-SHA-256, transaction-consistent initial snapshot,
  preflight checks, `source-setup` statements). A replication slot that is
  still active at stream start is a retryable `ownership_conflict`.
  **Implemented; integration-tested against live servers (local Docker,
  Windows; see IMPLEMENTATION_STATUS.md); not yet CI-qualified on all platforms.**
- **Neo4j graph sink** (bolt+s, parameterized Cypher, fence lock, bootstrap of
  constraints and indexes; constraints are created only by explicit
  bootstrap). **Implemented; integration-tested against live servers (local
  Docker, Windows; see IMPLEMENTATION_STATUS.md); not yet CI-qualified on all
  platforms.** Reports `qualified: false`, which produces a status warning.
- Vector indexing via a graph outbox with deterministic chunking and point
  ids, superseded-version handling and a request rate limit.
- **Qdrant vector sink** — implemented; integration-tested against live
  servers (local Docker, Windows; see IMPLEMENTATION_STATUS.md); not yet CI-qualified
  on all platforms. Reports `qualified: false`.
- **Pinecone vector sink** — experimental; never run against a live index.
- **OpenAI-compatible HTTP embedder** — experimental; unqualified.
- Hashing embedder (deterministic, not semantic; development profile only)
  and in-memory vector sink.
- Retrieval boundary: `RetrievalValidator` checks current existence, policy,
  tenant, classifications, CUI categories, export control, releasability and
  version for each hit.
- Policy: dataset allowlist, field allow/deny lists, unknown-field default,
  embed fields, labels and field labels, row-level tenant column (text,
  integer, big-integer, decimal and UUID columns compared by canonical text;
  exact snapshot/reconcile counts skipped for such datasets), runtime policy
  updates.
- Security profiles `development`, `enterprise` and `restricted`. The
  restricted profile currently refuses to start with the built-in adapters,
  built-in vector sinks or HTTP embedder (`restricted_provider_unavailable`);
  no validated crypto provider is bundled.
- Encrypted state (AES-256-GCM envelopes with AAD binding, key versions,
  rotation, per-key seal limit), redb state backend with exclusive file lock,
  pluggable `StateBackend`, rollback detection with state epochs and an
  optional anchor file (`store_id:epoch:kid:mac`, verifies after key
  rotation), checked before the graph fence is acquired so stale state cannot
  fence out a healthy owner, authorized recovery (state reset; source change
  with a new source epoch and a new snapshot).
- Secret references (`env:`, `file:`, development-only `literal:`) and a
  pluggable `SecretProvider`; secrets redacted from errors.
- Audit: encrypted spool (drained to the sinks at start before the start
  record is written), forwarding to audit sinks, HMAC-chained JSON-lines file
  sink with verification; each line records and authenticates its key id
  (`kid`), so verification survives key rotation while old keys stay
  configured.
- Admin functions: plan, preflight, bootstrap (graph and vector), source setup
  statements, key rotation, state reset acceptance, source change acceptance,
  reconciliation with verified readback hashes, evidence export, audit
  verification.
- Management router (feature `service`): about, liveness, readiness, status,
  Prometheus metrics (`counter` type for `*_total` series, `gauge`
  otherwise), pause, resume, mapping approval and retrieval validation, with
  hashed bearer tokens and scopes. OpenAPI document at
  `crates/db2graph-sync/api/openapi.json`.

#### CLI (`db2graph-sync-cli`)

- `db2graph-sync` binary with `validate`, `preflight`, `plan`, `bootstrap`,
  `source-setup`, `run`, `status`, `pause`, `resume`, `approve-mapping`,
  `reconcile`, `evidence`, `recover` (exactly one of `--accept-state-reset`
  or `--accept-source-change`), `rotate-keys`, `audit-verify`, `keygen`,
  `token-hash` and `about`. The standalone binary serves the
  management API over TLS (plaintext only in development).

#### Project

- Apache-2.0 license, governance, contribution, support and release
  documents.
- Integration test fixtures (`deploy/test/docker-compose.yml`) and scripts
  that fail when any integration test is `UNEXECUTED`.

### Fixed

- Neo4j sink: decoding a null version value (for example from
  `OPTIONAL MATCH` on a missing tombstone or watermark) failed; it is now
  read as `Option<String>`. Found by the live-server integration run.
- `TRUNCATE` of a declared join table no longer pauses the pipeline as
  `unsupported`. It is planned as `GraphOp::TruncateJoinRelation`: the sink
  stores a watermark for the join table and removes that join table's
  relationships with older versions; stale join-row events cannot recreate
  them. Implemented in the in-memory and Neo4j sinks (new Neo4j index
  `d2g_join_relation` on `_D2G_JoinState(_d2g_pipeline, relation)`), with
  conformance coverage.

### Testing

- Real-server integration tests executed and passed on 2026-10-05 (Windows 11
  x86_64, local Docker 29.3.1, `scripts/integration.ps1`, no `UNEXECUTED`
  tests) against PostgreSQL 16.15 (`postgres:16`), Neo4j 5.26.31 Community
  (`neo4j:5-community`) and Qdrant 1.13.4 (`qdrant/qdrant:v1.13.4`):
  `tests/pg_neo4j.rs` (`postgres_to_neo4j_end_to_end`,
  `neo4j_sink_conformance`,
  `missing_slot_after_progress_is_resume_history_lost`) 3/3 and
  `tests/qdrant.rs` (`qdrant_indexing_and_authorized_retrieval`) 1/1.
  Plaintext development exception only (no TLS); no CI run on Linux or
  macOS; RDS/Aurora, standbys, partitioned tables and Neo4j clusters not
  tested. Sinks still report `qualified: false`.

### Known limitations

See `LIMITATIONS.md` and `IMPLEMENTATION_STATUS.md`. Planned work, none of
which is implemented, is listed in [ROADMAP.md](ROADMAP.md).
