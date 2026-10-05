# Architecture overview

db2graph-sync is a one-way change-data-capture (CDC) projection engine. It reads
committed changes from an operational database, maps them deterministically to
graph operations, and applies them to a graph target. It can also maintain a
vector index derived from the graph. The source database stays authoritative;
nothing is ever written back to it.

This page describes the components, how they are laid out as crates and Tokio
tasks, and the extension interfaces. The detailed contracts are on the other
architecture pages:

- [Transactions and ordering](transactions-ordering.md)
- [Snapshots and schema](snapshots-schema.md)
- [Durability and crash recovery](durability.md)
- [Identity and relationships](identity-relationships.md)
- [Vector indexing and retrieval](vector-retrieval.md)
- [Ownership, HA and reconciliation](ha-reconciliation.md)

## One engine, two ways to run it

There is a single engine implementation (`crates/db2graph-sync/src/engine.rs`).
It is used in two ways:

- **Embedded**: a Rust application calls `db2graph_sync::start(config)` (or
  `start_with(config, StartOptions)`) on its own Tokio runtime. The library
  never creates a runtime, installs a logger or signal handler, binds a port,
  changes environment variables or exits the process.
- **Standalone**: the `db2graph-sync` binary (`crates/db2graph-sync-cli`)
  runs the same engine with `db2graph-sync run`, installs logging and signal
  handling, and, when `management.listen` is set, serves the management API.

Both modes share configuration, state format, error kinds and behaviour.
See [ADR 0001](../adr/0001-one-engine-pragmatic-workspace.md).

## What a single instance needs

One running instance needs:

- the source database (for the implemented connector: PostgreSQL with logical
  replication enabled, a publication, and a role with the required privileges);
- the graph target (for the implemented sink: one Neo4j server reachable over
  `bolt+s://`);
- a local state directory on durable storage, owned exclusively by that
  instance;
- the encryption key(s) for that state, resolved through secret references;
- optionally a vector sink and an embedding provider.

It does **not** need Kafka, Redis, Kubernetes, a JVM, ZooKeeper/etcd or any
other coordinator. There is also no built-in leader election; see
[Ownership, HA and reconciliation](ha-reconciliation.md).

## Component responsibilities

| Responsibility | Where it lives | Summary |
|---|---|---|
| Capture | `source/` (`SourceConnector`, `ChangeStream`) | Reads the source's native change log (PostgreSQL: `pgoutput` over a logical replication slot). Never polls business tables to detect changes. |
| Schema discovery | `SourceConnector::discover_schema`, `schema.rs` | Reads catalog metadata: relations, columns, primary keys, unique constraints, foreign keys, before-image mode. Schemas are versioned by content digest. |
| Deterministic mapping | `mapping.rs` | Generates a `MappingPlan` (labels, keys, relationship types, join tables, exclusions) from the schema, mapping overrides and policy. The plan has a digest that must be approved outside the development profile. |
| Transaction assembly | connector (`source/postgres/stream.rs`) | Assembles complete source transactions (`Begin` .. `Commit`) into `CapturedTransaction` units. Snapshot reads are emitted as snapshot-chunk units. |
| Policy enforcement | `policy.rs`, `engine.rs` (`normalize`) | Drops unmapped relations and strips denied fields **before** journaling. Key, foreign-key, unique-key and tenant columns are kept for identity computation but are only projected as properties if policy allows. |
| Durable journaling | `state/` | Appends each normalized unit to an encrypted journal (AES-256-GCM, fsync on commit) together with the acknowledgeable source position. Only then is the source acknowledged. |
| Ordered scheduling | `engine.rs` (`applier`) | Reads the journal in sequence order, plans each unit, groups whole units into batches, retries transient failures with capped exponential backoff. |
| Graph application | `plan.rs`, `sink/` (`GraphSink`) | Translates a unit into backend-neutral `GraphOp`s and applies a batch in one atomic target transaction with receipts, version guards, tombstones, fencing and the vector outbox. |
| Vector outbox processing | `vector/`, `engine.rs` (`vector_worker`) | Drains vector tasks created in the graph transaction: renders approved templates, embeds, upserts deterministic points, removes stale points, completes the task only if still current. |
| Audit | `audit.rs`, `engine.rs` (`AuditPipe`) | Appends audit records to an encrypted spool in the state store and forwards them to audit sinks (built in: a JSON-lines file with an HMAC chain). |
| Management and observability | `handle.rs`, `management.rs` (feature `service`) | `SyncHandle` status, phases, counters, backlog and freshness; pause/resume/approve; a management router (status, Prometheus metrics, pause/resume, mapping approval, retrieval validation) that the host mounts or the binary serves. |

## Data flow

```text
source change log
      │  (connector: pgoutput, transaction assembly)
      ▼
capture loop ── normalize (mapping + policy) ── journal append (encrypted, fsync,
      │                                          same commit stores ack position
      │                                          and snapshot progress)
      │── ack source (only after the journal commit)
      ▼
applier ── journal (in order) ── plan ── batch (whole units only)
      │
      ▼
GraphSink::apply  (one target transaction: pipeline lock + fence check,
      │            receipts, guarded writes, tombstones, pending edges,
      │            vector outbox, pipeline marker)
      ▼
advance contiguous checkpoint + compact journal (+ anchor file if configured)

graph outbox ── vector worker ── render/embed ── vector sink ── complete task
```

## Crate layout

| Crate | Kind | Contents |
|---|---|---|
| `crates/db2graph-sync` | library `db2graph_sync` | Engine, configuration, policy, mapping, planning, state store, crypto, audit, connectors, sinks, vector adapters, retrieval boundary, admin operations, optional management router. |
| `crates/db2graph-sync-cli` | binary `db2graph-sync` | Operator CLI and standalone runner (`validate`, `preflight`, `plan`, `bootstrap`, `source-setup`, `run`, remote `status`/`pause`/`resume`/`approve-mapping`, `reconcile`, `evidence`, `recover`, `rotate-keys`, `audit-verify`, `keygen`, `token-hash`, `about`). |

Cargo features of the library: `postgres` and `neo4j` (default), `vector-qdrant`,
`vector-pinecone`, `embedding-http`, `service`. `--no-default-features` builds
the core with the in-memory sink, the scripted source, the redb state backend
and the crypto provider only. Selecting a component that was not compiled in
fails with `feature_not_compiled`.

Main library modules:

| Module | Role |
|---|---|
| `engine.rs` | Supervisor and background tasks (not public). |
| `types.rs` | Canonical change model: `Value`, `FieldState`, `CapturedTransaction`, `Version`, `SourcePosition`. |
| `schema.rs`, `mapping.rs`, `plan.rs` | Schema model and diff, mapping plan, translation to `GraphOp`s. |
| `identity.rs` | Entity identity and key digests. |
| `policy.rs`, `retrieval.rs` | Data policy and the authorized retrieval boundary. |
| `state/` | Encrypted state store over a `StateBackend` (default redb). |
| `sink/` | `GraphSink` trait, in-memory reference sink, Neo4j sink, conformance suite. |
| `source/` | `SourceConnector` trait, PostgreSQL connector, scripted source for tests. |
| `vector/` | `VectorSink`, `EmbeddingProvider`, Qdrant, Pinecone, HTTP embedder, hashing embedder, in-memory vector sink. |
| `capability.rs` | Capability descriptors and the effective delivery guarantee. |
| `reconcile.rs` | Operator-invoked reconciliation. |
| `handle.rs` | `SyncHandle`, `Control`, `Status`, `Phase`. |

## Task layout

`start` spawns a supervisor on the caller's runtime. After draining any
leftover audit spool, preflight, identity check, schema discovery, graph
preflight, the rollback-evidence check and fence acquisition, the supervisor
starts background tasks and runs the capture loop itself:

```text
supervisor ── capture loop: source → normalize/policy → journal (fsync) → ack source
    │
    ├── applier: journal (ordered) → plan → graph batch (atomic, receipts, fence) → checkpoint
    ├── vector worker (only if [vector] is configured): graph outbox → render/embed → vector sink → complete task
    ├── audit forwarder: encrypted spool → audit sinks
    └── status poller (1 s): backlog gauges (journal, audit spool, vector tasks, pending edges)
```

Blocking state-store operations run on `spawn_blocking`. Any terminal error in
any task stops all tasks, is recorded in `status.failure`, and is returned from
`SyncHandle::failed` and `SyncHandle::wait`. Retryable errors are retried with
capped exponential backoff (100 ms doubling, capped at
`engine.retry_max_backoff_ms`, default 30 000 ms) and counted.

On orderly shutdown (`SyncHandle::shutdown(drain_timeout)`), capture stops, the
applier continues until the journal is drained or the deadline passes, other
tasks stop, audit is flushed once, and completion is reported only after all
references to the state store are released. Dropping the handle without
`shutdown` only requests cancellation; nothing is flushed.

Phases reported in status: `starting`, `preflight`, `recovering`,
`snapshotting`, `validating`, `streaming`, `paused`, `draining`, `stopped`,
`failed`. `ready` is true once streaming is established and the initial
snapshot generation is active.

## Extension interfaces

All extension points are traits supplied through `StartOptions`. Each declares
its capabilities; declaring a capability does not make it true, and the engine
reports what it can infer.

| Trait | Capability declaration | Notes |
|---|---|---|
| `SourceConnector` (+ `ChangeStream`) | `SourceCapabilities`: `transaction_boundaries`, `ordering` (`total_commit_order` / `per_partition` / `none`), `snapshot` and `resumed_snapshot` consistency, `before_images`, `truncate`, `schema_change_signal`, `qualified_versions`, `notes` | A source without transaction boundaries caps the effective guarantee at `at_least_once`. |
| `GraphSink` | `GraphSinkCapabilities`: `atomic_batches`, `receipts_in_target`, `guarded_versions`, `fencing`, `tombstones`, `atomic_vector_outbox`, `qualified`, `notes` | `guarantee()` is `effectively_once_effects` only if atomic batches, receipts, guarded versions, fencing and tombstones are all declared; otherwise `at_least_once`. A sink with `qualified: false` produces a status warning. The executable contract is `sink::conformance::run_all`. |
| `VectorSink` | `VectorSinkCapabilities`: `visibility` (`verified` / `unknown`), `delete_by_filter`, `qualified` | |
| `EmbeddingProvider` | `EmbeddingCapabilities`: `provider`, `model`, `model_version`, `dimensions`, `allows_protected`, `remote` | `allows_protected` is refused in the restricted profile. |
| `StateBackend` | `durable()`, `exclusive()` | Non-durable backends are refused outside development. A non-exclusive backend logs a warning: the host must then guarantee single ownership. |
| `SecretProvider`, `CryptoProvider`, `AuditSink` | Crypto providers report `validation` (`not_validated` / FIPS 140-3) | The bundled RustCrypto provider is not validated. |

The status field `delivery_guarantee` is the sink's declared guarantee,
lowered to `at_least_once` if the source does not deliver transaction
boundaries. See [Custom destinations](../destinations/custom.md).

## Source files

- `crates/db2graph-sync/src/engine.rs` — supervisor and tasks
- `crates/db2graph-sync/src/lib.rs` — `start`, `start_with`, `StartOptions`, `admin`
- `crates/db2graph-sync/src/capability.rs` — capability descriptors
