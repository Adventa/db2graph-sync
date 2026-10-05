# Rust API overview

The authoritative reference is rustdoc:

```sh
cargo doc -p db2graph-sync --features "postgres neo4j vector-qdrant vector-pinecone embedding-http service" --open
```

Version 0.1.0 is pre-1.0. The public API may change between releases. The
snake_case names of `ErrorKind` are intended to stay stable.

## Crate root

| Item | Purpose |
|---|---|
| `start(config) -> Result<SyncHandle>` | One-line startup on the caller's Tokio runtime |
| `start_with(config, StartOptions) -> Result<SyncHandle>` | Startup with components built in code |
| `StartOptions` | `source`, `graph_sink`, `vector_sink`, `embedder`, `secret_provider`, `crypto_provider`, `audit_sinks`, `state_backend` (all optional) |
| `about() -> About` | Name, version, license, attribution, homepage, compiled features (no network) |
| Re-exports | `Config`, `Profile`, `Error`, `ErrorKind`, `Result`, `SyncHandle`, `Control`, `Status`, `Phase`, `ShutdownReport` |

## `admin`: operator operations

These are what the CLI uses. Each takes `&Config`, and most also take
`StartOptions` so custom components can be injected.

| Function | Effect |
|---|---|
| `plan(config, opts) -> (MappingPlan, digest)` | Discovers the schema and computes the plan. Writes nothing. |
| `preflight(config, opts) -> Vec<PreflightReport>` | Source and graph prerequisite checks |
| `bootstrap_graph(config, opts) -> digest` | Creates graph constraints and indexes. Requires an approved digest outside `development`. |
| `bootstrap_vector(config) -> Option<String>` | Creates the Qdrant collection; other sinks are provisioned out of band |
| `source_setup_statements(config) -> Vec<String>` | Prints the source-side setup SQL; never executes it |
| `rotate_state_keys(config, opts) -> u64` | Re-encrypts state under `active_key` |
| `accept_state_reset(config, opts, reason) -> u64` | Authorized recovery after state loss or restore; returns the new state epoch |
| `accept_source_change(config, opts, reason) -> u32` | Authorized recovery after `source_identity_changed`: increments the source epoch and clears the stored source identity, acknowledged position, snapshot progress and snapshot completion; returns the new source epoch. See [Recovery](../operations/recovery.md#recover---accept-source-change). |
| `reconcile(config, opts, sample) -> ReconcileReport` | Counts plus verified readback hashes. Read-only. |
| `evidence(config, opts, sample) -> serde_json::Value` | Evidence document |
| `verify_audit(config, path) -> u64` | Verifies the audit file's HMAC chain |

## Modules

| Module | Contents |
|---|---|
| `handle` | `SyncHandle`, `Control`, `Status`, `Phase`, `Freshness`, `Backlog`, `Counters`, `FailureInfo`, `ShutdownReport` |
| `config` | `Config` and every section type, `Profile`, `TlsMode`, `Scope`, `Finding` |
| `error` | `Error` (`kind`, `message`, `is_retryable`), `ErrorKind` (`exit_code`, `is_terminal`) |
| `policy` | `PolicyConfig`, `DatasetPolicy`, `Labels`, `Policy`, `Principal`, `Ineligible`, `authorize` |
| `mapping` | `MappingConfig`, overrides, `MappingPlan`, `valid_graph_ident` |
| `retrieval` | `RetrievalValidator`, `Hit`, `VersionRule`, `Verdict` |
| `capability` | `DeliveryGuarantee`, source, graph, vector and embedding capabilities, `PreflightReport` |
| `source` | `SourceConnector`, `ChangeStream`, `SourceEvent`, `StartRequest`; `scripted::ScriptedSource`; `postgres` (feature) |
| `sink` | `GraphSink`, `ApplyContext`, `PipelineMarker`, `EntityState`; `memory::MemoryGraph`; `neo4j` (feature); `conformance::run_all` |
| `vector` | `VectorSink`, `EmbeddingProvider`, `HashingEmbedder`, `MemoryVectorSink`, `qdrant` / `pinecone` / `http_embedding` (features) |
| `state` | `StateStore`, `StateBackend`, `RedbBackend`, `WriteBatch` |
| `crypto` | `CryptoProvider`, `RustCryptoProvider`, `Validation`, `Keyring`, `Sealer` |
| `secret` | `SecretRef`, `Secret` (redacted, zeroized), `SecretProvider`, `BuiltinSecretProvider`, `redact_url` |
| `audit` | `AuditRecord`, `AuditSink`, `FileAuditSink` |
| `types`, `identity`, `schema`, `plan`, `reconcile` | Values, field states, versions, entity ids, schema model, graph-op planning, reconciliation |
| `management` (feature `service`) | `router(Control, Vec<ManagementToken>) -> axum::Router` |

## Extension traits

Every extension declares its capabilities. Implementing a trait doesn't
grant any guarantee by itself.

| Trait | Notes |
|---|---|
| `SourceConnector` (+ `ChangeStream`) | `capabilities`, `preflight`, `identity`, `discover_schema`, `start`. Optional `row_count` and `sample_rows` for reconciliation. |
| `GraphSink` | `capabilities`, `preflight`, `bootstrap`, `acquire_fence`, `marker`, `apply` (atomic, in order), `set_active_generation`, plus lookup, vector-outbox and pending-edge methods |
| `VectorSink` | `capabilities`, `preflight`, `upsert`, `delete_entity` |
| `EmbeddingProvider` | `capabilities` (model, model version, dimensions, `allows_protected`, `remote`), `embed` |
| `StateBackend` | `durable`, `exclusive`, atomic `commit(WriteBatch)`, `get`, ordered `range`, `last`, `len`. A non-durable backend is allowed only in `development`. |
| `SecretProvider` | `schemes`, `resolve` |
| `CryptoProvider` | Self-test plus `validation()` (`NotValidated` or `Fips140_3`) |
| `AuditSink` | Receives forwarded audit records |

`GraphSinkCapabilities::guarantee()` returns `EffectivelyOnceEffects` only
if `atomic_batches`, `receipts_in_target`, `guarded_versions`, `fencing` and
`tombstones` are all true; otherwise it returns `AtLeastOnce`.

A graph sink that reports `qualified: false` adds a status warning. In this
release that includes the built-in Neo4j sink (integration-tested against a
live server with local Docker on Windows, not yet CI-qualified on all
platforms) as well as all third-party sinks. Run `sink::conformance::run_all` against your sink; see
`examples/custom_graph_sink.rs` and [Custom sinks](../destinations/custom.md).
