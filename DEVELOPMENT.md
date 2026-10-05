# Development guide

db2graph-sync — an open-source project by Adventa. (<https://www.adventa.com.au>)

This guide describes the workspace and how its behaviour is tested. Setup
instructions and the required checks are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Workspace layout

```text
Cargo.toml                    workspace (version, edition 2024, MSRV 1.90, Apache-2.0)
crates/
  db2graph-sync/              library crate `db2graph_sync`
    src/
      lib.rs                  start / start_with / StartOptions / admin
      config.rs               configuration model and profile validation (offline)
      engine.rs               capture loop, applier, vector worker, audit forwarder
      handle.rs               SyncHandle, Control, status types
      state/                  encrypted durable state (StateStore, StateBackend, RedbBackend)
      crypto.rs               CryptoProvider, RustCryptoProvider, Keyring, Sealer
      secret.rs               SecretRef, SecretProvider, redaction
      policy.rs               dataset/field policy, labels, principals
      mapping.rs, plan.rs     relational-to-graph mapping and planning
      identity.rs, types.rs   entity ids, versions, value types
      audit.rs                audit records, FileAuditSink (HMAC chain)
      reconcile.rs            reconciliation (verified readback hashes)
      retrieval.rs            RetrievalValidator
      management.rs           axum management router (feature `service`)
      source/                 SourceConnector, scripted source, postgres/
      sink/                   GraphSink, memory sink, neo4j sink, conformance suite
      vector/                 VectorSink, EmbeddingProvider, qdrant, pinecone, http, hashing
    api/openapi.json          management API (v1)
    examples/                 runnable and compile-only examples
    tests/                    engine, management and real-service integration tests
  db2graph-sync-cli/          binary `db2graph-sync`
    tests/cli.rs              CLI tests
deploy/test/docker-compose.yml  integration-test services (test-only credentials)
examples/config/              example TOML configurations (dev, enterprise)
scripts/                      integration.ps1 / integration.sh and release helpers
docs/                         mdBook documentation (`mdbook build docs`)
```

## Crates and features

`db2graph-sync` (library):

| Feature | Default | Enables |
|---|---|---|
| `postgres` | yes | PostgreSQL logical-replication source |
| `neo4j` | yes | Neo4j graph sink |
| `vector-qdrant` | no | Qdrant vector sink |
| `vector-pinecone` | no | Pinecone vector sink (experimental, never run against a live index) |
| `embedding-http` | no | OpenAI-compatible HTTP embedder (experimental, unqualified) |
| `service` | no | axum management router; the library never binds a port |

`--no-default-features` builds the minimal core: engine, memory graph sink,
scripted source, redb state backend and the crypto provider. This build is
part of the required checks.

`db2graph-sync-cli` builds the `db2graph-sync` binary (subcommands such as
`validate`, `preflight`, `plan`, `bootstrap`, `source-setup`, `run`, `status`,
`reconcile`, `evidence`, `recover`, `rotate-keys`, `audit-verify`, `keygen`,
`token-hash`, `about`).

## Test layers

| Layer | Where | Needs services | What it covers |
|---|---|---|---|
| Unit | `#[cfg(test)]` modules in `src/` | no | identity, types, crypto KATs/rotation/nonces, secret redaction, policy (including non-text tenant columns: `tenant_matching_handles_non_text_columns`), mapping, planning, state store (tamper, substitution, corruption, rollback, gaps, rotation, spool limit, `anchor_survives_key_rotation`), audit chain (`chain_survives_key_rotation`), pgoutput decoding, wire helpers, vector render/chunk/ids, memory sink conformance |
| Engine acceptance | `crates/db2graph-sync/tests/engine.rs` | no | full engine with the scripted source and the memory sink: lifecycle, crash injection and replay (including `crash_after_journal_commit_before_source_ack_replays`), retries, ownership conflicts, split-brain fencing, a stale instance that must not fence out a healthy owner (`stale_instance_cannot_fence_out_healthy_owner`), state loss/rollback and authorized recovery with a re-snapshot under generation 2, source identity change and `accept_source_change` recovery, profile enforcement (including `restricted_profile_refuses_builtin_vector_components`), policy filtering, host safety, shutdown, reconciliation |
| Conformance suite | `sink::conformance::run_all` | depends on sink | executable specification any `GraphSink` must pass; run against the memory sink in unit tests and against Neo4j in the integration tests |
| Management API | `crates/db2graph-sync/tests/management.rs` | no | authentication, scopes, correlation/idempotency headers, invalid requests, retrieval validation, metrics, OpenAPI route parity |
| Real-service integration | `tests/pg_neo4j.rs`, `tests/qdrant.rs` | yes (Docker) | PostgreSQL 16 + Neo4j 5 (including Neo4j sink conformance) and Qdrant; run with `scripts/integration.ps1` / `scripts/integration.sh` |
| CLI | `crates/db2graph-sync-cli/tests/cli.rs` | no | argument handling, exit codes, offline commands |
| Examples | `crates/db2graph-sync/examples/` | see below | public API usage |

The real-service integration tests print a line beginning with `UNEXECUTED`
when their environment variables are not set; the integration scripts fail
when any such line appears. At the time of writing these tests have **not
been executed** (no Docker daemon was available), so the PostgreSQL
connector, the Neo4j sink and the Qdrant sink are implemented but not
qualified against real servers. See `IMPLEMENTATION_STATUS.md`.

### Examples

Runnable without any external service:

```sh
cargo run -p db2graph-sync --example host_lifecycle
cargo run -p db2graph-sync --example custom_graph_sink
cargo run -p db2graph-sync --example custom_state_backend
cargo run -p db2graph-sync --example restart_replay
cargo run -p db2graph-sync --example policy_filtering
cargo run -p db2graph-sync --example composite_keys
```

Require real services and have so far only been compiled (by
`cargo clippy --all-targets` / `cargo test`), not run:
`embedded_postgres_neo4j`, `qdrant_indexing`.

## Fixtures

`deploy/test/docker-compose.yml` starts PostgreSQL 16 (`wal_level=logical`),
Neo4j 5 Community and Qdrant on non-standard local ports. The credentials in
that file and in the integration scripts are **test-only** and must never be
reused elsewhere. The integration tests run in the development profile with
the plaintext exception against these local services. Use synthetic data
only.

## Fault injection

`StartOptions` has a hidden `fault_injector` hook used by tests to emulate a
crash at named points. Returning an error from the injector at a point aborts
the operation there, exactly as a process crash would leave durable state.
The points are:

| Point | Location |
|---|---|
| `journal.before_commit` | before captured units are durably appended to the journal |
| `journal.after_commit` | after the journal commit, before the source is acknowledged |
| `checkpoint.before_commit` | before the applied checkpoint advances |
| `graph.before_commit` | before a graph batch is committed |
| `graph.after_commit` | after a graph batch commits, before the checkpoint advances |
| `vector.after_delivery` | after vector points are delivered, before the task is completed |
| `shutdown.before_drain` | at the start of the shutdown drain |

Every point except `vector.after_delivery` and `shutdown.before_drain` is
exercised by an engine test; those two points have no test yet.

The hook is not part of the stable API and must not be used in production.

## Tips

- Building `ring` needs a C compiler; on Windows use the MSVC toolchain.
- If your home directory is on a slow or synced drive, you can point
  `CARGO_HOME` (and `CARGO_TARGET_DIR`) at a local disk to speed up builds.
- Run a single engine test with
  `cargo test -p db2graph-sync --test engine <name> -- --nocapture`.
