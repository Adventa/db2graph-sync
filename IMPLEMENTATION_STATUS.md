# Implementation status and qualification report

db2graph-sync — an open-source project by Adventa. <https://www.adventa.com.au>

Release: **0.1.0, unpublished.** Report date: 2026-10-05. Environment for all
evidence below: Windows 11 Home (x86_64), Rust 1.99.0 (MSVC toolchain, VS 2022
Build Tools 17.14), Node 21.7.1, Docker Desktop 29.3.1. Real-server tests ran
against PostgreSQL 16.15, Neo4j 5.26.31 Community and Qdrant 1.13.4 in local
containers (plaintext development exception). No container image build and no
CI run has been executed.

Legend — **Implemented**: code exists and its behaviour is covered by tests
that ran. **Integration-tested, not CI-qualified**: real-server tests passed
locally on Windows, but there is no green CI run on all platforms yet, so the
component still reports `qualified: false`. **Experimental**:
implemented against vendor documentation, never run against the real service.
**Planned**: not implemented (may have config placeholders that are rejected).
**Blocked**: cannot be completed without an external prerequisite.

## Components

| Area | Status | Evidence |
|------|--------|----------|
| One-line startup `db2graph_sync::start(config)` and `start_with` | Implemented | doctest compiles; `start_with` used by 20+ engine tests; `start` used by `tests/pg_neo4j.rs` (passed against live servers) and examples |
| Embedded lifecycle (readiness, status/freshness, `failed()`, pause/resume, shutdown/wait, drop, `Control`) | Implemented | `tests/engine.rs` |
| Host safety (caller runtime, no global logger/signals/env/ports, typed failures, bounded shutdown) | Implemented | `host_safety_no_global_logger_no_panic_bounded_shutdown`, `dropping_handle_without_shutdown_does_not_hang_or_panic`; code review: no `process::exit`, no `set_var`, no listener in library |
| Engine: journal-before-ack, ordered whole-unit batching, contiguous checkpoints, retries, backpressure | Implemented | engine + state tests |
| Encrypted durable state (AES-256-GCM, AAD binding, key versions, rotation, seal limit, epochs, anchor) | Implemented | `src/state` unit tests incl. tamper, substitution, corruption, rollback, gap, rotation |
| Pluggable `StateBackend` (redb default) | Implemented | `custom_state_backend` example runs |
| Crash recovery at journal/graph/checkpoint boundaries | Implemented (simulated crashes) | 5 crash-injection tests; no process-kill tests |
| Fencing / ownership / split-brain | Implemented | `split_brain_older_owner_is_fenced_out`, `stale_instance_cannot_fence_out_healthy_owner`, `second_engine_on_same_state_dir_is_rejected` |
| Rollback detection and authorized recovery (state reset, source change) | Implemented | `state_loss_is_detected_as_rollback`, `source_identity_change_requires_and_accepts_authorized_recovery` |
| Deterministic mapping from real keys/FKs/unique constraints; approval digest | Implemented | `mapping` unit tests; enterprise approval test |
| Graph semantics (receipts, version guards, tombstones, watermarks, pending edges, ownership-scoped delete, join tables, composite/self refs) | Implemented (reference sink) | `sink::conformance` suite passes on the memory sink |
| Policy (allow/deny, default-deny, labels as separate dimensions, tenant) | Implemented | `policy` unit tests; `denied_fields_are_never_journaled` |
| Retrieval boundary | Implemented | `tests/management.rs`, `policy_filtering` example; `qdrant_indexing_and_authorized_retrieval` passed against Qdrant 1.13.4 |
| Vector outbox + worker, templates, chunking, deterministic ids, hashing embedder, memory vector sink | Implemented | `vector` unit tests; `policy_filtering` example |
| Audit (encrypted spool, HMAC-chained file, verify, spool limit) | Implemented | `audit` + `state` unit tests |
| Reconciliation (counts + verified readback hashes) and evidence export | Implemented | `reconciliation_verifies_readback_and_detects_drift`; evidence not executed end-to-end against real services |
| Management API (axum router, tokens, scopes, correlation, idempotency) + OpenAPI v1 | Implemented | `tests/management.rs` (incl. route/spec parity) |
| CLI (`db2graph-sync`) | Implemented | `crates/db2graph-sync-cli/tests/cli.rs`; `run`, `preflight`, `bootstrap`, `reconcile`, `evidence` against real services not executed |
| Security profiles; restricted fails closed | Implemented | config tests, `restricted_profile_*` tests, CLI exit code test |
| **PostgreSQL connector** (pgoutput v1, exported snapshot, preflight, setup statements) | **Integration-tested, not CI-qualified** | unit tests; `postgres_to_neo4j_end_to_end` and `missing_slot_after_progress_is_resume_history_lost` passed against PostgreSQL 16.15 |
| **Neo4j sink** | **Integration-tested, not CI-qualified** | `neo4j_sink_conformance` (full conformance suite incl. join-table truncate) and end-to-end passed against Neo4j 5.26.31 Community |
| **Qdrant sink** | **Integration-tested, not CI-qualified** | `qdrant_indexing_and_authorized_retrieval` passed against Qdrant 1.13.4 |
| Pinecone sink | Experimental | id/encoding unit test only |
| HTTP (OpenAI-compatible) embedder | Experimental | response-parsing unit tests only |
| TypeScript service SDK | Implemented | `npm test`: 3/3 passed (Node 21.7.1, local mock) |
| Python service SDK | Implemented, not executed | tests written; no interpreter on the build machine (CI job defined) |
| MySQL, MongoDB/Atlas, DynamoDB Streams, SQL Server connectors | Planned | config types rejected with `unsupported` |
| Memgraph, Neptune sinks | Planned | config types rejected with `unsupported` |
| HA leader election, receipt pruning, protocol-v2 large transactions, re-embedding generations, CAC/PIV | Planned | — |
| FIPS 140-3 validated provider / restricted operation | Blocked | no validated module available; `neo4rs` pins `ring` (ADR 0005) |
| Benchmarks | Planned | no harness, no results |
| Container image | Implemented (Dockerfile), not built | no container runtime |

## Test results (this machine)

Executed 2026-10-05. All pass; none skipped.

| Suite | Result |
|-------|--------|
| Library unit tests (full features) | 57 passed |
| Library unit tests (`--no-default-features`) | 45 passed |
| `tests/engine.rs` (lifecycle, crash injection, fencing, recovery, policy, restricted) | 22 passed (5 consecutive runs) |
| `tests/management.rs` | 2 passed |
| CLI `tests/cli.rs` | 6 passed |
| Doctests | 1 passed |
| `tests/pg_neo4j.rs` via `scripts/integration.ps1` (PostgreSQL 16.15, Neo4j 5.26.31) | 3 passed, 0 UNEXECUTED |
| `tests/qdrant.rs` via `scripts/integration.ps1` (Qdrant 1.13.4) | 1 passed, 0 UNEXECUTED |
| Examples without external services (6) | ran successfully |
| TypeScript SDK (`npm test`, Node 21.7.1) | 3 passed |

Running against live servers found two defects that unit tests and the memory
sink did not: the Neo4j sink failed to decode null version values, and a
`TRUNCATE` of a join table paused the pipeline as unsupported. Both are fixed
(join-table truncate is now a sink operation covered by the conformance
suite).

## Static checks (executed)

* `cargo fmt --all --check`: pass.
* `cargo clippy -D warnings`: pass for `--no-default-features`, default, and
  the full compatible feature set, and for the CLI.
* `cargo deny --all-features check`: advisories ok (4 unmaintained crates via
  `neo4rs` acknowledged in `deny.toml`), bans ok, licenses ok, sources ok.
* Dependency audit: single rustls provider (`ring`); `cargo tree -i aws-lc-rs`
  and `-i openssl-sys` find nothing.
* `cargo package -p db2graph-sync` + clean independent consumer
  (`scripts/clean-consumer.ps1`): pass — packaged crate contains LICENSE,
  NOTICE and `api/openapi.json`, builds and syncs in a fresh project.
* `mdbook build docs`: see Documentation below.

## Not executed (with reason)

| Item | Reason |
|------|--------|
| TLS (`sslmode=verify-full`, `bolt+s://`, `https://`) against real servers | Test containers run plaintext under the development exception |
| Examples `embedded_postgres_neo4j`, `qdrant_indexing` | Need live services (compiled only) |
| CI workflows (`.github/workflows/ci.yml`, `release.yml`) | Not pushed to a repository |
| MSRV 1.90 build | Toolchain not installed locally (CI job defined) |
| Linux/macOS builds | No such machines; CI matrix defined |
| Python SDK tests | No Python interpreter |
| Container build/scan, SBOM, provenance, signatures | No container runtime; release workflow defined, never run |

## Publication blockers

1. Verified, owner-controlled GitHub organization and repository URL (not set
   anywhere in this repository by design).
2. crates.io ownership for `db2graph-sync` / `db2graph-sync-cli`; npm/PyPI
   names for the SDKs; container registry namespace.
3. Private vulnerability-reporting channel (SECURITY.md).
4. Code of Conduct enforcement contact (CODE_OF_CONDUCT.md).
5. Maintainer list (MAINTAINERS.md).
6. Documentation hosting location.
7. Signing/provenance configuration and protected `release` environment.
8. A green CI run on all three platforms, including the `integration` job
   (the live-server suite has passed locally on Windows only).

## Artifacts in this repository

* Library: `crates/db2graph-sync` · CLI: `crates/db2graph-sync-cli`
* OpenAPI: `crates/db2graph-sync/api/openapi.json`
* Docs site source: `docs/` (build with `mdbook build docs` → `docs/book/`)
* ADRs: `docs/src/adr/` · Threat model: `docs/src/security/threat-model.md`
* Examples: `crates/db2graph-sync/examples/`, `examples/config/`
* SDKs: `sdks/typescript`, `sdks/python`
* Test infrastructure: `deploy/test/docker-compose.yml`, `scripts/integration.{ps1,sh}`
* Supply chain: `deny.toml`, `THIRD_PARTY_NOTICES.md` (generated by `scripts/third-party-notices.*`), `scripts/clean-consumer.*`
* CI/release: `.github/workflows/`; container: `Dockerfile`
