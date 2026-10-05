# db2graph-sync

**db2graph-sync — an open-source project by Adventa.** <https://www.adventa.com.au>

db2graph-sync loads approved records from an operational database and keeps a
graph database (and, optionally, a vector index for GraphRAG) synchronized with
their changes. The source database stays authoritative: this is a one-way,
policy-filtered projection engine written in Rust, usable as an embedded
library, a standalone process, or a service controlled over HTTPS.

```rust
let sync = db2graph_sync::start(config).await?;
```

> **Status: pre-release (0.1.0, unpublished).** Read
> [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) and
> [LIMITATIONS.md](LIMITATIONS.md) before relying on any capability. In short:
> the engine, durability, policy, retrieval boundary, management API and CLI
> are implemented and tested; the PostgreSQL connector, Neo4j sink and Qdrant
> sink pass their integration tests against live servers (local Docker on
> Windows) but are **not yet CI-qualified** on all platforms; the other
> connectors and graph targets are **planned**.

## What it does

* Captures committed source transactions (PostgreSQL logical replication,
  `pgoutput`) and an initial snapshot that is transaction-consistent at the
  replication slot's consistent point.
* Applies whole transactions, in order, to the graph in atomic batches with
  receipts, version guards, tombstones, truncate watermarks, fencing and
  pending-relationship tracking, so duplicate or stale replays have no effect.
* Derives nodes from tables and relationships from **actual** foreign keys and
  unique constraints; semantic relationship names are used only when an
  operator approves them in the mapping.
* Enforces dataset/field allowlists and deny rules *before* anything is
  persisted, and models classification, CUI categories, export control,
  releasability and tenant as separate dimensions.
* Optionally renders approved text, embeds it, and indexes it in a vector store
  through a durable outbox created in the same graph transaction; an
  authorized retrieval boundary re-checks tenant, labels, existence and
  version against the current graph and policy.
* Keeps all durable state (journal, checkpoints, snapshot progress, schema
  history, audit spool) encrypted and authenticated, and detects tampering and
  rollback to older copies.

**Delivery contract:** at-least-once transport with effectively-once graph
effects under documented conditions. Graph and vector stores converge
asynchronously; there is no cross-store atomicity. See
[docs/src/architecture/transactions-ordering.md](docs/src/architecture/transactions-ordering.md).

## Integration modes

| Mode | How it runs | Entry point |
|------|-------------|-------------|
| Embedded Rust library | Inside your application's Tokio runtime | `db2graph_sync::start(config)` / `start_with(config, options)` |
| Standalone binary | Its own process | `db2graph-sync run -c config.toml` |
| Service SDK | Your app (TypeScript/Python) controls or observes a running service over HTTPS | `sdks/typescript`, `sdks/python` (HTTP clients; they do not embed the engine) |

The library never creates a runtime, installs a logger or signal handler,
binds a port, changes environment variables, or exits the process. Start it
once in your application's startup hook — never per request — and use exactly
one engine per state directory and replication slot.

## Installation

Registry packages are **pending publication** (`cargo add db2graph-sync` and
container images do not work yet). Tested local route:

```toml
# Cargo.toml of your application
[dependencies]
db2graph-sync = { path = "<checkout>/crates/db2graph-sync" }   # features: postgres, neo4j (default)
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```sh
cargo install --path crates/db2graph-sync-cli     # the `db2graph-sync` binary
```

Requirements: Rust **1.90+** (MSRV), a C compiler for `ring` (MSVC Build Tools
on Windows), and for integration tests Docker. Developed and tested on
Windows 11 x86_64; Linux and macOS are in the CI matrix but no CI run has
executed yet.

Cargo features: `postgres`, `neo4j` (default), `vector-qdrant`,
`vector-pinecone`, `embedding-http`, `service`. `--no-default-features` gives
a core build with no network drivers.

## Prerequisites (operator steps; the library never alters your databases)

1. PostgreSQL ≥ 13 with `wal_level = logical`, a role with `REPLICATION` (or
   `rds_replication`) and `SELECT` on published tables, and a publication.
   `db2graph-sync source-setup -c config.toml` prints the statements to review.
2. Neo4j 5 reachable over `bolt+s://`; run `db2graph-sync bootstrap -c config.toml`
   to create constraints and indexes.
3. Secrets in a secret manager or files, referenced as `env:` / `file:`; a
   32-byte state key (`db2graph-sync keygen`).
4. Outside development: approve the mapping digest printed by
   `db2graph-sync plan` and configure a dataset allowlist.

## Examples

Runnable without external services (synthetic data):
`host_lifecycle`, `custom_graph_sink`, `custom_state_backend`,
`restart_replay`, `policy_filtering`, `composite_keys`:

```sh
cargo run -p db2graph-sync --features service --example restart_replay
```

Against real services: `embedded_postgres_neo4j`, `qdrant_indexing` (see
`deploy/test/docker-compose.yml` and `examples/config/`).

## Documentation

* Local site: `mdbook build docs` (output in `docs/book/`) or `mdbook serve docs`.
* API reference: `cargo doc -p db2graph-sync --open`.
* Management API contract: [crates/db2graph-sync/api/openapi.json](crates/db2graph-sync/api/openapi.json).
* Architecture decisions: [docs/src/adr](docs/src/adr/index.md); threat model:
  [docs/src/security/threat-model.md](docs/src/security/threat-model.md).

## Project

[CONTRIBUTING.md](CONTRIBUTING.md) · [GOVERNANCE.md](GOVERNANCE.md) ·
[MAINTAINERS.md](MAINTAINERS.md) · [SECURITY.md](SECURITY.md) ·
[SUPPORT.md](SUPPORT.md) · [ROADMAP.md](ROADMAP.md) ·
[CHANGELOG.md](CHANGELOG.md) · [RELEASING.md](RELEASING.md) ·
[DEVELOPMENT.md](DEVELOPMENT.md) · [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## License

Apache-2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Using db2graph-sync requires
no Adventa account, license server, payment, telemetry or network call to
Adventa, and no Adventa branding in your application. Database, cloud,
embedding-provider and cryptographic-module terms remain separate.
