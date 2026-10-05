# Installation

## Prerequisites

| Requirement | Detail |
|---|---|
| Rust | 1.90 or newer (MSRV, required by `redb` 4.x). Edition 2024. |
| Async runtime (library use) | Tokio. `start` must be called from inside a Tokio runtime. |
| C compiler | Needed to build `ring` (see below). |

### Platform status

| Platform | Status |
|---|---|
| Windows 11 x86_64 (MSVC toolchain) | Developed and tested here, with Rust 1.99.0 |
| Linux | Intended; not yet built or tested (no CI run has happened) |
| macOS | Intended; not yet built or tested (no CI run has happened) |

### Native toolchain for `ring`

These all depend on `ring`, which compiles C and assembly code:

- the TLS stack (rustls with the `ring` provider), pulled in by the
  `postgres`, `vector-qdrant`, `vector-pinecone` and `embedding-http`
  features;
- the Neo4j driver (`neo4rs` 0.8.0, `neo4j` feature);
- the CLI crate, always.

You need a C toolchain for that:

- **Windows:** Visual Studio Build Tools with the "Desktop development with
  C++" workload (MSVC compiler and Windows SDK).
- **Linux:** `gcc` or `clang` (e.g. `build-essential`). Not yet verified.
- **macOS:** Xcode Command Line Tools (`xcode-select --install`). Not yet
  verified.

## Library: local path dependency (tested route)

The crates are not published yet. Clone the repository (its URL is TBD —
publication blocker, see IMPLEMENTATION_STATUS.md), then add the library by
path:

```sh
cargo add --path <checkout>/crates/db2graph-sync
```

or add it to your `Cargo.toml` yourself:

```toml
[dependencies]
db2graph-sync = { path = "<checkout>/crates/db2graph-sync" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Default features are `postgres` and `neo4j`. To choose features yourself:

```toml
db2graph-sync = { path = "<checkout>/crates/db2graph-sync", default-features = false, features = ["postgres", "neo4j", "service"] }
```

See [Feature flags](../reference/feature-flags.md).

## CLI and standalone binary: local install (tested route)

From the checkout root:

```sh
cargo install --path crates/db2graph-sync-cli
db2graph-sync about
```

This installs a binary named `db2graph-sync`. Its default features are
`postgres neo4j vector-qdrant vector-pinecone embedding-http`, and the
management API (`service`) is always compiled in. For a smaller build:

```sh
cargo install --path crates/db2graph-sync-cli --no-default-features --features postgres,neo4j
```

## Registry routes (pending publication)

None of these work yet. They are listed so you know what to expect later.

| Route | Status |
|---|---|
| `cargo add db2graph-sync` | Pending publication (crates.io ownership: TBD — publication blocker) |
| `cargo install db2graph-sync-cli` | Pending publication |
| `docker pull …` | Pending publication. No container image has been built or published; registry location TBD — publication blocker. |
| TypeScript / Python SDK packages | Unpublished; see [Service SDKs](../sdks/index.md) |

## Building and testing from a checkout

```sh
cargo build --workspace
cargo test --workspace
```

`cargo test` runs the unit, engine, management and CLI tests, none of which
need external services. The tests in
`crates/db2graph-sync/tests/pg_neo4j.rs` and `tests/qdrant.rs` need live
PostgreSQL 16, Neo4j 5 and Qdrant. Run them with
`scripts/integration.sh` or `scripts/integration.ps1`: these start
`deploy/test/docker-compose.yml` and fail if any test reports `UNEXECUTED`.
They were executed and passed with `scripts/integration.ps1` on Windows 11
with local Docker (see the [status page](../project/status.md));
`scripts/integration.sh` and Linux/macOS runs have not been executed.

Building the documentation:

```sh
mdbook build docs          # mdBook 0.4
cargo doc -p db2graph-sync --features "postgres neo4j vector-qdrant vector-pinecone embedding-http service" --open
```
