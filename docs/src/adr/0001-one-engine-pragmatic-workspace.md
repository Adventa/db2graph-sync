# 0001 — One shared engine and a pragmatic two-crate workspace

Status: Accepted
Date: 2026-10-05

## Context

The project must run embedded in a Rust application, as a standalone process,
and behind a service API, and it must not fork behaviour between those modes.
Users of a PostgreSQL→Neo4j pipeline must not compile every driver.
Excessive crate fragmentation adds publishing and versioning overhead without
improving correctness.

## Decision

* One library crate, `db2graph-sync` (`db2graph_sync`), contains the engine,
  the extension traits, the reference components (scripted source, in-memory
  graph and vector sinks), and the built-in adapters behind cargo features
  (`postgres`, `neo4j`, `vector-qdrant`, `vector-pinecone`, `embedding-http`,
  `service`).
* One binary crate, `db2graph-sync-cli`, provides the standalone process and
  operator CLI. It calls the same `start` function as embedded users and adds
  only process concerns (logging, signals, serving the management router).
* Service SDKs (TypeScript, Python) are HTTP clients of the management API in
  `sdks/`; they do not embed the engine.

## Consequences

* Embedded and standalone runs share every correctness path; tests of the
  library cover both.
* A minimal build (`--no-default-features`) carries no network drivers.
* Adapters that grow large may later move to their own crates without changing
  the public trait surface.

## Evidence

`cargo check -p db2graph-sync --no-default-features`, default features, and the
full compatible feature set all build; `cargo tree -i aws-lc-rs` and
`cargo tree -i openssl-sys` return no matches for the full set.
