# 0003 — PostgreSQL capture: own pgoutput client; Supabase ETL not adopted

Status: Accepted
Date: 2026-10-05

## Context

The project needs PostgreSQL logical replication (`pgoutput`) with exact
transaction boundaries, a transaction-consistent initial snapshot tied to the
slot's consistent point, acknowledgement only after durable journaling, and a
crates.io-publishable dependency graph. Supabase ETL
(<https://github.com/supabase/etl>, Apache-2.0) was evaluated for reuse.

## Evaluation (2026-10-05)

* Supabase ETL's workspace depends on `tokio-postgres` and
  `postgres-replication` from a **git fork** (`supabase/rust-postgres`, pinned
  revision), not from crates.io. crates.io rejects packages with git
  dependencies, so a published `db2graph-sync` could not depend on it.
* Its README states it "is installed from Git while we prepare for a crates.io
  release" and that "APIs and setup steps may change before the first stable
  release".
* It documents at-least-once delivery and an initial copy followed by
  catch-up; its README does not document transaction-boundary handling, and
  its destinations (BigQuery etc.) differ from ours. We did not find a
  documented mechanism for applying our policy filtering before persistence or
  for our encrypted state.
* The crates.io crate named `etl` is an unrelated project.
* Upstream `tokio-postgres` 0.7.18 on crates.io has no CopyBoth/replication
  support (no `copy_both`/replication symbols in its source).

## Decision

Implement a minimal replication client in `source/postgres/wire.rs` using the
crates.io `postgres-protocol` crate (frontend messages, SCRAM-SHA-256, MD5)
with our own backend-message framing (needed for `CopyBothResponse`), and use
upstream `tokio-postgres` for regular connections (discovery, snapshot reads,
preflight). Decode `pgoutput` protocol v1 ourselves (`pgoutput.rs`).

No Supabase ETL source code was copied. If code is reused in future, its
license and attribution will be preserved in THIRD_PARTY_NOTICES.md.

## Consequences

* We own correctness of the wire protocol and decoder; they are unit tested on
  constructed messages and must be qualified against live servers
  (`tests/pg_neo4j.rs`, not yet executed in this release).
* Protocol v2 streaming of in-progress transactions is not implemented; large
  transactions are buffered up to `max_transaction_changes`.
* Re-evaluate when Supabase ETL publishes to crates.io with upstream drivers.
