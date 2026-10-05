# Implementation status

The authoritative documents are in the repository root:

- **`IMPLEMENTATION_STATUS.md`**: what is implemented, tested, unqualified
  or planned, plus publication blockers.
- **`LIMITATIONS.md`**: known limitations and their operational impact.

This page summarizes them for version 0.1.0.

## Implemented and tested (Windows 11 x86_64 MSVC, Rust 1.99.0)

- Core engine: capture loop, encrypted journal, ordered planner and applier,
  whole-transaction batching, fencing (rollback evidence checked before the
  fence is taken), rollback detection, authorized recovery (state reset and
  source change with a new source epoch), pause and resume, mapping
  approval, backpressure.
- Encrypted durable state (redb backend, custom `StateBackend`), key
  rotation, crypto known-answer tests.
- Policy (allow/deny/embed fields, labels, tenancy) and the retrieval
  validator.
- Mapping plans: composite keys, unique-key references, self references,
  pending edges, join tables, truncate.
- Audit spool, plus the JSONL audit file with HMAC chain verification.
- In-memory graph sink (reference) and its conformance suite; scripted
  source.
- Management API (`service`) and CLI.
- Test runs that passed:
  - 57 library unit tests (45 with `--no-default-features`)
  - 22 engine tests (5 consecutive clean runs)
  - 2 management tests
  - 6 CLI tests
  - 1 doctest
- Examples that ran successfully: `host_lifecycle`, `custom_graph_sink`,
  `custom_state_backend`, `restart_replay`, `policy_filtering`,
  `composite_keys`.
- Examples that were compiled only: `embedded_postgres_neo4j`,
  `qdrant_indexing`.

## Implemented; integration-tested against live servers (local Docker, Windows); not yet CI-qualified on all platforms

- PostgreSQL source (pgoutput protocol v1).
- Neo4j 5 graph sink.
- Qdrant vector sink.

Their real-server integration tests were executed and passed on 2026-10-05
on Windows 11 x86_64 with local Docker 29.3.1, using
`scripts/integration.ps1` (no test reported `UNEXECUTED`):

| Test file | Tests | Result |
|---|---|---|
| `tests/pg_neo4j.rs` | `postgres_to_neo4j_end_to_end`, `neo4j_sink_conformance`, `missing_slot_after_progress_is_resume_history_lost` | 3/3 passed |
| `tests/qdrant.rs` | `qdrant_indexing_and_authorized_retrieval` | 1/1 passed |

Servers (from `deploy/test/docker-compose.yml`): PostgreSQL 16.15
(`postgres:16`), Neo4j 5.26.31 Community (`neo4j:5-community`) and Qdrant
1.13.4 (`qdrant/qdrant:v1.13.4`).

This run found and fixed two defects: the Neo4j sink failed to decode null
version values, and `TRUNCATE` of a declared join table paused the pipeline
as `unsupported` (it is now applied as a join-table watermark plus removal of
that join table's relationships, with conformance coverage).

What this run does not cover:

- Only Windows with Docker Desktop was used. Qualification additionally
  requires a green CI run on Linux, macOS and Windows, which has not
  happened, so the Neo4j and Qdrant sinks still report `qualified: false`
  (the engine shows a "not qualified" status warning for the Neo4j graph
  sink).
- The tests used the plaintext development exception (PostgreSQL TLS
  disabled, `bolt://`, `http://`). TLS (`verify-full`, `bolt+s://`,
  `https://`) against these servers is not tested.
- Amazon RDS/Aurora, standbys, partitioned tables and Neo4j clusters are not
  tested.
- The `embedded_postgres_neo4j` and `qdrant_indexing` examples were compiled,
  not run.

## Experimental or development-only

- Pinecone vector sink: never run against a live index.
- HTTP OpenAI-compatible embedder: unqualified.
- Hashing embedder: not semantic; `development` only.

## Planned (not implemented)

- MySQL, MongoDB/Atlas, DynamoDB Streams and SQL Server sources.
- Memgraph and Neptune graph targets.
- High availability beyond a single owner plus fencing (no leader
  election).
- Receipt pruning.
- Concurrency beyond ordered batching.
- Streaming of large in-progress transactions (protocol v2).
- Re-embedding generations, cutover and cost budgets.
- CAC/PIV integration.
- A FIPS 140-3 validated crypto provider.
- Re-snapshot sweep for generation > 1.
- Re-bootstrap/cleanup of entities left from a previous source id after
  `recover --accept-source-change`.
- Qualification for partitioned tables, and for RDS/Aurora.
- Benchmark results.
- Signed releases.

## Not yet done

- No CI run (Linux and macOS not built or tested; the live-server
  integration tests ran only locally on Windows).
- No benchmarks. The performance targets (graph p95 ≤ 20 ms, p99 ≤ 100 ms,
  20k changes/s/core) are proposed stretch goals and have not been measured.
- No container image built.
- No crate, npm or PyPI package published.
- Repository URL, crates.io ownership, maintainers and security contact: TBD
  — publication blocker (see IMPLEMENTATION_STATUS.md).
