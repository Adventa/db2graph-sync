# db2graph-sync

db2graph-sync is an embeddable, one-way change-data-capture (CDC) projection
engine written in Rust. It loads approved records from an operational database
and keeps their changes synchronized into a graph database and, optionally, a
vector index for GraphRAG. The source database stays authoritative, and
db2graph-sync never writes to it.

*db2graph-sync — an open-source project by Adventa.* <https://www.adventa.com.au>

Licensed under Apache-2.0. Integrators don't need to show Adventa branding;
see [Licensing and attribution](project/licensing.md).

## Component status in this release

| Component | Status |
|---|---|
| Core engine (journal, planner, batching, fencing, encrypted state, policy, mapping, audit, retrieval validator) | Implemented; covered by unit and in-process engine tests |
| In-memory graph sink (reference / executable specification), scripted source | Implemented; tested |
| PostgreSQL source (pgoutput logical replication) | Implemented; integration-tested against live servers (local Docker, Windows; see [status page](project/status.md)); not yet CI-qualified on all platforms |
| Neo4j 5 graph sink | Implemented; integration-tested against live servers (local Docker, Windows; see [status page](project/status.md)); not yet CI-qualified on all platforms |
| Qdrant vector sink | Implemented; integration-tested against live servers (local Docker, Windows; see [status page](project/status.md)); not yet CI-qualified on all platforms |
| Pinecone vector sink | Experimental; never run against a live index |
| HTTP (OpenAI-compatible) embedding provider | Experimental; unqualified |
| Hashing embedder | Development profile only; not semantic |
| Management HTTP API (`service` feature), standalone CLI | Implemented; tested in-process |
| MySQL, MongoDB/Atlas, DynamoDB Streams, SQL Server sources | **Planned** (config is parsed, then rejected with `unsupported`) |
| Memgraph, Amazon Neptune graph targets | **Planned** (config is parsed, then rejected with `unsupported`) |

[Implementation status](project/status.md) has the full list, including
everything that is planned.

## Delivery contract

**At-least-once transport, with effectively-once graph effects under the
documented conditions:**

- one owner per pipeline, enforced by a fence token in the graph, and
- a graph sink that declares and is qualified for atomic batches, receipts
  stored in the target, guarded versions, fencing and tombstones.

Graph and vector stores converge asynchronously, with no atomicity across the
two. Vector work is recorded as an outbox entry inside the same graph
transaction as the graph change it belongs to.

## How to integrate

- **Embedded library:** `db2graph_sync::start(config).await?` on your
  application's Tokio runtime.
- **Standalone binary:** `db2graph-sync run -c config.toml`.
- **Service SDKs** (TypeScript, Python): HTTP clients for the management API
  of an engine running elsewhere.

See [Integration modes](guides/integration-modes.md).

## What has been tested

All testing so far ran on Windows 11 x86_64 (MSVC) with Rust 1.99.0. These
suites passed:

- 51 library unit tests
- 18 engine tests
- 2 management API tests
- 6 CLI tests
- six runnable examples

The tests that need real PostgreSQL, Neo4j and Qdrant servers have not been
executed. No CI run, benchmark, container image or package publication has
happened yet. See [Implementation status](project/status.md).
