# 0004 — Receipts, tombstones, pending relationships and vector outbox live in the graph transaction

Status: Accepted
Date: 2026-10-05

## Context

A crash after the graph commits but before the local checkpoint must replay
without effect; stale events must not resurrect deleted entities;
relationships whose endpoint has not arrived must not be lost; graph changes
and the creation of vector work must have a proven atomic boundary. Local
state and the graph are separate stores, so any fact needed to decide
idempotency *at apply time* must be in the same transaction as the effect.

## Decision

Each batch is one target transaction containing, in order, per source unit:
a receipt check (same digest → duplicate, different digest → consistency
incident and rollback; the digest covers the unit id and the captured unit
content, not the planned operations, so a replay after an approved mapping or
policy change is a duplicate), version-guarded entity writes, owned relationship
changes, pending relationship records, tombstones and truncate watermarks,
vector outbox entries, the receipt, and finally the pipeline marker (fence
token check, state epoch, last unit). The sink contract is executable as
`sink::conformance::run_all`. A sink declares capabilities; only a sink with
atomic batches, receipts in the target, guarded versions, fencing and
tombstones is reported as `effectively_once_effects`.

## Consequences

* Duplicate replay is a no-op by construction; identical identity and version
  with different content is surfaced as an incident instead of overwritten.
* Vector work cannot be lost relative to graph changes; vector delivery is
  asynchronous and tracked separately (delivered vs verified visible).
* Graph storage carries pipeline metadata (`_D2G_*` nodes). Receipts are
  retained indefinitely in this release (pruning is planned).
* The memory sink is the reference implementation; the Neo4j sink implements
  the same semantics and must pass the same suite against a live server.

## Evidence

`sink::memory::tests::memory_sink_conforms`; engine crash tests
(`crash_after_graph_commit_before_checkpoint_replays_as_duplicate`, etc.).
Neo4j conformance (`tests/pg_neo4j.rs::neo4j_sink_conformance`) passed against
a local Docker Neo4j 5.26.31 Community on Windows (2026-10-05); it has not yet
run in CI on all platforms (see [status](../project/status.md)).
