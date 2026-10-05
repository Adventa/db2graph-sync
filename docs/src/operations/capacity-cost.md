# Capacity and cost

No capacity measurements have been made for this release. This page describes
what consumes resources, which settings bound it, and what to measure in your
own environment. See [Benchmarks](benchmarks.md) for methodology.

## Local state (`state.dir`)

| Consumer | Bound | Notes |
|---|---|---|
| Journal | `state.max_journal_units` units (default 50 000); capture waits at the limit | Bounded by **unit count, not bytes**. A unit is a whole source transaction (up to `source.max_transaction_changes`, default 200 000 changes) or a snapshot chunk (`snapshot.chunk_rows`, default 2 000 rows). Each unit is stored as sealed JSON of its policy-filtered row images. Size the volume for the limit times your largest expected unit, plus headroom. |
| Audit spool | `audit.spool_max_records` (default 100 000) | Records are removed once forwarded. Without audit sinks it is a ring of that size. |
| Schema history | one record per schema version | Never pruned. |
| Metadata | small | Checkpoint, positions, snapshot progress, identity, counters. |

redb files do not necessarily shrink when entries are deleted; measure the
file size under load rather than assuming it tracks the backlog.

Storage exhaustion stops the engine with `state_io`; see
[Durability](../architecture/durability.md#storage-exhaustion).

## Source: WAL retention (PostgreSQL)

The replication slot keeps WAL from its `restart_lsn`. Retained WAL grows
whenever the engine does not confirm progress:

- the engine is stopped or failed;
- the pipeline is paused (manually, waiting for mapping approval, or on an
  unplannable unit);
- the journal is full and capture applies backpressure (graph target slow or
  unavailable);
- a large transaction is being decoded.

The engine confirms positions to the server every
`source.status_interval_secs` (default 10 s), so some retention is normal.

Measure with:

```sql
SELECT slot_name, active, wal_status,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
FROM pg_replication_slots;
```

Set `source.max_slot_retained_bytes` to get a `retention` warning from
preflight. If the server limits retention (`max_slot_wal_keep_size`) and the
slot is invalidated, the pipeline stops with `resume_history_lost`, and there is
no in-place resynchronization. Size the WAL volume for the longest outage you
intend to survive.

## Graph target (Neo4j)

Projection metadata adds to the size of the projected data:

| Item | Count | Content |
|---|---|---|
| Per entity node | one node per projected row | `_D2G_Entity` label plus up to 12 `_d2g_*` properties (key, id, pipeline, relation, tenant, version, digest, labels JSON, owned-property list, null list, incomplete list, deleted flag) and one `_d2g_uk_*` property per referenced unique key |
| Per owned relationship | one per FK value or join row | 8 `_d2g_*` properties (owned flag, pipeline, edge key, owner, and four endpoint reference fields) |
| `_D2G_Receipt` | **one per applied unit, including every snapshot chunk** | key, pipeline, unit id, digest, version, applied time. **Never pruned in this release**: the count grows with the number of source transactions for the lifetime of the pipeline. Receipt pruning is planned. |
| `_D2G_Tombstone` | one per deleted entity or join row | kept until the entity is re-created; grows with deletes of distinct keys |
| `_D2G_Watermark` | one per truncated relation | |
| `_D2G_PendingEdge` | one per unresolved reference | watch `pending_edges`; references to rows that never arrive (for example filtered by policy or tenant) stay pending |
| `_D2G_VectorTask` | one per entity with pending vector work | drains as the worker completes tasks |
| `_D2G_JoinState` | one per join-table row | |
| Constraint and index entries | per the bootstrap list | |

Every batch takes a write lock on the pipeline node, so all writes of one
pipeline are serialized; throughput per pipeline is bounded by sequential
batch latency. Each operation issues several statements (guard read, write,
pending-relationship resolution), so round-trip latency to the server matters.

## Embedding and vector cost

| Control | Setting |
|---|---|
| Embedding request rate | `vector.max_embedding_requests_per_minute` (default 600). One request per entity version (all chunks of the entity in one call). |
| What is embedded | Only relations with a template, only fields in `embed_fields` that policy allows. |
| Chunk size | `chunk_chars` per template (default 2 000 characters). |

There is no token or currency budget. Re-processing (a task interrupted
after delivery, or superseded versions arriving between reads) repeats
embedding calls. Each entity change produces a new embedding call. Controlled
re-embedding with cost budgets is planned.

Vector store size: one point per chunk per entity, payload fields as listed in
[Vector indexing and retrieval](../architecture/vector-retrieval.md#deterministic-point-ids-and-payload).

## What to measure

Exposed in `SyncHandle::status()` and `GET /v1/metrics` (Prometheus text,
metric prefix `d2g_`, label `pipeline`):

| Metric | Use |
|---|---|
| `d2g_journal_backlog` | journaled units not yet applied; sustained growth means the graph cannot keep up |
| `d2g_last_lag_ms` | source commit time to graph apply time of the last applied transaction (includes clock difference between source and engine hosts) |
| `d2g_units_captured_total`, `d2g_units_applied_total` | throughput |
| `d2g_duplicates_total` | replays recognized by receipt (expected after restarts) |
| `d2g_graph_retries_total`, `d2g_source_reconnects_total` | transient failure rate |
| `d2g_vector_backlog` | pending vector tasks |
| `d2g_pending_edges` | unresolved references |
| `d2g_audit_spool` | audit records awaiting forwarding |
| `d2g_ready`, `d2g_failed` | health |

Also measure outside the engine: slot retained bytes, `state.dir` file size,
Neo4j store size and receipt count
(`MATCH (r:_D2G_Receipt {_d2g_pipeline: $p}) RETURN count(r)`), engine memory
during the largest transactions, and embedding provider usage.
