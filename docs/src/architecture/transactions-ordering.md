# Transactions and ordering

This page states what the engine guarantees about the order, grouping and
repetition of graph effects, and the exact conditions under which those
guarantees hold.

## Delivery contract

> **At-least-once transport with effectively-once graph effects under documented conditions.**

"At-least-once transport" means a source unit can be delivered to the engine
and to the graph sink more than once (after a crash, a reconnect, or a
replay of the journal). "Effectively-once graph effects" means that, when the
conditions below hold, a repeated unit has no additional effect on the graph:
it is recognized by its receipt and reported as a duplicate.

The conditions are:

1. **Single owner by fence.** Exactly one engine instance writes a pipeline's
   graph state at a time. This is enforced, not assumed: every batch verifies
   the pipeline fence token under the pipeline lock, and an instance with an
   older token fails with `fenced_out`. See
   [Ownership, HA and reconciliation](ha-reconciliation.md).
2. **The graph sink declares and implements the contract.** The sink must
   declare `atomic_batches`, `receipts_in_target`, `guarded_versions`,
   `fencing` and `tombstones`. Only then is `status.delivery_guarantee`
   `effectively_once_effects`; otherwise it is `at_least_once`. Declaring is
   not proving: the in-memory reference sink passes the conformance suite in
   the executed unit tests; the Neo4j sink implements the same contract, and its
   real-server conformance test passed against a local Docker Neo4j 5
   Community single instance on Windows, but it is not yet CI-qualified on all
   platforms (see [Neo4j](../destinations/neo4j.md)).
3. **The source delivers transaction boundaries.** If a connector declares
   `transaction_boundaries: false`, the effective guarantee is lowered to
   `at_least_once`. The PostgreSQL connector delivers boundaries.
4. **Stable unit identity and deterministic planning.** A replayed unit has the
   same `unit_id` (PostgreSQL: `pg:<system identifier>:<commit end LSN as 16
   hex digits>`) and the same receipt digest. The receipt digest covers the
   unit id and the captured unit content as journaled (its items, base version
   and kind), not the planned graph operations. Policy filtering happens
   before journaling, so replaying a journaled unit after an approved mapping
   or policy change (for example `update_policy` is called and the engine
   crashes before the checkpoint) is recognized as a duplicate, not a
   consistency incident. A receipt with the same unit id but a different
   digest is still reported as `consistency_incident` instead of silently
   applying different effects.
5. **Receipts are retained.** Duplicate detection relies on receipts stored in
   the graph target. Receipts are currently never pruned, so this holds; see
   [Capacity and cost](../operations/capacity-cost.md) for the growth this
   implies.
6. **Local state is not silently rolled back.** Rollback of the state
   directory is detected through the state epoch recorded in the graph
   pipeline marker (and the optional anchor file) and stops the engine with
   `state_rollback`. See [Durability](durability.md).
7. **The source retains history from the last acknowledged position.** If the
   replication slot is dropped or invalidated, the engine stops with
   `resume_history_lost` rather than skipping changes.

Graph and vector stores converge asynchronously. **There is no cross-store
atomicity.** Vector work is created atomically with the graph change (it is an
outbox record written in the same graph transaction), and is delivered to the
vector store later. See [Vector indexing and retrieval](vector-retrieval.md).

## Units

The unit of work is a `CapturedTransaction`:

- a committed **source transaction**, assembled by the connector from all of
  its changes (PostgreSQL: `Begin` to `Commit`), or
- a **snapshot chunk**: up to `source.snapshot.chunk_rows` rows (default 2 000)
  of one relation read during the initial snapshot.

Each unit carries a stable `unit_id`, an ordering scope, an acknowledgement
position, a base version, the native transaction id and commit time when
available, and its items in source order.

## Ordering scopes

A `SourcePosition` belongs to an `OrderingScope { source_id, epoch, partition }`.
Positions are comparable only within the same scope;
`SourcePosition::partial_cmp_scoped` returns `None` otherwise. Opaque positions
(no ordinal) are never compared.

| Field | PostgreSQL connector |
|---|---|
| `source_id` | `pg:<system identifier>` from `IDENTIFY_SYSTEM` |
| `epoch` | read from local state key `source_epoch`; defaults to 0 (see note) |
| `partition` | empty: one total commit order for the whole source |

> **Note on the source epoch.** A change of source system identifier stops the
> engine with `source_identity_changed`. The only way to continue is the
> authorized recovery `db2graph-sync recover --accept-source-change`, which
> increments `source_epoch` and clears the stored source identity,
> acknowledged position, snapshot progress and snapshot completion; the next
> start takes a new snapshot. Versions from the new epoch supersede versions
> from older epochs. Entity ids include the source id, so after a change to a
> different source id the rows are new identities and the previous source's
> entities remain until an operator removes them (see
> [Recovery](../operations/recovery.md#recover---accept-source-change)).

Connectors that cannot provide a total order must use per-partition scopes and
declare `ordering: per_partition`. No such connector exists in this release.

## Versions

Every graph effect carries a `Version { epoch: u32, position: u64, ordinal: u32 }`,
encoded as 32 lowercase hexadecimal characters (`%08x%016x%08x`) so that string
comparison in the target equals numeric comparison
([ADR 0006](../adr/0006-versions-and-ordering-scopes.md)).

| Unit kind | Version of each item |
|---|---|
| Streamed transaction | `(epoch, commit end LSN, event ordinal + 1)` |
| Snapshot chunk | `(epoch, snapshot boundary LSN, 0)` |

Consequences:

- Within a transaction, later changes to the same row carry higher versions.
- A snapshot row always loses to any streamed change committed after the
  snapshot boundary, which is how snapshot/stream overlap converges.
- The sink ignores writes whose version is not greater than the entity's
  current version, its tombstone, or its relation's truncate watermark
  ("stale"). The same entity id at the same version with a different content
  digest is a `consistency_incident`.

## Acknowledgement only after journaling

The capture loop processes one unit at a time:

1. apply backpressure: wait while the journal holds `state.max_journal_units`
   (default 50 000) or more units;
2. normalize the unit (drop unmapped relations, strip policy-denied fields);
3. append it to the encrypted journal; **the same commit** stores the
   acknowledgeable source position and, for snapshot chunks, the snapshot
   progress;
4. only then call `ChangeStream::ack` for that position;
5. wake the applier.

A connector must never confirm a position to the source on its own. The
PostgreSQL connector records the acknowledged LSN and reports it to the server
in standby status updates every `source.status_interval_secs` (default 10 s)
and when the server requests a reply. Keepalive positions are acknowledged
only when no source transaction is open, so everything before them has already
been delivered and journaled.

## Batching never splits or reorders units

The applier reads journal entries after the checkpoint, in sequence order, and
builds a batch:

- at most `engine.max_batch_transactions` units (default 64);
- whole units only; a unit is added only if the running change count stays at
  or below `engine.max_batch_changes` (default 5 000). The first unit is always
  taken, so a single unit larger than that limit forms a batch on its own and
  is never split;
- units keep journal order; a sink must apply the `GraphTx`s of a batch in
  order;
- if a unit cannot be planned (`schema_incompatible` or `unsupported`), the
  units before it are applied first, then the pipeline pauses with a
  `pause_reason` naming the unit. It is never skipped or reordered.

A batch is one atomic graph transaction. Graph readers can therefore observe
several source transactions becoming visible at once, but never part of one.
Batch size is a throughput setting; it does not change any guarantee.

Source transactions are buffered in memory until commit. A PostgreSQL
transaction with more than `source.max_transaction_changes` changes (default
200 000) stops the pipeline with `limit_exceeded`; streaming of in-progress
transactions (pgoutput protocol v2) is not implemented.

## Checkpoint contiguity

After a successful `GraphSink::apply`, the applier advances the checkpoint to
the last sequence of the batch and deletes those journal entries in the same
state commit. `StateStore::advance_checkpoint` verifies that every sequence
between the old and new checkpoint is present; a gap is an error and no partial
progress is recorded. Progress therefore always means "every journal unit up to
here has committed graph effects".

## Retries

| Failure | Behaviour |
|---|---|
| Retryable graph error (connection loss, Neo4j transient error) | The same batch is retried with capped exponential backoff; `counters.graph_retries` increases. |
| Retryable source error (including a PostgreSQL replication slot that is still `active` when the stream starts, reported as a retryable `ownership_conflict`) | The stream is dropped and restarted from the stored acknowledged position with capped exponential backoff; `counters.source_reconnects` increases. |
| Startup preflight, identity, fence acquisition | Retried while the error is retryable. |
| Non-retryable error | Terminal: the engine stops and reports the error kind. |

## Source files

- `crates/db2graph-sync/src/engine.rs` — `capture_unit`, `apply_available`
- `crates/db2graph-sync/src/state/mod.rs` — `append_journal`, `advance_checkpoint`
- `crates/db2graph-sync/src/types.rs` — `Version`, `SourcePosition`, `OrderingScope`
- `crates/db2graph-sync/src/sink/mod.rs` — sink contract
