# Incident response

Terminal errors stop every engine task. The error kind is reported in
`status.failure.kind`, returned from `SyncHandle::failed()` and
`SyncHandle::wait()`, written to the audit log (`failure` / `engine_failed`),
and, for the standalone binary, mapped to an exit code
([Errors and exit codes](../reference/errors.md)).

General rules:

- **Do not restart in a loop.** Most kinds below recur on restart until the
  cause is removed.
- **Preserve evidence first**: `db2graph-sync evidence -c <config> --out <file>`
  (with the engine stopped, the state section is included), the audit file, and
  the engine logs.
- There is no command to skip a unit or to force the graph to accept different
  effects.

## consistency_incident (exit 7)

**Meaning.** The graph already holds a different result for the same input:

- a unit's receipt exists with a different digest (the same unit id arrived
  with different captured content); the batch is rolled back;
- the same entity id and version arrives with different content;
- a `transaction_consistent` snapshot's row counts do not match the graph's
  entity counts at validation (relations with a `tenant_column` policy are not
  count-checked; they are recorded as `count_not_comparable`).

The receipt digest covers the captured unit (items, base version, kind), not
the planned graph operations, so replaying a journaled unit after an approved
mapping or policy change is a duplicate, not an incident.

**Likely causes.** The source delivered different content under a unit id that
was already applied; another writer modifying `_d2g_*` metadata or receipts; a
defect.

**Actions.**

1. Read the `consistency_incident` or `snapshot_validation` audit record; it
   names the unit or relation and both digests or counts.
2. Treat it as a defect or tampering unless the cause is evident: keep the
   graph and state as they are and collect evidence.

## fenced_out (exit 6)

**Meaning.** Another engine instance acquired the pipeline's fence; this
instance's batch had no effect.

**Actions.** Decide which instance is the intended owner. Stop the other one.
Restarting this instance fences out the other instance in turn (provided this
instance's state is not behind the graph's evidence; an instance with stale
state stops with `state_rollback` before taking the fence). See
[Ownership, HA and reconciliation](../architecture/ha-reconciliation.md).

## ownership_conflict (exit 6)

**Meaning.** The state directory is locked by another running engine (terminal,
exit 6), or the server rejected the replication start because the slot is in
use (SQLSTATE `55006`, terminal). (A slot that is already active at startup
fails the source preflight instead: `preflight`, exit 3.)

A replication slot found `active` when the stream is opened is **not**
terminal: it is a retryable `ownership_conflict`. The engine retries with
backoff, increments `counters.source_reconnects` and shows the error in
`status.warnings`; it continues once PostgreSQL releases the slot (for example
after `wal_sender_timeout` for a previous connection of this engine). A
genuinely competing consumer keeps it retrying.

**Actions.** Find the other process (same host for the directory lock;
`pg_replication_slots.active_pid` for the slot). Stop it, or wait until
PostgreSQL releases the slot after the old connection ends.

## state_rollback (exit 5)

**Meaning.** Local state is older than trusted evidence: the graph pipeline
marker recorded a higher state epoch, or the anchor file holds a higher epoch
or belongs to a different store.

**Actions.**

1. Establish what happened: restored backup, replaced volume, wrong
   `state.dir`, or a second deployment pointing at the same graph pipeline.
2. If the state directory is simply the wrong one, fix the configuration.
3. If the loss or restore is real and accepted, run
   [authorized recovery](recovery.md). Account for the data exposure described
   in [Durability](../architecture/durability.md#node-loss). If the state
   directory was replaced by a new, empty store and `state.anchor_file` is set,
   the old anchor names the old store id and keeps reporting
   `state_rollback`: move it aside as part of the recovery.

A second instance started with stale or empty state against a pipeline whose
owner is healthy stops here **before** taking the fence, so the running owner
is not affected.

## state_tampered (exit 5)

**Meaning.** A state record, the anchor file, or an audit line failed
authentication: wrong or missing key, modified or corrupted bytes, a record
moved between keys, or redb reported corruption.

**Actions.**

1. Check the key configuration: every key id used by stored records must be
   configured, and each id must resolve to the same key bytes as before. This
   includes key ids recorded in audit lines and in the anchor file (see
   [Key rotation](key-rotation.md)).
2. If the keys are correct, treat it as a security incident: preserve the
   directory, audit file and logs; do not modify them.
3. Recover by restoring `state.dir` from a backup (leads to `state_rollback`,
   then authorized recovery) or by starting with an empty directory (node-loss
   path).

## resume_history_lost (exit 3)

**Meaning.** The source no longer has the history needed to resume: the
replication slot is missing while local state has progress, the slot's
`wal_status` is `lost`, or the server reported that required WAL was removed.

**Impact.** Changes after the last acknowledged position cannot be obtained
from the source. The engine stops instead of skipping them.

**Actions.** There is no in-place resynchronization in this release
(in-place re-snapshot with sweep and cutover is planned). Build a new projection: drop
the invalidated slot (operator decision), remove this pipeline's data from the
graph or choose a new `pipeline.id`, use a new `state.dir`, run `bootstrap`,
and start with the snapshot enabled. Then review slot retention settings
([Capacity and cost](capacity-cost.md)).

## source_identity_changed (exit 4)

**Meaning.** The source reports a different identity (PostgreSQL system
identifier) than the one stored in local state.

**Actions.**

1. If the configuration points at the wrong server, correct it.
2. If the source really was replaced (new cluster, restore onto a new
   cluster) and the new history is accepted, stop the engine and run
   [`db2graph-sync recover --accept-source-change --reason "..."`](recovery.md#recover---accept-source-change).
   It increments the source epoch and clears the stored identity, position and
   snapshot state; the next start takes a new snapshot.
3. Entity ids include the source id, so the new snapshot's rows are **new
   identities**. Entities projected from the previous source remain in the
   graph until you remove them (re-bootstrap/cleanup of the previous source's
   entities is planned, not implemented). Alternatively build a new
   projection (new `pipeline.id` or cleaned graph, new `state.dir`, new slot)
   and retire the old one.

A timeline change with the same system identifier is not this error: it
produces a status warning and a `source_history_changed` audit record.

## audit_unavailable (exit 4)

**Meaning.** The audit spool reached `audit.spool_max_records` because records
could not be forwarded, or the audit file could not be written or read. The
engine stops rather than continue unaudited.

**Actions.** Fix the destination (disk space, permissions, path), then
restart. At start the engine first drains the spool to its sinks and only then
writes its own start record, so a spool filled during an outage does not block
the restart once the destination accepts records again. If the destination is
still down and the spool is still full, the start fails closed again with
`audit_unavailable`. Spooled records are not dropped while audit sinks are
configured. Verify the file afterwards with `db2graph-sync audit-verify`.

## limit_exceeded (exit 4)

**Meaning.** A source transaction has more changes than
`source.max_transaction_changes` (default 200 000). The transaction was not
journaled or acknowledged.

**Actions.** Raise the limit (memory use grows with transaction size) and
restart; the transaction is delivered again from the slot. Alternatively split
such transactions at the source.

## Paused pipeline (no error kind)

A pause is not a failure: `status.phase = paused`, `ready = false`, and
`status.pause_reason` says why. Two automatic causes:

**Mapping not approved.** `pause_reason`: "mapping plan `<digest>` is not
approved (schema `<version>`)". A schema change or configuration change
produced a new mapping digest.

1. Inspect the `schema_version` audit record (it lists the classified
   changes) and run `db2graph-sync plan -c <config>` to review the plan.
2. Approve: `db2graph-sync approve-mapping --url ... --token-file ... --digest <digest>`
   (or `SyncHandle::approve_mapping`, or `POST /v1/mapping/approve`).
3. Put the same digest in `mapping.approved_digest`; runtime approval is not
   persisted.

**Unit cannot be applied.** `pause_reason`: "unit `<id>` cannot be applied:
...". The unit is `schema_incompatible` (for example a key column missing from
an old-key image under `REPLICA IDENTITY USING INDEX`, or a column named with the
reserved `_d2g` prefix) or `unsupported` (an operation the planner or sink
cannot apply). Units before it were applied.

1. Remove the cause in configuration or mapping (for example adjust
   `key_constraint`, or rename the offending column), approve the new
   mapping if required.
2. `resume`. The same unit is planned again; if the cause remains, the
   pipeline pauses again.

While paused, the source retains changes (WAL); watch slot retention.

## Error kinds that indicate configuration problems

`config`, `mapping_not_approved`, `security`, `secret`,
`restricted_provider_unavailable`, `unsupported`, `feature_not_compiled` and
`preflight` at startup are configuration or environment problems. The message
lists every finding; `db2graph-sync validate` and `db2graph-sync preflight`
reproduce them without starting the pipeline.
