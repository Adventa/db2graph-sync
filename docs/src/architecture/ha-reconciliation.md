# Ownership, HA and reconciliation

## Single active owner per pipeline

A pipeline (identified by `pipeline.id`) must have exactly one active engine
instance writing its graph state. Three independent mechanisms enforce this:

| Mechanism | Scope | What happens on conflict |
|---|---|---|
| State directory lock | One engine per `state.dir` (OS file lock on `<state.dir>/LOCK`, plus redb's lock) | A second `start` on the same directory fails with `ownership_conflict`. Test: `second_engine_on_same_state_dir_is_rejected`. |
| PostgreSQL slot exclusivity | One consumer per replication slot (enforced by PostgreSQL) | Source preflight fails while the slot is active ("slot is active (another consumer is attached)"). If the slot is active when the stream is opened (it became active after preflight, or the server still holds a previous connection of this engine until `wal_sender_timeout`), opening the stream fails with a **retryable** `ownership_conflict`: the engine retries with backoff, counts `counters.source_reconnects` and shows status warnings, and keeps retrying while a competing consumer holds the slot. |
| Graph fence token | One writer per pipeline in the graph target | Every start increments the fence token stored on the pipeline marker (Neo4j: `_D2G_Pipeline {id}`) and uses the new value. Every batch and generation activation write-locks the marker and compares tokens; an instance holding an older token fails with `fenced_out` and its batch has no effect. |

The fence is the mechanism that makes graph writes safe when the other two do
not apply (for example two instances with different state directories against
a custom source).

Test `split_brain_older_owner_is_fenced_out` starts instance A, then instance B
with a different state directory against the same pipeline and graph. A's next
batch fails with `fenced_out`; B applies the change; the graph contains it
once. In that test A had not committed a batch yet.

Fencing is "newest owner wins" among instances with valid state. Startup order
is: source preflight, source identity, schema and mapping, graph preflight,
**rollback check** (graph marker state epoch against the local epoch, then the
anchor file if configured), **fence acquisition**, then the epoch check again
(another owner may have committed in between). Consequently:

- a second instance with stale or empty state fails with `state_rollback`
  **without** fencing out the healthy owner, which keeps applying. Test:
  `stale_instance_cannot_fence_out_healthy_owner`;
- a second instance with valid state (local epoch not behind the marker) still
  takes over: it acquires a higher fence token and the first instance fails
  with `fenced_out` at its next batch.

With PostgreSQL, a healthy running owner holds the slot, so a second instance
normally fails the source preflight before reaching the fence.

## No leader election

There is no leader election, lease, heartbeat or automatic failover. HA
beyond "single owner plus fencing" is **planned**. Running a hot standby that
starts automatically is not supported; takeover is an operator procedure.

## Standby takeover procedure

1. **Stop or isolate the old instance.** Confirm the process has exited or the
   host is down. If this cannot be confirmed, continue: the fence will reject
   the old instance's writes once the new instance starts, but the old
   instance may still hold the replication slot.
2. **Give the standby the same state.** Attach the old instance's `state.dir`
   (for example by moving the block volume) and provide the same state keys.
   Do not run the standby on a copy while the original is still in use. If the
   state cannot be recovered, see
   [Node loss](durability.md#node-loss): an empty state directory leads to
   `state_rollback` and requires authorized recovery, with the data exposure
   described there.
3. **Release the replication slot.** Check `pg_replication_slots.active` for
   the slot. It becomes inactive when the old connection ends (PostgreSQL
   detects a dead peer after `wal_sender_timeout`); an administrator may also
   terminate the backend in `active_pid`.
4. **Start the standby** with the same configuration (`pipeline.id`, graph
   target, slot). It acquires a higher fence token. If the old instance is
   still alive, it fails with `fenced_out` at its next batch.
5. **Verify**: `status.fence_token` increased, `phase` is `streaming`,
   `failure` is empty; then run `db2graph-sync reconcile`.

## Reconciliation

`db2graph-sync reconcile -c <config> [--sample N]` (default sample 100), or
`admin::reconcile` in code, compares the source with the graph. It is an
explicit operator action, read-only for both systems, and never used to detect
changes.

For every mapped relation (join tables are not included):

1. **Counts**: the source row count (PostgreSQL: `SELECT count(*)`) and the
   number of non-deleted graph entities for the relation.
2. **Verified readback**: the first `N` rows in primary-key order are read from
   the source; for each, the expected entity id and a hash of its
   policy-allowed fields in graph representation are computed; the entity is
   read back from the graph and the same hash is computed over its stored
   properties.

Two kinds of hashes appear in evidence and must not be confused:

| Hash | Meaning |
|---|---|
| Expected destination hash | Receipt digest of a unit the engine **intended** to apply (over the unit id and the captured unit content, not the planned operations); stored in receipts and in `batch_applied` audit records. |
| Verified readback hash | Hash of properties **read back** from the graph now, compared with the current, policy-filtered source row. |

The report lists, per relation, `source_rows`, `graph_entities`, `sampled`,
`verified`, `missing_in_graph` and `mismatched`, plus the plan digest and policy
version. `consistent` is true only if every comparable count matched and every
sampled entity verified. The CLI exits with code 7 when the report is inconsistent.
Test: `reconciliation_verifies_readback_and_detects_drift`.

Interpretation notes:

- Rows changing while reconciliation runs produce transient differences;
  re-run to confirm.
- The sample is the first `N` rows in key order, not a random sample.
- With a `tenant_column` policy, the source table holds other tenants' rows
  while the graph does not, so the count is not comparable: such relations
  report `source_rows: null` and are judged by the sampled readback only.
- A policy change after projection (for example a newly denied field) shows as
  mismatches, because properties already projected are not removed
  retroactively.

## Evidence export

`db2graph-sync evidence -c <config> --out <file> [--sample N]` writes a JSON
document assembled from facts that can be checked at that moment. Sections that
cannot be collected are recorded with the reason instead of failing the
export:

- attribution and compiled features, generation time, pipeline id, profile;
- a digest of the configuration (literal secret values redacted) and any
  configuration findings;
- crypto provider name, validation status and self-test result;
- source and graph capabilities and preflight reports;
- the graph pipeline marker (fence token, state epoch, last unit and version,
  active generation);
- the mapping digest, approved digest, approval state and exclusions;
- local state: store id, epoch, checkpoint, journal units, audit spool size.
  The default state backend is locked by a running engine, so this section is
  reported as unavailable unless the engine is stopped;
- audit file HMAC chain verification, when `audit.file` is set;
- a reconciliation report, when `--sample` is greater than 0.

## Source files

- `crates/db2graph-sync/src/engine.rs` — startup order, rollback check, fence acquisition
- `crates/db2graph-sync/src/sink/neo4j.rs` — `lock_and_fence`, `acquire_fence`
- `crates/db2graph-sync/src/reconcile.rs`
- `crates/db2graph-sync/src/lib.rs` — `admin::reconcile`, `admin::evidence`
