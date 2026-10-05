# Backup and restore

A pipeline's state is spread over four places. Each needs its own decision.

| Item | Contents | Needed to |
|---|---|---|
| State encryption keys (in your secret store) | 32-byte keys referenced by `state.keys` | Read the state directory and verify the audit file. **Without the keys the state is unreadable** (`state_tampered`). |
| `state.dir` | Encrypted journal, checkpoint, acknowledged position, snapshot marker, source identity, schema history, audit spool | Continue the pipeline without authorized recovery. |
| Graph target | Projected data plus receipts, tombstones, pending relationships, vector outbox, pipeline marker | Serve queries; it is the record of applied progress. |
| Audit file (`audit.file`) and anchor file (`state.anchor_file`) | HMAC-chained audit log; rollback evidence | Audit trail; rollback detection. |

Configuration files and the secret store should be backed up with your normal
configuration management. Secrets must not be stored in configuration files
outside the development profile.

## Backing up `state.dir`

- Copy the directory only while the engine is **stopped** (after
  `shutdown` / `db2graph-sync run` has exited). The engine holds an exclusive
  lock while running, and a copy taken during a write is not guaranteed to be
  consistent.
- Store the copy separately from the keys.
- Back up the anchor file, if used, on a different schedule and medium from
  `state.dir`; its purpose is to detect a restored `state.dir`.

What a state backup is good for: after loss of the state volume, restoring a
copy taken **after the initial snapshot completed** preserves the snapshot
marker and the source identity, so the restart after authorized recovery
streams from the slot instead of taking a new snapshot. Without such a copy a
new snapshot is taken under a new generation (see
[Durability](../architecture/durability.md#node-loss)); it does not collide
with the original snapshot's receipts, but it re-reads every mapped relation
and entities it does not see are not swept.

## Restoring `state.dir`

Restoring an older copy is **detected as a rollback**, by design:

1. Restore the directory (with the same keys configured) and start. The graph
   marker records a newer state epoch, so the engine stops with
   `state_rollback` (and, with an anchor file, the anchor also reports it).
2. Run [authorized recovery](recovery.md):
   `db2graph-sync recover -c <config> --accept-state-reset --reason "<what happened>"`.
3. Start the engine. Journaled units in the restored copy that were already
   applied replay as receipt duplicates (`counters.duplicates`).
4. Run `db2graph-sync reconcile`.

Data exposure: PostgreSQL resumes from the later of the restored position and
the slot's confirmed position. Changes that were acknowledged to the source but
not yet applied to the graph when the original state was lost are not
redelivered. The exposure is the journal backlog at the time of loss.

If the restored copy is from a **different store** (for example a fresh
directory), an existing anchor file names the old store id and keeps failing
with `state_rollback` after recovery. Move the old anchor file aside; it is
rewritten at the next checkpoint.

## Backing up the graph target

Use the target's own backup tooling (for Neo4j, its backup and dump
procedures). A graph backup includes the projection and the pipeline's
metadata (`_D2G_*` nodes, `_d2g_*` properties).

## Restoring the graph target

**Restoring the graph to an earlier point in time is not detected by the
engine.** The pipeline marker then holds an older state epoch than local state,
which is not treated as an error, and the local checkpoint is already past the
units whose effects (and receipts) the restore removed. Those units are not
replayed. The projection silently misses every change applied between the
graph backup and the restore.

After a graph restore, choose one of:

- **Rebuild the projection**: stop the engine, remove this pipeline's data from
  the graph (or use a new `pipeline.id`), use a new `state.dir` and a new
  replication slot, run `bootstrap`, and start so that a full initial snapshot
  is taken.
- **Measure the gap**: run `db2graph-sync reconcile` and decide whether the
  differences are acceptable. Reconciliation samples rows; it is not a repair
  tool.

There is no command that re-synchronizes a restored graph in place;
re-snapshotting into a new generation with sweep and cutover is planned.

## Audit file

Back up the audit file like any append-only log. Verify it with
`db2graph-sync audit-verify -c <config> [--file <path>]`. Verification needs
every key id that signed lines in the file, so keep retired keys available for
verification. See [Key rotation](key-rotation.md).
