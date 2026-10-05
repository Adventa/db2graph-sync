# Durability and crash recovery

This page describes what the engine stores, where, and what happens when the
process stops at each point of the pipeline.

## Two places for durable state

Durable state is split deliberately between the local state store and the graph
target ([ADR 0004](../adr/0004-graph-effects-in-target-transaction.md)).
Anything that must change atomically with graph effects lives in the graph
transaction; anything needed before the graph is reached lives locally.

| Local encrypted state store (`state.dir`) | Graph target (same transaction as the effects) |
|---|---|
| Journal of captured, policy-filtered units not yet applied | Receipts: one per applied unit (`unit_id` → content digest) |
| Checkpoint `{applied_seq, last_seq}` | Entity version, content digest and metadata properties |
| Acknowledged source position (`ack_position`) | Tombstones for deleted entities and join rows |
| Snapshot progress and `snapshot_done` | Relation truncate watermarks |
| Source identity (`source_id`, timeline) | Pending relationship records awaiting an endpoint |
| Schema history | Join-row state |
| Audit spool (records not yet forwarded) | Vector outbox tasks |
| Store id, format version (1), **state epoch**, per-key seal counters, recovery note | Pipeline marker: fence token, highest state epoch seen, last unit, last version, active generation |

## Encrypted journal and state store

`StateStore` (`crates/db2graph-sync/src/state/mod.rs`) seals every value before
it reaches the backend. The backend only stores ciphertext.

- Default crypto provider: RustCrypto AES-256-GCM, SHA-256 and HMAC-SHA-256,
  with known-answer self-tests at start. Not FIPS 140-3 validated.
- Envelope: `D2G1` | key id | 96-bit random nonce | ciphertext. The
  additional authenticated data binds each record to
  `<store_id>/<table>/<key>`, so a valid record moved to another key or another
  store fails authentication (`state_tampered`).
- Each key has a seal counter. When it would exceed 2^31 seals, writes fail
  with a `security` error that asks for key rotation
  (`db2graph-sync rotate-keys`, see [Key rotation](../operations/key-rotation.md)).
- A wrong key, modified bytes or a substituted record are reported as
  `state_tampered`.

The default backend is redb 4.x in `<state.dir>/state.redb`: copy-on-write
B-trees, fsync on commit, one atomic commit per write batch. Exclusive
ownership is enforced with an OS file lock on `<state.dir>/LOCK` plus redb's own
lock; a second engine on the same directory fails with `ownership_conflict`.
Custom backends implement `StateBackend` (atomic durable batch commit, ordered
ranges) and declare `durable()` and `exclusive()`; non-durable backends are
refused outside development.

The state format version is 1. Opening a store with another format fails with
`state_io`.

## Checkpoints

The checkpoint records the highest journal sequence whose graph effects have
committed (`applied_seq`) and the highest sequence assigned (`last_seq`).
Advancing it deletes the applied journal entries in the same commit and
verifies contiguity: a missing sequence is an error and nothing is recorded.
The unapplied backlog is `last_seq - applied_seq`.

## State epochs and rollback detection

Every state commit that changes progress increases the **state epoch**. Each
graph batch carries the local epoch, and the sink stores the maximum seen in
the pipeline marker in the same transaction. At startup, **before** acquiring
the fence, the engine reads the pipeline marker and checks the rollback
evidence:

- if the marker's `state_epoch` is greater than the local epoch, the local
  state is older than progress already committed to the graph (restored from a
  backup, replaced, or lost). The engine stops with `state_rollback`.
- if `state.anchor_file` is configured, the engine verifies it. The anchor file
  holds `<store_id>:<epoch>:<kid>:<mac>`: the HMAC covers the store id, the
  epoch and the key id `kid` of the state key that produced it, so the anchor
  still verifies after `rotate-keys` as long as the old key stays configured.
  It is rewritten atomically (temporary file, fsync, rename) after every
  checkpoint advance. An older anchored epoch than the local one is accepted;
  a newer one, or an anchor from a different store, is `state_rollback`; a bad
  HMAC or malformed file is `state_tampered`. A missing anchor file is
  accepted silently. Place the anchor file on storage that is not backed up
  and restored together with `state.dir`.

Because these checks run before the fence is taken, an instance with stale or
empty state fails with `state_rollback` without fencing out a healthy owner
(test `stale_instance_cannot_fence_out_healthy_owner`). After acquiring the
fence the engine re-checks the marker's epoch, in case another owner committed
in between.

The only way past `state_rollback` is the authorized recovery command
`db2graph-sync recover --accept-state-reset --reason "<text>"`, which moves the
local epoch past the graph's evidence and records the reason. See
[Recovery](../operations/recovery.md).

## Crash windows

Crash recovery is exercised by tests in `crates/db2graph-sync/tests/engine.rs`.
In these tests a "crash" is emulated: a fault injector returns a terminal error
at a named boundary, the engine stops, and a new engine is started in the same
process on the same state directory and the same in-memory graph. Process
kill, operating-system crash and power loss are **not** tested; recovery from
those relies on redb's fsync-on-commit durability and on the graph target's
transaction atomicity.

| Window | State after the crash | What happens on restart | Covered by |
|---|---|---|---|
| Before journal commit (`journal.before_commit`) | Unit not journaled, not acknowledged. | The source redelivers from the last acknowledged position; the unit is journaled and applied once. | `crash_before_journal_commit_is_redelivered_by_source` |
| After journal commit, before source acknowledgement (`journal.after_commit`) | Unit journaled; the stored `ack_position` already includes it; the source has not been told. | The journal is replayed and the unit applied. Streaming resumes from the stored position (PostgreSQL starts at the later of that position and the slot's confirmed position). If the source sends the unit again, it is journaled again and its second application is a receipt duplicate. | `crash_after_journal_commit_before_source_ack_replays` |
| Before graph commit (`graph.before_commit`) | Journal intact, checkpoint not advanced, no graph effects. | The unit is replayed and applied once; no duplicate is reported. | `crash_before_graph_commit_exposes_nothing_then_applies_once` |
| During the graph transaction | The target rolls the transaction back; nothing of the batch is visible. | As above. | Atomic rollback on receipt mismatch is checked by the conformance suite (`receipt_mismatch_is_incident_and_atomic`); a mid-transaction process kill is not tested. |
| After graph commit, before checkpoint (`graph.after_commit`) | Graph has the effects and the receipt; checkpoint not advanced. | The unit is replayed; the sink finds the receipt with the same digest and reports `duplicate`; nothing changes. | `crash_after_graph_commit_before_checkpoint_replays_as_duplicate` |
| During checkpoint (`checkpoint.before_commit`) | The checkpoint commit is atomic: either the old or the new checkpoint is stored. | Same as the previous row when the old checkpoint survived. | `crash_at_checkpoint_commit_recovers` |
| Vector: after delivery, before task completion (`vector.after_delivery`) | Points are in the vector store; the outbox task is still in the graph. | The task is processed again: the entity is re-rendered and re-embedded, points with the same deterministic ids are overwritten, older-version points deleted, then the task completes. The cost is a repeated embedding call. | No dedicated test (the fault point exists). |
| Orderly shutdown (`SyncHandle::shutdown`, `db2graph-sync run` on SIGINT/SIGTERM) | Capture stops; the applier drains until `drain_timeout`. | Units not applied by the deadline stay in the journal and are applied on the next start. `ShutdownReport` returns `drained` and `remaining_journal_units`. | `lifecycle_snapshot_then_stream_then_shutdown`, `host_safety_no_global_logger_no_panic_bounded_shutdown` |
| Handle dropped without `shutdown` | Cancellation is requested; nothing is drained or flushed. | Equivalent to a crash after the last completed commit. | `dropping_handle_without_shutdown_does_not_hang_or_panic` |

Related tests: `transient_failures_are_retried_not_terminal` (retryable graph and
source errors are retried, not terminal) and
`stale_and_duplicate_source_replay_has_no_effect`.

## Storage exhaustion

- **Journal growth** is bounded by unit count: capture waits while the journal
  holds `state.max_journal_units` units (default 50 000). The source then
  retains the changes instead (PostgreSQL: WAL held by the slot). There is no
  byte limit; one unit can hold up to `source.max_transaction_changes` changes.
- **Disk full on the state volume**: redb returns an I/O error, which becomes
  `state_io` and stops the engine. The commit that failed is not applied. This
  path has not been separately fault-tested on a full disk.
- **Audit spool full** (`audit.spool_max_records`, default 100 000, when audit
  sinks are configured but not accepting records): `audit_unavailable`, the
  engine stops. At the next start the engine first drains the spool to its
  sinks before writing its start record, so once the destination accepts
  records again the restart succeeds; if it is still down and the spool is
  still full, the start fails closed with `audit_unavailable`. Without audit
  sinks the spool is a bounded ring that drops the oldest records.
- **Graph target out of space**: surfaced as a `graph` error from the target.

## Node loss

The engine assumes `state.dir` is on durable storage that survives process and
operating-system restarts. If the volume is lost or replaced:

1. A new start finds an empty store. Once the pipeline has committed at least
   one batch, the graph marker's epoch is higher than the new store's, and the
   engine stops with `state_rollback`.
2. An operator runs `db2graph-sync recover --accept-state-reset --reason ...`.
3. The engine can start again. Consequences the operator must account for:
   - Units that were journaled and acknowledged to the source but **not yet
     applied** to the graph at the time of loss cannot be recovered: the
     source has already released them. The size of this window is the journal
     backlog (`status.backlog.journal_units`) at the time of loss. Run
     `db2graph-sync reconcile` afterwards.
   - The local snapshot marker is gone, so a new snapshot is taken unless
     `source.snapshot.enabled = false`. Its generation is the graph marker's
     `active_generation + 1` (generation 2 after an initial snapshot), so its
     chunk unit ids (`<source>:snapshot:<generation>:<relation>:<n>`) do not
     collide with the original snapshot's receipts; the new generation is
     activated when it completes. Test: `state_loss_is_detected_as_rollback`
     (in-process, scripted source and memory sink; not run against
     PostgreSQL). Entities that the new snapshot does not see are not swept
     (planned).
   - The stored source identity is gone and is re-recorded on the next start.
   - If `state.anchor_file` is configured, the old anchor names the old store
     id and keeps reporting `state_rollback`; move it aside as part of the
     recovery.

Restoring an older copy of `state.dir` from backup is detected the same way.
Journaled units in the restored copy that were already applied replay as
receipt duplicates. See [Backup and restore](../operations/backup-restore.md).

## Source files

- `crates/db2graph-sync/src/state/mod.rs`, `crates/db2graph-sync/src/state/backend.rs`
- `crates/db2graph-sync/src/crypto.rs`
- `crates/db2graph-sync/src/engine.rs` — epoch/anchor check before and after fence acquisition, `apply_available`
- `crates/db2graph-sync/tests/engine.rs`
