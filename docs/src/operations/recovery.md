# Authorized recovery

Some failures are not resolved by restarting, because continuing would
silently accept lost or rolled-back progress. They require an explicit,
audited operator action.

## `recover --accept-state-reset`

```text
db2graph-sync recover -c <config> --accept-state-reset --reason "<description, at least 8 characters>"
```

Library equivalent: `db2graph_sync::admin::accept_state_reset(&config, StartOptions::default(), reason)`.

**When**: the engine stopped with `state_rollback` because the local state
directory was lost, replaced or restored from an older copy, and the operator
accepts that.

**Preconditions**: the engine is stopped (the command opens the state store and
needs its exclusive lock); the configured keys can open the store; the graph
target is reachable.

**What it does**, in order:

1. reads the pipeline marker from the graph target and takes its recorded
   state epoch as evidence;
2. opens the local state store;
3. in one commit, sets the local state epoch to
   `max(local epoch, graph epoch) + 1` and stores a `recovery` note with the
   reason and the accepted epoch;
4. appends a `recovery` / `accept_state_reset` audit record (reason, graph
   epoch, new epoch) to the spool; it is forwarded to the audit sinks on the
   next start;
5. prints the new epoch.

The CLI requires exactly one of `--accept-state-reset` or
`--accept-source-change`, and a reason of at least 8 characters (otherwise
`invalid_request`, exit 64).

**What it does not do**:

- it does not recover changes that were acknowledged to the source but not yet
  applied to the graph before the state was lost;
- it does not restore the snapshot marker; with an empty state directory a new
  snapshot is taken on the next start, under generation
  `graph marker active_generation + 1`, so its unit ids do not collide with
  the earlier snapshot's receipts (see
  [Durability](../architecture/durability.md#node-loss));
- it does not rewrite the anchor file; if the store id changed (new, empty
  state directory), the old anchor still reports `state_rollback`: move the
  old anchor file aside;
- it does not clear the stored source identity or change the source epoch
  (that is `--accept-source-change`, below);
- it does not touch the graph.

**Afterwards**: start the engine, confirm it reaches `streaming`, and run
`db2graph-sync reconcile`. Test: `state_loss_is_detected_as_rollback` in
`crates/db2graph-sync/tests/engine.rs` (state directory removed, rollback
detected, recovery accepted, engine starts and re-snapshots under
generation 2).

## `recover --accept-source-change`

```text
db2graph-sync recover -c <config> --accept-source-change --reason "<description, at least 8 characters>"
```

Library equivalent: `db2graph_sync::admin::accept_source_change(&config, StartOptions::default(), reason)`.

**When**: the engine stopped with `source_identity_changed` because the source
was restored onto, or failed over to, a system with a different identity
(PostgreSQL: a different system identifier), and the operator accepts the new
source history.

**Preconditions**: the engine is stopped (the command opens the state store and
needs its exclusive lock); the configured keys can open the store. The graph
target is not contacted.

**What it does**, in one local state change:

1. increments the stored `source_epoch` (0 → 1 the first time);
2. clears the stored source identity, the acknowledged source position, the
   snapshot progress and the snapshot-completion marker;
3. appends a `recovery` / `accept_source_change` audit record (reason,
   previous identity, new source epoch) to the spool; it is forwarded to the
   audit sinks on the next start;
4. prints the new source epoch.

On the next start the engine records the new source identity and takes a new
snapshot (under generation `graph marker active_generation + 1`). Every
version carries the source epoch as its most significant component, so
versions from the new epoch supersede versions written under older epochs.

**What it does not do**:

- Entity ids include the source id (see
  [Identity](../architecture/identity-relationships.md#entity-identity)). After
  a change to a **different** source id, the new snapshot's rows are new
  identities; the entities projected from the previous source are not
  superseded or deleted and remain in the graph until an operator removes
  them. A re-bootstrap/cleanup of the previous source's entities is planned,
  not implemented.
- It does not recover changes the old source had that the new one lacks.
- It does not touch the graph.

**Afterwards**: start the engine, confirm the snapshot completes and the phase
reaches `streaming`, remove or account for the previous source's entities, and
run `db2graph-sync reconcile`. Test:
`source_identity_change_requires_and_accepts_authorized_recovery` in
`crates/db2graph-sync/tests/engine.rs` (identity change detected, recovery
accepted with source epoch 1, engine starts and applies new-epoch changes).

The management API has a `recover` token scope, but no route uses it; recovery
is available only through the CLI and the library.

## Other operator actions

| Action | Command | Engine state |
|---|---|---|
| Approve a mapping digest | `db2graph-sync approve-mapping --url U --token-file F --digest D`, `POST /v1/mapping/approve`, `SyncHandle::approve_mapping` | running |
| Pause / resume | `db2graph-sync pause` / `resume` (remote), `POST /v1/pause` / `/v1/resume`, `SyncHandle::pause` / `resume` | running |
| Re-encrypt state under the active key | `db2graph-sync rotate-keys -c <config>` | stopped |
| Reconcile source and graph | `db2graph-sync reconcile -c <config> [--sample N]` | either |
| Export evidence | `db2graph-sync evidence -c <config> --out F [--sample N]` | stopped for the state section |
| Verify the audit chain | `db2graph-sync audit-verify -c <config> [--file F]` | either |
| Create graph constraints and the vector collection | `db2graph-sync bootstrap -c <config>` | either |

## Situations without a recovery command

| Situation | Path |
|---|---|
| `resume_history_lost` | Build a new projection; see [Incidents](incidents.md#resume_history_lost-exit-3). |
| Graph target restored to an earlier point | Not detected; rebuild or reconcile; see [Backup and restore](backup-restore.md#restoring-the-graph-target). |
| A unit that cannot be applied | Remove the cause and resume; there is no skip. |

A new snapshot after authorized recovery is loaded into a new generation, but
sweeping entities that the new generation did not see, and cutover, are
planned, not implemented.
