# Upgrades

No release of db2graph-sync has been published yet, so no upgrade between
releases has been performed or tested. This page states what is versioned
today and the procedure to follow.

## What is versioned

| Item | Current version | Behaviour on mismatch |
|---|---|---|
| Local state format | `1` (stored in the state store's meta table) | Opening a store with a different format fails with `state_io` ("unsupported state format"). **No state migrations exist yet.** |
| Entity identity domain | `db2graph-sync/entity/v1` | Part of every entity id. A change would change every id. |
| Content digest domain | `db2graph-sync/content/v1` | Part of receipts, entity digests, point ids. A change would make replays of already-applied units digest differently. |
| Version encoding | 32 hex characters | Compared as strings in the target. |
| Management API | `/v1` routes, `api/openapi.json` | |
| Mapping plan digest | derived from the plan's serialized form | A change in how plans are generated or serialized changes the digest and requires approval outside development. |

Because receipts are compared by digest, a new version that plans the same
unit differently would report `consistency_incident` when replaying units that
were applied by the old version. Draining the journal before upgrading avoids
replays (see below).

## Procedure

1. **Read the release notes** of the target version for state format, identity
   or digest changes. If any of those change, the release must state how to
   migrate; do not proceed otherwise.
2. **Drain and stop**: `SyncHandle::shutdown(drain_timeout)` or stop
   `db2graph-sync run` (SIGINT/SIGTERM; `--drain-secs`, default 30). Confirm
   `ShutdownReport.drained = true` / `remaining_journal_units = 0`; otherwise
   restart the old version and drain again.
3. **Back up** `state.dir` (engine stopped) and the audit file; make sure the
   keys are available. See [Backup and restore](backup-restore.md).
4. **Install** the new library or binary. The minimum supported Rust version is
   1.90 (edition 2024).
5. **Validate offline**: `db2graph-sync validate -c <config>`.
6. **Check prerequisites**: `db2graph-sync preflight -c <config>`.
7. **Check the mapping**: `db2graph-sync plan -c <config>`. If the digest
   changed, review the plan and update `mapping.approved_digest`.
8. **Bootstrap** if the release adds graph constraints or indexes:
   `db2graph-sync bootstrap -c <config>` (idempotent).
9. **Start** and confirm `phase = streaming`, `ready = true`, no `failure`.
10. **Reconcile**: `db2graph-sync reconcile -c <config>`.

## Downgrades

Not supported or tested. With identical state format, identity and digest
versions a downgrade may work, but nothing guarantees it.

## Platform note

Development and testing used Windows 11 x86_64 (MSVC) with Rust 1.99.0. Linux
and macOS are intended targets with a CI matrix defined but not yet executed.
