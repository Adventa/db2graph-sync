# Microsoft SQL Server connector

**Status: Planned. Not implemented in this release.**

`source.type = "sqlserver"` is accepted by the configuration parser so that the
error is precise, and validation rejects it with error kind `unsupported`.
`db2graph-sync validate` exits with code 8. Nothing on this page describes
working behaviour; it lists the requirements an implementation must meet.

## Design requirements

A SQL Server connector must satisfy the [connector rules](index.md) and in
particular:

- **Native CDC only.** Read SQL Server's change data capture change tables
  through the documented CDC functions. No parsing of the transaction log by
  undocumented means, and no polling of business tables.
- **LSN intervals.** Query changes by LSN interval and treat LSNs as positions
  within one database's CDC scope.
- **Update pairs.** Combine the before and after rows of an update into one
  change; never treat them as a delete followed by an insert.
- **Transaction boundaries.** Group changes by commit LSN so that a source
  transaction is one unit and is never split or reordered.
- **Capture instances.** Handle the existence of multiple capture instances per
  table (used for schema changes), switch between them deliberately, and
  re-discover the schema when the capture instance changes.
- **Cleanup.** The CDC cleanup job removes old change rows. If the needed LSN
  range has been cleaned up, stop with `resume_history_lost` instead of
  skipping changes.
- **Snapshot.** Define how the initial load is aligned with a CDC LSN, and
  declare `transaction_consistent` only if it is.

## Until it exists

A SQL Server source can be integrated today only through a custom connector
(`source.type = "custom"` with `StartOptions::source`). Such a connector is not
qualified by this project.
