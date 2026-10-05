# MySQL connector

**Status: Planned. Not implemented in this release.**

`source.type = "mysql"` is accepted by the configuration parser so that the
error is precise, and validation rejects it with error kind `unsupported`
("this connector is planned but not implemented in this release").
`db2graph-sync validate` exits with code 8. Nothing on this page describes
working behaviour; it lists the requirements an implementation must meet before
it can be offered.

## Design requirements

A MySQL connector must satisfy the [connector rules](index.md) (native change
capture only, acknowledgement only after journaling, honest capability
declaration) and in particular:

- **Row-based binary log only.** Capture from the binary log with
  `binlog_format = ROW`. Statement-based or mixed logging must fail preflight;
  business tables must not be polled.
- **Table map events.** Decode row events using the preceding table map
  events, and detect when a table map disagrees with the discovered schema
  (the equivalent of a schema signal).
- **Positions.** Use GTID sets where available and binary log file and
  position otherwise. Declare which one is in use; positions from different
  servers or GTID domains must not be compared.
- **Log rotation and retention.** Follow rotation across files, and stop with
  `resume_history_lost` if the needed logs were purged instead of skipping
  changes.
- **Transaction boundaries.** Assemble units from transaction boundaries in the
  log, so a source transaction is never split or reordered.
- **Schema changes.** Detect DDL and re-discover the schema; follow the same
  pause-for-approval behaviour as other connectors.
- **Image configuration.** Check `binlog_row_image`; with `MINIMAL`, unchanged
  columns must be reported as missing or unavailable, never as `NULL`, and the
  connector must declare its before-image capability accordingly.
- **Failover.** Detect a change of server identity or GTID history and stop
  with `source_identity_changed` or `resume_history_lost` as appropriate.
- **Non-transactional tables.** Changes to non-transactional storage engines
  (for example MyISAM) are not atomic at the source; the connector must
  document this and must not claim transaction boundaries it does not have.
- **Snapshot.** Define how the initial snapshot is made consistent with the
  log position, and declare `transaction_consistent` only if it is.

## Until it exists

A MySQL source can be integrated today only through a custom connector
(`source.type = "custom"` with `StartOptions::source`). Such a connector is not
qualified by this project.
