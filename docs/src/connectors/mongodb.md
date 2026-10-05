# MongoDB and MongoDB Atlas connector

**Status: Planned. Not implemented in this release.**

`source.type = "mongodb"` is accepted by the configuration parser so that the
error is precise, and validation rejects it with error kind `unsupported`.
`db2graph-sync validate` exits with code 8. Nothing on this page describes
working behaviour; it lists the requirements an implementation must meet.

## Design requirements

A MongoDB connector must satisfy the [connector rules](index.md) and in
particular:

- **Change streams only.** Capture with change streams; never poll
  collections to detect changes.
- **Resume tokens.** Persist resume tokens through the engine's journal and
  acknowledgement contract. Resume tokens are opaque positions: they must not
  be compared as ordered values outside the scope MongoDB defines. If a resume
  token is no longer in the oplog window, stop with `resume_history_lost`.
- **Event types.** Handle `insert`, `update` (deltas: updated fields and
  removed fields), `replace`, `delete`, `drop`, `rename`, `dropDatabase` and
  `invalidate`. Removed fields map to `FieldState::Removed`; fields absent from
  a delta are `Missing`, never `null`.
- **Images.** Use pre- and post-images where the deployment has them enabled,
  and declare the before-image capability accordingly.
- **`updateLookup` is not an exact after-image.** A full document fetched with
  `fullDocument: "updateLookup"` reflects the document at lookup time, which
  can be later than the change event. The connector must not present it as the
  exact historical after-image of that event.
- **Transactions.** Do not claim reconstruction of multi-document transactions
  unless it is implemented and verified; until then declare
  `transaction_boundaries: false`, which lowers the effective guarantee to
  `at_least_once`.
- **Sharded clusters.** Qualify ordering and resume behaviour on sharded
  clusters separately from replica sets; declare per-partition ordering if
  total order is not available.
- **References.** Document-to-document references (there are no declared
  foreign keys) need an explicit, approved mapping; reference handling for
  document sources is planned.

## Until it exists

A MongoDB source can be integrated today only through a custom connector
(`source.type = "custom"` with `StartOptions::source`). Such a connector is not
qualified by this project.
