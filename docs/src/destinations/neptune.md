# Amazon Neptune

**Status: Planned. Not implemented in this release.**

`graph.type = "neptune"` is accepted by the configuration parser and rejected
by validation with error kind `unsupported` ("this graph target is planned but
not implemented in this release").

## Requirements before any guarantee is offered

A Neptune sink may declare `effectively_once_effects` only after it is shown,
for Neptune specifically, that:

- **Isolation and locking**: the guard-read-then-write sequence for an entity,
  receipt and pipeline marker is protected against concurrent writers of the
  same pipeline under Neptune's transaction semantics, which must be
  established and tested rather than assumed from Neo4j.
- **Atomic batches**: all effects of a batch, including receipts, tombstones,
  pending relationships, vector outbox entries and the pipeline marker, commit
  or roll back together within Neptune's transaction limits; batch sizes must
  respect those limits without splitting a source unit.
- **Fencing**: a stale fence token is detected inside the same transaction and
  the batch fails with `fenced_out`.
- **Version comparison**: fixed-width version strings compare correctly.
- **Retry classification**: concurrent-modification and throttling errors are
  classified as retryable, and other errors are not retried blindly.

The proof is the conformance suite (`sink::conformance::run_all`) run against a
real Neptune cluster, plus concurrency tests specific to Neptune.

Until then, Neptune can only be used through a custom sink (`graph.type =
"custom"`), which is reported `qualified: false`. See
[Custom destinations](custom.md).
