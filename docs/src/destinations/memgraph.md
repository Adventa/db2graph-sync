# Memgraph

**Status: Planned. Not implemented in this release.**

`graph.type = "memgraph"` is accepted by the configuration parser and rejected
by validation with error kind `unsupported` ("this graph target is planned but
not implemented in this release").

## Requirements before any guarantee is offered

The engine's graph guarantees depend on target-specific behaviour, not on
query-language compatibility. A Memgraph sink may declare
`effectively_once_effects` only after it is shown, for Memgraph specifically,
that:

- **Isolation and locking**: a guard read followed by a write inside one
  transaction cannot be invalidated by a concurrent writer of the same pipeline
  (the role of the pipeline-node write lock in the Neo4j sink). The behaviour of
  Memgraph's transaction isolation and conflict handling must be established
  and tested, not assumed from Neo4j.
- **Atomic batches**: receipts, entity writes, tombstones, watermarks, pending
  relationships, join state, vector outbox and the pipeline marker commit or
  roll back together.
- **Fencing**: a stale fence token is detected under the same lock and the
  batch fails with `fenced_out`.
- **Version comparison**: fixed-width version strings compare correctly with
  the target's string comparison.
- **Durability**: committed transactions survive a server restart under the
  documented storage configuration.

The proof is the conformance suite (`sink::conformance::run_all`) run against a
real Memgraph server, plus concurrency tests specific to its isolation model.

Until then, Memgraph can only be used through a custom sink (`graph.type =
"custom"`), which is reported `qualified: false`. See
[Custom destinations](custom.md).
