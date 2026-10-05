# 0006 — Versions as fixed-width strings scoped to a source epoch

Status: Accepted
Date: 2026-10-05

## Context

Version guards must compare reliably inside every target (Neo4j, and future
Memgraph/Neptune) and must never compare positions from unrelated sources or
opaque resume tokens.

## Decision

`Version { epoch: u32, position: u64, ordinal: u32 }` encoded as 32 lowercase
hex characters (`%08x%016x%08x`) so lexicographic string comparison equals
numeric comparison in every target. `position` is a comparable source position
(PostgreSQL commit end LSN); `ordinal` is the event ordinal + 1 within the
unit (0 for snapshot rows, which therefore lose to any streamed change after
the boundary). `epoch` is incremented only by authorized recovery after the
source history changes. `SourcePosition::partial_cmp_scoped` returns `None`
for different scopes. Connectors without a totally ordered position must
define per-partition scopes and may not claim more ordering than they have.

## Consequences

Snapshot/stream overlap converges by construction; restores to a different
source history require an explicit epoch change (`recover
--accept-source-change`, which also forces a new snapshot). Entity identity
never includes the version. Entity identity does include the source id, so
after a change to a different source id the new epoch's rows are new
identities rather than newer versions of the old ones; removing the previous
source's entities is an operator action (re-bootstrap/cleanup is planned).
