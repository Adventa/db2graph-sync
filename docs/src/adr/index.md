# Architecture decision records

Consequential decisions are recorded here. Each record states the context,
the decision, the consequences, and what evidence supported it at the time.
A superseded record is kept and marked as such.

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-one-engine-pragmatic-workspace.md) | One shared engine and a pragmatic two-crate workspace | Accepted |
| [0002](0002-encrypted-state-and-backends.md) | Encrypted durable state over a pluggable backend (redb default) | Accepted |
| [0003](0003-postgres-capture-supabase-etl.md) | PostgreSQL capture: own pgoutput client; Supabase ETL not adopted | Accepted |
| [0004](0004-graph-effects-in-target-transaction.md) | Receipts, tombstones, pending relationships and vector outbox live in the graph transaction | Accepted |
| [0005](0005-tls-and-crypto-providers.md) | Explicit rustls `ring` provider; restricted profile fails closed | Accepted |
| [0006](0006-versions-and-ordering-scopes.md) | Versions as fixed-width strings scoped to a source epoch | Accepted |
| [0007](0007-neo4j-driver.md) | Neo4j access through `neo4rs` 0.8 with a pipeline lock | Accepted |
| [0008](0008-management-api.md) | Management API as a router the host mounts; hashed tokens | Accepted |

New ADRs use the next number and the template below.

```markdown
# NNNN — Title
Status: Proposed | Accepted | Superseded by NNNN
Date: YYYY-MM-DD
## Context
## Decision
## Consequences
## Evidence
```
