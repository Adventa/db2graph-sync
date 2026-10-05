# Neo4j

**Status: implemented; integration-tested against live servers (local Docker,
Windows; see [status page](../project/status.md)); not yet CI-qualified on all
platforms.** The sink implements the full sink contract, and its real-server
tests (`tests/pg_neo4j.rs`: `neo4j_sink_conformance`,
`postgres_to_neo4j_end_to_end`) passed on 2026-10-05 against Neo4j 5.26.31
Community (`neo4j:5-community`, single instance, plaintext `bolt://` under the
development exception). `bolt+s://`, clusters and Enterprise editions were not
tested. The sink's capability descriptor reports `qualified: false` until
qualification, which also requires a green CI run on Linux, macOS and
Windows, so the engine adds the status warning "graph sink `neo4j` is not
qualified by this project".

Driver: `neo4rs` 0.8.0 over Bolt ([ADR 0007](../adr/0007-neo4j-driver.md)).
Targeted server version: Neo4j 5 (preflight warns for other major versions).

## Connection

```toml
[graph]
type = "neo4j"
uri = "bolt+s://neo4j-1.internal:7687"
user = "d2g"
password = "env:D2G_NEO4J_PASSWORD"
# database = "neo4j"
# max_connections = 8               # default; connection pool size
# ca_file = "/etc/d2g/neo4j-ca.pem" # extra trust anchors for the server certificate
```

| URI scheme | Result |
|---|---|
| `bolt+s://` | Accepted: TLS with certificate verification. |
| `bolt://` | Plaintext; accepted only in the development profile with `engine.allow_plaintext = true` (`security` otherwise). |
| `bolt+ssc://` | Rejected (`security`): use `bolt+s://` with `ca_file` for self-signed certificates. |
| `neo4j://`, `neo4j+s://`, `neo4j+ssc://` | Rejected (`unsupported`): the driver has no client-side routing. |

Because there is no routing, the sink talks to one server. In a Neo4j cluster,
point it at a member that accepts writes for the database; changes of the
writer are not followed. Cluster deployments are not qualified.

`max_connections` sizes the driver's connection pool. Constraints are created
only by the explicit bootstrap step; there is no option to create them at
startup (the former `auto_create_constraints` key was removed and is now
rejected as an unknown field).

## Bootstrap

Startup never creates schema objects. Run `db2graph-sync bootstrap -c <config>`
(outside development the computed mapping digest must equal
`mapping.approved_digest`). It creates, idempotently (`IF NOT EXISTS`):

| Object | Definition |
|---|---|
| `d2g_entity_key` | `_D2G_Entity._d2g_key` unique |
| `d2g_tombstone_key` | `_D2G_Tombstone._d2g_key` unique |
| `d2g_receipt_key` | `_D2G_Receipt._d2g_key` unique |
| `d2g_watermark_key` | `_D2G_Watermark._d2g_key` unique |
| `d2g_pending_key` | `_D2G_PendingEdge._d2g_key` unique |
| `d2g_vector_task_key` | `_D2G_VectorTask._d2g_key` unique |
| `d2g_join_state_key` | `_D2G_JoinState._d2g_key` unique |
| `d2g_pipeline_id` | `_D2G_Pipeline.id` unique |
| `d2g_entity_relation` | index on `_D2G_Entity(_d2g_pipeline, _d2g_relation)` |
| `d2g_pending_to`, `d2g_pending_from`, `d2g_pending_owner` | indexes on `_D2G_PendingEdge` lookups |
| `d2g_vector_task` | index on `_D2G_VectorTask(_d2g_pipeline, ver)` |
| `d2g_join_relation` | index on `_D2G_JoinState(_d2g_pipeline, relation)` (join-table truncate) |
| `d2g_uk_<name>` | one index per unique-key lookup property `_d2g_uk_<name>` in the plan |

It then waits for indexes to come online (`db.awaitIndexes(300)`). Preflight
fails if any of the constraints, or (when a plan is available) any lookup
index, is missing.

## Transaction and locking pattern

Each `apply` call is one explicit Neo4j transaction:

1. **Lock and fence.**
   `MATCH (p:_D2G_Pipeline {id: $p}) SET p._d2g_lock = true REMOVE p._d2g_lock RETURN p.fence`.
   Setting and removing a property takes the node's write lock, which Neo4j
   holds until commit (the documented pattern for preventing lost updates). All
   writers of a pipeline serialize on this lock. The returned fence token must
   equal the caller's token, otherwise the transaction fails with `fenced_out`.
   A missing pipeline node also fails with `fenced_out`.
2. **Per unit, in order:** read the receipt (`_D2G_Receipt`); same digest →
   `duplicate`, no effect; different digest → `consistency_incident` and the
   whole transaction rolls back. (The receipt digest covers the captured unit
   content, not the planned operations, so a replay after an approved mapping
   or policy change is a duplicate.) Otherwise execute the unit's operations and
   create the receipt.
3. **Per operation:** guard reads (current version, digest, tombstone,
   watermark) and the write run inside the same locked transaction, so a guard
   cannot be invalidated by a concurrent writer of the pipeline.
4. **Marker:** update the pipeline marker's `state_epoch` (maximum),
   `last_unit` and `last_version`; commit.

`acquire_fence` runs `MERGE (p:_D2G_Pipeline {id})` and increments `fence` in
its own transaction. `set_active_generation` takes the same lock and fence
check. Vector task completion locks the task node, then reads and deletes it
only if its version is unchanged.

All values are query parameters. Labels and relationship types are
interpolated only after validation against `[A-Za-z_][A-Za-z0-9_]{0,63}` and are
backtick-quoted; an invalid identifier is refused with `security`.

### Statement shape

Writes are issued as individual statements per operation (guard read, write,
pending-relationship resolution) inside the one batch transaction. `UNWIND` is
used only for reading pending relationships that match an entity and for
batched entity lookups; writes are not batched with `UNWIND` in this release.
The number of round trips therefore grows with the number of operations in a
batch. `engine.max_batch_transactions` and `engine.max_batch_changes` are
throughput tuning only; they never change atomicity or ordering guarantees.

## Graph model

### Entity nodes

Labels: `_D2G_Entity` plus the mapped labels. Source fields that policy allows
are stored as properties with the column name. Metadata properties:

| Property | Meaning |
|---|---|
| `_d2g_key` | `<pipeline>\|<entity id>` (unique) |
| `_d2g_id` | entity id |
| `_d2g_pipeline` | owning pipeline |
| `_d2g_relation` | source relation |
| `_d2g_tenant` | tenant |
| `_d2g_ver` | current version (32 hex characters) |
| `_d2g_digest` | content digest of the last applied upsert |
| `_d2g_labels` | policy labels as JSON |
| `_d2g_props` | names of properties owned by the pipeline |
| `_d2g_nulls` | fields that are `NULL` at the source (property absent) |
| `_d2g_incomplete` | fields whose value is not known yet (for example unchanged TOAST on a new entity) |
| `_d2g_deleted` | `true` for a stripped placeholder |
| `_d2g_uk_<constraint>` | unique-key lookup digest, when referenced |

Source column names starting with `_d2g` are refused (`schema_incompatible`).

### Value representation

| Canonical value | Stored as |
|---|---|
| `Bool`, `Int`, `Float` | boolean, integer, float |
| `Bytes` | byte array |
| `Text`, `Uuid` | string |
| `Decimal`, `BigInt`, `Date`, `Time`, `Timestamp`, `TimestampTz`, `Json` | string (lossless text; not Neo4j temporal or numeric types) |
| `Array` of one scalar type | list |
| other `Array` (mixed, nested, or containing `NULL`) | JSON string |
| `Unknown` | its text |
| `Null` | property removed and the field listed in `_d2g_nulls` |

### Relationships

Owned relationships carry the relationship type from the plan, the join row's
allowed columns (for join tables), and metadata: `_d2g_owned = true`,
`_d2g_pipeline`, `_d2g_edge` (`fk|<from id>|<constraint>` or `join|<row id>`),
`_d2g_owner`, and the endpoint references `_d2g_from_prop`,
`_d2g_from_digest`, `_d2g_to_prop`, `_d2g_to_digest`.

### Metadata nodes

| Label | Purpose | Retention |
|---|---|---|
| `_D2G_Pipeline` | `id`, `fence`, `state_epoch`, `last_unit`, `last_version`, `active_generation` | one per pipeline |
| `_D2G_Receipt` | `unit`, `digest`, `version`, `applied_at` per applied unit (snapshot chunks included) | never pruned in this release |
| `_D2G_Tombstone` | delete version of an entity or join row | kept until the entity is re-created |
| `_D2G_Watermark` | truncate version of a relation or declared join table | kept |
| `_D2G_PendingEdge` | relationship waiting for an endpoint | removed when resolved or when its owner is deleted |
| `_D2G_VectorTask` | vector outbox entry | removed when completed |
| `_D2G_JoinState` | version and start reference of a join-table relationship | kept |

## Ownership model

The pipeline owns entity nodes it created, the properties listed in
`_d2g_props`, its mapped labels, relationships marked `_d2g_owned` with its
pipeline id, and its `_D2G_*` metadata nodes. Other applications may add their
own relationships to entity nodes; a source delete then strips the node
(owned properties, metadata lists, lookup properties and mapped labels are
removed, `_d2g_deleted = true`) instead of deleting it, and their relationships
remain. Any relationship not owned by this pipeline counts as foreign,
including relationships owned by another pipeline. Other applications must not
modify `_d2g_*` properties or `_D2G_*` nodes. See
[Identity and relationships](../architecture/identity-relationships.md#delete-semantics-and-ownership).

## Error handling

| Neo4j condition | Error kind |
|---|---|
| Transient errors, session expired, I/O and connection errors | `graph`, retried with backoff |
| Authentication failure, security errors | `security` |
| Other server errors | `graph`, not retried |
| Receipt digest mismatch, same version with different content | `consistency_incident` |
| Stale fence token or missing pipeline node | `fenced_out` |

## Source files

- `crates/db2graph-sync/src/sink/neo4j.rs`
- `crates/db2graph-sync/src/sink/conformance.rs`
- `crates/db2graph-sync/tests/pg_neo4j.rs` (passed against a local Docker Neo4j 5.26.31; see status above)
