# Snapshots and schema

## Initial snapshot

On first start (no `snapshot_done` marker in local state and
`source.snapshot.enabled = true`, the default), the engine loads the existing
rows of every mapped relation and join table before it starts streaming.

### Snapshot boundary and exported snapshot (PostgreSQL)

When the replication slot does not exist yet, the connector creates it with
`CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput EXPORT_SNAPSHOT`. The slot's
consistent point becomes the **snapshot boundary**. A regular connection then
opens `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` and imports the
exported snapshot with `SET TRANSACTION SNAPSHOT`. All snapshot reads happen
inside that transaction, so they observe exactly the database state at the
boundary. Streaming later starts at the boundary, so every change committed
after it is delivered by the slot. Such a snapshot is reported as
`transaction_consistent`.

Rows are read with keyset pagination in primary-key order
(`WHERE (pk...) > (last key...) ORDER BY pk... LIMIT chunk_rows`) using the
simple-query protocol. Values come back in the same text output format that
`pgoutput` uses, under forced session settings, so snapshot and stream decode
to identical values. Literal quoting in these queries requires
`standard_conforming_strings = on`, which preflight checks.

Each chunk becomes one unit:

- `unit_id = <source_id>:snapshot:<generation>:<relation>:<chunk number>`;
- every row is a `read` change with version `(epoch, boundary LSN, 0)`;
- the acknowledgement position is the boundary;
- the journal commit also stores the snapshot progress (completed relations,
  current relation and last key, row counts per relation).

Relations without a primary key are skipped by the snapshot, including
relations mapped with a `key_constraint` override. Such relations receive only
streamed changes.

### Interrupted snapshot: convergent resume

If the engine stops during the snapshot, the next start resumes from the stored
progress. The slot already exists, so no exported snapshot is available: the
connector opens a new `REPEATABLE READ` transaction and continues after the
last stored key. The same happens when a snapshot is requested while the slot
already exists (for example after an [authorized state reset](../operations/recovery.md#recover---accept-state-reset)).
In both cases the snapshot is reported as `convergent`.

**A resumed snapshot is not transaction-consistent.** Rows read after the
resume reflect a later database state than rows read before it, and the
boundary version still applies to all of them. The graph converges because:

- rows read later carry version `(epoch, boundary, 0)`;
- every change committed after the boundary is streamed with a higher version
  and overwrites the snapshot value, or deletes the row (leaving a tombstone);
- writes with versions at or below the entity's current version, tombstone or
  truncate watermark are ignored.

Until streaming has caught up past the time of the last snapshot read, the
graph may show a combination of states that never existed in the source at
one instant.

### Validation and generation activation

When the connector reports `SnapshotComplete`, the engine (phase `validating`):

1. waits until every journaled unit is applied to the graph;
2. compares, per mapped relation, the number of rows read by the snapshot with
   the number of non-deleted entities the sink counts for that relation.
   Relations whose dataset policy has a `tenant_column` are skipped: rows of
   other tenants are read but not projected, so an exact count is not
   comparable;
3. records a `snapshot_validation` audit record with any mismatches and, under
   `count_not_comparable`, the relations whose counts were not compared;
4. on mismatch: if the snapshot was `transaction_consistent`, stops with
   `consistency_incident`; if `convergent`, adds a status warning (counts are
   expected to converge as streaming catches up);
5. calls `GraphSink::set_active_generation` (fenced, under the pipeline lock),
   stores `snapshot_done` locally, records `generation_activated`, and enters
   `streaming`.

Known limitations of validation:

- Relations with a `tenant_column` policy get no count check at all (they are
  listed as `count_not_comparable`); use `db2graph-sync reconcile`, whose
  sampled readback still applies to them.
- Entities left in the graph by an earlier run of the same pipeline are
  counted.

A new snapshot's generation is the graph marker's `active_generation + 1`
(1 for a new pipeline). A snapshot taken again after an
[authorized state reset or source change](../operations/recovery.md) therefore
uses a new generation, and its chunk unit ids do not collide with the receipts
of the earlier snapshot. Sweeping entities that the new generation did not see,
and cutover, are **planned**, not implemented. Setting
`source.snapshot.enabled = false` skips the snapshot; the pipeline then only
receives changes from the slot's start position.

## Schema discovery

`SourceConnector::discover_schema` returns a `SourceSchema`: relations with
columns (name, native type name, type id, nullability), primary key in key
order, unique constraints, foreign keys and the before-image mode.

The PostgreSQL connector reads:

| What | Catalog source |
|---|---|
| Captured relations | `pg_publication_tables` for the configured publication; on PostgreSQL 15+ the publication's column list (`attnames`) limits the columns |
| Columns | `pg_attribute` (non-dropped, `attnum > 0`), `format_type`, type OID, `attnotnull` |
| Primary key and unique keys | `pg_index`: unique, valid, non-partial, non-expression indexes; the index name is used as the key name |
| Foreign keys | `pg_constraint` (`contype = 'f'`), referencing and referenced columns in constraint order, and whether the referenced index is the primary key |
| Before-image mode | `relreplident`: `f` → full, `n` → nothing, `d` without primary key → nothing, otherwise key-only |

Discovery reads catalogs only. It never reads business rows.

## Schema versions

`SourceSchema::version()` is the first 16 hex characters of a SHA-256 content
digest over the source id and the serialized relations. When a discovered
schema's version differs from the latest stored one, the engine appends it to
the encrypted schema history (sequential numbers) and writes a
`schema_version` audit record containing the classified differences:
`relation_added`, `relation_removed`, `column_added`, `column_removed`,
`column_type_changed`, `key_changed`, `constraints_changed`,
`image_mode_changed`.

### Refresh triggers

The schema is re-discovered and the mapping plan regenerated:

- at startup, before the graph preflight;
- when the connector emits a schema signal: the PostgreSQL connector compares
  every `Relation` message with the discovered schema and signals when the
  relation is unknown or its column count, names or type OIDs differ;
- periodically, every `source.schema_refresh_interval_secs` (default 300 s).
  The timer is checked between source events.

### Additive versus incompatible changes

`schema.rs` classifies `column_added` and `relation_added` as additive. **The
engine does not currently use that classification to approve anything
automatically.** The mapping plan embeds the schema version, so any schema
change, additive or not, produces a new mapping digest:

| Profile | Effect of any schema change |
|---|---|
| `development` | The new plan is used immediately. |
| `enterprise`, `restricted` | The pipeline pauses until the new digest is approved. |

After approval, what an additive column means for the graph is decided by
policy: it is projected if the dataset allows it (no `allow_fields` list and
`unknown_fields = "allow"`), and never in the restricted profile, which forces
default-deny.

Effects of incompatible changes on already-projected data:

- **Dropped column.** Later images no longer contain the column, so the field is
  `missing` and the existing graph property is left unchanged. It is not
  removed automatically.
- **Changed column type.** Values are decoded with the new type from the next
  change on; existing properties keep their old representation until the row
  changes.
- **Primary-key definition change.** Entity identities derive from the key, so
  rows get new identities after the change; existing entities are not migrated.
- **Relation rename, drop and recreate**: see
  [Identity and relationships](identity-relationships.md#relation-rename-drop-and-recreate).

### Mapping approval digest

The mapping plan digest is the first 32 hex characters of a content digest over
the serialized plan (schema version, relations with labels, key columns,
unique keys and edges, join tables, exclusions). `db2graph-sync plan` prints
it. Outside development:

- configuration validation requires `mapping.approved_digest`;
- at runtime the engine compares the computed digest with the approved digest;
  on mismatch it records `mapping_awaiting_approval`, sets
  `status.pause_reason`, `mapping_digest` and `mapping_approved = false`, and
  pauses;
- approval is given with `SyncHandle::approve_mapping(digest)`,
  `Control::approve_mapping`, `POST /v1/mapping/approve` or
  `db2graph-sync approve-mapping --digest ...`. A digest that does not match
  the computed plan leaves the pipeline paused.

Runtime approval is held in memory only. Update `mapping.approved_digest` in the
configuration as well, or the next start pauses again.

### Pause instead of guess

The engine pauses rather than guessing in two situations:

1. the current mapping plan is not approved (capture stops reading the source;
   nothing new is journaled or acknowledged);
2. a journaled unit cannot be planned (`schema_incompatible`, for example a
   key column missing from an image or a column using the reserved `_d2g`
   prefix; or `unsupported`, an operation the planner or sink cannot apply). Units
   before it are applied, then the applier pauses with the unit id in
   `pause_reason`. `resume` retries the same unit; it pauses again unless the
   cause was removed (for example by an approved mapping change).

While capture is paused, the source keeps retaining changes (PostgreSQL: WAL
held by the slot). See [Capacity and cost](../operations/capacity-cost.md).

## Operator-only source changes

The library never changes the source's configuration. It does not create
publications, roles or grants, change replica identity, or change server
settings, and it never drops replication slots. The only object it creates is
its own replication slot, when missing. `db2graph-sync source-setup` prints the
statements an administrator reviews and runs (see
[PostgreSQL](../connectors/postgresql.md#setup)).

Adding a table to the publication is therefore an operator action. The new
relation appears at the next schema refresh and, outside development, requires
mapping approval (and a dataset allowlist entry to be captured). Its existing
rows are **not** loaded: re-snapshotting is planned, not implemented. Only
changes committed after it was added are streamed.

## Source files

- `crates/db2graph-sync/src/source/postgres/stream.rs` — slot creation, snapshot, streaming
- `crates/db2graph-sync/src/source/postgres/mod.rs` — discovery, preflight
- `crates/db2graph-sync/src/engine.rs` — `refresh_schema`, `complete_snapshot`
- `crates/db2graph-sync/src/schema.rs`, `crates/db2graph-sync/src/mapping.rs`
