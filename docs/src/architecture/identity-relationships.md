# Identity and relationships

## Entity identity

Every projected row has a deterministic entity id:

```text
entity_id = hex( SHA-256(
    lp("db2graph-sync/entity/v1") ||
    lp(tenant) || lp(source_id) || lp(relation) || lp(key_name) ||
    u64_be(number of key values) ||
    lp(canonical(v1)) || lp(canonical(v2)) || ...
) )

lp(x) = u64_be(len(x)) || x
```

- `tenant` is `pipeline.tenant`.
- `source_id` is the connector's stable source identity (PostgreSQL:
  `pg:<system identifier>`).
- `relation` is the stable relation name from the mapping
  (`schema.table`), not a volatile object id.
- `key_name` is empty for the primary key. The same function with a unique
  constraint's name as `key_name` produces the lookup digest used for
  references to that unique key. A primary-key reference digest is therefore
  equal to the target's entity id.
- Key values are encoded in declared key-column order.

Every component is length-prefixed, so different inputs cannot produce the
same preimage (`("ab","c")` and `("a","bc")` differ). The version is never part
of the identity.

```rust,ignore
// crates/db2graph-sync/src/identity.rs
pub fn entity_id(scope: IdentityScope<'_>, pk: &[&Value]) -> String {
    key_digest(scope, "", pk)
}
```

### Typed canonical encoding

`Value::encode_canonical` writes a type tag byte followed by a length-prefixed
payload, so integer `1` and text `"1"` have different identities.

| Value | Payload |
|---|---|
| `Int(i64)` | 8 bytes big-endian |
| `Float` | IEEE-754 bits; all NaNs and `-0.0`/`0.0` are canonicalized |
| `Bool` | one byte |
| `BigInt`, `Decimal`, `Text`, `Uuid`, `Date`, `Time`, `Timestamp`, `TimestampTz`, `Json` | UTF-8 text (decimals with trailing fractional zeros trimmed, UUIDs lower-cased, `timestamptz` normalized to UTC) |
| `Bytes` | raw bytes |
| `Array` | concatenated canonical encodings of the items |
| `Unknown { type_name, text }` | type name and text |

### Consequences

- Changing `pipeline.tenant`, the source identity, the relation name or the key
  definition changes every affected id. Existing entities are not migrated.
- Changing `pipeline.id` creates a separate projection: graph metadata is keyed
  by pipeline id.

## Tenants

The tenant is folded into every id. For row-level tenancy, a dataset policy can
name a `tenant_column`. A row is projected only if the canonical text of that
column's value equals `pipeline.tenant`. Text, integer, big integer, decimal
and UUID columns are compared this way (an integer column holding `42` matches
`tenant = "42"`; the comparison is exact, so write `pipeline.tenant` in the
same textual form the source value has). `NULL`,
missing and other value types never match. When a row does not match, the
planner emits a delete for the row's entity, so a row moved to another tenant
disappears from this projection (`policy::tenant_matches`, unit test
`tenant_matching_handles_non_text_columns`).

## Labels

Each mapped relation becomes a node label: the PascalCase table name
(`public.order_items` → `OrderItems`), or the PascalCase schema-qualified name
when two captured relations would collide. Labels can be set in
`[[mapping.relations]] labels = [...]`. Labels and relationship types must match
`[A-Za-z_][A-Za-z0-9_]{0,63}`; labels starting with `_D2G` are rejected.

## Keys

- **Primary key**: the default identity key.
- **No primary key**: the relation is excluded unless
  `[[mapping.relations]] key_constraint = "<unique constraint>"` names a unique
  key to use instead. (Such relations are not loaded by the initial snapshot;
  see [Snapshots](snapshots-schema.md).)
- **Composite keys**: supported; values are encoded in key-column order.
- **Unique-key references**: when a captured foreign key references a unique
  constraint instead of the primary key, the target entity stores the lookup
  digest in a property `_d2g_uk_<constraint name, lower-cased, non-alphanumerics
  as _>`, and bootstrap creates an index on it. Only unique keys referenced by
  some captured foreign key get such a property.

## Foreign keys become owned relationships

Every actual foreign-key constraint whose target relation is captured becomes
an owned relationship from the referencing entity to the referenced entity:

- default type `FK_<CONSTRAINT NAME>` (upper-cased, non-alphanumerics replaced
  by `_`, at most 64 characters);
- a semantic name such as `PLACED_BY` only when an operator declares it:

  ```toml
  [[mapping.relations]]
  relation = "public.orders"
  edges = [{ constraint = "orders_customer_id_fkey", type = "PLACED_BY" }]
  ```

  Declaring a name changes the mapping digest, so outside development it takes
  effect only after approval.

Rules applied by the planner and sinks:

| Situation | Behaviour |
|---|---|
| Multiple foreign keys between the same tables | One relationship per constraint; they are distinct. |
| Composite foreign key | Referencing columns are reordered into the target key's column order, so the digest equals the target's identity or unique-key digest. |
| Self reference, cycles | Supported (the referenced entity may be the same entity or appear later). |
| Any referencing column is SQL `NULL` | No reference: the owned relationship (and any pending record) is removed. |
| Update where all referencing columns are unchanged or absent | The relationship is not touched. |
| Foreign key value changes | The owned relationship for that constraint is replaced. |
| Owning upsert was stale (older version) | The relationship operation has no effect. |
| Target relation not captured | No relationship. |

### Missing endpoints become pending

A relationship is created only when both endpoints exist (live, not deleted).
Otherwise the sink stores a pending relationship record in the graph
(`_D2G_PendingEdge` in Neo4j) in the same transaction. When an entity whose id
or unique-key digest matches a pending record is upserted, the relationship is
created and the record removed. `status.backlog.pending_edges` reports the
count. Deferred constraints, out-of-order snapshot loading and cycles are
handled this way.

### Join tables

A table becomes a relationship instead of nodes only when declared:

```toml
[[mapping.join_tables]]
relation = "public.order_tags"
from_constraint = "order_tags_order_id_fkey"
to_constraint = "order_tags_tag_id_fkey"
type = "TAGGED"
```

The join row's own key is its identity. Its non-foreign-key columns that policy
allows become relationship properties. A `NULL` endpoint means no relationship.
Deletes leave a tombstone. `TRUNCATE` of a join table stores a watermark for
that join table and removes its relationships with older versions; join-row
upserts and deletes older than the watermark are ignored, so stale events
cannot recreate them.

## Primary-key changes

When an update changes the key (PostgreSQL sends the old key), the same unit
deletes the old identity and upserts the new one at the same version. The old
entity's incoming owned relationships become pending (they reference the old
key and resolve only if an entity with that key appears again). Referencing
rows that the source updates (for example through `ON UPDATE CASCADE`) produce
their own changes and relationships. Columns reported as unchanged (TOAST) are
not known for the new entity and are listed in `_d2g_incomplete` until a later
change provides them.

## Delete semantics and ownership

The pipeline owns:

- entity nodes it created (Neo4j: `_D2G_Entity` with
  `_d2g_key = "<pipeline>|<entity id>"`);
- the properties it wrote (listed in `_d2g_props`) and its mapped labels;
- relationships it created (`_d2g_owned = true`, `_d2g_pipeline = <pipeline>`);
- its metadata nodes (labels prefixed `_D2G_`).

On delete of an entity at a newer version:

1. its outgoing owned relationships are removed;
2. incoming owned relationships from other entities are turned back into
   pending records, so they are restored if the entity reappears;
3. pending records owned by the deleted entity are removed;
4. a tombstone with the delete version is stored, so stale events cannot
   resurrect the entity;
5. if the node still has relationships the pipeline does not own, the node is
   **stripped** instead of deleted: owned properties, metadata lists,
   `_d2g_uk_*` lookup properties and mapped labels are removed and
   `_d2g_deleted = true` is set. Deletes never remove relationships the
   pipeline does not own;
6. otherwise the node is deleted.

A later insert with a higher version revives the entity and clears the
tombstone. Truncate of a relation stores a relation watermark and removes every
owned entity of that relation with an older version, using the same rules.

## Relation rename, drop and recreate

These follow from identity being based on the relation **name**:

- **Rename** (`ALTER TABLE ... RENAME`). The schema shows the old relation
  removed and a new one added, so the mapping digest changes and, outside
  development, the pipeline pauses for approval. Dataset allowlist entries and
  mapping overrides are keyed by name: a renamed relation that is not in the
  allowlist is excluded, and a mapping override that names the old relation
  makes plan generation fail with `config` until the configuration is updated.
  Once mapped, the renamed relation's rows get **new identities**; the entities
  created under the old name are neither migrated nor deleted. Existing rows
  are not re-loaded (re-snapshot is planned), so new entities appear only as
  rows change.
- **Drop**. Logical decoding emits no row deletes for `DROP TABLE`. The graph
  entities stay; the relation disappears from the schema, which changes the
  mapping digest. Remove the entities out of band if required.
- **Drop and recreate under the same name**. Identities are the same for equal
  keys. New writes have higher versions and overwrite the old entities; rows
  that were not re-inserted remain as stale entities. To PostgreSQL the
  recreated table is a new object; it must be in the publication again unless
  the publication covers it automatically.

## Source restore

At startup the engine compares the source identity with the one stored in
local state:

- **different source id** (PostgreSQL system identifier changed: a different
  cluster, or a restore that created a new cluster): `source_identity_changed`.
  Positions are not comparable and ids would change. The engine stops until an
  operator runs the authorized recovery
  [`recover --accept-source-change`](../operations/recovery.md#recover---accept-source-change).
  See [Incidents](../operations/incidents.md).
- **same source id, different history** (PostgreSQL timeline changed, for
  example after promotion or point-in-time recovery): a warning is added to
  status, a `source_history_changed` audit record is written, and the new
  history is stored. Whether the replication slot survived is checked
  separately; a missing slot with stored progress is `resume_history_lost`.

The `epoch` component of versions is the **source epoch**. It starts at 0 and
is incremented only by the authorized recovery
`db2graph-sync recover --accept-source-change` (library:
`db2graph_sync::admin::accept_source_change`), which also clears the stored
source identity, acknowledged position, snapshot progress and snapshot
completion, so the next start takes a new snapshot. The epoch is the most
significant component of every version, so versions from the new epoch
supersede those written under older epochs.

Because `source_id` is part of every entity id, rows read after a change to a
**different** source id get **new identities**: they do not supersede the
entities written from the previous source. Those old entities remain in the
graph until an operator removes them out of band; an in-place re-bootstrap
and cleanup of the previous source's entities is planned, not implemented.
Test: `source_identity_change_requires_and_accepts_authorized_recovery`.

## NoSQL references

Reference handling for document and key-value sources (MongoDB, DynamoDB) is
**planned**: those connectors are not implemented. See
[Connectors](../connectors/index.md).

## Suggested mappings are inert until approved

A mapping, however it was produced (written by hand or suggested by a tool or
an AI model), is only configuration. Outside the development profile the engine
runs a plan only if its computed digest equals the approved digest; any other
plan pauses the pipeline. In the development profile no approval is required,
so do not point a development-profile pipeline at data you would not project
without review.

## Source files

- `crates/db2graph-sync/src/identity.rs`, `crates/db2graph-sync/src/types.rs`
- `crates/db2graph-sync/src/mapping.rs`, `crates/db2graph-sync/src/plan.rs`
- `crates/db2graph-sync/src/sink/memory.rs` (reference semantics), `crates/db2graph-sync/src/sink/neo4j.rs`
- `crates/db2graph-sync/src/sink/conformance.rs`
