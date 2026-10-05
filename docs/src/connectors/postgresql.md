# PostgreSQL connector

**Status: implemented; integration-tested against live servers (local Docker,
Windows; see [status page](../project/status.md)); not yet CI-qualified on all
platforms.** The real-server integration tests (`tests/pg_neo4j.rs`:
`postgres_to_neo4j_end_to_end`, `neo4j_sink_conformance`,
`missing_slot_after_progress_is_resume_history_lost`) passed on 2026-10-05
against PostgreSQL 16.15 (`postgres:16`) and Neo4j 5.26.31 Community, run
locally on Windows 11 with Docker via `scripts/integration.ps1`. They used a
single self-hosted server with the plaintext development exception; TLS
(`verify-full`), RDS/Aurora, standbys and partitioned tables were not part of
that run. Unit tests cover `pgoutput` decoding, value conversion, wire helpers
and snapshot query quoting. `capabilities().qualified_versions` is empty.

The connector uses logical replication with the built-in `pgoutput` plugin,
protocol version 1, through its own replication client
(`source/postgres/wire.rs`, using `postgres-protocol` for messages and
SCRAM). Regular (non-replication) connections use `tokio-postgres`. See
[ADR 0003](../adr/0003-postgres-capture-supabase-etl.md).

## Prerequisites

| Requirement | Checked by preflight |
|---|---|
| PostgreSQL 13 or later (`server_version_num >= 130000`) | `engine_version` (fail) |
| TLS with a certificate the client can verify (chain and host name) | `connect`, `tls` via `pg_stat_ssl` (fail) |
| `wal_level = logical` | `cdc_enabled` (fail) |
| `standard_conforming_strings = on` | `standard_conforming_strings` (fail) |
| A primary server (not a standby) | `primary` (fail) |
| A role with `REPLICATION` (or superuser, or membership in `rds_replication`) | `privileges.replication` (fail) |
| An operator-created publication | `publication` (fail) |
| At least one published relation visible to the role | `schema_visibility` (fail) |
| `SELECT` on every published table (for the snapshot and reconciliation) | `privileges.select` (fail) |
| A primary key on each captured table | `key:<relation>` (warn; such relations are excluded unless mapped with `key_constraint`) |
| Replica identity other than `NOTHING` | `image:<relation>` (fail) |
| Column types the connector interprets | `types:<relation>` (warn; others are kept as text) |
| A free replication slot (`max_replication_slots`) if the slot does not exist yet | `slot` (fail) |
| An existing slot uses `pgoutput`, the configured database, is not active, and is not `wal_status = lost` | `slot` (fail) |
| Retained WAL below `max_slot_retained_bytes`, if set | `retention` (warn) |

Run `db2graph-sync preflight -c <config>`; any `FAIL` exits with code 3.

## Setup

The library never creates publications, roles or grants, never changes replica
identity or server settings, and never drops replication slots. It creates only
its own replication slot, on first start. `db2graph-sync source-setup -c <config>`
prints the statements for an administrator to review and run. For a
configuration with datasets `public.customers` and `public.orders`, user
`d2g`, publication `d2g_pub` and slot `d2g_slot`, it prints:

```sql
-- Review and run as an administrator. db2graph-sync never executes these.
-- 1. Enable logical decoding (requires a restart). On RDS/Aurora set rds.logical_replication = 1 in the parameter group.
ALTER SYSTEM SET wal_level = 'logical';
-- 2. Replication role with least privilege (RDS/Aurora: GRANT rds_replication TO "d2g";)
ALTER ROLE "d2g" WITH REPLICATION;
-- 3. Publication limited to approved tables.
CREATE PUBLICATION "d2g_pub" FOR TABLE "public"."customers", "public"."orders";
GRANT SELECT ON "public"."customers", "public"."orders" TO "d2g";
-- 4. Optional: full before-images where needed (increases WAL volume).
-- ALTER TABLE <schema>.<table> REPLICA IDENTITY FULL;
-- The replication slot `d2g_slot` is created by db2graph-sync on first start.
```

The table list comes from `policy.datasets`; without datasets a placeholder is
printed. The RDS/Aurora hints are printed for convenience; those platforms are
not qualified.

## Configuration

```toml
[source]
type = "postgres"
host = "db.internal"
port = 5432                          # default
database = "shop"
user = "d2g"
password = "env:D2G_PG_PASSWORD"     # secret reference: env:, file: (literal: only in development)
publication = "d2g_pub"
slot = "d2g_slot"                    # [a-z0-9_]{1,63}
schema_refresh_interval_secs = 300   # default
max_transaction_changes = 200000     # default
status_interval_secs = 10            # default
# max_slot_retained_bytes = 10737418240

[source.tls]
mode = "verify-full"                 # default; "disable" only in development with engine.allow_plaintext
# ca_file = "/etc/d2g/pg-ca.pem"

[source.snapshot]
enabled = true                       # default
chunk_rows = 2000                    # default
```

## Connections, TLS and authentication

- **TLS** (`verify-full`, the default and only TLS mode): full certificate
  chain and host-name verification with rustls. Trust anchors are the platform
  roots plus `tls.ca_file`. The `ring` crypto provider is passed explicitly; the
  process-wide rustls default is never installed. If the server refuses
  `SSLRequest`, the connection fails with a `security` error; there is no
  plaintext fallback. There is no option to disable verification.
- **Plaintext** (`mode = "disable"`) is accepted only in the development
  profile with `engine.allow_plaintext = true`.
- **Authentication on the replication connection**: SCRAM-SHA-256 (channel
  binding is not used), MD5, and cleartext password only over TLS. Other
  methods fail with `security`.
- **Session settings** forced on every connection, so snapshot reads and
  `pgoutput` produce the same text values: `DateStyle=ISO,MDY`,
  `TimeZone=UTC`, `extra_float_digits=3`, `IntervalStyle=iso_8601`,
  `bytea_output=hex`.
- `application_name` is `db2graph-sync`.

## Capture

- **Slot**: on first start, if the slot does not exist, it is created with
  `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT` (when a
  snapshot is needed) or `NOEXPORT_SNAPSHOT`. See
  [Snapshots](../architecture/snapshots-schema.md).
- **Streaming**: `START_REPLICATION SLOT <slot> LOGICAL <lsn> (proto_version '1', publication_names '<publication>')`.
  The server starts at the later of the requested LSN and the slot's
  `confirmed_flush_lsn`.
- **Transaction assembly**: `Begin` .. `Commit` are buffered into one unit with
  `unit_id = pg:<system identifier>:<commit end LSN, 16 hex digits>`, version
  position = commit end LSN, and the commit timestamp. Transactions with no
  published changes are not journaled; their position is acknowledged.
- **Acknowledgement**: the engine acknowledges a unit's end LSN after
  journaling it. The connector reports the acknowledged LSN to the server in
  standby status updates every `status_interval_secs` and when the server
  requests a reply. Keepalive `wal_end` positions are acknowledged only when
  no transaction is open.
- **Schema signals**: every `Relation` message is compared with the discovered
  schema (column count, names, type OIDs, unknown relation). A difference
  triggers a schema refresh before the next change is processed.
- **Ignored messages**: `Origin`, `Type` and logical decoding `Message`.

## Type mapping

Values arrive in text format and are converted by type OID
(`pgoutput::text_to_value`):

| PostgreSQL type (OID) | Canonical value | Notes |
|---|---|---|
| `bool` (16) | `Bool` | |
| `int2` (21), `int4` (23), `int8` (20), `oid` (26) | `Int` (i64) | `BigInt` text if a value does not parse as i64 |
| `float4` (700), `float8` (701) | `Float` | `NaN`, `Infinity`, `-Infinity` preserved |
| `numeric` (1700) | `Decimal` | canonical text: trailing fractional zeros trimmed; `NaN` kept |
| `text` (25), `varchar` (1043), `bpchar` (1042), `name` (19), `xml` (142) | `Text` | |
| `uuid` (2950) | `Uuid` | lower-cased |
| `bytea` (17) | `Bytes` | decoded from hex output |
| `date` (1082) | `Date` | ISO text as output by the server |
| `time` (1083), `timetz` (1266) | `Time` | text as output by the server |
| `timestamp` (1114) | `Timestamp` | ISO text with `T` separator |
| `timestamptz` (1184) | `TimestampTz` | UTC; `+00` rendered as `Z` |
| `json` (114), `jsonb` (3802) | `Json` | exact text as output by the server |
| anything else (arrays, `interval`, enums, domains, `inet`, `money`, ranges, geometric, extension types, ...) | `Unknown { type_name, text }` | kept as text with its type name; preflight warns |

Binary-format tuple values are not requested and are rejected.

## Before-images and replica identity

| Replica identity | Delete | Update that changes the key | Other updates |
|---|---|---|---|
| `DEFAULT` with a primary key | old key (`K` tuple) | old key, then delete old identity + upsert new | new row only |
| `FULL` | full old row (`O` tuple) | full old row | full old row |
| `USING INDEX` | old values of the index columns | same | new row only |
| `NOTHING` (or `DEFAULT` without primary key) | not capturable; preflight fails | | |

With `USING INDEX`, the identity key columns must be present in the old-key
image. If the index columns do not include the mapped key (primary key, or the
`key_constraint` override), deletes cannot be planned and the pipeline pauses
with `schema_incompatible`.

## TOAST

Unchanged out-of-line (TOAST) values are not sent by `pgoutput` on update; the
connector reports them as `FieldState::Unchanged`. The graph property is left as
it is. If the entity is new to the graph (for example after a primary-key
change), the field is recorded in `_d2g_incomplete` until a later change
provides the value. Unchanged values are never written as `null`.

## Truncate

`TRUNCATE` (including the cascade flag) is captured as a relation-level
operation. For each mapped relation the sink stores a truncate watermark at the
unit's version and removes owned entities with older versions; events older
than the watermark cannot recreate them. `TRUNCATE` of a declared join table
stores a watermark for that join table and removes its relationships with
older versions; join-row events older than the watermark cannot recreate them.
Relations not in the mapping are ignored.

## Failover and source identity

- The source identity is `pg:<system identifier>` from `IDENTIFY_SYSTEM`; the
  history is the timeline id.
- A different system identifier than the one stored locally stops the engine
  with `source_identity_changed`. Continuing requires the authorized recovery
  `db2graph-sync recover --accept-source-change` (new source epoch, new
  snapshot); entities projected from the previous system identifier keep
  their old ids and remain until removed (see
  [Recovery](../operations/recovery.md#recover---accept-source-change)).
- A timeline change (promotion, point-in-time recovery) with the same system
  identifier adds a warning and a `source_history_changed` audit record.
- Logical decoding from standbys is not qualified; preflight fails on a server
  in recovery.
- Whether a replication slot exists on a newly promoted primary depends on the
  PostgreSQL version and configuration; this is not qualified. If the slot is
  missing while local state has progress, the engine stops with
  `resume_history_lost` instead of creating a new slot and skipping changes.

## Slot retention

The slot makes the server retain WAL from the slot's `restart_lsn`. WAL grows
while the engine is stopped, paused (including waiting for mapping approval),
or applying backpressure because the journal is full. Monitor
`pg_replication_slots` and set `max_slot_retained_bytes` to get a preflight
warning. If the server invalidates the slot (for example because of
`max_slot_wal_keep_size`), `wal_status` becomes `lost` and the engine stops with
`resume_history_lost`. The connector never drops slots; drop an abandoned slot
manually. See [Capacity and cost](../operations/capacity-cost.md).

The slot check runs every time the stream is opened, including reconnects
after a retryable source error. If PostgreSQL still considers the previous
connection's walsender active (for example after a silent network failure,
until `wal_sender_timeout` expires), opening the stream fails with a
**retryable** `ownership_conflict`. The engine retries with capped exponential
backoff, counts each attempt in `counters.source_reconnects` and shows the
error in `status.warnings`, and continues once the server releases the slot.
A genuinely competing consumer keeps it retrying; the state directory lock and
the graph fence still prevent two writers.

Slot checks at stream start and server errors on the replication connection
are classified as follows:

| Server condition | Error kind |
|---|---|
| Slot missing, local progress exists | `resume_history_lost` |
| Slot `wal_status = lost`, SQLSTATE `55000` or `58P01` | `resume_history_lost` |
| Slot `active` when the stream is opened | `ownership_conflict` (retried) |
| SQLSTATE `55006` (object in use) reported by the server | `ownership_conflict` (not retried) |
| SQLSTATE `42501`, `28000`, `28P01` | `security` |
| SQLSTATE `42704` (undefined object) | `preflight` |
| SQLSTATE `42601`, `0A000`, `42P01` | `source` (not retried) |
| Other server and connection errors on the replication connection | `source` (retried) |

## Large transactions

`pgoutput` protocol v1 sends a transaction only after it commits, and the
connector buffers all of its changes in memory until `Commit`. A transaction
with more than `max_transaction_changes` changes (default 200 000) stops the
pipeline with `limit_exceeded`. Raise the limit (memory use grows with
transaction size) or split the transaction at the source. Streaming of
in-progress transactions (protocol v2) is not implemented.

## Prepared transactions

Two-phase decoding is not enabled, so a prepared transaction is delivered as an
ordinary transaction when `COMMIT PREPARED` is decoded. This behaviour is not
separately qualified.

## Limitations

- Integration-tested only against a local single-server PostgreSQL 16.15 in
  Docker on Windows, without TLS; not yet CI-qualified on all platforms (see
  status above).
- Amazon RDS for PostgreSQL and Aurora PostgreSQL: not qualified.
- Logical decoding from standbys: not qualified (preflight fails).
- Partitioned tables and `publish_via_partition_root`: not qualified.
- Relations without a primary key are excluded unless mapped with
  `key_constraint`, and are not loaded by the snapshot.
- Re-snapshot of a relation (for example a table added to the publication
  later) is planned, not implemented.
- Sequences and DDL statements are not captured; schema changes are observed
  through `Relation` messages and catalog refresh.
- Types outside the table above are kept as text, not interpreted.
- SCRAM channel binding is not used.

## Source files

- `crates/db2graph-sync/src/source/postgres/mod.rs` — discovery, preflight, setup statements
- `crates/db2graph-sync/src/source/postgres/stream.rs` — slot, snapshot, streaming
- `crates/db2graph-sync/src/source/postgres/pgoutput.rs` — decoding and type conversion
- `crates/db2graph-sync/src/source/postgres/wire.rs` — replication client, TLS, authentication
