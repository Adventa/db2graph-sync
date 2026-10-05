# Source connectors

A source connector turns a database's native change capture into an ordered
stream of complete units for the engine. This page lists the connectors, the
capability descriptor every connector declares, and the rules all connectors
follow.

## Support matrix

| Source | `source.type` | Status | Capture mechanism |
|---|---|---|---|
| PostgreSQL | `postgres` | **Implemented; integration-tested against live servers (local Docker, Windows; see [status page](../project/status.md)); not yet CI-qualified on all platforms.** Its real-server tests (`tests/pg_neo4j.rs`) passed against PostgreSQL 16.15. | Logical replication, `pgoutput` protocol v1 |
| MySQL | `mysql` | Planned | Row-based binary log |
| MongoDB / MongoDB Atlas | `mongodb` | Planned | Change streams |
| Amazon DynamoDB | `dynamodb` | Planned | DynamoDB Streams |
| Microsoft SQL Server | `sqlserver` | Planned | Native CDC change tables |
| Custom (supplied in code) | `custom` | Supported extension point | Implement `SourceConnector` and pass it in `StartOptions::source` |

The planned types are accepted by the configuration parser so that the error is
precise: configuration validation reports `source.type` with error kind
`unsupported` ("this connector is planned but not implemented in this release").
`db2graph-sync validate` exits with code 8 for that finding.

A scripted in-memory source (`source::scripted::ScriptedSource`) exists for
tests and examples. It is not a database connector.

Pages: [PostgreSQL](postgresql.md), [MySQL](mysql.md), [MongoDB](mongodb.md),
[DynamoDB](dynamodb.md), [SQL Server](sqlserver.md).

## Capability descriptor

Every connector returns `SourceCapabilities` from `capabilities()`:

| Field | Meaning | PostgreSQL connector |
|---|---|---|
| `connector` | Name | `postgres-pgoutput` |
| `transaction_boundaries` | Whether complete source transactions are delivered as units. If false, the effective delivery guarantee is lowered to `at_least_once`. | `true` |
| `ordering` | `total_commit_order`, `per_partition` or `none` | `total_commit_order` |
| `snapshot` | Consistency of a fresh initial snapshot: `transaction_consistent`, `convergent` or `unsupported` | `transaction_consistent` |
| `resumed_snapshot` | Consistency after an interrupted snapshot is resumed | `convergent` |
| `before_images` | What the source provides about the old row | key-only (`REPLICA IDENTITY DEFAULT`/`INDEX`) or full (`REPLICA IDENTITY FULL`) |
| `truncate` | Whether truncates are delivered | `true` |
| `schema_change_signal` | Whether the connector signals schema changes in-stream | `true` |
| `qualified_versions` | Source versions the connector was qualified against by tests that actually ran; empty means unqualified | empty |
| `notes` | Free-text limitations | in-progress transactions not streamed; prepared transactions delivered at `COMMIT PREPARED`, not separately qualified; standby decoding, RDS and Aurora not qualified |

The engine trusts these declarations only to lower guarantees, never to raise
them.

## No business-table polling

Connectors never scan or poll business tables to detect changes. Changes come
only from the source's native change mechanism (for PostgreSQL, the logical
replication slot).

Reads of business tables are permitted only for:

- the **initial snapshot** (and resuming it);
- **operator-invoked reconciliation** (`row_count`, `sample_rows`), which is an
  explicit command and never feeds changes into the pipeline.

Reads of catalog metadata (schema discovery, preflight checks, replication slot
status) are permitted at any time.

## Acknowledgement contract

The engine calls `ChangeStream::ack(position)` only after every unit up to that
position is durably journaled. A connector must not confirm positions to the
source on its own. See [Transactions and ordering](../architecture/transactions-ordering.md).

## Writing a connector

Implement `SourceConnector` (`capabilities`, `preflight`, `identity`,
`discover_schema`, `start`, optionally `setup_statements`, `row_count`,
`sample_rows`) and `ChangeStream` (`next`, `ack`) from
`crates/db2graph-sync/src/source/mod.rs`. Configure `source.type = "custom"`
and pass the connector in `StartOptions::source`. Declare capabilities
honestly; leave `qualified_versions` empty unless qualification tests ran.
