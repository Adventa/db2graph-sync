# Summary

[Introduction](introduction.md)

# Getting started

- [Installation](getting-started/installation.md)
- [Quickstart: PostgreSQL to Neo4j](getting-started/quickstart.md)

# Guides

- [Integration modes](guides/integration-modes.md)
- [Embedded lifecycle](guides/embedded-lifecycle.md)
- [Standalone binary](guides/standalone.md)

# Reference

- [Configuration](reference/configuration.md)
- [Feature flags](reference/feature-flags.md)
- [Rust API](reference/rust-api.md)
- [CLI](reference/cli.md)
- [Management API](reference/management-api.md)
- [Errors and exit codes](reference/errors.md)

# Service SDKs

- [Service SDKs (TypeScript, Python)](sdks/index.md)

# Architecture

- [Overview](architecture/overview.md)
- [Transactions and ordering](architecture/transactions-ordering.md)
- [Snapshots and schema](architecture/snapshots-schema.md)
- [Durability](architecture/durability.md)
- [Identity and relationships](architecture/identity-relationships.md)
- [Vector indexing and retrieval](architecture/vector-retrieval.md)
- [High availability and reconciliation](architecture/ha-reconciliation.md)

# Connectors

- [Source connectors](connectors/index.md)
  - [PostgreSQL](connectors/postgresql.md)
  - [MySQL (planned)](connectors/mysql.md)
  - [MongoDB (planned)](connectors/mongodb.md)
  - [DynamoDB Streams (planned)](connectors/dynamodb.md)
  - [SQL Server (planned)](connectors/sqlserver.md)

# Destinations

- [Destinations](destinations/index.md)
  - [Neo4j](destinations/neo4j.md)
  - [Memgraph (planned)](destinations/memgraph.md)
  - [Amazon Neptune (planned)](destinations/neptune.md)
  - [Qdrant](destinations/qdrant.md)
  - [Pinecone (experimental)](destinations/pinecone.md)
  - [Custom sinks](destinations/custom.md)

# Security

- [Security profiles](security/profiles.md)
- [Data protection](security/data-protection.md)
- [Secrets](security/secrets.md)
- [Restricted profile](security/restricted.md)
- [Audit](security/audit.md)
- [Threat model](security/threat-model.md)

# Operations

- [Backup and restore](operations/backup-restore.md)
- [Key rotation](operations/key-rotation.md)
- [Upgrades](operations/upgrades.md)
- [Incident response](operations/incidents.md)
- [Recovery](operations/recovery.md)
- [Capacity and cost](operations/capacity-cost.md)
- [Benchmarks](operations/benchmarks.md)

# Project

- [Licensing and attribution](project/licensing.md)
- [Releasing](project/releasing.md)
- [Implementation status](project/status.md)
- [Architecture decision records](adr/index.md)
  - [0001 One engine, two crates](adr/0001-one-engine-pragmatic-workspace.md)
  - [0002 Encrypted state and backends](adr/0002-encrypted-state-and-backends.md)
  - [0003 PostgreSQL capture and Supabase ETL](adr/0003-postgres-capture-supabase-etl.md)
  - [0004 Effects in the target transaction](adr/0004-graph-effects-in-target-transaction.md)
  - [0005 TLS and crypto providers](adr/0005-tls-and-crypto-providers.md)
  - [0006 Versions and ordering scopes](adr/0006-versions-and-ordering-scopes.md)
  - [0007 Neo4j driver and locking](adr/0007-neo4j-driver.md)
  - [0008 Management API](adr/0008-management-api.md)
