# Roadmap

db2graph-sync — an open-source project by Adventa. (<https://www.adventa.com.au>)

> **Nothing on this page is implemented.** These are planned items. They have
> no committed dates, and their order may change. Configuration types exist
> for some planned connectors and sinks so that they fail validation with a
> precise `unsupported` error; that is not an implementation. For what exists
> today, see `IMPLEMENTATION_STATUS.md` and `LIMITATIONS.md`.

Consequential items require an RFC and new adapters follow the adapter
proposal process in [GOVERNANCE.md](GOVERNANCE.md).

## Qualification of what exists

Implemented components that still need evidence before they can be called
qualified:

- Execute the real-service integration tests (PostgreSQL 16 + Neo4j 5,
  Qdrant) and record the results; only then may the Neo4j and Qdrant sinks
  report `qualified: true` (they report `qualified: false` today).
- Tests for the `vector.after_delivery` and `shutdown.before_drain` fault
  points (the points exist; no test uses them yet).
- Execute the CI matrix on Linux and macOS.
- Qualification of PostgreSQL partitioned tables /
  `publish_via_partition_root`.
- Qualification on Amazon RDS / Aurora PostgreSQL.
- Benchmarks with published method and results. The proposed stretch
  objectives (graph apply p95 <= 20 ms, p99 <= 100 ms, 20k changes/s/core) are
  unmeasured and are not claims.

## Sources

- MySQL connector.
- MongoDB / MongoDB Atlas connector.
- DynamoDB Streams connector.
- SQL Server connector.
- Streaming of large in-progress PostgreSQL transactions (pgoutput protocol
  v2) instead of the current `max_transaction_changes` limit.

## Graph targets

- Memgraph sink.
- Amazon Neptune sink.
- Receipt pruning (receipts are currently retained indefinitely).
- Re-snapshot sweep for generations after the first.
- Re-bootstrap/cleanup of entities left from a previous source id after
  `recover --accept-source-change` (today they remain until an operator
  removes them).

## Scale and availability

- High availability beyond a single owner with fencing (for example leader
  election).
- Concurrency beyond ordered batching.

## Vector and retrieval

- Re-embedding with controlled generations, cutover and cost budgets (today
  only `model_version` in point payloads and a request rate limit exist).

## Security and assurance

- A FIPS 140-3 validated crypto provider and validated TLS for the built-in
  adapters (source, graph, vector sinks and HTTP embedder), so the restricted
  profile can start with them.
- CAC/PIV for human administration through a qualified identity integration
  (today human administration uses bearer tokens).

## Release engineering

- Signed releases with provenance.
- Published packages, container image and documentation site (blocked on
  the publication blockers in
  [docs/src/project/releasing.md](docs/src/project/releasing.md)).
