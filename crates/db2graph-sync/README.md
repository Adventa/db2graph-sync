# db2graph-sync (library)

**db2graph-sync — an open-source project by Adventa.** <https://www.adventa.com.au>

Embeddable one-way CDC projection engine: loads approved operational database
records and continuously synchronizes their changes into graph databases and
optional vector indexes for GraphRAG. The source database stays authoritative.

```rust,ignore
let sync = db2graph_sync::start(config).await?;
sync.ready(std::time::Duration::from_secs(60)).await?;
// ...
sync.shutdown(std::time::Duration::from_secs(30)).await?;
```

Pre-release 0.1.0. The PostgreSQL connector, Neo4j sink and Qdrant sink are
implemented; integration-tested against live servers (local Docker, Windows;
see the project's IMPLEMENTATION_STATUS.md); not yet CI-qualified on all
platforms. Other connectors and targets are planned. The project's IMPLEMENTATION_STATUS.md and LIMITATIONS.md
describe exactly what is implemented, tested and planned.

Delivery contract: at-least-once transport with effectively-once graph effects
under documented conditions; graph and vector stores converge asynchronously.

Features: `postgres`, `neo4j` (default), `vector-qdrant`, `vector-pinecone`,
`embedding-http`, `service`. MSRV 1.90.

Licensed under Apache-2.0 (see LICENSE and NOTICE in this package).
