# Destinations

db2graph-sync writes to one graph target per pipeline and, optionally, one
vector sink.

## Graph targets

| Target | `graph.type` | Status | Declared guarantee |
|---|---|---|---|
| Neo4j 5 (single member, `bolt+s://`) | `neo4j` | **Implemented; integration-tested against live servers (local Docker, Windows; see [status page](../project/status.md)); not yet CI-qualified on all platforms.** Its real-server conformance and end-to-end tests passed against Neo4j 5.26.31 Community (single instance, plaintext Bolt). Reports `qualified: false` (status warning). | `effectively_once_effects` (declared) |
| Memgraph | `memgraph` | Planned | — |
| Amazon Neptune | `neptune` | Planned | — |
| In-memory reference sink (`sink::memory::MemoryGraph`) | code only | Implemented; passes the conformance suite in unit tests. **Not durable.** For tests and examples. | `effectively_once_effects` |
| Custom (supplied in code) | `custom` | Extension point; reported `qualified: false` unless it sets otherwise | Derived from its declared capabilities |

Planned graph types are accepted by the parser and rejected by validation with
`unsupported` ("this graph target is planned but not implemented in this
release").

## Vector sinks

| Sink | `vector.sink.type` | Cargo feature | Status | Search visibility |
|---|---|---|---|---|
| Qdrant (REST) | `qdrant` | `vector-qdrant` | **Implemented; integration-tested against live servers (local Docker, Windows; see [status page](../project/status.md)); not yet CI-qualified on all platforms** (`tests/qdrant.rs` passed against Qdrant 1.13.4 over plaintext HTTP); reports `qualified: false` | Verified |
| Pinecone (data-plane REST) | `pinecone` | `vector-pinecone` | **Experimental, unqualified** (never run against a live index) | Unknown |
| In-memory (`vector::MemoryVectorSink`) | code only | — | Reference implementation; not durable | Verified |
| Custom | `custom` | — | Extension point | Declared by the sink |

## Embedding providers

| Provider | `vector.embedding.type` | Status |
|---|---|---|
| HTTP, OpenAI-compatible `/v1/embeddings` | `http` (feature `embedding-http`) | **Experimental, unqualified** |
| Hashing (feature hashing) | `hashing` | Deterministic, **not semantic**; development profile only |
| Custom | `custom` | Extension point |

In the `restricted` profile the built-in vector sinks and the HTTP embedder are
refused at start with `restricted_provider_unavailable`; vector indexing there
needs custom vector components supplied in code (see
[Restricted profile](../security/restricted.md)).

Pages: [Neo4j](neo4j.md), [Memgraph](memgraph.md), [Neptune](neptune.md),
[Qdrant](qdrant.md), [Pinecone](pinecone.md),
[Custom destinations](custom.md).
