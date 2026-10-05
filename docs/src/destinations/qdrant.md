# Qdrant

**Status: implemented; integration-tested against live servers (local Docker,
Windows; see [status page](../project/status.md)); not yet CI-qualified on all
platforms.** The integration test `tests/qdrant.rs`
(`qdrant_indexing_and_authorized_retrieval`) passed on 2026-10-05 against
Qdrant 1.13.4 (`qdrant/qdrant:v1.13.4`, single node, plaintext `http://` under
the development exception); `https://`, API-key authentication and Qdrant
Cloud were not tested. Unit tests cover template rendering, chunking and point
ids. The sink's capability descriptor reports `qualified: false` until
qualification, which also requires a green CI run on Linux, macOS and Windows
(visible in the capabilities section of the evidence export).

Requires the cargo feature `vector-qdrant`. The sink uses Qdrant's REST API.
For how vector work is produced and processed, see
[Vector indexing and retrieval](../architecture/vector-retrieval.md).

## Configuration

```toml
[vector]
max_embedding_requests_per_minute = 600   # default
batch_size = 32                           # default; outbox tasks read per poll

[vector.sink]
type = "qdrant"
url = "https://qdrant.internal:6333"
collection = "d2g_shop"                   # [A-Za-z0-9_.-]{1,64}
api_key = "env:D2G_QDRANT_API_KEY"        # optional; sent as the api-key header

[vector.embedding]
type = "http"
url = "https://embeddings.internal/v1/embeddings"
model = "example-embedding-model"
dimensions = 1024
api_key = "env:D2G_EMBEDDING_KEY"

[[vector.templates]]
relation = "public.customers"
template = "{name}. {notes}"
chunk_chars = 2000                        # default
```

`http://` URLs are accepted only in the development profile with
`engine.allow_plaintext = true`. HTTPS uses certificate verification with the
platform trust roots; there is no option to add a custom CA for the vector
client in this release.

## Bootstrap

The collection is never created at startup. `db2graph-sync bootstrap` calls
`ensure_collection`: if the collection does not exist, it is created with the
embedding dimensions and cosine distance, and keyword payload indexes are
created for `pipeline`, `entity_id`, `tenant` and `version`. An existing
collection is left unchanged. With a custom embedder, create the collection out
of band.

`VectorSink::preflight` checks that the collection exists and that its vector
size equals the embedding dimensions.

## Behaviour

| Operation | Request | Notes |
|---|---|---|
| Upsert | `PUT /collections/<c>/points?wait=true` | Visibility is reported as **verified** when Qdrant answers with status `completed`, otherwise unknown. |
| Remove stale points | `POST /collections/<c>/points/delete?wait=true` with a filter on `pipeline` and `entity_id`, excluding the current `version` | |
| Remove all points of an entity | same filter without the version exclusion | |
| Search helper (`QdrantSink::search`) | `POST /collections/<c>/points/search` filtered by `tenant` | Results must still pass the [retrieval boundary](../architecture/vector-retrieval.md#retrieval-boundary). |

Point ids are deterministic UUID-formatted values from
`(pipeline, entity id, chunk index, model_version)`; the payload is described
in [Vector indexing and retrieval](../architecture/vector-retrieval.md#deterministic-point-ids-and-payload).

When a request fails (any HTTP error status, timeout or connection error), the
outbox task stays in the graph, `vector.failures` increases, and the worker
backs off and processes the task again later.

## Source files

- `crates/db2graph-sync/src/vector/qdrant.rs`
- `crates/db2graph-sync/tests/qdrant.rs` (passed against a local Docker Qdrant 1.13.4; see status above)
