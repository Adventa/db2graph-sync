# Pinecone

**Status: experimental, unqualified.** The sink is implemented against
Pinecone's documented data-plane REST API (API version header `2024-07`) but
has never been run against a live index. Its capability descriptor reports
`qualified: false`, and its preflight always adds a warning saying so. Only
record-id formatting and URL encoding are unit-tested.

Requires the cargo feature `vector-pinecone`.

## Configuration

```toml
[vector.sink]
type = "pinecone"
index_host = "example-index.svc.example.pinecone.io"   # https:// is added if missing
api_key = "env:D2G_PINECONE_API_KEY"
# namespace = "shop"                                   # default: the empty namespace
```

The index must be created out of band; bootstrap does not provision it.
Preflight calls `describe_index_stats` and compares the index dimension with
the embedding dimensions.

## Behaviour

| Aspect | Behaviour |
|---|---|
| Record ids | `<pipeline>:<entity id>#<chunk index>` (not the UUID point ids used by Qdrant) |
| Metadata | the point payload (pipeline, entity id, relation, tenant, version, chunk index and count, content digest, model, model version, labels JSON) |
| Upsert | `POST /vectors/upsert` in batches of 100 |
| Visibility | always **unknown**: Pinecone is eventually consistent and the sink cannot confirm when a record is searchable |
| Remove stale records | list ids by prefix `<pipeline>:<entity id>#` (`/vectors/list`, which is available on serverless indexes), fetch them to read their `version`, delete those with another version (`/vectors/delete`, batches of 1 000) |
| Delete filter | not used (`delete_by_filter: false`) |

Because record ids do not include the model version, a model change overwrites
records with the same entity and chunk index. Listing by prefix is a serverless
index feature; behaviour on other index types is not established.

## Source files

- `crates/db2graph-sync/src/vector/pinecone.rs`
