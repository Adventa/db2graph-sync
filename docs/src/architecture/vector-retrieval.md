# Vector indexing and retrieval

Vector indexing is optional (`[vector]` in the configuration). When enabled,
the engine maintains points in a vector store derived from the **graph**, not
directly from the source. Graph and vector stores converge asynchronously;
there is no cross-store atomicity.

## Outbox

For every relation that has a vector template, the planner marks entity
upserts, deletes and truncates as vector-relevant. The graph sink then writes a
vector task **in the same graph transaction** as the entity change:

- one task per entity (Neo4j: `_D2G_VectorTask` keyed `<pipeline>|<entity id>`);
- the task carries the entity id, relation, version and a `delete` flag;
- a newer version replaces the task's version; an older one does not ("latest
  version wins").

Because the task commits with the graph change, a graph change is never lost to
the vector side, and a rolled-back graph batch creates no vector work.

## Worker steps

The vector worker reads up to `vector.batch_size` tasks (default 32; Neo4j
orders them by version) and processes them one at a time. Embedding calls are
spaced at least `60 000 / vector.max_embedding_requests_per_minute`
milliseconds apart (default 600 per minute). While the pipeline is paused the
worker waits.

For each task:

1. **Delete task**: delete every point of the entity in the vector sink, then
   complete the task.
2. Look up the entity's **current** graph state. If it is missing or deleted,
   delete all its points and complete the task.
3. If the entity's current version differs from the task's version, the task is
   **superseded**: it is left in place (the newer version is processed from the
   same outbox record) and counted as `superseded`.
4. If the relation has no template, complete the task.
5. In the restricted profile, an embedding provider that declares
   `allows_protected` is a terminal `security` error.
6. **Render** the template (`{field}` placeholders) from the entity's graph
   properties. A field is substituted only if it is allowed by policy **and**
   listed in the dataset's `embed_fields`. A field is protected if the dataset
   or field carries any classification, CUI category or export-control label;
   protected fields are substituted only if the provider is authorized for
   protected data (`allow_protected = true`) and the profile is not
   `restricted`. Withheld placeholders render as empty text. In the development
   profile without a dataset policy, all projected fields are embeddable.
7. **Chunk** deterministically: split on whitespace into chunks of at most
   `chunk_chars` characters (default 2 000, minimum 16); a single word longer
   than the limit is split. Empty text yields one empty chunk.
8. **Embed** all chunks of the entity in one provider call and check that the
   number and dimensions of vectors match.
9. **Upsert** the points, then **delete** the entity's points whose payload
   version differs from the current version.
10. **Complete** the task only if its version and delete flag are still the
    ones processed (Neo4j: write-lock the task node, then read and delete it,
    in one transaction). Otherwise a newer task has arrived and stays pending.

Failures (other than the terminal security error) are counted in
`vector.failures`, the worker backs off (capped exponential backoff) and the
task stays in the outbox.

## Deterministic point ids and payload

Point ids are UUID-formatted values derived from a content digest of
`(pipeline, entity id, chunk index, model_version)`. The same entity, chunk
and model therefore always map to the same point, so reprocessing overwrites
rather than duplicates. The Pinecone sink uses record ids
`<pipeline>:<entity id>#<chunk index>` instead, so it can list an entity's
records by prefix.

Each point's payload contains:

| Field | Meaning |
|---|---|
| `pipeline` | pipeline id |
| `entity_id` | graph entity id |
| `relation` | source relation |
| `tenant` | entity tenant |
| `version` | entity version the text was rendered from |
| `chunk_index`, `chunk_count` | position of the chunk |
| `content_digest` | digest of the chunk text |
| `model`, `model_version` | embedding model and versioned identifier |
| `labels_json` | policy labels at indexing time (informational) |

## Stale-task protection

Three mechanisms prevent an older graph state from overwriting a newer one in
the vector store:

- the version comparison in step 3 (rendering only from the current entity);
- conditional task completion in step 10;
- deletion of points with any other version after each upsert (step 9).

Between the upsert and the deletion of older points, a search can briefly
return chunks of both versions; the retrieval boundary's exact-version rule
filters them.

## Graph-applied, vector-delivered, search-visible

These are tracked separately:

| Stage | Where it is reported |
|---|---|
| Graph-applied | `counters.units_applied`, `freshness.last_applied_unit`, `last_applied_version`, `last_lag_ms` |
| Vector work pending | `backlog.vector_tasks`, `vector.backlog` |
| Vector-delivered | `vector.delivered` (upserts accepted by the vector sink), `vector.deleted` |
| Search-visible | `vector.verified_visible` when the sink confirms visibility; `vector.visibility_unknown` when it cannot |

Visibility is a property of the sink:

- **Verified**: Qdrant (upserts with `wait=true` reporting `completed`), the
  in-memory vector sink.
- **Unknown**: Pinecone (eventually consistent; the sink cannot confirm when a
  record becomes searchable).

These are counters, not per-entity freshness guarantees.

## Retrieval boundary

Vector indexes are eventually consistent copies, and they may contain points
for entities that were deleted or restricted after indexing. Before a GraphRAG
application uses vector hits, it should pass them through
`RetrievalValidator::validate(principal, hits, rule)` (from
`SyncHandle::retrieval_validator()`), or `POST /v1/retrieval/validate` on the
management API (read scope, at most 1 000 hits per request).

Each hit (`entity_id`, `version` from the point payload) is checked against the
**current** graph and the **current** policy, in this order, and the first
failing check is reported:

| Check | Ineligible reason |
|---|---|
| The entity exists in the graph | `not_found` |
| It is not deleted (including stripped placeholders) | `deleted` |
| Its relation is allowed by the current dataset allowlist | `dataset_not_allowed` |
| Its tenant equals the principal's tenant | `tenant_mismatch` |
| The principal holds every classification marking | `classification` |
| The principal holds every CUI category | `cui_category` |
| The principal is authorized for every export-control marking | `export_control` |
| If releasability is recorded, the principal's code is in the set | `releasability` |
| With `VersionRule::ExactVersion` (default), the hit's version equals the entity's current version | `stale_version` |

The labels evaluated are the union of the labels stored on the entity at write
time and the labels the current policy assigns to its relation and fields, so
a dataset that becomes more restricted is restricted at the boundary
immediately, before any re-indexing. `VersionRule::AnyVersion` accepts hits
indexed from older versions (the entity is authorized; the text may be stale).

> **Warning.** Applications that query the vector store directly and use the
> results without this boundary do **not** inherit these guarantees. They can
> return deleted entities, entities of other tenants, data the caller is not
> cleared for, and stale text. The Qdrant sink's `search` helper filters by
> tenant only; its results must still be validated.

## Re-embedding

Changing the embedding model changes `model_version` and therefore point ids,
but there is no controlled re-embedding: no generation of a new index, no
cutover, and no cost budget. Existing points are only replaced as entities
change. Controlled re-embedding with generations, cutover and cost budgets is
**planned**. The only cost control today is
`vector.max_embedding_requests_per_minute`.

## Adapter status

| Component | Status |
|---|---|
| Qdrant sink | Implemented; integration-tested against live servers (local Docker, Windows; see [status page](../project/status.md)); not yet CI-qualified on all platforms |
| Pinecone sink | Experimental, unqualified (never run against a live index) |
| HTTP (OpenAI-compatible) embedder | Experimental, unqualified |
| Hashing embedder | Deterministic feature hashing, **not semantic**; development profile only |
| In-memory vector sink | Reference implementation for tests and examples; not durable |

## Source files

- `crates/db2graph-sync/src/vector/mod.rs` — render, chunk, point ids, traits
- `crates/db2graph-sync/src/engine.rs` — `vector_worker`, `process_vector_task`
- `crates/db2graph-sync/src/retrieval.rs`, `crates/db2graph-sync/src/policy.rs`
