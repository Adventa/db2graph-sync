# Custom destinations

Graph sinks, vector sinks and embedding providers can be supplied in code
through `StartOptions`. Configure the corresponding section with
`type = "custom"` (an optional `name` is free-form):

```toml
[graph]
type = "custom"
name = "my-graph-sink"
```

```rust,ignore
// see crates/db2graph-sync/examples/custom_graph_sink.rs
let sync = db2graph_sync::start_with(
    config,
    StartOptions { graph_sink: Some(Arc::new(my_sink)), ..Default::default() },
).await?;
```

`type = "custom"` without the matching `StartOptions` field fails with
`config`.

## Implementing `GraphSink`

The trait is in `crates/db2graph-sync/src/sink/mod.rs`:

| Method | Contract |
|---|---|
| `capabilities()` | Declare what the implementation actually does (see below). |
| `preflight(plan)` | Check prerequisites (constraints, indexes, version); report `pass`/`warn`/`fail`/`not_executed` checks. A `fail` stops startup with `preflight`. |
| `bootstrap(plan)` | Create schema objects. Called only by the explicit bootstrap command or API, never by `start`. |
| `acquire_fence(pipeline)` | Return a fence token strictly greater than any previous owner received, and the pipeline marker as found. |
| `marker(pipeline)` | Read the pipeline marker. |
| `apply(ctx, batch)` | Apply the batch atomically and in order (see below). |
| `set_active_generation(ctx, generation)` | Fenced update of the marker's active generation. |
| `counts(pipeline)` | Non-deleted entity count per relation. |
| `lookup(pipeline, ids)` | Current entity state (for vector rendering, retrieval validation and reconciliation). Return deleted placeholders with `deleted = true`. |
| `pending_vector_tasks`, `vector_backlog`, `complete_vector_task` | Vector outbox access; complete a task only if its version and delete flag are unchanged, and return `false` otherwise. |
| `pending_edges(pipeline)` | Number of unresolved relationship records. |
| `outgoing(pipeline, id)` | Owned outgoing relationships of an entity, sorted (used by tests and diagnostics). |

To declare `effectively_once_effects`, `apply` must:

1. commit a batch atomically, including receipts, tombstones, pending
   relationship records, vector outbox entries and the pipeline marker;
2. check each unit's receipt **while holding the pipeline lock**: same digest →
   `ApplyStatus::Duplicate` with no effect; different digest → return
   `consistency_incident` and roll back the batch;
3. guard entity writes by version under a lock (a read followed by an unguarded
   write is not sufficient); same id and version with a different content
   digest is `consistency_incident`;
4. verify the fence token under the same lock and fail with `fenced_out` if a
   newer owner exists;
5. leave tombstones and truncate watermarks so stale events cannot resurrect
   entities, and never remove relationships the pipeline does not own;
6. record `ctx.state_epoch` (maximum) in the pipeline marker in the same
   transaction.

The in-memory sink (`crates/db2graph-sync/src/sink/memory.rs`) is the
executable specification of these semantics, including pending relationships,
join edges, stripping of nodes with foreign relationships and truncate
watermarks.

## Capability honesty

`GraphSinkCapabilities::guarantee()` returns `effectively_once_effects` only
if `atomic_batches`, `receipts_in_target`, `guarded_versions`, `fencing` and
`tombstones` are all true; otherwise `at_least_once`. The engine reports that
value in `status.delivery_guarantee`. It cannot verify the declaration: a sink
that declares a capability it does not implement makes the reported guarantee
false. Declare only what the implementation does, on the specific target
version and configuration you use.

Third-party sinks should report `qualified: false`. The engine then adds a
status warning ("graph sink `<name>` is not qualified by this project"). In
this release the built-in Neo4j sink reports `qualified: false` as well, until
a live-server qualification run. Implementing the trait never grants a stronger guarantee than the declared
capabilities.

## Conformance suite

`db2graph_sync::sink::conformance::run_all(&sink, prefix)` is the executable
form of the contract. It uses its own pipeline ids derived from `prefix`, so it
can run against a shared target. It checks:

| Check | What it verifies |
|---|---|
| `duplicate_replay` | A replayed unit is `duplicate` and changes nothing. |
| `receipt_mismatch_is_incident_and_atomic` | A receipt mismatch is `consistency_incident` and the whole batch rolls back. |
| `stale_and_tombstones` | Stale updates are ignored; tombstones block stale resurrection; a newer insert recreates. |
| `same_version_different_content` | Same version, different content is `consistency_incident`. |
| `relationships` | Pending relationships, resolution on endpoint arrival, FK change, stale edge operations, target delete back to pending, `NULL` FK, self reference. |
| `unique_key_and_join_edges` | Unique-key references; join edges pending, resolved, deleted, stale join edge ignored; join-table truncate removes older join edges and its watermark blocks stale join events. |
| `truncate_watermark` | Truncate removes entities; the watermark blocks stale writes; newer inserts recreate. |
| `fencing` | Fence tokens increase; a stale owner is `fenced_out` with no effect; the marker updates atomically. |
| `vector_outbox` | Tasks are created with graph changes, superseded by newer versions, completed conditionally; deletes create delete tasks. |

Passing the suite against the in-memory sink in-process does not qualify a
remote target. Run it against the real target, at the version and
configuration you deploy, before relying on the declared guarantee, and add
tests for the target's concurrency behaviour.

## Example

`crates/db2graph-sync/examples/custom_graph_sink.rs` wraps the in-memory sink,
counts applied units, reports `qualified: false`, runs the conformance suite,
and then runs a pipeline with a scripted source:

```text
cargo run --example custom_graph_sink
```

## Custom vector sinks and embedders

- `VectorSink` (`crates/db2graph-sync/src/vector/mod.rs`): `capabilities()`
  (`visibility`, `delete_by_filter`, `qualified`), `preflight(dimensions)`,
  `upsert(points)` returning the visibility actually achieved, and
  `delete_entity(pipeline, entity_id, keep_version)` which deletes the entity's
  points whose version differs from `keep_version` (all points when `None`).
  Upserts must be idempotent by point id.
- `EmbeddingProvider`: `capabilities()` (`model_version`, `dimensions`,
  `allows_protected`, `remote`) and `embed(texts)` returning one vector per
  input in order. Declare `allows_protected: true` only for a provider that is
  authorized to receive protected data; it is refused in the restricted
  profile.

Configure them with `vector.sink.type = "custom"` and
`vector.embedding.type = "custom"` and pass `StartOptions::vector_sink` and
`StartOptions::embedder`.
