# Limitations

db2graph-sync — an open-source project by Adventa. This file lists the known
correctness, compatibility, deployment and performance boundaries of release
0.1.0 (unpublished). It is kept in step with the code; if something here is
fixed, the fix and its test are referenced in CHANGELOG.md.

## Qualification

* **Not CI-qualified.** The PostgreSQL connector, Neo4j sink and Qdrant sink
  passed their real-server integration tests (`scripts/integration.ps1`)
  against PostgreSQL 16.15, Neo4j 5.26.31 Community and Qdrant 1.13.4 in local
  Docker on Windows only, over plaintext (development exception). TLS against
  these servers, Linux/macOS, managed services (RDS, Aurora, Neo4j Aura or
  clusters, Qdrant Cloud) and the live-service examples were not exercised.
  Capability descriptors therefore still report `qualified: false` and the
  engine shows a warning.
* Pinecone sink and the OpenAI-compatible HTTP embedder are experimental and
  have never been run against a live service.
* Only Windows 11 x86_64 (MSVC, Rust 1.99.0) was used. Linux and macOS are in
  the CI matrix; no CI run has executed yet. The MSRV (1.90) job has not run.
* Crash tests inject failures at named boundaries and restart in the same
  process; there are no process-kill or power-loss tests yet. Fault points
  `vector.after_delivery` and `shutdown.before_drain` have no tests.

## Not implemented (planned)

MySQL, MongoDB/Atlas, DynamoDB Streams and SQL Server connectors; Memgraph and
Neptune sinks (their config types are accepted and rejected with
`unsupported`); leader election / automatic HA; receipt pruning;
concurrency beyond ordered batching; PostgreSQL protocol v2 streaming of large
in-progress transactions; re-embedding generations with cutover and cost
budgets; CAC/PIV human-administration integration; a FIPS 140-3 validated
provider; cleanup of entities from a previous source history after
`accept-source-change`; benchmark harness and results; signed releases.

## Correctness boundaries

* **Delivery contract:** at-least-once transport with effectively-once graph
  effects only when a single owner holds the fence and the sink implements
  receipts, guarded versions, fencing, tombstones and atomic batches (the
  conformance suite). Graph and vector stores converge asynchronously; there is
  no cross-store atomicity.
* **Ordering:** per pipeline, units apply strictly in source commit order;
  throughput is bounded by serialized batches (Neo4j: pipeline-node lock).
* **Large transactions:** buffered in memory up to
  `source.max_transaction_changes` (default 200 000); larger transactions stop
  the pipeline with `limit_exceeded`.
* **Schema changes:** outside development, *any* schema change (including an
  added column) changes the mapping digest and pauses the pipeline until the
  new digest is approved. Relation rename/drop/recreate yields new identities
  and requires approval.
* **Snapshots:** an interrupted snapshot resumes as *convergent* (not
  transaction-consistent). Relations without a primary key are not
  snapshotted, even when mapped with `key_constraint`. Exact count validation
  is skipped for datasets with `tenant_column`. A second-generation snapshot
  does not sweep entities that disappeared from the source while the pipeline
  was stopped (reconcile to detect).
* **Replica identity:** `REPLICA IDENTITY USING INDEX` on an index that does
  not cover the mapped key makes deletes unresolvable; the pipeline pauses
  (`schema_incompatible`). `REPLICA IDENTITY NOTHING` fails preflight.
* **Graph restore:** restoring the graph target to an earlier point is not
  detected (the marker epoch only guards against *local state* rollback);
  reconcile after any graph restore.
* **State loss:** after `recover --accept-state-reset`, units that had been
  acknowledged to the source but not yet applied are lost (the source no
  longer retains them); reconcile afterwards. An anchor file belonging to the
  lost store must be moved aside as part of recovery.
* **Receipts** are retained indefinitely (graph storage grows with the number
  of source transactions).
* **Policy changes** apply immediately at the retrieval boundary and to new
  capture, but do not remove properties already projected; reconcile or
  re-bootstrap the projection.
* **Audit file:** the HMAC chain detects edits, reordering and deletions
  inside the file but **not truncation of its tail**; forward audit to an
  authenticated external store for that.
* **Vector:** visibility is `verified` for Qdrant (`wait=true`) and `unknown`
  for Pinecone. Pinecone record ids do not include the model version.
* **JSON values** are stored as the server's exact text (PostgreSQL `jsonb`
  text is already normalized; `json` keeps source whitespace).

## Compatibility

* PostgreSQL 13+ primaries only; standby decoding, partitioned tables
  (`publish_via_partition_root`), RDS and Aurora are not qualified. Text-format
  values only; types outside the interpreted set are kept as tagged text.
* Neo4j 5 via `neo4rs` 0.8.0: single-member `bolt+s://` connections only (no
  cluster routing); `bolt+ssc://` is rejected.
* Restricted profile: always refuses to start with the built-in source, graph,
  vector and embedding adapters (`restricted_provider_unavailable`).

## Deployment

* One engine per state directory and replication slot; a second instance with
  valid state takes over by fencing out the first (no election).
* The management API has no rate limiting; tokens are bearer credentials.
* The container image (`Dockerfile`) has not been built or scanned.
* Registry packages (crates.io, npm, PyPI, container registry) are not
  published.

## Performance

No benchmarks have been run. The proposed stretch objectives (same-region
graph p95 ≤ 20 ms, p99 ≤ 100 ms; 20 000 source changes/s per measured worker
core for normalization and mapping) are unmeasured and are not SLAs.

## Dependency debt

`cargo deny` acknowledges four unmaintained (not vulnerable) crates reached
only through `neo4rs` 0.8.0: `backoff` (RUSTSEC-2025-0012), `instant`
(RUSTSEC-2024-0384), `paste` (RUSTSEC-2024-0436), `rustls-pemfile`
(RUSTSEC-2025-0134). Removing them requires a newer Neo4j driver.
