# Benchmarks

**No benchmark results exist for this release, and no benchmark harness exists
yet (planned).** Nothing on this page is a measured number.

## Proposed stretch objectives

The project has the following **proposed stretch objectives**. They are not
service-level agreements, not commitments, and have **not been measured**:

| Objective | Target | Conditions |
|---|---|---|
| Graph apply latency | p95 ≤ 20 ms, p99 ≤ 100 ms | source, engine and graph target in the same region |
| Normalization and mapping throughput | 20 000 source changes per second per measured worker core | normalization and mapping only (not end-to-end) |

Until results produced with the methodology below are published, do not plan
capacity on these figures.

## Categories to benchmark

A benchmark report should cover each category separately, because they
exercise different bottlenecks:

1. **Normalization and mapping throughput** (in-process, no I/O): changes per
   second per core through policy filtering and planning.
2. **Journal throughput**: units per second through encrypted journal append
   with fsync, by unit size and storage type.
3. **Initial snapshot**: rows per second and total time by table size, row
   size and `chunk_rows`.
4. **Steady-state streaming**: sustained changes per second end to end, with
   journal backlog stable.
5. **End-to-end latency**: source commit to graph commit (p50, p95, p99), at
   several load levels below saturation.
6. **Large transactions**: memory use and latency for transactions near
   `max_transaction_changes`.
7. **Contention and hot rows**: repeated updates to the same rows; pending
   relationship churn.
8. **Relationship fan-out**: rows with many foreign keys, join tables, and
   entities with many incoming references (deletes convert them to pending).
9. **Recovery**: time to resume after a crash or restart, with a given journal
   backlog; replay of duplicates.
10. **Backlog drain**: time to drain a journal backlog after a graph outage.
11. **Vector indexing**: tasks per second and embedding calls under the rate
    limit; time from graph apply to vector delivery.
12. **Retrieval validation**: latency of validating a batch of hits.
13. **Resource use**: CPU, memory, state volume growth, WAL retention, graph
    store growth (including receipts).

## Required disclosures

Every published result must state:

| Disclosure | Examples of what to state |
|---|---|
| Software versions | db2graph-sync commit or version, Rust version, PostgreSQL version, Neo4j version, vector store and embedding model |
| Workload | schema, table count, operation mix (insert/update/delete), whether data is synthetic |
| Hardware | CPU model and core count available to each component, memory |
| Topology | which components share hosts, network round-trip times, region placement |
| Row size | average and distribution of row bytes, number of columns, TOAST usage |
| Transaction size | changes per transaction (distribution) |
| Contention | share of operations on hot keys |
| Fan-out | foreign keys per row, join tables, pending relationship counts |
| Encryption | state encryption (always on), TLS on each connection, crypto provider |
| Storage | state volume type, fsync behaviour, graph store disk |
| Target settings | Neo4j memory and transaction settings, `max_connections`, batch settings (`max_batch_transactions`, `max_batch_changes`), `snapshot.chunk_rows`, `status_interval_secs` |
| Duration and warm-up | run length, warm-up excluded, number of repetitions, variance |
| Measurement method | where timestamps are taken; clock synchronization for cross-host latency |

`d2g_last_lag_ms` is computed from the source commit time and the engine host
clock, so cross-host clock differences enter it directly; disclose clock
synchronization when quoting it.

## Status

| Item | Status |
|---|---|
| Benchmark harness | Planned |
| Published results | None |
| Proposed objectives | Not measured |
