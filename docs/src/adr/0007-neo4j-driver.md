# 0007 — Neo4j access through `neo4rs` 0.8 with a pipeline lock

Status: Accepted
Date: 2026-10-05

## Context

Neo4j runs read-committed. The Operations Manual ("Concurrent data access")
states that a read followed by a write is not protected unless a write lock is
taken first, that writing a property (e.g. `SET n._dummy_ = true REMOVE
n._dummy_`) acquires the node's write lock, and that locks are held until the
transaction ends. `neo4rs` 0.8.0 is the latest stable release (0.9 is a
release candidate); it has no client-side routing and accepts `+ssc` schemes.

## Decision

* Every batch first write-locks the pipeline node with the documented
  set/remove pattern and checks the fence token under that lock. All writers
  of a pipeline serialize on it, so subsequent guard reads in the transaction
  are protected.
* Only `bolt+s://` (and `bolt://` under the development plaintext exception)
  is accepted; `neo4j://` routing and `bolt+ssc://` are rejected by
  configuration validation.
* Operations are issued as parameterized statements inside the batch
  transaction (no `UNWIND` bulk path yet); `max_connections` is configurable.

## Consequences

Throughput per pipeline is bounded by serialized batches; batching whole units
is the tuning lever. Cluster routing needs a newer driver. Deadlocks and other
`TransientError`s are classified retryable.
