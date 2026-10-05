# Amazon DynamoDB Streams connector

**Status: Planned. Not implemented in this release.**

`source.type = "dynamodb"` is accepted by the configuration parser so that the
error is precise, and validation rejects it with error kind `unsupported`.
`db2graph-sync validate` exits with code 8. Nothing on this page describes
working behaviour; it lists the requirements an implementation must meet.

## Design requirements

A DynamoDB connector must satisfy the [connector rules](index.md) and in
particular:

- **Native stream API.** Read DynamoDB Streams through the stream API; never
  scan tables to detect changes.
- **Shard lineage.** Follow shard splits and parent/child lineage, and finish a
  parent shard before its children, so that per-item order is preserved.
- **Sequence numbers.** Use sequence numbers as positions only within their
  shard. Each shard lineage is its own ordering scope (the `partition` field of
  `OrderingScope`); positions from different shards must not be compared.
- **Ordering.** Order is guaranteed only per item within its shard. The
  connector must declare `ordering: per_partition`, never total commit order.
- **Images.** Respect the stream view type (`KEYS_ONLY`, `NEW_IMAGE`,
  `OLD_IMAGE`, `NEW_AND_OLD_IMAGES`) and declare before-image capability
  accordingly; missing attributes must not be written as `null`.
- **Retention.** Stream records are retained for a limited time. If the
  connector falls behind the retention window, it must stop with
  `resume_history_lost` rather than skip records.
- **No fabricated atomicity.** DynamoDB Streams do not deliver transaction
  boundaries. The connector must not group records into fabricated
  transactions; it declares `transaction_boundaries: false`, which lowers the
  effective guarantee to `at_least_once`.
- **Snapshot.** Define how existing items are loaded and how the load is
  reconciled with the stream; such a snapshot is `convergent`, not
  transaction-consistent.

## Until it exists

A DynamoDB source can be integrated today only through a custom connector
(`source.type = "custom"` with `StartOptions::source`). Such a connector is not
qualified by this project.
