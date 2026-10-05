# Errors and exit codes

Every error has an `ErrorKind`, which hosts, the CLI (exit codes) and the
management API (`code`) branch on. The snake_case names are stable; message
text is for humans and may change. Errors never contain secret values.

## Terminal versus retryable

- **`Error::is_retryable()`** is set per error instance, for transient
  conditions such as connectivity failures and deadlocks (errors built with
  `Error::source`/`Error::graph`, for example).
  - The engine retries these with capped exponential backoff
    (`engine.retry_max_backoff_ms`, default 30000) until they succeed or the
    engine shuts down.
  - This applies to startup preflight, source identity, fence acquisition,
    source (re)connection and graph batch application.
  - Retries are visible in `status.warnings`, `counters.graph_retries` and
    `counters.source_reconnects`.
- **`ErrorKind::is_terminal()`** is true for every kind except `timeout` and
  `invalid_request`. A non-retryable error of a terminal kind stops the
  engine: phase `failed`, `status.failure` set, `failed()` resolves.

## Kinds

| Kind | Meaning | Exit code |
|---|---|---|
| `config` | Configuration failed validation; nothing was started | 2 |
| `mapping_not_approved` | The mapping plan digest isn't approved for this profile | 2 |
| `preflight` | A source or target failed a preflight check | 3 |
| `resume_history_lost` | The source no longer retains the required history (slot missing with stored progress, or `wal_status = lost`) | 3 |
| `security` | A security rule rejected the operation (TLS, authentication, profile rules) | 5 |
| `restricted_provider_unavailable` | Restricted-profile crypto prerequisites aren't met (no validated provider, or built-in network adapters, vector sinks or HTTP embedder in use) | 5 |
| `secret` | Secret material couldn't be resolved | 5 |
| `state_tampered` | Durable state failed authentication (tampering, corruption or wrong key) | 5 |
| `state_rollback` | Durable state is older than trusted progress evidence (graph marker or anchor file) | 5 |
| `unauthorized` | The caller isn't authorized | 5 |
| `ownership_conflict` | Another process owns the state directory, or the replication slot is active elsewhere. A PostgreSQL slot found `active` when the stream starts is reported as a **retryable** instance of this kind (the engine retries with backoff) | 6 |
| `fenced_out` | A newer owner fenced out this instance | 6 |
| `consistency_incident` | Same identity and version with different content, a receipt digest mismatch (same unit id, different captured content), or a snapshot validation count mismatch | 7 |
| `schema_incompatible` | The schema changed incompatibly with the approved mapping | 7 |
| `feature_not_compiled` | A required cargo feature isn't in this build | 8 |
| `unsupported` | A capability or component isn't supported (including planned connectors and targets) | 8 |
| `invalid_request` | Invalid management or CLI request; never affects healthy synchronization | 64 |
| `source` | Source protocol or connectivity failure | 4 |
| `source_identity_changed` | The source system identifier changed (failover to a different system, or restore); needs authorized recovery (`recover --accept-source-change`) | 4 |
| `graph` | Graph target failure | 4 |
| `vector` | Vector sink or embedding failure | 4 |
| `audit_unavailable` | The audit spool is full; protected work stopped | 4 |
| `limit_exceeded` | A configured limit was exceeded (e.g. `max_transaction_changes`) | 4 |
| `timeout` | An operation missed its deadline (`ready`, `shutdown`) | 4 |
| `shutdown` | The engine was shut down | 4 |
| `state_io` | Durable state I/O failure, including storage exhaustion | 4 |
| `internal` | An internal invariant was violated; always a bug | 4 |

## Other CLI exit codes

| Situation | Code |
|---|---|
| Success, `--help`, `--version` | 0 |
| Usage error (bad arguments) | 64 |
| `preflight` with any FAIL check | 3 |
| `reconcile` found inconsistencies | 7 |
| Remote command: HTTP 401/403 | 5 |
| Remote command: HTTP 400 | 64 |
| Remote command: other non-2xx, or connection failure | 4 |
