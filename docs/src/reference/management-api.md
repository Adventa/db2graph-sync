# Management API

The contract is `crates/db2graph-sync/api/openapi.json` (OpenAPI 3.1,
version `v1`). It is available with the `service` feature:

- `db2graph_sync::management::router(control, tokens)` returns an
  `axum::Router`. The library never binds a socket.
- The standalone binary serves the router on `management.listen`, using TLS
  from `tls_cert` / `tls_key` (plaintext only in `development`).
- Embedded hosts mount it in their own server, behind their own TLS.

There is no endpoint that runs arbitrary database queries.

## Routes

| Method & path | Auth scope | Response |
|---|---|---|
| `GET /v1/about` | none | JSON: name, version, license, attribution, homepage, features |
| `GET /v1/health/live` | none | `200 ok` |
| `GET /v1/health/ready` | none | `200 ready` when live replication is established; otherwise `503` with the lowercase phase as the body |
| `GET /v1/status` | `read` | JSON `Status` (same fields as `SyncHandle::status`) |
| `GET /v1/metrics` | `read` | Prometheus text (`text/plain; version=0.0.4`) |
| `POST /v1/pause` | `operate` | `{ "ok": true, "phase": … }` |
| `POST /v1/resume` | `operate` | `{ "ok": true, "phase": … }` |
| `POST /v1/mapping/approve` | `operate` | Body `{ "digest": "<32 hex>" }`; approves and resumes. Body limit 64 KiB. |
| `POST /v1/retrieval/validate` | `read` | Body `{ principal, hits, version_rule? }`; returns `{ verdicts: [...] }`. At most 1000 hits; body limit 1 MiB. |

The `recover` scope is accepted in configuration, but no route uses it yet.
Policy updates and recovery operations aren't available over HTTP.

## Authentication

- Send `Authorization: Bearer <token>`.
- Only the SHA-256 hex of each token is configured
  (`management.tokens[].sha256`; generate it with `db2graph-sync token-hash`).
- The token is compared in constant time against every configured token.
- A missing or unknown token returns `401`. A valid token without the
  required scope returns `403`. Both use error code `unauthorized`.
- A router built with an empty token list rejects every authenticated route.

## Correlation and idempotency

- `x-correlation-id` is echoed if it is valid (1–128 characters of
  `[A-Za-z0-9_-]`); otherwise one is generated. It appears on every response
  and in error bodies.
- `idempotency-key` is echoed. The operations are idempotent by design
  (pause, resume and approval set state). No idempotency store is kept.
- Pause, resume and approve are logged with the token name and correlation
  id as `tracing` events.

## Errors

```json
{ "code": "invalid_request", "message": "digest must be 32 hex characters", "correlation_id": "…" }
```

`code` is an `ErrorKind` in snake_case (see [Errors](errors.md)).

| Status | When |
|---|---|
| 400 | Malformed body, body too large, bad digest, or more than 1000 hits (`invalid_request`). Synchronization is unaffected. |
| 401 / 403 | Authentication or scope failures (`unauthorized`) |
| 503 | `retrieval/validate` couldn't read the graph (the code is the underlying kind) |

The OpenAPI file lists these responses, including 403 on `metrics` and
`retrieval/validate`, 503 on `retrieval/validate`, and the request body limits
(64 KiB for `mapping/approve`, 1 MiB for `retrieval/validate`).

## Metrics

All metrics carry the label `pipeline` and are exposed with `# TYPE counter` for `*_total` series and `# TYPE gauge` otherwise:

- `d2g_ready`
- `d2g_failed`
- `d2g_units_captured_total`
- `d2g_units_applied_total`
- `d2g_duplicates_total`
- `d2g_graph_retries_total`
- `d2g_source_reconnects_total`
- `d2g_journal_backlog`
- `d2g_vector_backlog`
- `d2g_pending_edges`
- `d2g_audit_spool`
- `d2g_last_lag_ms` (present only once lag is known)

## Retrieval validation

Request:

- `principal`: `{ tenant, classifications[], cui_categories[],
  export_authorizations[], releasability_code? }`
- `hits`: `[{ entity_id, version }]`
- `version_rule`: `exact_version` (default) or `any_version`

Each verdict is either:

- `{ "verdict": "eligible", hit, entity }`, or
- `{ "verdict": "ineligible", hit, reason }`, where `reason` is one of
  `not_found`, `deleted`, `tenant_mismatch`, `dataset_not_allowed`,
  `classification`, `cui_category`, `export_control`, `releasability`,
  `stale_version`.

Checks are made against the **current** graph and policy. See
[Vector indexing and retrieval](../architecture/vector-retrieval.md).

## Tests

`crates/db2graph-sync/tests/management.rs` covers auth, scopes, correlation,
idempotency echo, invalid requests, validate and metrics, and checks that
the router's routes match the OpenAPI document. These tests run in-process
and bind no network listener.
