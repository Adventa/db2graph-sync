# 0008 — Management API as a router the host mounts; hashed tokens

Status: Accepted
Date: 2026-10-05

## Context

Embedded libraries must not bind ports by default; operators still need
authenticated, scoped, auditable control of standalone processes; service SDKs
need a stable contract.

## Decision

`management::router(control, tokens)` returns an `axum::Router`; the library
never binds a socket. The standalone binary serves it on `management.listen`
with rustls TLS (required outside development). Tokens are configured only as
SHA-256 digests with scopes (`read`, `operate`, `recover`) and compared in
constant time. Every response carries `x-correlation-id`; operations are
idempotent and echo `Idempotency-Key`. Errors are typed JSON. The contract is
versioned (`/v1`, `api/openapi.json`) and a test asserts the served routes
equal the documented routes. There is no endpoint that runs arbitrary queries.

## Consequences

Hosts may mount the router behind their own TLS and authentication. Human
administration with CAC/PIV requires an identity-aware proxy or a future
qualified identity integration; bearer tokens are workload/operator
credentials.
