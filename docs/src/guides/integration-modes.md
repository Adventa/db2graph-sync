# Integration modes

There are three ways to run db2graph-sync. Every mode runs the same engine
and uses the same configuration model.

| Mode | What runs where | Use when |
|---|---|---|
| Embedded library | `db2graph_sync::start` on your application's Tokio runtime, inside your process | Your Rust service should own synchronization and its lifecycle |
| Standalone binary | `db2graph-sync run` in its own process, with an optional management API over TLS | You want a separate deployable unit, operated through the CLI or HTTP |
| Service SDKs (TypeScript, Python) | HTTP clients for the management API of an engine running in either mode above | A non-Rust application needs status, pause/resume, mapping approval or retrieval validation |

## Embedded library

```rust,ignore
// excerpt; full tested version in examples/embedded_postgres_neo4j.rs (compiled, not executed)
let config = Config::from_file(&path)?;
let sync = db2graph_sync::start(config).await?;
```

- Runs on the caller's runtime. The library creates no runtime, binds no
  port, installs no logger or signal handler, doesn't change environment
  variables and never exits the process. See
  [Embedded lifecycle](embedded-lifecycle.md).
- You can add custom sources, sinks, embedders, secret providers, crypto
  providers, audit sinks and state backends through `start_with` and
  `StartOptions`.
- With the `service` feature, `db2graph_sync::management::router(control,
  tokens)` returns an `axum::Router` you can mount inside your own server,
  behind your own TLS (see `examples/host_lifecycle.rs`).
- Embedded mode does not inherit any container hardening. The host process
  provides the security boundary.

## Standalone binary

- `db2graph-sync run -c config.toml`: the binary owns its process, installs a
  logger and handles signals. See [Standalone binary](standalone.md).
- If `management.listen` is set, the management API is served with rustls
  TLS. Plaintext is allowed only in the `development` profile.
- Operator commands (`plan`, `bootstrap`, `reconcile`, `evidence`, `recover`,
  `rotate-keys`, …) are in the [CLI reference](../reference/cli.md).

## Service SDKs

The TypeScript (`sdks/typescript`) and Python (`sdks/python`) SDKs are
**HTTP service SDKs**. They call the [management API](../reference/management-api.md)
over HTTPS and return remote status. They never embed or start the engine,
and there are no native bindings for Node.js or Python. See
[Service SDKs](../sdks/index.md).

## Ownership rules (all modes)

- One running engine per state directory. An OS file lock enforces this;
  a second opener fails with `ownership_conflict`.
- One active owner per pipeline in the graph. Each new owner increments the
  fence token, and a stale owner's next batch fails with `fenced_out`.
  Leader election is planned; it is not implemented.
