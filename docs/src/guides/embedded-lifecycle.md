# Embedded lifecycle

This page covers the `SyncHandle` returned by `db2graph_sync::start`. The
examples cited here are:

- `examples/host_lifecycle.rs`: executed; uses the scripted source and the
  in-memory graph.
- `examples/embedded_postgres_neo4j.rs`: compiled only, not executed.

## Starting

```rust,ignore
// excerpt; full tested version in examples/embedded_postgres_neo4j.rs
let config = Config::from_file(&path)?;
let sync = db2graph_sync::start(config).await?;
```

`start` must be awaited inside a Tokio runtime; otherwise it returns a
`config` error. `start_with(config, StartOptions { .. })` takes components
built in code (`source`, `graph_sink`, `vector_sink`, `embedder`,
`secret_provider`, `crypto_provider`, `audit_sinks`, `state_backend`). If a
config section has `type = "custom"`, the matching component must be
supplied.

### What `Ok(handle)` means

All of the following succeeded:

1. The configuration validated for its profile.
2. The crypto provider passed its known-answer self-tests. For
   `restricted`, the provider must also report FIPS 140-3 validation, a
   custom source and sink must be supplied, and, when `[vector]` is
   configured, a custom vector sink and embedder too.
3. Secrets resolved.
4. The encrypted state store opened and is exclusively owned by this engine.
5. Adapters were constructed and the supervisor task was spawned on the
   caller's runtime.

It does **not** mean the source or graph is reachable, the snapshot is done,
or replication is live. Retryable errors (connectivity, for example) are
retried with capped exponential backoff (`engine.retry_max_backoff_ms`) with
no attempt limit, and each attempt is recorded in `status.warnings`.

## Readiness

```rust,ignore
// excerpt; full tested version in examples/embedded_postgres_neo4j.rs
match sync.ready(Duration::from_secs(120)).await {
    Ok(()) => println!("db2graph-sync ready: {:?}", sync.status().phase),
    Err(e) if e.kind() == ErrorKind::Timeout => {
        // The host decides: keep serving with a degraded health check, or stop.
        eprintln!("not ready yet: {e}; continuing and observing");
    }
    Err(e) => {
        eprintln!("synchronization failed during startup: {e}");
        let _ = sync.wait().await;
        return Err(e.into());
    }
}
```

`ready(timeout)` resolves as follows:

- `Ok(())` once the phase is `streaming`: live replication is established
  and the initial generation is active.
- `Err` with the failure's kind if the engine fails.
- `Err(shutdown)` if the engine stops first.
- `Err(timeout)` when the deadline passes. A timeout does not stop the
  engine.

Phases: `starting`, `preflight`, `recovering`, `snapshotting`,
`validating`, `streaming`, `paused`, `draining`, `stopped`, `failed`.

## Observing failures

`failed().await` resolves with the terminal error and never resolves after a
clean stop. Use `wait()` to wait for completion.

```rust,ignore
// excerpt; full tested version in examples/embedded_postgres_neo4j.rs
tokio::select! {
    err = sync.failed() => {
        eprintln!("synchronization stopped: [{}] {}", err.kind(), err.message());
        let _ = sync.wait().await;
        Err(err.into())
    }
    _ = tokio::signal::ctrl_c() => {
        let report = sync.shutdown(Duration::from_secs(30)).await?;
        println!("stopped; drained={} remaining={}", report.drained, report.remaining_journal_units);
        Ok(())
    }
}
```

When an engine fails, it stops protected synchronization; it does not keep
going in a degraded mode. The host decides its own shutdown policy.

## Status and freshness

`status()` returns a snapshot, and `subscribe()` returns a
`tokio::sync::watch::Receiver<Status>`. Fields:

- **Identity and state:** `pipeline`, `profile`, `phase`, `ready`,
  `pause_reason`, `failure{kind,message}`, `delivery_guarantee`,
  `crypto_provider`, `crypto_validation`, `source_id`, `acked_position`,
  `fence_token`, `state_epoch`, `schema_version`, `mapping_digest`,
  `mapping_approved`, `snapshot`, `warnings`.
- **`freshness`:** `last_applied_unit`, `last_applied_version`,
  `last_commit_time` (source commit time, RFC 3339), `last_applied_at`
  (graph commit wall-clock time), `last_lag_ms` (commit-to-graph lag, when
  the source reports commit time), `last_source_activity` (including
  keepalives).
- **`backlog`:** `journal_units`, `vector_tasks`, `pending_edges`,
  `audit_spool`.
- **`counters`:** `units_captured`, `units_applied`, `duplicates`,
  `changes_applied`, `graph_retries`, `source_reconnects`, `batches`.
- **`vector`** (when configured): `delivered`, `verified_visible`,
  `visibility_unknown`, `superseded`, `deleted`, `failures`, `backlog`.

## Pause, resume and mapping approval

- `pause()` and `resume()` are idempotent. `resume()` also clears
  `pause_reason`.
- The engine pauses itself, setting `pause_reason`, in two cases:
  - outside `development`, when the computed mapping digest doesn't match the
    approved digest (for example after a schema change);
  - when a unit cannot be planned (`schema_incompatible` / `unsupported`).
    The engine pauses rather than skip it.
- `approve_mapping(digest)` records the approved digest and resumes. If the
  digest doesn't match the plan the engine computed, the pipeline stays
  paused.

## Changing policy at runtime

`update_policy(&PolicyConfig) -> Result<String>` validates the new policy
for the profile (`invalid_request` on failure) and returns the new policy
version.

- The change applies immediately at the retrieval boundary and to newly
  captured work.
- It does not remove properties that were already projected. Use
  reconciliation or re-bootstrap for that.
- It is available on `SyncHandle` only; `Control` and the management API
  don't offer it.

`retrieval_validator()` returns the [authorized retrieval](../architecture/vector-retrieval.md)
validator.

## Health checks: `Control`

`control()` returns a cloneable `Control` that doesn't own the engine
(dropping it does nothing). It provides:

- `status`, `subscribe`
- `pause`, `resume`, `approve_mapping`
- `retrieval_validator`, `profile`, `policy_version`
- `request_shutdown(drain)`: non-blocking

```rust,ignore
// excerpt; full tested version in examples/host_lifecycle.rs
async fn health(State(s): State<AppState>) -> (StatusCode, String) {
    let st = s.sync.status();
    match (&st.failure, st.ready) {
        (Some(f), _) => (StatusCode::SERVICE_UNAVAILABLE, format!("sync failed: {}", f.kind)),
        (None, true) => (StatusCode::OK, "ok".into()),
        (None, false) => (StatusCode::SERVICE_UNAVAILABLE, format!("sync {:?}", st.phase)),
    }
}
```

## Shutdown versus drop

`shutdown(drain_timeout).await` does the following, in order:

1. stops capture;
2. applies journaled work until the deadline;
3. checkpoints;
4. flushes audit;
5. releases ownership.

It is bounded by `drain_timeout` plus a fixed 10 s grace period, and returns
`timeout` if that is exceeded. It returns a `ShutdownReport { drained,
remaining_journal_units, final_status }`. `drained` is `false` if journaled
units remain; they are replayed on the next start.

Completion is reported only after every reference to the state store has
been released, so a new engine can open the same state directory right away.

**Dropping** the handle without `shutdown` only requests cancellation. Nothing
is drained, checkpointed or flushed. Recovery on the next start is safe,
because work is journaled and receipts make replay idempotent. Still, always
await `shutdown` in your application's shutdown hook.

## Host-safety guarantees

The library:

- runs on the caller's Tokio runtime and never creates a nested runtime;
- never calls `process::exit`;
- installs no global logger or tracing subscriber. It only emits `tracing`
  events; the host chooses the subscriber;
- installs no signal handlers;
- never sets or removes environment variables (it only reads them, for
  `env:` secret references);
- binds no ports. The management router is returned for the host to mount;
- never silently discards a background failure: any failing background task
  ends the engine and surfaces as `status.failure`, `failed()` and the
  result of `wait()`/`shutdown()`.

The engine tests cover the absence of a global subscriber, bounded shutdown
and drop-without-shutdown.

## Rules for hosts

- Call `start` **once**, in the application's startup hook, never per
  request.
- Use one owner per state directory, and one active owner per pipeline in
  the graph. A second engine on the same directory gets
  `ownership_conflict`. A second process on another directory but the same
  pipeline fences out the first (`fenced_out`) if its state is valid; if its
  state is stale or empty (behind the graph's evidence) it fails with
  `state_rollback` before taking the fence, and the first keeps running.
- Keep the `SyncHandle` for as long as synchronization should run, and give
  clones of `Control` to anything else that needs to observe it.
