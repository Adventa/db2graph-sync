# Standalone binary

The `db2graph-sync` binary (crate `db2graph-sync-cli`) runs the engine in the
foreground and owns its process. Unlike the library, it installs a logger and
signal handlers.

```sh
db2graph-sync run -c /etc/db2graph-sync/config.toml [--log-format text|json] [--drain-secs 30]
```

`-c/--config` can also come from `D2G_CONFIG`.

## Startup

`run` calls `db2graph_sync::start`. Startup success has the same meaning as
in [embedded mode](embedded-lifecycle.md#what-okhandle-means). Startup errors
exit with the kind's exit code; for example, a config error exits 2 and a
`restricted_provider_unavailable` error exits 5.

## Signals and shutdown

| Event | Behaviour | Exit code |
|---|---|---|
| Ctrl-C / SIGINT (all platforms) | `shutdown(--drain-secs)`, then stop the management listener | 0 on success; otherwise the error kind's code |
| SIGTERM (Unix only) | Same as SIGINT | Same |
| Engine terminal failure | Stop the management listener, wait for the engine, print `error [kind]: message` | `ErrorKind::exit_code` |

On Windows, only Ctrl-C is handled.

## Logging

- Output goes to stderr via `tracing-subscriber`.
- The filter comes from the `D2G_LOG` environment variable (`EnvFilter`
  syntax, e.g. `D2G_LOG=info,db2graph_sync=debug`); the default is `info`.
- `--log-format json` produces JSON lines; `text` is the default.
- Secret values are not logged. Connection strings are redacted before they
  are put into error messages.

## Management listener

If `management.listen` is set (e.g. `0.0.0.0:8443`), the binary serves the
[management API](../reference/management-api.md):

- **TLS:** rustls (`ring` provider), using `management.tls_cert` (PEM
  chain) and `management.tls_key`. Outside `development`, validation requires
  both, plus at least one token. With no TLS files, `development` serves
  plaintext.
- **Protocol:** HTTP/1.1.
- **Failures:** a bind failure is a `config` error (exit 2). A bad
  certificate or key is a `security` error (exit 5).
- **Tokens:** configure the SHA-256 of each bearer token
  (`db2graph-sync token-hash` reads the token from stdin) and its scopes
  (`read`, `operate`, `recover`).
- **Remote commands:** `status`, `pause`, `resume` and `approve-mapping`
  call this API (see the [CLI reference](../reference/cli.md)).

## Exit codes

See [Errors and exit codes](../reference/errors.md). In summary:

| Code | Meaning |
|---|---|
| 0 | Success, or help/version output |
| 2 | Config or mapping not approved |
| 3 | Preflight or resume history lost |
| 4 | Runtime, connectivity or internal errors |
| 5 | Security, secrets or state integrity |
| 6 | Ownership or fencing |
| 7 | Consistency or schema |
| 8 | Feature not compiled or unsupported |
| 64 | Usage or invalid request |

## Containers

The repository may contain a Dockerfile at its root. However, **no container
image has been built or published in this release**; the registry location
is TBD — publication blocker. Any `docker pull` command is pending
publication. `deploy/test/docker-compose.yml` provides test dependencies
only (PostgreSQL, Neo4j, Qdrant). It does not run db2graph-sync.

When you run the binary in a container you build yourself:

- persist `state.dir` on a volume owned by exactly one container;
- keep `state.anchor_file` on separate storage that is not restored
  together with `state.dir`;
- provide secrets as files (`file:`) or environment variables (`env:`).
