# CLI reference

Binary: `db2graph-sync` (crate `db2graph-sync-cli`). `db2graph-sync --help`
lists the subcommands.

## Common arguments

| Argument | Env var | Used by |
|---|---|---|
| `-c, --config <FILE>` | `D2G_CONFIG` | All config-based commands |
| `--url <URL>` | `D2G_URL` | Remote commands. Base URL of the management API, e.g. `https://sync.internal:8443`. |
| `--token-file <FILE>` | `D2G_TOKEN_FILE` | Remote commands. A file containing the bearer token. |
| `--ca-file <PEM>` | none | Remote commands. Extra CA on top of the platform roots. |

Remote commands use rustls (`ring`) and a 30 s request timeout.

## Subcommands

| Command | Description | Exit codes |
|---|---|---|
| `validate -c F` | Offline validation. Prints `[kind] path: message` per finding, or `configuration valid (profile …)`. | 0, or the code of the most specific finding kind |
| `preflight -c F` | Validates, then runs source and graph preflight. Prints `[PASS|WARN|FAIL|NOT-EXECUTED] check: detail`. | 0; 3 if any check is FAIL; error kinds otherwise |
| `plan -c F [--json]` | Discovers the schema; prints the plan digest, schema version, relations, labels, keys, edges, join tables, exclusions and whether the digest is approved. `--json` prints `{digest, plan}`. Writes nothing. | 0; error kinds |
| `bootstrap -c F` | Validates. Creates graph constraints and indexes (needs an approved digest outside `development`), then the vector collection (Qdrant) or prints that the sink is provisioned out of band. | 0; 2 (`mapping_not_approved`); error kinds |
| `source-setup -c F` | Prints the SQL an administrator runs to enable capture (wal_level, REPLICATION, publication, GRANT SELECT). Never executes it. PostgreSQL only. | 0; 8 for other sources |
| `run -c F [--log-format text\|json] [--drain-secs N]` | Runs the engine in the foreground. `--drain-secs` defaults to 30. See [Standalone](../guides/standalone.md). | 0 on orderly stop; error kinds |
| `status` (remote) | `GET /v1/status` | 0; 5 on 401/403; 64 on 400; 4 on other non-2xx or connection errors |
| `pause` (remote) | `POST /v1/pause` | Same as `status` |
| `resume` (remote) | `POST /v1/resume` | Same |
| `approve-mapping --digest D` (remote) | `POST /v1/mapping/approve` | Same |
| `reconcile -c F [--sample N]` | Compares source and graph: counts plus verified readback hashes for up to N rows per relation (default 100). Read-only. Prints JSON. | 0 if consistent; 7 if not |
| `evidence -c F --out FILE [--sample N]` | Writes a JSON evidence document: about, config digest (literal secrets redacted), findings, crypto self-test, capabilities, preflight, graph marker, mapping approval, state summary, audit chain, and reconciliation when N > 0 (default 0). Sections that can't be collected record the reason. | 0; 2 if the file can't be written |
| `recover -c F (--accept-state-reset \| --accept-source-change) --reason R` | Authorized recovery; the engine must be stopped. Exactly one action flag is required. `--accept-state-reset`: after state loss or restore, accepts the graph's progress evidence and advances the local state epoch past it. `--accept-source-change`: after `source_identity_changed`, increments the source epoch and clears the stored source identity, acknowledged position and snapshot state, so the next start takes a new snapshot. `--reason` must be at least 8 characters and is audited. See [Recovery](../operations/recovery.md). | 0; 64 if neither or both flags are given or the reason is too short |
| `rotate-keys -c F` | Re-encrypts all durable state under `state.active_key`. The engine must be stopped. | 0; error kinds |
| `audit-verify -c F [--file P]` | Verifies the audit file's HMAC chain (default `audit.file`) | 0; 2 if no file is configured or given; 5 on chain failure |
| `keygen` | Prints a random 32-byte key, base64, for a state key secret | 0 |
| `token-hash` | Reads one line (the token) from stdin and prints its SHA-256 hex for `management.tokens[].sha256` | 0 |
| `about` | Prints attribution, version, license and compiled features as JSON | 0 |

Usage errors exit 64; `--help` and `--version` exit 0. Errors are printed to
stderr as `error [kind]: message`.

`plan` and `source-setup` don't run full validation first. Run `validate`
before them.
