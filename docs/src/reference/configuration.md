# Configuration reference

The configuration is TOML (`Config::from_file`, `Config::from_toml_str`) or
built in code. Most tables reject unknown fields. Validation
(`Config::validate`, `Config::findings`, `db2graph-sync validate`) runs
offline: passing means the configuration is consistent and allowed by its
profile, not that the servers can be reached.

Examples: `examples/config/postgres-neo4j.dev.toml` and
`examples/config/postgres-neo4j.enterprise.toml`.

## Secret references

Fields of type *secret ref* hold a `<scheme>:<value>` reference, never the
secret itself.

| Scheme | Resolution |
|---|---|
| `env:NAME` | Reads environment variable `NAME` (the library only reads it) |
| `file:/path` | Reads the file; trailing CR/LF is trimmed |
| `literal:value` | Allowed only with `profile = "development"` **and** `engine.allow_literal_secrets = true` |
| other | Resolved by a host-supplied `SecretProvider` (`StartOptions::secret_provider`) that declares that scheme |

## `[pipeline]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `id` | string | required | `[A-Za-z0-9_.-]{1,64}`. Used in receipts, fences and pipeline metadata. Changing it creates a new projection. |
| `tenant` | string | required | Non-empty. Included in every entity id. |
| `profile` | `development` \| `enterprise` \| `restricted` | `development` | See [Security profiles](../security/profiles.md). |

## `[source]`

`type` selects the connector.

| `type` | Status |
|---|---|
| `postgres` | Implemented; integration-tested against live servers (local Docker, Windows; see [status page](../project/status.md)); not yet CI-qualified on all platforms |
| `mysql`, `mongodb`, `dynamodb`, `sqlserver` | **Planned.** Any keys are accepted by the parser, then validation fails with `unsupported`. |
| `custom` | Needs `StartOptions::source`. Optional `name` (free-form label). |

### `type = "postgres"`

| Field | Type | Default | Notes |
|---|---|---|---|
| `host` | string | required | |
| `port` | u16 | `5432` | |
| `database` | string | required | |
| `user` | string | required | Needs REPLICATION (or superuser / `rds_replication`) and SELECT |
| `password` | secret ref | none | |
| `tls.mode` | `verify-full` \| `disable` | `verify-full` | Full chain and hostname verification, no fallback. `disable` requires `development` + `engine.allow_plaintext`. There is no option to turn off verification. |
| `tls.ca_file` | path | none | Extra PEM trust anchors; platform roots are used otherwise |
| `publication` | string | required | Non-empty. Created by the operator (`db2graph-sync source-setup`). |
| `slot` | string | required | `[a-z0-9_]{1,63}`. Created by the engine if missing; never dropped. |
| `schema_refresh_interval_secs` | u64 | `300` | > 0 |
| `snapshot.enabled` | bool | `true` | When `false`, the initial snapshot is skipped |
| `snapshot.chunk_rows` | u32 | `2000` | > 0. Keyset page size. |
| `max_transaction_changes` | usize | `200000` | A larger source transaction stops the pipeline with `limit_exceeded` |
| `status_interval_secs` | u64 | `10` | Replication status-update interval (minimum 1) |
| `max_slot_retained_bytes` | u64 | none | Preflight compares the slot's retained WAL against this |

## `[graph]`

| `type` | Status |
|---|---|
| `neo4j` | Implemented; integration-tested against live servers (local Docker, Windows; see [status page](../project/status.md)); not yet CI-qualified on all platforms |
| `memgraph`, `neptune` | **Planned.** Validation fails with `unsupported`. |
| `custom` | Needs `StartOptions::graph_sink`. Optional `name`. |

### `type = "neo4j"`

| Field | Type | Default | Notes |
|---|---|---|---|
| `uri` | string | required | `bolt+s://host:port` (verified TLS). `bolt://` needs `development` + `engine.allow_plaintext`. `neo4j://`, `neo4j+s://` and `neo4j+ssc://` → `unsupported`: the bundled driver has no client-side routing. `bolt+ssc://` → `security`: use `bolt+s://` with `ca_file`. |
| `user` | string | required | |
| `password` | secret ref | required | |
| `database` | string | none | Server default database |
| `max_connections` | usize | `8` | > 0 |
| `ca_file` | path | none | Extra PEM trust anchors |

## `[state]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `dir` | path | required | Owned by exactly one running engine (OS file lock) |
| `keys` | array of `{ id = u32, key = secret ref }` | required | At least one key; ids must be unique. Each key resolves to base64 of 32 bytes (`db2graph-sync keygen`). |
| `active_key` | u32 | required | Must match a configured `id`; used for new writes |
| `max_journal_units` | u64 | `50000` | Unapplied journal units at which capture applies backpressure |
| `anchor_file` | path | none | HMAC-protected anchor (`store_id:epoch:kid:mac`, key id authenticated) used as extra rollback evidence; checked before the fence is acquired. Verifies after key rotation while the old key is configured. Put it on storage that is not restored along with `dir`. |

## `[mapping]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `approved_digest` | string | none | Digest printed by `db2graph-sync plan`. Required outside `development`. |
| `[[mapping.relations]]` | | | Per-relation overrides |
| ↳ `relation` | string | required | `schema.table` |
| ↳ `labels` | string[] | derived | Default is the PascalCase table name |
| ↳ `key_constraint` | string | none | Unique constraint used as the identity when there is no primary key |
| ↳ `[[mapping.relations.edges]]` | `{ constraint, type }` | | Approved relationship type for a FK constraint (default `FK_<CONSTRAINT>`) |
| `[[mapping.join_tables]]` | `{ relation, from_constraint, to_constraint, type }` | | A declared join table, projected as relationships |

Labels and relationship types must match `[A-Za-z_][A-Za-z0-9_]{0,63}`.

## `[policy]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `unknown_fields` | `allow` \| `deny` | `allow` | Forced to `deny` in `restricted` |
| `[[policy.datasets]]` | | empty | The dataset allowlist. Required outside `development`. Without it, `development` captures every published relation. |
| ↳ `relation` | string | required | `schema.table`; each may appear once |
| ↳ `allow_fields` | string[] | none | If set, only these fields are projected. Required for every dataset in `restricted`. |
| ↳ `deny_fields` | string[] | `[]` | Deny wins over allow (listing a field in both is a validation error) |
| ↳ `embed_fields` | string[] | `[]` | Fields allowed into embedding text |
| ↳ `labels` | Labels | none | Labels for the whole dataset |
| ↳ `field_labels` | map field → Labels | none | Combined with the dataset labels |
| ↳ `tenant_column` | string | none | Row-level tenancy: rows whose value differs from `pipeline.tenant` are excluded. Text, integer, big-integer, decimal and UUID columns are compared by canonical text; `NULL` never matches. Snapshot validation and `reconcile` skip exact count comparison for such datasets. |

**Labels** fields:

| Field | Type | Notes |
|---|---|---|
| `classification` / `classifications` | string or string[] | |
| `cui_categories` | string[] | |
| `export_control` | string or string[] | |
| `releasability` | string[] | Country or organization codes |

Key, foreign-key, unique and tenant columns are always read, because they
define identity. Denied fields are removed before journaling.

## `[vector]` (optional)

| Field | Type | Default | Notes |
|---|---|---|---|
| `sink.type = "qdrant"` | `url`, `collection`, `api_key` (secret ref, optional) | | Feature `vector-qdrant`. Implemented; integration-tested against live servers (local Docker, Windows; see [status page](../project/status.md)); not yet CI-qualified on all platforms. `http://` needs the development exception. |
| `sink.type = "pinecone"` | `index_host`, `api_key` (secret ref), `namespace` (optional) | | Feature `vector-pinecone`. Experimental. |
| `sink.type = "custom"` | `name` | | Needs `StartOptions::vector_sink` |
| `embedding.type = "http"` | `url`, `model`, `api_key` (optional), `dimensions`, `allow_protected` (default `false`) | | Feature `embedding-http`. OpenAI-compatible `/v1/embeddings`. Experimental. `allow_protected = true` is rejected in `restricted`. |
| `embedding.type = "hashing"` | `dimensions` | | Not semantic; `development` only |
| `embedding.type = "custom"` | `name` | | Needs `StartOptions::embedder` |
| `[[vector.templates]]` | `relation`, `template` (`{field}` placeholders), `chunk_chars` (default `2000`) | | Only fields that are both policy-allowed and in `embed_fields` are substituted |
| `max_embedding_requests_per_minute` | u32 | `600` | Hard rate cap |
| `batch_size` | usize | `32` | Vector tasks per worker iteration (minimum 1) |

## `[audit]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `file` | path | none | Append-only JSONL file with an HMAC chain (`audit-verify`). Required in `restricted`. |
| `spool_max_records` | u64 | `100000` | When the encrypted spool of unforwarded records is full, protected work stops with `audit_unavailable` |

## `[management]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `listen` | string | none | Address the **standalone binary** serves on. The library never binds. |
| `tls_cert`, `tls_key` | path | none | PEM. Required outside `development` when `listen` is set. |
| `tokens` | array of `{ name, sha256, scopes }` | `[]` | `sha256` is 64 hex characters (`db2graph-sync token-hash`). Scopes: `read`, `operate`, `recover`. At least one token is required outside `development` when `listen` is set. |

## `[engine]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `max_batch_transactions` | usize | `64` | > 0. Whole source transactions per graph transaction; transactions are never split. |
| `max_batch_changes` | usize | `5000` | > 0 |
| `allow_plaintext` | bool | `false` | Development only (`security` finding otherwise) |
| `allow_literal_secrets` | bool | `false` | Development only |
| `retry_max_backoff_ms` | u64 | `30000` | Cap for exponential retry backoff |

## Profile rules enforced by validation

| Rule | development | enterprise | restricted |
|---|---|---|---|
| Plaintext source, graph or HTTP | only with `allow_plaintext` | rejected | rejected |
| `literal:` secrets | only with `allow_literal_secrets` | rejected | rejected |
| `mapping.approved_digest` | optional | required (`mapping_not_approved`) | required |
| `policy.datasets` allowlist | optional | required | required, with `allow_fields` per dataset |
| Management tokens and TLS when `listen` is set | optional | required | required |
| `audit.file` | optional | optional | required |
| Hashing embedder | allowed | rejected | rejected |
| Validated crypto provider plus custom source and sink | no | no | required at start (`restricted_provider_unavailable`) |
| Custom vector sink and embedder (when `[vector]` is set) | no | no | required at start: built-in vector sinks and the HTTP embedder are refused (`restricted_provider_unavailable`) |

When several findings fail, the reported kind is the first one that isn't
`config` (for example `security`); otherwise it is `config`.
