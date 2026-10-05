# Security profiles

Every pipeline runs under one security profile, set in configuration:

```toml
[pipeline]
id = "orders"
tenant = "example"
profile = "enterprise"   # development (default) | enterprise | restricted
```

The profile is enforced in two places:

1. **Offline validation** (`Config::findings` / `Config::validate`, the
   `db2graph-sync validate` command). This never touches the network. It
   reports every finding with a path, a message and an error kind; `validate`
   returns the most specific non-`config` kind (for example `security`).
2. **Startup** (`start` / `start_with`). Startup repeats validation and adds
   checks that need the components themselves: crypto self-tests, the
   restricted-profile provider and adapter checks, state backend durability,
   and embedding-provider capabilities.

Findings for the `source` or `graph` sections are skipped at startup when the
corresponding component is supplied in code through `StartOptions`, because
the configuration section does not describe that component.

## Rules that apply in every profile

- TLS has no "verify = false" option. When TLS is used, certificate chain and
  hostname verification are always performed. Extra trust anchors can be
  supplied (`source.tls.ca_file`, `graph.ca_file`).
- Neo4j routing schemes (`neo4j://`, `neo4j+s://`, `neo4j+ssc://`) are
  rejected as `unsupported` (the bundled driver does not implement
  client-side routing). `bolt+ssc://` is rejected as `security`; use
  `bolt+s://` with `graph.ca_file` for self-signed deployments.
- Secret references must have the form `<scheme>:<value>`.
- At least one state encryption key is required; `state.active_key` must
  name a configured key id; key ids must be unique.
- Crypto known-answer self-tests run at every start and fail closed.
- Management token hashes must be 64 hex characters (SHA-256).
- Planned connectors and sinks (MySQL, MongoDB, DynamoDB, SQL Server,
  Memgraph, Neptune) fail with `unsupported`.

## development

Intended for local development and tests only.

- **Plaintext** (`source.tls.mode = "disable"`, `bolt://`, `http://` vector
  and embedding URLs) is permitted only when `engine.allow_plaintext = true`
  is also set. Without it, plaintext is a `security` finding.
- **Literal secrets** (`literal:` references) are permitted only when
  `engine.allow_literal_secrets = true` is also set.
- The **hashing embedder** (deterministic, not semantic) is permitted.
- A mapping digest approval is not required, and no dataset allowlist is
  required (without one, all published relations are captured and
  `policy.unknown_fields` defaults to `allow`).
- Non-durable custom state backends are permitted.
- Management API: no TLS or token requirement is enforced by validation.

Each exception is explicit in configuration so it is visible in review.

## enterprise

Intended for production.

- `engine.allow_plaintext` and `engine.allow_literal_secrets` are
  `security` findings; plaintext connections and `literal:` secrets are
  rejected.
- **Verified TLS everywhere:** PostgreSQL `verify-full`, Neo4j `bolt+s://`,
  `https://` for the Qdrant and HTTP embedding endpoints.
- **Approved mapping:** `mapping.approved_digest` is required
  (`mapping_not_approved`). At runtime, if the computed mapping digest does
  not equal the approved digest (for example after a source schema change),
  the pipeline pauses with a `pause_reason` until the new digest is approved
  through `SyncHandle::approve_mapping`, the management API or
  `db2graph-sync approve-mapping`.
- **Dataset allowlist:** `policy.datasets` must not be empty. Relations that
  are not listed are not captured.
- **Management API** (when `management.listen` is set for the standalone
  binary): at least one token and both `management.tls_cert` and
  `management.tls_key` are required.
- The hashing embedder is rejected.
- Custom state backends must report themselves as durable.
- A status warning is raised when the crypto provider is not FIPS 140-3
  validated (the default provider is not), and when a graph sink is not
  qualified by this project.

## restricted

All enterprise rules, plus:

- **Validated crypto provider.** The crypto provider must report
  `Validation::Fips140_3 { module, certificate }` and pass its self-tests.
  The default `RustCryptoProvider` reports `NotValidated`, and **no validated
  provider is bundled**.
- **No built-in network adapters.** The built-in PostgreSQL source and Neo4j
  sink use rustls with the `ring` provider, which is not a validated module.
  Startup therefore fails unless **both** a custom source **and** a custom
  graph sink are supplied in code. The built-in vector sinks (Qdrant,
  Pinecone) and the HTTP embedder use the same TLS stack: when `[vector]` is
  configured, startup also fails unless a custom vector sink and a custom
  embedder are supplied in code.
- **Audit destination:** `audit.file` is required.
- **Explicit field allowlists:** every dataset needs `allow_fields`; the
  unknown-field default is forced to `deny` regardless of
  `policy.unknown_fields`.
- **No protected embedding:** `vector.embedding.allow_protected = true` is a
  `security` finding, and an embedder whose capabilities allow protected data
  is refused at startup and by the vector worker.

**Current behaviour:** with the built-in adapters — which includes every
pipeline started by the `db2graph-sync` CLI — the restricted profile **always
refuses to start** with error kind `restricted_provider_unavailable` (CLI exit
code 5). The error message states that the provider is not validated, or that
the TLS stack of the built-in source/graph adapters or of the built-in vector
components is not validated. This is intentional; see
[Restricted profile](restricted.md) for what an integrator must supply.

## Summary

| Rule | development | enterprise | restricted |
|---|---|---|---|
| Plaintext connections | only with `allow_plaintext` | rejected | rejected |
| `literal:` secrets | only with `allow_literal_secrets` | rejected | rejected |
| Approved mapping digest | not required | required | required |
| Dataset allowlist | optional | required | required |
| `allow_fields` per dataset | optional | optional | required |
| Unknown fields default | `policy.unknown_fields` | `policy.unknown_fields` | forced `deny` |
| Hashing embedder | allowed | rejected | rejected |
| Protected fields to embedder | with `allow_protected` | with `allow_protected` | forbidden |
| Management TLS + tokens (if `listen`) | not enforced | required | required |
| `audit.file` | optional | optional | required |
| Crypto provider | any (self-tests) | any (warning if not validated) | must report FIPS 140-3 |
| Built-in source/graph adapters | yes | yes | refused |
| Built-in vector sinks and HTTP embedder | yes | yes | refused |
| Non-durable state backend | allowed | rejected | rejected |

Nothing in this project is certified, authorized or validated by any
government or standards body. The profile names describe configuration rules
enforced by the software, not a compliance status.
