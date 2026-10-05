# Threat model

Scope: db2graph-sync as an embedded library or standalone process that reads
change data from a source database and writes a projection into a graph
database and optional vector index. This model reflects the code in this
release; mitigations marked *planned* are not implemented.

## Assets

| Asset | Why it matters |
|-------|----------------|
| Source data in transit and in the journal | May contain regulated or classified fields |
| Encrypted durable state (journal, checkpoints, schema history, audit spool) | Integrity determines correctness of the projection |
| State encryption keys | Decrypt state; forge state |
| Source/graph/vector credentials | Access to authoritative and derived data |
| Graph projection and vector index | Consumed by GraphRAG retrieval |
| Policy configuration and mapping approval | Decides what is projected and to whom |
| Management tokens | Control of a running pipeline |
| Audit trail | Accountability and incident reconstruction |

## Trust boundaries

1. Source database ↔ engine (network; TLS verify-full).
2. Engine ↔ graph target and vector store (network; TLS).
3. Engine ↔ embedding provider (network; plaintext text leaves the process).
4. Engine ↔ local state directory (filesystem; encrypted, authenticated).
5. Operators/automation ↔ management API (network; bearer tokens over TLS).
6. Host application ↔ embedded library (same process; shared memory).
7. Retrieval clients ↔ vector index (outside the engine; see the retrieval boundary).

## Threats and mitigations

| # | Threat (STRIDE) | Mitigation in this release | Residual / planned |
|---|-----------------|----------------------------|--------------------|
| T1 | Spoofed source/graph endpoint (S) | rustls verification of chain and host name; no verify-off option; SSLRequest refusal is an error (no fallback); `bolt+ssc` rejected | Certificate pinning not offered |
| T2 | Credential disclosure in config/logs (I) | Secret references only; `literal:` limited to development; `Secret` redacts Debug and zeroizes; URL redaction in errors | Host log configuration is the host's responsibility |
| T3 | Tampering with state files (T) | AES-256-GCM with AAD binding store/table/key/key-id; failures → `state_tampered` (terminal) | — |
| T4 | Rollback to an older, authentic state copy (T) | Monotonic epoch; graph marker and optional anchor file (key id authenticated) record epochs; lower local epoch → `state_rollback`, checked before the fence is taken; explicit authorized recovery | Anchor must live on storage not restored with the state dir |
| T5 | Two writers on one pipeline / split brain (T) | OS lock on state dir; PG slot exclusivity; graph fence token checked under the pipeline lock in every batch → `fenced_out`; an instance with stale state fails `state_rollback` before fencing out the owner | No leader election (planned); a second instance with valid state still takes over |
| T6 | Replay or reordering producing wrong graph (T) | Receipts with content digests; version guards under lock; tombstones/watermarks; batches never split units | Correctness depends on sink qualification |
| T7 | Over-projection of sensitive fields (I) | Dataset allowlist (required outside development), field allow/deny before journaling; restricted forces default-deny | Policy changes are not retroactive (reconcile) |
| T8 | Sensitive text sent to embedding provider (I) | Only embed-approved fields; protected (labelled) fields require explicit `allow_protected`, forbidden in restricted | Provider-side retention is outside our control |
| T9 | Unauthorized retrieval of stale/deleted/restricted entities (I) | Retrieval boundary re-checks current graph + current policy; tenant, classification(s), CUI, export, releasability evaluated independently | Clients that query vectors directly bypass it (documented) |
| T10 | Management API abuse (E) | Tokens stored as SHA-256, constant-time compare, scopes, TLS required outside development, no arbitrary-query endpoint, bounded bodies | No rate limiting; CAC/PIV via identity proxy (planned) |
| T11 | Malformed management requests disrupting sync (D) | Handlers validate and reject without touching the engine; tested | — |
| T12 | Cypher injection via identifiers (T) | All values parameterized; labels/relationship types validated against `[A-Za-z_][A-Za-z0-9_]{0,63}` and quoted | — |
| T13 | SQL injection via snapshot keyset literals (T) | Values originate from the source itself, are quoted with doubled quotes; `standard_conforming_strings=on` verified by preflight | — |
| T14 | Audit suppression (R) | Audit spooled in encrypted state before forwarding; spool limit stops protected work (`audit_unavailable`); HMAC-chained file (key id bound into each line's HMAC) verified by `audit-verify` | No external authenticated forwarding sink bundled (trait provided); tail truncation of the file is not detectable by the chain alone |
| T15 | Resource exhaustion (journal growth, WAL retention) (D) | Journal backpressure (`max_journal_units`); slot retention reported by preflight; large-transaction limit | WAL retention is ultimately bounded only by source settings |
| T16 | Weak crypto claims (I/R) | Default provider reports NotValidated; restricted profile fails closed (including for built-in vector sinks and the HTTP embedder); no FIPS/DoD/IL4 claims | Validated provider not bundled |
| T17 | Embedded host interference (D) | Caller runtime only, no global logger/signal/env changes, no port binding, typed failures, bounded shutdown | Host must start one engine per state dir |
| T18 | Supply chain (T) | `deny.toml` license/advisory/source policy; single TLS provider audited; CI pins actions (see workflows) | No artifact signing performed yet |

## Assumptions

* The host OS, filesystem and the account running the engine are trusted.
* Key material is provided by a secret manager or protected files.
* The graph and vector stores enforce their own access control for clients
  other than this engine.
* Clocks are only used for freshness reporting, never for ordering.
