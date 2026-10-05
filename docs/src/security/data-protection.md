# Data protection

This page describes how db2graph-sync protects data at rest in its local
state, how it limits which data leaves the source, and how labels are carried
to the retrieval boundary. It describes the implementation as it exists; see
`IMPLEMENTATION_STATUS.md` for qualification status.

## Encryption of local state

Everything the engine stores locally goes through an encrypted
`StateStore`: the journal of captured work, checkpoints, acknowledged source
positions, snapshot progress, schema history, the audit spool and the state
epoch. The storage backend (`StateBackend`; default `RedbBackend`) only ever
sees ciphertext.

Tombstones, receipts, unresolved relationships and vector tasks are kept in
the graph target, inside the same transaction as the effects they describe;
they are protected by the graph database's own controls, not by this
encryption.

### Algorithms

All cryptographic operations go through a `CryptoProvider`. The default
`RustCryptoProvider` uses AES-256-GCM, SHA-256 and HMAC-SHA-256 from the
RustCrypto project (`aes-gcm` 0.10, `sha2` 0.10, `hmac` 0.12) and the
operating system random source. It is **not** a FIPS 140-3 validated module
and reports `NotValidated`.

At every start the provider runs known-answer tests and fails closed if any
fails: AES-256-GCM (McGrew & Viega test case 14, including tag rejection),
SHA-256 (`"abc"`), HMAC-SHA-256 (RFC 4231 test case 2), and a non-zero
random-output check.

### Envelope format and AAD

Each stored value is sealed as:

```text
"D2G1" | key_id (u32, big-endian) | nonce (96-bit, random) | ciphertext + GCM tag
```

The additional authenticated data (AAD) is the first 8 header bytes
(`"D2G1"` and the key id) followed by a location context
`"<store_id>/<table>/<key>"`. The `store_id` is a random 128-bit identifier
created when the store is first opened. Binding the location means a validly
encrypted record cannot be moved to another key, table or store without
detection: opening it fails with `state_tampered`. Corrupted bytes, a
truncated envelope or a wrong key also fail with `state_tampered`.

### Nonces and the per-key seal limit

Nonces are 96 bits from the provider's random source. To stay far below the
2^32 invocation bound for random GCM nonces under one key, the store counts
seals per key and refuses further writes once the active key reaches
2^31 seals, with a `security` error asking you to rotate keys.

### Keys, versions and rotation

```toml
[state]
dir = "/var/lib/db2graph-sync/orders"
active_key = 2
keys = [
  { id = 1, key = "file:/run/secrets/d2g_state_key_1" },
  { id = 2, key = "file:/run/secrets/d2g_state_key_2" },
]
```

- Each key is 32 random bytes, base64-encoded, resolved through a secret
  reference (see [Secrets](secrets.md)). `db2graph-sync keygen` prints a new
  key for your secret store.
- New writes use `active_key`. Older records remain readable as long as the
  key id recorded in their envelope is configured.
- `db2graph-sync rotate-keys --config <file>` (engine stopped) re-encrypts
  every record not already under the active key in one commit. After it
  completes, older keys are no longer needed for the state store itself, but
  see below before removing them.
- Key material is held in zeroizing buffers and never logged.

Audit-file lines and the anchor file are authenticated with an HMAC key
derived from the state key that was active when they were written; each line
and the anchor record that key's id (`kid`), and the key id is covered by the
HMAC. Audit verification and anchor checks after `rotate-keys` therefore work
as long as the older keys remain configured (see [Audit](audit.md) and
[Key rotation](../operations/key-rotation.md)).

### Durability and exclusive ownership

The redb backend commits with fsync. A state directory is owned by one
running engine at a time, enforced by an OS file lock (`<dir>/LOCK`) plus
redb's own lock; a second opener fails with `ownership_conflict`. Custom
backends declare whether they are durable and exclusive; a non-durable
backend is refused outside the development profile, and a non-exclusive one
produces a warning that the host must enforce ownership.

## Rollback and substitution detection

Encryption alone does not detect a state directory restored from an older
backup. The engine therefore keeps a **state epoch**:

- The epoch increases with every committed state write.
- Every graph batch records the state epoch in the pipeline marker node in
  the graph target, in the same transaction.
- At startup, before acquiring the fence (and again after it), if the graph
  marker's epoch is greater than the local epoch the engine fails with
  `state_rollback`. Because the check precedes fencing, an instance with
  stale state cannot fence out a healthy owner.
- Optionally, `state.anchor_file` names an external anchor written
  atomically (temporary file, fsync, rename) containing
  `store_id:epoch:kid:mac`, where the HMAC covers the store id, epoch and key
  id; it still verifies after key rotation while the old key is configured.
  It is checked before fencing too. An anchor whose epoch is newer than the
  store fails with `state_rollback`; an anchor for a different store id also
  fails with `state_rollback` (after a state reset with a new, empty store,
  move the old anchor aside); an anchor whose HMAC does not verify fails
  with `state_tampered`. Place the anchor on storage that is **not** restored
  together with the state directory.
- Checkpoint contiguity is enforced: advancing past a missing journal entry
  is an error, and no partial progress is recorded.

Recovery from a legitimate state loss or restore is an explicit, audited
operator action: `db2graph-sync recover --accept-state-reset --reason "<why>"`
with the engine stopped.

Storage exhaustion surfaces as a terminal `state_io` error from the backend;
this has not been separately fault-tested on a full disk.

## Limiting what leaves the source

Data minimization is applied **before** anything is written to the journal,
so denied data is never persisted locally or sent to the graph.

### Dataset allowlist

`policy.datasets` lists the relations that may be captured (required outside
the development profile). Unlisted relations are dropped during
normalization.

### Field allow and deny lists

```toml
[[policy.datasets]]
relation = "public.customers"
allow_fields = ["id", "name", "segment", "notes"]
deny_fields = ["ssn"]
embed_fields = ["name", "notes"]
tenant_column = "tenant_id"
labels = { classification = "CONFIDENTIAL" }
field_labels = { notes = { cui_categories = ["PRVCY"] } }
```

- When `allow_fields` is set, only those fields are projected.
- `deny_fields` always wins over `allow_fields` (listing a field in both is
  reported as a finding).
- Key, foreign-key, unique-key and tenant columns are retained even when not
  allowed, because they define identity and relationships. They are used to
  compute identities and edges and are projected as properties only when
  allowed.

### Unknown fields

`policy.unknown_fields` (`allow` or `deny`) decides what happens to fields of
a listed dataset that has no `allow_fields` list — including columns added to
the source later. The default is `allow`. In the restricted profile the
default is forced to `deny` and every dataset must have `allow_fields`.

### Row-level tenancy

`tenant_column` restricts a dataset to rows whose tenant equals
`pipeline.tenant`; rows of other tenants are not projected (and are removed
if they move to another tenant). Text, integer, big-integer, decimal and UUID
tenant columns are compared by their canonical text (an integer `42` matches
`tenant = "42"`); `NULL` and other types never match. The tenant is also folded into every entity
identity.

### Policy changes

The policy has a version digest recorded with captured work. `update_policy`
at runtime takes effect immediately for retrieval validation and for newly
captured changes. It does **not** retroactively remove properties already
projected to the graph; run reconciliation and re-bootstrap if a field must
be removed from existing data.

## Labels: classification, CUI, export control, releasability, tenant

Labels are modelled as **independent dimensions**; none implies another:

| Dimension | Configuration | Retrieval rule |
|---|---|---|
| Classification markings | `classification` / `classifications` | principal must hold **every** marking |
| CUI categories | `cui_categories` | entity categories must be a subset of the principal's |
| Export control | `export_control` | every marking must be in the principal's authorizations |
| Releasability | `releasability` | when set, the principal's code must be in the set |
| Tenant | `pipeline.tenant` / `tenant_column` | principal tenant must equal entity tenant |

Dataset labels and field labels combine to the most restrictive set (markings
are unioned, releasability is intersected). Labels are stored with entities
at write time; the retrieval validator uses the union of stored labels and the
current policy's labels, so a label added later still applies.

These labels are metadata the engine enforces at its retrieval boundary
(`RetrievalValidator`, `POST /v1/retrieval/validate`). Queries that go
directly to the graph or vector store bypass that boundary and do not inherit
its guarantees. Marking data in configuration does not by itself make a
deployment suitable for any regulated category of information.

## Embedding protections

Text sent to an embedding provider is built from a template using only fields
that are **both** policy-allowed **and** listed in `embed_fields`. A field is
*protected* when its effective labels include any classification, CUI
category or export-control marking. Protected fields are included only if the
embedding provider is explicitly configured with `allow_protected = true`
**and** the profile is not restricted. In the restricted profile this is
refused at validation, at startup and in the vector worker.

Vector point payloads contain identifiers, version, chunk information,
content digest, model information and labels — not the source row. The
hashing embedder is not semantic and is limited to the development profile.

## Secrets in errors and logs

- Resolved secrets are wrapped in a type whose `Debug`/`Display` output is
  `<redacted>` and whose memory is zeroized on drop.
- `literal:` secret references are redacted in debug output and in evidence
  exports.
- URL-like strings are passed through a redaction function before they appear
  in errors or logs: user-info in URLs and `password`, `sslpassword`,
  `token`, `api_key` and `apikey` query parameters are replaced with
  `<redacted>`.
- Management tokens are configured only as SHA-256 hashes and compared in
  constant time.

Review logs and evidence exports before sharing them anyway: they contain
host names, relation names and identifiers that may be sensitive in your
environment.
