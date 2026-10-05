# Audit

db2graph-sync records security- and operations-relevant events as audit
records, stores them durably before forwarding them, and can write them to a
tamper-evident file.

## Records

Each record is an `AuditRecord`:

| Field | Content |
|---|---|
| `time` | RFC 3339 timestamp (UTC, microseconds) |
| `category` | `sync`, `policy`, `operation`, `failure`, `recovery` or `security` |
| `action` | event name, for example `start`, `batch_applied` |
| `detail` | JSON object with event-specific data |

Recorded events include: engine start (pipeline, profile, crypto provider and
its claimed validation, delivery guarantee); source and graph preflight
reports; fence acquisition; schema versions; the mapping plan and policy;
waits for mapping approval; pause and resume; each applied batch
(`batch_applied`, with the units' expected digests, duplicates, fence token
and state epoch); snapshot validation; generation activation; consistency
incidents; failures; and authorized recovery (with the operator's reason).

Records describe *what the engine did*; they do not contain source row
values.

## Spool and forwarding

1. A record is first written to the **audit spool** inside the encrypted
   state store, with the same durability as the journal.
2. A background forwarder delivers spooled records, in order and in batches,
   to every configured `AuditSink`. A sink's `deliver` must be durable when it
   returns `Ok`; only then are the records removed from the spool.
3. If forwarding fails, records stay in the spool and are retried. The spool
   size is visible as `backlog.audit_spool` in status.

Audit sinks are the built-in file sink (`audit.file`) plus any sinks passed in
`StartOptions::audit_sinks` (for example a forwarder to a SIEM). Each sink
declares whether its delivery is authenticated end to end.

### Spool limit behaviour

`audit.spool_max_records` (default 100000) bounds the spool:

- **With at least one audit sink configured:** when the spool is full —
  typically because forwarding has been failing — further protected work
  stops with `audit_unavailable` rather than proceeding unaudited. Restore
  the audit destination and restart: at start the engine first drains the
  spool to its sinks before writing its own start record, so a spool filled
  during an outage does not block the restart once the destination accepts
  records again. If the destination is still down and the spool is still
  full, the start fails closed with `audit_unavailable`.
- **Without any audit sink:** the spool is a bounded local retention buffer.
  When it is full the **oldest** record is dropped to make room. Records kept
  only in the spool are therefore not a complete audit trail. The restricted
  profile requires `audit.file` for this reason; configure an audit
  destination in every production deployment.

## Tamper-evident audit file

When `audit.file` is set, the `FileAuditSink` appends one JSON line per
record, each carrying:

- `prev` — the MAC of the previous line (`"genesis"` for the first);
- `kid` — the id of the state key whose derived MAC key produced `mac`;
- `mac` — HMAC-SHA-256 over `prev`, `kid` and the record body.

Each write is appended and synced to disk. A new engine instance continues the
existing chain.

The MAC key is derived (domain-separated) from the state encryption key that
is **active when the line is written**, so the chain cannot be recomputed
without the state keys. Because each line names its key id and the key id is
bound into the HMAC, verification uses the key each line was written with.

### Verification

```sh
db2graph-sync audit-verify --config <file> [--file <audit.jsonl>]
```

`audit-verify` recomputes the chain and reports the number of verified
records. It detects edited records, reordered or deleted lines in the middle
of the file, and malformed lines; failures are reported as `state_tampered`
(exit code 5). The evidence export includes an audit-chain verification
result when `audit.file` is set.

Limitations to plan for:

- **Truncation of the tail** (removing the most recent lines) leaves a valid
  shorter chain and is not detectable from the file alone. Forward records to
  an independent, append-only destination (another `AuditSink`) and compare
  record counts if this matters.
- **Key rotation:** after `rotate-keys` changes the active key, the chain
  continues under the new key and lines written under the previous key still
  verify, as long as the previous key remains configured in `state.keys`
  (unit test `chain_survives_key_rotation`). Removing an old key from the
  configuration makes the lines written under it unverifiable with that
  configuration; keep the keys needed for the files you retain.
- The file is integrity-protected, not encrypted. Protect it with filesystem
  permissions and place it on storage separate from the state directory.

## Expected versus verified readback hashes

Two different kinds of hash appear in audit records and evidence and must not
be confused:

- **Expected destination hash** — the receipt digest of a unit the engine
  *intended* to apply: a content digest over the unit id and the captured,
  policy-filtered unit content (items, base version, kind), not over the
  planned graph operations. It is stored in the graph receipt and in the
  `batch_applied` audit record. It identifies what was submitted in a
  committed batch, not what is in the graph now.
- **Verified readback hash** — computed by `reconcile` (and optionally the
  evidence export with `--sample N`) by reading the *current* graph entity
  back, hashing its properties, and comparing with the hash of the
  policy-filtered *current* source row. A match is evidence that the graph
  currently reflects the source for that sampled entity.

Reconciliation is read-only for both systems and samples a bounded number of
entities per relation; it is evidence, not proof of the whole dataset.

## Retention guidance

db2graph-sync does not delete or rotate audit files. Set retention to meet
your organization's requirements:

- Rotate the audit file by stopping the engine (or pointing `audit.file` at a
  new path), verifying the old file with `audit-verify`, recording the
  verification result and a checksum, and archiving it to write-once storage.
- Keep the state keys needed to verify archived files for as long as the
  files are retained, managed in your secret store.
- Forward records to a central audit system through an additional
  `AuditSink` where one exists.
- Monitor `backlog.audit_spool` and alert well before
  `audit.spool_max_records` is reached.
