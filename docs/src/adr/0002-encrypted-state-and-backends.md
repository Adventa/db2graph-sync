# 0002 — Encrypted durable state over a pluggable backend

Status: Accepted
Date: 2026-10-05

## Context

Captured work, checkpoints, acknowledged positions, snapshot progress, schema
history and the audit spool must survive crashes, must be encrypted, must
detect tampering and rollback to an older valid copy, and must not require an
external coordinator for single-instance operation.

## Decision

* `StateStore` owns the semantics: authenticated encryption of every value
  (AES-256-GCM through the `CryptoProvider` trait), AAD binding each record to
  `<store_id>/<table>/<key>` plus the envelope header (key id), a per-key seal
  counter with a hard limit (2^31) that forces rotation, a monotonic **epoch**
  incremented by every commit, journal/checkpoint contiguity, and anchors.
* `StateBackend` is a minimal ordered KV contract with atomic, durable batch
  commits over four tables. Backends see only ciphertext.
* The default backend is [redb](https://www.redb.org) 4.x (copy-on-write
  B-trees, fsync on commit), guarded by an OS file lock on `<dir>/LOCK` and
  redb's own lock.
* Rollback evidence: the graph pipeline marker records the state epoch of
  every committed batch; an optional anchor file (`store_id:epoch:kid:mac`,
  HMAC-authenticated including the key id, atomic rename) records the epoch
  after checkpoints. A local epoch lower than either is `state_rollback` and
  needs authorized recovery. The engine checks this evidence before acquiring
  the graph fence (and re-checks the marker after), so stale state cannot
  fence out a healthy owner.
* Facts that must be atomic with graph effects (receipts, tombstones, pending
  relationships, vector work) are **not** stored here (see ADR 0004).

## Consequences

* Restoring an authentic but older copy of the state directory is detected,
  not silently accepted.
* A lost state directory is also detected (fresh epoch < recorded epoch); the
  documented recovery accepts the graph's evidence and replays journaled work
  idempotently; a snapshot taken afterwards uses a new generation so it does
  not collide with earlier snapshot receipts.
* redb requires Rust 1.90, which sets the MSRV.
* Random 96-bit nonces bound the number of seals per key; the counter and
  `rotate-keys` keep usage far below the recommended limit.

## Evidence

Unit tests in `src/state/mod.rs`: contiguity and restart, checkpoint gap,
ownership conflict, wrong key, rollback via anchor, record substitution,
byte corruption, audit spool limit, key rotation,
`anchor_survives_key_rotation`. Engine tests
`state_loss_is_detected_as_rollback` and
`stale_instance_cannot_fence_out_healthy_owner`.
