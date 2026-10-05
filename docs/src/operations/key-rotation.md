# Key rotation

## What the state keys protect

`state.keys` lists data-encryption keys; `state.active_key` selects the key
for new writes. Each key is 32 bytes, base64-encoded, and resolved through a
secret reference:

```toml
[state]
dir = "/var/lib/db2graph-sync/shop"
active_key = 2
keys = [
  { id = 1, key = "file:/run/secrets/d2g_state_key_1" },
  { id = 2, key = "file:/run/secrets/d2g_state_key_2" },
]
```

The active key is used for:

- sealing every state record (AES-256-GCM; the key id is stored in each
  record's envelope, so records sealed with older keys stay readable while those
  keys are configured);
- the HMAC of each audit file line (the line records the key id `kid`, which
  is bound into the HMAC);
- the HMAC of the anchor file (`store_id:epoch:kid:mac`; the key id is
  likewise authenticated).

Because audit lines and the anchor name the key that authenticated them, they
are verified with that key, not with whichever key is active now.

Each key has a seal counter in the state store. When the active key would
exceed 2^31 seals, writes fail with a `security` error asking for rotation.
The counter is per key id, so switching to a new key resets it.

## Procedure

1. **Generate a key**: `db2graph-sync keygen` prints 32 random bytes as base64.
   Store it in your secret store; never in the configuration file outside
   development.
2. **Add it** to `state.keys` with a new id and set `active_key` to that id.
   Keep the old key in the list.
3. **Stop the engine** (`rotate-keys` needs the exclusive state lock).
4. **Re-encrypt**: `db2graph-sync rotate-keys -c <config>`. Every record in
   the meta, journal, schema history and audit spool tables that is not sealed
   with the active key is re-sealed in one commit. The command prints the
   number of records rewritten.
5. **Anchor file** (only if `state.anchor_file` is set): nothing to do. The
   existing anchor records the old key id and still verifies while the old key
   is configured (unit test `anchor_survives_key_rotation`). `rotate-keys` does
   not rewrite it; the engine rewrites it under the new active key at the next
   checkpoint advance.
6. **Start the engine** and confirm it reaches `streaming`.
7. **Retire the old key from `state.keys`** only after step 4 succeeded and,
   if an anchor file is used, after the engine has rewritten the anchor under
   the new key (at least one checkpoint advance). Keep a copy of the old key in
   the secret store if you need to verify audit file lines it signed:
   `audit-verify` needs every key id that appears in the file (unit test
   `chain_survives_key_rotation`).

Skipping step 4 is possible: new writes use the new key and old records remain
readable as long as the old key stays configured. Records are then re-sealed
only as they are rewritten, and journal entries sealed with the old key remain
until applied.

## Failure modes

| Symptom | Cause |
|---|---|
| `secret`: "state key N is required ... but is not configured", at start or in `audit-verify` | A key id used by stored records, audit lines or the anchor file was removed from `state.keys`. |
| `state_tampered` at start or in `audit-verify` after a key change | A key's value changed under the same id. |
| `secret` at start | A secret reference does not resolve. |
| `security`: "the active state key reached its encryption limit" | Seal counter exhausted; rotate. |

Management API bearer tokens are separate: they are configured as SHA-256
hashes (`db2graph-sync token-hash` reads a token from stdin). Rotate them by
adding the new hash, distributing the new token, and removing the old hash.
