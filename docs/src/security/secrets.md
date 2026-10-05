# Secrets

Configuration never contains secret values directly. Wherever a credential
or key is needed — database passwords, Neo4j password, vector store and
embedding API keys, state encryption keys — the configuration holds a
**secret reference** of the form `<scheme>:<value>`, resolved once at
startup.

## Built-in schemes

| Scheme | Example | Behaviour |
|---|---|---|
| `env:` | `env:D2G_PG_PASSWORD` | Reads the named environment variable. Fails with `secret` if it is not set. |
| `file:` | `file:/run/secrets/pg_password` | Reads the file. A trailing newline is trimmed when the secret is used as text. Fails with `secret` if unreadable. |
| `literal:` | `literal:dev-only-password` | The value itself. **Development only:** permitted only with `profile = "development"` **and** `engine.allow_literal_secrets = true`; otherwise a `security` error. |

Any other scheme fails with `secret` ("no secret provider registered for
scheme ...") unless a custom provider handles it.

The library only **reads** environment variables. It never sets, modifies or
removes them, so embedding it does not change the host process environment.

## Custom secret providers

Hosts can resolve their own schemes — for example a cloud secret manager or a
vault — by implementing `SecretProvider` and passing it in
`StartOptions::secret_provider`:

```rust,ignore
use db2graph_sync::secret::{Secret, SecretProvider, SecretRef};

struct VaultProvider { /* client */ }

#[async_trait::async_trait]
impl SecretProvider for VaultProvider {
    fn schemes(&self) -> &[&'static str] { &["vault"] }
    async fn resolve(&self, r: &SecretRef) -> db2graph_sync::Result<Secret> {
        // fetch r.0 ("vault:...") from your secret manager; never log the value
        todo!()
    }
}
```

The custom provider is consulted first for the schemes it declares; the
built-in provider handles `env:`, `file:` and `literal:`. Implementations
must not log secret values. `Secret` redacts itself in `Debug` output and
zeroizes its memory on drop.

## Recommendations

- **Use a secret manager** (through a custom provider, or files mounted by
  your orchestrator via `file:`) rather than long-lived values in
  environment variables. Environment variables are visible to other code in
  the same process and, on some platforms, to other processes of the same
  user.
- **Prefer short-lived credentials** where the database and services support
  them, and rotate long-lived ones. Secrets are resolved at startup; restart
  the engine (or the embedding host) to pick up rotated credentials.
- **Restrict file permissions** on secret files and on the state directory.
- **State encryption keys** are 32 random bytes, base64-encoded; generate
  them with `db2graph-sync keygen` and store them in your secret manager. See
  [Data protection](data-protection.md) for rotation.
- **Management tokens** are never configured in plain form. Configure the
  SHA-256 hash produced by `db2graph-sync token-hash` (reads the token from
  stdin) and give the token itself only to the operator or system that needs
  it, with the narrowest scope (`read`, `operate`).

## Workload identity versus human administration

Separate the identity the pipeline uses to replicate data from the identities
humans use to administer it:

- **Replication (workload) identity.** A dedicated database role with only
  the `REPLICATION` attribute (or the provider's equivalent) and `SELECT` on
  the published tables; a dedicated graph user limited to the target
  database; dedicated vector and embedding API keys. These credentials belong
  to the workload and are delivered through the secret provider. `db2graph-sync
  source-setup` prints the statements an administrator runs to create the
  publication and grants; the engine never creates roles or publications
  itself.
- **Human administration.** Operators use the management API with scoped
  bearer tokens, or the CLI with their own access to configuration and
  secrets. Human administrators should not reuse the replication
  credentials. Integration with smart-card identities (CAC/PIV) is not
  implemented; see [Restricted profile](restricted.md).

Configuration review should treat every `literal:` reference and every
`engine.allow_*` exception as a finding outside development.
