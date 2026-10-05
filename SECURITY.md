# Security policy

db2graph-sync — an open-source project by Adventa. <https://www.adventa.com.au>

## Reporting a vulnerability

**A private vulnerability-reporting channel has not been configured yet.**
This is a publication blocker: it will be published here (for example GitHub
private vulnerability reporting on the verified project repository) before the
first public release.

Until then:

* **Do not** open a public issue, discussion or pull request describing a
  suspected vulnerability.
* If you obtained this code directly from Adventa, report through the private
  contact you received it from.

No response-time commitment is made at this stage.

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x (unreleased) | Pre-release; fixes land on `main` |

## Scope and current security posture

Read [docs/src/security/threat-model.md](docs/src/security/threat-model.md),
[docs/src/security/profiles.md](docs/src/security/profiles.md) and
[LIMITATIONS.md](LIMITATIONS.md). Key facts:

* TLS certificate and host-name verification are always on; there is no
  option to disable them and no plaintext fallback (plaintext exists only as
  an explicit development-profile exception).
* Durable state is encrypted and authenticated (AES-256-GCM); tampering and
  rollback to older valid copies are detected and stop synchronization.
* The bundled cryptographic provider is **not** FIPS 140-3 validated. The
  restricted profile fails closed (`restricted_provider_unavailable`) rather
  than pretending otherwise. No DoD certification, IL4 authorization or
  CAC/PIV support is claimed.
* Dependency policy is enforced with `cargo deny` (`deny.toml`); four
  *unmaintained* (not vulnerable) transitive crates via `neo4rs` 0.8.0 are
  explicitly acknowledged there.
