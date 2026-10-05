# Support

db2graph-sync — an open-source project by Adventa. (<https://www.adventa.com.au>)

## Community support

Community support is provided on a best-effort basis through issues and
discussions on the project repository. The repository location is TBD —
publication blocker (see IMPLEMENTATION_STATUS.md). No response-time
commitments are made.

Before asking, please check:

- the documentation (`docs/`, built with `mdbook build docs`);
- `IMPLEMENTATION_STATUS.md` and `LIMITATIONS.md` — many questions are
  answered by what is implemented, qualified or planned;
- existing issues.

**Security vulnerabilities must not be reported in issues or discussions.**
See [SECURITY.md](SECURITY.md). A private reporting channel is not
configured yet and will be published before the first release.

## What to include in a report

Use the bug report template. It asks for:

- **Versions:** output of `db2graph-sync about`, the crate version if you embed
  the library, Rust version (`rustc -V`), operating system and architecture,
  enabled cargo features, and the versions of PostgreSQL, Neo4j and any
  vector store or embedding service involved.
- **Profile and configuration:** the security profile and a minimal
  configuration that reproduces the problem, with every secret reference
  value, host name and credential you consider sensitive redacted.
- **Status JSON:** output of `db2graph-sync status --url ... --token-file ...`
  (or `SyncHandle::status()` serialized) at the time of the problem,
  including `phase`, `failure`, `pause_reason`, `warnings` and `backlog`.
- **Evidence export:** `db2graph-sync evidence --config <file> --out evidence.json` where
  possible. Review it before attaching it: redact secrets, internal host
  names and any data values you are not permitted to share.
- **Error kind and exit code** (for example `ownership_conflict`, exit 6).
- **Logs** around the failure (`run --log-format json` is easiest to filter),
  with secrets and sensitive data removed.
- **Reproduction steps** using synthetic data only.

Never attach production data, personal data, real credentials, state
directories or encryption keys.

## Commercial support

Commercial support, if any, is offered separately by Adventa; contact details
TBD.
