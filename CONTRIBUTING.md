# Contributing to db2graph-sync

db2graph-sync — an open-source project by Adventa. (<https://www.adventa.com.au>)

Thank you for your interest in contributing. This document explains how to set
up a development environment, which checks a change must pass, and what
reviewers expect. Project roles and decision making are described in
[GOVERNANCE.md](GOVERNANCE.md); behaviour in all project spaces is governed by
the [Code of Conduct](CODE_OF_CONDUCT.md).

> **Security issues:** do **not** open a public issue, pull request or
> discussion for a suspected vulnerability. See [SECURITY.md](SECURITY.md).
> A private reporting channel is not configured yet and will be published
> before the first release.

## Licensing of contributions

db2graph-sync is licensed under the [Apache License, Version 2.0](LICENSE).
Contributions are accepted under the same license ("inbound = outbound"), as
described in section 5 of the license: unless you explicitly state otherwise,
any contribution intentionally submitted for inclusion in the project is
licensed under Apache-2.0 without additional terms or conditions.

No contributor license agreement (CLA) and no Developer Certificate of Origin
(DCO) sign-off is required. Only submit work you have the right to license
under Apache-2.0.

## Development environment

The project has been developed and tested on Windows 11 (x86_64, MSVC) with
Rust 1.99.0. Linux and macOS are intended targets and a CI matrix is defined
for them, but no CI run has been executed yet. Reports from those platforms
are especially welcome.

### Requirements (all platforms)

- **Rust >= 1.90** (the MSRV; `redb` 4.x requires it), installed through
  [rustup](https://rustup.rs). Edition 2024.
  `rustup component add rustfmt clippy`
- **A C compiler**, needed to build `ring` (used by the TLS stack).
- **Docker** (with `docker compose`), only for the real-service integration
  tests.
- **cargo-deny** for dependency/license checks: `cargo install cargo-deny`.
- **mdBook** for the documentation: `cargo install mdbook`.

### Windows

1. Install rustup from <https://rustup.rs> and accept the default
   `x86_64-pc-windows-msvc` toolchain.
2. Install the Visual Studio Build Tools with the "Desktop development with
   C++" workload (provides the MSVC C compiler used by `ring`).
3. Install Docker Desktop if you will run the integration tests.
4. Use PowerShell 7+ for `scripts/integration.ps1`.

### Linux

1. `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
2. Install a C toolchain, for example `build-essential` (Debian/Ubuntu) or
   `gcc` + `make` (Fedora/RHEL).
3. Install Docker Engine and the Compose plugin for integration tests.

### macOS

1. Install rustup as on Linux.
2. `xcode-select --install` (provides `clang`).
3. Install Docker Desktop (or another Docker-compatible runtime that provides
   `docker compose`) for integration tests.

See [DEVELOPMENT.md](DEVELOPMENT.md) for the workspace layout, cargo features
and the test layers.

## Checks every change must pass

Run these from the repository root:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --features "postgres neo4j vector-qdrant vector-pinecone embedding-http service" -- -D warnings
cargo test --workspace
cargo test -p db2graph-sync --no-default-features
cargo deny check
mdbook build docs
```

`cargo deny check` uses the policy in `deny.toml` at the repository root
(licenses, advisories, bans and sources).

### Real-service integration tests

Changes that touch the PostgreSQL connector, the Neo4j sink, the Qdrant sink or
shared delivery semantics must also run the integration tests against real
services:

```powershell
# Windows
./scripts/integration.ps1
```

```sh
# Linux / macOS
./scripts/integration.sh
```

The scripts start the services from `deploy/test/docker-compose.yml`
(test-only credentials, synthetic data) and run the integration test targets.
A test whose services are not configured prints a line starting with
`UNEXECUTED`; the scripts **fail if any such line appears**, so missing
services can never be mistaken for passing coverage. Do not describe a change
as integration-tested unless the script exited successfully with zero
`UNEXECUTED` lines, and say so in the pull request.

## Review expectations

Reviewers will look for the following. The pull request template repeats them
as a checklist.

- **Tests for behaviour.** New behaviour and bug fixes come with tests at the
  appropriate layer (unit, engine acceptance, conformance, integration, CLI).
  A bug fix should include a test that fails without the fix.
- **Documentation in the same pull request.** User-visible changes update the
  relevant pages under `docs/src`, the CLI help and examples as needed.
- **Status documents kept honest.** When a capability is added, removed or its
  qualification changes, update `IMPLEMENTATION_STATUS.md` and
  `LIMITATIONS.md` in the same pull request.
- **No claims without evidence.** Do not describe something as supported,
  tested, qualified, validated, certified or performant unless the pull
  request contains or links the evidence (test output, integration log,
  benchmark method and results). "Implemented but not qualified" is an
  acceptable and useful state; say so plainly. The same applies to code: a
  sink's capability descriptor reports `qualified: true` only after its
  live-server qualification suite has run for that release.
- **CHANGELOG.** Add an entry under `## [Unreleased]` in
  [CHANGELOG.md](CHANGELOG.md) for user-visible changes.
- **Security-sensitive areas** (crypto, state, secrets, policy, management
  API, audit) need review by a maintainer responsible for security; see
  [MAINTAINERS.md](MAINTAINERS.md).
- **Small, focused pull requests** are reviewed faster than large ones.

## Data and secrets

- Use **synthetic data only** in tests, fixtures, examples, issues and pull
  requests. Never include production data, personal data or customer data.
- Never commit real credentials, tokens, keys or connection strings. The
  credentials in `deploy/test/docker-compose.yml` and the integration scripts
  are test-only and must never be reused elsewhere.
- When attaching status output or evidence exports, redact secrets and any
  sensitive identifiers.

## Proposing larger changes

- **New source connectors or graph/vector sinks:** follow the adapter proposal
  process in [GOVERNANCE.md](GOVERNANCE.md#adapter-proposals) and open an
  issue with the "Adapter proposal" template before writing substantial code.
- **Consequential changes** (delivery semantics, state format, public API,
  security model, configuration compatibility): follow the RFC process in
  [GOVERNANCE.md](GOVERNANCE.md#rfcs).
- **Planned work** is listed in [ROADMAP.md](ROADMAP.md). Nothing listed there
  is implemented.

## Getting help

See [SUPPORT.md](SUPPORT.md).
