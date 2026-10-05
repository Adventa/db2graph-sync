# Releasing db2graph-sync

db2graph-sync — an open-source project by Adventa. (<https://www.adventa.com.au>)

> **Status: no release has been made.** None of the steps below has been
> performed yet. No CI run has executed, no package has been published to
> crates.io, no container image has been built or pushed, no SBOM has been
> generated and no artifact has been signed. Several steps depend on
> publication blockers listed in
> [docs/src/project/releasing.md](docs/src/project/releasing.md) and
> `IMPLEMENTATION_STATUS.md`.

Releases are performed by maintainers responsible for release engineering
(see [MAINTAINERS.md](MAINTAINERS.md)) using owner-controlled credentials held
by Adventa. Credentials are never stored in the repository or in personal
accounts outside Adventa's control.

## 1. Prepare

1. Decide the version according to the version policy in
   [GOVERNANCE.md](GOVERNANCE.md#version-policy) (SemVer; `0.x` minor
   releases may break compatibility with changelog notes; an MSRV increase is
   a minor-version event).
2. Bump `version` under `[workspace.package]` in the root `Cargo.toml`. Both
   crates inherit it. Update the `db2graph-sync` dependency version used by
   `db2graph-sync-cli` if it is pinned. Run `cargo update -w` to refresh
   `Cargo.lock`.
3. Update [CHANGELOG.md](CHANGELOG.md): move entries from `[Unreleased]` to a
   new version heading with the release date; call out breaking changes,
   MSRV changes, state-format or graph-model changes, and security fixes.
4. Confirm `IMPLEMENTATION_STATUS.md` and `LIMITATIONS.md` describe the
   release accurately (implemented / qualified / experimental / planned).

## 2. Verify

All of the following must pass on the release commit:

1. **Formatting and lints**
   ```sh
   cargo fmt --all --check
   cargo clippy --workspace --all-targets --features "postgres neo4j vector-qdrant vector-pinecone embedding-http service" -- -D warnings
   ```
2. **MSRV check** with the declared `rust-version` (1.90):
   ```sh
   rustup toolchain install 1.90
   cargo +1.90 check --workspace --all-targets
   ```
3. **Dependency policy:** `cargo deny check` (policy in `deny.toml`).
4. **Full test matrix** on every supported platform:
   ```sh
   cargo test --workspace
   cargo test -p db2graph-sync --no-default-features
   cargo test -p db2graph-sync --features "postgres neo4j vector-qdrant vector-pinecone embedding-http service"
   ```
5. **Real-service integration tests:** `scripts/integration.ps1` or
   `scripts/integration.sh` must exit successfully with **zero `UNEXECUTED`
   lines**. Keep `target/integration.log` as release evidence.
6. **Runnable examples** (no services required):
   ```sh
   cargo run -p db2graph-sync --example host_lifecycle
   cargo run -p db2graph-sync --example custom_graph_sink
   cargo run -p db2graph-sync --example custom_state_backend
   cargo run -p db2graph-sync --example restart_replay
   cargo run -p db2graph-sync --example policy_filtering
   cargo run -p db2graph-sync --example composite_keys
   ```
7. **Packaging:**
   ```sh
   cargo package -p db2graph-sync
   cargo package -p db2graph-sync-cli
   ```
   Then verify the packaged library from a clean consumer project outside the
   workspace using `scripts/clean-consumer.ps1` (Windows) or
   `scripts/clean-consumer.sh` (Linux/macOS). These scripts build a fresh
   project that depends only on the packaged crate.
8. **Documentation:** `mdbook build docs` and
   `cargo doc -p db2graph-sync --no-deps --all-features` build without
   warnings.

## 3. Produce release artifacts

1. **SBOM:** generate a CycloneDX SBOM for each crate, for example with
   `cargo install cargo-cyclonedx` and `cargo cyclonedx --all`. Attach the
   SBOM files to the release.
2. **Binaries:** build `db2graph-sync` (the CLI) in release mode for each
   target platform.
3. **Checksums:** produce SHA-256 checksums for every attached artifact
   (binaries, `.crate` files, SBOMs).
4. **Provenance and signatures, where configured:** sign artifacts and
   produce build provenance using keys and tooling controlled by Adventa.
   Signing keys are a publication blocker; until they exist, releases are
   unsigned and must be described as unsigned.
5. **Container image:** build the image from the `Dockerfile`, tag it with the
   release version, and push it to a verified registry namespace controlled by
   Adventa (namespace: TBD — publication blocker (see IMPLEMENTATION_STATUS.md)).
   Record the image digest in the release notes. Note that embedded (library)
   use does not inherit any container hardening.

## 4. Publish

1. **Tag** the release commit: `git tag -s vX.Y.Z` (signed where signing is
   configured, otherwise annotated: `git tag -a vX.Y.Z`), and push the tag.
2. **GitHub release** on the verified, owner-controlled repository
   (location: TBD — publication blocker (see IMPLEMENTATION_STATUS.md)):
   release notes from the changelog, artifacts, checksums, SBOMs, signatures
   and provenance where available, the integration log summary, and the
   container image digest.
3. **crates.io**, in dependency order, with owner-controlled credentials:
   1. `cargo publish -p db2graph-sync`
   2. wait until the new version is available in the index;
   3. `cargo publish -p db2graph-sync-cli`

   Publishing requires a verified crates.io ownership/namespace controlled by
   Adventa. This is a publication blocker until confirmed. The `repository`
   field is intentionally absent from `Cargo.toml` until the repository
   location is verified.
4. **docs.rs:** confirm the documentation build for the new version succeeded.
5. **Documentation site:** publish the mdBook output to the documentation
   host (TBD — publication blocker (see IMPLEMENTATION_STATUS.md)).

## 5. After the release

1. Open a new `## [Unreleased]` section in the changelog.
2. Announce the release on the project repository.
3. If a security fix is included, follow the disclosure steps in
   [SECURITY.md](SECURITY.md).
4. If a release is found to be broken, yank it on crates.io
   (`cargo yank --version X.Y.Z`), note it in the changelog, and publish a fix.
