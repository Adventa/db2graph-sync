# Releasing

The full release procedure is in `RELEASING.md` at the repository root. It
covers the version bump, changelog, MSRV check, `cargo deny`, the full test
matrix including the real-service integration scripts (with zero
`UNEXECUTED` tests), packaging and clean-consumer verification, documentation
build, SBOM, checksums, signatures and provenance where configured, tagging,
the GitHub release, crates.io publication (`db2graph-sync`, then
`db2graph-sync-cli`), docs.rs, and the container image.

**No release has been made.** None of the release steps has been performed:
no CI run, no published crate, no container image, no SBOM, no signatures.

## Publication blockers

The following must be confirmed by Adventa before the first public release.
Until then each appears in the project documents as "TBD — publication
blocker (see IMPLEMENTATION_STATUS.md)".

| Blocker | Needed for |
|---|---|
| Verified, owner-controlled GitHub organization and repository | `repository` field in `Cargo.toml`, issue and discussion links, GitHub releases |
| crates.io ownership of `db2graph-sync` and `db2graph-sync-cli` | publishing the crates with owner-controlled credentials |
| Private vulnerability reporting channel | `SECURITY.md`; until it exists, vulnerabilities must not be reported publicly |
| Code of Conduct enforcement contact | `CODE_OF_CONDUCT.md` |
| Maintainers list | `MAINTAINERS.md`, review and release responsibilities |
| Container registry namespace | publishing the container image built from the `Dockerfile` |
| Documentation hosting | publishing the mdBook site |
| Signing keys (and provenance tooling) | signed tags, signed artifacts and provenance; until available, releases would be unsigned |
