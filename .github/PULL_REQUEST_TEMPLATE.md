<!--
Thank you for contributing to db2graph-sync. Contributions are accepted under
Apache-2.0 (inbound = outbound, section 5 of the license); no CLA or DCO is
required. Do not use pull requests to disclose vulnerabilities; see SECURITY.md.
-->

## Summary

<!-- What does this change do, and why? Link related issues or RFCs. -->

## Evidence

<!--
How was this verified? Paste or link test output. If you ran the real-service
integration tests, state the script used and confirm it reported zero
UNEXECUTED lines. If you did not run them, say so.
-->

## Checklist

- [ ] Tests added or updated for the changed behaviour (unit / engine acceptance / conformance / integration / CLI as appropriate)
- [ ] `cargo fmt --all --check`, `cargo clippy ... -D warnings`, `cargo test --workspace` and `cargo test -p db2graph-sync --no-default-features` pass
- [ ] Integration scripts run with zero `UNEXECUTED` lines, or not applicable / not run (stated above)
- [ ] `cargo deny check` passes (for dependency changes)
- [ ] Documentation updated in this pull request (`docs/src`, examples, CLI help)
- [ ] `IMPLEMENTATION_STATUS.md` and `LIMITATIONS.md` updated if a capability or its qualification changed
- [ ] `CHANGELOG.md` entry added under `[Unreleased]` for user-visible changes
- [ ] No secrets, credentials, real or personal data in code, tests, fixtures or logs (synthetic data only)
- [ ] Every claim (supported, tested, qualified, validated, performance) is backed by evidence in this pull request
