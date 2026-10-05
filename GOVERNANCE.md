# Governance

db2graph-sync — an open-source project by Adventa. (<https://www.adventa.com.au>)

db2graph-sync is maintained by Adventa under the Apache License 2.0. This
document describes, transparently, how decisions are made, who can make them
and how this document itself changes.

Values that are not yet confirmed are marked
"TBD — publication blocker (see IMPLEMENTATION_STATUS.md)". They will be
filled in by Adventa before the first public release.

## Principles

- **Transparency.** Technical decisions are made in public issues, pull
  requests and RFCs on the project repository (location: TBD — publication
  blocker (see IMPLEMENTATION_STATUS.md)). The only exception is the private
  handling of security reports (see [SECURITY.md](SECURITY.md)) and Code of
  Conduct reports.
- **Evidence over assertion.** Capabilities are described as implemented,
  qualified or planned according to the evidence that exists
  (`IMPLEMENTATION_STATUS.md`, `LIMITATIONS.md`).
- **No lock-in.** The project has no mandatory accounts, telemetry, license
  server, or network calls to Adventa. See
  [docs/src/project/licensing.md](docs/src/project/licensing.md).

## Roles

### Contributors

Anyone who opens an issue, comments, reviews, writes documentation or submits
a pull request. Contributions are accepted under Apache-2.0 (inbound =
outbound, section 5 of the license); no CLA or DCO is required. See
[CONTRIBUTING.md](CONTRIBUTING.md).

### Reviewers

Contributors with a record of sustained, high-quality contributions in an area
who are invited by the maintainers to review pull requests in that area.
Reviewers can approve pull requests; their approval counts towards merge
requirements for their area but a maintainer performs the merge.

### Maintainers

Maintainers are appointed by Adventa. They have write access to the
repository, triage issues, merge pull requests, run releases and are
accountable for the areas listed in [MAINTAINERS.md](MAINTAINERS.md). The
current list of maintainers is TBD — publication blocker (see
IMPLEMENTATION_STATUS.md).

Adventa retains final responsibility for the project: the trademark and
branding of Adventa, the release credentials, the repository and package
namespaces, and the appointment of maintainers.

### Becoming a reviewer or maintainer

A maintainer may nominate a contributor in a public issue. The nomination
passes by lazy consensus of the maintainers (see below) and, for maintainers,
confirmation by Adventa. Inactive reviewers or maintainers (no activity for an
extended period) may be moved to emeritus status after being contacted.

## Decision process

### Lazy consensus on pull requests

Most decisions are made on pull requests by lazy consensus:

- A pull request needs approval from at least one maintainer (or a reviewer
  for the affected area plus a maintainer merge) and must pass the required
  checks listed in [CONTRIBUTING.md](CONTRIBUTING.md).
- Changes to security-sensitive areas (crypto, state, secrets, policy,
  management API, audit) require approval from the maintainer responsible for
  security/crypto.
- If anyone raises a reasoned objection, the change waits until the objection
  is resolved or a maintainer decision is recorded in the pull request.
- If maintainers cannot reach consensus, Adventa makes the final decision and
  records the reasoning publicly.

### RFCs

Consequential changes require a written RFC before implementation is merged.
Examples:

- delivery semantics, ordering, fencing or recovery behaviour;
- the durable state format or the graph metadata model;
- public API or configuration changes that break compatibility;
- the security model, security profiles, cryptography or audit format;
- adding, deprecating or removing a supported adapter;
- changes to this governance document.

Process:

1. Open an issue describing the problem and proposed direction.
2. Submit the RFC as a pull request, either as an architecture decision record
   under `docs/src/adr/` (numbered `NNNN-short-title.md`) or under `rfcs/`.
   It states the context, decision, alternatives considered, consequences,
   compatibility and migration impact, security impact, and how the change
   will be tested and evidenced.
3. The RFC stays open for comment for a reasonable period announced in the
   pull request.
4. The RFC is accepted by lazy consensus of the maintainers, or decided by
   Adventa if consensus is not reached. Accepted, rejected and superseded RFCs
   remain in the repository as a record.

## Issue triage

Maintainers and reviewers triage new issues. Triage confirms the issue
template was followed, asks for missing information (versions, status JSON,
redacted evidence export), and applies labels. Issues that appear to describe
a vulnerability are not discussed publicly; the reporter is directed to
[SECURITY.md](SECURITY.md) and the issue may be hidden or locked.

Labels:

| Label | Meaning |
|---|---|
| `needs-triage` | New issue, not yet triaged (applied by issue templates) |
| `bug` | Behaviour contradicts documentation or the delivery contract |
| `enhancement` | New capability or improvement |
| `adapter-proposal` | Proposal for a new source connector or sink |
| `documentation` | Documentation only |
| `needs-info` | Waiting on the reporter |
| `needs-rfc` | Change is consequential; an RFC is required |
| `confirmed` | Reproduced by a maintainer or reviewer |
| `good-first-issue` | Suitable for new contributors |
| `help-wanted` | Maintainers welcome a contribution |
| `area/core`, `area/postgres`, `area/graph`, `area/vector`, `area/security`, `area/cli`, `area/release`, `area/docs` | Affected area |
| `qualification` | Work to qualify an implemented capability against real services |
| `wontfix`, `duplicate` | Closed without change |

No response-time commitments are made. Response times will be documented
here if and when Adventa commits to them.

## Adapter proposals

New source connectors and graph or vector sinks follow this process (use the
"Adapter proposal" issue template):

1. **Capability descriptor.** State the capabilities the adapter will declare
   (for example, for graph sinks: atomic batches, receipts stored in the
   target, guarded versions, fencing, tombstones; for sources: transaction
   boundaries, ordering scope, resumable positions, snapshot consistency) and
   the resulting delivery guarantee. Capabilities that cannot be provided must
   be declared as absent; the engine derives the guarantee from them.
2. **Qualification plan.** Which server versions, deployment variants (for
   example managed services, clusters, replicas) and failure modes will be
   tested, and what evidence will be produced.
3. **Conformance suite.** Graph sinks must pass the executable conformance
   suite (`sink::conformance::run_all`); comparable suites apply to other
   adapter kinds where they exist.
4. **Live-service tests.** Tests against real services, runnable through the
   integration scripts, that report `UNEXECUTED` (and therefore fail the
   script) when services are not available.

An adapter is described as **qualified** only after its live-service tests
have run successfully and the evidence is recorded in
`IMPLEMENTATION_STATUS.md`. Until then it is documented as implemented but
not qualified, or as experimental. Third-party adapters supplied in code
report `qualified: false` and produce a status warning.

## Version policy

- The project follows [Semantic Versioning](https://semver.org).
- While the version is `0.x`, minor releases may contain breaking changes.
  Every breaking change is called out in [CHANGELOG.md](CHANGELOG.md) with
  migration notes.
- Raising the minimum supported Rust version (MSRV, currently 1.90) is a
  minor-version event and is noted in the changelog.
- Changes to the durable state format, the graph metadata model or the
  management API (`/v1`) are treated as compatibility changes and require an
  RFC.

## Security reporting

See [SECURITY.md](SECURITY.md). A private vulnerability reporting channel is
not configured yet; it will be published before the first release. Do not
report vulnerabilities in public issues.

## Releases

Releases are prepared and published by maintainers responsible for release
engineering, using owner-controlled credentials held by Adventa, following
[RELEASING.md](RELEASING.md). No release has been made yet.

## Commercial support

Commercial support, if any, is offered separately by Adventa; contact details
TBD. Commercial arrangements do not change the license, the governance
process, or how contributions are reviewed.

## Changing this document

Changes to this document follow the RFC process above, are made by pull
request, and require approval by Adventa. The history of this file is the
record of governance changes.
