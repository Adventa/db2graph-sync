# Restricted profile

The `restricted` profile is for deployments that require every relevant
cryptographic operation to run in a FIPS 140-3 validated cryptographic module,
default-deny data handling and mandatory audit. This page explains what the
profile enforces, why it **currently cannot start with the built-in
adapters**, and what an integrator would have to supply.

> **No certification or authorization claims.** db2graph-sync is not DoD
> certified, not authorized at any Impact Level (including IL4), contains no
> FIPS 140-3 validated module, and does not implement CAC/PIV. Nothing on this
> page should be read as a statement that a deployment is compliant with any
> regime. Assessment and authorization are the responsibility of the
> deploying organization.

## What the profile requires

In addition to every [enterprise](profiles.md#enterprise) rule (verified TLS,
approved mapping digest, dataset allowlist, authenticated management over
TLS, no plaintext or literal-secret exceptions, durable state backend):

1. **A crypto provider that reports `Validation::Fips140_3 { module,
   certificate }`** and passes its known-answer self-tests at startup.
2. **No built-in network adapters.** Both the source and the graph sink must
   be supplied in code (`StartOptions::source` and
   `StartOptions::graph_sink`). When `[vector]` is configured, the vector sink
   and the embedder must also be supplied in code (`StartOptions::vector_sink`
   and `StartOptions::embedder`); the built-in vector sinks and the HTTP
   embedder are refused.
3. **`audit.file`** must be configured.
4. **`allow_fields` for every dataset**; the unknown-field default is forced
   to `deny`.
5. **No protected plaintext to embedding providers** (`allow_protected` is
   rejected and embedders that allow protected data are refused).

## Current behaviour: startup always fails with the built-in adapters

With this release, a restricted pipeline that uses the built-in PostgreSQL
source or Neo4j sink — which includes every pipeline started through the
`db2graph-sync` CLI — **refuses to start** with error kind
`restricted_provider_unavailable` (CLI exit code 5). Three checks produce it,
in this order:

- the crypto provider does not report a validated module. The default
  `RustCryptoProvider` reports `NotValidated`, and **no validated provider is
  bundled**. The message names the provider and states that no validated
  provider is bundled with this release;
- the built-in source/graph adapters are in use. They use rustls with the
  `ring` crypto provider, which is not a validated module. The message states
  that the built-in adapters' TLS stack is not FIPS 140-3 validated and that
  qualified adapters are required;
- when `[vector]` is configured, a built-in vector sink (Qdrant, Pinecone) or
  the built-in HTTP embedder would be used, i.e. no custom vector sink, or no
  custom embedder, was supplied in code. They use the same TLS stack. (The
  hashing embedder is already rejected outside development by validation.)
  Test: `restricted_profile_refuses_builtin_vector_components`.

This is deliberate: the engine fails closed rather than running a
"restricted" pipeline whose TLS or state encryption is not performed by a
validated module.

Why not simply switch to a validated backend? An `aws-lc-rs` FIPS build has
not been integrated: it requires a build toolchain that was not available,
and the Neo4j driver used (`neo4rs` 0.8) pins `ring`. A validated provider and
validated TLS for the built-in adapters are listed as planned work in
`ROADMAP.md` at the repository root; they are not implemented.

## What an integrator must supply

To run the restricted profile today, an integrator embedding the library must
provide, in code:

1. **A validated `CryptoProvider`.** An implementation whose `aead_seal`,
   `aead_open`, `random`, `sha256` and `hmac_sha256` all run inside a FIPS
   140-3 validated module operating in an approved mode, and whose
   `validation()` returns `Fips140_3 { module, certificate }` naming that
   module and its certificate. Its `self_test()` must fail closed; the shared
   known-answer tests (`crypto::run_known_answer_tests`) can be called in
   addition to the module's own power-on self-tests.
2. **A custom source connector and a custom graph sink** whose network
   connections use TLS implemented by a validated module, with certificate and
   hostname verification. Graph sinks should pass the conformance suite
   (`sink::conformance::run_all`) and declare their capabilities accurately.
3. **Vector components, if used.** The built-in Qdrant, Pinecone and HTTP
   embedding clients use rustls with `ring` and are refused at startup; a
   restricted deployment that indexes vectors must supply its own
   `VectorSink` and `EmbeddingProvider` (with validated TLS) in code.
4. **Secret delivery** through a `SecretProvider` appropriate to the
   environment (see [Secrets](secrets.md)).
5. **A protected audit destination**: `audit.file` on protected storage, and
   optionally additional `AuditSink`s that forward to the organization's
   audit system.

### A cargo feature is not validation

Enabling a cargo feature, linking a library that *has* a FIPS mode, or
setting an environment variable does not make a deployment validated. The
engine never infers validation from build flags. A provider reporting
`Fips140_3` is a claim made **by the integrator**; the engine records the
claimed module name and certificate in the audit log, in status
(`crypto_provider`, `crypto_validation`) and in evidence exports so the claim
can be checked by assessors.

## Evidence expectations

An integrator claiming that a restricted deployment uses validated
cryptography should be able to show, at minimum:

- the module name, version and certificate number, and the security policy
  operating conditions (approved mode, platform) under which it is used;
- that every crypto path — state encryption, audit MACs, TLS for every
  network connection, random generation — goes through that module;
- the evidence export (`db2graph-sync evidence` equivalent via the `admin`
  API) showing the provider name, claimed validation and self-test result;
- conformance and live-service test results for the custom adapters;
- the audit file and a successful `audit-verify` result.

## Human administration: CAC/PIV

Human administration of the management API currently uses bearer tokens with
scopes. **CAC/PIV authentication is not implemented.** The planned approach is
for human administrators to authenticate through a qualified identity
integration (for example an identity-aware proxy or gateway in front of the
management API) rather than through the engine itself; this is planned, not
available. Until then, place the management API behind an access-control
layer your organization has qualified, and keep replication (workload)
credentials separate from human administration.

## Embedded mode does not inherit container hardening

When db2graph-sync runs as a library inside a host application, it runs with
that host's privileges, filesystem access, network access and process
environment. Any hardening applied to a container image of the standalone
binary (non-root user, read-only filesystem, dropped capabilities, network
policy) does **not** apply to embedded use. The host is responsible for
equivalent controls, including protecting the state directory, the anchor
file, the audit file and the secret sources.
