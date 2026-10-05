# 0005 — Explicit rustls `ring` provider; restricted profile fails closed

Status: Accepted
Date: 2026-10-05

## Context

TLS must always verify certificates and host names with no plaintext
fallback. An embedded library must not change process-global state, and
rustls 0.23 panics (or requires a global `install_default`) when more than one
crypto provider is compiled in and the implicit builder is used. The
restricted profile needs a FIPS 140-3 validated module for every relevant
crypto path, and a cargo feature is not validation.

## Decision

* Every TLS client we construct uses `ClientConfig::builder_with_provider`
  with `rustls::crypto::ring::default_provider()`; we never call
  `install_default`. reqwest uses `use_preconfigured_tls`.
* `neo4rs` 0.8 enables rustls with `ring` and uses the implicit builder, so
  the dependency graph is kept single-provider (`ring`): verified with
  `cargo tree -i aws-lc-rs` (no match).
* Durable-state crypto goes through the `CryptoProvider` trait. The bundled
  provider (RustCrypto AES-256-GCM/SHA-256/HMAC) runs known-answer tests at
  start and reports `NotValidated`.
* The restricted profile refuses to start unless the provider reports
  `Fips140_3 { module, certificate }` **and** both source and graph are custom
  components supplied in code, because the built-in adapters' TLS (ring) is
  not a validated module. The error is `restricted_provider_unavailable`.

## Consequences

* No validated configuration is bundled in this release; restricted operation
  is blocked, not simulated.
* Integrating AWS-LC FIPS would require replacing the Neo4j driver's TLS
  stack (it pins `ring`) and a build toolchain (CMake, Go) that was not
  available; recorded as a blocker.
* A provider claiming validation is an integrator assertion, recorded in
  status, audit and evidence so it can be checked.
