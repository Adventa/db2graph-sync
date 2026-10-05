# Feature flags

## Library crate `db2graph-sync`

| Feature | Default | Enables | Pulls in |
|---|---|---|---|
| `postgres` | yes | PostgreSQL source (`source::postgres`) | `tokio-postgres` 0.7, `postgres-protocol` 0.6, `tokio-postgres-rustls` (ring), plus internal `tls` |
| `neo4j` | yes | Neo4j graph sink (`sink::neo4j`) | `neo4rs` 0.8.0 |
| `vector-qdrant` | no | Qdrant vector sink (`vector::qdrant`); `bootstrap` creates the collection | internal `http-client` |
| `vector-pinecone` | no | Pinecone vector sink (experimental) | internal `http-client` |
| `embedding-http` | no | OpenAI-compatible HTTP embedder (experimental) | internal `http-client` |
| `service` | no | `management::router` (axum `Router`); the library never binds a port | `axum` 0.8, `tower` 0.5 |
| `tls` (internal) | via the above | rustls TLS helpers | `rustls` 0.23 (`ring`), `tokio-rustls`, `rustls-native-certs` |
| `http-client` (internal) | via the above | HTTP client | `reqwest` 0.12 (rustls, no default provider), plus `tls` |

`tls` and `http-client` are helpers that other features enable. Don't enable
them directly.

**Always included:** the core engine, in-memory graph sink, scripted source,
redb state backend, the RustCrypto provider (AES-256-GCM, SHA-256,
HMAC-SHA-256), policy, mapping, audit and the retrieval validator.

If a config selects a component whose feature isn't compiled, it fails with
`feature_not_compiled` (exit 8).

There is **no** restricted-provider or FIPS feature. No FIPS 140-3 validated
provider is bundled, and `aws-lc-rs` FIPS is not integrated. See
[Restricted profile](../security/restricted.md).

## Combinations tested in this release

| Combination | Command |
|---|---|
| Minimal core | `cargo build -p db2graph-sync --no-default-features` |
| Default | `cargo build -p db2graph-sync` |
| Full compatible set | `cargo build -p db2graph-sync --features "postgres neo4j vector-qdrant vector-pinecone embedding-http service"` |

Tested on Windows 11 x86_64 (MSVC) only.

## CLI crate `db2graph-sync-cli`

| Feature | Default | Maps to |
|---|---|---|
| `postgres` | yes | `db2graph-sync/postgres` |
| `neo4j` | yes | `db2graph-sync/neo4j` |
| `vector-qdrant` | yes | `db2graph-sync/vector-qdrant` |
| `vector-pinecone` | yes | `db2graph-sync/vector-pinecone` |
| `embedding-http` | yes | `db2graph-sync/embedding-http` |

The CLI always enables the library's `service` feature. It always depends on
rustls with `ring`, so it always needs a C toolchain.

## docs.rs

`package.metadata.docs.rs` builds with
`postgres neo4j vector-qdrant vector-pinecone embedding-http service`.
Publication to docs.rs is pending (crate unpublished).
