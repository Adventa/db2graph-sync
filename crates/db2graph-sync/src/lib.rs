//! # db2graph-sync
//!
//! An embeddable, one-way CDC projection engine: it loads approved
//! operational database records and continuously synchronizes their changes
//! into a graph database and, optionally, a vector index for GraphRAG. The
//! source database remains authoritative.
//!
//! *db2graph-sync — an open-source project by Adventa. <https://www.adventa.com.au>*
//!
//! ## One-line startup
//!
//! ```no_run
//! # async fn demo() -> Result<(), db2graph_sync::Error> {
//! let config = db2graph_sync::Config::from_file("db2graph-sync.toml")?;
//! let sync = db2graph_sync::start(config).await?;
//! sync.ready(std::time::Duration::from_secs(60)).await?;
//! // ... application runs ...
//! sync.shutdown(std::time::Duration::from_secs(30)).await?;
//! # Ok(()) }
//! ```
//!
//! `start` runs on the caller's Tokio runtime. It never creates a runtime,
//! installs a logger or signal handler, binds a port, changes environment
//! variables or exits the process. See [`handle`] for exactly what a
//! successful start means, and `docs/` for the delivery contract:
//! **at-least-once transport with effectively-once graph effects under the
//! documented conditions**; graph and vector stores converge asynchronously.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]

pub mod audit;
pub mod capability;
pub mod config;
pub mod crypto;
mod engine;
pub mod error;
pub mod handle;
pub mod identity;
#[cfg(feature = "service")]
pub mod management;
pub mod mapping;
pub mod plan;
pub mod policy;
pub mod reconcile;
pub mod retrieval;
pub mod schema;
pub mod secret;
pub mod sink;
pub mod source;
pub mod state;
#[cfg(feature = "tls")]
pub(crate) mod tls;
pub mod types;
pub mod vector;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::{Arc, RwLock};

use serde::Serialize;
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

pub use config::{Config, Profile};
pub use error::{Error, ErrorKind, Result};
pub use handle::{Control, Phase, ShutdownReport, Status, SyncHandle};

use crate::audit::{AuditSink, FileAuditSink};
use crate::capability::DeliveryGuarantee;
use crate::config::{EmbeddingConfig, GraphConfig, SourceConfig, VectorSinkConfig};
use crate::crypto::{CryptoProvider, Keyring, RustCryptoProvider, Sealer, Validation};
use crate::engine::{AuditPipe, Engine};
use crate::handle::{Backlog, Counters, Freshness, Shared};
use crate::policy::Policy;
use crate::retrieval::RetrievalValidator;
use crate::secret::{BuiltinSecretProvider, SecretProvider, SecretResolver};
use crate::sink::GraphSink;
use crate::source::SourceConnector;
use crate::state::{FaultInjector, StateStore};
use crate::vector::{EmbeddingProvider, HashingEmbedder, VectorRuntime, VectorSink};

/// Attribution and build information (also served at `GET /v1/about`).
#[derive(Debug, Clone, Serialize)]
pub struct About {
    pub name: &'static str,
    pub version: &'static str,
    pub license: &'static str,
    pub project: &'static str,
    pub homepage: &'static str,
    pub features: Vec<&'static str>,
}

/// Attribution and compiled features. Makes no network calls.
pub fn about() -> About {
    let mut features = Vec::new();
    if cfg!(feature = "postgres") {
        features.push("postgres");
    }
    if cfg!(feature = "neo4j") {
        features.push("neo4j");
    }
    if cfg!(feature = "vector-qdrant") {
        features.push("vector-qdrant");
    }
    if cfg!(feature = "vector-pinecone") {
        features.push("vector-pinecone");
    }
    if cfg!(feature = "embedding-http") {
        features.push("embedding-http");
    }
    if cfg!(feature = "service") {
        features.push("service");
    }
    About {
        name: "db2graph-sync",
        version: env!("CARGO_PKG_VERSION"),
        license: "Apache-2.0",
        project: "db2graph-sync — an open-source project by Adventa.",
        homepage: "https://www.adventa.com.au",
        features,
    }
}

/// Components and hooks supplied in code. All optional; anything not
/// supplied is built from configuration.
#[derive(Default)]
pub struct StartOptions {
    pub source: Option<Arc<dyn SourceConnector>>,
    pub graph_sink: Option<Arc<dyn GraphSink>>,
    pub vector_sink: Option<Arc<dyn VectorSink>>,
    pub embedder: Option<Arc<dyn EmbeddingProvider>>,
    pub secret_provider: Option<Arc<dyn SecretProvider>>,
    pub crypto_provider: Option<Arc<dyn CryptoProvider>>,
    pub audit_sinks: Vec<Arc<dyn AuditSink>>,
    /// Custom storage for encrypted durable state (default: redb in `state.dir`).
    pub state_backend: Option<Arc<dyn crate::state::StateBackend>>,
    /// Test-only: inject failures at named boundaries to emulate crashes.
    #[doc(hidden)]
    pub fault_injector: Option<FaultInjector>,
}

/// Start synchronization with components built from `config`.
///
/// This is the one-line integration point:
/// `let sync = db2graph_sync::start(config).await?;`
pub async fn start(config: Config) -> Result<SyncHandle> {
    start_with(config, StartOptions::default()).await
}

/// Start with custom components (sources, sinks, providers).
pub async fn start_with(config: Config, opts: StartOptions) -> Result<SyncHandle> {
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(Error::config("db2graph_sync::start must be called from within a Tokio runtime"));
    }
    let prepared = prepare(&config, opts).await?;
    Ok(launch(config, prepared))
}

/// Everything resolved before the supervisor starts.
pub(crate) struct Prepared {
    provider: Arc<dyn CryptoProvider>,
    sealer: Sealer,
    state: Arc<StateStore>,
    source: Arc<dyn SourceConnector>,
    sink: Arc<dyn GraphSink>,
    vector: Option<VectorRuntime>,
    audit_sinks: Vec<Arc<dyn AuditSink>>,
    faults: Option<FaultInjector>,
}

async fn resolve_keyring(config: &Config, secrets: &SecretResolver) -> Result<Keyring> {
    let mut kr = Keyring::new(config.state.active_key);
    for k in &config.state.keys {
        let s = secrets.resolve(&k.key).await?;
        kr.add_base64(k.id, s.expose_str()?)?;
    }
    Ok(kr)
}

/// Validate, self-test crypto, resolve secrets and open state. Shared by
/// `start` and the CLI's offline commands.
pub(crate) async fn prepare(config: &Config, opts: StartOptions) -> Result<Prepared> {
    let custom_source = opts.source.is_some();
    let custom_sink = opts.graph_sink.is_some();
    let mut findings = config.findings();
    // A `custom` component must actually be supplied, and supplied components
    // satisfy the corresponding config section.
    if matches!(config.source, SourceConfig::Custom(_)) && !custom_source {
        return Err(Error::config("source.type = \"custom\" requires StartOptions::source"));
    }
    if matches!(config.graph, GraphConfig::Custom(_)) && !custom_sink {
        return Err(Error::config("graph.type = \"custom\" requires StartOptions::graph_sink"));
    }
    findings.retain(|f| !(custom_source && f.path.starts_with("source")) && !(custom_sink && f.path.starts_with("graph")));
    if let Some(f) = findings.iter().find(|f| f.kind != ErrorKind::Config).or(findings.first()) {
        let msg = findings.iter().map(|f| format!("{}: {}", f.path, f.message)).collect::<Vec<_>>().join("; ");
        return Err(Error::new(f.kind, msg));
    }

    let provider: Arc<dyn CryptoProvider> = opts.crypto_provider.unwrap_or_else(|| Arc::new(RustCryptoProvider));
    provider.self_test()?;
    if config.profile() == Profile::Restricted {
        match provider.validation() {
            Validation::Fips140_3 { .. } => {}
            Validation::NotValidated => {
                return Err(Error::new(
                    ErrorKind::RestrictedProviderUnavailable,
                    format!(
                        "the restricted profile requires a FIPS 140-3 validated cryptographic module; provider `{}` is not validated. \
                         No validated provider is bundled with this release (see docs/security/restricted.md).",
                        provider.name()
                    ),
                ));
            }
        }
        // Built-in network adapters use rustls with the `ring` provider, which is
        // not a validated module, so they cannot satisfy the restricted profile.
        if !custom_source || !custom_sink {
            return Err(Error::new(
                ErrorKind::RestrictedProviderUnavailable,
                "the built-in source/graph adapters use a TLS stack that is not FIPS 140-3 validated; the restricted profile \
                 requires qualified adapters (none are bundled in this release)",
            ));
        }
        // The built-in vector sinks and HTTP embedder use the same TLS stack.
        if let Some(v) = &config.vector {
            // Components supplied in code replace whatever the config names.
            let builtin_sink = opts.vector_sink.is_none();
            let builtin_embedder = opts.embedder.is_none() && !matches!(v.embedding, EmbeddingConfig::Hashing { .. });
            if builtin_sink || builtin_embedder {
                return Err(Error::new(
                    ErrorKind::RestrictedProviderUnavailable,
                    "the built-in vector sinks and HTTP embedder use a TLS stack that is not FIPS 140-3 validated; the \
                     restricted profile requires custom, qualified vector components supplied in code",
                ));
            }
        }
    }

    let secrets = SecretResolver::new(
        BuiltinSecretProvider { allow_literal: config.profile() == Profile::Development && config.engine.allow_literal_secrets },
        opts.secret_provider.clone(),
    );
    let keyring = Arc::new(resolve_keyring(config, &secrets).await?);
    let sealer = Sealer::new(provider.clone(), keyring);

    let dir = config.state.dir.clone();
    let s2 = sealer.clone();
    let faults = opts.fault_injector.clone();
    let f2 = faults.clone();
    if let Some(b) = &opts.state_backend {
        if !b.durable() && config.profile() != Profile::Development {
            return Err(Error::new(
                ErrorKind::Config,
                format!("state backend `{}` is not durable; only permitted in development", b.name()),
            ));
        }
        if !b.exclusive() {
            tracing::warn!(backend = b.name(), "state backend does not enforce exclusive ownership; the host must");
        }
    }
    let backend = opts.state_backend.clone();
    let state = tokio::task::spawn_blocking(move || match backend {
        Some(b) => StateStore::with_backend(b, s2, f2),
        None => StateStore::open(&dir, s2, f2),
    })
    .await
    .map_err(|e| Error::internal(e.to_string()))??;
    let state = Arc::new(state);

    let source: Arc<dyn SourceConnector> = match opts.source {
        Some(s) => s,
        None => build_source(config, &secrets).await?,
    };
    let sink: Arc<dyn GraphSink> = match opts.graph_sink {
        Some(s) => s,
        None => build_graph(config, &secrets).await?,
    };
    let vector = match &config.vector {
        None => None,
        Some(v) => {
            let vsink: Arc<dyn VectorSink> = match opts.vector_sink {
                Some(s) => s,
                None => build_vector_sink(&v.sink, &secrets).await?,
            };
            let embedder: Arc<dyn EmbeddingProvider> = match opts.embedder {
                Some(e) => e,
                None => build_embedder(&v.embedding, &secrets).await?,
            };
            let caps = embedder.capabilities();
            if caps.allows_protected && config.profile() == Profile::Restricted {
                return Err(Error::new(
                    ErrorKind::Security,
                    "protected plaintext may not be sent to an embedding provider in the restricted profile",
                ));
            }
            Some(VectorRuntime {
                sink: vsink,
                embedder,
                templates: v.templates.iter().map(|t| (t.relation.clone(), t.clone())).collect::<BTreeMap<_, _>>(),
                batch_size: v.batch_size.max(1),
                max_requests_per_minute: v.max_embedding_requests_per_minute,
            })
        }
    };

    let mut audit_sinks = opts.audit_sinks;
    if let Some(path) = &config.audit.file {
        audit_sinks.push(Arc::new(FileAuditSink::new(path.clone(), sealer.clone())));
    }

    Ok(Prepared { provider, sealer, state, source, sink, vector, audit_sinks, faults })
}

fn launch(config: Config, p: Prepared) -> SyncHandle {
    let profile = config.profile();
    let pipeline = config.pipeline.id.clone();
    let mut guarantee = p.sink.capabilities().guarantee();
    if !p.source.capabilities().transaction_boundaries {
        guarantee = guarantee.min(DeliveryGuarantee::AtLeastOnce);
    }
    let mut warnings = Vec::new();
    if !p.sink.capabilities().qualified {
        warnings.push(format!("graph sink `{}` is not qualified by this project", p.sink.capabilities().sink));
    }
    if let Some(v) = &p.vector {
        let vc = v.sink.capabilities();
        if !vc.qualified {
            warnings.push(format!("vector sink `{}` is not qualified by this project", vc.sink));
        }
    }
    if matches!(p.provider.validation(), Validation::NotValidated) && profile != Profile::Development {
        warnings.push(format!("crypto provider `{}` is not FIPS 140-3 validated", p.provider.name()));
    }
    let status = Status {
        pipeline: pipeline.clone(),
        profile,
        phase: Phase::Starting,
        ready: false,
        pause_reason: None,
        failure: None,
        delivery_guarantee: guarantee,
        crypto_provider: p.provider.name().to_owned(),
        crypto_validation: p.provider.validation(),
        source_id: None,
        acked_position: None,
        fence_token: None,
        state_epoch: 0,
        schema_version: None,
        mapping_digest: None,
        mapping_approved: false,
        snapshot: None,
        freshness: Freshness::default(),
        backlog: Backlog::default(),
        counters: Counters::default(),
        vector: None,
        warnings,
    };
    let shared = Arc::new(Shared {
        status: watch::Sender::new(status),
        cancel: CancellationToken::new(),
        paused: watch::Sender::new(false),
        approved_digest: RwLock::new(config.mapping.approved_digest.clone()),
        done: watch::Sender::new(None),
        drain_deadline: RwLock::new(None),
    });
    let policy = Arc::new(RwLock::new(Policy::compile(&config.policy, profile)));
    let validator = RetrievalValidator::new(p.sink.clone(), policy.clone(), pipeline.clone());
    let handle_policy = policy.clone();
    let audit =
        Arc::new(AuditPipe { state: p.state.clone(), sinks: p.audit_sinks, max: config.audit.spool_max_records, notify: Notify::new() });
    let _ = p.sealer;
    let engine = Arc::new(Engine {
        cfg: Arc::new(config),
        pipeline,
        state: p.state,
        source: p.source,
        sink: p.sink,
        policy,
        vector: p.vector,
        audit,
        shared: shared.clone(),
        faults: p.faults,
        apply_notify: Arc::new(Notify::new()),
        plan: RwLock::new(None),
        schema: RwLock::new(None),
        fence: AtomicI64::new(0),
        graph_generation: AtomicU64::new(0),
        source_id: RwLock::new(String::new()),
        vector_stats: tokio::sync::Mutex::new(Default::default()),
    });
    tokio::spawn(engine.run());
    SyncHandle { shared, validator, policy: handle_policy, profile, finished: false }
}

#[allow(unused_variables)]
async fn build_source(config: &Config, secrets: &SecretResolver) -> Result<Arc<dyn SourceConnector>> {
    match &config.source {
        #[cfg(feature = "postgres")]
        SourceConfig::Postgres(pg) => {
            let password = match &pg.password {
                Some(r) => Some(secrets.resolve(r).await?),
                None => None,
            };
            Ok(Arc::new(source::postgres::PostgresSource::new(pg.clone(), password, config.engine.allow_plaintext)?))
        }
        #[cfg(not(feature = "postgres"))]
        SourceConfig::Postgres(_) => Err(Error::feature_not_compiled("postgres", "the PostgreSQL source")),
        SourceConfig::Custom(_) => Err(Error::config("source.type = \"custom\" requires StartOptions::source")),
        _ => Err(Error::new(ErrorKind::Unsupported, "this source connector is not implemented in this release")),
    }
}

#[allow(unused_variables)]
async fn build_graph(config: &Config, secrets: &SecretResolver) -> Result<Arc<dyn GraphSink>> {
    match &config.graph {
        #[cfg(feature = "neo4j")]
        GraphConfig::Neo4j(n) => {
            let password = secrets.resolve(&n.password).await?;
            Ok(Arc::new(sink::neo4j::Neo4jSink::connect(n.clone(), password).await?))
        }
        #[cfg(not(feature = "neo4j"))]
        GraphConfig::Neo4j(_) => Err(Error::feature_not_compiled("neo4j", "the Neo4j graph sink")),
        GraphConfig::Custom(_) => Err(Error::config("graph.type = \"custom\" requires StartOptions::graph_sink")),
        _ => Err(Error::new(ErrorKind::Unsupported, "this graph target is not implemented in this release")),
    }
}

#[allow(unused_variables)]
async fn build_vector_sink(cfg: &VectorSinkConfig, secrets: &SecretResolver) -> Result<Arc<dyn VectorSink>> {
    match cfg {
        #[cfg(feature = "vector-qdrant")]
        VectorSinkConfig::Qdrant { url, collection, api_key } => {
            let key = match api_key {
                Some(r) => Some(secrets.resolve(r).await?),
                None => None,
            };
            Ok(Arc::new(vector::qdrant::QdrantSink::new(url, collection, key)?))
        }
        #[cfg(not(feature = "vector-qdrant"))]
        VectorSinkConfig::Qdrant { .. } => Err(Error::feature_not_compiled("vector-qdrant", "the Qdrant vector sink")),
        #[cfg(feature = "vector-pinecone")]
        VectorSinkConfig::Pinecone { index_host, api_key, namespace } => {
            let key = secrets.resolve(api_key).await?;
            Ok(Arc::new(vector::pinecone::PineconeSink::new(index_host, key, namespace.clone())?))
        }
        #[cfg(not(feature = "vector-pinecone"))]
        VectorSinkConfig::Pinecone { .. } => Err(Error::feature_not_compiled("vector-pinecone", "the Pinecone vector sink")),
        VectorSinkConfig::Custom(_) => Err(Error::config("vector.sink.type = \"custom\" requires StartOptions::vector_sink")),
    }
}

#[allow(unused_variables)]
async fn build_embedder(cfg: &EmbeddingConfig, secrets: &SecretResolver) -> Result<Arc<dyn EmbeddingProvider>> {
    match cfg {
        EmbeddingConfig::Hashing { dimensions } => Ok(Arc::new(HashingEmbedder { dimensions: *dimensions })),
        #[cfg(feature = "embedding-http")]
        EmbeddingConfig::Http { url, model, api_key, dimensions, allow_protected } => {
            let key = match api_key {
                Some(r) => Some(secrets.resolve(r).await?),
                None => None,
            };
            Ok(Arc::new(vector::http_embedding::HttpEmbedder::new(url, model, key, *dimensions, *allow_protected)?))
        }
        #[cfg(not(feature = "embedding-http"))]
        EmbeddingConfig::Http { .. } => Err(Error::feature_not_compiled("embedding-http", "the HTTP embedding provider")),
        EmbeddingConfig::Custom(_) => Err(Error::config("vector.embedding.type = \"custom\" requires StartOptions::embedder")),
    }
}

/// Offline and administrative operations used by the CLI and service.
pub mod admin {
    use super::*;
    use crate::capability::PreflightReport;
    use crate::mapping::MappingPlan;

    /// Discover the schema and compute the mapping plan (no writes anywhere).
    pub async fn plan(config: &Config, opts: StartOptions) -> Result<(MappingPlan, String)> {
        let source = match opts.source {
            Some(s) => s,
            None => {
                let secrets = resolver(config, opts.secret_provider.clone());
                build_source(config, &secrets).await?
            }
        };
        let schema = source.discover_schema().await?;
        let policy = Policy::compile(&config.policy, config.profile());
        let plan = MappingPlan::generate(&schema, &config.mapping, &policy)?;
        let digest = plan.digest();
        Ok((plan, digest))
    }

    fn resolver(config: &Config, extra: Option<Arc<dyn SecretProvider>>) -> SecretResolver {
        SecretResolver::new(
            BuiltinSecretProvider { allow_literal: config.profile() == Profile::Development && config.engine.allow_literal_secrets },
            extra,
        )
    }

    /// Source and target preflight without starting synchronization.
    pub async fn preflight(config: &Config, opts: StartOptions) -> Result<Vec<PreflightReport>> {
        let secrets = resolver(config, opts.secret_provider.clone());
        let source = match opts.source {
            Some(s) => s,
            None => build_source(config, &secrets).await?,
        };
        let sink = match opts.graph_sink {
            Some(s) => s,
            None => build_graph(config, &secrets).await?,
        };
        let mut out = vec![source.preflight().await?];
        let plan = match source.discover_schema().await {
            Ok(schema) => {
                let policy = Policy::compile(&config.policy, config.profile());
                MappingPlan::generate(&schema, &config.mapping, &policy).ok()
            }
            Err(_) => None,
        };
        out.push(sink.preflight(plan.as_ref()).await?);
        Ok(out)
    }

    /// Create graph constraints/indexes required by the approved plan. This
    /// is an explicit operator action; `start` never calls it.
    pub async fn bootstrap_graph(config: &Config, opts: StartOptions) -> Result<String> {
        let secrets = resolver(config, opts.secret_provider.clone());
        let sink = match opts.graph_sink.clone() {
            Some(s) => s,
            None => build_graph(config, &secrets).await?,
        };
        let (plan, digest) = plan(config, opts).await?;
        if config.profile() != Profile::Development && config.mapping.approved_digest.as_deref() != Some(digest.as_str()) {
            return Err(Error::new(ErrorKind::MappingNotApproved, format!("plan digest {digest} is not approved")));
        }
        sink.bootstrap(&plan).await?;
        Ok(digest)
    }

    /// Print the statements an operator runs to enable capture. Never executed.
    pub fn source_setup_statements(config: &Config) -> Result<Vec<String>> {
        match &config.source {
            #[cfg(feature = "postgres")]
            SourceConfig::Postgres(pg) => Ok(source::postgres::setup_statements(pg, &config.policy)),
            _ => Err(Error::new(ErrorKind::Unsupported, "no setup statements for this source type in this build")),
        }
    }

    /// Re-encrypt durable state under the active key.
    pub async fn rotate_state_keys(config: &Config, opts: StartOptions) -> Result<u64> {
        let p = prepare_state_only(config, opts).await?;
        tokio::task::spawn_blocking(move || p.rotate_keys()).await.map_err(|e| Error::internal(e.to_string()))?
    }

    /// Authorized recovery after state loss or restore: accept the graph
    /// target's progress as authoritative and advance the local state epoch
    /// past the recorded evidence. Journaled-but-unapplied work in the
    /// restored state is replayed idempotently on the next start.
    pub async fn accept_state_reset(config: &Config, opts: StartOptions, reason: &str) -> Result<u64> {
        let secrets = resolver(config, opts.secret_provider.clone());
        let sink = match opts.graph_sink.clone() {
            Some(s) => s,
            None => build_graph(config, &secrets).await?,
        };
        let marker = sink.marker(&config.pipeline.id).await?.unwrap_or_default();
        let state = prepare_state_only(config, opts).await?;
        let reason = reason.to_owned();
        tokio::task::spawn_blocking(move || -> Result<u64> {
            let epoch = state
                .advance_epoch_past(marker.state_epoch, &serde_json::json!({"reason": reason, "accepted_epoch": marker.state_epoch}))?;
            state.spool_audit(
                &[crate::audit::AuditRecord::new(
                    "recovery",
                    "accept_state_reset",
                    serde_json::json!({"reason": reason, "graph_epoch": marker.state_epoch, "new_epoch": epoch}),
                )],
                u64::MAX,
            )?;
            Ok(epoch)
        })
        .await
        .map_err(|e| Error::internal(e.to_string()))?
    }

    /// Authorized recovery after the source identity changed (restore to a
    /// different history, failover to a different system): increment the
    /// source epoch, forget the stored identity and resume position, and
    /// require a new snapshot. Versions carry the epoch as their most
    /// significant component, so data from the new history supersedes the
    /// old projection.
    pub async fn accept_source_change(config: &Config, opts: StartOptions, reason: &str) -> Result<u32> {
        let state = prepare_state_only(config, opts).await?;
        let reason = reason.to_owned();
        tokio::task::spawn_blocking(move || -> Result<u32> {
            let epoch: u32 = state.get_meta::<Option<u32>>("source_epoch")?.flatten().unwrap_or(0) + 1;
            let previous: serde_json::Value = state.get_meta("source_identity")?.unwrap_or(serde_json::Value::Null);
            state.put_meta("source_epoch", &epoch)?;
            for k in ["source_identity", "ack_position", "snapshot", "snapshot_done"] {
                state.put_meta(k, &serde_json::Value::Null)?;
            }
            state.spool_audit(
                &[crate::audit::AuditRecord::new(
                    "recovery",
                    "accept_source_change",
                    serde_json::json!({"reason": reason, "previous_identity": previous, "new_source_epoch": epoch}),
                )],
                u64::MAX,
            )?;
            Ok(epoch)
        })
        .await
        .map_err(|e| Error::internal(e.to_string()))?
    }

    async fn prepare_state_only(config: &Config, opts: StartOptions) -> Result<StateStore> {
        let provider: Arc<dyn CryptoProvider> = opts.crypto_provider.unwrap_or_else(|| Arc::new(RustCryptoProvider));
        provider.self_test()?;
        let secrets = resolver(config, opts.secret_provider);
        let sealer = Sealer::new(provider, Arc::new(resolve_keyring(config, &secrets).await?));
        let dir = config.state.dir.clone();
        let backend = opts.state_backend;
        tokio::task::spawn_blocking(move || match backend {
            Some(b) => StateStore::with_backend(b, sealer, None),
            None => StateStore::open(&dir, sealer, None),
        })
        .await
        .map_err(|e| Error::internal(e.to_string()))?
    }

    /// Create the vector collection/index for the configured vector sink
    /// (explicit operator action).
    pub async fn bootstrap_vector(config: &Config) -> Result<Option<String>> {
        let Some(v) = &config.vector else { return Ok(None) };
        let dims = match &v.embedding {
            EmbeddingConfig::Http { dimensions, .. } | EmbeddingConfig::Hashing { dimensions } => *dimensions,
            EmbeddingConfig::Custom(_) => return Ok(Some("custom embedder: create the collection out of band".into())),
        };
        match &v.sink {
            #[cfg(feature = "vector-qdrant")]
            VectorSinkConfig::Qdrant { url, collection, api_key } => {
                let secrets = resolver(config, None);
                let key = match api_key {
                    Some(r) => Some(secrets.resolve(r).await?),
                    None => None,
                };
                vector::qdrant::QdrantSink::new(url, collection, key)?.ensure_collection(dims).await?;
                Ok(Some(format!("qdrant collection `{collection}` ready ({dims} dimensions)")))
            }
            _ => {
                let _ = dims;
                Ok(Some("this vector sink is provisioned out of band (see its documentation)".into()))
            }
        }
    }

    fn redact_literals(v: &mut serde_json::Value) {
        match v {
            serde_json::Value::String(s) if s.starts_with("literal:") => *s = "literal:<redacted>".into(),
            serde_json::Value::Array(a) => a.iter_mut().for_each(redact_literals),
            serde_json::Value::Object(o) => o.values_mut().for_each(redact_literals),
            _ => {}
        }
    }

    /// Reconcile source and graph: counts plus verified readback hashes for a
    /// bounded sample per relation. Read-only for both systems.
    pub async fn reconcile(config: &Config, opts: StartOptions, sample: u32) -> Result<crate::reconcile::ReconcileReport> {
        let secrets = resolver(config, opts.secret_provider.clone());
        let source = match opts.source.clone() {
            Some(s) => s,
            None => build_source(config, &secrets).await?,
        };
        let sink = match opts.graph_sink.clone() {
            Some(s) => s,
            None => build_graph(config, &secrets).await?,
        };
        let schema = source.discover_schema().await?;
        let policy = Policy::compile(&config.policy, config.profile());
        let plan = MappingPlan::generate(&schema, &config.mapping, &policy)?;
        crate::reconcile::reconcile(&config.pipeline.id, &config.pipeline.tenant, source.as_ref(), sink.as_ref(), &plan, &policy, sample)
            .await
    }

    /// Assemble an evidence document from facts that can be checked now.
    /// Sections that cannot be collected are recorded with the reason.
    pub async fn evidence(config: &Config, opts: StartOptions, sample: u32) -> Result<serde_json::Value> {
        use serde_json::json;
        let provider: Arc<dyn CryptoProvider> = opts.crypto_provider.clone().unwrap_or_else(|| Arc::new(RustCryptoProvider));
        let self_test = match provider.self_test() {
            Ok(()) => json!("pass"),
            Err(e) => json!({"fail": e.message()}),
        };
        let mut redacted_config = serde_json::to_value(config).map_err(|e| Error::internal(e.to_string()))?;
        redact_literals(&mut redacted_config);
        let config_digest = crate::identity::content_digest(&[redacted_config.to_string().as_bytes()]);
        let findings = config.findings();
        let mut doc = json!({
            "about": about(),
            "generated_at": chrono::Utc::now().to_rfc3339(),
            "pipeline": config.pipeline.id,
            "profile": config.profile(),
            "config_digest": config_digest,
            "config_findings": findings,
            "crypto": {"provider": provider.name(), "validation": provider.validation(), "self_test": self_test},
            "hash_kinds": {
                "expected_destination": "content digest of intended graph effects (receipts, audit batch_applied)",
                "verified_readback": "hash of properties read back from the graph compared with the current source row (reconciliation)"
            }
        });
        let secrets = resolver(config, opts.secret_provider.clone());
        let source = match opts.source.clone() {
            Some(s) => Ok(s),
            None => build_source(config, &secrets).await,
        };
        let sink = match opts.graph_sink.clone() {
            Some(s) => Ok(s),
            None => build_graph(config, &secrets).await,
        };
        match (&source, &sink) {
            (Ok(src), Ok(snk)) => {
                doc["capabilities"] = json!({"source": src.capabilities(), "graph": snk.capabilities()});
                doc["preflight"] = match (src.preflight().await, snk.preflight(None).await) {
                    (Ok(a), Ok(b)) => json!([a, b]),
                    (a, b) => json!({"error": format!("{:?} / {:?}", a.err().map(|e| e.to_string()), b.err().map(|e| e.to_string()))}),
                };
                doc["graph_marker"] = match snk.marker(&config.pipeline.id).await {
                    Ok(m) => json!(m),
                    Err(e) => json!({"unavailable": e.to_string()}),
                };
            }
            _ => {
                doc["capabilities"] = json!({"unavailable": format!("{:?} / {:?}", source.as_ref().err(), sink.as_ref().err())});
            }
        }
        doc["mapping"] = match plan(config, StartOptions { source: source.as_ref().ok().cloned(), ..Default::default() }).await {
            Ok((p, digest)) => json!({
                "digest": digest,
                "approved_digest": config.mapping.approved_digest,
                "approved": config.profile() == Profile::Development || config.mapping.approved_digest.as_deref() == Some(digest.as_str()),
                "excluded": p.excluded,
            }),
            Err(e) => json!({"unavailable": e.to_string()}),
        };
        doc["state"] = match prepare_state_only(
            config,
            StartOptions {
                crypto_provider: opts.crypto_provider.clone(),
                secret_provider: opts.secret_provider.clone(),
                ..Default::default()
            },
        )
        .await
        {
            Ok(s) => {
                json!({"store_id": s.store_id(), "epoch": s.epoch()?, "checkpoint": s.checkpoint()?, "journal_units": s.journal_len()?, "audit_spool": s.audit_spool_len()?})
            }
            Err(e) => json!({"unavailable": e.to_string()}),
        };
        if let Some(file) = &config.audit.file {
            doc["audit_chain"] = match verify_audit(config, file).await {
                Ok(n) => json!({"verified_records": n}),
                Err(e) => json!({"failed": e.to_string()}),
            };
        }
        if sample > 0 {
            doc["reconciliation"] =
                match reconcile(config, StartOptions { source: source.ok(), graph_sink: sink.ok(), ..Default::default() }, sample).await {
                    Ok(r) => json!(r),
                    Err(e) => json!({"unavailable": e.to_string()}),
                };
        }
        Ok(doc)
    }

    /// Verify an audit file's HMAC chain with the configured keys.
    pub async fn verify_audit(config: &Config, path: &std::path::Path) -> Result<u64> {
        let secrets = resolver(config, None);
        let provider: Arc<dyn CryptoProvider> = Arc::new(RustCryptoProvider);
        let sealer = Sealer::new(provider, Arc::new(resolve_keyring(config, &secrets).await?));
        let p = path.to_owned();
        tokio::task::spawn_blocking(move || FileAuditSink::verify(&p, &sealer)).await.map_err(|e| Error::internal(e.to_string()))?
    }
}
