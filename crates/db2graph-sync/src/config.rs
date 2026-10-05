//! Configuration model and validation.
//!
//! Configuration is plain data (TOML or constructed in code). [`Config::validate`]
//! checks it against the selected [`Profile`] *without* touching the network.
//! Validation succeeding means only that the configuration is internally
//! consistent and permitted by the profile; it says nothing about whether the
//! source or target is reachable (that is preflight's job).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{Error, ErrorKind, Result};
use crate::mapping::MappingConfig;
use crate::policy::PolicyConfig;
use crate::secret::SecretRef;

/// Security profile. See `docs/security/profiles.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    /// Local development. Exceptions (plaintext, literal secrets) are allowed
    /// only when each is explicitly enabled.
    #[default]
    Development,
    /// Production: verified TLS everywhere, approved mappings, dataset
    /// allowlists, authenticated management.
    Enterprise,
    /// Enterprise plus a FIPS 140-3 validated cryptographic module for all
    /// relevant crypto paths, default-deny unknown fields and mandatory audit.
    Restricted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub pipeline: PipelineConfig,
    pub source: SourceConfig,
    pub graph: GraphConfig,
    pub state: StateConfig,
    #[serde(default)]
    pub mapping: MappingConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
    #[serde(default)]
    pub vector: Option<VectorConfig>,
    #[serde(default)]
    pub audit: AuditConfig,
    #[serde(default)]
    pub management: ManagementConfig,
    #[serde(default)]
    pub engine: EngineTuning,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Stable pipeline identity. Used in receipts, fences and identities of
    /// pipeline-owned graph metadata. Changing it creates a new projection.
    pub id: String,
    /// Tenant identity folded into every entity id.
    pub tenant: String,
    #[serde(default)]
    pub profile: Profile,
}

// Built once per process; boxing the large variant would only complicate the API.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Postgres(PostgresSourceConfig),
    /// Accepted by the parser so that capability errors are precise; the
    /// connectors below are not implemented in this release (see
    /// IMPLEMENTATION_STATUS.md) and fail validation with `Unsupported`.
    Mysql(UnimplementedConnector),
    Mongodb(UnimplementedConnector),
    Dynamodb(UnimplementedConnector),
    Sqlserver(UnimplementedConnector),
    /// A connector supplied in code through `StartOptions::source`.
    Custom(CustomComponent),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UnimplementedConnector {
    #[serde(flatten)]
    pub settings: toml::Table,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CustomComponent {
    /// Free-form label reported in status.
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceConfig {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<SecretRef>,
    #[serde(default)]
    pub tls: TlsConfig,
    /// Publication that selects captured tables. Created by the operator
    /// (`db2graph-sync source-setup` prints the statements; it never runs them).
    pub publication: String,
    /// Logical replication slot owned by this pipeline.
    pub slot: String,
    #[serde(default = "default_schema_refresh")]
    pub schema_refresh_interval_secs: u64,
    #[serde(default)]
    pub snapshot: SnapshotConfig,
    /// Maximum changes buffered for one source transaction before the
    /// pipeline stops with `LimitExceeded`. See LIMITATIONS.md.
    #[serde(default = "default_max_tx_changes")]
    pub max_transaction_changes: usize,
    #[serde(default = "default_status_interval")]
    pub status_interval_secs: u64,
    /// Maximum WAL (bytes) the slot may retain before preflight warns.
    #[serde(default)]
    pub max_slot_retained_bytes: Option<u64>,
}

fn default_pg_port() -> u16 {
    5432
}
fn default_schema_refresh() -> u64 {
    300
}
fn default_max_tx_changes() -> usize {
    200_000
}
fn default_status_interval() -> u64 {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotConfig {
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default = "default_chunk_rows")]
    pub chunk_rows: u32,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self { enabled: true, chunk_rows: default_chunk_rows() }
    }
}

fn yes() -> bool {
    true
}
fn default_chunk_rows() -> u32 {
    2_000
}

/// TLS settings. There is no "verify = false" option: certificate and
/// hostname verification are always performed when TLS is used.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default)]
    pub mode: TlsMode,
    /// Additional trust anchors (PEM). Platform roots are used otherwise.
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TlsMode {
    /// TLS with full certificate chain and hostname verification. No
    /// plaintext fallback.
    #[default]
    VerifyFull,
    /// Plaintext. Only permitted in the development profile when
    /// `engine.allow_plaintext = true` is also set.
    Disable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GraphConfig {
    Neo4j(Neo4jConfig),
    /// Not implemented in this release; fails validation with `Unsupported`.
    Memgraph(UnimplementedConnector),
    /// Not implemented in this release; fails validation with `Unsupported`.
    Neptune(UnimplementedConnector),
    /// A sink supplied in code through `StartOptions::graph_sink`.
    Custom(CustomComponent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Neo4jConfig {
    /// `bolt+s://host:7687` (verified TLS) or `bolt://` (development only).
    /// Routing schemes (`neo4j://`) are rejected: the driver used does not
    /// implement client-side routing.
    pub uri: String,
    pub user: String,
    pub password: SecretRef,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default = "default_pool")]
    pub max_connections: usize,
    /// Extra trust anchors (PEM) for the server certificate.
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
}

fn default_pool() -> usize {
    8
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    /// Directory owned exclusively by one running engine (enforced by an OS
    /// file lock).
    pub dir: PathBuf,
    /// Data-encryption keys. Each key is 32 bytes, base64 encoded, resolved
    /// through a secret reference.
    pub keys: Vec<StateKeyConfig>,
    /// Key id used for new writes.
    pub active_key: u32,
    /// Unapplied journal units at which capture applies backpressure (the
    /// source then retains changes instead of the local disk).
    #[serde(default = "default_journal_limit")]
    pub max_journal_units: u64,
    /// Optional external anchor file used as additional rollback evidence
    /// (place it on storage that is not restored together with `dir`).
    #[serde(default)]
    pub anchor_file: Option<PathBuf>,
}

fn default_journal_limit() -> u64 {
    50_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateKeyConfig {
    pub id: u32,
    pub key: SecretRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorConfig {
    pub sink: VectorSinkConfig,
    pub embedding: EmbeddingConfig,
    #[serde(default)]
    pub templates: Vec<VectorTemplateConfig>,
    /// Hard cap on embedding requests per minute (cost control).
    #[serde(default = "default_embed_rpm")]
    pub max_embedding_requests_per_minute: u32,
    #[serde(default = "default_vector_batch")]
    pub batch_size: usize,
}

fn default_embed_rpm() -> u32 {
    600
}
fn default_vector_batch() -> usize {
    32
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VectorSinkConfig {
    Qdrant { url: String, collection: String, api_key: Option<SecretRef> },
    Pinecone { index_host: String, api_key: SecretRef, namespace: Option<String> },
    Custom(CustomComponent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EmbeddingConfig {
    /// OpenAI-compatible `/v1/embeddings` endpoint.
    Http {
        url: String,
        model: String,
        api_key: Option<SecretRef>,
        dimensions: usize,
        /// Explicit authorization to send fields classified as protected.
        #[serde(default)]
        allow_protected: bool,
    },
    /// Deterministic local feature hashing. Not semantic; intended for tests,
    /// demos and air-gapped pipeline validation.
    Hashing {
        dimensions: usize,
    },
    Custom(CustomComponent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorTemplateConfig {
    pub relation: String,
    /// Text template with `{field}` placeholders. Only fields that are both
    /// policy-allowed and embed-approved are substituted.
    pub template: String,
    #[serde(default = "default_chunk_chars")]
    pub chunk_chars: usize,
}

fn default_chunk_chars() -> usize {
    2_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditConfig {
    /// Append-only, hash-chained JSON-lines audit file. When unset, audit
    /// records are kept only in the encrypted state spool.
    pub file: Option<PathBuf>,
    /// Maximum unforwarded records held in the spool. When reached, protected
    /// work stops with `AuditUnavailable`.
    #[serde(default = "default_audit_spool")]
    pub spool_max_records: u64,
}

fn default_audit_spool() -> u64 {
    100_000
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self { file: None, spool_max_records: default_audit_spool() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ManagementConfig {
    /// Address for the standalone binary to serve the management API on.
    /// The library itself never binds a port.
    pub listen: Option<String>,
    /// PEM certificate chain and private key for the management listener.
    /// Required outside the development profile (no plaintext management).
    #[serde(default)]
    pub tls_cert: Option<PathBuf>,
    #[serde(default)]
    pub tls_key: Option<PathBuf>,
    #[serde(default)]
    pub tokens: Vec<ManagementToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementToken {
    pub name: String,
    /// Hex SHA-256 of the bearer token. The token itself is never configured.
    pub sha256: String,
    pub scopes: Vec<Scope>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Read,
    Operate,
    Recover,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineTuning {
    /// Maximum source transactions combined into one graph transaction.
    /// Transactions are never split; batching only groups whole transactions.
    #[serde(default = "default_batch")]
    pub max_batch_transactions: usize,
    #[serde(default = "default_batch_changes")]
    pub max_batch_changes: usize,
    /// Development-only exception permitting `TlsMode::Disable` / `bolt://`.
    #[serde(default)]
    pub allow_plaintext: bool,
    /// Development-only exception permitting `literal:` secret references.
    #[serde(default)]
    pub allow_literal_secrets: bool,
    #[serde(default = "default_retry_max")]
    pub retry_max_backoff_ms: u64,
}

impl Default for EngineTuning {
    fn default() -> Self {
        Self {
            max_batch_transactions: default_batch(),
            max_batch_changes: default_batch_changes(),
            allow_plaintext: false,
            allow_literal_secrets: false,
            retry_max_backoff_ms: default_retry_max(),
        }
    }
}

fn default_batch() -> usize {
    64
}
fn default_batch_changes() -> usize {
    5_000
}
fn default_retry_max() -> u64 {
    30_000
}

/// A single validation finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Finding {
    pub path: String,
    pub message: String,
    pub kind: ErrorKind,
}

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Self> {
        toml::from_str(s).map_err(|e| Error::config(format!("invalid configuration: {e}")))
    }

    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let p = path.as_ref();
        let s = std::fs::read_to_string(p).map_err(|e| Error::config(format!("cannot read configuration `{}`: {e}", p.display())))?;
        Self::from_toml_str(&s)
    }

    pub fn profile(&self) -> Profile {
        self.pipeline.profile
    }

    /// Validate without network access. Returns every finding.
    pub fn findings(&self) -> Vec<Finding> {
        let mut f = Vec::new();
        let profile = self.profile();
        let dev = profile == Profile::Development;
        let mut push = |path: &str, kind: ErrorKind, msg: String| {
            f.push(Finding { path: path.into(), message: msg, kind });
        };

        if !valid_ident(&self.pipeline.id) {
            push("pipeline.id", ErrorKind::Config, "must match [A-Za-z0-9_.-]{1,64}".into());
        }
        if self.pipeline.tenant.is_empty() {
            push("pipeline.tenant", ErrorKind::Config, "must not be empty".into());
        }
        if !dev && self.engine.allow_plaintext {
            push("engine.allow_plaintext", ErrorKind::Security, "plaintext is only permitted in the development profile".into());
        }
        if !dev && self.engine.allow_literal_secrets {
            push(
                "engine.allow_literal_secrets",
                ErrorKind::Security,
                "literal secrets are only permitted in the development profile".into(),
            );
        }
        if self.engine.max_batch_transactions == 0 || self.engine.max_batch_changes == 0 {
            push("engine", ErrorKind::Config, "batch limits must be positive".into());
        }

        match &self.source {
            SourceConfig::Postgres(pg) => {
                if pg.tls.mode == TlsMode::Disable && !(dev && self.engine.allow_plaintext) {
                    push(
                        "source.tls.mode",
                        ErrorKind::Security,
                        "plaintext requires profile=development and engine.allow_plaintext=true".into(),
                    );
                }
                if !valid_pg_ident(&pg.slot) {
                    push("source.slot", ErrorKind::Config, "slot names must match [a-z0-9_]{1,63}".into());
                }
                if pg.publication.is_empty() {
                    push("source.publication", ErrorKind::Config, "must not be empty".into());
                }
                if pg.schema_refresh_interval_secs == 0 {
                    push("source.schema_refresh_interval_secs", ErrorKind::Config, "must be positive".into());
                }
                if pg.snapshot.chunk_rows == 0 {
                    push("source.snapshot.chunk_rows", ErrorKind::Config, "must be positive".into());
                }
                if let Some(p) = &pg.password {
                    check_secret(&mut push, "source.password", p, dev && self.engine.allow_literal_secrets);
                }
            }
            SourceConfig::Mysql(_) | SourceConfig::Mongodb(_) | SourceConfig::Dynamodb(_) | SourceConfig::Sqlserver(_) => {
                push(
                    "source.type",
                    ErrorKind::Unsupported,
                    "this connector is planned but not implemented in this release; see IMPLEMENTATION_STATUS.md".into(),
                );
            }
            SourceConfig::Custom(_) => {}
        }

        match &self.graph {
            GraphConfig::Neo4j(n) => {
                let scheme = n.uri.split("://").next().unwrap_or("");
                match scheme {
                    "bolt+s" => {}
                    "bolt" => {
                        if !(dev && self.engine.allow_plaintext) {
                            push(
                                "graph.uri",
                                ErrorKind::Security,
                                "bolt:// is plaintext; use bolt+s:// (plaintext requires profile=development and engine.allow_plaintext=true)".into(),
                            );
                        }
                    }
                    "neo4j" | "neo4j+s" | "neo4j+ssc" => push(
                        "graph.uri",
                        ErrorKind::Unsupported,
                        "routing schemes are not supported by the bundled driver; connect to a specific member with bolt+s://".into(),
                    ),
                    "bolt+ssc" => {
                        push("graph.uri", ErrorKind::Security, "use bolt+s:// with graph.ca_file for self-signed deployments".into())
                    }
                    _ => push("graph.uri", ErrorKind::Config, "unrecognized scheme".into()),
                }
                if n.max_connections == 0 {
                    push("graph.max_connections", ErrorKind::Config, "must be positive".into());
                }
                check_secret(&mut push, "graph.password", &n.password, dev && self.engine.allow_literal_secrets);
            }
            GraphConfig::Memgraph(_) | GraphConfig::Neptune(_) => push(
                "graph.type",
                ErrorKind::Unsupported,
                "this graph target is planned but not implemented in this release; see IMPLEMENTATION_STATUS.md".into(),
            ),
            GraphConfig::Custom(_) => {}
        }

        if self.state.keys.is_empty() {
            push("state.keys", ErrorKind::Config, "at least one state encryption key is required".into());
        }
        if !self.state.keys.iter().any(|k| k.id == self.state.active_key) {
            push("state.active_key", ErrorKind::Config, "must reference a configured key id".into());
        }
        let mut ids: Vec<u32> = self.state.keys.iter().map(|k| k.id).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != self.state.keys.len() {
            push("state.keys", ErrorKind::Config, "key ids must be unique".into());
        }
        for (i, k) in self.state.keys.iter().enumerate() {
            check_secret(&mut push, &format!("state.keys[{i}].key"), &k.key, dev && self.engine.allow_literal_secrets);
        }

        if let Some(v) = &self.vector {
            if let VectorSinkConfig::Qdrant { url, .. } = &v.sink
                && url.starts_with("http://")
                && !(dev && self.engine.allow_plaintext)
            {
                push("vector.sink.url", ErrorKind::Security, "plaintext HTTP requires the development exception".into());
            }
            if let EmbeddingConfig::Http { url, allow_protected, .. } = &v.embedding {
                if url.starts_with("http://") && !(dev && self.engine.allow_plaintext) {
                    push("vector.embedding.url", ErrorKind::Security, "plaintext HTTP requires the development exception".into());
                }
                if *allow_protected && profile == Profile::Restricted {
                    push(
                        "vector.embedding.allow_protected",
                        ErrorKind::Security,
                        "sending protected plaintext to an embedding provider is not permitted in the restricted profile".into(),
                    );
                }
            }
            if let EmbeddingConfig::Hashing { .. } = &v.embedding
                && !dev
            {
                push(
                    "vector.embedding",
                    ErrorKind::Config,
                    "the hashing embedder is not semantic and is restricted to the development profile".into(),
                );
            }
        }

        if !dev {
            if self.mapping.approved_digest.is_none() {
                push(
                    "mapping.approved_digest",
                    ErrorKind::MappingNotApproved,
                    "non-development profiles require an approved mapping plan digest (`db2graph-sync plan`)".into(),
                );
            }
            if self.policy.datasets.is_empty() {
                push("policy.datasets", ErrorKind::Config, "non-development profiles require a dataset allowlist".into());
            }
            if self.management.listen.is_some() && self.management.tokens.is_empty() {
                push("management.tokens", ErrorKind::Security, "the management API requires at least one token".into());
            }
            if self.management.listen.is_some() && (self.management.tls_cert.is_none() || self.management.tls_key.is_none()) {
                push("management.tls_cert", ErrorKind::Security, "the management API requires TLS outside the development profile".into());
            }
        }
        if profile == Profile::Restricted && self.audit.file.is_none() {
            push("audit.file", ErrorKind::Security, "the restricted profile requires a protected audit destination".into());
        }
        for (i, t) in self.management.tokens.iter().enumerate() {
            if t.sha256.len() != 64 || hex::decode(&t.sha256).is_err() {
                push(&format!("management.tokens[{i}].sha256"), ErrorKind::Config, "must be 64 hex characters".into());
            }
        }
        for issue in self.policy.findings(profile) {
            push("policy", ErrorKind::Config, issue);
        }
        for issue in self.mapping.findings() {
            push("mapping", ErrorKind::Config, issue);
        }
        f
    }

    /// Validate and return the first blocking finding as an error.
    pub fn validate(&self) -> Result<()> {
        let findings = self.findings();
        if findings.is_empty() {
            return Ok(());
        }
        // Prefer the most specific kind: security beats config.
        let kind = findings.iter().map(|f| f.kind).find(|k| *k != ErrorKind::Config).unwrap_or(ErrorKind::Config);
        let msg = findings.iter().map(|f| format!("{}: {}", f.path, f.message)).collect::<Vec<_>>().join("; ");
        Err(Error::new(kind, msg))
    }
}

fn check_secret(push: &mut impl FnMut(&str, ErrorKind, String), path: &str, r: &SecretRef, literal_ok: bool) {
    if r.is_literal() && !literal_ok {
        push(path, ErrorKind::Security, "literal secrets require profile=development and engine.allow_literal_secrets=true".into());
    }
    if r.scheme().is_empty() {
        push(path, ErrorKind::Config, "secret references must be `<scheme>:<value>` (e.g. env:NAME)".into());
    }
}

pub(crate) fn valid_ident(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64 && s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

fn valid_pg_ident(s: &str) -> bool {
    !s.is_empty() && s.len() <= 63 && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}
