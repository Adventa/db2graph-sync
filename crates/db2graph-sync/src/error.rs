//! Typed errors.
//!
//! Every error carries an [`ErrorKind`] that is stable across releases and is
//! what hosts, the CLI (exit codes) and the management API (`code` field)
//! branch on. Display strings are for humans and may change.
//!
//! Errors never contain secret values: connection strings are rendered through
//! [`crate::secret::redact_url`] before they are placed in a message.

use std::fmt;

/// Stable, machine-readable error classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorKind {
    /// The configuration failed validation. Nothing was started.
    Config,
    /// A source or target failed a preflight check.
    Preflight,
    /// A required feature was not compiled into this build.
    FeatureNotCompiled,
    /// The restricted profile's cryptographic prerequisites are not met.
    RestrictedProviderUnavailable,
    /// A security policy rejected the operation (TLS, authentication, profile rules).
    Security,
    /// Secret material could not be resolved.
    Secret,
    /// Durable state failed authentication (tampering or wrong key).
    StateTampered,
    /// Durable state is older than trusted progress evidence (rollback/restore).
    StateRollback,
    /// Durable state I/O failure (including storage exhaustion).
    StateIo,
    /// Another process owns the state directory or the pipeline fence.
    OwnershipConflict,
    /// This instance was fenced out by a newer owner.
    FencedOut,
    /// A source protocol or connectivity failure.
    Source,
    /// The source identity changed (failover to a different system, restore).
    SourceIdentityChanged,
    /// Required resume history is no longer retained by the source.
    ResumeHistoryLost,
    /// The schema changed incompatibly with the approved mapping.
    SchemaIncompatible,
    /// The active mapping plan was not approved for this profile.
    MappingNotApproved,
    /// A graph target failure.
    Graph,
    /// Same identity and version with different content.
    ConsistencyIncident,
    /// A vector sink or embedding failure.
    Vector,
    /// The audit pipeline cannot accept more records; protected work stopped.
    AuditUnavailable,
    /// A configured limit was exceeded (transaction size, backlog, cost).
    LimitExceeded,
    /// An operation did not complete before its deadline.
    Timeout,
    /// The engine was shut down.
    Shutdown,
    /// An invalid management request. Never affects healthy synchronization.
    InvalidRequest,
    /// The caller is not authorized for the operation.
    Unauthorized,
    /// A capability is declared unsupported by the adapter.
    Unsupported,
    /// An internal invariant was violated. Always a bug.
    Internal,
}

impl ErrorKind {
    /// Process exit code used by the standalone binary. Documented in
    /// `docs/reference/errors.md`.
    pub fn exit_code(self) -> i32 {
        match self {
            ErrorKind::Config | ErrorKind::MappingNotApproved => 2,
            ErrorKind::Preflight | ErrorKind::ResumeHistoryLost => 3,
            ErrorKind::Security
            | ErrorKind::RestrictedProviderUnavailable
            | ErrorKind::Secret
            | ErrorKind::StateTampered
            | ErrorKind::StateRollback
            | ErrorKind::Unauthorized => 5,
            ErrorKind::OwnershipConflict | ErrorKind::FencedOut => 6,
            ErrorKind::ConsistencyIncident | ErrorKind::SchemaIncompatible => 7,
            ErrorKind::FeatureNotCompiled | ErrorKind::Unsupported => 8,
            ErrorKind::InvalidRequest => 64,
            _ => 4,
        }
    }

    /// Whether this error stops protected synchronization (as opposed to
    /// being retried internally).
    pub fn is_terminal(self) -> bool {
        !matches!(self, ErrorKind::Timeout | ErrorKind::InvalidRequest)
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = serde_json::to_value(self).ok().and_then(|v| v.as_str().map(str::to_owned)).unwrap_or_else(|| format!("{self:?}"));
        f.write_str(&s)
    }
}

/// The crate error type.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct Error {
    kind: ErrorKind,
    message: String,
    /// Whether an internal retry might succeed (connectivity, deadlocks).
    retryable: bool,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into(), retryable: false }
    }
    pub fn retryable(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into(), retryable: true }
    }
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
    pub fn config(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Config, msg)
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, msg)
    }
    pub fn source(msg: impl Into<String>) -> Self {
        Self::retryable(ErrorKind::Source, msg)
    }
    pub fn graph(msg: impl Into<String>) -> Self {
        Self::retryable(ErrorKind::Graph, msg)
    }
    pub fn state_io(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::StateIo, msg)
    }
    pub fn feature_not_compiled(feature: &str, what: &str) -> Self {
        Self::new(
            ErrorKind::FeatureNotCompiled,
            format!("{what} requires the `{feature}` cargo feature, which is not enabled in this build"),
        )
    }
    /// Prefix the message with context, keeping the kind.
    pub fn context(mut self, ctx: impl fmt::Display) -> Self {
        self.message = format!("{ctx}: {}", self.message);
        self
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<redb::Error> for Error {
    fn from(e: redb::Error) -> Self {
        map_redb(e.to_string())
    }
}
impl From<redb::DatabaseError> for Error {
    fn from(e: redb::DatabaseError) -> Self {
        match e {
            redb::DatabaseError::DatabaseAlreadyOpen => {
                Error::new(ErrorKind::OwnershipConflict, "state database is already open by another process or handle")
            }
            other => map_redb(other.to_string()),
        }
    }
}
impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Self {
        map_redb(e.to_string())
    }
}
impl From<redb::TableError> for Error {
    fn from(e: redb::TableError) -> Self {
        map_redb(e.to_string())
    }
}
impl From<redb::StorageError> for Error {
    fn from(e: redb::StorageError) -> Self {
        map_redb(e.to_string())
    }
}
impl From<redb::CommitError> for Error {
    fn from(e: redb::CommitError) -> Self {
        map_redb(e.to_string())
    }
}

fn map_redb(msg: String) -> Error {
    if msg.contains("corrupt") || msg.contains("Corrupt") {
        Error::new(ErrorKind::StateTampered, format!("state store integrity failure: {msg}"))
    } else {
        Error::state_io(format!("state store: {msg}"))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::state_io(e.to_string())
    }
}
