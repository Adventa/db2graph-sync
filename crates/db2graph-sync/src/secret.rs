//! Secret references and providers.
//!
//! Configuration never holds secret values directly. It holds a *reference*
//! such as `env:PG_PASSWORD` or `file:/run/secrets/pg_password`, resolved at
//! startup through a [`SecretProvider`]. Hosts can supply their own provider
//! (for example a cloud secret manager client) through
//! [`crate::StartOptions::secret_provider`].
//!
//! `literal:` references are accepted only in the development profile so that
//! the exception is explicit and visible in configuration review.
//!
//! The library only *reads* environment variables. It never sets or removes
//! them.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::{Error, ErrorKind, Result};

/// A resolved secret. `Debug` and `Display` are redacted; the memory is
/// zeroized on drop.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct Secret(Vec<u8>);

impl Secret {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Secret(bytes.into())
    }
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
    /// Expose as UTF-8 (trailing newline from secret files is trimmed).
    pub fn expose_str(&self) -> Result<&str> {
        std::str::from_utf8(&self.0)
            .map(|s| s.trim_end_matches(['\r', '\n']))
            .map_err(|_| Error::new(ErrorKind::Secret, "secret is not valid UTF-8"))
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// A reference to a secret, e.g. `env:NAME`, `file:/path`, `provider:key`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretRef(pub String);

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The reference itself is not secret unless it is a literal.
        if self.0.starts_with("literal:") { f.write_str("SecretRef(literal:<redacted>)") } else { write!(f, "SecretRef({})", self.0) }
    }
}

impl SecretRef {
    pub fn scheme(&self) -> &str {
        self.0.split_once(':').map(|(s, _)| s).unwrap_or("")
    }
    pub fn is_literal(&self) -> bool {
        self.scheme() == "literal"
    }
}

/// Resolves secret references. Implementations must not log secret values.
#[async_trait]
pub trait SecretProvider: Send + Sync {
    /// Schemes this provider handles (e.g. `["vault"]`).
    fn schemes(&self) -> &[&'static str];
    async fn resolve(&self, reference: &SecretRef) -> Result<Secret>;
}

/// Built-in provider for `env:`, `file:` and (development only) `literal:`.
#[derive(Debug, Default, Clone)]
pub struct BuiltinSecretProvider {
    pub allow_literal: bool,
}

#[async_trait]
impl SecretProvider for BuiltinSecretProvider {
    fn schemes(&self) -> &[&'static str] {
        &["env", "file", "literal"]
    }

    async fn resolve(&self, reference: &SecretRef) -> Result<Secret> {
        let (scheme, rest) =
            reference.0.split_once(':').ok_or_else(|| Error::new(ErrorKind::Secret, "secret reference must be `<scheme>:<value>`"))?;
        match scheme {
            "env" => std::env::var_os(rest)
                .map(|v| Secret::new(v.to_string_lossy().into_owned().into_bytes()))
                .ok_or_else(|| Error::new(ErrorKind::Secret, format!("environment variable `{rest}` is not set"))),
            "file" => {
                let path = rest.to_owned();
                tokio::task::spawn_blocking(move || std::fs::read(&path).map_err(|e| (path, e)))
                    .await
                    .map_err(|e| Error::internal(e.to_string()))?
                    .map(Secret::new)
                    .map_err(|(p, e)| Error::new(ErrorKind::Secret, format!("cannot read secret file `{p}`: {e}")))
            }
            "literal" if self.allow_literal => Ok(Secret::new(rest.as_bytes().to_vec())),
            "literal" => Err(Error::new(ErrorKind::Security, "`literal:` secret references are only permitted in the development profile")),
            other => Err(Error::new(ErrorKind::Secret, format!("no secret provider registered for scheme `{other}`"))),
        }
    }
}

/// Dispatches to the first provider that handles a scheme.
#[derive(Clone)]
pub struct SecretResolver {
    providers: Vec<Arc<dyn SecretProvider>>,
}

impl SecretResolver {
    pub fn new(builtin: BuiltinSecretProvider, extra: Option<Arc<dyn SecretProvider>>) -> Self {
        let mut providers: Vec<Arc<dyn SecretProvider>> = Vec::new();
        if let Some(p) = extra {
            providers.push(p);
        }
        providers.push(Arc::new(builtin));
        Self { providers }
    }

    pub async fn resolve(&self, r: &SecretRef) -> Result<Secret> {
        let scheme = r.scheme();
        for p in &self.providers {
            if p.schemes().contains(&scheme) {
                return p.resolve(r).await;
            }
        }
        Err(Error::new(ErrorKind::Secret, format!("no secret provider registered for scheme `{scheme}`")))
    }
}

/// Remove userinfo/password and sensitive query parameters from a URL-like
/// string before it is placed in an error or log line.
pub fn redact_url(s: &str) -> String {
    let mut out = s.to_owned();
    if let Some(scheme_end) = out.find("://") {
        let rest_start = scheme_end + 3;
        if let Some(at) = out[rest_start..].find('@') {
            let at = rest_start + at;
            let slash = out[rest_start..].find('/').map(|i| rest_start + i).unwrap_or(out.len());
            if at < slash {
                out.replace_range(rest_start..at, "<redacted>");
            }
        }
    }
    for key in ["password=", "sslpassword=", "token=", "api_key=", "apikey="] {
        while let Some(i) = out.to_ascii_lowercase().find(key) {
            let start = i + key.len();
            let end = out[start..].find(['&', ' ']).map(|j| start + j).unwrap_or(out.len());
            if out[start..end] == *"<redacted>" {
                break;
            }
            out.replace_range(start..end, "<redacted>");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_userinfo_and_params() {
        assert_eq!(
            redact_url("postgres://u:hunter2@db:5432/x?sslmode=verify-full&password=p"),
            "postgres://<redacted>@db:5432/x?sslmode=verify-full&password=<redacted>"
        );
        assert_eq!(redact_url("bolt+s://graph:7687"), "bolt+s://graph:7687");
    }

    #[test]
    fn secret_debug_is_redacted() {
        let s = Secret::new(b"hunter2".to_vec());
        assert!(!format!("{s:?}").contains("hunter2"));
        let r = SecretRef("literal:hunter2".into());
        assert!(!format!("{r:?}").contains("hunter2"));
    }

    #[tokio::test]
    async fn literal_rejected_outside_dev() {
        let p = BuiltinSecretProvider { allow_literal: false };
        let e = p.resolve(&SecretRef("literal:x".into())).await.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Security);
    }
}
