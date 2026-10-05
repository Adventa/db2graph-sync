//! Shared HTTP client for vector and embedding adapters (explicit rustls
//! provider; certificate verification always on).

use std::time::Duration;

use crate::error::{Error, ErrorKind, Result};

pub(crate) fn http_client() -> Result<reqwest::Client> {
    let tls = crate::tls::client_config(None)?;
    reqwest::Client::builder()
        .use_preconfigured_tls((*tls).clone())
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| Error::new(ErrorKind::Vector, format!("HTTP client: {e}")))
}

pub(crate) fn http_err(what: &str, e: reqwest::Error) -> Error {
    let msg = crate::secret::redact_url(&e.to_string());
    if e.is_timeout() || e.is_connect() || e.status().is_some_and(|s| s.is_server_error() || s.as_u16() == 429) {
        Error::retryable(ErrorKind::Vector, format!("{what}: {msg}"))
    } else {
        Error::new(ErrorKind::Vector, format!("{what}: {msg}"))
    }
}
