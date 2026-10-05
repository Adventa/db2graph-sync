//! Pinecone vector sink (data-plane REST API). **Experimental**: implemented
//! against the documented API but not qualified against a live index in this
//! release (no project-controlled account; see IMPLEMENTATION_STATUS.md).
//!
//! * Record ids are `<pipeline>:<entity_id>#<chunk>` so that an entity's
//!   chunks can be listed by prefix (serverless indexes) for cleanup.
//! * Pinecone is eventually consistent; upserts report
//!   [`Visibility::Unknown`].

use async_trait::async_trait;
use serde_json::{Value as Json, json};

use super::http::{http_client, http_err};
use super::{DeliveryReport, VectorPoint, VectorSink};
use crate::capability::{CheckStatus, PreflightReport, VectorSinkCapabilities, Visibility};
use crate::error::{Error, ErrorKind, Result};
use crate::secret::Secret;

const API_VERSION: &str = "2024-07";

pub struct PineconeSink {
    http: reqwest::Client,
    host: String,
    api_key: Secret,
    namespace: String,
}

pub(crate) fn record_id(pipeline: &str, entity_id: &str, chunk: u32) -> String {
    format!("{pipeline}:{entity_id}#{chunk}")
}

impl PineconeSink {
    pub fn new(index_host: &str, api_key: Secret, namespace: Option<String>) -> Result<Self> {
        let host = if index_host.starts_with("https://") { index_host.to_owned() } else { format!("https://{index_host}") };
        Ok(Self { http: http_client()?, host: host.trim_end_matches('/').to_owned(), api_key, namespace: namespace.unwrap_or_default() })
    }

    async fn call(&self, method: reqwest::Method, path: &str, body: Option<Json>) -> Result<Json> {
        let mut rb = self
            .http
            .request(method, format!("{}{path}", self.host))
            .header("Api-Key", self.api_key.expose_str()?)
            .header("X-Pinecone-API-Version", API_VERSION);
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        let resp = rb.send().await.map_err(|e| http_err("pinecone", e))?;
        let status = resp.status();
        let body: Json = resp.json().await.unwrap_or(Json::Null);
        if status.is_success() {
            Ok(body)
        } else if status.is_server_error() || status.as_u16() == 429 {
            Err(Error::retryable(ErrorKind::Vector, format!("pinecone {path}: HTTP {status}")))
        } else {
            Err(Error::new(ErrorKind::Vector, format!("pinecone {path}: HTTP {status}: {body}")))
        }
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut path = format!("/vectors/list?prefix={}&namespace={}", urlencode(prefix), urlencode(&self.namespace));
            if let Some(t) = &token {
                path.push_str(&format!("&paginationToken={}", urlencode(t)));
            }
            let body = self.call(reqwest::Method::GET, &path, None).await?;
            for v in body["vectors"].as_array().into_iter().flatten() {
                if let Some(id) = v["id"].as_str() {
                    ids.push(id.to_owned());
                }
            }
            token = body["pagination"]["next"].as_str().map(str::to_owned);
            if token.is_none() {
                return Ok(ids);
            }
        }
    }
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[async_trait]
impl VectorSink for PineconeSink {
    fn capabilities(&self) -> VectorSinkCapabilities {
        VectorSinkCapabilities { sink: "pinecone".into(), visibility: Visibility::Unknown, delete_by_filter: false, qualified: false }
    }

    async fn preflight(&self, dimensions: usize) -> Result<PreflightReport> {
        let mut r = PreflightReport::new("vector:pinecone");
        let stats = self.call(reqwest::Method::POST, "/describe_index_stats", Some(json!({}))).await?;
        let dim = stats["dimension"].as_u64();
        r.push(
            "dimensions",
            if dim == Some(dimensions as u64) { CheckStatus::Pass } else { CheckStatus::Fail },
            format!("index dimension {dim:?}, embedding dimensions {dimensions}"),
        );
        r.push("qualification", CheckStatus::Warn, "Pinecone adapter is experimental and not qualified in this release");
        Ok(r)
    }

    async fn upsert(&self, points: &[VectorPoint]) -> Result<DeliveryReport> {
        let vectors: Vec<Json> = points
            .iter()
            .map(|p| {
                json!({
                    "id": record_id(&p.payload.pipeline, &p.payload.entity_id, p.payload.chunk_index),
                    "values": p.vector,
                    "metadata": serde_json::to_value(&p.payload).unwrap_or(Json::Null),
                })
            })
            .collect();
        for batch in vectors.chunks(100) {
            self.call(reqwest::Method::POST, "/vectors/upsert", Some(json!({"vectors": batch, "namespace": self.namespace}))).await?;
        }
        Ok(DeliveryReport { visibility: Visibility::Unknown })
    }

    async fn delete_entity(&self, pipeline: &str, entity_id: &str, keep_version: Option<&str>) -> Result<()> {
        let ids = self.list_prefix(&format!("{pipeline}:{entity_id}#")).await?;
        if ids.is_empty() {
            return Ok(());
        }
        let doomed: Vec<String> = match keep_version {
            None => ids,
            Some(v) => {
                let q = ids.iter().map(|i| format!("ids={}", urlencode(i))).collect::<Vec<_>>().join("&");
                let fetched =
                    self.call(reqwest::Method::GET, &format!("/vectors/fetch?{q}&namespace={}", urlencode(&self.namespace)), None).await?;
                ids.into_iter().filter(|id| fetched["vectors"][id]["metadata"]["version"].as_str() != Some(v)).collect()
            }
        };
        for batch in doomed.chunks(1000) {
            self.call(reqwest::Method::POST, "/vectors/delete", Some(json!({"ids": batch, "namespace": self.namespace}))).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_and_encoding() {
        assert_eq!(record_id("p", "abc", 2), "p:abc#2");
        assert_eq!(urlencode("p:abc#"), "p%3Aabc%23");
    }
}
