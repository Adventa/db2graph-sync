//! Qdrant vector sink (REST API).
//!
//! * Upserts use `wait=true`; Qdrant answers `completed` once the points are
//!   applied, which this sink reports as [`Visibility::Verified`].
//! * Stale chunks are deleted with a payload filter
//!   (`pipeline`, `entity_id`, version ≠ current).
//! * The collection is created by [`QdrantSink::ensure_collection`] (called by
//!   the explicit bootstrap command), never implicitly at startup.

use async_trait::async_trait;
use serde_json::{Value as Json, json};

use super::http::{http_client, http_err};
use super::{DeliveryReport, VectorPoint, VectorSink};
use crate::capability::{CheckStatus, PreflightReport, VectorSinkCapabilities, Visibility};
use crate::error::{Error, ErrorKind, Result};
use crate::secret::Secret;

pub struct QdrantSink {
    http: reqwest::Client,
    base: String,
    collection: String,
    api_key: Option<Secret>,
}

impl QdrantSink {
    pub fn new(url: &str, collection: &str, api_key: Option<Secret>) -> Result<Self> {
        if !crate::config::valid_ident(collection) {
            return Err(Error::config("qdrant collection name must match [A-Za-z0-9_.-]{1,64}"));
        }
        Ok(Self { http: http_client()?, base: url.trim_end_matches('/').to_owned(), collection: collection.to_owned(), api_key })
    }

    fn req(&self, m: reqwest::Method, path: &str) -> Result<reqwest::RequestBuilder> {
        let mut r = self.http.request(m, format!("{}{path}", self.base));
        if let Some(k) = &self.api_key {
            r = r.header("api-key", k.expose_str()?);
        }
        Ok(r)
    }

    async fn send(&self, rb: reqwest::RequestBuilder, what: &str) -> Result<Json> {
        let resp = rb.send().await.map_err(|e| http_err(what, e))?;
        let status = resp.status();
        let body: Json = resp.json().await.unwrap_or(Json::Null);
        if status.is_success() {
            Ok(body)
        } else if status.is_server_error() || status.as_u16() == 429 {
            Err(Error::retryable(ErrorKind::Vector, format!("{what}: HTTP {status}: {body}")))
        } else {
            Err(Error::new(ErrorKind::Vector, format!("{what}: HTTP {status}: {body}")))
        }
    }

    /// Create the collection (cosine distance) if it does not exist.
    pub async fn ensure_collection(&self, dimensions: usize) -> Result<()> {
        let path = format!("/collections/{}", self.collection);
        let exists = self.req(reqwest::Method::GET, &path)?.send().await.map_err(|e| http_err("qdrant get collection", e))?;
        if exists.status().is_success() {
            return Ok(());
        }
        self.send(
            self.req(reqwest::Method::PUT, &path)?.json(&json!({"vectors": {"size": dimensions, "distance": "Cosine"}})),
            "qdrant create collection",
        )
        .await?;
        for field in ["pipeline", "entity_id", "tenant", "version"] {
            self.send(
                self.req(reqwest::Method::PUT, &format!("{path}/index?wait=true"))?
                    .json(&json!({"field_name": field, "field_schema": "keyword"})),
                "qdrant create payload index",
            )
            .await?;
        }
        Ok(())
    }

    /// Nearest-neighbour search returning (score, payload). Results must be
    /// passed through the retrieval boundary before use.
    pub async fn search(&self, vector: &[f32], limit: usize, tenant: &str) -> Result<Vec<(f32, Json)>> {
        let body = self
            .send(
                self.req(reqwest::Method::POST, &format!("/collections/{}/points/search", self.collection))?.json(&json!({
                    "vector": vector,
                    "limit": limit,
                    "with_payload": true,
                    "filter": {"must": [{"key": "tenant", "match": {"value": tenant}}]}
                })),
                "qdrant search",
            )
            .await?;
        Ok(body["result"]
            .as_array()
            .map(|a| a.iter().map(|p| (p["score"].as_f64().unwrap_or(0.0) as f32, p["payload"].clone())).collect())
            .unwrap_or_default())
    }
}

#[async_trait]
impl VectorSink for QdrantSink {
    fn capabilities(&self) -> VectorSinkCapabilities {
        VectorSinkCapabilities {
            sink: "qdrant".into(),
            visibility: Visibility::Verified,
            delete_by_filter: true, // Set to true only after the live-server qualification suite has run for a release.
            qualified: false,
        }
    }

    async fn preflight(&self, dimensions: usize) -> Result<PreflightReport> {
        let mut r = PreflightReport::new("vector:qdrant");
        let resp = self
            .req(reqwest::Method::GET, &format!("/collections/{}", self.collection))?
            .send()
            .await
            .map_err(|e| http_err("qdrant preflight", e))?;
        if !resp.status().is_success() {
            r.push("collection", CheckStatus::Fail, format!("collection `{}` missing; run bootstrap", self.collection));
            return Ok(r);
        }
        let body: Json = resp.json().await.unwrap_or(Json::Null);
        let size = body["result"]["config"]["params"]["vectors"]["size"].as_u64();
        r.push(
            "dimensions",
            if size == Some(dimensions as u64) { CheckStatus::Pass } else { CheckStatus::Fail },
            format!("collection size {size:?}, embedding dimensions {dimensions}"),
        );
        Ok(r)
    }

    async fn upsert(&self, points: &[VectorPoint]) -> Result<DeliveryReport> {
        if points.is_empty() {
            return Ok(DeliveryReport { visibility: Visibility::Verified });
        }
        let pts: Vec<Json> = points
            .iter()
            .map(|p| json!({"id": p.id, "vector": p.vector, "payload": serde_json::to_value(&p.payload).unwrap_or(Json::Null)}))
            .collect();
        let body = self
            .send(
                self.req(reqwest::Method::PUT, &format!("/collections/{}/points?wait=true", self.collection))?
                    .json(&json!({"points": pts})),
                "qdrant upsert",
            )
            .await?;
        let visibility = if body["result"]["status"] == "completed" { Visibility::Verified } else { Visibility::Unknown };
        Ok(DeliveryReport { visibility })
    }

    async fn delete_entity(&self, pipeline: &str, entity_id: &str, keep_version: Option<&str>) -> Result<()> {
        let mut filter = json!({"must": [
            {"key": "pipeline", "match": {"value": pipeline}},
            {"key": "entity_id", "match": {"value": entity_id}}
        ]});
        if let Some(v) = keep_version {
            filter["must_not"] = json!([{"key": "version", "match": {"value": v}}]);
        }
        self.send(
            self.req(reqwest::Method::POST, &format!("/collections/{}/points/delete?wait=true", self.collection))?
                .json(&json!({"filter": filter})),
            "qdrant delete",
        )
        .await?;
        Ok(())
    }
}
