//! OpenAI-compatible embeddings endpoint (`POST {url}` with
//! `{"model", "input": [..]}` returning `{"data": [{"index", "embedding"}]}`).
//! **Experimental**: not qualified against a hosted provider in this release.
//!
//! Text sent here has already passed policy: only embed-approved fields, and
//! protected fields only when `allow_protected` was explicitly configured
//! (never in the restricted profile).

use async_trait::async_trait;
use serde_json::{Value as Json, json};

use super::EmbeddingProvider;
use super::http::{http_client, http_err};
use crate::capability::EmbeddingCapabilities;
use crate::error::{Error, ErrorKind, Result};
use crate::secret::Secret;

pub struct HttpEmbedder {
    http: reqwest::Client,
    url: String,
    model: String,
    api_key: Option<Secret>,
    dimensions: usize,
    allow_protected: bool,
}

impl HttpEmbedder {
    pub fn new(url: &str, model: &str, api_key: Option<Secret>, dimensions: usize, allow_protected: bool) -> Result<Self> {
        Ok(Self { http: http_client()?, url: url.to_owned(), model: model.to_owned(), api_key, dimensions, allow_protected })
    }
}

pub(crate) fn parse_response(body: &Json, expected: usize, dims: usize) -> Result<Vec<Vec<f32>>> {
    let data = body["data"].as_array().ok_or_else(|| Error::new(ErrorKind::Vector, "embedding response has no `data` array"))?;
    let mut out: Vec<Option<Vec<f32>>> = vec![None; expected];
    for (pos, d) in data.iter().enumerate() {
        let idx = d["index"].as_u64().map(|i| i as usize).unwrap_or(pos);
        let v: Vec<f32> = d["embedding"]
            .as_array()
            .ok_or_else(|| Error::new(ErrorKind::Vector, "embedding item without vector"))?
            .iter()
            .map(|x| x.as_f64().map(|f| f as f32))
            .collect::<Option<_>>()
            .ok_or_else(|| Error::new(ErrorKind::Vector, "non-numeric embedding value"))?;
        if v.len() != dims {
            return Err(Error::new(ErrorKind::Vector, format!("embedding has {} dimensions, expected {dims}", v.len())));
        }
        *out.get_mut(idx).ok_or_else(|| Error::new(ErrorKind::Vector, "embedding index out of range"))? = Some(v);
    }
    out.into_iter().collect::<Option<Vec<_>>>().ok_or_else(|| Error::new(ErrorKind::Vector, "embedding response missing items"))
}

#[async_trait]
impl EmbeddingProvider for HttpEmbedder {
    fn capabilities(&self) -> EmbeddingCapabilities {
        EmbeddingCapabilities {
            provider: "http-openai-compatible".into(),
            model: self.model.clone(),
            model_version: format!("{}@d{}", self.model, self.dimensions),
            dimensions: self.dimensions,
            allows_protected: self.allow_protected,
            remote: true,
        }
    }

    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut rb = self.http.post(&self.url).json(&json!({"model": self.model, "input": texts}));
        if let Some(k) = &self.api_key {
            rb = rb.bearer_auth(k.expose_str()?);
        }
        let resp = rb.send().await.map_err(|e| http_err("embedding request", e))?;
        let status = resp.status();
        let body: Json = resp.json().await.unwrap_or(Json::Null);
        if !status.is_success() {
            let retry = status.is_server_error() || status.as_u16() == 429;
            let msg = format!("embedding provider returned HTTP {status}");
            return Err(if retry { Error::retryable(ErrorKind::Vector, msg) } else { Error::new(ErrorKind::Vector, msg) });
        }
        parse_response(&body, texts.len(), self.dimensions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_out_of_order_items() {
        let body = json!({"data": [{"index": 1, "embedding": [0.0, 1.0]}, {"index": 0, "embedding": [1.0, 0.0]}]});
        let v = parse_response(&body, 2, 2).unwrap();
        assert_eq!(v[0], vec![1.0, 0.0]);
        assert_eq!(v[1], vec![0.0, 1.0]);
    }

    #[test]
    fn rejects_wrong_dimensions_and_missing_items() {
        assert!(parse_response(&json!({"data": [{"index": 0, "embedding": [1.0]}]}), 1, 2).is_err());
        assert!(parse_response(&json!({"data": []}), 1, 2).is_err());
    }
}
