//! Optional vector indexing.
//!
//! Vector work is created in the graph target, atomically with the graph
//! change (the outbox). The worker here drains it:
//!
//! 1. read the task and the entity's *current* graph state;
//! 2. if the entity is gone or newer than the task, the task is superseded;
//! 3. render the approved template using only policy-allowed, embed-approved
//!    fields (protected fields need explicit provider authorization);
//! 4. chunk deterministically, embed, upsert points whose ids derive from
//!    (entity, chunk, model version) and whose payload carries graph identity,
//!    version, content digest, tenant, labels and model version;
//! 5. delete points of older versions; complete the task only if its version
//!    is still current.
//!
//! Graph and vector stores converge asynchronously; there is no cross-store
//! atomicity. Retrieval must go through [`crate::retrieval`] to inherit
//! authorization and freshness guarantees.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::capability::{EmbeddingCapabilities, VectorSinkCapabilities, Visibility};
use crate::error::{Error, ErrorKind, Result};
use crate::policy::Policy;
use crate::sink::EntityState;
use crate::types::Value;

#[cfg(feature = "http-client")]
pub(crate) mod http;
#[cfg(feature = "embedding-http")]
pub mod http_embedding;
#[cfg(feature = "vector-pinecone")]
pub mod pinecone;
#[cfg(feature = "vector-qdrant")]
pub mod qdrant;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorPoint {
    /// Deterministic UUID derived from (entity, chunk index, model version).
    pub id: String,
    pub vector: Vec<f32>,
    pub payload: PointPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointPayload {
    pub pipeline: String,
    pub entity_id: String,
    pub relation: String,
    pub tenant: String,
    pub version: String,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub content_digest: String,
    pub model: String,
    pub model_version: String,
    /// JSON-serialized policy labels at indexing time (informational; the
    /// retrieval boundary re-evaluates against the current graph and policy).
    pub labels_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryReport {
    pub visibility: Visibility,
}

#[async_trait]
pub trait VectorSink: Send + Sync {
    fn capabilities(&self) -> VectorSinkCapabilities;
    async fn preflight(&self, dimensions: usize) -> Result<crate::capability::PreflightReport>;
    async fn upsert(&self, points: &[VectorPoint]) -> Result<DeliveryReport>;
    /// Delete points for `entity_id` whose version differs from `keep_version`
    /// (all points when `None`).
    async fn delete_entity(&self, pipeline: &str, entity_id: &str, keep_version: Option<&str>) -> Result<()>;
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    fn capabilities(&self) -> EmbeddingCapabilities;
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
}

/// Deterministic feature-hashing embedder. **Not semantic.** For tests,
/// demos and pipeline validation without an external provider.
#[derive(Debug, Clone)]
pub struct HashingEmbedder {
    pub dimensions: usize,
}

#[async_trait]
impl EmbeddingProvider for HashingEmbedder {
    fn capabilities(&self) -> EmbeddingCapabilities {
        EmbeddingCapabilities {
            provider: "hashing".into(),
            model: "feature-hash".into(),
            model_version: format!("feature-hash-v1-d{}", self.dimensions),
            dimensions: self.dimensions,
            allows_protected: false,
            remote: false,
        }
    }
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        use sha2::{Digest, Sha256};
        Ok(texts
            .iter()
            .map(|t| {
                let mut v = vec![0f32; self.dimensions];
                for tok in t.split_whitespace() {
                    let h = Sha256::digest(tok.to_lowercase().as_bytes());
                    let idx = u64::from_be_bytes(h[..8].try_into().expect("8")) as usize % self.dimensions;
                    v[idx] += if h[8] & 1 == 0 { 1.0 } else { -1.0 };
                }
                let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                if n > 0.0 {
                    v.iter_mut().for_each(|x| *x /= n);
                }
                v
            })
            .collect())
    }
}

/// Result of rendering a template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    pub text: String,
    /// Placeholders left empty because policy did not allow embedding them.
    pub withheld: Vec<String>,
}

fn value_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bytes(_) => String::new(),
        Value::Array(a) => a.iter().map(value_text).collect::<Vec<_>>().join(", "),
        Value::Unknown { text, .. } => text.clone(),
        Value::BigInt(s)
        | Value::Decimal(s)
        | Value::Text(s)
        | Value::Uuid(s)
        | Value::Date(s)
        | Value::Time(s)
        | Value::Timestamp(s)
        | Value::TimestampTz(s)
        | Value::Json(s) => s.clone(),
    }
}

/// Render `{field}` placeholders. Only embeddable fields are substituted.
pub fn render(template: &str, relation: &str, props: &BTreeMap<String, Value>, policy: &Policy, allow_protected: bool) -> Rendered {
    let mut out = String::new();
    let mut withheld = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        match after.find('}') {
            Some(end) => {
                let field = &after[..end];
                if policy.embeddable(relation, field, allow_protected) {
                    if let Some(v) = props.get(field) {
                        out.push_str(&value_text(v));
                    }
                } else {
                    withheld.push(field.to_owned());
                }
                rest = &after[end + 1..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    Rendered { text: out, withheld }
}

/// Split text into chunks of at most `max_chars` characters, preferring
/// whitespace boundaries. Deterministic.
pub fn chunk(text: &str, max_chars: usize) -> Vec<String> {
    let max_chars = max_chars.max(16);
    let mut chunks = Vec::new();
    let mut cur = String::new();
    for word in text.split_inclusive(char::is_whitespace) {
        if cur.chars().count() + word.chars().count() > max_chars && !cur.is_empty() {
            chunks.push(std::mem::take(&mut cur).trim().to_owned());
        }
        if word.chars().count() > max_chars {
            let chars: Vec<char> = word.chars().collect();
            for piece in chars.chunks(max_chars) {
                chunks.push(piece.iter().collect::<String>().trim().to_owned());
            }
        } else {
            cur.push_str(word);
        }
    }
    if !cur.trim().is_empty() {
        chunks.push(cur.trim().to_owned());
    }
    if chunks.is_empty() {
        chunks.push(String::new());
    }
    chunks
}

/// Deterministic point id (UUID text form) for a chunk.
pub fn point_id(pipeline: &str, entity_id: &str, chunk_index: u32, model_version: &str) -> String {
    let d =
        crate::identity::content_digest(&[pipeline.as_bytes(), entity_id.as_bytes(), &chunk_index.to_be_bytes(), model_version.as_bytes()]);
    let b = hex::decode(&d[..32]).expect("hex");
    let mut u = [0u8; 16];
    u.copy_from_slice(&b);
    u[6] = (u[6] & 0x0f) | 0x50; // version-5-style marker (name based)
    u[8] = (u[8] & 0x3f) | 0x80; // RFC 4122 variant
    let h = hex::encode(u);
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// Build points for an entity's current state.
pub async fn build_points(
    pipeline: &str,
    entity: &EntityState,
    template: &crate::config::VectorTemplateConfig,
    policy: &Policy,
    embedder: &dyn EmbeddingProvider,
) -> Result<(Vec<VectorPoint>, Rendered)> {
    let caps = embedder.capabilities();
    let rendered = render(&template.template, &entity.relation, &entity.properties, policy, caps.allows_protected);
    let chunks = chunk(&rendered.text, template.chunk_chars);
    let vectors = embedder.embed(&chunks).await?;
    if vectors.len() != chunks.len() || vectors.iter().any(|v| v.len() != caps.dimensions) {
        return Err(Error::new(ErrorKind::Vector, "embedding provider returned unexpected shape"));
    }
    let n = chunks.len() as u32;
    Ok((
        chunks
            .iter()
            .zip(vectors)
            .enumerate()
            .map(|(i, (text, vector))| VectorPoint {
                id: point_id(pipeline, &entity.id, i as u32, &caps.model_version),
                vector,
                payload: PointPayload {
                    pipeline: pipeline.to_owned(),
                    entity_id: entity.id.clone(),
                    relation: entity.relation.clone(),
                    tenant: entity.tenant.clone(),
                    version: entity.version.clone(),
                    chunk_index: i as u32,
                    chunk_count: n,
                    content_digest: crate::identity::content_digest(&[text.as_bytes()]),
                    model: caps.model.clone(),
                    model_version: caps.model_version.clone(),
                    labels_json: entity.labels_json.clone(),
                },
            })
            .collect(),
        rendered,
    ))
}

/// In-memory reference vector sink (not durable; for tests and examples).
#[derive(Debug, Clone, Default)]
pub struct MemoryVectorSink {
    points: Arc<std::sync::Mutex<BTreeMap<String, VectorPoint>>>,
}

impl MemoryVectorSink {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn points(&self) -> Vec<VectorPoint> {
        self.points.lock().expect("poisoned").values().cloned().collect()
    }
    /// Brute-force cosine search (for examples of the retrieval boundary).
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(f32, VectorPoint)> {
        let mut scored: Vec<(f32, VectorPoint)> =
            self.points().into_iter().map(|p| (p.vector.iter().zip(query).map(|(a, b)| a * b).sum::<f32>(), p)).collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }
}

#[async_trait]
impl VectorSink for MemoryVectorSink {
    fn capabilities(&self) -> VectorSinkCapabilities {
        VectorSinkCapabilities { sink: "memory".into(), visibility: Visibility::Verified, delete_by_filter: true, qualified: true }
    }
    async fn preflight(&self, _dimensions: usize) -> Result<crate::capability::PreflightReport> {
        Ok(crate::capability::PreflightReport::new("vector:memory"))
    }
    async fn upsert(&self, points: &[VectorPoint]) -> Result<DeliveryReport> {
        let mut g = self.points.lock().expect("poisoned");
        for p in points {
            g.insert(p.id.clone(), p.clone());
        }
        Ok(DeliveryReport { visibility: Visibility::Verified })
    }
    async fn delete_entity(&self, pipeline: &str, entity_id: &str, keep_version: Option<&str>) -> Result<()> {
        self.points.lock().expect("poisoned").retain(|_, p| {
            !(p.payload.pipeline == pipeline && p.payload.entity_id == entity_id && keep_version != Some(p.payload.version.as_str()))
        });
        Ok(())
    }
}

/// Counters maintained by the vector worker.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorStats {
    pub delivered: u64,
    pub verified_visible: u64,
    pub visibility_unknown: u64,
    pub superseded: u64,
    pub deleted: u64,
    pub failures: u64,
    pub backlog: u64,
}

/// Components of a configured vector pipeline.
#[derive(Clone)]
pub struct VectorRuntime {
    pub sink: Arc<dyn VectorSink>,
    pub embedder: Arc<dyn EmbeddingProvider>,
    pub templates: BTreeMap<String, crate::config::VectorTemplateConfig>,
    pub batch_size: usize,
    pub max_requests_per_minute: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Profile;
    use crate::policy::{DatasetPolicy, Labels, PolicyConfig};

    fn policy() -> Policy {
        Policy::compile(
            &PolicyConfig {
                unknown_fields: Default::default(),
                datasets: vec![DatasetPolicy {
                    relation: "public.customers".into(),
                    allow_fields: None,
                    deny_fields: vec!["ssn".into()],
                    embed_fields: vec!["name".into(), "notes".into()],
                    labels: Labels::default(),
                    field_labels: [("notes".to_string(), Labels { classifications: ["CUI".to_string()].into(), ..Default::default() })]
                        .into(),
                    tenant_column: None,
                }],
            },
            Profile::Enterprise,
        )
    }

    #[test]
    fn denied_and_protected_fields_are_withheld() {
        let props: BTreeMap<String, Value> = [
            ("name".to_string(), Value::Text("Ada".into())),
            ("ssn".to_string(), Value::Text("000-00-0000".into())),
            ("notes".to_string(), Value::Text("secret".into())),
        ]
        .into();
        let r = render("{name} {ssn} {notes}", "public.customers", &props, &policy(), false);
        assert_eq!(r.text.trim(), "Ada");
        assert_eq!(r.withheld, vec!["ssn", "notes"]);
        assert!(!r.text.contains("000-00") && !r.text.contains("secret"));
    }

    #[test]
    fn chunking_is_deterministic_and_bounded() {
        let text = "word ".repeat(100);
        let a = chunk(&text, 40);
        assert_eq!(a, chunk(&text, 40));
        assert!(a.iter().all(|c| c.chars().count() <= 40));
    }

    #[test]
    fn point_ids_are_stable_uuids() {
        let a = point_id("p", "e", 0, "m1");
        assert_eq!(a, point_id("p", "e", 0, "m1"));
        assert_ne!(a, point_id("p", "e", 1, "m1"));
        assert_ne!(a, point_id("p", "e", 0, "m2"));
        assert_eq!(a.len(), 36);
    }
}
