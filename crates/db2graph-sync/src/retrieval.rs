//! Authorized retrieval boundary.
//!
//! Vector indexes are eventually consistent copies. Before a GraphRAG
//! application uses vector hits, it passes them through
//! [`RetrievalValidator::validate`], which checks — against the **current**
//! graph and the **current** policy — that each hit's entity:
//!
//! * exists and is not deleted,
//! * belongs to the caller's tenant,
//! * is in an allowed dataset,
//! * satisfies classification, CUI category, export-control and
//!   releasability for the principal (each dimension independently),
//! * and (optionally) is at the version the hit was indexed from.
//!
//! Deleted or newly restricted entities therefore become ineligible at this
//! boundary immediately, even while physical vector cleanup is pending.
//! Applications that query the vector store directly and skip this boundary
//! do **not** inherit these guarantees.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::policy::{Ineligible, Labels, Policy, Principal, authorize};
use crate::sink::{EntityState, GraphSink};

/// A candidate returned by a vector search.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hit {
    pub entity_id: String,
    /// Version recorded in the point payload.
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum VersionRule {
    /// Reject hits whose indexed version differs from the current entity.
    #[default]
    ExactVersion,
    /// Accept older index versions (text may be stale; entity is authorized).
    AnyVersion,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum Verdict {
    Eligible { hit: Hit, entity: Box<EntityState> },
    Ineligible { hit: Hit, reason: Ineligible },
}

impl Verdict {
    pub fn is_eligible(&self) -> bool {
        matches!(self, Verdict::Eligible { .. })
    }
}

/// Validates vector hits for a principal. Cheap to clone.
#[derive(Clone)]
pub struct RetrievalValidator {
    sink: Arc<dyn GraphSink>,
    policy: Arc<RwLock<Policy>>,
    pipeline: String,
}

impl RetrievalValidator {
    pub fn new(sink: Arc<dyn GraphSink>, policy: Arc<RwLock<Policy>>, pipeline: impl Into<String>) -> Self {
        Self { sink, policy, pipeline: pipeline.into() }
    }

    pub async fn validate(&self, principal: &Principal, hits: &[Hit], rule: VersionRule) -> Result<Vec<Verdict>> {
        let ids: Vec<String> = hits.iter().map(|h| h.entity_id.clone()).collect();
        let found: HashMap<String, EntityState> =
            self.sink.lookup(&self.pipeline, &ids).await?.into_iter().map(|e| (e.id.clone(), e)).collect();
        let policy = self.policy.read().expect("policy lock poisoned").clone();
        Ok(hits
            .iter()
            .map(|h| match check(&policy, principal, h, found.get(&h.entity_id), rule) {
                Ok(e) => Verdict::Eligible { hit: h.clone(), entity: Box::new(e.clone()) },
                Err(reason) => Verdict::Ineligible { hit: h.clone(), reason },
            })
            .collect())
    }
}

fn check<'a>(
    policy: &Policy,
    principal: &Principal,
    hit: &Hit,
    entity: Option<&'a EntityState>,
    rule: VersionRule,
) -> Result<&'a EntityState, Ineligible> {
    let e = entity.ok_or(Ineligible::NotFound)?;
    if e.deleted {
        return Err(Ineligible::Deleted);
    }
    if !policy.relation_allowed(&e.relation) {
        return Err(Ineligible::DatasetNotAllowed);
    }
    // Labels stored at write time, combined with the labels the *current*
    // policy assigns (so a newly restricted dataset is restricted now).
    let stored: Labels = serde_json::from_str(&e.labels_json).unwrap_or_default();
    let current = policy.entity_labels(&e.relation, e.properties.keys().map(String::as_str));
    authorize(principal, &e.tenant, &stored.union(&current))?;
    if rule == VersionRule::ExactVersion && e.version != hit.version {
        return Err(Ineligible::StaleVersion);
    }
    Ok(e)
}
