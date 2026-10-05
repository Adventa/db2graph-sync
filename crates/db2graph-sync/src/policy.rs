//! Data policy: dataset/field allowlists, deny rules, classification labels,
//! embedding approval, and retrieval-time authorization.
//!
//! Policy is applied *before* captured work is journaled, so denied fields are
//! never persisted, projected, or embedded. Key columns are always read (they
//! define identity) but are only projected as properties when allowed.
//!
//! Classification, CUI categories, export control, tenant and releasability
//! are modeled as independent dimensions. A principal must satisfy every
//! dimension; none implies another.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::config::Profile;
use crate::identity::content_digest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FieldDefault {
    /// Fields not mentioned by an allowlist are projected (development default).
    #[default]
    Allow,
    /// Fields must be explicitly allowed (forced in the restricted profile).
    Deny,
}

/// Security labels attached to a dataset or field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Labels {
    /// Classification markings (free-form, e.g. `CUI`, `CONFIDENTIAL`). A
    /// principal must hold **every** marking present. Configuration accepts
    /// `classification = "X"` or `classifications = ["X", "Y"]`.
    #[serde(default, alias = "classification", deserialize_with = "one_or_many")]
    pub classifications: BTreeSet<String>,
    #[serde(default)]
    pub cui_categories: BTreeSet<String>,
    /// Export-control markings (e.g. `ITAR`, `EAR99`); each must be authorized.
    #[serde(default, deserialize_with = "one_or_many")]
    pub export_control: BTreeSet<String>,
    /// Country/organization codes the data may be released to. Empty means
    /// no releasability restriction is recorded.
    #[serde(default)]
    pub releasability: BTreeSet<String>,
}

fn one_or_many<'de, D: serde::Deserializer<'de>>(d: D) -> Result<BTreeSet<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(BTreeSet<String>),
    }
    Ok(match OneOrMany::deserialize(d)? {
        OneOrMany::One(s) => [s].into(),
        OneOrMany::Many(m) => m,
    })
}

impl Labels {
    pub fn is_protected(&self) -> bool {
        !self.classifications.is_empty() || !self.cui_categories.is_empty() || !self.export_control.is_empty()
    }

    /// Most restrictive combination of two label sets: every marking and
    /// category is required, and releasability is intersected.
    pub fn union(&self, other: &Labels) -> Labels {
        let releasability = match (self.releasability.is_empty(), other.releasability.is_empty()) {
            (true, _) => other.releasability.clone(),
            (_, true) => self.releasability.clone(),
            _ => self.releasability.intersection(&other.releasability).cloned().collect(),
        };
        Labels {
            classifications: self.classifications.union(&other.classifications).cloned().collect(),
            cui_categories: self.cui_categories.union(&other.cui_categories).cloned().collect(),
            export_control: self.export_control.union(&other.export_control).cloned().collect(),
            releasability,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DatasetPolicy {
    pub relation: String,
    /// When set, only these fields are projected (plus nothing else).
    #[serde(default)]
    pub allow_fields: Option<Vec<String>>,
    #[serde(default)]
    pub deny_fields: Vec<String>,
    /// Fields approved for inclusion in embedding text.
    #[serde(default)]
    pub embed_fields: Vec<String>,
    #[serde(default)]
    pub labels: Labels,
    #[serde(default)]
    pub field_labels: BTreeMap<String, Labels>,
    /// Column holding the tenant id, when tenancy is row-level. Rows whose
    /// tenant does not equal `pipeline.tenant` are excluded.
    #[serde(default)]
    pub tenant_column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    #[serde(default)]
    pub unknown_fields: FieldDefault,
    #[serde(default)]
    pub datasets: Vec<DatasetPolicy>,
}

impl PolicyConfig {
    pub fn findings(&self, profile: Profile) -> Vec<String> {
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        for d in &self.datasets {
            if !seen.insert(d.relation.clone()) {
                out.push(format!("dataset `{}` is listed more than once", d.relation));
            }
            if let Some(allow) = &d.allow_fields {
                for f in &d.deny_fields {
                    if allow.contains(f) {
                        out.push(format!("`{}.{f}` is both allowed and denied (deny wins; remove one)", d.relation));
                    }
                }
            }
            if profile == Profile::Restricted && d.allow_fields.is_none() {
                out.push(format!("restricted profile: dataset `{}` needs an explicit allow_fields list", d.relation));
            }
        }
        out
    }
}

/// Whether a row's tenant column value equals the pipeline tenant. Compares
/// the canonical text of scalar values, so integer, UUID and text tenant
/// columns all work; NULL, missing or non-scalar values never match.
pub fn tenant_matches(state: Option<&crate::types::FieldState>, tenant: &str) -> bool {
    use crate::types::{FieldState, Value};
    match state {
        Some(FieldState::Present(v)) => match v {
            Value::Int(i) => i.to_string() == tenant,
            Value::Text(s) | Value::Uuid(s) | Value::BigInt(s) | Value::Decimal(s) => s == tenant,
            _ => false,
        },
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
}

/// Compiled, versioned policy.
#[derive(Debug, Clone)]
pub struct Policy {
    profile: Profile,
    default: FieldDefault,
    datasets: BTreeMap<String, DatasetPolicy>,
    version: String,
}

impl Policy {
    pub fn compile(cfg: &PolicyConfig, profile: Profile) -> Policy {
        let default = if profile == Profile::Restricted { FieldDefault::Deny } else { cfg.unknown_fields };
        let datasets: BTreeMap<String, DatasetPolicy> = cfg.datasets.iter().map(|d| (d.relation.clone(), d.clone())).collect();
        let bytes = serde_json::to_vec(&(profile, default, &cfg.datasets)).expect("policy serializes");
        let version = content_digest(&[&bytes])[..16].to_owned();
        Policy { profile, default, datasets, version }
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn profile(&self) -> Profile {
        self.profile
    }

    /// Whether a relation may be captured at all.
    pub fn relation_allowed(&self, relation: &str) -> bool {
        if self.datasets.is_empty() {
            // No allowlist configured: only acceptable in development
            // (validation rejects this elsewhere).
            return self.profile == Profile::Development;
        }
        self.datasets.contains_key(relation)
    }

    pub fn dataset(&self, relation: &str) -> Option<&DatasetPolicy> {
        self.datasets.get(relation)
    }

    /// Whether a field may be projected / persisted.
    pub fn field(&self, relation: &str, field: &str) -> Decision {
        if !self.relation_allowed(relation) {
            return Decision::Deny;
        }
        let Some(d) = self.datasets.get(relation) else {
            return if self.default == FieldDefault::Allow { Decision::Allow } else { Decision::Deny };
        };
        if d.deny_fields.iter().any(|f| f == field) {
            return Decision::Deny;
        }
        match &d.allow_fields {
            Some(allow) if allow.iter().any(|f| f == field) => Decision::Allow,
            Some(_) => Decision::Deny,
            None if self.default == FieldDefault::Allow => Decision::Allow,
            None => Decision::Deny,
        }
    }

    /// Whether a field may be included in text sent to an embedding provider.
    pub fn embeddable(&self, relation: &str, field: &str, provider_allows_protected: bool) -> bool {
        if self.field(relation, field) == Decision::Deny {
            return false;
        }
        let Some(d) = self.datasets.get(relation) else {
            // Development without dataset policy: allow unless restricted.
            return self.profile == Profile::Development;
        };
        if !d.embed_fields.iter().any(|f| f == field) {
            return false;
        }
        let labels = self.field_labels(relation, field);
        !labels.is_protected() || (provider_allows_protected && self.profile != Profile::Restricted)
    }

    pub fn relation_labels(&self, relation: &str) -> Labels {
        self.datasets.get(relation).map(|d| d.labels.clone()).unwrap_or_default()
    }

    pub fn field_labels(&self, relation: &str, field: &str) -> Labels {
        let Some(d) = self.datasets.get(relation) else { return Labels::default() };
        match d.field_labels.get(field) {
            Some(fl) => d.labels.union(fl),
            None => d.labels.clone(),
        }
    }

    /// Labels for an entity whose projected fields are `fields`.
    pub fn entity_labels<'a>(&self, relation: &str, fields: impl IntoIterator<Item = &'a str>) -> Labels {
        let mut l = self.relation_labels(relation);
        for f in fields {
            if let Some(fl) = self.datasets.get(relation).and_then(|d| d.field_labels.get(f)) {
                l = l.union(fl);
            }
        }
        l
    }
}

/// The caller on whose behalf retrieval happens.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Principal {
    pub tenant: String,
    /// Classification markings the principal may see. Unclassified data
    /// (no marking) is always visible to principals of the same tenant.
    #[serde(default)]
    pub classifications: BTreeSet<String>,
    #[serde(default)]
    pub cui_categories: BTreeSet<String>,
    /// Export-control markings the principal is authorized for.
    #[serde(default)]
    pub export_authorizations: BTreeSet<String>,
    /// The principal's releasability code (e.g. country code).
    #[serde(default)]
    pub releasability_code: Option<String>,
}

/// Why an entity was found ineligible. Each dimension is reported separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Ineligible {
    NotFound,
    Deleted,
    TenantMismatch,
    DatasetNotAllowed,
    Classification,
    CuiCategory,
    ExportControl,
    Releasability,
    StaleVersion,
}

/// Evaluate a principal against entity labels.
pub fn authorize(principal: &Principal, entity_tenant: &str, labels: &Labels) -> Result<(), Ineligible> {
    if principal.tenant != entity_tenant {
        return Err(Ineligible::TenantMismatch);
    }
    if !labels.classifications.is_subset(&principal.classifications) {
        return Err(Ineligible::Classification);
    }
    if !labels.cui_categories.is_subset(&principal.cui_categories) {
        return Err(Ineligible::CuiCategory);
    }
    if !labels.export_control.is_subset(&principal.export_authorizations) {
        return Err(Ineligible::ExportControl);
    }
    if !labels.releasability.is_empty() {
        match &principal.releasability_code {
            Some(code) if labels.releasability.contains(code) => {}
            _ => return Err(Ineligible::Releasability),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> PolicyConfig {
        PolicyConfig {
            unknown_fields: FieldDefault::Allow,
            datasets: vec![DatasetPolicy {
                relation: "public.customers".into(),
                allow_fields: None,
                deny_fields: vec!["ssn".into()],
                embed_fields: vec!["name".into(), "notes".into()],
                labels: Labels::default(),
                field_labels: [("notes".to_string(), Labels { classifications: ["CUI".to_string()].into(), ..Default::default() })].into(),
                tenant_column: None,
            }],
        }
    }

    #[test]
    fn deny_wins_and_unlisted_relation_denied() {
        let p = Policy::compile(&cfg(), Profile::Enterprise);
        assert_eq!(p.field("public.customers", "ssn"), Decision::Deny);
        assert_eq!(p.field("public.customers", "name"), Decision::Allow);
        assert_eq!(p.field("public.secrets", "x"), Decision::Deny);
    }

    #[test]
    fn restricted_forces_default_deny() {
        let p = Policy::compile(&cfg(), Profile::Restricted);
        assert_eq!(p.field("public.customers", "name"), Decision::Deny);
    }

    #[test]
    fn protected_fields_not_embedded_without_authorization() {
        let p = Policy::compile(&cfg(), Profile::Enterprise);
        assert!(p.embeddable("public.customers", "name", false));
        assert!(!p.embeddable("public.customers", "notes", false));
        assert!(p.embeddable("public.customers", "notes", true));
        assert!(!p.embeddable("public.customers", "ssn", true));
        let r = Policy::compile(&cfg(), Profile::Restricted);
        assert!(!r.embeddable("public.customers", "notes", true));
    }

    #[test]
    fn tenant_matching_handles_non_text_columns() {
        use crate::types::{FieldState, Value};
        assert!(tenant_matches(Some(&FieldState::Present(Value::Int(42))), "42"));
        assert!(tenant_matches(Some(&FieldState::Present(Value::Text("acme".into()))), "acme"));
        assert!(!tenant_matches(Some(&FieldState::Present(Value::Int(42))), "acme"));
        assert!(!tenant_matches(Some(&FieldState::Present(Value::Null)), "acme"));
        assert!(!tenant_matches(Some(&FieldState::Unchanged), "acme"));
        assert!(!tenant_matches(None, "acme"));
    }

    #[test]
    fn combined_markings_require_every_marking() {
        let dataset = Labels { classifications: ["CUI".to_string()].into(), ..Default::default() };
        let field: Labels = toml::from_str("classification = \"CONFIDENTIAL\"").unwrap();
        let both = dataset.union(&field);
        assert_eq!(both.classifications.len(), 2, "union never drops a marking");
        let mut p = Principal { tenant: "t".into(), classifications: ["CUI".to_string()].into(), ..Default::default() };
        assert_eq!(authorize(&p, "t", &both), Err(Ineligible::Classification));
        p.classifications.insert("CONFIDENTIAL".into());
        assert_eq!(authorize(&p, "t", &both), Ok(()));
        let many: Labels = toml::from_str("classifications = [\"A\", \"B\"]\nexport_control = \"ITAR\"").unwrap();
        assert_eq!(many.classifications.len(), 2);
        assert!(many.export_control.contains("ITAR"));
    }

    #[test]
    fn authorization_dimensions_are_independent() {
        let labels = Labels {
            classifications: ["CUI".to_string()].into(),
            cui_categories: ["PRVCY".to_string()].into(),
            export_control: Default::default(),
            releasability: ["AUS".to_string()].into(),
        };
        let mut p = Principal { tenant: "t".into(), ..Default::default() };
        assert_eq!(authorize(&p, "t", &labels), Err(Ineligible::Classification));
        p.classifications.insert("CUI".into());
        assert_eq!(authorize(&p, "t", &labels), Err(Ineligible::CuiCategory));
        p.cui_categories.insert("PRVCY".into());
        assert_eq!(authorize(&p, "t", &labels), Err(Ineligible::Releasability));
        p.releasability_code = Some("AUS".into());
        assert_eq!(authorize(&p, "t", &labels), Ok(()));
        assert_eq!(authorize(&p, "other", &labels), Err(Ineligible::TenantMismatch));
    }
}
