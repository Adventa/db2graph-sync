//! Canonical change model shared by every connector and sink.
//!
//! Connectors translate native change records into [`CapturedTransaction`]s.
//! Nothing downstream of a connector interprets native positions except
//! through [`SourcePosition`]'s ordering scope rules.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// A typed, canonical field value.
///
/// Values that cannot be represented exactly as a native graph type are kept
/// in a lossless textual form (`Decimal`, `BigInt`, temporal types) and the
/// type tag is preserved, so `1` (integer) and `"1"` (text) never collide in
/// identities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    /// Integers outside i64 (e.g. unsigned 64-bit), canonical decimal text.
    BigInt(String),
    /// Floating point. NaN and infinities are preserved.
    Float(f64),
    /// Arbitrary-precision decimal, canonical text (no exponent, trimmed).
    Decimal(String),
    Text(String),
    Bytes(#[serde(with = "b64")] Vec<u8>),
    Uuid(String),
    /// ISO-8601 date `YYYY-MM-DD`.
    Date(String),
    /// ISO-8601 time without zone.
    Time(String),
    /// Timestamp without time zone, ISO-8601 text as reported by the source.
    Timestamp(String),
    /// Timestamp with time zone, normalized to UTC ISO-8601 where possible.
    TimestampTz(String),
    /// JSON document, canonical (sorted keys) serialization.
    Json(String),
    Array(Vec<Value>),
    /// A source type this build does not interpret; text form and type name kept.
    Unknown {
        type_name: String,
        text: String,
    },
}

impl Value {
    pub fn type_tag(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::BigInt(_) => 3,
            Value::Float(_) => 4,
            Value::Decimal(_) => 5,
            Value::Text(_) => 6,
            Value::Bytes(_) => 7,
            Value::Uuid(_) => 8,
            Value::Date(_) => 9,
            Value::Time(_) => 10,
            Value::Timestamp(_) => 11,
            Value::TimestampTz(_) => 12,
            Value::Json(_) => 13,
            Value::Array(_) => 14,
            Value::Unknown { .. } => 15,
        }
    }

    /// Append a canonical, unambiguous binary encoding (tag + length-prefixed
    /// payload) used for identities and content digests.
    pub fn encode_canonical(&self, out: &mut Vec<u8>) {
        out.push(self.type_tag());
        let mut put = |b: &[u8]| {
            out.extend_from_slice(&(b.len() as u64).to_be_bytes());
            out.extend_from_slice(b);
        };
        match self {
            Value::Null => put(&[]),
            Value::Bool(b) => put(&[*b as u8]),
            Value::Int(i) => put(&i.to_be_bytes()),
            Value::Float(f) => {
                // Canonicalize NaN payloads and -0.0 so equal values hash equal.
                let bits = if f.is_nan() {
                    f64::NAN.to_bits()
                } else if *f == 0.0 {
                    0u64
                } else {
                    f.to_bits()
                };
                put(&bits.to_be_bytes())
            }
            Value::BigInt(s)
            | Value::Decimal(s)
            | Value::Text(s)
            | Value::Uuid(s)
            | Value::Date(s)
            | Value::Time(s)
            | Value::Timestamp(s)
            | Value::TimestampTz(s)
            | Value::Json(s) => put(s.as_bytes()),
            Value::Bytes(b) => put(b),
            Value::Array(items) => {
                let mut inner = Vec::new();
                for v in items {
                    v.encode_canonical(&mut inner);
                }
                put(&inner)
            }
            Value::Unknown { type_name, text } => {
                let mut inner = Vec::new();
                inner.extend_from_slice(&(type_name.len() as u64).to_be_bytes());
                inner.extend_from_slice(type_name.as_bytes());
                inner.extend_from_slice(text.as_bytes());
                put(&inner)
            }
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_owned())
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

/// The state of one field in a captured image.
///
/// These five states are deliberately distinct: a sink must never write
/// `null` for a field that was merely absent from a partial image.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "s", content = "v", rename_all = "snake_case")]
pub enum FieldState {
    /// The field is present with this value (which may be `Value::Null`).
    Present(Value),
    /// The field is not part of this image (e.g. key-only old image).
    Missing,
    /// The source explicitly reported the value as unchanged and did not send
    /// it (e.g. PostgreSQL unchanged TOAST).
    Unchanged,
    /// The source could not provide the value (e.g. image not configured).
    Unavailable,
    /// The field was removed from the document (e.g. MongoDB `$unset`).
    Removed,
}

/// A row/document image: column name -> field state. Ordered for determinism.
pub type Image = BTreeMap<String, FieldState>;

/// Opaque, connector-defined position. Positions are only comparable when they
/// share the same [`OrderingScope`]; comparing positions from unrelated sources
/// (or opaque resume tokens) is a programming error and returns `None`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourcePosition {
    pub scope: OrderingScope,
    /// Monotonic within `scope` when `comparable` is true (e.g. PostgreSQL LSN).
    pub ordinal: Option<u64>,
    /// Native representation for resume (LSN text, GTID set, resume token).
    pub native: String,
}

impl SourcePosition {
    /// Compare two positions. Returns `None` when they are not comparable.
    pub fn partial_cmp_scoped(&self, other: &SourcePosition) -> Option<std::cmp::Ordering> {
        if self.scope != other.scope {
            return None;
        }
        match (self.ordinal, other.ordinal) {
            (Some(a), Some(b)) => Some(a.cmp(&b)),
            _ => None,
        }
    }
}

/// The scope within which a source guarantees ordering.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderingScope {
    /// Stable source identity (e.g. `pg:<system_identifier>`).
    pub source_id: String,
    /// Source epoch: incremented by an authorized recovery after failover or
    /// restore to a different history.
    pub epoch: u32,
    /// Sub-scope, e.g. a DynamoDB shard lineage. Empty for whole-source order.
    pub partition: String,
}

/// Entity version: totally ordered within a source scope.
///
/// Encoded as a fixed-width hex string so graph stores can compare versions
/// with plain string comparison, which every supported target implements
/// identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Version {
    pub epoch: u32,
    pub position: u64,
    pub ordinal: u32,
}

impl Version {
    pub const ZERO: Version = Version { epoch: 0, position: 0, ordinal: 0 };

    pub fn encode(&self) -> String {
        format!("{:08x}{:016x}{:08x}", self.epoch, self.position, self.ordinal)
    }

    pub fn decode(s: &str) -> Option<Version> {
        if s.len() != 32 || !s.is_ascii() {
            return None;
        }
        Some(Version {
            epoch: u32::from_str_radix(&s[0..8], 16).ok()?,
            position: u64::from_str_radix(&s[8..24], 16).ok()?,
            ordinal: u32::from_str_radix(&s[24..32], 16).ok()?,
        })
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{:X}/{}", self.epoch, self.position, self.ordinal)
    }
}

/// Fully qualified source relation name (`schema.table`, `db.collection`, table name).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RelationName(pub String);

impl fmt::Display for RelationName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// How complete the images in a change are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Completeness {
    /// Every column of the new image is present (possibly some `Unchanged`).
    FullAfterImage,
    /// Only changed fields (a delta); other fields are `Missing`.
    Delta,
    /// Only the key is known (e.g. key-only delete).
    KeyOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
    /// Snapshot read of an existing record.
    Read,
}

/// One row-level change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Change {
    pub relation: RelationName,
    pub op: ChangeOp,
    /// Ordinal of this change within its transaction (0-based).
    pub event_ordinal: u32,
    /// Key values of the record *before* the change (for deletes and key changes).
    pub old_key: Option<Image>,
    /// Old image if the source supplied one.
    pub before: Option<Image>,
    /// New image (insert/update/read).
    pub after: Option<Image>,
    pub completeness: Completeness,
    /// Schema version (digest) the change was decoded with.
    pub schema_version: String,
}

/// A truncate of one or more relations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Truncate {
    pub relations: Vec<RelationName>,
    pub event_ordinal: u32,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TxItem {
    Change(Change),
    Truncate(Truncate),
}

/// A complete source transaction (or a snapshot chunk) as assembled by a
/// connector. Supported transactions are never split or reordered downstream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapturedTransaction {
    /// Stable receipt key: identical across replays of the same source unit.
    pub unit_id: String,
    pub kind: UnitKind,
    pub source_id: String,
    pub scope: OrderingScope,
    /// Position to acknowledge to the source once this unit is recoverable.
    pub ack_position: SourcePosition,
    /// Version assigned to the effects of this unit (ordinal added per item).
    pub base_version: Version,
    /// Native transaction id, when the source exposes one.
    pub native_tx_id: Option<String>,
    /// Commit time reported by the source (RFC 3339), when available.
    pub commit_time: Option<String>,
    pub items: Vec<TxItem>,
    pub mapping_version: String,
    pub policy_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum UnitKind {
    /// A committed source transaction.
    Transaction,
    /// A chunk of an initial snapshot belonging to `generation`.
    SnapshotChunk { generation: u64, relation: RelationName, chunk: u64 },
}

mod b64 {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(v))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_encoding_orders_like_struct() {
        let a = Version { epoch: 0, position: 0x1_0000_0000, ordinal: 2 };
        let b = Version { epoch: 0, position: 0x1_0000_0000, ordinal: 10 };
        let c = Version { epoch: 1, position: 0, ordinal: 0 };
        assert!(a < b && b < c);
        assert!(a.encode() < b.encode() && b.encode() < c.encode());
        assert_eq!(Version::decode(&b.encode()), Some(b));
    }

    #[test]
    fn typed_encoding_distinguishes_int_and_text() {
        let mut a = Vec::new();
        Value::Int(1).encode_canonical(&mut a);
        let mut b = Vec::new();
        Value::Text("1".into()).encode_canonical(&mut b);
        assert_ne!(a, b);
    }

    #[test]
    fn float_zero_and_nan_canonical() {
        let enc = |f: f64| {
            let mut v = Vec::new();
            Value::Float(f).encode_canonical(&mut v);
            v
        };
        assert_eq!(enc(0.0), enc(-0.0));
        assert_eq!(enc(f64::NAN), enc(-f64::NAN));
    }

    #[test]
    fn positions_from_different_scopes_are_incomparable() {
        let s1 = OrderingScope { source_id: "pg:1".into(), epoch: 0, partition: String::new() };
        let s2 = OrderingScope { source_id: "pg:2".into(), epoch: 0, partition: String::new() };
        let a = SourcePosition { scope: s1, ordinal: Some(1), native: "0/1".into() };
        let b = SourcePosition { scope: s2, ordinal: Some(2), native: "0/2".into() };
        assert_eq!(a.partial_cmp_scoped(&b), None);
    }
}
