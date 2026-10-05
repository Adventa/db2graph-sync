//! pgoutput (protocol version 1) decoding and value conversion.
//!
//! Reference: PostgreSQL documentation, "Logical Replication Message Formats".
//! Values arrive in text format; [`text_to_value`] converts them using the
//! column type OID. The same converter is used for snapshot rows (read with
//! the simple-query protocol, i.e. the same type output functions under the
//! same session settings), so snapshot and stream produce identical values.

use bytes::{Buf, Bytes};

use crate::error::{Error, Result};
use crate::types::{FieldState, Image, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelColumn {
    pub name: String,
    pub type_oid: u32,
    pub type_mod: i32,
    /// Part of the replica identity key.
    pub key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationMsg {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<RelColumn>,
}

impl RelationMsg {
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Tuple {
    Null,
    Unchanged,
    Text(Bytes),
    Binary(Bytes),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Msg {
    Begin { final_lsn: u64, commit_ts: i64, xid: u32 },
    Commit { commit_lsn: u64, end_lsn: u64, commit_ts: i64 },
    Origin,
    Relation(RelationMsg),
    Type,
    Insert { rel: u32, new: Vec<Tuple> },
    Update { rel: u32, key: Option<Vec<Tuple>>, old: Option<Vec<Tuple>>, new: Vec<Tuple> },
    Delete { rel: u32, key: Option<Vec<Tuple>>, old: Option<Vec<Tuple>> },
    Truncate { rels: Vec<u32>, cascade: bool },
    Message,
}

fn bad(what: &str) -> Error {
    Error::new(crate::error::ErrorKind::Source, format!("pgoutput decode error: {what}"))
}

fn need(b: &Bytes, n: usize, what: &str) -> Result<()> {
    if b.remaining() < n { Err(bad(what)) } else { Ok(()) }
}

fn cstr(b: &mut Bytes) -> Result<String> {
    let end = b.iter().position(|c| *c == 0).ok_or_else(|| bad("unterminated string"))?;
    let s = String::from_utf8(b[..end].to_vec()).map_err(|_| bad("invalid UTF-8"))?;
    b.advance(end + 1);
    Ok(s)
}

fn tuple(b: &mut Bytes) -> Result<Vec<Tuple>> {
    need(b, 2, "tuple header")?;
    let n = b.get_i16();
    let mut out = Vec::with_capacity(n.max(0) as usize);
    for _ in 0..n {
        need(b, 1, "tuple kind")?;
        match b.get_u8() {
            b'n' => out.push(Tuple::Null),
            b'u' => out.push(Tuple::Unchanged),
            k @ (b't' | b'b') => {
                need(b, 4, "tuple length")?;
                let len = b.get_i32();
                if len < 0 {
                    return Err(bad("negative length"));
                }
                need(b, len as usize, "tuple data")?;
                let v = b.split_to(len as usize);
                out.push(if k == b't' { Tuple::Text(v) } else { Tuple::Binary(v) });
            }
            other => return Err(bad(&format!("unknown tuple kind {other}"))),
        }
    }
    Ok(out)
}

/// Decode one pgoutput message (the payload of an XLogData record).
pub fn decode(mut b: Bytes) -> Result<Msg> {
    need(&b, 1, "empty message")?;
    let tag = b.get_u8();
    Ok(match tag {
        b'B' => {
            need(&b, 20, "begin")?;
            Msg::Begin { final_lsn: b.get_u64(), commit_ts: b.get_i64(), xid: b.get_u32() }
        }
        b'C' => {
            need(&b, 25, "commit")?;
            let _flags = b.get_u8();
            Msg::Commit { commit_lsn: b.get_u64(), end_lsn: b.get_u64(), commit_ts: b.get_i64() }
        }
        b'O' => Msg::Origin,
        b'Y' => Msg::Type,
        b'M' => Msg::Message,
        b'R' => {
            need(&b, 4, "relation")?;
            let oid = b.get_u32();
            let namespace = cstr(&mut b)?;
            let name = cstr(&mut b)?;
            need(&b, 3, "relation header")?;
            let replica_identity = b.get_u8();
            let n = b.get_i16();
            let mut columns = Vec::with_capacity(n.max(0) as usize);
            for _ in 0..n {
                need(&b, 1, "column flags")?;
                let flags = b.get_u8();
                let cname = cstr(&mut b)?;
                need(&b, 8, "column type")?;
                columns.push(RelColumn { name: cname, type_oid: b.get_u32(), type_mod: b.get_i32(), key: flags & 1 == 1 });
            }
            Msg::Relation(RelationMsg {
                oid,
                namespace: if namespace.is_empty() { "pg_catalog".into() } else { namespace },
                name,
                replica_identity,
                columns,
            })
        }
        b'I' => {
            need(&b, 5, "insert")?;
            let rel = b.get_u32();
            if b.get_u8() != b'N' {
                return Err(bad("insert without new tuple"));
            }
            Msg::Insert { rel, new: tuple(&mut b)? }
        }
        b'U' => {
            need(&b, 5, "update")?;
            let rel = b.get_u32();
            let (mut key, mut old) = (None, None);
            let mut t = b.get_u8();
            if t == b'K' {
                key = Some(tuple(&mut b)?);
                need(&b, 1, "update new tag")?;
                t = b.get_u8();
            } else if t == b'O' {
                old = Some(tuple(&mut b)?);
                need(&b, 1, "update new tag")?;
                t = b.get_u8();
            }
            if t != b'N' {
                return Err(bad("update without new tuple"));
            }
            Msg::Update { rel, key, old, new: tuple(&mut b)? }
        }
        b'D' => {
            need(&b, 5, "delete")?;
            let rel = b.get_u32();
            match b.get_u8() {
                b'K' => Msg::Delete { rel, key: Some(tuple(&mut b)?), old: None },
                b'O' => Msg::Delete { rel, key: None, old: Some(tuple(&mut b)?) },
                _ => return Err(bad("delete without key")),
            }
        }
        b'T' => {
            need(&b, 5, "truncate")?;
            let n = b.get_u32();
            let options = b.get_u8();
            need(&b, 4 * n as usize, "truncate relations")?;
            let rels = (0..n).map(|_| b.get_u32()).collect();
            Msg::Truncate { rels, cascade: options & 1 == 1 }
        }
        other => return Err(bad(&format!("unknown message type `{}`", other as char))),
    })
}

/// Build an image from a tuple. `only_keys` keeps key columns (for 'K' tuples).
pub fn image(rel: &RelationMsg, t: &[Tuple], only_keys: bool) -> Result<Image> {
    if t.len() != rel.columns.len() {
        return Err(bad(&format!("tuple has {} columns, relation {} has {}", t.len(), rel.qualified(), rel.columns.len())));
    }
    let mut img = Image::new();
    for (c, v) in rel.columns.iter().zip(t) {
        if only_keys && !c.key {
            continue;
        }
        let state = match v {
            Tuple::Null => FieldState::Present(Value::Null),
            Tuple::Unchanged => FieldState::Unchanged,
            Tuple::Text(b) => {
                let s = std::str::from_utf8(b).map_err(|_| bad("non-UTF-8 text value"))?;
                FieldState::Present(text_to_value(c.type_oid, s, None))
            }
            Tuple::Binary(_) => return Err(bad("binary format values are not requested and not supported")),
        };
        img.insert(c.name.clone(), state);
    }
    Ok(img)
}

/// Canonical decimal text: trims trailing fractional zeros. `NaN` kept.
pub fn canonical_decimal(s: &str) -> String {
    let s = s.trim();
    if !s.contains('.') || s.contains(['e', 'E']) {
        return s.to_owned();
    }
    let t = s.trim_end_matches('0').trim_end_matches('.');
    if t.is_empty() || t == "-" { "0".to_owned() } else { t.to_owned() }
}

/// Convert PostgreSQL text output (ISO DateStyle, UTC, hex bytea) to a value.
pub fn text_to_value(type_oid: u32, s: &str, type_name: Option<&str>) -> Value {
    match type_oid {
        16 => Value::Bool(s == "t"),
        20 | 21 | 23 | 26 => s.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::BigInt(s.to_owned())),
        700 | 701 => match s {
            "NaN" => Value::Float(f64::NAN),
            "Infinity" => Value::Float(f64::INFINITY),
            "-Infinity" => Value::Float(f64::NEG_INFINITY),
            _ => s.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::Unknown { type_name: "float".into(), text: s.into() }),
        },
        1700 => Value::Decimal(canonical_decimal(s)),
        25 | 1043 | 1042 | 19 | 142 => Value::Text(s.to_owned()),
        2950 => Value::Uuid(s.to_lowercase()),
        17 => match s.strip_prefix("\\x").map(hex::decode) {
            Some(Ok(b)) => Value::Bytes(b),
            _ => Value::Unknown { type_name: "bytea".into(), text: s.into() },
        },
        1082 => Value::Date(s.to_owned()),
        1083 | 1266 => Value::Time(s.to_owned()),
        1114 => Value::Timestamp(s.replacen(' ', "T", 1)),
        1184 => Value::TimestampTz(normalize_tz(s)),
        114 | 3802 => Value::Json(s.to_owned()),
        _ => Value::Unknown { type_name: type_name.map(str::to_owned).unwrap_or_else(|| format!("oid:{type_oid}")), text: s.to_owned() },
    }
}

/// `2024-01-02 03:04:05.123456+00` -> `2024-01-02T03:04:05.123456Z` (UTC session).
fn normalize_tz(s: &str) -> String {
    let t = s.replacen(' ', "T", 1);
    if let Some(stripped) = t.strip_suffix("+00") { format!("{stripped}Z") } else { t }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn rel_msg() -> Bytes {
        let mut b = BytesMut::new();
        b.put_u8(b'R');
        b.put_u32(16384);
        b.put_slice(b"public\0orders\0");
        b.put_u8(b'd');
        b.put_i16(2);
        b.put_u8(1);
        b.put_slice(b"id\0");
        b.put_u32(20);
        b.put_i32(-1);
        b.put_u8(0);
        b.put_slice(b"note\0");
        b.put_u32(25);
        b.put_i32(-1);
        b.freeze()
    }

    #[test]
    fn decodes_relation_update_with_unchanged_toast() {
        let Msg::Relation(r) = decode(rel_msg()).unwrap() else { panic!() };
        assert_eq!(r.qualified(), "public.orders");
        assert!(r.columns[0].key && !r.columns[1].key);

        let mut b = BytesMut::new();
        b.put_u8(b'U');
        b.put_u32(16384);
        b.put_u8(b'K');
        b.put_i16(2);
        b.put_u8(b't');
        b.put_i32(1);
        b.put_slice(b"7");
        b.put_u8(b'n');
        b.put_u8(b'N');
        b.put_i16(2);
        b.put_u8(b't');
        b.put_i32(1);
        b.put_slice(b"8");
        b.put_u8(b'u');
        let Msg::Update { key, new, old, .. } = decode(b.freeze()).unwrap() else { panic!() };
        assert!(old.is_none());
        let key = image(&r, &key.unwrap(), true).unwrap();
        assert_eq!(key.len(), 1);
        assert_eq!(key["id"], FieldState::Present(Value::Int(7)));
        let new = image(&r, &new, false).unwrap();
        assert_eq!(new["id"], FieldState::Present(Value::Int(8)));
        assert_eq!(new["note"], FieldState::Unchanged);
    }

    #[test]
    fn decodes_begin_commit_truncate() {
        let mut b = BytesMut::new();
        b.put_u8(b'B');
        b.put_u64(100);
        b.put_i64(5);
        b.put_u32(42);
        assert_eq!(decode(b.freeze()).unwrap(), Msg::Begin { final_lsn: 100, commit_ts: 5, xid: 42 });
        let mut b = BytesMut::new();
        b.put_u8(b'C');
        b.put_u8(0);
        b.put_u64(100);
        b.put_u64(120);
        b.put_i64(5);
        assert_eq!(decode(b.freeze()).unwrap(), Msg::Commit { commit_lsn: 100, end_lsn: 120, commit_ts: 5 });
        let mut b = BytesMut::new();
        b.put_u8(b'T');
        b.put_u32(2);
        b.put_u8(1);
        b.put_u32(1);
        b.put_u32(2);
        assert_eq!(decode(b.freeze()).unwrap(), Msg::Truncate { rels: vec![1, 2], cascade: true });
    }

    #[test]
    fn truncated_input_is_an_error_not_a_panic() {
        let full = rel_msg();
        for n in 1..full.len() {
            let _ = decode(full.slice(..n));
        }
        assert!(decode(Bytes::from_static(b"U")).is_err());
    }

    #[test]
    fn value_conversion() {
        assert_eq!(text_to_value(1700, "10.500", None), Value::Decimal("10.5".into()));
        assert_eq!(text_to_value(1700, "10.000", None), Value::Decimal("10".into()));
        assert_eq!(text_to_value(1700, "NaN", None), Value::Decimal("NaN".into()));
        assert_eq!(text_to_value(17, "\\x0aff", None), Value::Bytes(vec![10, 255]));
        assert_eq!(text_to_value(1184, "2024-01-02 03:04:05.5+00", None), Value::TimestampTz("2024-01-02T03:04:05.5Z".into()));
        assert_eq!(text_to_value(20, "9223372036854775807", None), Value::Int(i64::MAX));
        assert_eq!(text_to_value(16, "t", None), Value::Bool(true));
        assert!(matches!(text_to_value(1186, "P1D", Some("interval")), Value::Unknown { .. }));
        assert!(matches!(text_to_value(701, "NaN", None), Value::Float(f) if f.is_nan()));
    }
}
