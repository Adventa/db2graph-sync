//! Canonical entity identities.
//!
//! `entity_id = hex(SHA-256(domain || tenant || source || relation || key_name || typed key))`
//!
//! Every component is length-prefixed, so no concatenation of different inputs
//! can produce the same preimage. The key encoding is typed
//! ([`Value::encode_canonical`]), so integer `1` and text `"1"` yield different
//! identities, and composite keys are encoded in declared key-column order.
//!
//! The same function, with a unique-constraint name as `key_name`, produces the
//! lookup digest for relationships that reference a unique constraint rather
//! than the primary key. The primary key uses the empty key name, so a
//! primary-key reference digest equals the entity id.

use sha2::{Digest, Sha256};

use crate::types::Value;

const DOMAIN: &[u8] = b"db2graph-sync/entity/v1";

/// Inputs for an identity digest.
#[derive(Debug, Clone, Copy)]
pub struct IdentityScope<'a> {
    pub tenant: &'a str,
    pub source_id: &'a str,
    /// Stable relation name from the mapping (not a volatile OID).
    pub relation: &'a str,
}

fn put(h: &mut Sha256, b: &[u8]) {
    h.update((b.len() as u64).to_be_bytes());
    h.update(b);
}

/// Digest for a key (primary key when `key_name` is empty).
pub fn key_digest(scope: IdentityScope<'_>, key_name: &str, values: &[&Value]) -> String {
    let mut h = Sha256::new();
    put(&mut h, DOMAIN);
    put(&mut h, scope.tenant.as_bytes());
    put(&mut h, scope.source_id.as_bytes());
    put(&mut h, scope.relation.as_bytes());
    put(&mut h, key_name.as_bytes());
    h.update((values.len() as u64).to_be_bytes());
    let mut buf = Vec::new();
    for v in values {
        buf.clear();
        v.encode_canonical(&mut buf);
        put(&mut h, &buf);
    }
    hex::encode(h.finalize())
}

/// The entity id for a primary key.
pub fn entity_id(scope: IdentityScope<'_>, pk: &[&Value]) -> String {
    key_digest(scope, "", pk)
}

/// Content digest over arbitrary canonical bytes (receipts, chunk ids).
pub fn content_digest(parts: &[&[u8]]) -> String {
    let mut h = Sha256::new();
    put(&mut h, b"db2graph-sync/content/v1");
    for p in parts {
        put(&mut h, p);
    }
    hex::encode(h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s<'a>(t: &'a str) -> IdentityScope<'a> {
        IdentityScope { tenant: t, source_id: "pg:1", relation: "public.orders" }
    }

    #[test]
    fn deterministic_and_tenant_scoped() {
        let k = Value::Int(7);
        assert_eq!(entity_id(s("a"), &[&k]), entity_id(s("a"), &[&k]));
        assert_ne!(entity_id(s("a"), &[&k]), entity_id(s("b"), &[&k]));
    }

    #[test]
    fn composite_key_boundaries_are_unambiguous() {
        let a = [Value::Text("ab".into()), Value::Text("c".into())];
        let b = [Value::Text("a".into()), Value::Text("bc".into())];
        assert_ne!(entity_id(s("t"), &[&a[0], &a[1]]), entity_id(s("t"), &[&b[0], &b[1]]));
    }

    #[test]
    fn composite_key_order_matters() {
        let x = Value::Int(1);
        let y = Value::Int(2);
        assert_ne!(entity_id(s("t"), &[&x, &y]), entity_id(s("t"), &[&y, &x]));
    }

    #[test]
    fn unique_key_digest_differs_from_pk() {
        let v = Value::Text("x@example.test".into());
        assert_ne!(key_digest(s("t"), "", &[&v]), key_digest(s("t"), "customers_email_key", &[&v]));
    }
}
