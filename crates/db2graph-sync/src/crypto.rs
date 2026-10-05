//! Cryptographic provider abstraction and the default provider.
//!
//! All durable-state encryption, integrity chaining and digests used for
//! security decisions go through a [`CryptoProvider`]. The default
//! [`RustCryptoProvider`] (AES-256-GCM, SHA-256, HMAC-SHA-256 from the RustCrypto
//! project) is **not** a FIPS 140-3 validated module and reports
//! [`Validation::NotValidated`]. The restricted profile refuses to start with a
//! provider that does not report a validated module *and* pass its self-tests.
//!
//! A provider reporting `Validation::Fips140_3` is making a claim on behalf
//! of the integrator; the claim (module name, certificate) is recorded in the
//! audit log and in evidence exports so that it can be checked. A cargo
//! feature alone is never treated as validation.

use std::collections::BTreeMap;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::error::{Error, ErrorKind, Result};

/// Validation status reported by a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Validation {
    NotValidated,
    /// The integrator asserts that every operation of this provider runs
    /// inside the named FIPS 140-3 validated module in an approved mode.
    Fips140_3 {
        module: String,
        certificate: String,
    },
}

/// Cryptographic operations required by the engine.
pub trait CryptoProvider: Send + Sync {
    fn name(&self) -> &str;
    fn validation(&self) -> Validation;
    /// Known-answer self-tests. Must fail closed.
    fn self_test(&self) -> Result<()>;
    fn aead_seal(&self, key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>>;
    fn aead_open(&self, key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>>;
    fn random(&self, buf: &mut [u8]) -> Result<()>;
    fn sha256(&self, data: &[u8]) -> [u8; 32];
    fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> [u8; 32];
}

/// Default provider backed by RustCrypto crates. Not FIPS validated.
#[derive(Debug, Default, Clone, Copy)]
pub struct RustCryptoProvider;

impl CryptoProvider for RustCryptoProvider {
    fn name(&self) -> &str {
        "rustcrypto-aes256gcm"
    }
    fn validation(&self) -> Validation {
        Validation::NotValidated
    }
    fn self_test(&self) -> Result<()> {
        run_known_answer_tests(self)
    }
    fn aead_seal(&self, key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| Error::internal("bad key length"))?;
        cipher.encrypt(Nonce::from_slice(nonce), Payload { msg: plaintext, aad }).map_err(|_| Error::internal("AEAD seal failed"))
    }
    fn aead_open(&self, key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| Error::internal("bad key length"))?;
        cipher
            .decrypt(Nonce::from_slice(nonce), Payload { msg: ciphertext, aad })
            .map_err(|_| Error::new(ErrorKind::StateTampered, "authenticated decryption failed (tampering or wrong key)"))
    }
    fn random(&self, buf: &mut [u8]) -> Result<()> {
        getrandom::fill(buf).map_err(|e| Error::internal(format!("OS random source failed: {e}")))
    }
    fn sha256(&self, data: &[u8]) -> [u8; 32] {
        Sha256::digest(data).into()
    }
    fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> [u8; 32] {
        let mut m = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
        m.update(data);
        m.finalize().into_bytes().into()
    }
}

/// Known-answer tests shared by providers.
pub fn run_known_answer_tests(p: &dyn CryptoProvider) -> Result<()> {
    let fail = |what: &str| Error::new(ErrorKind::Security, format!("crypto self-test failed: {what}"));
    // AES-256-GCM, McGrew & Viega GCM specification test case 14.
    let key = [0u8; 32];
    let nonce = [0u8; 12];
    let ct = p.aead_seal(&key, &nonce, &[], &[0u8; 16])?;
    let expected = hex::decode("cea7403d4d606b6e074ec5d3baf39d18d0d1c8a799996bf0265b98b5d48ab919").expect("hex");
    if ct != expected {
        return Err(fail("AES-256-GCM encrypt KAT"));
    }
    if p.aead_open(&key, &nonce, &[], &ct)? != [0u8; 16] {
        return Err(fail("AES-256-GCM decrypt KAT"));
    }
    let mut tampered = ct.clone();
    tampered[0] ^= 1;
    if p.aead_open(&key, &nonce, &[], &tampered).is_ok() {
        return Err(fail("AES-256-GCM tag verification"));
    }
    // SHA-256("abc"), FIPS 180-4 example.
    if hex::encode(p.sha256(b"abc")) != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
        return Err(fail("SHA-256 KAT"));
    }
    // HMAC-SHA-256, RFC 4231 test case 2.
    if hex::encode(p.hmac_sha256(b"Jefe", b"what do ya want for nothing?"))
        != "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    {
        return Err(fail("HMAC-SHA-256 KAT"));
    }
    let mut r = [0u8; 32];
    p.random(&mut r)?;
    if r == [0u8; 32] {
        return Err(fail("random source returned zeros"));
    }
    Ok(())
}

/// Versioned data-encryption keys.
pub struct Keyring {
    keys: BTreeMap<u32, Zeroizing<[u8; 32]>>,
    active: u32,
}

impl std::fmt::Debug for Keyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keyring").field("key_ids", &self.keys.keys().collect::<Vec<_>>()).field("active", &self.active).finish()
    }
}

impl Keyring {
    pub fn new(active: u32) -> Self {
        Self { keys: BTreeMap::new(), active }
    }

    /// Add a key from its base64 encoding (32 bytes after decoding).
    pub fn add_base64(&mut self, id: u32, b64: &str) -> Result<()> {
        use base64::Engine;
        let raw = Zeroizing::new(
            base64::engine::general_purpose::STANDARD
                .decode(b64.trim())
                .map_err(|_| Error::new(ErrorKind::Secret, format!("state key {id} is not valid base64")))?,
        );
        let arr: [u8; 32] = raw
            .as_slice()
            .try_into()
            .map_err(|_| Error::new(ErrorKind::Secret, format!("state key {id} must decode to exactly 32 bytes")))?;
        self.keys.insert(id, Zeroizing::new(arr));
        Ok(())
    }

    pub fn active_id(&self) -> u32 {
        self.active
    }

    pub fn ids(&self) -> Vec<u32> {
        self.keys.keys().copied().collect()
    }

    fn get(&self, id: u32) -> Result<&[u8; 32]> {
        self.keys.get(&id).map(|k| &**k).ok_or_else(|| {
            Error::new(ErrorKind::Secret, format!("state key {id} is required to read existing state but is not configured"))
        })
    }
}

/// Envelope format: `"D2G1" | key_id:u32be | nonce:12 | ciphertext+tag`.
/// The AAD binds the ciphertext to its location (`context`) and header so
/// records cannot be swapped between tables, keys or stores undetected.
#[derive(Clone)]
pub struct Sealer {
    provider: Arc<dyn CryptoProvider>,
    keyring: Arc<Keyring>,
}

const MAGIC: &[u8; 4] = b"D2G1";

impl Sealer {
    pub fn new(provider: Arc<dyn CryptoProvider>, keyring: Arc<Keyring>) -> Self {
        Self { provider, keyring }
    }

    pub fn provider(&self) -> &Arc<dyn CryptoProvider> {
        &self.provider
    }

    pub fn active_key_id(&self) -> u32 {
        self.keyring.active
    }

    pub fn seal(&self, context: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        self.seal_with(self.keyring.active, context, plaintext)
    }

    pub fn seal_with(&self, key_id: u32, context: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let key = self.keyring.get(key_id)?;
        let mut nonce = [0u8; 12];
        self.provider.random(&mut nonce)?;
        let mut header = Vec::with_capacity(20);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&key_id.to_be_bytes());
        header.extend_from_slice(&nonce);
        let aad = [&header[..8], context].concat();
        let ct = self.provider.aead_seal(key, &nonce, &aad, plaintext)?;
        header.extend_from_slice(&ct);
        Ok(header)
    }

    /// Returns (key_id, plaintext).
    pub fn open(&self, context: &[u8], envelope: &[u8]) -> Result<(u32, Vec<u8>)> {
        if envelope.len() < 20 + 16 || &envelope[..4] != MAGIC {
            return Err(Error::new(ErrorKind::StateTampered, "state record has an invalid envelope"));
        }
        let key_id = u32::from_be_bytes(envelope[4..8].try_into().expect("len checked"));
        let nonce: [u8; 12] = envelope[8..20].try_into().expect("len checked");
        let key = self.keyring.get(key_id)?;
        let aad = [&envelope[..8], context].concat();
        let pt = self.provider.aead_open(key, &nonce, &aad, &envelope[20..])?;
        Ok((key_id, pt))
    }

    pub fn mac(&self, context: &[u8], data: &[u8]) -> Result<[u8; 32]> {
        self.mac_with(self.keyring.active, context, data)
    }

    /// MAC under a specific key version (for verifying records written before
    /// a rotation).
    pub fn mac_with(&self, key_id: u32, context: &[u8], data: &[u8]) -> Result<[u8; 32]> {
        let key = self.keyring.get(key_id)?;
        // Domain-separated MAC key derived from the DEK.
        let mk = self.provider.hmac_sha256(key, b"db2graph-sync/mac-key/v1");
        Ok(self.provider.hmac_sha256(&mk, &[context, data].concat()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sealer() -> Sealer {
        let mut kr = Keyring::new(1);
        kr.add_base64(1, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
        Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr))
    }

    #[test]
    fn kats_pass() {
        RustCryptoProvider.self_test().unwrap();
    }

    #[test]
    fn roundtrip_and_context_binding() {
        let s = sealer();
        let e = s.seal(b"journal/1", b"hello").unwrap();
        assert_eq!(s.open(b"journal/1", &e).unwrap().1, b"hello");
        assert_eq!(s.open(b"journal/2", &e).unwrap_err().kind(), ErrorKind::StateTampered);
        let mut t = e.clone();
        let n = t.len() - 1;
        t[n] ^= 0x80;
        assert_eq!(s.open(b"journal/1", &t).unwrap_err().kind(), ErrorKind::StateTampered);
    }

    #[test]
    fn nonces_are_unique() {
        let s = sealer();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let e = s.seal(b"c", b"x").unwrap();
            assert!(seen.insert(e[8..20].to_vec()));
        }
    }

    #[test]
    fn rotation_reads_old_keys() {
        let mut kr = Keyring::new(1);
        kr.add_base64(1, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
        let old = Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr));
        let e = old.seal(b"c", b"v1").unwrap();
        let mut kr2 = Keyring::new(2);
        kr2.add_base64(1, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
        kr2.add_base64(2, "HxwdHh8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo=").unwrap();
        let new = Sealer::new(Arc::new(RustCryptoProvider), Arc::new(kr2));
        assert_eq!(new.open(b"c", &e).unwrap(), (1, b"v1".to_vec()));
        let e2 = new.seal(b"c", b"v2").unwrap();
        assert_eq!(new.open(b"c", &e2).unwrap().0, 2);
    }
}
