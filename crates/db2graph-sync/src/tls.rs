//! TLS client configuration shared by built-in adapters.
//!
//! * Certificate chain and hostname verification are always on; there is no
//!   option to disable them.
//! * The crypto provider is passed explicitly (`ring`). The process-wide
//!   default provider is never installed, so embedding applications keep
//!   control of their own rustls configuration.
//! * `ring` is **not** a FIPS 140-3 validated module; the restricted profile
//!   therefore rejects built-in network adapters.

use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConfig, RootCertStore};

use crate::error::{Error, ErrorKind, Result};

pub(crate) fn client_config(ca_file: Option<&Path>) -> Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        let _ = roots.add(cert);
    }
    if let Some(path) = ca_file {
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
            .map_err(|e| Error::new(ErrorKind::Security, format!("cannot read CA file {}: {e}", path.display())))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| Error::new(ErrorKind::Security, format!("invalid PEM in {}: {e}", path.display())))?;
        if certs.is_empty() {
            return Err(Error::new(ErrorKind::Security, format!("CA file {} contains no certificates", path.display())));
        }
        for c in certs {
            roots.add(c).map_err(|e| Error::new(ErrorKind::Security, format!("invalid CA certificate in {}: {e}", path.display())))?;
        }
    }
    if roots.is_empty() {
        return Err(Error::new(ErrorKind::Security, "no trust anchors available (no platform roots and no ca_file)"));
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::new(ErrorKind::Security, format!("TLS configuration: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}
