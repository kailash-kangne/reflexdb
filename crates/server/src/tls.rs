//! TLS listener support via `rustls` (the `ring` crypto provider — no
//! cmake/C++ toolchain needed, unlike the `aws-lc-rs` default). See
//! `docs/SECURITY.md`.
//!
//! Scope, deliberately: server-side TLS only. No mutual TLS (client
//! certificate verification, `tls-auth-clients`), no `tls-ca-cert-file`,
//! no `tls-replication` (wrapping the replication link in TLS). Both are
//! optional TLS transport features not implemented here.

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// Loads a cert/key pair and builds a `TlsAcceptor` ready to wrap accepted
/// `TcpStream`s. Called once at startup.
pub fn build_acceptor(cert_path: &Path, key_path: &Path) -> io::Result<TlsAcceptor> {
    // rustls 0.23 requires an explicit crypto provider install; a second
    // call (e.g. if this is somehow invoked twice) is harmless to ignore.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("invalid TLS cert/key: {e}")))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading tls-cert-file {}: {e}", path.display())))?;
    rustls_pemfile::certs(&mut BufReader::new(file)).collect()
}

fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading tls-key-file {}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))?.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, format!("no private key found in {}", path.display()))
    })
}
