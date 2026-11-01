//! TLS support via `rustls` (the `ring` crypto provider — no cmake/C++
//! toolchain needed, unlike the `aws-lc-rs` default). See
//! `docs/SECURITY.md`.
//!
//! - Server listener: `tls-port` + `tls-cert-file`/`tls-key-file`.
//! - Optional client-certificate verification (`tls-auth-clients yes` +
//!   `tls-ca-cert-file` — mTLS): the client must present a certificate
//!   chaining to the CA file.
//! - Replication link: `tls-replication yes` wraps the outbound
//!   replica→primary connection in TLS (the primary's `tls-port`). The
//!   replica verifies the primary against `tls-ca-cert-file` and presents
//!   its own `tls-cert-file`/`tls-key-file` as a client certificate — the
//!   same files serving double duty, matching Redis's convention.

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Server listener: `cert_path`/`key_path` are the server's identity;
/// `client_ca` (set only when `tls-auth-clients yes`) turns on client-cert
/// verification against those roots.
pub fn build_acceptor(
    cert_path: &Path,
    key_path: &Path,
    client_ca: Option<&Path>,
) -> io::Result<TlsAcceptor> {
    // rustls 0.23 requires an explicit crypto provider install; a second
    // call (e.g. if this is somehow invoked twice) is harmless to ignore.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let builder = ServerConfig::builder();
    let builder = match client_ca {
        Some(ca_path) => {
            let roots = load_root_store(ca_path)?;
            let verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid tls-ca-cert-file {}: {e}", ca_path.display()),
                    )
                })?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let config = builder
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("invalid TLS cert/key: {e}")))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Replication-link client: `ca_path` is the trust root for the primary's
/// certificate (there is no system-roots fallback — an explicit CA file
/// keeps the trust source obvious). `cert`/`key` are the replica's client
/// identity — its own `tls-cert-file`/`tls-key-file` when configured —
/// required only when the primary runs `tls-auth-clients yes`.
pub fn build_connector(ca_path: &Path, cert: Option<&Path>, key: Option<&Path>) -> io::Result<TlsConnector> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let roots = load_root_store(ca_path)?;
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let config = match (cert, key) {
        (Some(cert_path), Some(key_path)) => {
            builder.with_client_auth_cert(load_certs(cert_path)?, load_key(key_path)?).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("invalid TLS client cert/key: {e}"))
            })?
        }
        _ => builder.with_no_client_auth(),
    };
    Ok(TlsConnector::from(Arc::new(config)))
}

fn load_root_store(path: &Path) -> io::Result<RootCertStore> {
    let certs = load_certs(path)?;
    let mut roots = RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(certs);
    if added == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no usable CA certificates in {}", path.display()),
        ));
    }
    if ignored > 0 {
        tracing::warn!(ignored, path = %path.display(), "ignored unparsable CA certificates");
    }
    Ok(roots)
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading cert file {}: {e}", path.display())))?;
    rustls_pemfile::certs(&mut BufReader::new(file)).collect()
}

fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading key file {}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))?.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, format!("no private key found in {}", path.display()))
    })
}
