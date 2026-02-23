use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::PrivateKeyDer;

/// Load TLS configuration from PEM certificate chain and private key files.
pub fn load_tls_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>, String> {
    let cert_file = File::open(cert_path)
        .map_err(|e| format!("failed to open cert file {}: {}", cert_path.display(), e))?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse certificates: {e}"))?;

    if certs.is_empty() {
        return Err("no certificates found in cert file".into());
    }

    let key_file = File::open(key_path)
        .map_err(|e| format!("failed to open key file {}: {}", key_path.display(), e))?;
    let mut key_reader = BufReader::new(key_file);

    let key: PrivateKeyDer = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| format!("failed to parse private key: {e}"))?
        .ok_or_else(|| "no private key found in key file".to_string())?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("TLS config error: {e}"))?;

    Ok(Arc::new(config))
}
