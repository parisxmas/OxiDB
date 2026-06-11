use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use super::helpers::parse_query;
use super::http::HttpRequest;

pub struct S3Auth {
    /// Multiple credentials: access_key → secret_key.
    pub credentials: HashMap<String, String>,
}

impl S3Auth {
    /// Parse credentials from env vars.
    /// Supports single pair (OXIDB_S3_ACCESS_KEY + OXIDB_S3_SECRET_KEY)
    /// and multi-user (OXIDB_S3_CREDENTIALS="ak1:sk1,ak2:sk2").
    pub fn from_env() -> Option<Self> {
        let mut creds = HashMap::new();

        // Multi-user: OXIDB_S3_CREDENTIALS=ak1:sk1,ak2:sk2
        if let Ok(multi) = std::env::var("OXIDB_S3_CREDENTIALS") {
            for pair in multi.split(',') {
                let pair = pair.trim();
                if let Some((ak, sk)) = pair.split_once(':') {
                    if !ak.is_empty() && !sk.is_empty() {
                        creds.insert(ak.to_string(), sk.to_string());
                    }
                }
            }
        }

        // Single pair (backwards compatible)
        if let (Ok(ak), Ok(sk)) = (
            std::env::var("OXIDB_S3_ACCESS_KEY"),
            std::env::var("OXIDB_S3_SECRET_KEY"),
        ) {
            if !ak.is_empty() && !sk.is_empty() {
                creds.insert(ak, sk);
            }
        }

        if creds.is_empty() {
            None
        } else {
            Some(Self { credentials: creds })
        }
    }

    fn get_secret(&self, access_key: &str) -> Option<&str> {
        self.credentials.get(access_key).map(|s| s.as_str())
    }
}

pub fn verify_auth(req: &HttpRequest, auth: &S3Auth) -> bool {
    let params = parse_query(&req.query);
    if params.contains_key("X-Amz-Signature") {
        return verify_presigned(req, auth, &params);
    }

    let auth_header = match req.headers.get("authorization") {
        Some(h) => h,
        None => return false,
    };

    if !auth_header.starts_with("AWS4-HMAC-SHA256") {
        return false;
    }

    let cred_start = match auth_header.find("Credential=") {
        Some(i) => i + 11,
        None => return false,
    };
    let cred_end = auth_header[cred_start..].find('/').unwrap_or(0) + cred_start;
    let access_key = &auth_header[cred_start..cred_end];

    let secret_key = match auth.get_secret(access_key) {
        Some(sk) => sk,
        None => return false,
    };

    let sig_start = match auth_header.find("Signature=") {
        Some(i) => i + 10,
        None => return false,
    };
    let signature = auth_header[sig_start..].trim();

    let signed_headers_start = match auth_header.find("SignedHeaders=") {
        Some(i) => i + 14,
        None => return false,
    };
    let signed_headers_end = auth_header[signed_headers_start..]
        .find(',')
        .unwrap_or(auth_header.len() - signed_headers_start)
        + signed_headers_start;
    let signed_headers_str = &auth_header[signed_headers_start..signed_headers_end];

    let scope_end = auth_header[cred_start..]
        .find(',')
        .unwrap_or(auth_header.len() - cred_start)
        + cred_start;
    let credential_scope = &auth_header[cred_end + 1..scope_end];
    let date_stamp = credential_scope.split('/').next().unwrap_or("");

    let method = &req.method;
    let canonical_uri = if req.path.is_empty() { "/" } else { &req.path };
    let canonical_querystring = canonical_query(&req.query);

    let signed_headers: Vec<&str> = signed_headers_str.split(';').collect();
    let mut canonical_headers = String::new();
    for h in &signed_headers {
        let val = req.headers.get(*h).map(|v| v.as_str()).unwrap_or("");
        canonical_headers.push_str(&format!("{}:{}\n", h, val.trim()));
    }

    let payload_hash = req
        .headers
        .get("x-amz-content-sha256")
        .cloned()
        .unwrap_or_else(|| sha256_hex(&req.body));

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        canonical_uri,
        canonical_querystring,
        canonical_headers,
        signed_headers_str,
        payload_hash
    );

    let amz_date = req
        .headers
        .get("x-amz-date")
        .map(|v| v.as_str())
        .unwrap_or("");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = derive_signing_key(secret_key, date_stamp, credential_scope);
    let computed_sig = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());

    constant_time_eq(signature.as_bytes(), computed_sig.as_bytes())
}

fn verify_presigned(req: &HttpRequest, auth: &S3Auth, params: &HashMap<String, String>) -> bool {
    let algorithm = params
        .get("X-Amz-Algorithm")
        .map(|s| s.as_str())
        .unwrap_or("");
    if algorithm != "AWS4-HMAC-SHA256" {
        return false;
    }

    let credential = params
        .get("X-Amz-Credential")
        .map(|s| s.as_str())
        .unwrap_or("");
    let parts: Vec<&str> = credential.splitn(2, '/').collect();
    if parts.len() < 2 {
        return false;
    }
    let access_key = parts[0];
    let credential_scope = parts[1];

    let secret_key = match auth.get_secret(access_key) {
        Some(sk) => sk,
        None => return false,
    };

    let date_stamp = credential_scope.split('/').next().unwrap_or("");
    let amz_date = params.get("X-Amz-Date").map(|s| s.as_str()).unwrap_or("");
    let signed_headers_str = params
        .get("X-Amz-SignedHeaders")
        .map(|s| s.as_str())
        .unwrap_or("host");
    let signature = params
        .get("X-Amz-Signature")
        .map(|s| s.as_str())
        .unwrap_or("");

    // Check expiration
    if let Some(expires) = params
        .get("X-Amz-Expires")
        .and_then(|v| v.parse::<u64>().ok())
    {
        if let Some(date_str) = params.get("X-Amz-Date") {
            if let Ok(request_time) = parse_amz_date(date_str) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if now > request_time + expires {
                    return false;
                }
            }
        }
    }

    let method = &req.method;
    let canonical_uri = if req.path.is_empty() { "/" } else { &req.path };

    // Build canonical query string WITHOUT the signature param
    let mut qpairs: Vec<(&str, &str)> = req
        .query
        .split('&')
        .filter_map(|p| {
            let mut parts = p.splitn(2, '=');
            let k = parts.next()?;
            let v = parts.next().unwrap_or("");
            Some((k, v))
        })
        .filter(|(k, _)| *k != "X-Amz-Signature")
        .collect();
    qpairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let canonical_querystring = qpairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let signed_headers: Vec<&str> = signed_headers_str.split(';').collect();
    let mut canonical_headers = String::new();
    for h in &signed_headers {
        let val = req.headers.get(*h).map(|v| v.as_str()).unwrap_or("");
        canonical_headers.push_str(&format!("{}:{}\n", h, val.trim()));
    }

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
        method, canonical_uri, canonical_querystring, canonical_headers, signed_headers_str
    );

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = derive_signing_key(secret_key, date_stamp, credential_scope);
    let computed_sig = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());

    constant_time_eq(signature.as_bytes(), computed_sig.as_bytes())
}

fn parse_amz_date(s: &str) -> Result<u64, ()> {
    if s.len() < 15 {
        return Err(());
    }
    let year: u64 = s[0..4].parse().map_err(|_| ())?;
    let month: u64 = s[4..6].parse().map_err(|_| ())?;
    let day: u64 = s[6..8].parse().map_err(|_| ())?;
    let hour: u64 = s[9..11].parse().map_err(|_| ())?;
    let min: u64 = s[11..13].parse().map_err(|_| ())?;
    let sec: u64 = s[13..15].parse().map_err(|_| ())?;
    let days = (year - 1970) * 365 + (year - 1969) / 4 + days_before_month(month, year) + day - 1;
    Ok(days * 86400 + hour * 3600 + min * 60 + sec)
}

fn days_before_month(month: u64, year: u64) -> u64 {
    let leap = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
        1
    } else {
        0
    };
    match month {
        1 => 0,
        2 => 31,
        3 => 59 + leap,
        4 => 90 + leap,
        5 => 120 + leap,
        6 => 151 + leap,
        7 => 181 + leap,
        8 => 212 + leap,
        9 => 243 + leap,
        10 => 273 + leap,
        11 => 304 + leap,
        12 => 334 + leap,
        _ => 0,
    }
}

// Crypto helpers

fn canonical_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(&str, &str)> = query
        .split('&')
        .filter_map(|p| {
            let mut parts = p.splitn(2, '=');
            let k = parts.next()?;
            let v = parts.next().unwrap_or("");
            Some((k, v))
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn sha256_hex(data: &[u8]) -> String {
    hex_encode(&Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    hex_encode(&hmac_sha256(key, data))
}

fn derive_signing_key(secret_key: &str, date_stamp: &str, credential_scope: &str) -> Vec<u8> {
    let parts: Vec<&str> = credential_scope.split('/').collect();
    let region = parts.get(1).unwrap_or(&"us-east-1");
    let service = parts.get(2).unwrap_or(&"s3");

    let k_date = hmac_sha256(
        format!("AWS4{}", secret_key).as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_query() {
        assert_eq!(canonical_query("b=2&a=1"), "a=1&b=2");
        assert_eq!(canonical_query(""), "");
    }

    #[test]
    fn test_parse_amz_date() {
        let ts = parse_amz_date("20260310T120000Z").unwrap();
        assert!(ts > 0);
    }
}
