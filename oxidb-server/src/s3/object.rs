use oxidb::OxiDb;
use oxidb::error::Error as OxiError;
use std::collections::HashMap;

/// Detects aws-chunked / streaming PUT bodies. AWS CLI / boto3 send
/// these by default; the chunk envelope hides the real payload behind
/// per-chunk size headers and (optionally) trailing checksums.
pub(crate) fn is_aws_chunked(headers: &HashMap<String, String>) -> bool {
    if headers
        .get("content-encoding")
        .map(|v| v.to_ascii_lowercase().contains("aws-chunked"))
        .unwrap_or(false)
    {
        return true;
    }
    headers
        .get("x-amz-content-sha256")
        .map(|v| v.starts_with("STREAMING-"))
        .unwrap_or(false)
}

/// Strip aws-chunked framing back to the original payload. The format
/// per chunk is:
///   <size_hex>[;<ext>=<val>...]\r\n
///   <size bytes>\r\n
///   ...
///   0[;<ext>=<val>...]\r\n
///   [trailer-headers]\r\n
///   \r\n
/// We tolerate missing trailers and partial reads — anything that
/// fails parsing falls back to whatever was reassembled so far rather
/// than rejecting the request outright.
pub(crate) fn decode_aws_chunked(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        // Find end of chunk-size line.
        let nl = match input[i..].windows(2).position(|w| w == b"\r\n") {
            Some(p) => i + p,
            None => break,
        };
        let header = match std::str::from_utf8(&input[i..nl]) {
            Ok(s) => s,
            Err(_) => break,
        };
        // Strip extensions (chunk-signature, etc.) — everything after ';'.
        let size_hex = header.split(';').next().unwrap_or("").trim();
        let size = match usize::from_str_radix(size_hex, 16) {
            Ok(n) => n,
            Err(_) => break,
        };
        i = nl + 2;
        if size == 0 {
            // Final chunk; ignore any trailer headers that follow.
            break;
        }
        if i + size > input.len() {
            break;
        }
        out.extend_from_slice(&input[i..i + size]);
        i += size;
        // Skip the trailing \r\n after each data chunk.
        if i + 2 <= input.len() && &input[i..i + 2] == b"\r\n" {
            i += 2;
        }
    }
    out
}

use super::S3State;
use super::encryption::{
    SseMode, add_encryption_headers, decrypt_data, encrypt_data, is_sse_c, is_sse_s3,
    parse_sse_headers, sse_metadata_marker,
};
use super::helpers::{iso_to_httpdate, parse_range, url_decode, xml_escape};
use super::http::{HttpRequest, HttpResponse, error_response};

pub fn handle_put_object(
    state: &S3State,
    bucket: &str,
    key: &str,
    req: &HttpRequest,
) -> HttpResponse {
    let content_type = req
        .headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".to_string());

    // Determine encryption mode
    let sse_mode = match parse_sse_headers(&req.headers, state.encryption.as_deref()) {
        Ok(m) => m,
        Err(resp) => return resp,
    };

    // Strip aws-chunked framing if present. Modern boto3 / aws-cli
    // default to STREAMING uploads, which arrive as
    //   <size_hex>[;<ext>]\r\n<data>\r\n ... 0\r\n[trailers]\r\n
    // inside the Content-Length-bounded body. Without this step the
    // raw frames end up persisted as the object's bytes.
    let raw_body: &[u8] = &req.body;
    let decoded_body;
    let body_ref: &[u8] = if is_aws_chunked(&req.headers) {
        decoded_body = decode_aws_chunked(raw_body);
        &decoded_body
    } else {
        raw_body
    };

    // Encrypt data if needed
    let data_to_store = match encrypt_data(body_ref, &sse_mode, state.encryption.as_deref()) {
        Ok(d) => d,
        Err(e) => {
            return error_response(
                500,
                "InternalError",
                &format!("encryption failed: {e}"),
                key,
            );
        }
    };

    let mut metadata = HashMap::new();
    for (k, v) in &req.headers {
        if let Some(meta_key) = k.strip_prefix("x-amz-meta-") {
            metadata.insert(meta_key.to_string(), v.clone());
        }
    }

    // Store SSE marker in metadata
    if let Some((mk, mv)) = sse_metadata_marker(&sse_mode) {
        metadata.insert(mk, mv);
    }

    match state
        .db
        .put_object(bucket, key, &data_to_store, &content_type, metadata)
    {
        Ok(meta) => {
            let etag = meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
            let resp =
                HttpResponse::ok_xml(String::new()).with_header("ETag", &format!("\"{etag}\""));
            add_encryption_headers(resp, &sse_mode)
        }
        Err(e) => {
            eprintln!("[s3] put_object error: {e}");
            error_response(500, "InternalError", "Failed to store object", key)
        }
    }
}

pub fn handle_get_object(
    state: &S3State,
    bucket: &str,
    key: &str,
    req: &HttpRequest,
) -> HttpResponse {
    match state.db.get_object(bucket, key) {
        Ok((raw_data, meta)) => {
            let content_type = meta["content_type"]
                .as_str()
                .unwrap_or("application/octet-stream");
            let etag_val = meta["etag"].as_str().unwrap_or("");
            let created = meta["created_at"].as_str().unwrap_or("");
            let etag_quoted = format!("\"{etag_val}\"");

            // Determine decryption mode
            let sse_mode = if is_sse_c(&meta) {
                // SSE-C: need customer key from request headers
                match parse_sse_headers(&req.headers, state.encryption.as_deref()) {
                    Ok(m @ SseMode::CustomerKey(_)) => m,
                    _ => {
                        return error_response(
                            400,
                            "InvalidRequest",
                            "This object was encrypted with SSE-C. Provide the customer encryption key.",
                            key,
                        );
                    }
                }
            } else if is_sse_s3(&meta) {
                SseMode::S3
            } else {
                SseMode::None
            };

            // Decrypt
            let data = match decrypt_data(&raw_data, &sse_mode, state.encryption.as_deref()) {
                Ok(d) => d,
                Err(e) => {
                    return error_response(
                        500,
                        "InternalError",
                        &format!("decryption failed: {e}"),
                        key,
                    );
                }
            };

            // Conditional: If-None-Match
            if let Some(inm) = req.headers.get("if-none-match")
                && (inm.trim_matches('"') == etag_val || inm == "*")
            {
                return HttpResponse {
                    status: 304,
                    status_text: "Not Modified",
                    content_type: String::new(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    content_length_override: None,
                }
                .with_header("ETag", &etag_quoted);
            }

            // Conditional: If-Match
            if let Some(im) = req.headers.get("if-match")
                && im != "*"
                && im.trim_matches('"') != etag_val
            {
                return error_response(412, "PreconditionFailed", "Precondition Failed", key);
            }

            // Conditional: If-Modified-Since (compare as HTTP-date strings)
            if let Some(ims) = req.headers.get("if-modified-since") {
                let obj_date = iso_to_httpdate(created);
                if obj_date == *ims {
                    return HttpResponse {
                        status: 304,
                        status_text: "Not Modified",
                        content_type: String::new(),
                        headers: Vec::new(),
                        body: Vec::new(),
                        content_length_override: None,
                    }
                    .with_header("ETag", &etag_quoted);
                }
            }

            // Conditional: If-Unmodified-Since
            if let Some(ius) = req.headers.get("if-unmodified-since") {
                let obj_date = iso_to_httpdate(created);
                if obj_date != *ius {
                    return error_response(412, "PreconditionFailed", "Precondition Failed", key);
                }
            }

            // Range request (on decrypted data)
            if let Some(range_header) = req.headers.get("range") {
                if let Some(range) = parse_range(range_header, data.len() as u64) {
                    let total = data.len() as u64;
                    let slice = data[range.0 as usize..=range.1 as usize].to_vec();
                    let range_str = format!("{}-{}", range.0, range.1);
                    let mut resp = HttpResponse::partial(slice, content_type, &range_str, total)
                        .with_header("ETag", &etag_quoted)
                        .with_header("Last-Modified", &iso_to_httpdate(created))
                        .with_header("Accept-Ranges", "bytes");
                    resp = add_encryption_headers(resp, &sse_mode);
                    if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                        for (k, v) in user_meta {
                            if k == "_sse" {
                                continue;
                            }
                            if let Some(val) = v.as_str() {
                                resp = resp.with_header(&format!("x-amz-meta-{k}"), val);
                            }
                        }
                    }
                    return resp;
                }
                return error_response(
                    416,
                    "InvalidRange",
                    "The requested range is not satisfiable",
                    key,
                );
            }

            // Full response
            let mut resp = HttpResponse::data(data, content_type)
                .with_header("ETag", &etag_quoted)
                .with_header("Last-Modified", &iso_to_httpdate(created))
                .with_header("Accept-Ranges", "bytes");

            resp = add_encryption_headers(resp, &sse_mode);

            if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                for (k, v) in user_meta {
                    if k == "_sse" {
                        continue;
                    }
                    if let Some(val) = v.as_str() {
                        resp = resp.with_header(&format!("x-amz-meta-{k}"), val);
                    }
                }
            }
            resp
        }
        Err(OxiError::BlobNotFound { .. }) => {
            error_response(404, "NoSuchKey", "The specified key does not exist", key)
        }
        Err(OxiError::BucketNotFound(_)) => error_response(
            404,
            "NoSuchBucket",
            "The specified bucket does not exist",
            bucket,
        ),
        Err(e) => {
            eprintln!("[s3] get_object error: {e}");
            error_response(500, "InternalError", "Failed to retrieve object", key)
        }
    }
}

pub fn handle_head_object(state: &S3State, bucket: &str, key: &str) -> HttpResponse {
    match state.db.head_object(bucket, key) {
        Ok(meta) => {
            let content_type = meta["content_type"]
                .as_str()
                .unwrap_or("application/octet-stream");
            let etag = meta["etag"].as_str().unwrap_or("");
            let size = meta["size"].as_u64().unwrap_or(0);
            let created = meta["created_at"].as_str().unwrap_or("");

            let sse_mode = if is_sse_c(&meta) {
                SseMode::CustomerKey(vec![]) // marker only, no actual key needed for HEAD
            } else if is_sse_s3(&meta) {
                SseMode::S3
            } else {
                SseMode::None
            };

            let mut resp = HttpResponse {
                status: 200,
                status_text: "OK",
                content_type: content_type.to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                content_length_override: Some(size),
            }
            .with_header("ETag", &format!("\"{etag}\""))
            .with_header("Last-Modified", &iso_to_httpdate(created))
            .with_header("Accept-Ranges", "bytes");

            resp = add_encryption_headers(resp, &sse_mode);

            if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                for (k, v) in user_meta {
                    if k == "_sse" {
                        continue;
                    }
                    if let Some(val) = v.as_str() {
                        resp = resp.with_header(&format!("x-amz-meta-{k}"), val);
                    }
                }
            }
            resp
        }
        Err(OxiError::BlobNotFound { .. } | OxiError::BucketNotFound(_)) => {
            error_response(404, "NoSuchKey", "The specified key does not exist", key)
        }
        Err(e) => {
            eprintln!("[s3] head_object error: {e}");
            error_response(
                500,
                "InternalError",
                "Failed to retrieve object metadata",
                key,
            )
        }
    }
}

pub fn handle_delete_object(db: &OxiDb, bucket: &str, key: &str) -> HttpResponse {
    match db.delete_object(bucket, key) {
        Ok(_) => HttpResponse::no_content(),
        Err(OxiError::BlobNotFound { .. } | OxiError::BucketNotFound(_)) => {
            HttpResponse::no_content()
        }
        Err(e) => {
            eprintln!("[s3] delete_object error: {e}");
            error_response(500, "InternalError", "Failed to delete object", key)
        }
    }
}

pub fn handle_copy_object(
    state: &S3State,
    dest_bucket: &str,
    dest_key: &str,
    req: &HttpRequest,
) -> HttpResponse {
    let copy_source = req.headers.get("x-amz-copy-source").unwrap();
    let source = url_decode(copy_source);
    let source = source.strip_prefix('/').unwrap_or(&source);
    let source_parts: Vec<&str> = source.splitn(2, '/').collect();
    if source_parts.len() < 2 {
        return error_response(
            400,
            "InvalidArgument",
            "Invalid x-amz-copy-source",
            dest_key,
        );
    }
    let src_bucket = source_parts[0];
    let src_key = source_parts[1];

    // Conditional copy
    if let Some(im) = req.headers.get("x-amz-copy-source-if-match")
        && let Ok(meta) = state.db.head_object(src_bucket, src_key)
    {
        let etag = meta["etag"].as_str().unwrap_or("");
        if im.trim_matches('"') != etag && im != "*" {
            return error_response(412, "PreconditionFailed", "Precondition Failed", src_key);
        }
    }
    if let Some(inm) = req.headers.get("x-amz-copy-source-if-none-match")
        && let Ok(meta) = state.db.head_object(src_bucket, src_key)
    {
        let etag = meta["etag"].as_str().unwrap_or("");
        if inm.trim_matches('"') == etag {
            return error_response(412, "PreconditionFailed", "Precondition Failed", src_key);
        }
    }

    match state.db.get_object(src_bucket, src_key) {
        Ok((raw_data, src_meta)) => {
            let content_type = src_meta["content_type"]
                .as_str()
                .unwrap_or("application/octet-stream");

            // Decrypt source if encrypted
            let src_sse = if is_sse_s3(&src_meta) {
                SseMode::S3
            } else if is_sse_c(&src_meta) {
                // For SSE-C copy, need source key headers (x-amz-copy-source-server-side-encryption-customer-*)
                // For simplicity, use the same SSE-C headers from the request
                match parse_sse_headers(&req.headers, state.encryption.as_deref()) {
                    Ok(m @ SseMode::CustomerKey(_)) => m,
                    _ => {
                        return error_response(
                            400,
                            "InvalidRequest",
                            "Source object is SSE-C encrypted. Provide customer key.",
                            src_key,
                        );
                    }
                }
            } else {
                SseMode::None
            };

            let plaintext = match decrypt_data(&raw_data, &src_sse, state.encryption.as_deref()) {
                Ok(d) => d,
                Err(e) => {
                    return error_response(
                        500,
                        "InternalError",
                        &format!("decryption failed: {e}"),
                        src_key,
                    );
                }
            };

            // Determine destination encryption
            let dest_sse = match parse_sse_headers(&req.headers, state.encryption.as_deref()) {
                Ok(m) => m,
                Err(resp) => return resp,
            };

            let data_to_store =
                match encrypt_data(&plaintext, &dest_sse, state.encryption.as_deref()) {
                    Ok(d) => d,
                    Err(e) => {
                        return error_response(
                            500,
                            "InternalError",
                            &format!("encryption failed: {e}"),
                            dest_key,
                        );
                    }
                };

            let metadata = if req
                .headers
                .get("x-amz-metadata-directive")
                .map(|v| v.as_str())
                == Some("REPLACE")
            {
                let mut m = HashMap::new();
                for (k, v) in &req.headers {
                    if let Some(mk) = k.strip_prefix("x-amz-meta-") {
                        m.insert(mk.to_string(), v.clone());
                    }
                }
                if let Some((mk, mv)) = sse_metadata_marker(&dest_sse) {
                    m.insert(mk, mv);
                }
                m
            } else {
                let mut m: HashMap<String, String> = src_meta
                    .get("metadata")
                    .and_then(|v| v.as_object())
                    .map(|obj| {
                        obj.iter()
                            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                // Update SSE marker for destination
                m.remove("_sse");
                if let Some((mk, mv)) = sse_metadata_marker(&dest_sse) {
                    m.insert(mk, mv);
                }
                m
            };

            let ct = req
                .headers
                .get("content-type")
                .map(|s| s.as_str())
                .unwrap_or(content_type);

            match state
                .db
                .put_object(dest_bucket, dest_key, &data_to_store, ct, metadata)
            {
                Ok(new_meta) => {
                    let etag = new_meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
                    let xml = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult>\n  <ETag>\"{}\"</ETag>\n  <LastModified>{}</LastModified>\n</CopyObjectResult>",
                        xml_escape(etag),
                        new_meta
                            .get("created_at")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                    );
                    let resp = HttpResponse::ok_xml(xml);
                    add_encryption_headers(resp, &dest_sse)
                }
                Err(e) => {
                    eprintln!("[s3] copy put_object error: {e}");
                    error_response(
                        500,
                        "InternalError",
                        "Failed to store copied object",
                        dest_key,
                    )
                }
            }
        }
        Err(OxiError::BlobNotFound { .. } | OxiError::BucketNotFound(_)) => error_response(
            404,
            "NoSuchKey",
            "The specified key does not exist",
            src_key,
        ),
        Err(e) => {
            eprintln!("[s3] copy get_object error: {e}");
            error_response(
                500,
                "InternalError",
                "Failed to read source object",
                src_key,
            )
        }
    }
}

#[cfg(test)]
mod chunked_tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn decodes_simple_chunk() {
        let body = b"b4\r\nPLAINTEXT-WITNESS-OXIDB-SECRET-DOCUMENT-DEMO\nPLAINTEXT-WITNESS-OXIDB-SECRET-DOCUMENT-DEMO\nPLAINTEXT-WITNESS-OXIDB-SECRET-DOCUMENT-DEMO\nPLAINTEXT-WITNESS-OXIDB-SECRET-DOCUMENT-DEMO\n\r\n0\r\nx-amz-checksum-crc32:RENCRQ==\r\n\r\n";
        let out = decode_aws_chunked(body);
        let expected = b"PLAINTEXT-WITNESS-OXIDB-SECRET-DOCUMENT-DEMO\n".repeat(4);
        assert_eq!(out, expected);
    }

    #[test]
    fn detects_via_streaming_header() {
        let mut h = HashMap::new();
        h.insert(
            "x-amz-content-sha256".to_string(),
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER".to_string(),
        );
        assert!(is_aws_chunked(&h));
    }

    #[test]
    fn detects_via_content_encoding() {
        let mut h = HashMap::new();
        h.insert("content-encoding".to_string(), "aws-chunked".to_string());
        assert!(is_aws_chunked(&h));
    }
}
