use std::collections::HashMap;

use super::encryption::{parse_sse_headers, encrypt_data, add_encryption_headers, sse_metadata_marker, SseMode};
use super::helpers::{xml_escape, crc32};
use super::http::{HttpRequest, HttpResponse, error_response};
use super::S3State;

pub fn handle_create_multipart(state: &S3State, bucket: &str, key: &str, req: &HttpRequest) -> HttpResponse {
    let content_type = req.headers.get("content-type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".to_string());

    // Determine encryption mode at initiation time
    let sse_mode = match parse_sse_headers(&req.headers, state.encryption.as_deref()) {
        Ok(m) => m,
        Err(resp) => return resp,
    };

    let sse_marker = match &sse_mode {
        SseMode::None => None,
        SseMode::S3 => Some("AES256".to_string()),
        SseMode::CustomerKey(k) => {
            // Store the key for later parts (in real AWS this is per-request, but we need it for assembly)
            Some(format!("SSE-C:{}", super::encryption::base64_encode_key(k)))
        }
    };

    let mut metadata = HashMap::new();
    for (k, v) in &req.headers {
        if let Some(mk) = k.strip_prefix("x-amz-meta-") {
            metadata.insert(mk.to_string(), v.clone());
        }
    }

    let upload_id = format!("{:016x}", rand::random::<u64>());

    let upload = super::MultipartUpload {
        bucket: bucket.to_string(),
        key: key.to_string(),
        content_type,
        metadata,
        parts: HashMap::new(),
        total_bytes: 0,
        created_at: std::time::Instant::now(),
        sse_marker,
    };

    state.uploads.lock().unwrap().insert(upload_id.clone(), upload);

    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n</InitiateMultipartUploadResult>",
        xml_escape(bucket), xml_escape(key), xml_escape(&upload_id)
    );
    let resp = HttpResponse::ok_xml(xml);
    add_encryption_headers(resp, &sse_mode)
}

pub fn handle_upload_part(state: &S3State, _bucket: &str, _key: &str, req: &HttpRequest, params: &HashMap<String, String>) -> HttpResponse {
    let upload_id = params.get("uploadId").unwrap();
    let part_number: u32 = match params.get("partNumber").and_then(|v| v.parse().ok()) {
        Some(n) => n,
        None => return error_response(400, "InvalidArgument", "Invalid partNumber", ""),
    };

    if part_number > super::MAX_MULTIPART_PARTS {
        return error_response(400, "InvalidArgument",
            &format!("Part number exceeds maximum ({})", super::MAX_MULTIPART_PARTS), "");
    }

    let mut uploads = state.uploads.lock().unwrap();
    match uploads.get_mut(upload_id.as_str()) {
        Some(upload) => {
            let new_total = upload.total_bytes + req.body.len();
            if new_total > super::MAX_MULTIPART_TOTAL {
                return error_response(400, "EntityTooLarge",
                    "Multipart upload exceeds maximum total size", "");
            }
            let etag = format!("{:08x}", crc32(&req.body));
            upload.total_bytes = new_total;
            upload.parts.insert(part_number, req.body.clone());
            HttpResponse::ok_xml(String::new())
                .with_header("ETag", &format!("\"{etag}\""))
        }
        None => error_response(404, "NoSuchUpload", "The specified upload does not exist", upload_id),
    }
}

pub fn handle_complete_multipart(state: &S3State, _bucket: &str, _key: &str, params: &HashMap<String, String>) -> HttpResponse {
    let upload_id = params.get("uploadId").unwrap();

    let mut upload = {
        let mut uploads = state.uploads.lock().unwrap();
        match uploads.remove(upload_id.as_str()) {
            Some(u) => u,
            None => return error_response(404, "NoSuchUpload", "The specified upload does not exist", upload_id),
        }
    };

    let mut part_nums: Vec<u32> = upload.parts.keys().copied().collect();
    part_nums.sort();

    let mut assembled = Vec::new();
    for num in &part_nums {
        if let Some(part) = upload.parts.get(num) {
            assembled.extend_from_slice(part);
        }
    }

    // Determine encryption from stored marker, then zeroize key material
    let (sse_mode, data_to_store) = match &upload.sse_marker {
        None => (SseMode::None, assembled),
        Some(marker) if marker == "AES256" => {
            match encrypt_data(&assembled, &SseMode::S3, state.encryption.as_deref()) {
                Ok(d) => (SseMode::S3, d),
                Err(e) => return error_response(500, "InternalError", &format!("encryption failed: {e}"), &upload.key),
            }
        }
        Some(marker) if marker.starts_with("SSE-C:") => {
            let key_b64 = &marker[6..];
            if let Some(key_bytes) = super::encryption::base64_decode_key(key_b64) {
                match encrypt_data(&assembled, &SseMode::CustomerKey(key_bytes.clone()), state.encryption.as_deref()) {
                    Ok(d) => (SseMode::CustomerKey(key_bytes), d),
                    Err(e) => return error_response(500, "InternalError", &format!("encryption failed: {e}"), &upload.key),
                }
            } else {
                return error_response(500, "InternalError", "Failed to recover SSE-C key for multipart", &upload.key);
            }
        }
        _ => (SseMode::None, assembled),
    };

    // Zeroize SSE-C key material from memory
    upload.zeroize_key();

    // Add SSE metadata marker
    let mut metadata = upload.metadata;
    if let Some((mk, mv)) = sse_metadata_marker(&sse_mode) {
        metadata.insert(mk, mv);
    }

    match state.db.put_object(&upload.bucket, &upload.key, &data_to_store, &upload.content_type, metadata) {
        Ok(meta) => {
            let etag = meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <ETag>\"{}\"</ETag>\n</CompleteMultipartUploadResult>",
                xml_escape(&upload.bucket), xml_escape(&upload.key), xml_escape(etag)
            );
            let resp = HttpResponse::ok_xml(xml);
            add_encryption_headers(resp, &sse_mode)
        }
        Err(e) => {
            eprintln!("[s3] complete_multipart put_object error: {e}");
            error_response(500, "InternalError", "Failed to store multipart object", &upload.key)
        }
    }
}

pub fn handle_abort_multipart(state: &S3State, _key: &str, params: &HashMap<String, String>) -> HttpResponse {
    let upload_id = params.get("uploadId").unwrap();
    let mut uploads = state.uploads.lock().unwrap();
    if let Some(mut upload) = uploads.remove(upload_id.as_str()) {
        upload.zeroize_key();
    }
    HttpResponse::no_content()
}
