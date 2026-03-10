use std::collections::HashMap;

use super::helpers::{xml_escape, crc32};
use super::http::{HttpRequest, HttpResponse, error_response};
use super::S3State;

pub fn handle_create_multipart(state: &S3State, bucket: &str, key: &str, req: &HttpRequest) -> HttpResponse {
    let content_type = req.headers.get("content-type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".to_string());

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
    };

    state.uploads.lock().unwrap().insert(upload_id.clone(), upload);

    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n</InitiateMultipartUploadResult>",
        xml_escape(bucket), xml_escape(key), xml_escape(&upload_id)
    );
    HttpResponse::ok_xml(xml)
}

pub fn handle_upload_part(state: &S3State, _bucket: &str, _key: &str, req: &HttpRequest, params: &HashMap<String, String>) -> HttpResponse {
    let upload_id = params.get("uploadId").unwrap();
    let part_number: u32 = match params.get("partNumber").and_then(|v| v.parse().ok()) {
        Some(n) => n,
        None => return error_response(400, "InvalidArgument", "Invalid partNumber", ""),
    };

    let mut uploads = state.uploads.lock().unwrap();
    match uploads.get_mut(upload_id.as_str()) {
        Some(upload) => {
            let etag = format!("{:08x}", crc32(&req.body));
            upload.parts.insert(part_number, req.body.clone());
            HttpResponse::ok_xml(String::new())
                .with_header("ETag", &format!("\"{etag}\""))
        }
        None => error_response(404, "NoSuchUpload", "The specified upload does not exist", upload_id),
    }
}

pub fn handle_complete_multipart(state: &S3State, _bucket: &str, _key: &str, params: &HashMap<String, String>) -> HttpResponse {
    let upload_id = params.get("uploadId").unwrap();

    let upload = {
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

    match state.db.put_object(&upload.bucket, &upload.key, &assembled, &upload.content_type, upload.metadata) {
        Ok(meta) => {
            let etag = meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <ETag>\"{}\"</ETag>\n</CompleteMultipartUploadResult>",
                xml_escape(&upload.bucket), xml_escape(&upload.key), xml_escape(etag)
            );
            HttpResponse::ok_xml(xml)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), &upload.key),
    }
}

pub fn handle_abort_multipart(state: &S3State, _key: &str, params: &HashMap<String, String>) -> HttpResponse {
    let upload_id = params.get("uploadId").unwrap();
    let mut uploads = state.uploads.lock().unwrap();
    uploads.remove(upload_id.as_str());
    HttpResponse::no_content()
}
