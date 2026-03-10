use std::collections::HashMap;
use oxidb::OxiDb;

use super::helpers::{xml_escape, url_decode, parse_range};
use super::http::{HttpRequest, HttpResponse, error_response};

pub fn handle_put_object(db: &OxiDb, bucket: &str, key: &str, req: &HttpRequest) -> HttpResponse {
    let content_type = req.headers.get("content-type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let mut metadata = HashMap::new();
    for (k, v) in &req.headers {
        if let Some(meta_key) = k.strip_prefix("x-amz-meta-") {
            metadata.insert(meta_key.to_string(), v.clone());
        }
    }

    match db.put_object(bucket, key, &req.body, &content_type, metadata) {
        Ok(meta) => {
            let etag = meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
            HttpResponse::ok_xml(String::new())
                .with_header("ETag", &format!("\"{etag}\""))
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

pub fn handle_get_object(db: &OxiDb, bucket: &str, key: &str, req: &HttpRequest) -> HttpResponse {
    match db.get_object(bucket, key) {
        Ok((data, meta)) => {
            let content_type = meta["content_type"].as_str().unwrap_or("application/octet-stream");
            let etag_val = meta["etag"].as_str().unwrap_or("");
            let created = meta["created_at"].as_str().unwrap_or("");
            let etag_quoted = format!("\"{etag_val}\"");

            // Conditional: If-None-Match
            if let Some(inm) = req.headers.get("if-none-match") {
                if inm.trim_matches('"') == etag_val || inm == "*" {
                    return HttpResponse {
                        status: 304,
                        status_text: "Not Modified",
                        content_type: String::new(),
                        headers: Vec::new(),
                        body: Vec::new(),
                        content_length_override: None,
                    }.with_header("ETag", &etag_quoted);
                }
            }

            // Conditional: If-Match
            if let Some(im) = req.headers.get("if-match") {
                if im != "*" && im.trim_matches('"') != etag_val {
                    return error_response(412, "PreconditionFailed", "Precondition Failed", key);
                }
            }

            // Conditional: If-Modified-Since
            if let Some(ims) = req.headers.get("if-modified-since") {
                if created == ims.as_str() {
                    return HttpResponse {
                        status: 304,
                        status_text: "Not Modified",
                        content_type: String::new(),
                        headers: Vec::new(),
                        body: Vec::new(),
                        content_length_override: None,
                    }.with_header("ETag", &etag_quoted);
                }
            }

            // Conditional: If-Unmodified-Since
            if let Some(ius) = req.headers.get("if-unmodified-since") {
                if created != ius.as_str() {
                    return error_response(412, "PreconditionFailed", "Precondition Failed", key);
                }
            }

            // Range request
            if let Some(range_header) = req.headers.get("range") {
                if let Some(range) = parse_range(range_header, data.len() as u64) {
                    let total = data.len() as u64;
                    let slice = data[range.0 as usize..=range.1 as usize].to_vec();
                    let range_str = format!("{}-{}", range.0, range.1);
                    let mut resp = HttpResponse::partial(slice, content_type, &range_str, total)
                        .with_header("ETag", &etag_quoted)
                        .with_header("Last-Modified", created)
                        .with_header("Accept-Ranges", "bytes");
                    if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                        for (k, v) in user_meta {
                            if let Some(val) = v.as_str() {
                                resp = resp.with_header(&format!("x-amz-meta-{k}"), val);
                            }
                        }
                    }
                    return resp;
                }
                return error_response(416, "InvalidRange", "The requested range is not satisfiable", key);
            }

            // Full response
            let mut resp = HttpResponse::data(data, content_type)
                .with_header("ETag", &etag_quoted)
                .with_header("Last-Modified", created)
                .with_header("Accept-Ranges", "bytes");

            if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                for (k, v) in user_meta {
                    if let Some(val) = v.as_str() {
                        resp = resp.with_header(&format!("x-amz-meta-{k}"), val);
                    }
                }
            }
            resp
        }
        Err(e) if e.to_string().contains("blob not found") || e.to_string().contains("not found") => {
            error_response(404, "NoSuchKey", "The specified key does not exist", key)
        }
        Err(e) if e.to_string().contains("bucket not found") => {
            error_response(404, "NoSuchBucket", "The specified bucket does not exist", bucket)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

pub fn handle_head_object(db: &OxiDb, bucket: &str, key: &str) -> HttpResponse {
    match db.head_object(bucket, key) {
        Ok(meta) => {
            let content_type = meta["content_type"].as_str().unwrap_or("application/octet-stream");
            let etag = meta["etag"].as_str().unwrap_or("");
            let size = meta["size"].as_u64().unwrap_or(0);
            let created = meta["created_at"].as_str().unwrap_or("");
            let mut resp = HttpResponse {
                status: 200,
                status_text: "OK",
                content_type: content_type.to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                content_length_override: Some(size),
            }
            .with_header("ETag", &format!("\"{etag}\""))
            .with_header("Last-Modified", created)
            .with_header("Accept-Ranges", "bytes");

            if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                for (k, v) in user_meta {
                    if let Some(val) = v.as_str() {
                        resp = resp.with_header(&format!("x-amz-meta-{k}"), val);
                    }
                }
            }
            resp
        }
        Err(e) if e.to_string().contains("not found") => {
            error_response(404, "NoSuchKey", "The specified key does not exist", key)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

pub fn handle_delete_object(db: &OxiDb, bucket: &str, key: &str) -> HttpResponse {
    match db.delete_object(bucket, key) {
        Ok(_) => HttpResponse::no_content(),
        Err(e) if e.to_string().contains("not found") => HttpResponse::no_content(),
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

pub fn handle_copy_object(db: &OxiDb, dest_bucket: &str, dest_key: &str, req: &HttpRequest) -> HttpResponse {
    let copy_source = req.headers.get("x-amz-copy-source").unwrap();
    let source = url_decode(copy_source);
    let source = source.strip_prefix('/').unwrap_or(&source);
    let source_parts: Vec<&str> = source.splitn(2, '/').collect();
    if source_parts.len() < 2 {
        return error_response(400, "InvalidArgument", "Invalid x-amz-copy-source", dest_key);
    }
    let src_bucket = source_parts[0];
    let src_key = source_parts[1];

    // Conditional copy
    if let Some(im) = req.headers.get("x-amz-copy-source-if-match") {
        if let Ok(meta) = db.head_object(src_bucket, src_key) {
            let etag = meta["etag"].as_str().unwrap_or("");
            if im.trim_matches('"') != etag && im != "*" {
                return error_response(412, "PreconditionFailed", "Precondition Failed", src_key);
            }
        }
    }
    if let Some(inm) = req.headers.get("x-amz-copy-source-if-none-match") {
        if let Ok(meta) = db.head_object(src_bucket, src_key) {
            let etag = meta["etag"].as_str().unwrap_or("");
            if inm.trim_matches('"') == etag {
                return error_response(412, "PreconditionFailed", "Precondition Failed", src_key);
            }
        }
    }

    match db.get_object(src_bucket, src_key) {
        Ok((data, src_meta)) => {
            let content_type = src_meta["content_type"].as_str().unwrap_or("application/octet-stream");

            let metadata = if req.headers.get("x-amz-metadata-directive").map(|v| v.as_str()) == Some("REPLACE") {
                let mut m = HashMap::new();
                for (k, v) in &req.headers {
                    if let Some(mk) = k.strip_prefix("x-amz-meta-") {
                        m.insert(mk.to_string(), v.clone());
                    }
                }
                m
            } else {
                src_meta.get("metadata")
                    .and_then(|v| v.as_object())
                    .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect())
                    .unwrap_or_default()
            };

            let ct = req.headers.get("content-type").map(|s| s.as_str()).unwrap_or(content_type);

            match db.put_object(dest_bucket, dest_key, &data, ct, metadata) {
                Ok(new_meta) => {
                    let etag = new_meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
                    let xml = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult>\n  <ETag>\"{}\"</ETag>\n  <LastModified>{}</LastModified>\n</CopyObjectResult>",
                        xml_escape(etag),
                        new_meta.get("created_at").and_then(|v| v.as_str()).unwrap_or("")
                    );
                    HttpResponse::ok_xml(xml)
                }
                Err(e) => error_response(500, "InternalError", &e.to_string(), dest_key),
            }
        }
        Err(e) if e.to_string().contains("not found") => {
            error_response(404, "NoSuchKey", "The specified key does not exist", src_key)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), src_key),
    }
}
