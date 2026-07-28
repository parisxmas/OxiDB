use oxidb::OxiDb;
use std::collections::HashMap;

use super::helpers::{extract_xml_tag_pairs, xml_escape};
use super::http::{HttpRequest, HttpResponse, error_response};

pub fn handle_get_tagging(db: &OxiDb, bucket: &str, key: &str) -> HttpResponse {
    match db.head_object(bucket, key) {
        Ok(meta) => {
            let mut xml =
                String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging>\n  <TagSet>\n");

            if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                for (k, v) in user_meta {
                    if let Some(tag_key) = k.strip_prefix("tag-")
                        && let Some(val) = v.as_str()
                    {
                        xml.push_str(&format!(
                            "    <Tag><Key>{}</Key><Value>{}</Value></Tag>\n",
                            xml_escape(tag_key),
                            xml_escape(val)
                        ));
                    }
                }
            }

            xml.push_str("  </TagSet>\n</Tagging>");
            HttpResponse::ok_xml(xml)
        }
        Err(e) if e.to_string().contains("not found") => {
            error_response(404, "NoSuchKey", "The specified key does not exist", key)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

pub fn handle_put_tagging(db: &OxiDb, bucket: &str, key: &str, req: &HttpRequest) -> HttpResponse {
    let (data, meta) = match db.get_object(bucket, key) {
        Ok(r) => r,
        Err(e) if e.to_string().contains("not found") => {
            return error_response(404, "NoSuchKey", "The specified key does not exist", key);
        }
        Err(e) => return error_response(500, "InternalError", &e.to_string(), key),
    };

    let content_type = meta["content_type"]
        .as_str()
        .unwrap_or("application/octet-stream");

    let mut metadata: HashMap<String, String> = meta
        .get("metadata")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        })
        .unwrap_or_default();

    metadata.retain(|k, _| !k.starts_with("tag-"));

    let body_str = String::from_utf8_lossy(&req.body);
    let tag_keys = extract_xml_tag_pairs(&body_str);
    for (tk, tv) in &tag_keys {
        metadata.insert(format!("tag-{tk}"), tv.clone());
    }

    match db.put_object(bucket, key, &data, content_type, metadata) {
        Ok(_) => HttpResponse::ok_xml(String::new()),
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

pub fn handle_delete_tagging(db: &OxiDb, bucket: &str, key: &str) -> HttpResponse {
    let (data, meta) = match db.get_object(bucket, key) {
        Ok(r) => r,
        Err(e) if e.to_string().contains("not found") => {
            return error_response(404, "NoSuchKey", "The specified key does not exist", key);
        }
        Err(e) => return error_response(500, "InternalError", &e.to_string(), key),
    };

    let content_type = meta["content_type"]
        .as_str()
        .unwrap_or("application/octet-stream");
    let mut metadata: HashMap<String, String> = meta
        .get("metadata")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        })
        .unwrap_or_default();

    metadata.retain(|k, _| !k.starts_with("tag-"));

    match db.put_object(bucket, key, &data, content_type, metadata) {
        Ok(_) => HttpResponse::no_content(),
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}
