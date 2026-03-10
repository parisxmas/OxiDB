use std::collections::HashMap;
use oxidb::OxiDb;

use super::helpers::xml_escape;
use super::http::{HttpResponse, error_response};

pub fn handle_list_buckets(db: &OxiDb) -> HttpResponse {
    let buckets = db.list_buckets();
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Owner><ID>oxidb</ID><DisplayName>oxidb</DisplayName></Owner>\n  <Buckets>\n");
    for name in &buckets {
        xml.push_str(&format!(
            "    <Bucket><Name>{}</Name><CreationDate>2026-01-01T00:00:00Z</CreationDate></Bucket>\n",
            xml_escape(name)
        ));
    }
    xml.push_str("  </Buckets>\n</ListAllMyBucketsResult>");
    HttpResponse::ok_xml(xml)
}

pub fn handle_create_bucket(db: &OxiDb, bucket: &str) -> HttpResponse {
    match db.create_bucket(bucket) {
        Ok(_) => HttpResponse::ok_xml(String::new())
            .with_header("Location", &format!("/{bucket}")),
        Err(e) => error_response(500, "InternalError", &e.to_string(), bucket),
    }
}

pub fn handle_delete_bucket(db: &OxiDb, bucket: &str) -> HttpResponse {
    match db.delete_bucket(bucket) {
        Ok(_) => HttpResponse::no_content(),
        Err(e) if e.to_string().contains("bucket not found") => {
            error_response(404, "NoSuchBucket", "The specified bucket does not exist", bucket)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), bucket),
    }
}

pub fn handle_head_bucket(db: &OxiDb, bucket: &str) -> HttpResponse {
    let buckets = db.list_buckets();
    if buckets.iter().any(|b| b == bucket) {
        HttpResponse {
            status: 200,
            status_text: "OK",
            content_type: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
            content_length_override: None,
        }
    } else {
        error_response(404, "NoSuchBucket", "The specified bucket does not exist", bucket)
    }
}

pub fn handle_list_objects(db: &OxiDb, bucket: &str, params: &HashMap<String, String>) -> HttpResponse {
    let prefix = params.get("prefix").map(|s| s.as_str());
    let max_keys: usize = params.get("max-keys").and_then(|v| v.parse().ok()).unwrap_or(1000);
    let delimiter = params.get("delimiter").map(|s| s.as_str());
    let start_after = params.get("start-after").map(|s| s.as_str());

    match db.list_objects(bucket, prefix, Some(max_keys + 1000)) {
        Ok(all_objects) => {
            let objects: Vec<_> = all_objects.into_iter()
                .filter(|obj| {
                    if let Some(start) = start_after {
                        let meta = serde_json::to_value(obj).unwrap_or_default();
                        let key = meta["key"].as_str().unwrap_or("");
                        key > start
                    } else {
                        true
                    }
                })
                .take(max_keys)
                .collect();

            let mut common_prefixes: Vec<String> = Vec::new();
            if let Some(delim) = delimiter {
                let pfx = prefix.unwrap_or("");
                let mut seen = std::collections::HashSet::new();
                for obj in &objects {
                    let meta = serde_json::to_value(obj).unwrap_or_default();
                    let key = meta["key"].as_str().unwrap_or("");
                    if key.starts_with(pfx) {
                        let rest = &key[pfx.len()..];
                        if let Some(idx) = rest.find(delim) {
                            let cp = format!("{}{}{}", pfx, &rest[..idx], delim);
                            if seen.insert(cp.clone()) {
                                common_prefixes.push(cp);
                            }
                        }
                    }
                }
                common_prefixes.sort();
            }

            let truncated = objects.len() >= max_keys;
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Name>{}</Name>\n  <Prefix>{}</Prefix>\n  <MaxKeys>{}</MaxKeys>\n  <IsTruncated>{}</IsTruncated>\n  <KeyCount>{}</KeyCount>\n",
                xml_escape(bucket),
                xml_escape(prefix.unwrap_or("")),
                max_keys,
                truncated,
                objects.len(),
            );
            if let Some(d) = delimiter {
                xml.push_str(&format!("  <Delimiter>{}</Delimiter>\n", xml_escape(d)));
            }
            for obj in &objects {
                let meta = serde_json::to_value(obj).unwrap_or_default();
                let key = meta["key"].as_str().unwrap_or("");
                let size = meta["size"].as_u64().unwrap_or(0);
                let etag = meta["etag"].as_str().unwrap_or("");
                let created = meta["created_at"].as_str().unwrap_or("");

                if delimiter.is_some() && common_prefixes.iter().any(|cp| key.starts_with(cp.as_str()) && key != cp.as_str()) {
                    continue;
                }

                xml.push_str(&format!(
                    "  <Contents>\n    <Key>{}</Key>\n    <LastModified>{}</LastModified>\n    <ETag>\"{}\"</ETag>\n    <Size>{}</Size>\n    <StorageClass>STANDARD</StorageClass>\n  </Contents>\n",
                    xml_escape(key), xml_escape(created), xml_escape(etag), size
                ));
            }
            for cp in &common_prefixes {
                xml.push_str(&format!(
                    "  <CommonPrefixes>\n    <Prefix>{}</Prefix>\n  </CommonPrefixes>\n",
                    xml_escape(cp)
                ));
            }
            xml.push_str("</ListBucketResult>");
            HttpResponse::ok_xml(xml)
        }
        Err(e) if e.to_string().contains("bucket not found") => {
            error_response(404, "NoSuchBucket", "The specified bucket does not exist", bucket)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), bucket),
    }
}
