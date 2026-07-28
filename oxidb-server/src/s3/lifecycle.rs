//! Bucket lifecycle rules (`?lifecycle`): a minimal Expiration/Days subset.
//! `PUT /bucket?lifecycle` stores the rule, a background sweeper deletes
//! objects older than the configured age — S3's expiration semantics wired
//! to OxiDB's blob store. Rules persist in the `_s3_lifecycle` collection.

use std::collections::HashMap;
use std::sync::Arc;

use oxidb::OxiDb;
use serde_json::json;

use super::helpers::xml_escape;
use super::http::{HttpResponse, error_response};

pub fn handle_put_lifecycle(db: &OxiDb, bucket: &str, body: &[u8]) -> HttpResponse {
    let xml = String::from_utf8_lossy(body);
    // Minimal parse: first <Days>N</Days> inside the document.
    let days: Option<u64> = xml
        .split("<Days>")
        .nth(1)
        .and_then(|rest| rest.split("</Days>").next())
        .and_then(|d| d.trim().parse().ok());
    let Some(days) = days else {
        return error_response(400, "MalformedXML", "expected <Days>N</Days>", bucket);
    };
    let _ = db.delete("_s3_lifecycle", &json!({"bucket": bucket}));
    if db
        .insert("_s3_lifecycle", json!({"bucket": bucket, "days": days}))
        .is_err()
    {
        return error_response(500, "InternalError", "rule store failed", bucket);
    }
    HttpResponse::no_content()
}

pub fn handle_get_lifecycle(db: &OxiDb, bucket: &str) -> HttpResponse {
    match db.find("_s3_lifecycle", &json!({"bucket": bucket})) {
        Ok(rows) if !rows.is_empty() => {
            let days = rows[0]["days"].as_u64().unwrap_or(0);
            HttpResponse::ok_xml(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LifecycleConfiguration>\n  <Rule>\n    <ID>{}-expiration</ID>\n    <Status>Enabled</Status>\n    <Expiration><Days>{}</Days></Expiration>\n  </Rule>\n</LifecycleConfiguration>",
                xml_escape(bucket),
                days
            ))
        }
        _ => error_response(
            404,
            "NoSuchLifecycleConfiguration",
            "no lifecycle rule",
            bucket,
        ),
    }
}

pub fn handle_delete_lifecycle(db: &OxiDb, bucket: &str) -> HttpResponse {
    let _ = db.delete("_s3_lifecycle", &json!({"bucket": bucket}));
    HttpResponse::no_content()
}

/// One sweep pass: delete objects older than each bucket's Days rule.
/// Returns the number of objects expired.
pub fn sweep(db: &Arc<OxiDb>) -> usize {
    let rules = match db.find("_s3_lifecycle", &json!({})) {
        Ok(r) => r,
        Err(_) => return 0,
    };
    let mut expired = 0;
    for rule in rules {
        let bucket = rule["bucket"].as_str().unwrap_or("");
        let days = rule["days"].as_u64().unwrap_or(0);
        if bucket.is_empty() || days == 0 {
            continue;
        }
        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(days * 86_400);
        let Ok(objects) = db.list_objects(bucket, None, None) else {
            continue;
        };
        for obj in objects {
            let meta: HashMap<String, serde_json::Value> = match serde_json::to_value(&obj)
                .ok()
                .and_then(|v| serde_json::from_value(v).ok())
            {
                Some(m) => m,
                None => continue,
            };
            let key = meta.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let created = meta
                .get("created_at")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            // created_at is RFC3339/ISO; parse the epoch cheaply.
            if let Some(secs) = iso_to_epoch(created)
                && secs < cutoff
                && !key.is_empty()
                && db.delete_object(bucket, key).is_ok()
            {
                expired += 1;
            }
        }
    }
    expired
}

/// Tiny ISO-8601 (UTC) → epoch seconds parser (YYYY-MM-DDTHH:MM:SS…).
fn iso_to_epoch(s: &str) -> Option<u64> {
    if s.len() < 19 {
        return None;
    }
    let y: i64 = s.get(0..4)?.parse().ok()?;
    let mo: i64 = s.get(5..7)?.parse().ok()?;
    let d: i64 = s.get(8..10)?.parse().ok()?;
    let h: i64 = s.get(11..13)?.parse().ok()?;
    let mi: i64 = s.get(14..16)?.parse().ok()?;
    let se: i64 = s.get(17..19)?.parse().ok()?;
    // Days since epoch (civil calendar algorithm).
    let (y2, mo2) = if mo <= 2 { (y - 1, mo + 12) } else { (y, mo) };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let doy = (153 * (mo2 - 3) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some((days * 86_400 + h * 3_600 + mi * 60 + se).max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_epoch_matches_known_values() {
        assert_eq!(iso_to_epoch("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(iso_to_epoch("2026-01-01T00:00:00Z"), Some(1_767_225_600));
        assert_eq!(iso_to_epoch("bad"), None);
    }
}
