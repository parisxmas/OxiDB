use std::collections::HashMap;

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xEDB88320 } else { crc >> 1 };
        }
    }
    !crc
}

pub fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().unwrap_or(b'0');
            let lo = chars.next().unwrap_or(b'0');
            let hex = [hi, lo];
            if let Ok(val) = u8::from_str_radix(std::str::from_utf8(&hex).unwrap_or("00"), 16) {
                result.push(val as char);
            }
        } else if b == b'+' {
            result.push(' ');
        } else {
            result.push(b as char);
        }
    }
    result
}

pub fn parse_query(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if query.is_empty() {
        return map;
    }
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(url_decode(k), url_decode(v));
        } else {
            map.insert(url_decode(pair), String::new());
        }
    }
    map
}

/// Extract values between <Tag>value</Tag> from XML.
pub fn extract_xml_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut results = Vec::new();
    let mut pos = 0;
    while let Some(start) = xml[pos..].find(&open) {
        let start = pos + start + open.len();
        if let Some(end) = xml[start..].find(&close) {
            results.push(xml[start..start + end].to_string());
            pos = start + end + close.len();
        } else {
            break;
        }
    }
    results
}

/// Extract <Tag><Key>k</Key><Value>v</Value></Tag> pairs from XML.
pub fn extract_xml_tag_pairs(xml: &str) -> Vec<(String, String)> {
    let keys = extract_xml_values(xml, "Key");
    let values = extract_xml_values(xml, "Value");
    keys.into_iter().zip(values.into_iter()).collect()
}

pub fn parse_range(header: &str, total: u64) -> Option<(u64, u64)> {
    let s = header.strip_prefix("bytes=")?;
    let (start_s, end_s) = s.split_once('-')?;

    if start_s.is_empty() {
        let suffix: u64 = end_s.parse().ok()?;
        if suffix > total { return None; }
        Some((total - suffix, total - 1))
    } else {
        let start: u64 = start_s.parse().ok()?;
        if start >= total { return None; }
        let end = if end_s.is_empty() {
            total - 1
        } else {
            let e: u64 = end_s.parse().ok()?;
            e.min(total - 1)
        };
        if start > end { return None; }
        Some((start, end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range("bytes=0-499", 1000), Some((0, 499)));
        assert_eq!(parse_range("bytes=500-999", 1000), Some((500, 999)));
        assert_eq!(parse_range("bytes=500-", 1000), Some((500, 999)));
        assert_eq!(parse_range("bytes=-200", 1000), Some((800, 999)));
        assert_eq!(parse_range("bytes=0-0", 1000), Some((0, 0)));
        assert_eq!(parse_range("bytes=1000-", 1000), None);
    }

    #[test]
    fn test_xml_extract() {
        let xml = "<Delete><Object><Key>file1.txt</Key></Object><Object><Key>file2.txt</Key></Object></Delete>";
        let keys = extract_xml_values(xml, "Key");
        assert_eq!(keys, vec!["file1.txt", "file2.txt"]);
    }

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let c = crc32(data);
        assert_eq!(format!("{:08x}", c), "0d4a1185");
    }
}
