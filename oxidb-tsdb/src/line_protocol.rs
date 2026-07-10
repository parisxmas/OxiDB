//! InfluxDB line-protocol parser.
//!
//! ```text
//! measurement[,tag=val,...] field=val[,field=val,...] [timestamp]
//! ```
//! Field values: `1.5` float, `10i` integer, `t`/`true`/`f`/`false` boolean.
//! (String field values `"..."` are skipped — this engine stores numerics.)
//! The timestamp is epoch **milliseconds** here; when absent the caller supplies
//! one. Backslash escapes for `,` ` ` `=` in identifiers are honored.

use crate::model::{FieldValue, Point};

/// Parse one or more newline-separated lines. `default_ts` is used for lines
/// without an explicit timestamp. Blank lines and `#` comments are ignored.
pub fn parse(input: &str, default_ts: i64) -> Result<Vec<Point>, String> {
    let mut out = Vec::new();
    for (lineno, raw) in input.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        out.push(parse_line(line, default_ts).map_err(|e| format!("line {}: {e}", lineno + 1))?);
    }
    Ok(out)
}

fn parse_line(line: &str, default_ts: i64) -> Result<Point, String> {
    // Split into the three top-level parts on unescaped spaces.
    let parts = split_unescaped(line, ' ');
    if parts.is_empty() {
        return Err("empty line".into());
    }
    if parts.len() < 2 {
        return Err("missing field set".into());
    }
    // Part 0: measurement[,tags]
    let mut key_parts = split_unescaped(&parts[0], ',');
    let measurement = unescape(&key_parts.remove(0));
    let ts = if parts.len() >= 3 {
        parts[2]
            .parse::<i64>()
            .map_err(|_| format!("invalid timestamp {:?}", parts[2]))?
    } else {
        default_ts
    };
    let mut point = Point::new(&measurement, ts);
    for tag in key_parts {
        let (k, v) = split_kv(&tag)?;
        point = point.tag(&unescape(&k), &unescape(&v));
    }
    // Part 1: field set
    let mut any = false;
    for f in split_unescaped(&parts[1], ',') {
        let (k, v) = split_kv(&f)?;
        let name = unescape(&k);
        match parse_field_value(&v) {
            Some(FieldValue::Float(x)) => point = point.field(&name, x),
            Some(FieldValue::Int(x)) => point = point.field_int(&name, x),
            Some(FieldValue::Bool(x)) => point = point.field_bool(&name, x),
            None => continue, // string field — skip
        }
        any = true;
    }
    if !any {
        return Err("no numeric/boolean fields".into());
    }
    Ok(point)
}

fn parse_field_value(v: &str) -> Option<FieldValue> {
    if v.starts_with('"') {
        return None; // string field, unsupported
    }
    match v {
        "t" | "T" | "true" | "True" | "TRUE" => return Some(FieldValue::Bool(true)),
        "f" | "F" | "false" | "False" | "FALSE" => return Some(FieldValue::Bool(false)),
        _ => {}
    }
    if let Some(stripped) = v.strip_suffix(['i', 'u'])
        && let Ok(n) = stripped.parse::<i64>()
    {
        return Some(FieldValue::Int(n));
    }
    v.parse::<f64>().ok().map(FieldValue::Float)
}

fn split_kv(s: &str) -> Result<(String, String), String> {
    let kv = split_unescaped(s, '=');
    if kv.len() != 2 {
        return Err(format!("expected key=value, got {s:?}"));
    }
    Ok((kv[0].clone(), kv[1].clone()))
}

/// Split on an unescaped delimiter (a `\` escapes the next char).
fn split_unescaped(s: &str, delim: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut esc = false;
    for c in s.chars() {
        if esc {
            cur.push('\\');
            cur.push(c);
            esc = false;
        } else if c == '\\' {
            esc = true;
        } else if c == delim {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    if esc {
        cur.push('\\');
    }
    out.push(cur);
    out
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut esc = false;
    for c in s.chars() {
        if esc {
            out.push(c);
            esc = false;
        } else if c == '\\' {
            esc = true;
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::FieldValue;

    #[test]
    fn parses_tags_fields_types_and_ts() {
        let pts = parse(
            "cpu,host=a,region=eu usage=0.9,cores=8i,up=true 1700000000000",
            0,
        )
        .unwrap();
        assert_eq!(pts.len(), 1);
        let p = &pts[0];
        assert_eq!(p.measurement, "cpu");
        assert_eq!(p.ts, 1700000000000);
        assert_eq!(
            p.tags,
            vec![("host".into(), "a".into()), ("region".into(), "eu".into())]
        );
        assert_eq!(p.fields[0], ("usage".into(), FieldValue::Float(0.9)));
        assert_eq!(p.fields[1], ("cores".into(), FieldValue::Int(8)));
        assert_eq!(p.fields[2], ("up".into(), FieldValue::Bool(true)));
    }

    #[test]
    fn default_ts_and_no_tags() {
        let pts = parse("mem free=1024i", 42).unwrap();
        assert_eq!(pts[0].ts, 42);
        assert!(pts[0].tags.is_empty());
        assert_eq!(pts[0].fields[0], ("free".into(), FieldValue::Int(1024)));
    }

    #[test]
    fn escaped_spaces_in_tag_values() {
        let pts = parse(r"weather,location=us\ midwest temp=82", 0).unwrap();
        assert_eq!(pts[0].tags, vec![("location".into(), "us midwest".into())]);
        assert_eq!(pts[0].fields[0], ("temp".into(), FieldValue::Float(82.0)));
    }
}
