//! HTTP request/response types and request parsing. Moved verbatim from the
//! server's former `s3::http` (pure `std`, no engine dependency).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;

/// Cached CORS origin — read once from env at first use.
fn cors_origin() -> &'static str {
    static ORIGIN: OnceLock<String> = OnceLock::new();
    ORIGIN.get_or_init(|| std::env::var("OXIDB_S3_CORS_ORIGIN").unwrap_or_else(|_| "*".to_string()))
}

/// Maximum request body size: 5 GiB (S3 single PUT limit).
const MAX_BODY_SIZE: usize = 5 * 1024 * 1024 * 1024;
/// Maximum single header/request line size: 8 KiB.
const MAX_LINE_SIZE: usize = 8 * 1024;

pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub status_text: &'static str,
    pub content_type: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub content_length_override: Option<u64>,
}

impl HttpResponse {
    pub fn xml(status: u16, status_text: &'static str, body: String) -> Self {
        Self {
            status,
            status_text,
            content_type: "application/xml".to_string(),
            headers: Vec::new(),
            body: body.into_bytes(),
            content_length_override: None,
        }
    }

    pub fn ok_xml(body: String) -> Self {
        Self::xml(200, "OK", body)
    }

    pub fn no_content() -> Self {
        Self {
            status: 204,
            status_text: "No Content",
            content_type: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
            content_length_override: None,
        }
    }

    pub fn data(body: Vec<u8>, content_type: &str) -> Self {
        Self {
            status: 200,
            status_text: "OK",
            content_type: content_type.to_string(),
            headers: Vec::new(),
            body,
            content_length_override: None,
        }
    }

    pub fn partial(body: Vec<u8>, content_type: &str, range: &str, total: u64) -> Self {
        Self {
            status: 206,
            status_text: "Partial Content",
            content_type: content_type.to_string(),
            headers: vec![(
                "Content-Range".to_string(),
                format!("bytes {range}/{total}"),
            )],
            body,
            content_length_override: None,
        }
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_string(), value.to_string()));
        self
    }

    pub fn with_cors(self) -> Self {
        self.with_cors_origin(cors_origin())
    }

    pub fn with_cors_origin(self, origin: &str) -> Self {
        self.with_header("Access-Control-Allow-Origin", origin)
            .with_header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS")
            .with_header("Access-Control-Allow-Headers", "Content-Type, Authorization, x-amz-date, x-amz-content-sha256, x-amz-copy-source, Range, If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since")
            .with_header("Access-Control-Expose-Headers", "ETag, x-amz-request-id, Content-Range, Accept-Ranges")
            .with_header("Access-Control-Max-Age", "3600")
    }

    pub fn write_to(self, stream: &mut TcpStream) {
        self.write_to_keepalive(stream, false);
    }

    /// Threshold for switching to chunked transfer encoding (1 MiB).
    const CHUNK_THRESHOLD: usize = 1024 * 1024;
    /// Chunk size for large responses (256 KiB).
    const CHUNK_SIZE: usize = 256 * 1024;

    pub fn write_to_keepalive(self, stream: &mut TcpStream, keep_alive: bool) {
        let conn = if keep_alive { "keep-alive" } else { "close" };
        let use_chunked =
            self.content_length_override.is_none() && self.body.len() > Self::CHUNK_THRESHOLD;

        let mut resp = format!(
            "HTTP/1.1 {} {}\r\nConnection: {}\r\nServer: OxiDB-S3\r\n",
            self.status, self.status_text, conn
        );

        if use_chunked {
            resp.push_str("Transfer-Encoding: chunked\r\n");
        } else {
            let content_length = self
                .content_length_override
                .unwrap_or(self.body.len() as u64);
            resp.push_str(&format!("Content-Length: {content_length}\r\n"));
        }

        if !self.content_type.is_empty() {
            resp.push_str(&format!("Content-Type: {}\r\n", self.content_type));
        }
        for (k, v) in &self.headers {
            resp.push_str(&format!("{k}: {v}\r\n"));
        }
        resp.push_str("\r\n");
        if stream.write_all(resp.as_bytes()).is_err() {
            return;
        }

        if self.body.is_empty() {
            let _ = stream.flush();
            return;
        }

        if use_chunked {
            // Stream body in chunks — avoids holding entire response in one write buffer
            for chunk in self.body.chunks(Self::CHUNK_SIZE) {
                let header = format!("{:x}\r\n", chunk.len());
                if stream.write_all(header.as_bytes()).is_err() {
                    return;
                }
                if stream.write_all(chunk).is_err() {
                    return;
                }
                if stream.write_all(b"\r\n").is_err() {
                    return;
                }
            }
            if stream.write_all(b"0\r\n\r\n").is_err() {
                return;
            }
        } else if stream.write_all(&self.body).is_err() {
            return;
        }
        let _ = stream.flush();
    }
}

/// Read a line with a hard size limit using `take()` to prevent unbounded allocation.
fn read_line_bounded(reader: &mut BufReader<impl Read>, limit: usize) -> Option<String> {
    let mut line = String::new();
    // Read byte-by-byte up to limit, looking for newline
    let mut count = 0;
    loop {
        let buf = reader.fill_buf().ok()?;
        if buf.is_empty() {
            return if count == 0 { None } else { Some(line) };
        }
        // Find newline in the buffered data
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let to_consume = pos + 1;
            if count + to_consume > limit {
                return None;
            }
            line.push_str(&String::from_utf8_lossy(&buf[..to_consume]));
            reader.consume(to_consume);
            return Some(line);
        }
        // No newline yet — consume entire buffer
        let len = buf.len();
        if count + len > limit {
            return None;
        }
        line.push_str(&String::from_utf8_lossy(&buf[..len]));
        count += len;
        reader.consume(len);
    }
}

pub fn parse_request_from_reader(
    reader: &mut BufReader<impl Read>,
    writer: &TcpStream,
) -> Option<HttpRequest> {
    let line = read_line_bounded(reader, MAX_LINE_SIZE)?;
    let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
    if parts.len() < 2 {
        return None;
    }
    let method = parts[0].to_uppercase();
    let raw_path = parts[1];

    let (path, query) = match raw_path.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (raw_path.to_string(), String::new()),
    };

    let mut headers = HashMap::new();
    while let Some(hline) = read_line_bounded(reader, MAX_LINE_SIZE) {
        let hline = hline.trim_end();
        if hline.is_empty() {
            break;
        }
        if let Some((k, v)) = hline.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }

    // Handle Expect: 100-continue
    if headers
        .get("expect")
        .is_some_and(|v| v.contains("100-continue"))
    {
        let cont = b"HTTP/1.1 100 Continue\r\n\r\n";
        let _ = writer.try_clone().ok()?.write_all(cont);
    }

    let body = if let Some(cl) = headers.get("content-length") {
        let len: usize = cl.parse().unwrap_or(0);
        if len > MAX_BODY_SIZE {
            return None; // reject oversized requests
        }
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).ok()?;
        buf
    } else if headers
        .get("transfer-encoding")
        .is_some_and(|v| v.contains("chunked"))
    {
        read_chunked(reader).unwrap_or_default()
    } else {
        Vec::new()
    };

    Some(HttpRequest {
        method,
        path,
        query,
        headers,
        body,
    })
}

fn read_chunked(reader: &mut BufReader<impl Read>) -> Option<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let size = usize::from_str_radix(line.trim(), 16).ok()?;
        if size == 0 {
            let mut trail = String::new();
            let _ = reader.read_line(&mut trail);
            break;
        }
        if body.len() + size > MAX_BODY_SIZE {
            return None; // reject oversized chunked body
        }
        let mut chunk = vec![0u8; size];
        reader.read_exact(&mut chunk).ok()?;
        body.extend_from_slice(&chunk);
        let mut trail = String::new();
        let _ = reader.read_line(&mut trail);
    }
    Some(body)
}

/// Who a request came from, as far as the edge in front will say.
///
/// Behind Cloudflare the socket peer is Cloudflare, so the useful identity is in
/// headers: `CF-Connecting-IP` is the visitor, and the `CF-IP*` family carries
/// coarse geolocation. Those location headers are **not sent by default** — they
/// appear once "Add visitor location headers" is switched on for the zone
/// (Cloudflare dashboard → Rules → Settings → Managed Transforms), except
/// `CF-IPCountry`, which follows the IP Geolocation toggle.
///
/// Everything is borrowed and empty-when-absent, so logging it costs nothing on
/// a deployment with no proxy in front.
pub struct ClientMeta<'a> {
    /// The visitor's address: `CF-Connecting-IP`, else the first hop of
    /// `X-Forwarded-For`, else `X-Real-IP`. Empty when nothing said.
    pub ip: &'a str,
    /// ISO country, e.g. `TR`. `XX` means Cloudflare could not tell; `T1` is Tor.
    pub country: &'a str,
    pub city: &'a str,
    pub region: &'a str,
    pub continent: &'a str,
    pub timezone: &'a str,
    /// Approximate coordinates of the visitor's city, when the zone sends them.
    /// Cloudflare's own accuracy caveat applies: it is a city, not a person.
    pub latitude: &'a str,
    pub longitude: &'a str,
    /// Cloudflare's request id — the handle for finding the same request in
    /// Cloudflare's own logs.
    pub ray: &'a str,
    pub user_agent: &'a str,
}

impl<'a> ClientMeta<'a> {
    /// The non-empty fields, ready to attach to a log record.
    pub fn fields(&self) -> Vec<(&'static str, &'a str)> {
        [
            ("ip", self.ip),
            ("country", self.country),
            ("city", self.city),
            ("region", self.region),
            ("continent", self.continent),
            ("timezone", self.timezone),
            ("lat", self.latitude),
            ("lon", self.longitude),
            ("cf_ray", self.ray),
            ("user_agent", self.user_agent),
        ]
        .into_iter()
        .filter(|(_, v)| !v.is_empty())
        .collect()
    }
}

impl HttpRequest {
    /// Read the caller's address and location off the edge's headers.
    pub fn client_meta(&self) -> ClientMeta<'_> {
        let h = |name: &str| self.headers.get(name).map(String::as_str).unwrap_or("");
        // The first hop of X-Forwarded-For is the client; the rest are proxies.
        let forwarded = h("x-forwarded-for").split(',').next().unwrap_or("").trim();
        let ip = match h("cf-connecting-ip") {
            "" => match forwarded {
                "" => h("x-real-ip"),
                f => f,
            },
            cf => cf,
        };
        ClientMeta {
            ip,
            country: h("cf-ipcountry"),
            city: h("cf-ipcity"),
            region: h("cf-region"),
            continent: h("cf-ipcontinent"),
            timezone: h("cf-timezone"),
            latitude: h("cf-iplatitude"),
            longitude: h("cf-iplongitude"),
            ray: h("cf-ray"),
            user_agent: h("user-agent"),
        }
    }
}

#[cfg(test)]
mod client_meta_tests {
    use super::*;
    use std::collections::HashMap;

    fn req(headers: &[(&str, &str)]) -> HttpRequest {
        HttpRequest {
            method: "GET".into(),
            path: "/".into(),
            query: String::new(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            body: Vec::new(),
        }
    }

    #[test]
    fn cloudflare_wins_over_the_forwarded_chain() {
        let r = req(&[
            ("cf-connecting-ip", "203.0.113.7"),
            ("x-forwarded-for", "203.0.113.7, 172.70.1.1"),
        ]);
        assert_eq!(r.client_meta().ip, "203.0.113.7");
    }

    #[test]
    fn the_first_hop_is_the_client() {
        // The rest of the chain is proxies; logging the last one would record
        // Cloudflare's address as the visitor's.
        let r = req(&[("x-forwarded-for", "203.0.113.7, 172.70.1.1, 10.0.0.3")]);
        assert_eq!(r.client_meta().ip, "203.0.113.7");
    }

    #[test]
    fn absent_headers_are_empty_not_missing_fields() {
        let bare = req(&[]);
        let meta = bare.client_meta();
        assert_eq!(meta.ip, "");
        assert!(
            meta.fields().is_empty(),
            "nothing to log when nothing is known"
        );
    }

    #[test]
    fn location_headers_are_carried_when_the_zone_sends_them() {
        let r = req(&[
            ("cf-connecting-ip", "203.0.113.7"),
            ("cf-ipcountry", "TR"),
            ("cf-ipcity", "Istanbul"),
            ("cf-ray", "9a1b2c3d4e5f6789-IST"),
            ("cf-iplatitude", "41.01384"),
            ("cf-iplongitude", "28.94966"),
            ("user-agent", "curl/8"),
        ]);
        let meta = r.client_meta();
        let fields = meta.fields();
        assert!(fields.contains(&("country", "TR")));
        assert!(fields.contains(&("city", "Istanbul")));
        assert!(fields.contains(&("cf_ray", "9a1b2c3d4e5f6789-IST")));
        assert!(fields.contains(&("lat", "41.01384")));
        assert!(fields.contains(&("lon", "28.94966")));
        // Not sent by the zone → not invented.
        assert!(!fields.iter().any(|(k, _)| *k == "timezone"));
    }
}
