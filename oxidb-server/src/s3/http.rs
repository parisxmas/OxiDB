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
        } else {
            if stream.write_all(&self.body).is_err() {
                return;
            }
        }
        let _ = stream.flush();
    }
}

pub fn error_response(status: u16, code: &str, message: &str, resource: &str) -> HttpResponse {
    let status_text = match status {
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        412 => "Precondition Failed",
        416 => "Requested Range Not Satisfiable",
        _ => "Internal Server Error",
    };
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>{}</Code>\n  <Message>{}</Message>\n  <Resource>{}</Resource>\n  <RequestId>0</RequestId>\n</Error>",
        super::helpers::xml_escape(code),
        super::helpers::xml_escape(message),
        super::helpers::xml_escape(resource)
    );
    HttpResponse::xml(status, status_text, xml)
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
    loop {
        let hline = match read_line_bounded(reader, MAX_LINE_SIZE) {
            Some(l) => l,
            None => break,
        };
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
