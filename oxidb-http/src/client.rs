//! A tiny blocking HTTP/1.1 client for service-to-service calls (e.g. `oxibase`
//! → `oxidb-server`). Enough to send a JSON request and read the response;
//! `http://` only, no TLS (internal traffic behind the reverse proxy).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// A parsed HTTP response.
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Response {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Send an HTTP request. `url` is `http://host[:port]/path[?query]`. Extra
/// `headers` are sent verbatim; a `Content-Length` and `Host` are added
/// automatically. Returns the status and body.
pub fn request(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<Response> {
    let (host, port, target) = split_url(url)?;
    let mut stream = TcpStream::connect((host.as_str(), port))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    let _ = stream.set_nodelay(true);

    let mut req = format!(
        "{method} {target} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes())?;
    if !body.is_empty() {
        stream.write_all(body)?;
    }
    stream.flush()?;

    read_response(&mut stream)
}

/// `POST` a JSON body with an optional bearer token.
pub fn post_json(url: &str, bearer: Option<&str>, json: &[u8]) -> std::io::Result<Response> {
    let auth = bearer.map(|t| format!("Bearer {t}"));
    let mut headers: Vec<(&str, &str)> = vec![("Content-Type", "application/json")];
    if let Some(a) = &auth {
        headers.push(("Authorization", a));
    }
    request("POST", url, &headers, json)
}

/// `GET` a URL with an optional bearer token.
pub fn get(url: &str, bearer: Option<&str>) -> std::io::Result<Response> {
    let auth = bearer.map(|t| format!("Bearer {t}"));
    let mut headers: Vec<(&str, &str)> = Vec::new();
    if let Some(a) = &auth {
        headers.push(("Authorization", a));
    }
    request("GET", url, &headers, &[])
}

fn split_url(url: &str) -> std::io::Result<(String, u16, String)> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| err("only http:// URLs are supported"))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().map_err(|_| err("invalid port"))?),
        None => (authority.to_string(), 80),
    };
    Ok((host, port, path.to_string()))
}

fn read_response(stream: &mut TcpStream) -> std::io::Result<Response> {
    let mut reader = BufReader::new(stream);

    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| err("malformed status line"))?;

    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim();
            if k == "content-length" {
                content_length = v.parse().ok();
            } else if k == "transfer-encoding" && v.eq_ignore_ascii_case("chunked") {
                chunked = true;
            }
        }
    }

    let body = if chunked {
        read_chunked(&mut reader)?
    } else if let Some(len) = content_length {
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        buf
    } else {
        // Connection: close with no length — read to EOF.
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        buf
    };
    Ok(Response { status, body })
}

fn read_chunked(reader: &mut BufReader<&mut TcpStream>) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let size = usize::from_str_radix(line.trim(), 16).map_err(|_| err("bad chunk size"))?;
        if size == 0 {
            let mut trail = String::new();
            let _ = reader.read_line(&mut trail);
            break;
        }
        let mut chunk = vec![0u8; size];
        reader.read_exact(&mut chunk)?;
        body.extend_from_slice(&chunk);
        let mut trail = String::new();
        let _ = reader.read_line(&mut trail);
    }
    Ok(body)
}

fn err(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}
