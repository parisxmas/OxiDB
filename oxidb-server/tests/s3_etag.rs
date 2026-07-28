//! S3 says an ETag is an MD5, and clients believe it.
//!
//! This is not pedantry about a spec. The AWS SDK for .NET re-computes the MD5
//! of what it uploaded and throws when the returned ETag disagrees, so an ETag
//! that is "some 32-hex-character digest" is not merely non-standard — it is
//! indistinguishable from a corrupted upload. These pin the two rules, which
//! are different from each other and easy to conflate:
//!
//!   single PUT  → hex MD5 of the object's bytes
//!   multipart   → hex MD5 of the concatenated part MD5 *digests*, then `-N`
//!
//! The second is why real multipart ETags end in `-3`, and why it cannot be
//! recovered from the assembled object once the part boundaries are gone.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};

fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    Md5::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

struct Server {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    // Every test gets its own port AND its own data dir. Deriving the port from
    // the pid alone gave all three tests in this binary the same one: two
    // servers failed to bind, and the tests quietly talked to whichever won.
    // They passed — while sharing one server and asserting on each other's
    // objects. A test that passes by accident proves nothing.
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let port = 15000 + (std::process::id() % 400) as u16 * 10 + NEXT.fetch_add(1, Ordering::SeqCst);
    let bin = env!("CARGO_BIN_EXE_oxidb-server");
    // The child is owned by a guard that kills and waits on Drop.
    #[allow(clippy::zombie_processes)]
    let child = Command::new(bin)
        .env("OXIDB_ADDR", format!("127.0.0.1:{}", port + 1000))
        .env("OXIDB_S3_PORT", port.to_string())
        .env("OXIDB_DATA", dir.path())
        .spawn()
        .expect("start oxidb-server");

    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Server {
                child,
                port,
                _dir: dir,
            };
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("the S3 listener never came up");
}

/// Minimal HTTP/1.1 — the point is to see the raw ETag header, so no SDK.
fn request(port: u16, method: &str, path: &str, body: &[u8]) -> (u16, String, Vec<u8>) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    s.flush().unwrap();

    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header terminator");
    let headers = String::from_utf8_lossy(&raw[..split]).to_string();
    let status = headers
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    (status, headers, raw[split + 4..].to_vec())
}

fn etag_of(headers: &str) -> String {
    headers
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("etag:"))
        .map(|l| l[5..].trim().trim_matches('"').to_string())
        .unwrap_or_default()
}

#[test]
fn single_put_etag_is_the_md5_of_the_object() {
    let srv = start();
    let body = b"CERTIFICATE OF CONFORMITY\nShipment SHP-1004\n".repeat(10);

    request(srv.port, "PUT", "/etags", b"");
    let (status, headers, _) = request(srv.port, "PUT", "/etags/cert.txt", &body);
    assert_eq!(status, 200, "{headers}");

    let etag = etag_of(&headers);
    assert_eq!(
        etag,
        md5_hex(&body),
        "S3 defines a single-part ETag as the hex MD5 of the bytes"
    );

    // It has to persist, not just be returned: clients keep it and send it
    // back as If-Match.
    let (_, head_headers, _) = request(srv.port, "HEAD", "/etags/cert.txt", b"");
    assert_eq!(
        etag_of(&head_headers),
        etag,
        "HEAD must report the same ETag PUT handed out"
    );
}

#[test]
fn an_etag_is_thirty_two_hex_characters() {
    let srv = start();
    request(srv.port, "PUT", "/shape", b"");
    let (_, headers, _) = request(srv.port, "PUT", "/shape/o", b"x");
    let etag = etag_of(&headers);

    assert_eq!(etag.len(), 32, "got {etag:?}");
    assert!(
        etag.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "lowercase hex only, got {etag:?}"
    );
}

#[test]
fn multipart_etag_is_the_md5_of_the_part_md5s_with_a_part_count() {
    let srv = start();
    request(srv.port, "PUT", "/mp", b"");

    // Two parts, each big enough to be a plausible part.
    let p1 = b"A".repeat(1024);
    let p2 = b"B".repeat(2048);

    let (status, headers, body) = request(srv.port, "POST", "/mp/big?uploads", b"");
    assert_eq!(status, 200, "{headers}");
    let xml = String::from_utf8_lossy(&body);
    let upload_id = xml
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .expect("no UploadId")
        .to_string();

    let mut part_digests = Vec::new();
    for (n, part) in [(1u32, &p1), (2u32, &p2)] {
        let (st, h, _) = request(
            srv.port,
            "PUT",
            &format!("/mp/big?partNumber={n}&uploadId={upload_id}"),
            part,
        );
        assert_eq!(st, 200, "{h}");
        // Each part's ETag is that part's MD5 — this was a CRC32, which is not
        // even the right length.
        assert_eq!(etag_of(&h), md5_hex(part), "part {n}");
        part_digests.extend_from_slice(&{
            use md5::{Digest, Md5};
            Md5::digest(part.as_slice())
        });
    }

    let complete = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part>\
         <Part><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>"
        .to_string();
    let (st, h, body) = request(
        srv.port,
        "POST",
        &format!("/mp/big?uploadId={upload_id}"),
        complete.as_bytes(),
    );
    assert_eq!(st, 200, "{h}");

    let want = format!("{}-2", md5_hex(&part_digests));
    let xml = String::from_utf8_lossy(&body);
    let got = xml
        .split("<ETag>")
        .nth(1)
        .and_then(|s| s.split("</ETag>").next())
        .unwrap_or("")
        .trim_matches('"')
        .to_string();
    assert_eq!(
        got, want,
        "a completed multipart ETag is md5(concat of part md5 digests) + \"-N\""
    );

    // And crucially it is NOT the MD5 of the assembled object — conflating the
    // two is the easy mistake, and it is the difference between an ETag with a
    // `-2` on it and one without.
    let mut assembled = p1.clone();
    assembled.extend_from_slice(&p2);
    assert_ne!(got, md5_hex(&assembled));

    // The stored tag must agree with the one Complete returned, or If-Match
    // against it fails forever after.
    let (_, head_headers, _) = request(srv.port, "HEAD", "/mp/big", b"");
    assert_eq!(etag_of(&head_headers), want, "HEAD disagrees with Complete");
}
