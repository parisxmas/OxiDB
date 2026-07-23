//! A blocking, thread-pooled HTTP listener — the generalized form of the
//! server's REST listener loop, so any OxiDB service (e.g. `oxibase`) can serve
//! HTTP with one `serve(addr, …, handler)` call.

use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::message::{HttpRequest, HttpResponse, parse_request_from_reader};

/// Serve HTTP on `addr` with `pool_size` worker threads and a bounded accept
/// queue, dispatching each request to `handler`. Blocks forever (spawns the
/// accept loop on the calling thread). Keep-alive is honored unless the client
/// sends `Connection: close`.
pub fn serve<H>(addr: &str, pool_size: usize, max_queued: usize, handler: H) -> std::io::Result<()>
where
    H: Fn(&HttpRequest) -> HttpResponse + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr)?;
    let handler = Arc::new(handler);

    let (tx, rx) = std::sync::mpsc::sync_channel::<TcpStream>(max_queued);
    let rx = Arc::new(Mutex::new(rx));

    for i in 0..pool_size.max(1) {
        let rx = Arc::clone(&rx);
        let handler = Arc::clone(&handler);
        std::thread::Builder::new()
            .name(format!("http-worker-{i}"))
            .spawn(move || {
                loop {
                    let stream = match rx.lock().unwrap().recv() {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        handle_connection(stream, &*handler);
                    }));
                    if let Err(e) = result {
                        let msg = e
                            .downcast_ref::<&str>()
                            .map(|s| s.to_string())
                            .or_else(|| e.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "unknown panic".to_string());
                        eprintln!("[http] handler panicked: {msg}");
                    }
                }
            })?;
    }

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                if tx.try_send(s).is_err() {
                    eprintln!("[http] connection rejected: queue full");
                }
            }
            Err(e) => eprintln!("[http] accept error: {e}"),
        }
    }
    Ok(())
}

fn handle_connection<H>(mut stream: TcpStream, handler: &H)
where
    H: Fn(&HttpRequest) -> HttpResponse,
{
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let Ok(read_stream) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(read_stream);

    loop {
        let req = match parse_request_from_reader(&mut reader, &stream) {
            Some(r) => r,
            None => return,
        };
        let wants_close = req
            .headers
            .get("connection")
            .is_some_and(|v| v.eq_ignore_ascii_case("close"));

        let resp = handler(&req);
        resp.write_to_keepalive(&mut stream, !wants_close);

        if wants_close {
            return;
        }
    }
}
