mod handler;
mod protocol;

use std::env;
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;

use oxidb::OxiDb;

fn main() {
    let addr = env::var("OXIDB_ADDR").unwrap_or_else(|_| "127.0.0.1:4444".to_string());
    let data_dir = env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".to_string());

    let db = OxiDb::open(Path::new(&data_dir)).expect("failed to open database");
    let db = Arc::new(db);

    let listener = TcpListener::bind(&addr).expect("failed to bind TCP listener");
    eprintln!("oxidb-server listening on {addr}");

    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept error: {e}");
                continue;
            }
        };

        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            let peer = stream
                .peer_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|_| "unknown".into());
            eprintln!("client connected: {peer}");

            loop {
                let msg = match protocol::read_message(&mut stream) {
                    Ok(m) => m,
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::UnexpectedEof {
                            eprintln!("read error from {peer}: {e}");
                        }
                        break;
                    }
                };

                let request: serde_json::Value = match serde_json::from_slice(&msg) {
                    Ok(v) => v,
                    Err(e) => {
                        let resp =
                            serde_json::json!({"ok": false, "error": format!("invalid JSON: {e}")});
                        let _ = protocol::write_message(
                            &mut stream,
                            resp.to_string().as_bytes(),
                        );
                        continue;
                    }
                };

                let response = handler::handle_request(&db, &request);
                let resp_bytes = response.to_string().into_bytes();

                if let Err(e) = protocol::write_message(&mut stream, &resp_bytes) {
                    eprintln!("write error to {peer}: {e}");
                    break;
                }
            }

            eprintln!("client disconnected: {peer}");
        });
    }
}
