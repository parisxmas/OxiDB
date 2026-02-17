use oxidb_server::handler;
use oxidb_server::protocol;

use std::env;
use std::io::{BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxidb::OxiDb;

fn configure_stream(stream: &TcpStream, idle_timeout: Duration) {
    let _ = stream.set_read_timeout(Some(idle_timeout));
    let _ = stream.set_nodelay(true);
}

fn handle_client(stream: TcpStream, db: &Arc<OxiDb>, idle_timeout: Duration) {
    configure_stream(&stream, idle_timeout);

    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    eprintln!("client connected: {peer}");

    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let mut active_tx: Option<u64> = None;

    loop {
        let msg = match protocol::read_message(&mut reader) {
            Ok(m) => m,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    eprintln!("idle timeout for {peer}, disconnecting");
                } else if e.kind() != std::io::ErrorKind::UnexpectedEof {
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
                    &mut writer,
                    resp.to_string().as_bytes(),
                );
                continue;
            }
        };

        let response = handler::handle_request(db, request, &mut active_tx);
        let resp_bytes = response.to_string().into_bytes();

        if let Err(e) = protocol::write_message(&mut writer, &resp_bytes) {
            eprintln!("write error to {peer}: {e}");
            break;
        }
    }

    // Auto-rollback on disconnect
    if let Some(tx_id) = active_tx {
        let _ = db.rollback_transaction(tx_id);
    }

    eprintln!("client disconnected: {peer}");
}

fn main() {
    let addr = env::var("OXIDB_ADDR").unwrap_or_else(|_| "127.0.0.1:4444".to_string());
    let data_dir = env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".to_string());
    let pool_size: usize = env::var("OXIDB_POOL_SIZE")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .expect("OXIDB_POOL_SIZE must be a valid usize");

    let idle_timeout_secs: u64 = env::var("OXIDB_IDLE_TIMEOUT")
        .unwrap_or_else(|_| "30".to_string())
        .parse()
        .expect("OXIDB_IDLE_TIMEOUT must be a valid u64 (seconds)");
    let idle_timeout = Duration::from_secs(idle_timeout_secs);

    let db = OxiDb::open(Path::new(&data_dir)).expect("failed to open database");
    let db = Arc::new(db);

    let listener = TcpListener::bind(&addr).expect("failed to bind TCP listener");
    eprintln!("oxidb-server listening on {addr} (pool_size={pool_size}, data_dir={data_dir}, idle_timeout={idle_timeout_secs}s)");

    let (tx, rx) = mpsc::channel::<TcpStream>();
    let rx = Arc::new(Mutex::new(rx));

    for _ in 0..pool_size {
        let rx = Arc::clone(&rx);
        let db = Arc::clone(&db);
        std::thread::spawn(move || loop {
            let stream = rx.lock().unwrap().recv();
            match stream {
                Ok(stream) => handle_client(stream, &db, idle_timeout),
                Err(_) => break,
            }
        });
    }

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if let Err(e) = tx.send(s) {
                    eprintln!("failed to dispatch connection: {e}");
                }
            }
            Err(e) => {
                eprintln!("accept error: {e}");
            }
        }
    }
}
