use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex};

use oxidb::OxiDb;
use serde::Serialize;
use serde_json::json;
use tauri::State;

use crate::state::DbBackend;

#[derive(Serialize)]
pub struct ConnectionStatus {
    pub connected: bool,
    pub mode: String,
    pub detail: String,
}

#[tauri::command]
pub fn open_embedded(
    path: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<ConnectionStatus, String> {
    let db =
        OxiDb::open(Path::new(&path)).map_err(|e| format!("failed to open database: {e}"))?;

    // Scan for existing .dat files to auto-load collections
    if let Ok(entries) = std::fs::read_dir(&path) {
        for entry in entries.flatten() {
            let fname = entry.file_name();
            let fname = fname.to_string_lossy();
            if fname.ends_with(".dat") {
                let col_name = &fname[..fname.len() - 4];
                // Force collection load by doing a count
                let _ = db.count(col_name, &json!({}));
            }
        }
    }

    let mut backend = state.lock().unwrap();
    *backend = DbBackend::Embedded {
        db: Arc::new(db),
        active_tx: None,
        data_path: path.clone(),
    };
    Ok(ConnectionStatus {
        connected: true,
        mode: "embedded".to_string(),
        detail: path,
    })
}

#[tauri::command]
pub fn connect_remote(
    host: String,
    port: u16,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<ConnectionStatus, String> {
    let stream = TcpStream::connect((&host[..], port))
        .map_err(|e| format!("connection failed: {e}"))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .ok();

    let detail = format!("{host}:{port}");
    let mut backend = state.lock().unwrap();
    *backend = DbBackend::Client {
        stream,
        host: host.clone(),
        port,
    };
    Ok(ConnectionStatus {
        connected: true,
        mode: "client".to_string(),
        detail,
    })
}

#[tauri::command]
pub fn disconnect(state: State<'_, Mutex<DbBackend>>) -> Result<(), String> {
    let mut backend = state.lock().unwrap();
    *backend = DbBackend::Disconnected;
    Ok(())
}

#[tauri::command]
pub fn get_connection_status(state: State<'_, Mutex<DbBackend>>) -> ConnectionStatus {
    let backend = state.lock().unwrap();
    match &*backend {
        DbBackend::Embedded { data_path, .. } => ConnectionStatus {
            connected: true,
            mode: "embedded".to_string(),
            detail: data_path.clone(),
        },
        DbBackend::Client { host, port, .. } => ConnectionStatus {
            connected: true,
            mode: "client".to_string(),
            detail: format!("{host}:{port}"),
        },
        DbBackend::Disconnected => ConnectionStatus {
            connected: false,
            mode: "disconnected".to_string(),
            detail: String::new(),
        },
    }
}
