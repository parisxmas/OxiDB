//! Minimal filesystem access for import: read a user-picked file's text.
//! The path always comes from the OS file-open dialog, so it's a deliberate
//! user choice rather than arbitrary app-driven access.

use std::fs;

use base64::Engine;

/// Read any file (binary-safe) as base64 — for uploading a blob object.
#[tauri::command]
pub fn read_file_base64(path: String) -> Result<String, String> {
    let meta = fs::metadata(&path).map_err(|e| format!("stat {path}: {e}"))?;
    const MAX: u64 = 128 * 1024 * 1024; // 128 MiB
    if meta.len() > MAX {
        return Err(format!(
            "file is {} MiB — larger than the 128 MiB upload limit",
            meta.len() / (1024 * 1024)
        ));
    }
    let bytes = fs::read(&path).map_err(|e| format!("read {path}: {e}"))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(&bytes))
}

/// Write base64 content to a path — for downloading a blob object to disk.
#[tauri::command]
pub fn write_file_base64(path: String, base64_data: String) -> Result<(), String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(base64_data.as_bytes())
        .map_err(|e| format!("invalid base64: {e}"))?;
    fs::write(&path, bytes).map_err(|e| format!("write {path}: {e}"))
}

#[tauri::command]
pub fn read_file_text(path: String) -> Result<String, String> {
    // Guard against accidentally slurping a huge file into the webview.
    let meta = fs::metadata(&path).map_err(|e| format!("stat {path}: {e}"))?;
    const MAX: u64 = 64 * 1024 * 1024; // 64 MiB
    if meta.len() > MAX {
        return Err(format!(
            "file is {} MiB — larger than the 64 MiB import limit",
            meta.len() / (1024 * 1024)
        ));
    }
    fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))
}
