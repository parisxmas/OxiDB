//! Minimal filesystem access for import: read a user-picked file's text.
//! The path always comes from the OS file-open dialog, so it's a deliberate
//! user choice rather than arbitrary app-driven access.

use std::fs;

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
