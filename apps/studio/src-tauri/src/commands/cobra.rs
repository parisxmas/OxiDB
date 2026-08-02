//! Compile a Cobra stored procedure locally: run the user's `cobra` CLI on
//! their machine to turn `.cobra` source into portable `.cobrac` bytecode,
//! returned base64-encoded and ready for `CREATE PROCEDURE ... LANGUAGE
//! COBRA AS '<base64>'`. The Go toolchain stays the compiler (ADR-0014); the
//! app only orchestrates it.

use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;

/// Where a `cobra` binary commonly lives. Tauri apps launched from Finder get
/// a minimal PATH, so we probe explicit locations in addition to PATH.
fn candidate_paths() -> Vec<PathBuf> {
    let mut v = vec![
        PathBuf::from("/usr/local/bin/cobra"),
        PathBuf::from("/opt/homebrew/bin/cobra"),
        PathBuf::from("/usr/bin/cobra"),
    ];
    if let Ok(home) = std::env::var("HOME") {
        v.push(PathBuf::from(format!("{home}/go/bin/cobra")));
        v.push(PathBuf::from(format!("{home}/source/cobra/cobra")));
        v.push(PathBuf::from(format!("{home}/.local/bin/cobra")));
    }
    // PATH entries.
    if let Ok(path) = std::env::var("PATH") {
        for dir in path.split(':') {
            v.push(PathBuf::from(dir).join("cobra"));
        }
    }
    v
}

/// Resolve the cobra binary: an explicit path wins; otherwise the first
/// existing candidate.
fn resolve(explicit: &Option<String>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        if !p.is_empty() {
            let pb = PathBuf::from(p);
            return pb.exists().then_some(pb);
        }
    }
    candidate_paths().into_iter().find(|p| p.exists())
}

/// Report the resolved cobra binary path, if one is found.
#[tauri::command]
pub fn cobra_detect(cobra_path: Option<String>) -> Option<String> {
    resolve(&cobra_path).map(|p| p.to_string_lossy().into_owned())
}

/// Compile `.cobra` source → base64 of the portable `.cobrac`. On a compiler
/// error, returns the compiler's message (stderr) as Err.
#[tauri::command]
pub fn cobra_compile(source: String, cobra_path: Option<String>) -> Result<String, String> {
    let bin = resolve(&cobra_path).ok_or_else(|| {
        "cobra compiler not found — build it (`cd ~/source/cobra && go build -o cobra .`) \
         and set its path"
            .to_string()
    })?;

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir();
    let src = dir.join(format!("oxidb_sp_{stamp}.cobra"));
    let out = dir.join(format!("oxidb_sp_{stamp}.cobrac"));

    std::fs::write(&src, source).map_err(|e| format!("write temp source: {e}"))?;

    let result = Command::new(&bin)
        .args([
            "build",
            "--portable",
            &src.to_string_lossy(),
            &out.to_string_lossy(),
        ])
        .output();

    // Clean up the source regardless.
    let _ = std::fs::remove_file(&src);

    let output = match result {
        Ok(o) => o,
        Err(e) => {
            let _ = std::fs::remove_file(&out);
            return Err(format!("failed to run cobra: {e}"));
        }
    };

    if !output.status.success() {
        let _ = std::fs::remove_file(&out);
        let msg = if output.stderr.is_empty() {
            String::from_utf8_lossy(&output.stdout).into_owned()
        } else {
            String::from_utf8_lossy(&output.stderr).into_owned()
        };
        return Err(msg.trim().to_string());
    }

    let bytes = std::fs::read(&out).map_err(|e| format!("read compiled bytecode: {e}"))?;
    let _ = std::fs::remove_file(&out);
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}
