mod commands;
mod scram;
mod state;

use std::sync::Mutex;

use state::DbBackend;
use tauri::{Emitter, WindowEvent};
use tauri_plugin_global_shortcut::{Code, GlobalShortcutExt, Shortcut, ShortcutState};

/// F5 (no modifiers) — the "Run query" accelerator. Captured globally so it
/// reaches the app even when macOS would otherwise route the function-key row
/// to a system service (Dictation, etc.) without holding Fn. Registered only
/// while the window is focused, so it doesn't swallow F5 in other apps.
fn run_shortcut() -> Shortcut {
    Shortcut::new(None, Code::F5)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(
            tauri_plugin_global_shortcut::Builder::new()
                .with_handler(|app, shortcut, event| {
                    if *shortcut == run_shortcut() && event.state() == ShortcutState::Pressed {
                        // Frontend runs it only when the SQL page is mounted.
                        let _ = app.emit("run-sql-shortcut", ());
                    }
                })
                .build(),
        )
        .setup(|app| {
            // The window is focused at launch; register F5 now.
            let _ = app.global_shortcut().register(run_shortcut());
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::Focused(focused) = event {
                let gs = window.global_shortcut();
                if *focused {
                    let _ = gs.register(run_shortcut());
                } else {
                    let _ = gs.unregister(run_shortcut());
                }
            }
        })
        .manage(Mutex::new(DbBackend::Disconnected))
        .manage(commands::oximem::OxiMemState::default())
        .invoke_handler(tauri::generate_handler![
            // Connection
            commands::connection::open_embedded,
            commands::connection::connect_remote,
            commands::connection::disconnect,
            commands::connection::get_connection_status,
            // Databases (ADR-0012)
            commands::database::list_databases,
            commands::database::create_database,
            commands::database::drop_database,
            // Dashboard
            commands::dashboard::get_dashboard_stats,
            // Collections
            commands::collections::list_collections,
            commands::collections::create_collection,
            commands::collections::drop_collection,
            commands::collections::compact_collection,
            // Documents
            commands::documents::find_documents,
            commands::documents::insert_document,
            commands::documents::update_documents,
            commands::documents::delete_documents,
            commands::documents::count_documents,
            // Indexes
            commands::indexes::list_indexes,
            commands::indexes::create_index,
            commands::indexes::create_unique_index,
            commands::indexes::create_composite_index,
            commands::indexes::create_text_index,
            commands::indexes::drop_index,
            // Query
            commands::query::execute_raw_command,
            // SQL engine (ADR-0010)
            commands::sql::run_sql,
            // Cobra stored procedure compilation (ADR-0014)
            commands::cobra::cobra_detect,
            commands::cobra::cobra_compile,
            // Filesystem (import + blob upload/download)
            commands::fs::read_file_text,
            commands::fs::read_file_base64,
            commands::fs::write_file_base64,
            // OxiMem (Redis-compatible KV)
            commands::oximem::oximem_connect,
            commands::oximem::oximem_disconnect,
            commands::oximem::oximem_status,
            commands::oximem::oximem_scan,
            commands::oximem::oximem_get,
            commands::oximem::oximem_set_string,
            commands::oximem::oximem_del,
            commands::oximem::oximem_exec,
            // Aggregation
            commands::aggregation::run_aggregation,
            // Blobs
            commands::blobs::list_buckets,
            commands::blobs::create_bucket,
            commands::blobs::delete_bucket,
            commands::blobs::list_objects,
            commands::blobs::put_object,
            commands::blobs::get_object,
            commands::blobs::delete_object,
            commands::blobs::search_objects,
            // Transactions
            commands::transactions::begin_transaction,
            commands::transactions::commit_transaction,
            commands::transactions::rollback_transaction,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
