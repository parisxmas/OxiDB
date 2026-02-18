mod commands;
mod state;

use std::sync::Mutex;

use state::DbBackend;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .manage(Mutex::new(DbBackend::Disconnected))
        .invoke_handler(tauri::generate_handler![
            // Connection
            commands::connection::open_embedded,
            commands::connection::connect_remote,
            commands::connection::disconnect,
            commands::connection::get_connection_status,
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
