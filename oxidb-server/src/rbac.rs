use crate::auth::Role;

/// Check if a role is permitted to execute a given command.
///
/// - **Admin**: all commands
/// - **ReadWrite**: CRUD, indexes, transactions, blobs, search, compact, list_collections
/// - **Read**: find, find_one, count, aggregate, list_*, get_object, head_object, search, ping
pub fn is_permitted(role: Role, cmd: &str) -> bool {
    match role {
        Role::Admin => true,
        Role::ReadWrite => matches!(
            cmd,
            "ping"
                | "insert"
                | "insert_many"
                | "find"
                | "find_one"
                | "update"
                | "find_and_modify"
                | "delete"
                | "count"
                | "create_index"
                | "create_unique_index"
                | "create_composite_index"
                | "create_collection"
                | "create_collection_with_options"
                | "list_collections"
                | "compact"
                | "aggregate"
                | "aggregate_docs"
                | "begin_tx"
                | "commit_tx"
                | "rollback_tx"
                | "create_bucket"
                | "put_object"
                | "get_object"
                | "head_object"
                | "extract_text"
                | "delete_object"
                | "list_objects"
                | "list_buckets"
                | "search"
                | "fts_status"
                | "proc_status"
                | "sql"
                | "call_procedure"
                | "enable_schedule"
                | "disable_schedule"
                | "create_vector_index"
                | "vector_search"
                | "use_db"
                | "list_databases"
        ),
        Role::Read => matches!(
            cmd,
            "ping"
                | "find"
                | "find_one"
                | "count"
                | "aggregate"
                | "aggregate_docs"
                // SELECT-only: the session layer flags Read-role sql requests
                // and the SQL bridge rejects any non-SELECT statement.
                | "sql"
                | "list_collections"
                | "list_buckets"
                | "list_objects"
                | "get_object"
                | "head_object"
                | "extract_text"
                | "search"
                | "fts_status"
                | "proc_status"
                | "list_procedures"
                | "get_procedure"
                | "list_schedules"
                | "get_schedule"
                | "vector_search"
                | "use_db"
                | "list_databases"
        ),
    }
}
