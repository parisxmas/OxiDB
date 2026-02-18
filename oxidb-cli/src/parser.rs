use serde_json::{Value, json};

/// Parse a MongoDB-style command string into a JSON command Value.
///
/// Supported forms:
///   db.<collection>.<method>(args...)
///   db.<collection>.<method>(args...).sort({...}).limit(N).skip(N)
///   show collections
///   show buckets
///   db.createCollection("name")
///   db.createBucket("name")
///   db.deleteBucket("name")
///   db.beginTransaction()
///   db.commitTransaction()
///   db.rollbackTransaction()
///   db.search("query", limit)
///   ping
///   help
///   exit
pub fn parse(input: &str) -> Result<Value, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("empty input".into());
    }

    // Simple commands
    match input {
        "ping" => return Ok(json!({"cmd": "ping"})),
        "help" => return Ok(json!({"cmd": "help"})),
        "exit" | "quit" => return Ok(json!({"cmd": "exit"})),
        _ => {}
    }

    // show <thing>
    if let Some(rest) = input.strip_prefix("show ") {
        let what = rest.trim();
        return match what {
            "collections" => Ok(json!({"cmd": "list_collections"})),
            "buckets" => Ok(json!({"cmd": "list_buckets"})),
            _ => Err(format!("unknown: show {what}")),
        };
    }

    // Everything else must start with "db."
    if !input.starts_with("db.") {
        return Err(format!("unknown command: {input}"));
    }

    let after_db = &input[3..]; // skip "db."

    // db-level methods: db.methodName(args...)
    if let Some(cmd) = try_parse_db_method(after_db)? {
        return Ok(cmd);
    }

    // db.<collection>.<method>(args...) possibly with chained modifiers
    parse_collection_method(after_db)
}

/// Try to parse a db-level method like db.createCollection("name").
fn try_parse_db_method(input: &str) -> Result<Option<Value>, String> {
    let (method, rest) = match input.find('(') {
        Some(i) => (&input[..i], &input[i..]),
        None => return Ok(None),
    };

    // Only match db-level methods (no dot in method name)
    if method.contains('.') {
        return Ok(None);
    }

    let args_str = extract_parens(rest)?;

    match method {
        "createCollection" => {
            let name = parse_string_arg(args_str)?;
            Ok(Some(json!({"cmd": "create_collection", "collection": name})))
        }
        "createBucket" => {
            let name = parse_string_arg(args_str)?;
            Ok(Some(json!({"cmd": "create_bucket", "bucket": name})))
        }
        "deleteBucket" => {
            let name = parse_string_arg(args_str)?;
            Ok(Some(json!({"cmd": "delete_bucket", "bucket": name})))
        }
        "beginTransaction" => Ok(Some(json!({"cmd": "begin_tx"}))),
        "commitTransaction" => Ok(Some(json!({"cmd": "commit_tx"}))),
        "rollbackTransaction" => Ok(Some(json!({"cmd": "rollback_tx"}))),
        "search" => {
            let args = split_args(args_str)?;
            if args.is_empty() {
                return Err("search requires a query string".into());
            }
            let query = parse_string_arg(args[0])?;
            let limit = if args.len() > 1 {
                args[1]
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| "invalid limit".to_string())?
            } else {
                10
            };
            Ok(Some(json!({"cmd": "search", "query": query, "limit": limit})))
        }
        _ => Ok(None),
    }
}

/// Parse db.<collection>.<method>(args...) with optional chained .sort/.limit/.skip
fn parse_collection_method(input: &str) -> Result<Value, String> {
    // Find collection name: everything up to the next dot
    let dot_pos = input
        .find('.')
        .ok_or_else(|| format!("expected db.<collection>.<method>(...), got: db.{input}"))?;
    let collection = &input[..dot_pos];
    if collection.is_empty() {
        return Err("empty collection name".into());
    }
    let after_col = &input[dot_pos + 1..];

    // Find method name: everything up to the first '('
    let paren_pos = after_col
        .find('(')
        .ok_or_else(|| format!("expected method call with (), got: db.{input}"))?;
    let method = &after_col[..paren_pos];
    let rest = &after_col[paren_pos..];

    // Extract the arguments inside the first set of balanced parens
    let (args_str, remainder) = extract_parens_with_remainder(rest)?;

    // Build the base command
    let mut cmd = build_method_command(collection, method, args_str)?;

    // Parse chained modifiers: .sort({...}).limit(N).skip(N)
    parse_chain_modifiers(remainder, &mut cmd)?;

    Ok(cmd)
}

/// Build a JSON command from collection, method name, and argument string.
fn build_method_command(
    collection: &str,
    method: &str,
    args_str: &str,
) -> Result<Value, String> {
    match method {
        "insert" => {
            let doc: Value = parse_json_arg(args_str)?;
            Ok(json!({"cmd": "insert", "collection": collection, "doc": doc}))
        }
        "insertMany" => {
            let docs: Value = parse_json_arg(args_str)?;
            Ok(json!({"cmd": "insert_many", "collection": collection, "docs": docs}))
        }
        "find" => {
            let args = split_args(args_str)?;
            let query = if args.is_empty() || args[0].trim().is_empty() {
                json!({})
            } else {
                parse_json_arg(args[0])?
            };
            Ok(json!({"cmd": "find", "collection": collection, "query": query}))
        }
        "findOne" => {
            let args = split_args(args_str)?;
            let query = if args.is_empty() || args[0].trim().is_empty() {
                json!({})
            } else {
                parse_json_arg(args[0])?
            };
            Ok(json!({"cmd": "find_one", "collection": collection, "query": query}))
        }
        "update" => {
            let args = split_args(args_str)?;
            if args.len() < 2 {
                return Err("update requires (query, update)".into());
            }
            let query: Value = parse_json_arg(args[0])?;
            let update: Value = parse_json_arg(args[1])?;
            Ok(json!({"cmd": "update", "collection": collection, "query": query, "update": update}))
        }
        "updateOne" => {
            let args = split_args(args_str)?;
            if args.len() < 2 {
                return Err("updateOne requires (query, update)".into());
            }
            let query: Value = parse_json_arg(args[0])?;
            let update: Value = parse_json_arg(args[1])?;
            Ok(json!({"cmd": "update_one", "collection": collection, "query": query, "update": update}))
        }
        "delete" => {
            let args = split_args(args_str)?;
            let query = if args.is_empty() || args[0].trim().is_empty() {
                json!({})
            } else {
                parse_json_arg(args[0])?
            };
            Ok(json!({"cmd": "delete", "collection": collection, "query": query}))
        }
        "deleteOne" => {
            let args = split_args(args_str)?;
            let query = if args.is_empty() || args[0].trim().is_empty() {
                json!({})
            } else {
                parse_json_arg(args[0])?
            };
            Ok(json!({"cmd": "delete_one", "collection": collection, "query": query}))
        }
        "count" => {
            let args = split_args(args_str)?;
            let query = if args.is_empty() || args[0].trim().is_empty() {
                json!({})
            } else {
                parse_json_arg(args[0])?
            };
            Ok(json!({"cmd": "count", "collection": collection, "query": query}))
        }
        "createIndex" => {
            let field = parse_string_arg(args_str)?;
            Ok(json!({"cmd": "create_index", "collection": collection, "field": field}))
        }
        "createUniqueIndex" => {
            let field = parse_string_arg(args_str)?;
            Ok(json!({"cmd": "create_unique_index", "collection": collection, "field": field}))
        }
        "createCompositeIndex" => {
            let fields: Value = parse_json_arg(args_str)?;
            Ok(json!({"cmd": "create_composite_index", "collection": collection, "fields": fields}))
        }
        "createTextIndex" => {
            let fields: Value = parse_json_arg(args_str)?;
            Ok(json!({"cmd": "create_text_index", "collection": collection, "fields": fields}))
        }
        "textSearch" => {
            let args = split_args(args_str)?;
            if args.is_empty() {
                return Err("textSearch requires a query string".into());
            }
            let query = parse_string_arg(args[0])?;
            let limit = if args.len() > 1 {
                args[1]
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| "invalid limit".to_string())?
            } else {
                10
            };
            Ok(json!({"cmd": "text_search", "collection": collection, "query": query, "limit": limit}))
        }
        "aggregate" => {
            let pipeline: Value = parse_json_arg(args_str)?;
            Ok(json!({"cmd": "aggregate", "collection": collection, "pipeline": pipeline}))
        }
        "compact" => Ok(json!({"cmd": "compact", "collection": collection})),
        "drop" => Ok(json!({"cmd": "drop_collection", "collection": collection})),
        _ => Err(format!("unknown method: {method}")),
    }
}

/// Parse chained modifiers like .sort({"age": -1}).limit(10).skip(5)
fn parse_chain_modifiers(mut input: &str, cmd: &mut Value) -> Result<(), String> {
    let input_ref = &mut input;
    loop {
        let s = input_ref.trim();
        if s.is_empty() {
            break;
        }
        if !s.starts_with('.') {
            return Err(format!("unexpected trailing: {s}"));
        }
        let s = &s[1..]; // skip '.'
        let paren = s
            .find('(')
            .ok_or_else(|| format!("expected modifier(), got: .{s}"))?;
        let modifier = &s[..paren];
        let rest = &s[paren..];
        let (arg, remainder) = extract_parens_with_remainder(rest)?;

        match modifier {
            "sort" => {
                let sort_val: Value = parse_json_arg(arg)?;
                cmd["sort"] = sort_val;
            }
            "limit" => {
                let n: u64 = arg
                    .trim()
                    .parse()
                    .map_err(|_| "invalid limit value".to_string())?;
                cmd["limit"] = json!(n);
            }
            "skip" => {
                let n: u64 = arg
                    .trim()
                    .parse()
                    .map_err(|_| "invalid skip value".to_string())?;
                cmd["skip"] = json!(n);
            }
            _ => return Err(format!("unknown modifier: .{modifier}()")),
        }

        *input_ref = remainder;
    }
    Ok(())
}

// ---- Argument parsing helpers ----

/// Extract content between balanced parentheses. Input must start with '('.
fn extract_parens(input: &str) -> Result<&str, String> {
    let (inner, _) = extract_parens_with_remainder(input)?;
    Ok(inner)
}

/// Extract content between balanced parentheses, returning (inner, remainder).
fn extract_parens_with_remainder(input: &str) -> Result<(&str, &str), String> {
    if !input.starts_with('(') {
        return Err("expected '('".into());
    }
    let mut depth = 0;
    let mut in_string = false;
    let mut escape = false;
    for (i, ch) in input.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok((&input[1..i], &input[i + 1..]));
                }
            }
            _ => {}
        }
    }
    Err("unmatched parenthesis".into())
}

/// Split arguments by top-level commas (respecting braces, brackets, strings).
fn split_args(input: &str) -> Result<Vec<&str>, String> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(vec![]);
    }
    let mut result = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    let mut start = 0;

    for (i, ch) in input.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        match ch {
            '{' | '[' | '(' => depth += 1,
            '}' | ']' | ')' => depth -= 1,
            ',' if depth == 0 => {
                result.push(&input[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&input[start..]);
    Ok(result)
}

/// Parse a JSON value from an argument string.
fn parse_json_arg(input: &str) -> Result<Value, String> {
    let input = input.trim();
    serde_json::from_str(input).map_err(|e| format!("invalid JSON: {e}"))
}

/// Parse a string argument: "value" or 'value' (strip quotes).
fn parse_string_arg(input: &str) -> Result<String, String> {
    let input = input.trim();
    if (input.starts_with('"') && input.ends_with('"'))
        || (input.starts_with('\'') && input.ends_with('\''))
    {
        Ok(input[1..input.len() - 1].to_string())
    } else {
        // Try parsing as JSON string
        serde_json::from_str::<String>(input)
            .map_err(|_| format!("expected a quoted string, got: {input}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping() {
        let cmd = parse("ping").unwrap();
        assert_eq!(cmd["cmd"], "ping");
    }

    #[test]
    fn test_show_collections() {
        let cmd = parse("show collections").unwrap();
        assert_eq!(cmd["cmd"], "list_collections");
    }

    #[test]
    fn test_show_buckets() {
        let cmd = parse("show buckets").unwrap();
        assert_eq!(cmd["cmd"], "list_buckets");
    }

    #[test]
    fn test_insert() {
        let cmd = parse(r#"db.users.insert({"name": "Alice", "age": 30})"#).unwrap();
        assert_eq!(cmd["cmd"], "insert");
        assert_eq!(cmd["collection"], "users");
        assert_eq!(cmd["doc"]["name"], "Alice");
        assert_eq!(cmd["doc"]["age"], 30);
    }

    #[test]
    fn test_insert_many() {
        let cmd = parse(r#"db.users.insertMany([{"name": "Bob"}, {"name": "Charlie"}])"#).unwrap();
        assert_eq!(cmd["cmd"], "insert_many");
        assert_eq!(cmd["collection"], "users");
        assert_eq!(cmd["docs"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_find_empty() {
        let cmd = parse("db.users.find({})").unwrap();
        assert_eq!(cmd["cmd"], "find");
        assert_eq!(cmd["collection"], "users");
    }

    #[test]
    fn test_find_with_query() {
        let cmd = parse(r#"db.users.find({"city": "New York"})"#).unwrap();
        assert_eq!(cmd["cmd"], "find");
        assert_eq!(cmd["query"]["city"], "New York");
    }

    #[test]
    fn test_find_with_chain() {
        let cmd = parse(r#"db.users.find({}).sort({"age": -1}).limit(10).skip(5)"#).unwrap();
        assert_eq!(cmd["cmd"], "find");
        assert_eq!(cmd["sort"]["age"], -1);
        assert_eq!(cmd["limit"], 10);
        assert_eq!(cmd["skip"], 5);
    }

    #[test]
    fn test_find_one() {
        let cmd = parse(r#"db.users.findOne({"name": "Alice"})"#).unwrap();
        assert_eq!(cmd["cmd"], "find_one");
    }

    #[test]
    fn test_update() {
        let cmd =
            parse(r#"db.users.update({"name": "Alice"}, {"$set": {"age": 31}})"#).unwrap();
        assert_eq!(cmd["cmd"], "update");
        assert_eq!(cmd["query"]["name"], "Alice");
        assert_eq!(cmd["update"]["$set"]["age"], 31);
    }

    #[test]
    fn test_update_one() {
        let cmd =
            parse(r#"db.users.updateOne({"name": "Alice"}, {"$set": {"age": 31}})"#).unwrap();
        assert_eq!(cmd["cmd"], "update_one");
    }

    #[test]
    fn test_delete() {
        let cmd = parse(r#"db.users.delete({"name": "Alice"})"#).unwrap();
        assert_eq!(cmd["cmd"], "delete");
    }

    #[test]
    fn test_delete_one() {
        let cmd = parse(r#"db.users.deleteOne({"name": "Alice"})"#).unwrap();
        assert_eq!(cmd["cmd"], "delete_one");
    }

    #[test]
    fn test_count_empty() {
        let cmd = parse("db.users.count()").unwrap();
        assert_eq!(cmd["cmd"], "count");
    }

    #[test]
    fn test_count_with_query() {
        let cmd = parse(r#"db.users.count({"city": "NY"})"#).unwrap();
        assert_eq!(cmd["cmd"], "count");
        assert_eq!(cmd["query"]["city"], "NY");
    }

    #[test]
    fn test_create_index() {
        let cmd = parse(r#"db.users.createIndex("email")"#).unwrap();
        assert_eq!(cmd["cmd"], "create_index");
        assert_eq!(cmd["field"], "email");
    }

    #[test]
    fn test_create_unique_index() {
        let cmd = parse(r#"db.users.createUniqueIndex("email")"#).unwrap();
        assert_eq!(cmd["cmd"], "create_unique_index");
        assert_eq!(cmd["field"], "email");
    }

    #[test]
    fn test_create_composite_index() {
        let cmd = parse(r#"db.users.createCompositeIndex(["city", "age"])"#).unwrap();
        assert_eq!(cmd["cmd"], "create_composite_index");
        assert_eq!(cmd["fields"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_create_text_index() {
        let cmd = parse(r#"db.users.createTextIndex(["title", "body"])"#).unwrap();
        assert_eq!(cmd["cmd"], "create_text_index");
    }

    #[test]
    fn test_text_search() {
        let cmd = parse(r#"db.users.textSearch("rust programming", 10)"#).unwrap();
        assert_eq!(cmd["cmd"], "text_search");
        assert_eq!(cmd["query"], "rust programming");
        assert_eq!(cmd["limit"], 10);
    }

    #[test]
    fn test_aggregate() {
        let cmd = parse(
            r#"db.users.aggregate([{"$group": {"_id": "$city", "n": {"$count": true}}}])"#,
        )
        .unwrap();
        assert_eq!(cmd["cmd"], "aggregate");
        assert_eq!(cmd["collection"], "users");
    }

    #[test]
    fn test_compact() {
        let cmd = parse("db.users.compact()").unwrap();
        assert_eq!(cmd["cmd"], "compact");
    }

    #[test]
    fn test_drop() {
        let cmd = parse("db.users.drop()").unwrap();
        assert_eq!(cmd["cmd"], "drop_collection");
    }

    #[test]
    fn test_create_collection() {
        let cmd = parse(r#"db.createCollection("orders")"#).unwrap();
        assert_eq!(cmd["cmd"], "create_collection");
        assert_eq!(cmd["collection"], "orders");
    }

    #[test]
    fn test_create_bucket() {
        let cmd = parse(r#"db.createBucket("files")"#).unwrap();
        assert_eq!(cmd["cmd"], "create_bucket");
        assert_eq!(cmd["bucket"], "files");
    }

    #[test]
    fn test_delete_bucket() {
        let cmd = parse(r#"db.deleteBucket("files")"#).unwrap();
        assert_eq!(cmd["cmd"], "delete_bucket");
    }

    #[test]
    fn test_begin_transaction() {
        let cmd = parse("db.beginTransaction()").unwrap();
        assert_eq!(cmd["cmd"], "begin_tx");
    }

    #[test]
    fn test_commit_transaction() {
        let cmd = parse("db.commitTransaction()").unwrap();
        assert_eq!(cmd["cmd"], "commit_tx");
    }

    #[test]
    fn test_rollback_transaction() {
        let cmd = parse("db.rollbackTransaction()").unwrap();
        assert_eq!(cmd["cmd"], "rollback_tx");
    }

    #[test]
    fn test_db_search() {
        let cmd = parse(r#"db.search("hello world", 10)"#).unwrap();
        assert_eq!(cmd["cmd"], "search");
        assert_eq!(cmd["query"], "hello world");
        assert_eq!(cmd["limit"], 10);
    }

    #[test]
    fn test_help_and_exit() {
        assert_eq!(parse("help").unwrap()["cmd"], "help");
        assert_eq!(parse("exit").unwrap()["cmd"], "exit");
        assert_eq!(parse("quit").unwrap()["cmd"], "exit");
    }

    #[test]
    fn test_empty_input() {
        assert!(parse("").is_err());
    }

    #[test]
    fn test_unknown_command() {
        assert!(parse("foobar").is_err());
    }

    #[test]
    fn test_find_no_args() {
        let cmd = parse("db.users.find()").unwrap();
        assert_eq!(cmd["cmd"], "find");
        assert_eq!(cmd["query"], json!({}));
    }
}
