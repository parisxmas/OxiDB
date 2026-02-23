use colored::Colorize;
use serde_json::Value;

/// Format a response Value for display to the user.
/// If `raw_json` is true, output unformatted JSON suitable for piping.
pub fn format_response(response: &Value, raw_json: bool) -> String {
    if raw_json {
        return response.to_string();
    }

    let ok = response.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);

    if !ok {
        let error = response
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");
        return format!("{} {}", "Error:".red().bold(), error.red());
    }

    let data = match response.get("data") {
        Some(d) => d,
        None => return "ok".green().to_string(),
    };

    format_value(data)
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Array(arr) if arr.is_empty() => "(empty)".dimmed().to_string(),
        Value::Array(arr) => {
            let mut lines = Vec::with_capacity(arr.len());
            for item in arr {
                lines.push(pretty_json(item));
            }
            lines.join("\n")
        }
        Value::Null => "null".dimmed().to_string(),
        Value::String(s) => s.green().to_string(),
        Value::Number(n) => n.to_string().cyan().to_string(),
        Value::Bool(b) => {
            if *b {
                "true".green().to_string()
            } else {
                "false".yellow().to_string()
            }
        }
        Value::Object(_) => pretty_json(value),
    }
}

fn pretty_json(value: &Value) -> String {
    let formatted = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    colorize_json(&formatted)
}

fn colorize_json(json_str: &str) -> String {
    let mut result = String::with_capacity(json_str.len() * 2);
    let mut in_key = false;
    let mut in_string = false;
    let mut is_key = true; // next string is a key
    let mut escape = false;
    let mut token = String::new();

    for ch in json_str.chars() {
        if escape {
            token.push(ch);
            escape = false;
            continue;
        }

        if ch == '\\' && (in_key || in_string) {
            token.push(ch);
            escape = true;
            continue;
        }

        if ch == '"' {
            if in_key {
                // End of key
                token.push('"');
                result.push_str(&token.blue().bold().to_string());
                token.clear();
                in_key = false;
                is_key = false;
                continue;
            } else if in_string {
                // End of value string
                token.push('"');
                result.push_str(&token.green().to_string());
                token.clear();
                in_string = false;
                continue;
            } else if is_key {
                // Start of key
                in_key = true;
                token.push('"');
                continue;
            } else {
                // Start of value string
                in_string = true;
                token.push('"');
                continue;
            }
        }

        if in_key || in_string {
            token.push(ch);
            continue;
        }

        // Outside strings
        match ch {
            ':' => {
                result.push_str(&":".dimmed().to_string());
                is_key = false;
            }
            ',' => {
                result.push(',');
                is_key = true;
            }
            '{' => {
                result.push('{');
                is_key = true;
            }
            '}' | ']' => {
                if !token.is_empty() {
                    result.push_str(&colorize_primitive(&token));
                    token.clear();
                }
                result.push(ch);
                is_key = false;
            }
            '[' => {
                result.push('[');
                is_key = false;
            }
            _ if ch.is_whitespace() => {
                if !token.is_empty() {
                    result.push_str(&colorize_primitive(&token));
                    token.clear();
                }
                result.push(ch);
            }
            _ => {
                token.push(ch);
            }
        }
    }

    if !token.is_empty() {
        result.push_str(&colorize_primitive(&token));
    }

    result
}

fn colorize_primitive(s: &str) -> String {
    let trimmed = s.trim();
    match trimmed {
        "true" | "false" => trimmed.yellow().to_string(),
        "null" => trimmed.dimmed().to_string(),
        _ if trimmed.parse::<f64>().is_ok() => trimmed.cyan().to_string(),
        _ => s.to_string(),
    }
}

/// Print the startup banner with 3D ASCII logo.
pub fn print_banner(version: &str) {
    let logo = r#"
   ____          _  ____    ____
  / __ \  __  __(_)|  _ \  | __ )
 | |  | | \ \/ /| || | | | |  _ \
 | |__| |  >  < | || |_| | | |_) |
  \____/  /_/\_\|_||____/  |____/
"#;

    let colored_logo = logo
        .lines()
        .map(|line| format!("{}", line.cyan().bold()))
        .collect::<Vec<_>>()
        .join("\n");

    println!("{}", colored_logo);
    println!(
        "  {} {}    {}",
        "v".dimmed(),
        version.white().bold(),
        "Type \"help\" for usage, \"exit\" to quit.".dimmed()
    );
    println!();
}

/// Print the help message.
pub fn print_help() {
    let help = r#"
OxiDB CLI - Interactive shell for OxiDB

Collection commands:
  db.<col>.insert({...})                Insert a document
  db.<col>.insertMany([{...}, ...])     Insert multiple documents
  db.<col>.find({})                     Find documents
  db.<col>.find({}).sort({}).limit(N)   Find with sort/limit/skip
  db.<col>.findOne({...})               Find one document
  db.<col>.update({q}, {u})             Update documents
  db.<col>.updateOne({q}, {u})          Update one document
  db.<col>.delete({...})                Delete documents
  db.<col>.deleteOne({...})             Delete one document
  db.<col>.count()                      Count documents
  db.<col>.count({...})                 Count matching documents
  db.<col>.aggregate([...])             Aggregation pipeline
  db.<col>.createIndex("field")         Create index
  db.<col>.createUniqueIndex("field")   Create unique index
  db.<col>.createCompositeIndex([...])  Create composite index
  db.<col>.createTextIndex([...])       Create text index
  db.<col>.textSearch("query", N)       Full-text search
  db.<col>.compact()                    Compact collection
  db.<col>.drop()                       Drop collection

Database commands:
  show collections                      List all collections
  show buckets                          List all buckets
  db.createCollection("name")           Create a collection
  db.createBucket("name")               Create a bucket
  db.deleteBucket("name")               Delete a bucket
  db.search("query", limit)             Full-text search across blobs

Transaction commands:
  db.beginTransaction()                 Begin a transaction
  db.commitTransaction()                Commit active transaction
  db.rollbackTransaction()              Rollback active transaction

Other:
  ping                                  Check connectivity
  help                                  Show this help
  exit / quit                           Exit the shell
"#;
    println!("{}", help.trim());
}
