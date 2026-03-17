use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

// ─── Shard Configuration ────────────────────────────────────────────

/// Number of virtual chunks (must be power of 2 for fast modulo).
const DEFAULT_NUM_CHUNKS: u32 = 256;

/// Per-collection shard key configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardKeyConfig {
    pub field: String,
}

/// Full shard cluster configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardConfig {
    /// Number of virtual chunks.
    pub num_chunks: u32,
    /// chunk_map[chunk_id] = shard_id.
    pub chunk_map: Vec<u32>,
    /// Shard endpoint addresses (shard_id → "host:port").
    pub shards: Vec<String>,
    /// Per-collection shard key (collection_name → config).
    /// Collections not listed here are unsharded (routed to shard 0).
    pub collection_keys: HashMap<String, ShardKeyConfig>,
}

impl ShardConfig {
    /// Load shard config from `OXIPOOL_SHARD_CONFIG` JSON file,
    /// or build one from `OXIPOOL_SHARDS` env var (comma-separated addresses).
    /// Returns `None` if sharding is not configured.
    pub fn from_env() -> Option<Self> {
        // Try config file first
        if let Ok(path) = env::var("OXIPOOL_SHARD_CONFIG") {
            return Self::load_from_file(&path);
        }

        // Try simple env-based config
        let shards_str = env::var("OXIPOOL_SHARDS").ok()?;
        if shards_str.is_empty() {
            return None;
        }

        let shards: Vec<String> = shards_str.split(',').map(|s| s.trim().to_string()).collect();
        if shards.len() < 2 {
            return None; // Need at least 2 shards
        }

        let num_chunks = env::var("OXIPOOL_NUM_CHUNKS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_NUM_CHUNKS);

        // Evenly distribute chunks across shards
        let num_shards = shards.len() as u32;
        let chunk_map: Vec<u32> = (0..num_chunks).map(|c| c % num_shards).collect();

        // Parse shard keys from env: OXIPOOL_SHARD_KEYS="orders:customer_id,users:region"
        let mut collection_keys = HashMap::new();
        if let Ok(keys_str) = env::var("OXIPOOL_SHARD_KEYS") {
            for entry in keys_str.split(',') {
                let parts: Vec<&str> = entry.trim().split(':').collect();
                if parts.len() == 2 {
                    collection_keys.insert(
                        parts[0].to_string(),
                        ShardKeyConfig {
                            field: parts[1].to_string(),
                        },
                    );
                }
            }
        }

        Some(Self {
            num_chunks,
            chunk_map,
            shards,
            collection_keys,
        })
    }

    fn load_from_file(path: &str) -> Option<Self> {
        let data = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&data).ok()
    }

    /// Save current config to a JSON file.
    #[allow(dead_code)]
    pub fn save_to_file(&self, path: &str) -> Result<(), String> {
        let data = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        std::fs::write(path, data).map_err(|e| e.to_string())
    }

    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }
}

// ─── Shard Router ───────────────────────────────────────────────────

/// Determines which shard a request should go to.
#[allow(dead_code)]
pub struct ShardRouter {
    pub config: RwLock<ShardConfig>,
}

impl ShardRouter {
    pub fn new(config: ShardConfig) -> Arc<Self> {
        Arc::new(Self {
            config: RwLock::new(config),
        })
    }

    /// Route by extracting the shard key from a JSON payload.
    /// Returns `Some(shard_id)` for targeted routing, `None` for scatter-gather.
    pub async fn route(&self, collection: &str, payload: &serde_json::Value) -> Option<u32> {
        let config = self.config.read().await;

        let key_config = config.collection_keys.get(collection)?;
        let shard_key_value = resolve_field(payload, &key_config.field)?;

        let hash = crc32fast::hash(shard_key_value.as_bytes());
        let chunk_id = hash % config.num_chunks;

        Some(config.chunk_map[chunk_id as usize])
    }

    /// Route a document for insertion — always has the shard key in the doc.
    /// Falls back to shard 0 for unsharded collections.
    pub async fn route_insert(&self, collection: &str, doc: &serde_json::Value) -> u32 {
        match self.route(collection, doc).await {
            Some(shard) => shard,
            None => 0, // Unsharded → shard 0
        }
    }

    /// Check if a collection is sharded.
    pub async fn is_sharded(&self, collection: &str) -> bool {
        let config = self.config.read().await;
        config.collection_keys.contains_key(collection)
    }

    /// Get the number of shards.
    #[allow(dead_code)]
    pub async fn num_shards(&self) -> usize {
        let config = self.config.read().await;
        config.shards.len()
    }

    /// Dynamically register a shard key for a collection.
    #[allow(dead_code)]
    pub async fn set_shard_key(&self, collection: &str, field: &str) {
        let mut config = self.config.write().await;
        config.collection_keys.insert(
            collection.to_string(),
            ShardKeyConfig {
                field: field.to_string(),
            },
        );
    }
}

/// Extract a field value from a JSON object and convert to a string for hashing.
fn resolve_field(value: &serde_json::Value, path: &str) -> Option<String> {
    let mut current = value;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    Some(match current {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        other => other.to_string(),
    })
}

// ─── Command Parsing ────────────────────────────────────────────────

/// Parsed shard-relevant fields from a request payload.
#[allow(dead_code)]
pub struct ParsedCommand {
    pub cmd: String,
    pub collection: Option<String>,
    pub routing: CommandRouting,
}

pub enum CommandRouting {
    /// Route to a specific shard (shard key found in query/doc).
    Targeted(u32),
    /// Fan out to all shards and merge results.
    ScatterGather,
    /// Send to all shards (DDL, index operations).
    Broadcast,
    /// Route to shard 0 (unsharded collection or admin commands).
    Primary,
    /// Transaction commands — handled separately.
    Transaction,
}

/// Commands that should be broadcast to all shards.
const BROADCAST_COMMANDS: &[&str] = &[
    "create_collection",
    "drop_collection",
    "create_index",
    "create_unique_index",
    "create_composite_index",
    "create_text_index",
    "create_vector_index",
    "drop_index",
    "compact",
    "create_bucket",
    "delete_bucket",
    "create_database",
    "drop_database",
    "create_user",
    "drop_user",
    "update_user",
    "grant_db_role",
    "revoke_db_role",
];

/// Commands that always go to shard 0 (admin/meta).
const PRIMARY_ONLY_COMMANDS: &[&str] = &[
    "list_collections",
    "list_indexes",
    "list_databases",
    "list_users",
    "list_buckets",
    "list_objects",
    "list_schedules",
    "get_schedule",
    "create_schedule",
    "delete_schedule",
    "enable_schedule",
    "disable_schedule",
    "ping",
    "auth_simple",
];

/// Parse a command payload and determine routing.
pub async fn parse_and_route(
    router: &ShardRouter,
    payload: &[u8],
) -> Result<ParsedCommand, String> {
    let json: serde_json::Value =
        serde_json::from_slice(payload).map_err(|e| format!("invalid JSON: {}", e))?;

    let cmd = json
        .get("cmd")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let collection = json
        .get("collection")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Transaction commands
    if cmd == "begin_tx" || cmd == "commit_tx" || cmd == "rollback_tx" {
        return Ok(ParsedCommand {
            cmd,
            collection,
            routing: CommandRouting::Transaction,
        });
    }

    // Broadcast commands (DDL)
    if BROADCAST_COMMANDS.contains(&cmd.as_str()) {
        return Ok(ParsedCommand {
            cmd,
            collection,
            routing: CommandRouting::Broadcast,
        });
    }

    // Primary-only commands
    if PRIMARY_ONLY_COMMANDS.contains(&cmd.as_str()) {
        return Ok(ParsedCommand {
            cmd,
            collection,
            routing: CommandRouting::Primary,
        });
    }

    // For data commands, check if collection is sharded
    let coll = match &collection {
        Some(c) => c.as_str(),
        None => {
            return Ok(ParsedCommand {
                cmd,
                collection,
                routing: CommandRouting::Primary,
            });
        }
    };

    if !router.is_sharded(coll).await {
        return Ok(ParsedCommand {
            cmd,
            collection,
            routing: CommandRouting::Primary,
        });
    }

    // Sharded collection — determine targeted vs scatter-gather
    let routing = match cmd.as_str() {
        "insert" => {
            if let Some(doc) = json.get("doc") {
                let shard = router.route_insert(coll, doc).await;
                CommandRouting::Targeted(shard)
            } else {
                CommandRouting::Primary
            }
        }

        "insert_many" => {
            // insert_many needs special handling (split by shard) — use ScatterGather
            CommandRouting::ScatterGather
        }

        "find" | "find_one" | "count" | "update" | "update_one" | "delete" | "delete_one" => {
            if let Some(query) = json.get("query") {
                match router.route(coll, query).await {
                    Some(shard) => CommandRouting::Targeted(shard),
                    None => CommandRouting::ScatterGather,
                }
            } else {
                CommandRouting::ScatterGather
            }
        }

        "aggregate" => {
            // Check if the first $match stage has the shard key
            if let Some(pipeline) = json.get("pipeline").and_then(|v| v.as_array()) {
                if let Some(first) = pipeline.first() {
                    if let Some(match_stage) = first.get("$match") {
                        if let Some(shard) = router.route(coll, match_stage).await {
                            CommandRouting::Targeted(shard)
                        } else {
                            CommandRouting::ScatterGather
                        }
                    } else {
                        CommandRouting::ScatterGather
                    }
                } else {
                    CommandRouting::ScatterGather
                }
            } else {
                CommandRouting::ScatterGather
            }
        }

        "text_search" | "vector_search" | "search" => CommandRouting::ScatterGather,

        // Blob operations — always shard 0
        "put_object" | "get_object" | "delete_object" | "head_object" => {
            CommandRouting::Primary
        }

        _ => CommandRouting::Primary,
    };

    Ok(ParsedCommand {
        cmd,
        collection,
        routing,
    })
}
