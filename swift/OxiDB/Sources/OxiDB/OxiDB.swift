import Foundation
import COxiDB
import COxiDBEmbedded

// MARK: - Error Types

public enum OxiDBError: Error, LocalizedError {
    case connectionFailed
    case databaseOpenFailed
    case operationFailed(String)
    case transactionConflict(String)

    public var errorDescription: String? {
        switch self {
        case .connectionFailed:
            return "Failed to connect to OxiDB server"
        case .databaseOpenFailed:
            return "Failed to open OxiDB database"
        case .operationFailed(let msg):
            return "OxiDB operation failed: \(msg)"
        case .transactionConflict(let msg):
            return "Transaction conflict: \(msg)"
        }
    }
}

// MARK: - Shared Helpers

/// Parse a JSON response string and check for errors.
private func parseResponse(_ str: String) throws -> [String: Any] {
    guard let data = str.data(using: .utf8),
          let parsed = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
        throw OxiDBError.operationFailed("Invalid JSON response")
    }

    if let ok = parsed["ok"] as? Bool, !ok {
        let msg = parsed["error"] as? String ?? "Unknown error"
        if msg.lowercased().contains("conflict") || msg.lowercased().contains("transaction") {
            throw OxiDBError.transactionConflict(msg)
        }
        throw OxiDBError.operationFailed(msg)
    }

    return parsed
}

/// Serialize a value to a JSON string.
private func jsonString(_ value: Any) throws -> String {
    let data = try JSONSerialization.data(withJSONObject: value)
    guard let str = String(data: data, encoding: .utf8) else {
        throw OxiDBError.operationFailed("Failed to serialize JSON")
    }
    return str
}

// MARK: - Client

public final class OxiDBClient {
    private var conn: OpaquePointer?

    private init(conn: OpaquePointer) {
        self.conn = conn
    }

    deinit {
        disconnect()
    }

    /// Connect to an OxiDB server.
    public static func connect(host: String = "127.0.0.1", port: UInt16 = 4444) throws -> OxiDBClient {
        guard let raw = oxidb_connect(host, port) else {
            throw OxiDBError.connectionFailed
        }
        return OxiDBClient(conn: OpaquePointer(raw))
    }

    /// Disconnect from the server. Safe to call multiple times.
    public func disconnect() {
        if let c = conn {
            oxidb_disconnect(UnsafeMutableRawPointer(c))
            conn = nil
        }
    }

    // MARK: - Core Operations

    /// Ping the server.
    @discardableResult
    public func ping() throws -> [String: Any] {
        return try call { oxidb_ping($0) }
    }

    /// Insert a single document.
    @discardableResult
    public func insert(collection: String, document: [String: Any]) throws -> [String: Any] {
        let json = try jsonString(document)
        return try call { oxidb_insert($0, collection, json) }
    }

    /// Insert multiple documents.
    @discardableResult
    public func insertMany(collection: String, documents: [[String: Any]]) throws -> [String: Any] {
        let json = try jsonString(documents)
        return try call { oxidb_insert_many($0, collection, json) }
    }

    /// Find documents matching a query.
    public func find(collection: String, query: [String: Any] = [:]) throws -> [[String: Any]] {
        let json = try jsonString(query)
        let result = try call { oxidb_find($0, collection, json) }
        guard let data = result["data"] as? [[String: Any]] else {
            return []
        }
        return data
    }

    /// Find a single document matching a query.
    public func findOne(collection: String, query: [String: Any]) throws -> [String: Any]? {
        let json = try jsonString(query)
        let result = try call { oxidb_find_one($0, collection, json) }
        return result["data"] as? [String: Any]
    }

    /// Update documents matching a query.
    @discardableResult
    public func update(collection: String, query: [String: Any], update: [String: Any]) throws -> [String: Any] {
        let queryJson = try jsonString(query)
        let updateJson = try jsonString(update)
        return try call { oxidb_update($0, collection, queryJson, updateJson) }
    }

    /// Delete documents matching a query.
    @discardableResult
    public func delete(collection: String, query: [String: Any]) throws -> [String: Any] {
        let json = try jsonString(query)
        return try call { oxidb_delete($0, collection, json) }
    }

    /// Count documents in a collection.
    public func count(collection: String) throws -> [String: Any] {
        return try call { oxidb_count($0, collection) }
    }

    // MARK: - Indexes

    /// Create a single-field index.
    @discardableResult
    public func createIndex(collection: String, field: String) throws -> [String: Any] {
        return try call { oxidb_create_index($0, collection, field) }
    }

    /// Create a composite (multi-field) index.
    @discardableResult
    public func createCompositeIndex(collection: String, fields: [String]) throws -> [String: Any] {
        let json = try jsonString(fields)
        return try call { oxidb_create_composite_index($0, collection, json) }
    }

    // MARK: - Collections

    /// List all collections.
    public func listCollections() throws -> [String: Any] {
        return try call { oxidb_list_collections($0) }
    }

    /// Drop a collection.
    @discardableResult
    public func dropCollection(_ name: String) throws -> [String: Any] {
        return try call { oxidb_drop_collection($0, name) }
    }

    // MARK: - Aggregation

    /// Run an aggregation pipeline.
    public func aggregate(collection: String, pipeline: [[String: Any]]) throws -> [String: Any] {
        let json = try jsonString(pipeline)
        return try call { oxidb_aggregate($0, collection, json) }
    }

    // MARK: - Transactions

    /// Begin a transaction.
    @discardableResult
    public func beginTransaction() throws -> [String: Any] {
        return try call { oxidb_begin_tx($0) }
    }

    /// Commit the current transaction.
    @discardableResult
    public func commitTransaction() throws -> [String: Any] {
        return try call { oxidb_commit_tx($0) }
    }

    /// Rollback the current transaction.
    @discardableResult
    public func rollbackTransaction() throws -> [String: Any] {
        return try call { oxidb_rollback_tx($0) }
    }

    /// Execute a block within a transaction. Auto-rolls back on error.
    public func transaction(_ body: () throws -> Void) throws {
        try beginTransaction()
        do {
            try body()
            try commitTransaction()
        } catch {
            try? rollbackTransaction()
            throw error
        }
    }

    // MARK: - Blob Storage

    /// Create a blob storage bucket.
    @discardableResult
    public func createBucket(_ name: String) throws -> [String: Any] {
        return try call { oxidb_create_bucket($0, name) }
    }

    /// List all buckets.
    public func listBuckets() throws -> [String: Any] {
        return try call { oxidb_list_buckets($0) }
    }

    /// Delete a bucket.
    @discardableResult
    public func deleteBucket(_ name: String) throws -> [String: Any] {
        return try call { oxidb_delete_bucket($0, name) }
    }

    /// Upload an object (data as base64-encoded string).
    @discardableResult
    public func putObject(
        bucket: String,
        key: String,
        dataBase64: String,
        contentType: String? = nil,
        metadata: [String: Any]? = nil
    ) throws -> [String: Any] {
        let metaJson: String? = try metadata.map { try jsonString($0) }
        return try call { oxidb_put_object($0, bucket, key, dataBase64, contentType, metaJson) }
    }

    /// Download an object.
    public func getObject(bucket: String, key: String) throws -> [String: Any] {
        return try call { oxidb_get_object($0, bucket, key) }
    }

    /// Get object metadata without downloading.
    public func headObject(bucket: String, key: String) throws -> [String: Any] {
        return try call { oxidb_head_object($0, bucket, key) }
    }

    /// Delete an object.
    @discardableResult
    public func deleteObject(bucket: String, key: String) throws -> [String: Any] {
        return try call { oxidb_delete_object($0, bucket, key) }
    }

    /// List objects in a bucket.
    public func listObjects(bucket: String, prefix: String? = nil, limit: Int32 = 0) throws -> [String: Any] {
        return try call { oxidb_list_objects($0, bucket, prefix, limit) }
    }

    // MARK: - Full-Text Search

    /// Search indexed content.
    public func search(query: String, bucket: String? = nil, limit: Int32 = 0) throws -> [String: Any] {
        return try call { oxidb_search($0, query, bucket, limit) }
    }

    // MARK: - Private Helpers

    private func call(_ fn: (UnsafeMutableRawPointer) -> UnsafeMutablePointer<CChar>?) throws -> [String: Any] {
        guard let c = conn else {
            throw OxiDBError.connectionFailed
        }

        let raw = UnsafeMutableRawPointer(c)
        guard let cStr = fn(raw) else {
            throw OxiDBError.operationFailed("Native call returned null")
        }

        defer { oxidb_free_string(cStr) }

        let str = String(cString: cStr)
        return try parseResponse(str)
    }
}

// MARK: - Embedded Database

/// An embedded (in-process) OxiDB database. No server required.
///
/// Uses the same JSON command protocol as the TCP server, executed directly
/// in-process via the `oxidb-embedded-ffi` library.
public final class OxiDBDatabase {
    private var handle: OpaquePointer?

    private init(handle: OpaquePointer) {
        self.handle = handle
    }

    deinit {
        close()
    }

    /// Open a database at the given directory path.
    public static func open(path: String) throws -> OxiDBDatabase {
        guard let raw = oxidb_open(path) else {
            throw OxiDBError.databaseOpenFailed
        }
        return OxiDBDatabase(handle: OpaquePointer(raw))
    }

    /// Open a database with AES-GCM encryption.
    /// `encryptionKeyPath` points to a file containing a 32-byte key.
    public static func open(path: String, encryptionKeyPath: String) throws -> OxiDBDatabase {
        guard let raw = oxidb_open_encrypted(path, encryptionKeyPath) else {
            throw OxiDBError.databaseOpenFailed
        }
        return OxiDBDatabase(handle: OpaquePointer(raw))
    }

    /// Close the database. Safe to call multiple times.
    public func close() {
        if let h = handle {
            oxidb_close(UnsafeMutableRawPointer(h))
            handle = nil
        }
    }

    // MARK: - Core Operations

    /// Ping (in-process, always succeeds if open).
    @discardableResult
    public func ping() throws -> [String: Any] {
        return try execute(["cmd": "ping"])
    }

    /// Insert a single document.
    @discardableResult
    public func insert(collection: String, document: [String: Any]) throws -> [String: Any] {
        return try execute(["cmd": "insert", "collection": collection, "doc": document])
    }

    /// Insert multiple documents.
    @discardableResult
    public func insertMany(collection: String, documents: [[String: Any]]) throws -> [String: Any] {
        return try execute(["cmd": "insert_many", "collection": collection, "docs": documents])
    }

    /// Find documents matching a query.
    public func find(collection: String, query: [String: Any] = [:]) throws -> [[String: Any]] {
        let result = try execute(["cmd": "find", "collection": collection, "query": query])
        guard let data = result["data"] as? [[String: Any]] else {
            return []
        }
        return data
    }

    /// Find a single document matching a query.
    public func findOne(collection: String, query: [String: Any]) throws -> [String: Any]? {
        let result = try execute(["cmd": "find_one", "collection": collection, "query": query])
        return result["data"] as? [String: Any]
    }

    /// Update documents matching a query.
    @discardableResult
    public func update(collection: String, query: [String: Any], update: [String: Any]) throws -> [String: Any] {
        return try execute([
            "cmd": "update", "collection": collection,
            "query": query, "update": update
        ])
    }

    /// Update a single document matching a query.
    @discardableResult
    public func updateOne(collection: String, query: [String: Any], update: [String: Any]) throws -> [String: Any] {
        return try execute([
            "cmd": "update_one", "collection": collection,
            "query": query, "update": update
        ])
    }

    /// Delete documents matching a query.
    @discardableResult
    public func delete(collection: String, query: [String: Any]) throws -> [String: Any] {
        return try execute(["cmd": "delete", "collection": collection, "query": query])
    }

    /// Delete a single document matching a query.
    @discardableResult
    public func deleteOne(collection: String, query: [String: Any]) throws -> [String: Any] {
        return try execute(["cmd": "delete_one", "collection": collection, "query": query])
    }

    /// Count documents in a collection.
    public func count(collection: String, query: [String: Any] = [:]) throws -> [String: Any] {
        return try execute(["cmd": "count", "collection": collection, "query": query])
    }

    // MARK: - Indexes

    /// Create a single-field index.
    @discardableResult
    public func createIndex(collection: String, field: String) throws -> [String: Any] {
        return try execute(["cmd": "create_index", "collection": collection, "field": field])
    }

    /// Create a unique index.
    @discardableResult
    public func createUniqueIndex(collection: String, field: String) throws -> [String: Any] {
        return try execute(["cmd": "create_unique_index", "collection": collection, "field": field])
    }

    /// Create a composite (multi-field) index.
    @discardableResult
    public func createCompositeIndex(collection: String, fields: [String]) throws -> [String: Any] {
        return try execute(["cmd": "create_composite_index", "collection": collection, "fields": fields])
    }

    // MARK: - Collections

    /// Create a collection explicitly.
    @discardableResult
    public func createCollection(_ name: String) throws -> [String: Any] {
        return try execute(["cmd": "create_collection", "collection": name])
    }

    /// List all collections.
    public func listCollections() throws -> [String: Any] {
        return try execute(["cmd": "list_collections"])
    }

    /// Drop a collection.
    @discardableResult
    public func dropCollection(_ name: String) throws -> [String: Any] {
        return try execute(["cmd": "drop_collection", "collection": name])
    }

    /// Compact a collection (reclaim space from deleted documents).
    @discardableResult
    public func compact(collection: String) throws -> [String: Any] {
        return try execute(["cmd": "compact", "collection": collection])
    }

    // MARK: - Aggregation

    /// Run an aggregation pipeline.
    public func aggregate(collection: String, pipeline: [[String: Any]]) throws -> [String: Any] {
        return try execute(["cmd": "aggregate", "collection": collection, "pipeline": pipeline])
    }

    // MARK: - Transactions

    /// Begin a transaction.
    @discardableResult
    public func beginTransaction() throws -> [String: Any] {
        return try execute(["cmd": "begin_tx"])
    }

    /// Commit the current transaction.
    @discardableResult
    public func commitTransaction() throws -> [String: Any] {
        return try execute(["cmd": "commit_tx"])
    }

    /// Rollback the current transaction.
    @discardableResult
    public func rollbackTransaction() throws -> [String: Any] {
        return try execute(["cmd": "rollback_tx"])
    }

    /// Execute a block within a transaction. Auto-rolls back on error.
    public func transaction(_ body: () throws -> Void) throws {
        try beginTransaction()
        do {
            try body()
            try commitTransaction()
        } catch {
            try? rollbackTransaction()
            throw error
        }
    }

    // MARK: - Blob Storage

    /// Create a blob storage bucket.
    @discardableResult
    public func createBucket(_ name: String) throws -> [String: Any] {
        return try execute(["cmd": "create_bucket", "bucket": name])
    }

    /// List all buckets.
    public func listBuckets() throws -> [String: Any] {
        return try execute(["cmd": "list_buckets"])
    }

    /// Delete a bucket.
    @discardableResult
    public func deleteBucket(_ name: String) throws -> [String: Any] {
        return try execute(["cmd": "delete_bucket", "bucket": name])
    }

    /// Upload an object (data as base64-encoded string).
    @discardableResult
    public func putObject(
        bucket: String,
        key: String,
        dataBase64: String,
        contentType: String? = nil,
        metadata: [String: String]? = nil
    ) throws -> [String: Any] {
        var cmd: [String: Any] = [
            "cmd": "put_object",
            "bucket": bucket,
            "key": key,
            "data": dataBase64
        ]
        if let ct = contentType { cmd["content_type"] = ct }
        if let meta = metadata { cmd["metadata"] = meta }
        return try execute(cmd)
    }

    /// Download an object.
    public func getObject(bucket: String, key: String) throws -> [String: Any] {
        return try execute(["cmd": "get_object", "bucket": bucket, "key": key])
    }

    /// Get object metadata without downloading.
    public func headObject(bucket: String, key: String) throws -> [String: Any] {
        return try execute(["cmd": "head_object", "bucket": bucket, "key": key])
    }

    /// Delete an object.
    @discardableResult
    public func deleteObject(bucket: String, key: String) throws -> [String: Any] {
        return try execute(["cmd": "delete_object", "bucket": bucket, "key": key])
    }

    /// List objects in a bucket.
    public func listObjects(bucket: String, prefix: String? = nil, limit: Int? = nil) throws -> [String: Any] {
        var cmd: [String: Any] = ["cmd": "list_objects", "bucket": bucket]
        if let p = prefix { cmd["prefix"] = p }
        if let l = limit { cmd["limit"] = l }
        return try execute(cmd)
    }

    // MARK: - Full-Text Search

    /// Search indexed content.
    public func search(query: String, bucket: String? = nil, limit: Int? = nil) throws -> [String: Any] {
        var cmd: [String: Any] = ["cmd": "search", "query": query]
        if let b = bucket { cmd["bucket"] = b }
        if let l = limit { cmd["limit"] = l }
        return try execute(cmd)
    }

    // MARK: - Raw Execute

    /// Execute a raw JSON command dictionary. Returns the parsed response.
    @discardableResult
    public func execute(_ command: [String: Any]) throws -> [String: Any] {
        guard let h = handle else {
            throw OxiDBError.databaseOpenFailed
        }

        let cmdJson = try jsonString(command)
        let raw = UnsafeMutableRawPointer(h)

        guard let cStr = oxidb_execute(raw, cmdJson) else {
            throw OxiDBError.operationFailed("Execute returned null")
        }

        defer { COxiDBEmbedded.oxidb_free_string(cStr) }

        let str = String(cString: cStr)
        return try parseResponse(str)
    }
}
