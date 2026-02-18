import Foundation

@MainActor
final class EmbeddedDB: ObservableObject {
    @Published var isOpen = false
    @Published var logs: [LogEntry] = []

    private var handle: OpaquePointer?

    struct LogEntry: Identifiable {
        let id = UUID()
        let timestamp = Date()
        let message: String
        let isError: Bool
    }

    func open() {
        let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        let path = docs.appendingPathComponent("oxidb_demo").path

        guard let raw = oxidb_open(path) else {
            log("Failed to open database", isError: true)
            return
        }
        handle = OpaquePointer(raw)
        isOpen = true
        log("Opened database at: \(path)")
    }

    func close() {
        guard let h = handle else { return }
        oxidb_close(UnsafeMutableRawPointer(h))
        handle = nil
        isOpen = false
        log("Database closed")
    }

    @discardableResult
    func execute(_ cmd: [String: Any]) -> [String: Any]? {
        guard let h = handle else { return nil }
        guard let data = try? JSONSerialization.data(withJSONObject: cmd),
              let json = String(data: data, encoding: .utf8),
              let result = oxidb_execute(UnsafeMutableRawPointer(h), json) else {
            return nil
        }
        defer { oxidb_free_string(result) }
        let str = String(cString: result)
        guard let rData = str.data(using: .utf8),
              let parsed = try? JSONSerialization.jsonObject(with: rData) as? [String: Any] else {
            return nil
        }
        if let ok = parsed["ok"] as? Bool, !ok {
            log("Error: \(parsed["error"] ?? "unknown")", isError: true)
            return nil
        }
        return parsed
    }

    // MARK: - Demo Operations

    func ping() {
        if let r = execute(["cmd": "ping"]) {
            log("Ping: \(r["data"] ?? "")")
        }
    }

    func insertSample() {
        let docs: [[String: Any]] = [
            ["name": "Alice", "age": 30, "city": "New York"],
            ["name": "Bob", "age": 25, "city": "San Francisco"],
            ["name": "Charlie", "age": 35, "city": "New York"],
        ]
        if let r = execute(["cmd": "insert_many", "collection": "users", "docs": docs]) {
            log("Inserted: \(r["data"] ?? "")")
        }
    }

    func query() {
        if let r = execute(["cmd": "find", "collection": "users", "query": ["city": "New York"]]) {
            if let data = r["data"] as? [[String: Any]] {
                log("Found \(data.count) docs in New York:")
                for doc in data { log("  \(doc)") }
            }
        }
    }

    func count() {
        if let r = execute(["cmd": "count", "collection": "users"]) {
            log("Count: \(r["data"] ?? "")")
        }
    }

    func createIndex() {
        if execute(["cmd": "create_index", "collection": "users", "field": "city"]) != nil {
            log("Index created on 'city'")
        }
    }

    func aggregate() {
        let pipeline: [[String: Any]] = [
            ["$group": ["_id": "city", "count": ["$count": true]]],
            ["$sort": ["count": -1]]
        ]
        if let r = execute(["cmd": "aggregate", "collection": "users", "pipeline": pipeline]) {
            log("Aggregation: \(r["data"] ?? "")")
        }
    }

    func listCollections() {
        if let r = execute(["cmd": "list_collections"]) {
            log("Collections: \(r["data"] ?? "")")
        }
    }

    func dropUsers() {
        if execute(["cmd": "drop_collection", "collection": "users"]) != nil {
            log("Dropped 'users' collection")
        }
    }

    func clearLogs() { logs.removeAll() }

    func log(_ msg: String, isError: Bool = false) {
        logs.append(LogEntry(message: msg, isError: isError))
    }
}
