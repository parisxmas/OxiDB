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
    func execute(_ cmd: [String: Any]) -> (result: [String: Any], ms: Double)? {
        guard let h = handle else { return nil }
        guard let data = try? JSONSerialization.data(withJSONObject: cmd),
              let json = String(data: data, encoding: .utf8) else {
            return nil
        }
        let start = CFAbsoluteTimeGetCurrent()
        guard let result = oxidb_execute(UnsafeMutableRawPointer(h), json) else {
            return nil
        }
        let ms = (CFAbsoluteTimeGetCurrent() - start) * 1000.0
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
        return (parsed, ms)
    }

    // MARK: - Demo Operations

    private func fmt(_ ms: Double) -> String {
        String(format: "%.2fms", ms)
    }

    func ping() {
        if let r = execute(["cmd": "ping"]) {
            log("Ping: \(r.result["data"] ?? "") (\(fmt(r.ms)))")
        }
    }

    func insertSample() {
        let docs: [[String: Any]] = [
            ["name": "Alice", "age": 30, "city": "New York"],
            ["name": "Bob", "age": 25, "city": "San Francisco"],
            ["name": "Charlie", "age": 35, "city": "New York"],
        ]
        if let r = execute(["cmd": "insert_many", "collection": "users", "docs": docs]) {
            log("Inserted: \(r.result["data"] ?? "") (\(fmt(r.ms)))")
        }
    }

    func query() {
        if let r = execute(["cmd": "find", "collection": "users", "query": ["city": "New York"]]) {
            if let data = r.result["data"] as? [[String: Any]] {
                log("Found \(data.count) docs in New York (\(fmt(r.ms))):")
                for doc in data { log("  \(doc)") }
            }
        }
    }

    func count() {
        if let r = execute(["cmd": "count", "collection": "users"]) {
            log("Count: \(r.result["data"] ?? "") (\(fmt(r.ms)))")
        }
    }

    func createIndex() {
        if let r = execute(["cmd": "create_index", "collection": "users", "field": "city"]) {
            log("Index created on 'city' (\(fmt(r.ms)))")
        }
    }

    func aggregate() {
        let pipeline: [[String: Any]] = [
            ["$group": ["_id": "city", "count": ["$count": true]]],
            ["$sort": ["count": -1]]
        ]
        if let r = execute(["cmd": "aggregate", "collection": "users", "pipeline": pipeline]) {
            log("Aggregation: \(r.result["data"] ?? "") (\(fmt(r.ms)))")
        }
    }

    func listCollections() {
        if let r = execute(["cmd": "list_collections"]) {
            log("Collections: \(r.result["data"] ?? "") (\(fmt(r.ms)))")
        }
    }

    func dropUsers() {
        if let r = execute(["cmd": "drop_collection", "collection": "users"]) {
            log("Dropped 'users' collection (\(fmt(r.ms)))")
        }
    }

    // MARK: - Full-Text Search Demo

    func insertArticles() {
        let docs: [[String: Any]] = [
            ["title": "Getting Started with Rust", "body": "Rust is a systems programming language focused on safety, speed, and concurrency."],
            ["title": "Swift for iOS Development", "body": "Swift is a powerful and intuitive programming language for building iOS and macOS apps."],
            ["title": "Rust and WebAssembly", "body": "Rust compiles to WebAssembly, enabling fast and safe web applications in the browser."],
            ["title": "Database Design Patterns", "body": "Document databases store data as JSON documents, offering flexibility and performance."],
            ["title": "Building Mobile Apps", "body": "Mobile development with Swift and SwiftUI makes creating beautiful iOS apps easy and fun."],
        ]
        if let r = execute(["cmd": "insert_many", "collection": "articles", "docs": docs]) {
            log("Inserted 5 articles (\(fmt(r.ms)))")
        }
    }

    func createTextIndex() {
        if let r = execute(["cmd": "create_text_index", "collection": "articles", "fields": ["title", "body"]]) {
            log("Text index created on [title, body] (\(fmt(r.ms)))")
        }
    }

    func searchArticles(_ query: String) {
        if let r = execute(["cmd": "text_search", "collection": "articles", "query": query, "limit": 10]) {
            if let data = r.result["data"] as? [[String: Any]] {
                log("Search \"\(query)\": \(data.count) results (\(fmt(r.ms)))")
                for doc in data {
                    let title = doc["title"] as? String ?? "?"
                    let score = doc["_score"] as? Double ?? 0
                    log("  \(title) (score: \(String(format: "%.3f", score)))")
                }
            }
        }
    }

    func dropArticles() {
        if let r = execute(["cmd": "drop_collection", "collection": "articles"]) {
            log("Dropped 'articles' collection (\(fmt(r.ms)))")
        }
    }

    func clearLogs() { logs.removeAll() }

    func log(_ msg: String, isError: Bool = false) {
        logs.append(LogEntry(message: msg, isError: isError))
    }
}
