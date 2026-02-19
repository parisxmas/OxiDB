import Foundation

@MainActor
final class EmbeddedDB: ObservableObject {
    @Published var isOpen = false
    @Published var logs: [LogEntry] = []
    @Published var watchRules: [WatchRule] = []
    @Published var watchToast: WatchToast?

    private var handle: OpaquePointer?
    private var mutationObservers: [UUID: (MutationEvent) -> Void] = [:]
    private var mutationLogObserverID: UUID?
    private var watchToastTask: Task<Void, Never>?

    enum MutationOperation: String {
        case insert
        case insertMany = "insert_many"
        case update
        case delete
        case commitTx = "commit_tx"
    }

    struct MutationEvent {
        let operation: MutationOperation
        let collection: String?
        let timestamp: Date
        let metadata: [String: Any]
    }

    struct LogEntry: Identifiable {
        let id = UUID()
        let timestamp = Date()
        let message: String
        let isError: Bool
    }

    struct EditableDocument: Identifiable {
        let id: UInt64
        let data: [String: Any]
        let summary: String
    }

    struct WatchRule: Identifiable {
        let id: String
        let name: String
        let collection: String
        let mode: String // document | property | query
        let documentID: UInt64?
        let propertyPath: String?
        let query: [String: Any]?
        let enabled: Bool
    }

    struct WatchToast: Identifiable, Equatable {
        let id = UUID()
        let title: String
        let subtitle: String
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
        installMutationLogObserverIfNeeded()
        loadWatchRules()
        log("Opened database at: \(path)")
    }

    func close() {
        guard let h = handle else { return }
        oxidb_close(UnsafeMutableRawPointer(h))
        handle = nil
        isOpen = false
        watchRules = []
        watchToastTask?.cancel()
        watchToastTask = nil
        watchToast = nil
        if let id = mutationLogObserverID {
            removeMutationObserver(id)
            mutationLogObserverID = nil
        }
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
        emitMutationIfNeeded(command: cmd, response: parsed)
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
        insertUsers(count: 3)
    }

    func insertUsers(count: Int, batchSize: Int = 1000) {
        guard count > 0 else {
            log("Insert users: count must be > 0", isError: true)
            return
        }

        let cities = ["New York", "San Francisco", "Berlin", "Tokyo", "Istanbul", "Paris"]
        let started = CFAbsoluteTimeGetCurrent()
        var inserted = 0

        while inserted < count {
            let size = min(batchSize, count - inserted)
            var docs: [[String: Any]] = []
            docs.reserveCapacity(size)

            for i in 0..<size {
                let n = inserted + i + 1
                docs.append([
                    "name": "User\(n)",
                    "age": 18 + (n % 53),
                    "city": cities[n % cities.count]
                ])
            }

            guard execute(["cmd": "insert_many", "collection": "users", "docs": docs]) != nil else {
                log("Insert users aborted at \(inserted)/\(count)", isError: true)
                return
            }
            inserted += size
        }

        let ms = (CFAbsoluteTimeGetCurrent() - started) * 1000.0
        let rate = Double(count) / max(ms / 1000.0, 0.001)
        log("Inserted \(count) users in \(fmt(ms)) (\(Int(rate)) docs/sec)")
    }

    func query() {
        runQueryScenario(
            collection: "users",
            queryJSON: #"{"city":"New York"}"#,
            sortField: nil,
            sortDescending: false,
            skip: nil,
            limit: nil
        )
    }

    func runQueryScenario(
        collection: String,
        queryJSON: String,
        sortField: String?,
        sortDescending: Bool,
        skip: Int?,
        limit: Int?
    ) {
        guard let queryData = queryJSON.data(using: .utf8),
              let queryObj = try? JSONSerialization.jsonObject(with: queryData) as? [String: Any] else {
            log("Query scenario failed: invalid JSON query", isError: true)
            return
        }

        var cmd: [String: Any] = [
            "cmd": "find",
            "collection": collection,
            "query": queryObj
        ]

        if let field = sortField, !field.isEmpty {
            cmd["sort"] = [field: sortDescending ? -1 : 1]
        }
        if let skip, skip > 0 {
            cmd["skip"] = skip
        }
        if let limit, limit > 0 {
            cmd["limit"] = limit
        }

        guard let r = execute(cmd) else { return }
        guard let data = r.result["data"] as? [[String: Any]] else {
            log("Query returned no data array (\(fmt(r.ms)))", isError: true)
            return
        }

        log("Query scenario -> collection=\(collection), results=\(data.count) (\(fmt(r.ms)))")
        for (idx, doc) in data.prefix(5).enumerated() {
            log("  [\(idx)] \(doc)")
        }
        if data.count > 5 {
            log("  ... \(data.count - 5) more rows")
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

    func listDocumentsForEditing(collection: String, limit: Int = 100) -> [EditableDocument] {
        guard limit > 0 else { return [] }
        guard let r = execute([
            "cmd": "find",
            "collection": collection,
            "query": [:],
            "sort": ["_id": 1],
            "limit": limit
        ]) else { return [] }

        guard let rows = r.result["data"] as? [[String: Any]] else { return [] }
        return rows.compactMap { row in
            guard let idNum = row["_id"] as? NSNumber else { return nil }
            let docID = idNum.uint64Value
            let summary = (row["name"] as? String)
                ?? (row["title"] as? String)
                ?? (row["city"] as? String)
                ?? "document"
            return EditableDocument(id: docID, data: row, summary: summary)
        }
    }

    func updateDocumentByID(collection: String, documentID: UInt64, editedJSON: String) -> Bool {
        guard let data = editedJSON.data(using: .utf8),
              let edited = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            log("Edit failed: invalid document JSON", isError: true)
            return false
        }

        guard let current = execute([
            "cmd": "find_one",
            "collection": collection,
            "query": ["_id": documentID]
        ])?.result["data"] as? [String: Any] else {
            log("Edit failed: document _id=\(documentID) not found", isError: true)
            return false
        }

        var setMap = edited
        setMap.removeValue(forKey: "_id")
        setMap.removeValue(forKey: "_version")

        if setMap.isEmpty {
            log("Edit failed: nothing to update (only _id/_version present)", isError: true)
            return false
        }

        var changedFields: [String: Any] = [:]
        for (key, newValue) in setMap {
            if !jsonValueEquals(current[key], newValue) {
                changedFields[key] = newValue
            }
        }

        if changedFields.isEmpty {
            log("Edit skipped: no field value changes for _id=\(documentID)")
            return false
        }

        guard let r = execute([
            "cmd": "update_one",
            "collection": collection,
            "query": ["_id": documentID],
            "update": ["$set": changedFields]
        ]) else { return false }

        let modified = ((r.result["data"] as? [String: Any])?["modified"] as? Int) ?? -1
        if modified == 1 {
            log("Edited _id=\(documentID) in \(collection) (\(fmt(r.ms)))")
            return true
        }

        if modified == 0 {
            log("Edit warning: _id=\(documentID) not modified (\(fmt(r.ms)))", isError: true)
            return false
        }

        log("Edit completed for _id=\(documentID) (\(fmt(r.ms)))")
        return true
    }

    // MARK: - Watch Rules

    func addDocumentWatchRule(collection: String, documentID: UInt64) {
        addWatchRule([
            "type": "watch_rule",
            "name": "Watch #\(documentID)",
            "collection": collection,
            "mode": "document",
            "document_id": documentID,
            "enabled": true,
            "created_at": ISO8601DateFormatter().string(from: Date())
        ])
    }

    func addPropertyWatchRule(collection: String, documentID: UInt64?, propertyPath: String) {
        let ruleName: String
        if let documentID {
            ruleName = "Watch \(propertyPath) on #\(documentID)"
        } else {
            ruleName = "Watch \(propertyPath) in \(collection)"
        }
        var rule: [String: Any] = [
            "type": "watch_rule",
            "name": ruleName,
            "collection": collection,
            "mode": "property",
            "property_path": propertyPath,
            "enabled": true,
            "created_at": ISO8601DateFormatter().string(from: Date())
        ]
        if let documentID {
            rule["document_id"] = documentID
        }
        addWatchRule(rule)
    }

    func addQueryWatchRule(collection: String, queryJSON: String) {
        guard let data = queryJSON.data(using: .utf8),
              let queryObj = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            log("Watch rule failed: invalid query JSON", isError: true)
            return
        }

        addWatchRule([
            "type": "watch_rule",
            "name": "Watch new docs in \(collection)",
            "collection": collection,
            "mode": "query",
            "query": queryObj,
            "enabled": true,
            "created_at": ISO8601DateFormatter().string(from: Date())
        ])
    }

    func removeWatchRule(_ ruleID: String) {
        _ = execute([
            "cmd": "delete",
            "collection": "_config",
            "query": ["type": "watch_rule", "rule_id": ruleID]
        ])
        loadWatchRules()
        log("Removed watch rule \(ruleID)")
    }

    func loadWatchRules() {
        guard isOpen else {
            watchRules = []
            return
        }

        guard let r = execute([
            "cmd": "find",
            "collection": "_config",
            "query": ["type": "watch_rule", "enabled": true],
            "sort": ["_id": 1],
            "limit": 200
        ]) else {
            watchRules = []
            return
        }

        let rows = (r.result["data"] as? [[String: Any]]) ?? []
        watchRules = rows.compactMap { row in
            guard let id = row["rule_id"] as? String,
                  let name = row["name"] as? String,
                  let collection = row["collection"] as? String,
                  let mode = row["mode"] as? String else {
                return nil
            }
            let docID = (row["document_id"] as? NSNumber)?.uint64Value
            let propertyPath = row["property_path"] as? String
            let query = row["query"] as? [String: Any]
            let enabled = (row["enabled"] as? Bool) ?? false
            return WatchRule(
                id: id,
                name: name,
                collection: collection,
                mode: mode,
                documentID: docID,
                propertyPath: propertyPath,
                query: query,
                enabled: enabled
            )
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
        insertArticles(count: 5)
    }

    func insertArticles(count: Int, batchSize: Int = 1000) {
        guard count > 0 else {
            log("Insert articles: count must be > 0", isError: true)
            return
        }

        let templates: [(String, String)] = [
            ("Getting Started with Rust", "Rust is a systems programming language focused on safety, speed, and concurrency."),
            ("Swift for iOS Development", "Swift is a powerful and intuitive programming language for building iOS and macOS apps."),
            ("Rust and WebAssembly", "Rust compiles to WebAssembly, enabling fast and safe web applications in the browser."),
            ("Database Design Patterns", "Document databases store data as JSON documents, offering flexibility and performance."),
            ("Building Mobile Apps", "Mobile development with Swift and SwiftUI makes creating beautiful iOS apps easy and fun."),
        ]

        let started = CFAbsoluteTimeGetCurrent()
        var inserted = 0

        while inserted < count {
            let size = min(batchSize, count - inserted)
            var docs: [[String: Any]] = []
            docs.reserveCapacity(size)

            for i in 0..<size {
                let n = inserted + i + 1
                let template = templates[n % templates.count]
                docs.append([
                    "title": "\(template.0) #\(n)",
                    "body": "\(template.1) Benchmark row \(n)."
                ])
            }

            guard execute(["cmd": "insert_many", "collection": "articles", "docs": docs]) != nil else {
                log("Insert articles aborted at \(inserted)/\(count)", isError: true)
                return
            }
            inserted += size
        }

        let ms = (CFAbsoluteTimeGetCurrent() - started) * 1000.0
        let rate = Double(count) / max(ms / 1000.0, 0.001)
        log("Inserted \(count) articles in \(fmt(ms)) (\(Int(rate)) docs/sec)")
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

    // MARK: - Local Mutation Watching

    @discardableResult
    func addMutationObserver(_ handler: @escaping (MutationEvent) -> Void) -> UUID {
        let id = UUID()
        mutationObservers[id] = handler
        return id
    }

    func removeMutationObserver(_ id: UUID) {
        mutationObservers.removeValue(forKey: id)
    }

    private func emitMutation(_ event: MutationEvent) {
        evaluateWatchRules(event)
        let handlers = Array(mutationObservers.values)
        for handler in handlers {
            handler(event)
        }
    }

    private func installMutationLogObserverIfNeeded() {
        guard mutationLogObserverID == nil else { return }
        mutationLogObserverID = addMutationObserver { [weak self] event in
            guard let self else { return }
            let collection = event.collection ?? "-"
            let detail = event.metadata
                .map { "\($0.key)=\($0.value)" }
                .sorted()
                .joined(separator: ", ")
            if detail.isEmpty {
                self.log("WATCH \(event.operation.rawValue) collection=\(collection)")
            } else {
                self.log("WATCH \(event.operation.rawValue) collection=\(collection) \(detail)")
            }
        }
    }

    private func emitMutationIfNeeded(command: [String: Any], response: [String: Any]) {
        guard let cmd = command["cmd"] as? String else { return }
        let collection = command["collection"] as? String

        let event: MutationEvent?
        switch cmd {
        case "insert":
            let insertedID = uint64FromAny((response["data"] as? [String: Any])?["id"])
            event = MutationEvent(
                operation: .insert,
                collection: collection,
                timestamp: Date(),
                metadata: [
                    "documentCount": 1,
                    "id": insertedID as Any
                ]
            )
        case "insert_many":
            let count = (command["docs"] as? [[String: Any]])?.count ?? 0
            let insertedIDs = ((response["data"] as? [Any]) ?? []).compactMap(uint64FromAny)
            event = MutationEvent(
                operation: .insertMany,
                collection: collection,
                timestamp: Date(),
                metadata: [
                    "documentCount": count,
                    "ids": insertedIDs
                ]
            )
        case "update", "update_one":
            event = MutationEvent(
                operation: .update,
                collection: collection,
                timestamp: Date(),
                metadata: [
                    "single": cmd == "update_one",
                    "query": command["query"] as Any,
                    "update": command["update"] as Any
                ]
            )
        case "delete", "delete_one":
            event = MutationEvent(
                operation: .delete,
                collection: collection,
                timestamp: Date(),
                metadata: [
                    "single": cmd == "delete_one",
                    "query": command["query"] as Any
                ]
            )
        case "commit_tx":
            event = MutationEvent(
                operation: .commitTx,
                collection: nil,
                timestamp: Date(),
                metadata: [:]
            )
        default:
            event = nil
        }

        if let event {
            emitMutation(event)
        }
    }

    private func addWatchRule(_ document: [String: Any]) {
        guard isOpen else { return }
        var doc = document
        doc["rule_id"] = UUID().uuidString
        guard execute([
            "cmd": "insert",
            "collection": "_config",
            "doc": doc
        ]) != nil else { return }

        loadWatchRules()
        log("Added watch rule: \(doc["name"] ?? "rule")")
    }

    private func eventAffectedDocumentIDs(_ event: MutationEvent) -> [UInt64] {
        var ids: [UInt64] = []
        if let id = uint64FromAny(event.metadata["id"]) {
            ids.append(id)
        }
        if let many = event.metadata["ids"] as? [Any] {
            ids.append(contentsOf: many.compactMap(uint64FromAny))
        }
        if let query = event.metadata["query"] as? [String: Any],
           let qid = uint64FromAny(query["_id"]) {
            ids.append(qid)
        }
        return Array(Set(ids))
    }

    private func isPropertyTouched(path: String, event: MutationEvent, docID: UInt64?) -> Bool {
        switch event.operation {
        case .update:
            guard let update = event.metadata["update"] as? [String: Any] else {
                return false
            }
            for (_, payload) in update {
                if let fields = payload as? [String: Any] {
                    for key in fields.keys where propertyPathMatches(watchPath: path, changedPath: key) {
                        return true
                    }
                }
            }
            return false
        case .insert, .insertMany:
            guard let docID else { return false }
            return documentHasPath(collection: event.collection ?? "", docID: docID, path: path)
        case .delete, .commitTx:
            return false
        }
    }

    private func documentHasPath(collection: String, docID: UInt64, path: String) -> Bool {
        guard !collection.isEmpty else { return false }
        guard let r = execute([
            "cmd": "find_one",
            "collection": collection,
            "query": ["_id": docID]
        ]) else { return false }
        guard let doc = r.result["data"] as? [String: Any] else { return false }

        var current: Any = doc
        for part in path.split(separator: ".").map(String.init) {
            guard let obj = current as? [String: Any],
                  let next = obj[part] else {
                // Allow a leaf fallback so a rule like `user.name` can still match docs using `name`.
                if let leaf = path.split(separator: ".").last.map(String.init),
                   doc[leaf] != nil {
                    return true
                }
                return false
            }
            current = next
        }
        return true
    }

    private func documentMatchesRuleQuery(collection: String, docID: UInt64, query: [String: Any]) -> Bool {
        guard let r = execute([
            "cmd": "find_one",
            "collection": collection,
            "query": ["$and": [["_id": docID], query]]
        ]) else { return false }
        return (r.result["data"] as? [String: Any]) != nil
    }

    private func evaluateWatchRules(_ event: MutationEvent) {
        guard event.collection != "_config" else { return }
        guard !watchRules.isEmpty else { return }

        let affectedIDs = eventAffectedDocumentIDs(event)

        for rule in watchRules where rule.enabled {
            guard rule.collection == event.collection else { continue }

            var matched = false
            var matchedDoc: UInt64?

            switch rule.mode {
            case "document":
                guard let docID = rule.documentID else { continue }
                matched = affectedIDs.contains(docID)
                matchedDoc = matched ? docID : nil
            case "property":
                guard let path = rule.propertyPath else { continue }
                if let docID = rule.documentID {
                    matched = affectedIDs.contains(docID) && isPropertyTouched(path: path, event: event, docID: docID)
                    matchedDoc = matched ? docID : nil
                } else {
                    if affectedIDs.isEmpty {
                        matched = isPropertyTouched(path: path, event: event, docID: nil)
                        matchedDoc = nil
                    } else if let firstMatched = affectedIDs.first(where: { isPropertyTouched(path: path, event: event, docID: $0) }) {
                        matched = true
                        matchedDoc = firstMatched
                    }
                }
            case "query":
                guard (event.operation == .insert || event.operation == .insertMany),
                      let query = rule.query else { continue }
                for id in affectedIDs where documentMatchesRuleQuery(collection: rule.collection, docID: id, query: query) {
                    matched = true
                    matchedDoc = id
                    break
                }
            default:
                continue
            }

            if matched {
                let trigger: [String: Any] = [
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "collection": rule.collection,
                    "mode": rule.mode,
                    "matched_doc_id": matchedDoc as Any,
                    "event": [
                        "operation": event.operation.rawValue,
                        "timestamp": ISO8601DateFormatter().string(from: event.timestamp),
                        "metadata": event.metadata
                    ]
                ]

                if let data = try? JSONSerialization.data(withJSONObject: trigger, options: [.sortedKeys]),
                   let json = String(data: data, encoding: .utf8) {
                    log("WATCH TRIGGER \(json)")
                } else {
                    log("WATCH TRIGGER rule=\(rule.name)")
                }
                showWatchToast(rule: rule, event: event, matchedDoc: matchedDoc)
            }
        }
    }

    func clearLogs() { logs.removeAll() }

    func dismissWatchToast() {
        watchToastTask?.cancel()
        watchToastTask = nil
        watchToast = nil
    }

    func log(_ msg: String, isError: Bool = false) {
        logs.append(LogEntry(message: msg, isError: isError))
    }

    private func showWatchToast(rule: WatchRule, event: MutationEvent, matchedDoc: UInt64?) {
        let op = event.operation.rawValue
        let docPart = matchedDoc.map { "doc #\($0)" } ?? "rule match"
        watchToast = WatchToast(
            title: "Watch Triggered",
            subtitle: "\(rule.collection) • \(op) • \(docPart)"
        )

        watchToastTask?.cancel()
        watchToastTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: 2_500_000_000)
            guard let self, !Task.isCancelled else { return }
            self.watchToast = nil
            self.watchToastTask = nil
        }
    }

    private func uint64FromAny(_ value: Any?) -> UInt64? {
        switch value {
        case let v as UInt64:
            return v
        case let v as UInt:
            return UInt64(v)
        case let v as Int:
            return v >= 0 ? UInt64(v) : nil
        case let v as NSNumber:
            return v.uint64Value
        case let v as String:
            return UInt64(v)
        default:
            return nil
        }
    }

    private func jsonValueEquals(_ lhs: Any?, _ rhs: Any?) -> Bool {
        switch (lhs, rhs) {
        case (nil, nil):
            return true
        case let (l?, r?):
            return (l as AnyObject).isEqual(r)
        default:
            return false
        }
    }

    private func propertyPathMatches(watchPath: String, changedPath: String) -> Bool {
        let w = watchPath.trimmingCharacters(in: .whitespacesAndNewlines)
        let c = changedPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !w.isEmpty, !c.isEmpty else { return false }

        if w == c || w.hasPrefix(c + ".") || c.hasPrefix(w + ".") {
            return true
        }

        let wLeaf = w.split(separator: ".").last.map(String.init) ?? w
        let cLeaf = c.split(separator: ".").last.map(String.init) ?? c
        return wLeaf == cLeaf
    }
}
