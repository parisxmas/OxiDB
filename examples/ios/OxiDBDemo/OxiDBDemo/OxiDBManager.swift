import Foundation
import OxiDB

@MainActor
final class OxiDBManager: ObservableObject {
    static let shared = OxiDBManager()

    @Published var isConnected = false
    @Published var logs: [LogEntry] = []

    private var client: OxiDBClient?

    struct LogEntry: Identifiable {
        let id = UUID()
        let timestamp = Date()
        let message: String
        let isError: Bool
    }

    func connect(host: String, port: UInt16) {
        do {
            client = try OxiDBClient.connect(host: host, port: port)
            isConnected = true
            log("Connected to \(host):\(port)")
        } catch {
            log("Connection failed: \(error.localizedDescription)", isError: true)
        }
    }

    func disconnect() {
        client?.disconnect()
        client = nil
        isConnected = false
        log("Disconnected")
    }

    func ping() {
        guard let client else { return }
        do {
            let result = try client.ping()
            log("Ping: \(result)")
        } catch {
            log("Ping failed: \(error.localizedDescription)", isError: true)
        }
    }

    func insertSampleDocuments() {
        guard let client else { return }
        do {
            let docs: [[String: Any]] = [
                ["name": "Alice", "age": 30, "city": "New York"],
                ["name": "Bob", "age": 25, "city": "San Francisco"],
                ["name": "Charlie", "age": 35, "city": "New York"],
            ]
            let result = try client.insertMany(collection: "users", documents: docs)
            log("Inserted documents: \(result)")
        } catch {
            log("Insert failed: \(error.localizedDescription)", isError: true)
        }
    }

    func queryDocuments() {
        guard let client else { return }
        do {
            let results = try client.find(collection: "users", query: ["city": "New York"])
            log("Found \(results.count) documents in New York:")
            for doc in results {
                log("  \(doc)")
            }
        } catch {
            log("Query failed: \(error.localizedDescription)", isError: true)
        }
    }

    func createIndexDemo() {
        guard let client else { return }
        do {
            let result = try client.createIndex(collection: "users", field: "city")
            log("Index created: \(result)")
        } catch {
            log("Create index failed: \(error.localizedDescription)", isError: true)
        }
    }

    func aggregateDemo() {
        guard let client else { return }
        do {
            let pipeline: [[String: Any]] = [
                ["$group": ["_id": "city", "count": ["$count": true]]],
                ["$sort": ["count": -1]],
            ]
            let result = try client.aggregate(collection: "users", pipeline: pipeline)
            log("Aggregation result: \(result)")
        } catch {
            log("Aggregation failed: \(error.localizedDescription)", isError: true)
        }
    }

    func transactionDemo() {
        guard let client else { return }
        do {
            try client.transaction {
                try client.insert(collection: "ledger", document: [
                    "from": "Alice", "to": "Bob", "amount": 100
                ])
                try client.insert(collection: "ledger", document: [
                    "from": "Bob", "to": "Charlie", "amount": 50
                ])
            }
            log("Transaction committed successfully")
        } catch {
            log("Transaction failed: \(error.localizedDescription)", isError: true)
        }
    }

    func updateOneDemo() {
        guard let client else { return }
        do {
            let result = try client.updateOne(
                collection: "users",
                query: ["name": "Alice"],
                update: ["$set": ["city": "Boston"]]
            )
            log("updateOne: \(result)")
        } catch {
            log("updateOne failed: \(error.localizedDescription)", isError: true)
        }
    }

    func deleteOneDemo() {
        guard let client else { return }
        do {
            let result = try client.deleteOne(collection: "users", query: ["name": "Bob"])
            log("deleteOne: \(result)")
        } catch {
            log("deleteOne failed: \(error.localizedDescription)", isError: true)
        }
    }

    func countDocuments() {
        guard let client else { return }
        do {
            let result = try client.count(collection: "users")
            log("Count: \(result)")
        } catch {
            log("Count failed: \(error.localizedDescription)", isError: true)
        }
    }

    func listIndexes() {
        guard let client else { return }
        do {
            let result = try client.listIndexes(collection: "users")
            log("Indexes: \(result)")
        } catch {
            log("List indexes failed: \(error.localizedDescription)", isError: true)
        }
    }

    func compactDemo() {
        guard let client else { return }
        do {
            let result = try client.compact(collection: "users")
            log("Compact: \(result)")
        } catch {
            log("Compact failed: \(error.localizedDescription)", isError: true)
        }
    }

    func listCollections() {
        guard let client else { return }
        do {
            let result = try client.listCollections()
            log("Collections: \(result)")
        } catch {
            log("List collections failed: \(error.localizedDescription)", isError: true)
        }
    }

    func clearLogs() {
        logs.removeAll()
    }

    private func log(_ message: String, isError: Bool = false) {
        logs.append(LogEntry(message: message, isError: isError))
    }
}
