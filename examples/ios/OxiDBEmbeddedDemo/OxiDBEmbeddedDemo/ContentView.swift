import SwiftUI

struct ContentView: View {
    @StateObject private var db = EmbeddedDB()
    @State private var searchQuery = ""

    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                statusBar
                actionButtons
                ftsSection
                logView
            }
            .navigationTitle("OxiDB Embedded")
            .navigationBarTitleDisplayMode(.inline)
        }
    }

    private var statusBar: some View {
        HStack {
            Circle()
                .fill(db.isOpen ? Color.green : Color.red)
                .frame(width: 10, height: 10)
            Text(db.isOpen ? "Database Open" : "Database Closed")
                .foregroundColor(db.isOpen ? .primary : .secondary)
            Spacer()
            Button(db.isOpen ? "Close" : "Open") {
                if db.isOpen { db.close() } else { db.open() }
            }
            .buttonStyle(.borderedProminent)
            .tint(db.isOpen ? .red : .green)
        }
        .padding()
    }

    private var actionButtons: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 8) {
                ActionButton(title: "Ping", icon: "bolt.fill") { db.ping() }
                ActionButton(title: "Insert", icon: "plus.circle.fill") { db.insertSample() }
                ActionButton(title: "Query", icon: "magnifyingglass") { db.query() }
                ActionButton(title: "Count", icon: "number") { db.count() }
                ActionButton(title: "Index", icon: "list.bullet.indent") { db.createIndex() }
                ActionButton(title: "Aggregate", icon: "chart.bar.fill") { db.aggregate() }
                ActionButton(title: "Collections", icon: "folder.fill") { db.listCollections() }
                ActionButton(title: "Drop", icon: "trash.fill") { db.dropUsers() }
            }
            .padding(.horizontal)
        }
        .padding(.vertical, 8)
        .disabled(!db.isOpen)
        .opacity(db.isOpen ? 1.0 : 0.5)
    }

    private var ftsSection: some View {
        VStack(spacing: 8) {
            Divider()
            Text("Full-Text Search").font(.caption).foregroundColor(.secondary)
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    ActionButton(title: "Add Articles", icon: "doc.text.fill") { db.insertArticles() }
                    ActionButton(title: "Text Index", icon: "text.magnifyingglass") { db.createTextIndex() }
                    ActionButton(title: "Drop Articles", icon: "trash") { db.dropArticles() }
                }
                .padding(.horizontal)
            }
            HStack(spacing: 8) {
                TextField("Search articles...", text: $searchQuery)
                    .textFieldStyle(.roundedBorder)
                    .font(.caption)
                    .submitLabel(.search)
                    .onSubmit { if !searchQuery.isEmpty { db.searchArticles(searchQuery) } }
                Button("Search") { db.searchArticles(searchQuery) }
                    .buttonStyle(.bordered)
                    .font(.caption)
                    .disabled(searchQuery.isEmpty)
            }
            .padding(.horizontal)
        }
        .padding(.bottom, 8)
        .disabled(!db.isOpen)
        .opacity(db.isOpen ? 1.0 : 0.5)
    }

    private var logView: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack {
                Text("Log").font(.headline)
                Spacer()
                Button("Clear") { db.clearLogs() }.font(.caption)
            }
            .padding(.horizontal)
            .padding(.vertical, 6)

            Divider()

            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 4) {
                        ForEach(db.logs) { entry in
                            HStack(alignment: .top, spacing: 6) {
                                Text(entry.timestamp, style: .time)
                                    .font(.caption2)
                                    .foregroundColor(.secondary)
                                Text(entry.message)
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundColor(entry.isError ? .red : .primary)
                            }
                            .id(entry.id)
                        }
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 4)
                }
                .onChange(of: db.logs.count) { _ in
                    if let last = db.logs.last {
                        proxy.scrollTo(last.id, anchor: .bottom)
                    }
                }
            }
        }
        .background(Color(.systemGroupedBackground))
    }
}

private struct ActionButton: View {
    let title: String
    let icon: String
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Label(title, systemImage: icon)
                .font(.caption)
                .padding(.horizontal, 12)
                .padding(.vertical, 8)
        }
        .buttonStyle(.bordered)
    }
}
