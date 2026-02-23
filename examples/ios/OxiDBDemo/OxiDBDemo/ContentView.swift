import SwiftUI

struct ContentView: View {
    @StateObject private var manager = OxiDBManager.shared
    @State private var host = "127.0.0.1"
    @State private var port = "4444"

    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                connectionBar
                actionButtons
                logView
            }
            .navigationTitle("OxiDB Demo")
            .navigationBarTitleDisplayMode(.inline)
        }
    }

    // MARK: - Connection Bar

    private var connectionBar: some View {
        HStack(spacing: 8) {
            TextField("Host", text: $host)
                .textFieldStyle(.roundedBorder)
                .autocapitalization(.none)
                .disableAutocorrection(true)
            TextField("Port", text: $port)
                .textFieldStyle(.roundedBorder)
                .keyboardType(.numberPad)
                .frame(width: 70)
            Button(manager.isConnected ? "Disconnect" : "Connect") {
                if manager.isConnected {
                    manager.disconnect()
                } else {
                    let p = UInt16(port) ?? 4444
                    manager.connect(host: host, port: p)
                }
            }
            .buttonStyle(.borderedProminent)
            .tint(manager.isConnected ? .red : .green)
        }
        .padding()
    }

    // MARK: - Action Buttons

    private var actionButtons: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 8) {
                ActionButton(title: "Ping", icon: "bolt.fill") {
                    manager.ping()
                }
                ActionButton(title: "Insert", icon: "plus.circle.fill") {
                    manager.insertSampleDocuments()
                }
                ActionButton(title: "Query", icon: "magnifyingglass") {
                    manager.queryDocuments()
                }
                ActionButton(title: "Update One", icon: "pencil.circle.fill") {
                    manager.updateOneDemo()
                }
                ActionButton(title: "Delete One", icon: "minus.circle.fill") {
                    manager.deleteOneDemo()
                }
                ActionButton(title: "Count", icon: "number") {
                    manager.countDocuments()
                }
                ActionButton(title: "Index", icon: "list.bullet.indent") {
                    manager.createIndexDemo()
                }
                ActionButton(title: "Indexes", icon: "list.number") {
                    manager.listIndexes()
                }
                ActionButton(title: "Aggregate", icon: "chart.bar.fill") {
                    manager.aggregateDemo()
                }
                ActionButton(title: "Transaction", icon: "arrow.triangle.2.circlepath") {
                    manager.transactionDemo()
                }
                ActionButton(title: "Compact", icon: "arrow.down.right.and.arrow.up.left") {
                    manager.compactDemo()
                }
                ActionButton(title: "Collections", icon: "folder.fill") {
                    manager.listCollections()
                }
            }
            .padding(.horizontal)
        }
        .padding(.vertical, 8)
        .disabled(!manager.isConnected)
        .opacity(manager.isConnected ? 1.0 : 0.5)
    }

    // MARK: - Log View

    private var logView: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack {
                Text("Log")
                    .font(.headline)
                Spacer()
                Button("Clear") {
                    manager.clearLogs()
                }
                .font(.caption)
            }
            .padding(.horizontal)
            .padding(.vertical, 6)

            Divider()

            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 4) {
                        ForEach(manager.logs) { entry in
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
                .onChange(of: manager.logs.count) { _ in
                    if let last = manager.logs.last {
                        proxy.scrollTo(last.id, anchor: .bottom)
                    }
                }
            }
        }
        .background(Color(.systemGroupedBackground))
    }
}

// MARK: - Action Button

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
