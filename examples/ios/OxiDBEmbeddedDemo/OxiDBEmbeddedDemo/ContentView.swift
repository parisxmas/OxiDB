import SwiftUI

struct ContentView: View {
    @StateObject private var db = EmbeddedDB()
    @State private var searchQuery = ""
    @State private var showUsersInsertSheet = false
    @State private var showArticlesInsertSheet = false
    @State private var showQuerySheet = false
    @State private var showEditSheet = false
    @State private var showWatchSheet = false
    @State private var usersInsertCount = "1000"
    @State private var articlesInsertCount = "5000"
    @State private var queryCollection = "users"
    @State private var queryJSON = #"{"city":"New York"}"#
    @State private var querySortField = ""
    @State private var querySortDescending = false
    @State private var querySkip = ""
    @State private var queryLimit = "100"

    var body: some View {
        ZStack(alignment: .top) {
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

            if let watchToast = db.watchToast {
                WatchToastBannerView(toast: watchToast) {
                    withAnimation(.easeOut(duration: 0.2)) {
                        db.dismissWatchToast()
                    }
                }
                .padding(.horizontal, 12)
                .padding(.top, 8)
                .transition(.move(edge: .top).combined(with: .opacity))
            }
        }
        .animation(.easeInOut(duration: 0.2), value: db.watchToast?.id)
        .sheet(isPresented: $showUsersInsertSheet) {
            InsertCountSheet(
                title: "Insert Users",
                subtitle: "Choose how many user documents to insert.",
                countText: $usersInsertCount,
                presetCounts: [1000, 5000, 10000, 50000],
                buttonTitle: "Start Insert"
            ) { count in
                db.insertUsers(count: count)
            }
        }
        .sheet(isPresented: $showArticlesInsertSheet) {
            InsertCountSheet(
                title: "Insert Articles",
                subtitle: "Choose how many article documents to insert.",
                countText: $articlesInsertCount,
                presetCounts: [1000, 5000, 10000, 25000],
                buttonTitle: "Start Insert"
            ) { count in
                db.insertArticles(count: count)
            }
        }
        .sheet(isPresented: $showQuerySheet) {
            QueryScenarioSheet(
                collection: $queryCollection,
                queryJSON: $queryJSON,
                sortField: $querySortField,
                sortDescending: $querySortDescending,
                skipText: $querySkip,
                limitText: $queryLimit
            ) {
                db.runQueryScenario(
                    collection: queryCollection,
                    queryJSON: queryJSON,
                    sortField: querySortField.isEmpty ? nil : querySortField,
                    sortDescending: querySortDescending,
                    skip: Int(querySkip),
                    limit: Int(queryLimit)
                )
            }
        }
        .sheet(isPresented: $showEditSheet) {
            DocumentEditorSheet(db: db)
        }
        .sheet(isPresented: $showWatchSheet) {
            WatchConfigurationSheet(db: db)
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
                ActionButton(title: "Insert", icon: "plus.circle.fill") { showUsersInsertSheet = true }
                ActionButton(title: "Query", icon: "magnifyingglass") { showQuerySheet = true }
                ActionButton(title: "Edit Demo", icon: "pencil") { showEditSheet = true }
                ActionButton(title: "Count", icon: "number") { db.count() }
                ActionButton(title: "Index", icon: "list.bullet.indent") { db.createIndex() }
                ActionButton(title: "Aggregate", icon: "chart.bar.fill") { db.aggregate() }
                ActionButton(title: "Watch", icon: "eye.fill") { showWatchSheet = true }
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
                    ActionButton(title: "Add Articles", icon: "doc.text.fill") { showArticlesInsertSheet = true }
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

private struct WatchToastBannerView: View {
    let toast: EmbeddedDB.WatchToast
    let onTap: () -> Void

    var body: some View {
        Button(action: onTap) {
            HStack(spacing: 10) {
                Image(systemName: "eye.fill")
                    .foregroundColor(.white)
                VStack(alignment: .leading, spacing: 2) {
                    Text(toast.title)
                        .font(.caption.weight(.semibold))
                        .foregroundColor(.white)
                    Text(toast.subtitle)
                        .font(.caption2)
                        .foregroundColor(.white.opacity(0.92))
                        .lineLimit(2)
                }
                Spacer(minLength: 0)
            }
            .padding(10)
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(Color.green.opacity(0.95))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 12)
                    .stroke(Color.white.opacity(0.25), lineWidth: 1)
            )
            .shadow(color: .black.opacity(0.15), radius: 6, x: 0, y: 3)
        }
        .buttonStyle(.plain)
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

private struct InsertCountSheet: View {
    @Environment(\.dismiss) private var dismiss

    let title: String
    let subtitle: String
    @Binding var countText: String
    let presetCounts: [Int]
    let buttonTitle: String
    let onSubmit: (Int) -> Void

    private var parsedCount: Int? {
        Int(countText.trimmingCharacters(in: .whitespacesAndNewlines))
    }

    private var isValid: Bool {
        guard let value = parsedCount else { return false }
        return value > 0
    }

    var body: some View {
        NavigationView {
            VStack(alignment: .leading, spacing: 16) {
                Text(subtitle)
                    .font(.subheadline)
                    .foregroundColor(.secondary)

                TextField("Document count", text: $countText)
                    .keyboardType(.numberPad)
                    .textFieldStyle(.roundedBorder)

                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(presetCounts, id: \.self) { value in
                            Button("\(value)") { countText = "\(value)" }
                                .buttonStyle(.bordered)
                        }
                    }
                }

                Spacer()
            }
            .padding()
            .navigationTitle(title)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button(buttonTitle) {
                        if let count = parsedCount, count > 0 {
                            onSubmit(count)
                            dismiss()
                        }
                    }
                    .disabled(!isValid)
                }
            }
        }
    }
}

private struct QueryScenarioSheet: View {
    @Environment(\.dismiss) private var dismiss

    @Binding var collection: String
    @Binding var queryJSON: String
    @Binding var sortField: String
    @Binding var sortDescending: Bool
    @Binding var skipText: String
    @Binding var limitText: String
    let onRun: () -> Void

    private var queryLooksValid: Bool {
        guard let data = queryJSON.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data) else {
            return false
        }
        return obj is [String: Any]
    }

    var body: some View {
        NavigationView {
            VStack(alignment: .leading, spacing: 12) {
                Picker("Collection", selection: $collection) {
                    Text("users").tag("users")
                    Text("articles").tag("articles")
                }
                .pickerStyle(.segmented)

                Text("Query JSON")
                    .font(.caption)
                    .foregroundColor(.secondary)
                TextEditor(text: $queryJSON)
                    .font(.system(.caption, design: .monospaced))
                    .frame(minHeight: 120)
                    .overlay(
                        RoundedRectangle(cornerRadius: 8)
                            .stroke(Color.secondary.opacity(0.3), lineWidth: 1)
                    )

                HStack(spacing: 12) {
                    TextField("Sort field (optional)", text: $sortField)
                        .textFieldStyle(.roundedBorder)
                    Toggle("Desc", isOn: $sortDescending)
                        .toggleStyle(.switch)
                        .frame(width: 90)
                }

                HStack(spacing: 12) {
                    TextField("Skip", text: $skipText)
                        .keyboardType(.numberPad)
                        .textFieldStyle(.roundedBorder)
                    TextField("Limit", text: $limitText)
                        .keyboardType(.numberPad)
                        .textFieldStyle(.roundedBorder)
                }

                HStack(spacing: 8) {
                    Button("All") { queryJSON = "{}" }
                        .buttonStyle(.bordered)
                    Button("City=NY") { queryJSON = #"{"city":"New York"}"# }
                        .buttonStyle(.bordered)
                    Button("Age>=30") { queryJSON = #"{"age":{"$gte":30}}"# }
                        .buttonStyle(.bordered)
                }
                .font(.caption)

                if !queryLooksValid {
                    Text("Query JSON must be a valid JSON object.")
                        .font(.caption)
                        .foregroundColor(.red)
                }

                Spacer()
            }
            .padding()
            .navigationTitle("Query Scenario")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Run") {
                        onRun()
                        dismiss()
                    }
                    .disabled(!queryLooksValid)
                }
            }
        }
    }
}

private struct DocumentEditorSheet: View {
    @Environment(\.dismiss) private var dismiss
    @ObservedObject var db: EmbeddedDB

    @State private var collection = "users"
    @State private var limitText = "100"
    @State private var documents: [EmbeddedDB.EditableDocument] = []
    @State private var selected: EmbeddedDB.EditableDocument?
    @State private var editorJSON = "{}"
    @State private var validationError = ""

    private var editedJSONValid: Bool {
        guard let data = editorJSON.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data) else {
            return false
        }
        return obj is [String: Any]
    }

    private var limitValue: Int {
        Int(limitText) ?? 100
    }

    private func prettyJSONString(_ object: [String: Any]) -> String {
        guard JSONSerialization.isValidJSONObject(object),
              let data = try? JSONSerialization.data(withJSONObject: object, options: [.prettyPrinted, .sortedKeys]),
              let str = String(data: data, encoding: .utf8) else {
            return "{}"
        }
        return str
    }

    private func loadDocuments() {
        documents = db.listDocumentsForEditing(collection: collection, limit: max(limitValue, 1))
        if let first = documents.first {
            selected = first
            editorJSON = prettyJSONString(first.data)
        } else {
            selected = nil
            editorJSON = "{}"
        }
    }

    private func saveSelectedDocument() {
        guard let selected else { return }
        guard editedJSONValid else {
            validationError = "Edited JSON must be a valid JSON object."
            return
        }

        let ok = db.updateDocumentByID(
            collection: collection,
            documentID: selected.id,
            editedJSON: editorJSON
        )
        if ok {
            validationError = ""
            loadDocuments()
        }
    }

    var body: some View {
        NavigationView {
            VStack(alignment: .leading, spacing: 10) {
                Picker("Collection", selection: $collection) {
                    Text("users").tag("users")
                    Text("articles").tag("articles")
                }
                .pickerStyle(.segmented)
                .onChange(of: collection) { _ in
                    loadDocuments()
                }

                HStack(spacing: 8) {
                    TextField("Load limit", text: $limitText)
                        .keyboardType(.numberPad)
                        .textFieldStyle(.roundedBorder)
                        .frame(width: 110)
                    Button("Load Items") { loadDocuments() }
                        .buttonStyle(.borderedProminent)
                }

                if documents.isEmpty {
                    Text("No documents loaded.")
                        .font(.caption)
                        .foregroundColor(.secondary)
                } else {
                    ScrollView {
                        LazyVStack(alignment: .leading, spacing: 6) {
                            ForEach(documents) { doc in
                                Button {
                                    selected = doc
                                    editorJSON = prettyJSONString(doc.data)
                                } label: {
                                    HStack {
                                        Text("#\(doc.id)")
                                            .font(.caption.monospaced())
                                        Text(doc.summary)
                                            .font(.caption)
                                            .lineLimit(1)
                                        Spacer()
                                        if selected?.id == doc.id {
                                            Image(systemName: "checkmark.circle.fill")
                                                .foregroundColor(.green)
                                        }
                                    }
                                    .padding(8)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .background(
                                        RoundedRectangle(cornerRadius: 8)
                                            .fill(selected?.id == doc.id ? Color.blue.opacity(0.12) : Color.secondary.opacity(0.08))
                                    )
                                }
                                .buttonStyle(.plain)
                            }
                        }
                    }
                    .frame(maxHeight: 180)
                }

                Text("Edit Selected Document JSON")
                    .font(.caption)
                    .foregroundColor(.secondary)
                TextEditor(text: $editorJSON)
                    .font(.system(.caption, design: .monospaced))
                    .frame(minHeight: 170)
                    .overlay(
                        RoundedRectangle(cornerRadius: 8)
                            .stroke(Color.secondary.opacity(0.3), lineWidth: 1)
                    )

                HStack(spacing: 8) {
                    Button("Save Selected") { saveSelectedDocument() }
                        .buttonStyle(.borderedProminent)
                        .disabled(selected == nil)
                    Button("Reload") { loadDocuments() }
                        .buttonStyle(.bordered)
                }

                if !validationError.isEmpty {
                    Text(validationError)
                        .font(.caption)
                        .foregroundColor(.red)
                } else if !editedJSONValid {
                    Text("Edited JSON must be a valid JSON object.")
                        .font(.caption)
                        .foregroundColor(.red)
                }

                Spacer()
            }
            .padding()
            .navigationTitle("Edit Item")
            .navigationBarTitleDisplayMode(.inline)
            .onAppear {
                loadDocuments()
            }
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") {
                        dismiss()
                    }
                }
            }
        }
    }
}

private struct WatchConfigurationSheet: View {
    @Environment(\.dismiss) private var dismiss
    @ObservedObject var db: EmbeddedDB

    @State private var collection = "users"
    @State private var loadLimitText = "100"
    @State private var mode = "document"
    @State private var propertyScope = "single"
    @State private var documents: [EmbeddedDB.EditableDocument] = []
    @State private var selectedDocumentID: UInt64?
    @State private var propertyPath = "name"
    @State private var queryJSON = #"{"city":"New York"}"#
    @State private var validationError = ""

    private var loadLimit: Int {
        max(Int(loadLimitText) ?? 100, 1)
    }

    private var queryValid: Bool {
        guard let data = queryJSON.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data) else {
            return false
        }
        return obj is [String: Any]
    }

    private var canAddRule: Bool {
        switch mode {
        case "document":
            return selectedDocumentID != nil
        case "property":
            if propertyScope == "single" {
                return selectedDocumentID != nil && !propertyPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            }
            return !propertyPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        case "query":
            return queryValid
        default:
            return false
        }
    }

    private func loadDocuments() {
        documents = db.listDocumentsForEditing(collection: collection, limit: loadLimit)
        if let first = documents.first, selectedDocumentID == nil {
            selectedDocumentID = first.id
        } else if let selectedDocumentID,
                  !documents.contains(where: { $0.id == selectedDocumentID }) {
            self.selectedDocumentID = documents.first?.id
        }
    }

    private func addRule() {
        validationError = ""

        switch mode {
        case "document":
            guard let docID = selectedDocumentID else {
                validationError = "Pick a document first."
                return
            }
            db.addDocumentWatchRule(collection: collection, documentID: docID)
        case "property":
            let path = propertyPath.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !path.isEmpty else {
                validationError = "Property path is required."
                return
            }
            if propertyScope == "single" {
                guard let docID = selectedDocumentID else {
                    validationError = "Pick a document first."
                    return
                }
                db.addPropertyWatchRule(collection: collection, documentID: docID, propertyPath: path)
            } else {
                db.addPropertyWatchRule(collection: collection, documentID: nil, propertyPath: path)
            }
        case "query":
            guard queryValid else {
                validationError = "Query JSON must be a valid JSON object."
                return
            }
            db.addQueryWatchRule(collection: collection, queryJSON: queryJSON)
        default:
            validationError = "Invalid watch mode."
        }
    }

    var body: some View {
        NavigationView {
            VStack(alignment: .leading, spacing: 12) {
                Picker("Collection", selection: $collection) {
                    Text("users").tag("users")
                    Text("articles").tag("articles")
                }
                .pickerStyle(.segmented)
                .onChange(of: collection) { _ in
                    selectedDocumentID = nil
                    loadDocuments()
                }

                HStack(spacing: 8) {
                    TextField("Load limit", text: $loadLimitText)
                        .keyboardType(.numberPad)
                        .textFieldStyle(.roundedBorder)
                        .frame(width: 110)
                    Button("Load Documents") { loadDocuments() }
                        .buttonStyle(.bordered)
                }

                if documents.isEmpty {
                    Text("No documents loaded for this collection.")
                        .font(.caption)
                        .foregroundColor(.secondary)
                } else {
                    Text("Pick Document")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    ScrollView {
                        LazyVStack(alignment: .leading, spacing: 6) {
                            ForEach(documents) { doc in
                                Button {
                                    selectedDocumentID = doc.id
                                } label: {
                                    HStack {
                                        Text("#\(doc.id)")
                                            .font(.caption.monospaced())
                                        Text(doc.summary)
                                            .font(.caption)
                                            .lineLimit(1)
                                        Spacer()
                                        if selectedDocumentID == doc.id {
                                            Image(systemName: "checkmark.circle.fill")
                                                .foregroundColor(.green)
                                        }
                                    }
                                    .padding(8)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .background(
                                        RoundedRectangle(cornerRadius: 8)
                                            .fill(selectedDocumentID == doc.id ? Color.blue.opacity(0.12) : Color.secondary.opacity(0.08))
                                    )
                                }
                                .buttonStyle(.plain)
                            }
                        }
                    }
                    .frame(maxHeight: 140)
                }

                Picker("Watch Mode", selection: $mode) {
                    Text("Document").tag("document")
                    Text("Property").tag("property")
                    Text("New Docs (Query)").tag("query")
                }
                .pickerStyle(.segmented)

                if mode == "property" {
                    Picker("Scope", selection: $propertyScope) {
                        Text("Selected Doc").tag("single")
                        Text("Any Document").tag("any")
                    }
                    .pickerStyle(.segmented)
                    TextField("Property path (e.g. name or profile.name)", text: $propertyPath)
                        .textFieldStyle(.roundedBorder)
                }

                if mode == "query" {
                    Text("Criteria JSON for new documents")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    TextEditor(text: $queryJSON)
                        .font(.system(.caption, design: .monospaced))
                        .frame(minHeight: 100)
                        .overlay(
                            RoundedRectangle(cornerRadius: 8)
                                .stroke(Color.secondary.opacity(0.3), lineWidth: 1)
                        )
                    if !queryValid {
                        Text("Query JSON must be a valid JSON object.")
                            .font(.caption)
                            .foregroundColor(.red)
                    }
                }

                HStack(spacing: 8) {
                    Button("Add Watch Rule") { addRule() }
                        .buttonStyle(.borderedProminent)
                        .disabled(!canAddRule)
                    Button("Refresh Rules") { db.loadWatchRules() }
                        .buttonStyle(.bordered)
                }

                if !validationError.isEmpty {
                    Text(validationError)
                        .font(.caption)
                        .foregroundColor(.red)
                }

                Text("Stored Rules (_config)")
                    .font(.caption)
                    .foregroundColor(.secondary)
                if db.watchRules.isEmpty {
                    Text("No active watch rules.")
                        .font(.caption)
                        .foregroundColor(.secondary)
                } else {
                    ScrollView {
                        LazyVStack(alignment: .leading, spacing: 6) {
                            ForEach(db.watchRules) { rule in
                                HStack(alignment: .top) {
                                    VStack(alignment: .leading, spacing: 2) {
                                        Text(rule.name)
                                            .font(.caption.weight(.semibold))
                                        Text("collection=\(rule.collection), mode=\(rule.mode)")
                                            .font(.caption2)
                                            .foregroundColor(.secondary)
                                        if let docID = rule.documentID {
                                            Text("doc=#\(docID)")
                                                .font(.caption2)
                                                .foregroundColor(.secondary)
                                        }
                                        if let path = rule.propertyPath {
                                            Text("path=\(path)")
                                                .font(.caption2)
                                                .foregroundColor(.secondary)
                                        }
                                        if let query = rule.query,
                                           let data = try? JSONSerialization.data(withJSONObject: query, options: [.sortedKeys]),
                                           let json = String(data: data, encoding: .utf8) {
                                            Text("query=\(json)")
                                                .font(.system(size: 10, weight: .regular, design: .monospaced))
                                                .foregroundColor(.secondary)
                                                .lineLimit(2)
                                        }
                                    }
                                    Spacer()
                                    Button("Delete") { db.removeWatchRule(rule.id) }
                                        .buttonStyle(.bordered)
                                        .font(.caption2)
                                }
                                .padding(8)
                                .background(
                                    RoundedRectangle(cornerRadius: 8)
                                        .fill(Color.secondary.opacity(0.08))
                                )
                            }
                        }
                    }
                    .frame(maxHeight: 180)
                }

                Text("When a rule is triggered, the app logs `WATCH TRIGGER { ... }` with operation, metadata, and matched document.")
                    .font(.caption2)
                    .foregroundColor(.secondary)
                if mode == "property" {
                    Text("Tip: For users collection, use `name` to watch all name changes. `user.name` is also accepted.")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                }

                Spacer()
            }
            .padding()
            .navigationTitle("Watch Configuration")
            .navigationBarTitleDisplayMode(.inline)
            .onAppear {
                db.loadWatchRules()
                loadDocuments()
            }
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Close") { dismiss() }
                }
            }
        }
    }
}
