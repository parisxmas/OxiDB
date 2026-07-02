"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.activate = activate;
exports.deactivate = deactivate;
const vscode = require("vscode");
const client_1 = require("./client");
const treeView_1 = require("./treeView");
const documentView_1 = require("./documentView");
let client = null;
let treeProvider;
let sqlTreeProvider;
let docView = null;
let sqlResults;
let statusBar;
function setConnected(connected) {
    vscode.commands.executeCommand('setContext', 'oxidb.connected', connected);
    if (connected && client) {
        statusBar.text = `$(database) OxiDB: ${client.address}`;
        statusBar.tooltip = 'Connected to OxiDB — click to disconnect';
        statusBar.command = 'oxidb.disconnect';
    }
    else {
        statusBar.text = '$(database) OxiDB: disconnected';
        statusBar.tooltip = 'Click to connect to OxiDB';
        statusBar.command = 'oxidb.connect';
    }
}
function requireClient() {
    if (!client || !client.isConnected()) {
        vscode.window.showWarningMessage('Connect to OxiDB first');
        return null;
    }
    return client;
}
/**
 * Extract SQL to run from the active editor: the selection if non-empty,
 * else the whole document. A leading `-- params: [...]` comment binds
 * positional parameters.
 */
function extractSql(editor) {
    const raw = editor.selection.isEmpty
        ? editor.document.getText()
        : editor.document.getText(editor.selection);
    let params = [];
    const m = raw.match(/^\s*--\s*params:\s*(\[.*\])\s*$/m);
    if (m) {
        try {
            params = JSON.parse(m[1]);
        }
        catch {
            throw new Error(`Invalid JSON in "-- params:" line: ${m[1]}`);
        }
    }
    // Strip line comments; the server parses the rest.
    const sql = raw.replace(/^\s*--.*$/gm, '').trim();
    return { sql, params };
}
function activate(context) {
    treeProvider = new treeView_1.CollectionTreeProvider();
    sqlTreeProvider = new treeView_1.SqlTreeProvider();
    sqlResults = new documentView_1.SqlResultsView();
    vscode.window.registerTreeDataProvider('oxidb.collections', treeProvider);
    vscode.window.registerTreeDataProvider('oxidb.sql', sqlTreeProvider);
    statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
    context.subscriptions.push(statusBar);
    setConnected(false);
    statusBar.show();
    // ─── Connection ───────────────────────────────────────────
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.connect', async () => {
        const config = vscode.workspace.getConfiguration('oxidb');
        const host = config.get('host', '127.0.0.1');
        const port = config.get('port', 4444);
        const hostInput = await vscode.window.showInputBox({ prompt: 'OxiDB Host', value: host });
        if (!hostInput) {
            return;
        }
        const portInput = await vscode.window.showInputBox({
            prompt: 'OxiDB Port',
            value: String(port),
        });
        if (!portInput) {
            return;
        }
        try {
            client = new client_1.OxiDBClient(hostInput, parseInt(portInput));
            await client.connect();
            const pong = await client.ping();
            vscode.window.showInformationMessage(`Connected to OxiDB (${pong})`);
            client.onClose = () => {
                client = null;
                treeProvider.setClient(null);
                sqlTreeProvider.setClient(null);
                docView = null;
                setConnected(false);
                vscode.window.showWarningMessage('OxiDB connection lost');
            };
            treeProvider.setClient(client);
            sqlTreeProvider.setClient(client);
            docView = new documentView_1.DocumentViewProvider(client);
            setConnected(true);
        }
        catch (e) {
            vscode.window.showErrorMessage(`Failed to connect: ${e.message}`);
            client = null;
            setConnected(false);
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.disconnect', () => {
        if (client) {
            client.disconnect();
            client = null;
            treeProvider.setClient(null);
            sqlTreeProvider.setClient(null);
            docView = null;
            setConnected(false);
            vscode.window.showInformationMessage('Disconnected from OxiDB');
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.refreshCollections', () => {
        treeProvider.refresh();
    }));
    // ─── Document engine ──────────────────────────────────────
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.viewCollection', async (item) => {
        if (!requireClient() || !docView) {
            return;
        }
        await docView.showCollection(item.name);
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.newQuery', async () => {
        const c = requireClient();
        if (!c) {
            return;
        }
        const collections = await c.listCollections();
        const coll = await vscode.window.showQuickPick(collections.filter((x) => !x.startsWith('_')), { placeHolder: 'Select collection' });
        if (!coll) {
            return;
        }
        const doc = await vscode.workspace.openTextDocument({
            content: `// OxiDB Query — Collection: ${coll}\n// Run with "OxiDB: Run Query"\n\n// Find:\n{"cmd": "find", "collection": "${coll}", "query": {}, "limit": 20}\n\n// Aggregate:\n// {"cmd": "aggregate", "collection": "${coll}", "pipeline": [\n//   {"$group": {"_id": "$field", "count": {"$sum": 1}}}\n// ]}\n`,
            language: 'json',
        });
        await vscode.window.showTextDocument(doc);
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.runQuery', async () => {
        const c = requireClient();
        if (!c || !docView) {
            return;
        }
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            return;
        }
        let text = editor.document.getText(editor.selection.isEmpty ? undefined : editor.selection);
        text = text.replace(/\/\/.*$/gm, '').trim();
        if (!text) {
            return;
        }
        try {
            const payload = JSON.parse(text);
            const start = Date.now();
            const result = await c.send(payload);
            const elapsed = Date.now() - start;
            if (result.ok) {
                await docView.showQueryResults(`Results (${elapsed}ms)`, result.data);
            }
            else {
                vscode.window.showErrorMessage(`OxiDB Error: ${result.error}`);
            }
        }
        catch (e) {
            vscode.window.showErrorMessage(`Invalid JSON: ${e.message}`);
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.dropCollection', async (item) => {
        const c = requireClient();
        if (!c) {
            return;
        }
        const confirm = await vscode.window.showWarningMessage(`Drop collection "${item.name}"? This cannot be undone.`, { modal: true }, 'Drop');
        if (confirm === 'Drop') {
            await c.dropCollection(item.name);
            vscode.window.showInformationMessage(`Dropped "${item.name}"`);
            treeProvider.refresh();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.createIndex', async (item) => {
        const c = requireClient();
        if (!c) {
            return;
        }
        const field = await vscode.window.showInputBox({
            prompt: `Create index on "${item.name}" — field name`,
            placeHolder: 'e.g. email, status, created_at',
        });
        if (!field) {
            return;
        }
        await c.createIndex(item.name, field);
        vscode.window.showInformationMessage(`Index created on "${item.name}.${field}"`);
        treeProvider.refresh();
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.insertDocument', async (item) => {
        const c = requireClient();
        if (!c) {
            return;
        }
        const input = await vscode.window.showInputBox({
            prompt: `Insert document into "${item.name}"`,
            placeHolder: '{"name": "Alice", "age": 30}',
        });
        if (!input) {
            return;
        }
        try {
            const doc = JSON.parse(input);
            await c.insert(item.name, doc);
            vscode.window.showInformationMessage('Document inserted');
            treeProvider.refresh();
        }
        catch (e) {
            vscode.window.showErrorMessage(`Insert failed: ${e.message}`);
        }
    }));
    // ─── SQL engine ───────────────────────────────────────────
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.refreshSql', () => {
        sqlTreeProvider.refresh();
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.newSqlQuery', async () => {
        const doc = await vscode.workspace.openTextDocument({
            content: `-- OxiDB SQL — run with Cmd/Ctrl+Enter (selection or whole file)
-- Bind ? / $N placeholders with a params line:
-- params: []

SHOW TABLES;
`,
            language: 'sql',
        });
        await vscode.window.showTextDocument(doc);
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.runSql', async () => {
        const c = requireClient();
        if (!c) {
            return;
        }
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            return;
        }
        try {
            const { sql, params } = extractSql(editor);
            if (!sql) {
                return;
            }
            const start = Date.now();
            const results = await c.sql(sql, params);
            const elapsed = Date.now() - start;
            sqlResults.show(sql, results, elapsed);
            // DDL/DML may have changed the catalog; keep the tree fresh.
            if (results.some((r) => !('columns' in r))) {
                sqlTreeProvider.refresh();
            }
        }
        catch (e) {
            vscode.window.showErrorMessage(`OxiDB SQL: ${e.message}`);
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.sqlSelectTop', async (item) => {
        const c = requireClient();
        if (!c) {
            return;
        }
        try {
            const sql = `SELECT * FROM ${item.name} LIMIT 100`;
            const start = Date.now();
            const results = await c.sql(sql);
            sqlResults.show(sql, results, Date.now() - start);
        }
        catch (e) {
            vscode.window.showErrorMessage(`OxiDB SQL: ${e.message}`);
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.sqlDropTable', async (item) => {
        const c = requireClient();
        if (!c) {
            return;
        }
        const kind = item.kind === 'view' ? 'VIEW' : 'TABLE';
        const confirm = await vscode.window.showWarningMessage(`Drop ${kind.toLowerCase()} "${item.name}"? This cannot be undone.`, { modal: true }, 'Drop');
        if (confirm !== 'Drop') {
            return;
        }
        try {
            await c.sql(`DROP ${kind} ${item.name}`);
            vscode.window.showInformationMessage(`Dropped ${kind.toLowerCase()} "${item.name}"`);
            sqlTreeProvider.refresh();
        }
        catch (e) {
            vscode.window.showErrorMessage(`OxiDB SQL: ${e.message}`);
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('oxidb.sqlDescribe', async (item) => {
        const c = requireClient();
        if (!c) {
            return;
        }
        try {
            const sql = `DESCRIBE ${item.name}`;
            const start = Date.now();
            const results = await c.sql(sql);
            sqlResults.show(sql, results, Date.now() - start);
        }
        catch (e) {
            vscode.window.showErrorMessage(`OxiDB SQL: ${e.message}`);
        }
    }));
    // ─── Auto-connect ─────────────────────────────────────────
    const config = vscode.workspace.getConfiguration('oxidb');
    if (config.get('autoConnect', false)) {
        vscode.commands.executeCommand('oxidb.connect');
    }
}
function deactivate() {
    if (client) {
        client.disconnect();
    }
}
//# sourceMappingURL=extension.js.map