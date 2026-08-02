"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SqlTreeProvider = exports.SqlItem = exports.CollectionTreeProvider = exports.CollectionItem = void 0;
const vscode = require("vscode");
// ─── Document engine: collections ──────────────────────────────────────────
class CollectionItem extends vscode.TreeItem {
    constructor(name, docCount, itemType) {
        super(name, itemType === 'collection'
            ? vscode.TreeItemCollapsibleState.Collapsed
            : vscode.TreeItemCollapsibleState.None);
        this.name = name;
        this.docCount = docCount;
        this.itemType = itemType;
        if (itemType === 'message') {
            this.iconPath = new vscode.ThemeIcon('info');
            this.contextValue = 'message';
        }
        else if (itemType === 'collection') {
            this.description = `${docCount} docs`;
            this.iconPath = new vscode.ThemeIcon('symbol-namespace');
            this.contextValue = 'collection';
            this.command = {
                command: 'oxidb.viewCollection',
                title: 'View Documents',
                arguments: [this],
            };
        }
        else {
            this.iconPath = new vscode.ThemeIcon('key');
            this.contextValue = 'index';
        }
    }
}
exports.CollectionItem = CollectionItem;
class CollectionTreeProvider {
    constructor() {
        this._onDidChangeTreeData = new vscode.EventEmitter();
        this.onDidChangeTreeData = this._onDidChangeTreeData.event;
        this.client = null;
        this.collections = new Map();
        this.indexes = new Map();
    }
    setClient(client) {
        this.client = client;
        this.refresh();
    }
    async refresh() {
        if (!this.client || !this.client.isConnected()) {
            this.collections.clear();
            this.indexes.clear();
            this._onDidChangeTreeData.fire(undefined);
            return;
        }
        try {
            const names = await this.client.listCollections();
            this.collections.clear();
            this.indexes.clear();
            for (const name of names) {
                if (name.startsWith('_')) {
                    continue;
                } // skip system collections
                const count = await this.client.count(name);
                this.collections.set(name, count);
                try {
                    const idx = await this.client.listIndexes(name);
                    this.indexes.set(name, idx);
                }
                catch {
                    this.indexes.set(name, []);
                }
            }
        }
        catch (e) {
            console.error('Failed to refresh collections:', e);
        }
        this._onDidChangeTreeData.fire(undefined);
    }
    getTreeItem(element) {
        return element;
    }
    async getChildren(element) {
        if (!this.client || !this.client.isConnected()) {
            return [];
        }
        if (!element) {
            const items = [];
            for (const [name, count] of this.collections) {
                items.push(new CollectionItem(name, count, 'collection'));
            }
            if (!items.length) {
                // Never return an empty root while connected — that would show the
                // view's "Not connected" welcome content.
                return [new CollectionItem('No collections yet', 0, 'message')];
            }
            return items.sort((a, b) => a.name.localeCompare(b.name));
        }
        // Children of a collection: its indexes.
        const indexes = this.indexes.get(element.name) || [];
        return indexes.map((idx) => new CollectionItem(`${idx.name || idx.field || 'index'} (${idx.index_type || 'field'})`, 0, 'index'));
    }
}
exports.CollectionTreeProvider = CollectionTreeProvider;
class SqlItem extends vscode.TreeItem {
    constructor(name, kind, label, description, tooltip) {
        super(label, kind === 'table' || kind === 'view'
            ? vscode.TreeItemCollapsibleState.Collapsed
            : vscode.TreeItemCollapsibleState.None);
        this.name = name;
        this.kind = kind;
        this.description = description;
        if (tooltip) {
            this.tooltip = tooltip;
        }
        this.contextValue = `sql-${kind}`;
        switch (kind) {
            case 'table':
                this.iconPath = new vscode.ThemeIcon('table');
                this.command = {
                    command: 'oxidb.sqlSelectTop',
                    title: 'Select Top 100',
                    arguments: [this],
                };
                break;
            case 'view':
                this.iconPath = new vscode.ThemeIcon('eye');
                this.command = {
                    command: 'oxidb.sqlSelectTop',
                    title: 'Select Top 100',
                    arguments: [this],
                };
                break;
            case 'column':
                this.iconPath = new vscode.ThemeIcon('symbol-field');
                break;
            case 'index':
                this.iconPath = new vscode.ThemeIcon('key');
                break;
            case 'message':
                this.iconPath = new vscode.ThemeIcon('info');
                break;
        }
    }
}
exports.SqlItem = SqlItem;
class SqlTreeProvider {
    constructor() {
        this._onDidChangeTreeData = new vscode.EventEmitter();
        this.onDidChangeTreeData = this._onDidChangeTreeData.event;
        this.client = null;
    }
    setClient(client) {
        this.client = client;
        this.refresh();
    }
    refresh() {
        this._onDidChangeTreeData.fire(undefined);
    }
    getTreeItem(element) {
        return element;
    }
    async getChildren(element) {
        if (!this.client || !this.client.isConnected()) {
            return [];
        }
        try {
            if (!element) {
                const enabled = await this.client.sqlEnabled();
                if (!enabled) {
                    return [new SqlItem('', 'message', 'SQL engine disabled', 'set OXIDB_SQL=1 on the server')];
                }
                const [tables, views] = await Promise.all([
                    this.client.sqlTables(),
                    this.client.sqlViews(),
                ]);
                if (!tables.length && !views.length) {
                    // Never return an empty root while connected — that would show the
                    // view's "Not connected" welcome content.
                    return [new SqlItem('', 'message', 'No tables yet', 'create one with OxiDB: New SQL Query')];
                }
                return [
                    ...tables.map((t) => new SqlItem(t.name, 'table', t.name, t.rows === null ? undefined : `${t.rows} rows`)),
                    ...views.map((v) => new SqlItem(v.name, 'view', v.name, 'view', v.definition)),
                ];
            }
            if (element.kind === 'table') {
                const [columns, indexes] = await Promise.all([
                    this.client.sqlColumns(element.name),
                    this.client.sqlIndexes(element.name),
                ]);
                return [
                    ...columns.map((c) => {
                        const badges = [c.primaryKey ? 'PK' : '', c.nullable ? '' : 'NOT NULL']
                            .filter(Boolean)
                            .join(' ');
                        return new SqlItem(c.name, 'column', c.name, `${c.type}${badges ? ' · ' + badges : ''}`);
                    }),
                    ...indexes.map((i) => new SqlItem(i.name, 'index', i.name, `(${i.columns})`)),
                ];
            }
            if (element.kind === 'view') {
                const columns = await this.client.sqlColumns(element.name).catch(() => []);
                return columns.map((c) => new SqlItem(c.name, 'column', c.name, c.type));
            }
        }
        catch (e) {
            return [new SqlItem('', 'message', 'Error', String(e.message))];
        }
        return [];
    }
}
exports.SqlTreeProvider = SqlTreeProvider;
//# sourceMappingURL=treeView.js.map