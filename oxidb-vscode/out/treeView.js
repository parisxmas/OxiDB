"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CollectionTreeProvider = exports.CollectionItem = void 0;
const vscode = require("vscode");
class CollectionItem extends vscode.TreeItem {
    constructor(name, docCount, itemType) {
        super(name, itemType === 'collection'
            ? vscode.TreeItemCollapsibleState.Collapsed
            : vscode.TreeItemCollapsibleState.None);
        this.name = name;
        this.docCount = docCount;
        this.itemType = itemType;
        if (itemType === 'collection') {
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
            // Root level: show collections
            const items = [];
            for (const [name, count] of this.collections) {
                items.push(new CollectionItem(name, count, 'collection'));
            }
            return items.sort((a, b) => a.name.localeCompare(b.name));
        }
        // Children of a collection: show indexes
        const indexes = this.indexes.get(element.name) || [];
        return indexes.map((idx) => new CollectionItem(`${idx.name || idx.field || 'index'} (${idx.index_type || 'field'})`, 0, 'index'));
    }
}
exports.CollectionTreeProvider = CollectionTreeProvider;
//# sourceMappingURL=treeView.js.map