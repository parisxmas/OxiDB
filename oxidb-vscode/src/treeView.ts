import * as vscode from 'vscode';
import { OxiDBClient } from './client';

export class CollectionItem extends vscode.TreeItem {
  constructor(
    public readonly name: string,
    public readonly docCount: number,
    public readonly itemType: 'collection' | 'index'
  ) {
    super(
      name,
      itemType === 'collection'
        ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None
    );

    if (itemType === 'collection') {
      this.description = `${docCount} docs`;
      this.iconPath = new vscode.ThemeIcon('symbol-namespace');
      this.contextValue = 'collection';
      this.command = {
        command: 'oxidb.viewCollection',
        title: 'View Documents',
        arguments: [this],
      };
    } else {
      this.iconPath = new vscode.ThemeIcon('key');
      this.contextValue = 'index';
    }
  }
}

export class CollectionTreeProvider implements vscode.TreeDataProvider<CollectionItem> {
  private _onDidChangeTreeData = new vscode.EventEmitter<CollectionItem | undefined>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private client: OxiDBClient | null = null;
  private collections: Map<string, number> = new Map();
  private indexes: Map<string, any[]> = new Map();

  setClient(client: OxiDBClient | null): void {
    this.client = client;
    this.refresh();
  }

  async refresh(): Promise<void> {
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
        if (name.startsWith('_')) { continue; } // skip system collections
        const count = await this.client.count(name);
        this.collections.set(name, count);
        try {
          const idx = await this.client.listIndexes(name);
          this.indexes.set(name, idx);
        } catch {
          this.indexes.set(name, []);
        }
      }
    } catch (e) {
      console.error('Failed to refresh collections:', e);
    }

    this._onDidChangeTreeData.fire(undefined);
  }

  getTreeItem(element: CollectionItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: CollectionItem): Promise<CollectionItem[]> {
    if (!this.client || !this.client.isConnected()) {
      return [];
    }

    if (!element) {
      // Root level: show collections
      const items: CollectionItem[] = [];
      for (const [name, count] of this.collections) {
        items.push(new CollectionItem(name, count, 'collection'));
      }
      return items.sort((a, b) => a.name.localeCompare(b.name));
    }

    // Children of a collection: show indexes
    const indexes = this.indexes.get(element.name) || [];
    return indexes.map(
      (idx: any) =>
        new CollectionItem(
          `${idx.name || idx.field || 'index'} (${idx.index_type || 'field'})`,
          0,
          'index'
        )
    );
  }
}
