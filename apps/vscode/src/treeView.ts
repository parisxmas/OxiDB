import * as vscode from 'vscode';
import { OxiDBClient } from './client';

// ─── Document engine: collections ──────────────────────────────────────────

export class CollectionItem extends vscode.TreeItem {
  constructor(
    public readonly name: string,
    public readonly docCount: number,
    public readonly itemType: 'collection' | 'index' | 'message'
  ) {
    super(
      name,
      itemType === 'collection'
        ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None
    );

    if (itemType === 'message') {
      this.iconPath = new vscode.ThemeIcon('info');
      this.contextValue = 'message';
    } else if (itemType === 'collection') {
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
      const items: CollectionItem[] = [];
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

// ─── SQL engine: tables / views / columns / indexes ─────────────────────────

type SqlNodeKind = 'table' | 'view' | 'column' | 'index' | 'message';

export class SqlItem extends vscode.TreeItem {
  constructor(
    public readonly name: string,
    public readonly kind: SqlNodeKind,
    label: string,
    description?: string,
    tooltip?: string
  ) {
    super(
      label,
      kind === 'table' || kind === 'view'
        ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None
    );
    this.description = description;
    if (tooltip) { this.tooltip = tooltip; }
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

export class SqlTreeProvider implements vscode.TreeDataProvider<SqlItem> {
  private _onDidChangeTreeData = new vscode.EventEmitter<SqlItem | undefined>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private client: OxiDBClient | null = null;

  setClient(client: OxiDBClient | null): void {
    this.client = client;
    this.refresh();
  }

  refresh(): void {
    this._onDidChangeTreeData.fire(undefined);
  }

  getTreeItem(element: SqlItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: SqlItem): Promise<SqlItem[]> {
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
          ...tables.map(
            (t) => new SqlItem(t.name, 'table', t.name, t.rows === null ? undefined : `${t.rows} rows`)
          ),
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
    } catch (e: any) {
      return [new SqlItem('', 'message', 'Error', String(e.message))];
    }

    return [];
  }
}
