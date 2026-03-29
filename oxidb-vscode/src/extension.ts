import * as vscode from 'vscode';
import { OxiDBClient } from './client';
import { CollectionTreeProvider, CollectionItem } from './treeView';
import { DocumentViewProvider } from './documentView';

let client: OxiDBClient | null = null;
let treeProvider: CollectionTreeProvider;
let docView: DocumentViewProvider | null = null;

export function activate(context: vscode.ExtensionContext) {
  treeProvider = new CollectionTreeProvider();
  vscode.window.registerTreeDataProvider('oxidb.collections', treeProvider);

  // Connect
  context.subscriptions.push(
    vscode.commands.registerCommand('oxidb.connect', async () => {
      const config = vscode.workspace.getConfiguration('oxidb');
      const host = config.get<string>('host', '127.0.0.1');
      const port = config.get<number>('port', 4444);

      const hostInput = await vscode.window.showInputBox({
        prompt: 'OxiDB Host',
        value: host,
      });
      if (!hostInput) { return; }

      const portInput = await vscode.window.showInputBox({
        prompt: 'OxiDB Port',
        value: String(port),
      });
      if (!portInput) { return; }

      try {
        client = new OxiDBClient(hostInput, parseInt(portInput));
        await client.connect();
        const pong = await client.ping();
        vscode.window.showInformationMessage(`Connected to OxiDB (${pong})`);
        treeProvider.setClient(client);
        docView = new DocumentViewProvider(client);
      } catch (e: any) {
        vscode.window.showErrorMessage(`Failed to connect: ${e.message}`);
        client = null;
      }
    })
  );

  // Disconnect
  context.subscriptions.push(
    vscode.commands.registerCommand('oxidb.disconnect', () => {
      if (client) {
        client.disconnect();
        client = null;
        treeProvider.setClient(null);
        docView = null;
        vscode.window.showInformationMessage('Disconnected from OxiDB');
      }
    })
  );

  // Refresh
  context.subscriptions.push(
    vscode.commands.registerCommand('oxidb.refreshCollections', () => {
      treeProvider.refresh();
    })
  );

  // View collection documents
  context.subscriptions.push(
    vscode.commands.registerCommand('oxidb.viewCollection', async (item: CollectionItem) => {
      if (!client || !docView) {
        vscode.window.showWarningMessage('Connect to OxiDB first');
        return;
      }
      await docView.showCollection(item.name);
    })
  );

  // New query
  context.subscriptions.push(
    vscode.commands.registerCommand('oxidb.newQuery', async () => {
      if (!client) {
        vscode.window.showWarningMessage('Connect to OxiDB first');
        return;
      }

      const collections = await client.listCollections();
      const coll = await vscode.window.showQuickPick(
        collections.filter((c) => !c.startsWith('_')),
        { placeHolder: 'Select collection' }
      );
      if (!coll) { return; }

      const doc = await vscode.workspace.openTextDocument({
        content: `// OxiDB Query — Collection: ${coll}\n// Press Ctrl+Shift+P → "OxiDB: Run Query" to execute\n\n// Find:\n{"cmd": "find", "collection": "${coll}", "query": {}, "limit": 20}\n\n// Aggregate:\n// {"cmd": "aggregate", "collection": "${coll}", "pipeline": [\n//   {"$group": {"_id": "$field", "count": {"$sum": 1}}}\n// ]}\n`,
        language: 'json',
      });
      await vscode.window.showTextDocument(doc);
    })
  );

  // Run query from editor
  context.subscriptions.push(
    vscode.commands.registerCommand('oxidb.runQuery', async () => {
      if (!client || !docView) {
        vscode.window.showWarningMessage('Connect to OxiDB first');
        return;
      }

      const editor = vscode.window.activeTextEditor;
      if (!editor) { return; }

      // Get selected text or entire document
      let text = editor.document.getText(editor.selection.isEmpty ? undefined : editor.selection);

      // Strip comments
      text = text.replace(/\/\/.*$/gm, '').trim();
      if (!text) { return; }

      try {
        const payload = JSON.parse(text);
        const start = Date.now();
        const result = await client.send(payload);
        const elapsed = Date.now() - start;

        if (result.ok) {
          const title = `Results (${elapsed}ms)`;
          await docView.showQueryResults(title, result.data);
        } else {
          vscode.window.showErrorMessage(`OxiDB Error: ${result.error}`);
        }
      } catch (e: any) {
        vscode.window.showErrorMessage(`Invalid JSON: ${e.message}`);
      }
    })
  );

  // Drop collection
  context.subscriptions.push(
    vscode.commands.registerCommand('oxidb.dropCollection', async (item: CollectionItem) => {
      if (!client) { return; }
      const confirm = await vscode.window.showWarningMessage(
        `Drop collection "${item.name}"? This cannot be undone.`,
        { modal: true },
        'Drop'
      );
      if (confirm === 'Drop') {
        await client.dropCollection(item.name);
        vscode.window.showInformationMessage(`Dropped "${item.name}"`);
        treeProvider.refresh();
      }
    })
  );

  // Create index
  context.subscriptions.push(
    vscode.commands.registerCommand('oxidb.createIndex', async (item: CollectionItem) => {
      if (!client) { return; }
      const field = await vscode.window.showInputBox({
        prompt: `Create index on "${item.name}" — field name`,
        placeHolder: 'e.g. email, status, created_at',
      });
      if (!field) { return; }
      await client.createIndex(item.name, field);
      vscode.window.showInformationMessage(`Index created on "${item.name}.${field}"`);
      treeProvider.refresh();
    })
  );

  // Insert document
  context.subscriptions.push(
    vscode.commands.registerCommand('oxidb.insertDocument', async (item: CollectionItem) => {
      if (!client) { return; }
      const input = await vscode.window.showInputBox({
        prompt: `Insert document into "${item.name}"`,
        placeHolder: '{"name": "Alice", "age": 30}',
      });
      if (!input) { return; }
      try {
        const doc = JSON.parse(input);
        await client.insert(item.name, doc);
        vscode.window.showInformationMessage('Document inserted');
        treeProvider.refresh();
      } catch (e: any) {
        vscode.window.showErrorMessage(`Insert failed: ${e.message}`);
      }
    })
  );

  // Auto-connect
  const config = vscode.workspace.getConfiguration('oxidb');
  if (config.get<boolean>('autoConnect', false)) {
    vscode.commands.executeCommand('oxidb.connect');
  }
}

export function deactivate() {
  if (client) {
    client.disconnect();
  }
}
