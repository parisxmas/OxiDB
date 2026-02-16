#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { Worker } = require('worker_threads');
const OxiDBClient = require('./oxidb.js');

// ─── MIME mapping ──────────────────────────────────────────
const EXT_MAP = {
  '.pdf':  'application/pdf',
  '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  '.txt':  'text/plain',
  '.html': 'text/html',
  '.htm':  'text/html',
  '.json': 'application/json',
  '.csv':  'text/csv',
  '.xml':  'application/xml',
  '.md':   'text/markdown',
  '.rtf':  'application/rtf',
};

function getContentType(filename) {
  const ext = path.extname(filename).toLowerCase();
  return EXT_MAP[ext] || 'application/octet-stream';
}

function isSupported(filename) {
  return path.extname(filename).toLowerCase() in EXT_MAP;
}

// ─── CLI arg parsing ───────────────────────────────────────
function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    host: '127.0.0.1',
    port: 4444,
    collection: null,
    file: null,
    dir: null,
    manifest: null,
    meta: null,
    concurrency: 2,
    create: false,
    schema: null,
  };

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--host':    case '-h': opts.host = args[++i]; break;
      case '--port':    case '-p': opts.port = parseInt(args[++i], 10); break;
      case '--collection': case '-c': opts.collection = args[++i]; break;
      case '--file':    case '-f': opts.file = args[++i]; break;
      case '--dir':     case '-d': opts.dir = args[++i]; break;
      case '--manifest':case '-m': opts.manifest = args[++i]; break;
      case '--meta':               opts.meta = args[++i]; break;
      case '--concurrency':        opts.concurrency = parseInt(args[++i], 10); break;
      case '--create':             opts.create = true; break;
      case '--schema':  case '-s': opts.schema = args[++i]; break;
      case '--help':
        printHelp();
        process.exit(0);
      default:
        // Treat bare args as files
        if (!args[i].startsWith('-') && !opts.file && !opts.dir) {
          opts.file = args[i];
        }
    }
  }
  return opts;
}

function printHelp() {
  console.log(`
docdb-index — Standalone document indexer for OxiDB

USAGE:
  docdb-index --collection <name> --file <path>         Index a single file
  docdb-index --collection <name> --dir <path>           Index all files in directory
  docdb-index --collection <name> --dir <path> -m <manifest.json>
                                                         Index with metadata from manifest

OPTIONS:
  -h, --host <addr>        OxiDB server address  (default: 127.0.0.1)
  -p, --port <port>        OxiDB server port     (default: 4444)
  -c, --collection <name>  Target collection name (required)
  -f, --file <path>        Single file to index
  -d, --dir <path>         Directory of files to index
  -m, --manifest <path>    JSON manifest mapping filenames to metadata
      --meta <json>        Inline metadata JSON (for single file mode)
      --concurrency <n>    Parallel workers      (default: 2)
      --create             Create collection + bucket if they don't exist
  -s, --schema <path>      Schema JSON file (used with --create)
      --help               Show this help

SCHEMA FILE FORMAT (schema.json):
  {
    "fields": [
      { "name": "title",    "type": "string",  "indexed": true  },
      { "name": "author",   "type": "string",  "indexed": true  },
      { "name": "year",     "type": "number",  "indexed": true  },
      { "name": "category", "type": "string",  "indexed": false }
    ]
  }

MANIFEST FILE FORMAT (manifest.json):
  {
    "report.pdf":  { "title": "Q1 Report",  "author": "Alice", "year": 2024 },
    "paper.pdf":   { "title": "Research",   "author": "Bob",   "year": 2023 }
  }
  Files in --dir not listed in the manifest are indexed with empty metadata.

EXAMPLES:
  # Index a single PDF with metadata
  docdb-index -c research_papers -f paper.pdf --meta '{"title":"My Paper","author":"Me"}'

  # Index all files in a folder
  docdb-index -c research_papers -d ./documents/

  # Create collection first, then batch index with manifest
  docdb-index -c invoices --create -s schema.json -d ./invoices/ -m manifest.json

  # Index into remote OxiDB
  docdb-index -h 192.168.1.50 -p 4444 -c reports -d ./reports/
`);
}

// ─── Dot-notation expansion ────────────────────────────────
function expandDotFields(flat) {
  const result = {};
  for (const [key, value] of Object.entries(flat)) {
    const parts = key.split('.');
    let current = result;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!(parts[i] in current) || typeof current[parts[i]] !== 'object') {
        current[parts[i]] = {};
      }
      current = current[parts[i]];
    }
    current[parts[parts.length - 1]] = value;
  }
  return result;
}

function coerceValue(value, type) {
  if (value === '' || value == null) return null;
  switch (type) {
    case 'number': { const n = Number(value); return isNaN(n) ? null : n; }
    case 'boolean': return value === 'true' || value === true;
    case 'date': return value;
    default: return String(value);
  }
}

// ─── Main ──────────────────────────────────────────────────
async function main() {
  const opts = parseArgs();

  if (!opts.collection) {
    console.error('Error: --collection is required. Use --help for usage.');
    process.exit(1);
  }
  if (!opts.file && !opts.dir) {
    console.error('Error: --file or --dir is required. Use --help for usage.');
    process.exit(1);
  }

  // Connect to OxiDB
  const db = new OxiDBClient(opts.host, opts.port);
  console.log(`Connecting to OxiDB at ${opts.host}:${opts.port}...`);
  await db.connect();
  await db.ping();
  console.log('Connected.');

  const bucket = `${opts.collection}_files`;

  // Load schema if provided
  let fields = [];
  if (opts.schema) {
    const schemaData = JSON.parse(fs.readFileSync(opts.schema, 'utf8'));
    fields = schemaData.fields || [];
  } else {
    // Try to fetch schema from _schemas collection
    try {
      const schema = await db.findOne('_schemas', { collectionName: opts.collection });
      if (schema) {
        fields = schema.fields || [];
        console.log(`Loaded schema from _schemas (${fields.length} fields)`);
      }
    } catch (e) {
      // _schemas may not exist
    }
  }

  // Create collection + bucket if requested
  if (opts.create) {
    try {
      await db.createCollection(opts.collection);
      console.log(`Created collection: ${opts.collection}`);
    } catch (e) {
      console.log(`Collection "${opts.collection}" already exists`);
    }

    try {
      await db.createBucket(bucket);
      console.log(`Created bucket: ${bucket}`);
    } catch (e) {
      console.log(`Bucket "${bucket}" already exists`);
    }

    // Create indexes
    for (const field of fields) {
      if (field.indexed) {
        try {
          await db.createIndex(opts.collection, field.name);
        } catch (e) { /* exists */ }
      }
    }

    // Store schema in _schemas
    if (fields.length > 0) {
      try {
        await db.createCollection('_schemas');
      } catch (e) { /* exists */ }
      try {
        await db.createUniqueIndex('_schemas', 'collectionName');
      } catch (e) { /* exists */ }
      try {
        await db.insert('_schemas', {
          collectionName: opts.collection,
          fields,
          createdAt: new Date().toISOString(),
        });
        console.log('Schema stored in _schemas');
      } catch (e) {
        console.log('Schema already in _schemas (or duplicate)');
      }
    }
  }

  // Load manifest
  let manifest = {};
  if (opts.manifest) {
    manifest = JSON.parse(fs.readFileSync(opts.manifest, 'utf8'));
    console.log(`Loaded manifest with ${Object.keys(manifest).length} entries`);
  }

  // Gather files
  let files = [];
  if (opts.file) {
    const filePath = path.resolve(opts.file);
    files.push({
      path: filePath,
      name: path.basename(filePath),
      meta: opts.meta ? JSON.parse(opts.meta) : manifest[path.basename(filePath)] || {},
    });
  } else {
    const dirPath = path.resolve(opts.dir);
    const entries = fs.readdirSync(dirPath);
    for (const entry of entries) {
      const fullPath = path.join(dirPath, entry);
      const stat = fs.statSync(fullPath);
      if (!stat.isFile()) continue;
      if (!isSupported(entry)) {
        console.log(`  Skipping unsupported: ${entry}`);
        continue;
      }
      if (stat.size > 12 * 1024 * 1024) {
        console.log(`  Skipping too large (>12MB): ${entry}`);
        continue;
      }
      files.push({
        path: fullPath,
        name: entry,
        meta: manifest[entry] || {},
      });
    }
  }

  if (files.length === 0) {
    console.log('No files to index.');
    await db.close();
    return;
  }

  // Close main connection to free a pool slot for workers
  await db.close();

  console.log(`\nIndexing ${files.length} file(s) into "${opts.collection}"...\n`);

  // Spin up worker pool
  const concurrency = Math.min(opts.concurrency, files.length);
  const workers = [];
  const pending = new Map();
  let completed = 0;
  let errors = 0;
  let fileIndex = 0;

  await new Promise((resolveAll) => {
    function onDone() {
      if (completed + errors === files.length) {
        for (const w of workers) w.terminate();
        resolveAll();
      }
    }

    for (let i = 0; i < concurrency; i++) {
      const w = new Worker(path.join(__dirname, 'worker.js'), {
        workerData: { host: opts.host, port: opts.port },
      });

      w.on('message', (msg) => {
        const info = pending.get(msg.jobId);
        if (msg.status === 'completed') {
          completed++;
          console.log(`  [${completed + errors}/${files.length}] OK   ${info?.name} → id:${msg.documentId}`);
        } else {
          errors++;
          console.error(`  [${completed + errors}/${files.length}] FAIL ${info?.name} → ${msg.error}`);
        }
        pending.delete(msg.jobId);

        // Feed next file
        if (fileIndex < files.length) {
          sendFile(w, files[fileIndex++]);
        }
        onDone();
      });

      w.on('error', (err) => {
        console.error('Worker error:', err.message);
      });

      workers.push(w);

      // Seed initial file
      if (fileIndex < files.length) {
        sendFile(w, files[fileIndex++]);
      }
    }

    function sendFile(w, file) {
      const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      const raw = fs.readFileSync(file.path);
      // Copy into a fresh ArrayBuffer so it's transferable
      const ab = new ArrayBuffer(raw.length);
      new Uint8Array(ab).set(raw);
      const contentType = getContentType(file.name);
      const key = `${Date.now()}_${file.name}`;

      pending.set(jobId, file);

      w.postMessage({
        jobId,
        collection: opts.collection,
        bucket,
        key,
        fileBuffer: ab,
        contentType,
        originalName: file.name,
        fields,
        metadata: file.meta,
      }, [ab]);
    }
  });

  console.log(`\nDone. ${completed} indexed, ${errors} failed.`);
  await db.close();
}

main().catch((err) => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
