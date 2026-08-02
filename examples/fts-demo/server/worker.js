const { parentPort } = require('worker_threads');
const OxiDBClient = require('./oxidb.js');

const db = new OxiDBClient(
  process.env.OXIDB_HOST || '127.0.0.1',
  parseInt(process.env.OXIDB_PORT || '4444', 10)
);

let connected = false;

async function ensureConnected() {
  if (!connected) {
    await db.connect();
    connected = true;
  }
}

/**
 * Expand dot-notation fields into nested objects.
 * e.g. { "address.city": "NY" } → { address: { city: "NY" } }
 */
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

/**
 * Coerce a string value to the specified type.
 */
function coerceValue(value, type) {
  if (value === '' || value == null) return null;
  switch (type) {
    case 'number': {
      const n = Number(value);
      return isNaN(n) ? null : n;
    }
    case 'boolean':
      return value === 'true' || value === true;
    case 'date':
      return value; // keep as ISO string
    default:
      return String(value);
  }
}

parentPort.on('message', async (job) => {
  const { jobId, collection, bucket, key, fileBuffer, contentType, originalName, fields, metadata } = job;

  try {
    await ensureConnected();

    // 1. Upload file to blob storage (triggers FTS auto-indexing)
    const base64Data = Buffer.from(fileBuffer).toString('base64');
    await db.putObject(bucket, key, base64Data, contentType);

    // 2. Build structured document from metadata
    const flatDoc = {};
    for (const field of fields) {
      const raw = metadata[field.name];
      if (raw != null && raw !== '') {
        flatDoc[field.name] = coerceValue(raw, field.type);
      }
    }

    // Expand dot-notation fields into nested objects
    const doc = expandDotFields(flatDoc);

    // Add file reference and timestamp
    doc._file = {
      bucket,
      key,
      originalName,
      contentType,
      size: fileBuffer.byteLength,
    };
    doc._uploadedAt = new Date().toISOString();

    // 3. Insert structured document
    const result = await db.insert(collection, doc);

    parentPort.postMessage({
      jobId,
      status: 'completed',
      documentId: result.id,
    });
  } catch (err) {
    parentPort.postMessage({
      jobId,
      status: 'error',
      error: err.message,
    });
  }
});
