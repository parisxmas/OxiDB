const { Router } = require('express');
const multer = require('multer');
const path = require('path');
const { Worker } = require('worker_threads');
const { getContentType, isAllowed } = require('./content-types.js');

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 12 * 1024 * 1024 }, // 12 MB
});

const SCHEMAS_COLLECTION = '_schemas';

module.exports = function createRouter(db) {
  const router = Router();

  // In-memory job tracker
  const jobs = new Map();

  // Start worker thread
  const worker = new Worker(path.join(__dirname, 'worker.js'));
  worker.on('message', (msg) => {
    const job = jobs.get(msg.jobId);
    if (job) {
      job.status = msg.status;
      if (msg.documentId) job.documentId = msg.documentId;
      if (msg.error) job.error = msg.error;
    }
  });
  worker.on('error', (err) => {
    console.error('Worker error:', err);
  });

  // ─── Collections ────────────────────────────────────────────

  // List all collections with schemas
  router.get('/collections', async (req, res) => {
    try {
      const schemas = await db.find(SCHEMAS_COLLECTION, {});
      res.json(schemas);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Create a new collection
  router.post('/collections', async (req, res) => {
    try {
      const { collectionName, fields } = req.body;
      if (!collectionName || !fields || !Array.isArray(fields)) {
        return res.status(400).json({ error: 'collectionName and fields[] required' });
      }

      // Validate collection name
      if (collectionName.startsWith('_')) {
        return res.status(400).json({ error: 'Collection names starting with _ are reserved' });
      }

      // Create the collection in OxiDB
      await db.createCollection(collectionName);

      // Create blob bucket for file storage
      const bucket = `${collectionName}_files`;
      await db.createBucket(bucket);

      // Create indexes for indexed fields
      for (const field of fields) {
        if (field.indexed) {
          try {
            await db.createIndex(collectionName, field.name);
          } catch (e) {
            // Index may already exist, continue
            console.warn(`Index creation warning for ${field.name}:`, e.message);
          }
        }
      }

      // Store schema
      const schema = {
        collectionName,
        fields,
        createdAt: new Date().toISOString(),
      };
      await db.insert(SCHEMAS_COLLECTION, schema);

      res.status(201).json(schema);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Delete a collection
  router.delete('/collections/:name', async (req, res) => {
    try {
      const name = req.params.name;

      // Drop the collection
      try { await db.dropCollection(name); } catch (e) { /* may not exist */ }

      // Delete the blob bucket
      try { await db.deleteBucket(`${name}_files`); } catch (e) { /* may not exist */ }

      // Remove schema
      await db.delete(SCHEMAS_COLLECTION, { collectionName: name });

      res.json({ deleted: name });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Get schema for a collection
  router.get('/collections/:name/schema', async (req, res) => {
    try {
      const schema = await db.findOne(SCHEMAS_COLLECTION, { collectionName: req.params.name });
      if (!schema) {
        return res.status(404).json({ error: 'Collection not found' });
      }
      res.json(schema);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ─── Documents ──────────────────────────────────────────────

  // Upload document (multipart: file + metadata fields)
  router.post('/collections/:name/documents', upload.single('file'), async (req, res) => {
    try {
      const collection = req.params.name;

      // Get schema
      const schema = await db.findOne(SCHEMAS_COLLECTION, { collectionName: collection });
      if (!schema) {
        return res.status(404).json({ error: 'Collection not found' });
      }

      if (!req.file) {
        return res.status(400).json({ error: 'File is required' });
      }

      if (!isAllowed(req.file.originalname)) {
        return res.status(400).json({ error: 'File type not allowed' });
      }

      const bucket = `${collection}_files`;
      const key = `${Date.now()}_${req.file.originalname}`;
      const contentType = getContentType(req.file.originalname);

      // Parse metadata from form fields
      let metadata = {};
      if (req.body.metadata) {
        try {
          metadata = JSON.parse(req.body.metadata);
        } catch (e) {
          return res.status(400).json({ error: 'Invalid metadata JSON' });
        }
      }

      const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      jobs.set(jobId, { status: 'processing', collection });

      // Send to worker with file buffer as transferable
      const fileBuffer = req.file.buffer;
      worker.postMessage(
        {
          jobId,
          collection,
          bucket,
          key,
          fileBuffer,
          contentType,
          originalName: req.file.originalname,
          fields: schema.fields,
          metadata,
        },
        [fileBuffer.buffer]
      );

      res.status(202).json({ jobId });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Download a file from blob storage
  router.get('/collections/:name/files/:key', async (req, res) => {
    try {
      const bucket = `${req.params.name}_files`;
      const key = req.params.key;
      const obj = await db.getObject(bucket, key);
      const buffer = Buffer.from(obj.content, 'base64');
      const contentType = obj.metadata?.content_type || 'application/octet-stream';
      res.set('Content-Type', contentType);
      res.set('Content-Disposition', `inline; filename="${key.replace(/^\d+_/, '')}"`);
      res.set('Content-Length', buffer.length);
      res.send(buffer);
    } catch (err) {
      res.status(404).json({ error: err.message });
    }
  });

  // List documents (paginated)
  router.get('/collections/:name/documents', async (req, res) => {
    try {
      const collection = req.params.name;
      const skip = parseInt(req.query.skip || '0', 10);
      const limit = parseInt(req.query.limit || '20', 10);

      const docs = await db.find(collection, {}, { skip, limit, sort: { _uploadedAt: -1 } });
      const total = await db.count(collection, {});

      res.json({ docs, total: total.count, skip, limit });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ─── Jobs ───────────────────────────────────────────────────

  router.get('/jobs/:jobId', (req, res) => {
    const job = jobs.get(req.params.jobId);
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }
    res.json(job);
  });

  // ─── Search ─────────────────────────────────────────────────

  router.post('/collections/:name/search', async (req, res) => {
    try {
      const collection = req.params.name;
      const { filters, textQuery, skip = 0, limit = 20 } = req.body;
      const bucket = `${collection}_files`;

      const hasFilters = filters && Object.keys(filters).length > 0;
      const hasText = textQuery && textQuery.trim().length > 0;

      // Mode 1: Structured only
      if (hasFilters && !hasText) {
        const query = buildQuery(filters);
        const docs = await db.find(collection, query, { skip, limit });
        return res.json({ docs, mode: 'structured' });
      }

      // Mode 2: FTS only
      if (!hasFilters && hasText) {
        const ftsResults = await db.search(textQuery, bucket, 500);

        // Only look up the page we need, not all 500
        const pageHits = ftsResults.slice(skip, skip + limit);
        const scoredDocs = await Promise.all(
          pageHits.map(async (hit) => {
            const doc = await db.findOne(collection, { '_file.key': hit.key });
            if (doc) doc._score = hit.score;
            return doc;
          })
        );

        return res.json({
          docs: scoredDocs.filter(Boolean),
          total: ftsResults.length,
          mode: 'fts',
        });
      }

      // Mode 3: Combined (structured + FTS)
      if (hasFilters && hasText) {
        // Step 1: Get FTS matches first (capped at 500, already ranked)
        const ftsResults = await db.search(textQuery, bucket, 500);

        // Step 2: For each FTS hit, check if it passes the structured filter
        // by looking up the doc and re-checking — avoids loading millions of
        // structured matches into memory.
        const query = buildQuery(filters);
        const intersection = [];

        for (const hit of ftsResults) {
          // Find doc by file key AND structured filters together
          const combined = { $and: [{ '_file.key': hit.key }, query] };
          const doc = await db.findOne(collection, combined);
          if (doc) {
            doc._score = hit.score;
            intersection.push(doc);
          }
          // Stop early once we have enough for this page + skip
          if (intersection.length >= skip + limit) break;
        }

        const paged = intersection.slice(skip, skip + limit);
        return res.json({ docs: paged, total: intersection.length, mode: 'combined' });
      }

      // No filters and no text — return all
      const docs = await db.find(collection, {}, { skip, limit, sort: { _uploadedAt: -1 } });
      res.json({ docs, mode: 'all' });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  return router;
};

/**
 * Build an OxiDB query from filter descriptors.
 * Each filter: { field, op, value } or { field, min, max } for ranges.
 */
function buildQuery(filters) {
  const conditions = [];

  for (const [field, filter] of Object.entries(filters)) {
    if (filter == null) continue;

    // Range filter (number/date): { min, max }
    if (filter.min != null || filter.max != null) {
      if (filter.min != null && filter.min !== '') {
        conditions.push({ [field]: { $gte: filter.min } });
      }
      if (filter.max != null && filter.max !== '') {
        conditions.push({ [field]: { $lte: filter.max } });
      }
      continue;
    }

    // Simple equality / value filter
    if (filter.value != null && filter.value !== '') {
      conditions.push({ [field]: filter.value });
    }
  }

  if (conditions.length === 0) return {};
  if (conditions.length === 1) return conditions[0];
  return { $and: conditions };
}
