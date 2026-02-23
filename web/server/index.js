const express = require('express');
const cors = require('cors');
const OxiDBClient = require('./oxidb.js');
const createRouter = require('./routes.js');

const PORT = process.env.PORT || 3000;
const OXIDB_HOST = process.env.OXIDB_HOST || '127.0.0.1';
const OXIDB_PORT = parseInt(process.env.OXIDB_PORT || '4444', 10);

async function main() {
  const db = new OxiDBClient(OXIDB_HOST, OXIDB_PORT);

  console.log(`Connecting to OxiDB at ${OXIDB_HOST}:${OXIDB_PORT}...`);
  await db.connect();
  const pong = await db.ping();
  console.log(`OxiDB connected: ${pong}`);

  // Bootstrap _schemas collection with unique index on collectionName
  try {
    await db.createCollection('_schemas');
    console.log('Created _schemas collection');
  } catch (e) {
    // Already exists, that's fine
  }
  try {
    await db.createUniqueIndex('_schemas', 'collectionName');
    console.log('Created unique index on _schemas.collectionName');
  } catch (e) {
    // Already exists
  }

  const app = express();
  app.use(cors());
  app.use(express.json());
  app.use('/api', createRouter(db));

  app.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
  });
}

main().catch((err) => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
