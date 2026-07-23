// Integration test: the REAL @supabase/postgrest-js client, unmodified, driving
// OxiDB's PostgREST-compatible surface (ADR-0019) — over BOTH engines through
// the same client and the same base URL (`/rest/v1`).
//
// Usage:
//   OXIDB_SQL=1 OXIDB_HTTP_PORT=14590 ... oxidb-server &   # server with SQL on
//   OXIDB_REST_URL=http://127.0.0.1:14590 node test.mjs
//
// Exits 0 on success, 1 on the first failed assertion.

import { PostgrestClient } from '@supabase/postgrest-js'

const BASE = process.env.OXIDB_REST_URL || 'http://127.0.0.1:14590'
const REST = `${BASE}/rest/v1`
const client = new PostgrestClient(REST)

let passed = 0
function ok(cond, msg) {
  if (!cond) {
    console.error(`  ✗ FAIL: ${msg}`)
    process.exit(1)
  }
  passed++
  console.log(`  ✓ ${msg}`)
}
function eq(a, b, msg) {
  ok(JSON.stringify(a) === JSON.stringify(b), `${msg}  (got ${JSON.stringify(a)})`)
}
// Raw call for the one thing postgrest-js can't do: issue SQL DDL to set up the
// SQL-engine tables. Everything else goes through the real client.
async function sql(stmt) {
  const r = await fetch(`${BASE}/api/sql`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql: stmt }),
  })
  if (!r.ok) throw new Error(`SQL setup failed: ${await r.text()}`)
}

// ── Document engine ────────────────────────────────────────────────────────
console.log('\n# Document engine (collections) via postgrest-js')
{
  // insert (array) with .select() → representation
  const { data: cust, error: e1 } = await client
    .from('customers')
    .insert([{ name: 'Ada' }, { name: 'Linus' }])
    .select()
  ok(!e1, `insert customers ok (${e1?.message ?? 'no error'})`)
  ok(Array.isArray(cust) && cust.length === 2, 'insert returned 2 rows (representation)')
  const ada = cust.find((c) => c.name === 'Ada')._id
  const linus = cust.find((c) => c.name === 'Linus')._id

  await client
    .from('orders')
    .insert([
      { item: 'Keyboard', price: 30, customer_id: ada },
      { item: 'Mouse', price: 20, customer_id: ada },
      { item: 'Monitor', price: 300, customer_id: linus },
    ])

  // select + filter + order
  const { data: pricey } = await client
    .from('orders')
    .select('item,price')
    .gt('price', 25)
    .order('price', { ascending: false })
  eq(pricey, [{ item: 'Monitor', price: 300 }, { item: 'Keyboard', price: 30 }], 'gt + order desc')

  // .in() filter
  const { data: two } = await client.from('orders').select('item').in('item', ['Mouse', 'Monitor'])
  eq(two.map((r) => r.item).sort(), ['Monitor', 'Mouse'], 'in.() filter')

  // .or() filter
  const { data: ored } = await client
    .from('orders')
    .select('item')
    .or('price.lt.25,item.eq.Monitor')
    .order('item')
  eq(ored, [{ item: 'Monitor' }, { item: 'Mouse' }], 'or() filter')

  // .like() — SQL-native `%` wildcard, as postgrest-js emits it
  const { data: liked } = await client.from('orders').select('item').like('item', 'M%').order('item')
  eq(liked, [{ item: 'Monitor' }, { item: 'Mouse' }], 'like M% (prefix)')

  // embedding: belongs-to (order → its customer)
  const { data: bt } = await client
    .from('orders')
    .select('item,customers(name)')
    .eq('item', 'Keyboard')
  eq(bt, [{ item: 'Keyboard', customers: { name: 'Ada' } }], 'embed belongs-to')

  // embedding: has-many (customer → its orders)
  const { data: hm } = await client
    .from('customers')
    .select('name,orders(item)')
    .eq('name', 'Ada')
  ok(hm.length === 1 && hm[0].name === 'Ada', 'embed has-many: row is Ada')
  eq(hm[0].orders.map((o) => o.item).sort(), ['Keyboard', 'Mouse'], 'embed has-many: Ada has 2 orders')

  // update with .select()
  const { data: upd } = await client
    .from('orders')
    .update({ on_sale: true })
    .lt('price', 25)
    .select('item,on_sale')
  eq(upd, [{ item: 'Mouse', on_sale: true }], 'update lt(price,25) → Mouse on_sale')

  // delete
  const { error: eDel } = await client.from('orders').delete().eq('item', 'Mouse')
  ok(!eDel, 'delete Mouse ok')
  const { data: rest } = await client.from('orders').select('item').order('item')
  eq(rest, [{ item: 'Keyboard' }, { item: 'Monitor' }], 'Mouse gone after delete')
}

// ── SQL engine (same client, same base URL) ────────────────────────────────
console.log('\n# SQL engine (tables) via the SAME postgrest-js client')
{
  await sql('CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)')
  await sql('CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER REFERENCES authors(id))')

  // insert with .select() → write representation (RETURNING *)
  const { data: insAuthors } = await client
    .from('authors')
    .insert([{ id: 1, name: 'Asimov' }, { id: 2, name: 'Le Guin' }])
    .select('id,name')
  eq(insAuthors, [{ id: 1, name: 'Asimov' }, { id: 2, name: 'Le Guin' }], 'SQL insert representation')
  await client.from('books').insert([
    { id: 10, title: 'Foundation', author_id: 1 },
    { id: 11, title: 'I, Robot', author_id: 1 },
    { id: 12, title: 'The Dispossessed', author_id: 2 },
  ])

  // filter + select + order over a SQL table — routed to the SQL engine
  const { data: books } = await client
    .from('books')
    .select('title,author_id')
    .eq('author_id', 1)
    .order('title')
  eq(books, [{ title: 'Foundation', author_id: 1 }, { title: 'I, Robot', author_id: 1 }], 'SQL: eq + order')

  // embedding via CATALOG FK — belongs-to
  const { data: bt } = await client.from('books').select('title,authors(name)').eq('id', 12)
  eq(bt, [{ title: 'The Dispossessed', authors: { name: 'Le Guin' } }], 'SQL embed belongs-to (catalog FK)')

  // embedding via CATALOG FK — has-many
  const { data: hm } = await client.from('authors').select('name,books(title)').eq('id', 1)
  ok(hm.length === 1 && hm[0].name === 'Asimov', 'SQL embed has-many: row is Asimov')
  eq(hm[0].books.map((b) => b.title).sort(), ['Foundation', 'I, Robot'], 'SQL embed has-many: 2 books')

  // update with .select() → representation echoes the modified rows
  const { data: upd } = await client
    .from('books')
    .update({ title: 'Foundation (rev)' })
    .eq('id', 10)
    .select('id,title')
  eq(upd, [{ id: 10, title: 'Foundation (rev)' }], 'SQL update representation')

  // delete with .select() → representation echoes the deleted rows
  const { data: del } = await client.from('books').delete().eq('id', 11).select('id,title')
  eq(del, [{ id: 11, title: 'I, Robot' }], 'SQL delete representation')

  const { data: remaining } = await client.from('books').select('id').eq('author_id', 1)
  eq(remaining, [{ id: 10 }], 'SQL delete actually removed the row')
}

// ── TSDB engine (same client, .schema('tsdb') → Accept/Content-Profile) ─────
console.log('\n# TSDB engine (time-series) via postgrest-js .schema("tsdb")')
{
  const tsdb = client.schema('tsdb')

  // write points (Content-Profile: tsdb) — flat objects, string→tag, number→field
  const { error: wErr } = await tsdb.from('cpu').insert([
    { ts: 1000, host: 'web1', usage: 10 },
    { ts: 2000, host: 'web1', usage: 20 },
    { ts: 3000, host: 'web1', usage: 30 },
  ])
  ok(!wErr, `TSDB write ok (${wErr?.message ?? 'no error'})`)

  // read (Accept-Profile: tsdb) — default agg=mean over the range for web1
  const { data: mean } = await tsdb
    .from('cpu')
    .select('usage')
    .eq('host', 'web1')
    .gte('ts', 0)
    .lt('ts', 10000)
  eq(mean, [{ ts: 0, value: 20 }], 'TSDB mean aggregate via postgrest-js')
}

console.log(`\n✅ ALL ${passed} assertions passed — real @supabase/postgrest-js drives OxiDB (all three engines) unmodified.`)
