// One-time setup: grant the browser-safe ANON key access to the demo's
// collection by installing a security rule (OxiBase's RLS analog).
//
// The anon key is read-only by default and anon WRITES are denied unless a rule
// opts the collection in. This rule makes `demo_notes` publicly readable and
// writable — appropriate for a public demo. A real app would scope the rule to
// the row owner, e.g. create/update/delete: "auth.uid == doc.user_id".
//
// Run once with the SERVICE_ROLE key (admin) — NOT from the browser:
//   OXIBASE_URL=http://127.0.0.1:8087 \
//   OXIBASE_REF=<ref> \
//   OXIBASE_SERVICE_ROLE_KEY=<service_role key> \
//   node setup.mjs

const URL = process.env.OXIBASE_URL || "http://127.0.0.1:8087";
const REF = process.env.OXIBASE_REF;
const KEY = process.env.OXIBASE_SERVICE_ROLE_KEY;
const TABLE = "demo_notes";

if (!REF || !KEY) {
  console.error("Set OXIBASE_REF and OXIBASE_SERVICE_ROLE_KEY (service_role, not anon).");
  process.exit(1);
}

const rule = { read: "true", create: "true", update: "true", delete: "true" };

const res = await fetch(`${URL}/api/rules/${TABLE}?db=${encodeURIComponent(REF)}`, {
  method: "POST",
  headers: { "Content-Type": "application/json", Authorization: `Bearer ${KEY}` },
  body: JSON.stringify(rule),
});

if (!res.ok) {
  console.error(`Failed (${res.status}): ${await res.text()}`);
  process.exit(1);
}
console.log(`✓ public rule installed on "${TABLE}" — the anon key can now read & write it.`);
