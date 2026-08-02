// 10_oxiscript_proc.js — server-side stored procedure.
//   One round-trip executes find + validate + update + insert atomically.
const { OxiDB } = require("../index.js");

const TRANSFER = `
proc transfer(from, to, amount) {
    let s = find_one("accounts", {id: from})
    if s == null            { abort "sender not found" }
    if s.balance < amount   { abort "insufficient funds" }
    update("accounts", {id: from}, {$inc: {balance: -amount}})
    update("accounts", {id: to},   {$inc: {balance:  amount}})
    insert("ledger", {from: from, to: to, amount: amount})
    return {ok: true, sender_left: s.balance - amount}
}
`;

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080");
  const accounts = db.collection("accounts");

  await accounts.delete({});
  await db.collection("ledger").delete({});

  await accounts.insert({ id: "alice", balance: 1000 });
  await accounts.insert({ id: "bob",   balance:    0 });

  await db.createProcedure(TRANSFER);

  const r = await db.callProcedure("transfer", {
    from: "alice", to: "bob", amount: 250,
  });
  console.log("transfer:", r);

  console.log("alice:", await accounts.findOne({ id: "alice" }));
  console.log("bob:  ", await accounts.findOne({ id: "bob"   }));
})().catch((e) => { console.error(e); process.exit(1); });
