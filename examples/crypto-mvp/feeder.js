// feeder.js — pipe Binance BTCUSDT trades into OxiDB.
//   wss://stream.binance.com:9443/ws/btcusdt@trade  →  collection "trades"
//   Auto-reconnects on disconnect. Logs throughput once per second.

"use strict";

const { OxiDB } = require("../../clients/js/index.js");

const SYMBOL    = process.env.SYMBOL    || "btcusdt";
const STREAM    = `wss://stream.binance.com:9443/ws/${SYMBOL}@trade`;
const OXIDB_URL = process.env.OXIDB_URL || "http://127.0.0.1:9080";
const KEEP_LAST = Number(process.env.KEEP_LAST || 5000); // soft cap

const db = new OxiDB(OXIDB_URL);
const trades = db.collection("trades");

let inserted = 0;
let lastReport = Date.now();

async function reset() {
  try {
    await trades.delete({});
    console.log(`[feeder] cleared trades for ${SYMBOL.toUpperCase()}`);
  } catch (e) {
    console.warn("[feeder] reset failed (non-fatal):", e.message);
  }
}

async function trim() {
  // Cheap retention: drop everything older than the most-recent KEEP_LAST.
  // Once OxiDB retention policies are wired into the SDK, swap to setRetention.
  try {
    const total = await trades.count();
    if (total > KEEP_LAST * 2) {
      const cutoffDoc = (await trades.find({},
        { sort: { ts: -1 }, skip: KEEP_LAST, limit: 1 }))[0];
      if (cutoffDoc) {
        await trades.delete({ ts: { $lt: cutoffDoc.ts } });
      }
    }
  } catch (e) { /* swallow */ }
}

function connect() {
  console.log("[feeder] connecting to", STREAM);
  const ws = new WebSocket(STREAM);

  ws.addEventListener("open", () => console.log("[feeder] open"));

  ws.addEventListener("message", async (ev) => {
    try {
      const t = JSON.parse(ev.data);
      // Binance trade event: e=trade, p=price, q=qty, T=trade time, m=isBuyerMaker
      if (t.e !== "trade") return;
      const doc = {
        symbol: t.s,
        ts:     t.T,
        price:  Number(t.p),
        qty:    Number(t.q),
        side:   t.m ? "sell" : "buy",
      };
      await trades.insert(doc);
      inserted++;
    } catch (e) {
      // OxiDB writes can be lossy; just log occasionally
      if (inserted % 100 === 0) console.warn("[feeder] insert error:", e.message);
    }
  });

  ws.addEventListener("close", () => {
    console.log("[feeder] closed — reconnecting in 1 s");
    setTimeout(connect, 1000);
  });

  ws.addEventListener("error", (e) => {
    console.warn("[feeder] error:", e.message || e);
  });
}

setInterval(() => {
  const now = Date.now();
  const dt  = (now - lastReport) / 1000;
  console.log(`[feeder] +${inserted} trades in ${dt.toFixed(1)}s `
    + `(${(inserted / dt).toFixed(1)}/s)`);
  inserted = 0;
  lastReport = now;
  trim();
}, 1000);

(async () => {
  await reset();
  connect();
})();
