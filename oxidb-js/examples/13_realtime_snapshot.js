// 13_realtime_snapshot.js — real-time subscription via WebSocket.
//   Listens for inserts on `events` and prints each as it arrives.
//   Requires OXIDB_WS_PORT to be set on the server (default ws path: /ws).
const { OxiDB } = require("../index.js");

(async () => {
  const db = new OxiDB("http://127.0.0.1:9080", {
    wsUrl: "ws://127.0.0.1:9082/ws",
  });
  const events = db.collection("events");
  await events.delete({});

  console.log("subscribing — will exit after 5 events or 10s");
  let received = 0;
  const unsub = events.onSnapshot((evt) => {
    received++;
    console.log(`  [${evt.op}] doc#${evt.docId}:`, evt.doc);
    if (received >= 5) {
      unsub();
      db.closeWebSocket();
      process.exit(0);
    }
  });

  // Generate inserts over time so we see them stream in.
  for (let i = 1; i <= 5; i++) {
    setTimeout(() => events.insert({ n: i, ts: Date.now() }), i * 400);
  }

  setTimeout(() => {
    console.log(`done — saw ${received} events`);
    unsub();
    db.closeWebSocket();
    process.exit(0);
  }, 10000);
})().catch((e) => { console.error(e); process.exit(1); });
