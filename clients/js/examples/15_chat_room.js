// 15_chat_room.js — ad-hoc chat room. Producer inserts; consumer streams.
//   Two clients in one process: one publishes, one subscribes. Demonstrates
//   pub/sub-style usage of WebSocket onSnapshot for realtime UI updates.
const { OxiDB } = require("../index.js");

const REST = "http://127.0.0.1:9080";
const WS   = "ws://127.0.0.1:9082/ws";
const ROOM = "lobby";

(async () => {
  const writer = new OxiDB(REST);
  const reader = new OxiDB(REST, { wsUrl: WS });

  const messages = writer.collection("messages");
  await messages.delete({ room: ROOM });

  // Subscriber prints any message landing in our room.
  const unsub = reader.collection("messages").onSnapshot(
    { room: ROOM },
    (evt) => {
      if (evt.op !== "insert") return;
      const m = evt.doc;
      console.log(`[${ROOM}] ${m.user}: ${m.text}`);
    },
  );

  // Publisher posts 5 messages over 2 seconds.
  const users = ["alice", "bob", "carol"];
  const lines = [
    "hey",
    "is anyone there?",
    "I just shipped v0.25.3",
    "nice 🎉",
    "/quit",
  ];
  for (let i = 0; i < lines.length; i++) {
    await new Promise((r) => setTimeout(r, 400));
    await messages.insert({
      room: ROOM,
      user: users[i % users.length],
      text: lines[i],
      ts: Date.now(),
    });
  }

  // Give the subscriber a moment to drain, then exit.
  setTimeout(() => {
    unsub();
    reader.closeWebSocket();
    process.exit(0);
  }, 1500);
})().catch((e) => { console.error(e); process.exit(1); });
