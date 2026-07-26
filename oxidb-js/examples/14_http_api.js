// 14_http_api.js — tiny HTTP REST API in front of OxiDB. No external deps.
//   Wraps the `tasks` collection with /tasks endpoints.
//
// Try with:
//   curl -X POST  http://127.0.0.1:3000/tasks -d '{"title":"buy milk"}' -H 'Content-Type: application/json'
//   curl          http://127.0.0.1:3000/tasks
//   curl -X PATCH http://127.0.0.1:3000/tasks/1 -d '{"done":true}'      -H 'Content-Type: application/json'
//   curl -X DELETE http://127.0.0.1:3000/tasks/1
const http = require("node:http");
const { OxiDB } = require("../index.js");

const db = new OxiDB("http://127.0.0.1:9080");
const tasks = db.collection("tasks");

async function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {}); }
      catch (e) { reject(e); }
    });
    req.on("error", reject);
  });
}

const send = (res, status, obj) => {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
};

const server = http.createServer(async (req, res) => {
  const m = req.url.match(/^\/tasks(?:\/(\d+))?$/);
  if (!m) return send(res, 404, { error: "not found" });
  const id = m[1] ? Number(m[1]) : null;

  try {
    if (req.method === "GET" && !id) {
      return send(res, 200, await tasks.find({}, { sort: { _id: -1 } }));
    }
    if (req.method === "POST" && !id) {
      const body = await readJson(req);
      const r = await tasks.insert({ ...body, done: false, ts: Date.now() });
      return send(res, 201, r);
    }
    if (req.method === "PATCH" && id) {
      const body = await readJson(req);
      await tasks.update({ _id: id }, { $set: body });
      return send(res, 200, await tasks.findOne({ _id: id }));
    }
    if (req.method === "DELETE" && id) {
      const r = await tasks.delete({ _id: id });
      return send(res, 200, r);
    }
    send(res, 405, { error: "method not allowed" });
  } catch (e) {
    send(res, 500, { error: e.message });
  }
});

server.listen(3000, () => console.log("listening on http://127.0.0.1:3000"));
