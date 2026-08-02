// serve.js — tiny static file server for the chart UI.
//   Defaults to ./public on http://127.0.0.1:3000

"use strict";

const http = require("node:http");
const fs   = require("node:fs/promises");
const path = require("node:path");

const ROOT = path.join(__dirname, "public");
// Also expose the local oxidb-js SDK at /oxidb.js for the browser.
const SDK_PATH = path.join(__dirname, "..", "oxidb-js", "index.js");
const PORT = Number(process.env.PORT || 3000);

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js":   "application/javascript; charset=utf-8",
  ".css":  "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg":  "image/svg+xml",
  ".png":  "image/png",
};

http.createServer(async (req, res) => {
  let url = req.url.split("?")[0];
  if (url === "/") url = "/index.html";

  // Special case: serve the OxiDB JS SDK from the sibling oxidb-js/ directory.
  let file;
  if (url === "/oxidb.js") {
    file = SDK_PATH;
  } else {
    file = path.join(ROOT, url);
    if (!file.startsWith(ROOT)) { res.writeHead(403).end("forbidden"); return; }
  }
  try {
    const buf = await fs.readFile(file);
    res.writeHead(200, { "Content-Type": TYPES[path.extname(file)] || "application/octet-stream" });
    res.end(buf);
  } catch {
    res.writeHead(404).end("not found");
  }
}).listen(PORT, () => console.log(`[serve] http://127.0.0.1:${PORT}`));
