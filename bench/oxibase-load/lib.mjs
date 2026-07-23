// Shared helpers for the OxiBase load benchmark: the native OxiWire protocol
// (admin provisioning) and ES256 key/JWT minting (so we hold each project's
// service_role token without going through Google-only developer signup).

import net from "node:net";
import crypto from "node:crypto";

// ── OxiWire: length-prefixed JSON over TCP (u32 LE len + payload) ────────────
export function connectWire(host, port) {
  const sock = net.connect({ host, port });
  sock.setNoDelay(true);
  let buf = Buffer.alloc(0);
  const waiters = [];
  sock.on("data", (d) => {
    buf = Buffer.concat([buf, d]);
    while (buf.length >= 4) {
      const len = buf.readUInt32LE(0);
      if (buf.length < 4 + len) break;
      const payload = buf.subarray(4, 4 + len);
      buf = buf.subarray(4 + len);
      const w = waiters.shift();
      if (w) w(JSON.parse(payload.toString("utf8")));
    }
  });
  const ready = new Promise((res, rej) => {
    sock.once("connect", res);
    sock.once("error", rej);
  });
  function call(obj) {
    return new Promise((res, rej) => {
      waiters.push(res);
      const p = Buffer.from(JSON.stringify(obj), "utf8");
      const hdr = Buffer.alloc(4);
      hdr.writeUInt32LE(p.length, 0);
      sock.write(Buffer.concat([hdr, p]), (e) => e && rej(e));
    });
  }
  return { ready, call, end: () => sock.end() };
}

// ── ES256 keys (P-256) + JWT minting ────────────────────────────────────────
export function genKeypair() {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("ec", {
    namedCurve: "P-256",
  });
  const jwk = publicKey.export({ format: "jwk" }); // { crv, kty, x, y }
  const x = Buffer.from(jwk.x, "base64url");
  const y = Buffer.from(jwk.y, "base64url");
  const sec1 = Buffer.concat([Buffer.from([0x04]), x, y]); // 65-byte uncompressed point
  return { privateKey, pubB64: sec1.toString("base64") };
}

const b64url = (b) => Buffer.from(b).toString("base64url");

export function mintJwt(privateKey, { sub, role, ttlSecs = 3600 }) {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: "ES256", typ: "JWT" }));
  const payload = b64url(JSON.stringify({ sub, role, iat: now, exp: now + ttlSecs }));
  const signingInput = `${header}.${payload}`;
  // ieee-p1363 → raw R||S (64 bytes), which is what JWT ES256 requires.
  const sig = crypto.sign("sha256", Buffer.from(signingInput), {
    key: privateKey,
    dsaEncoding: "ieee-p1363",
  });
  return `${signingInput}.${b64url(sig)}`;
}

// ── small concurrency limiter ───────────────────────────────────────────────
export async function pool(items, concurrency, worker) {
  const results = new Array(items.length);
  let i = 0;
  const runners = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await worker(items[idx], idx);
    }
  });
  await Promise.all(runners);
  return results;
}

export function pct(sorted, p) {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx];
}
