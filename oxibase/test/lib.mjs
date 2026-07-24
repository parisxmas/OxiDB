// Shared helpers for the OxiBase control-plane e2e scripts.
//
// Developer sign-in is Google-only, so a test cannot sign up with a password.
// Instead it mints the same HS256 session token the control plane would issue,
// using the deployment's platform secret — the technique the operator uses for
// admin one-offs.

import { createHmac } from "node:crypto";

const b64url = (buf) =>
  Buffer.from(buf).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

/// A developer session token for `email`, signed with the platform secret.
export function devToken(email, secret, ttlSecs = 3600) {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = b64url(
    JSON.stringify({ sub: email, role: "admin", iat: now, exp: now + ttlSecs }),
  );
  const signingInput = `${header}.${payload}`;
  const sig = b64url(createHmac("sha256", secret).update(signingInput).digest());
  return `${signingInput}.${sig}`;
}

export function counter() {
  let pass = 0;
  return {
    ok(cond, msg) {
      if (cond) {
        pass++;
        console.log("  ✓", msg);
      } else {
        console.error("  ✗ FAIL:", msg);
        process.exit(1);
      }
    },
    total: () => pass,
  };
}
