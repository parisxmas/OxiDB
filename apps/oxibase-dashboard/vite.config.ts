import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Static SPA: `vite build` emits plain assets to dist/, served by nginx/CDN.
// The oxibase API base URL is injected at build time via VITE_OXIBASE_URL, or
// left empty to call the same origin (when the dashboard sits behind the proxy
// that routes /platform/* to oxibase).
export default defineConfig({
  plugins: [react()],
  server: { port: 5173 },
});
