import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";

// Resolve the bare specifier `oxibase-js` to the SDK source in this repo, so the
// example imports it exactly as a published app would (`from "oxibase-js"`),
// while running against local source with no build/publish step.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "oxibase-js": fileURLToPath(new URL("../../src/index.ts", import.meta.url)),
    },
  },
});
