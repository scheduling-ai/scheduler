import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { resolve } from "node:path";

// Four HTML entry points, four bundles.  The customer UI is the only
// thing that ships to customer installs; the rest are reachable only
// under /dev/ in dev/internal deployments and 404 in production.
//
//   index.html              → src/customer/main.ts        (LiveState)
//   dev/index.html          → src/dev/chooser/main.ts     (no state)
//   dev/replay/index.html   → src/dev/replay/main.ts      (ReplayState)
//   dev/generator/index.html → src/dev/generator/main.ts  (GeneratorState)
//
// Shared rendering components (ClusterGrid, ScaleView, …) live in
// src/components/ and read their playback state from a Svelte context
// each app populates at mount.  No dev code enters the customer bundle.
export default defineConfig({
  plugins: [svelte()],
  build: {
    rollupOptions: {
      input: {
        customer: resolve(__dirname, "index.html"),
        chooser: resolve(__dirname, "dev/index.html"),
        replay: resolve(__dirname, "dev/replay/index.html"),
        generator: resolve(__dirname, "dev/generator/index.html"),
      },
    },
  },
  server: {
    proxy: {
      "/api": "http://localhost:8000",
      "/state": "http://localhost:8000",
      "/scenarios": "http://localhost:8000",
    },
  },
});
