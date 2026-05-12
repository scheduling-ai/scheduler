import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { resolve } from "node:path";

// Two HTML entry points share src/ but produce separate bundles:
//
//   index.html  → main.ts     → App.svelte     (production: live-only,
//                                                no chooser, no replay,
//                                                no fake-job generator)
//   dev.html    → main-dev.ts → AppDev.svelte  (dev tools: chooser,
//                                                replay, scenarios,
//                                                generator)
//
// The dev bundle's JS never enters the prod bundle's dependency graph,
// so a customer can't navigate to /replay or trigger the generator
// just by typing the URL. Server-side gating of /dev.html is a separate
// concern handled by the Python server's UI_LANDING_PATH logic.
export default defineConfig({
  plugins: [svelte()],
  build: {
    rollupOptions: {
      input: {
        main: resolve(__dirname, "index.html"),
        dev: resolve(__dirname, "dev.html"),
      },
    },
  },
  server: {
    proxy: {
      "/api": "http://localhost:8000",
    },
  },
});
