import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { cloudflare } from "@cloudflare/vite-plugin";

// `vite dev` runs the API worker (worker/) in the real Workers runtime next to the web app,
// so one dev server serves both with hot reload. `vite build` emits the app and the worker.
export default defineConfig({
  plugins: [react(), cloudflare()],
  server: { host: true, port: 5173 },
});
