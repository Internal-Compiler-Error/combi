import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In development the API runs separately (`cargo run -p combi-server`, or the `server` compose
// service); Vite proxies /api to it so the browser sees a single origin.
export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5173,
    proxy: { "/api": process.env.API_URL ?? "http://localhost:3000" },
  },
});
