import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

/** The simula server (`simula --memory` listens on 8081 by default). */
const server = process.env.SIMULA_URL ?? "http://127.0.0.1:8081";

export default defineConfig({
  plugins: [react()],
  // `simula --web-dir` serves the build; two-segment paths like `/assets/x.js` would hit
  // the Bot API's `/<token>/<method>`, so assets go under the reserved `/_simula/`.
  build: { assetsDir: "_simula/web" },
  server: {
    proxy: {
      "/_simula": server,
      "/session": { target: server.replace(/^http/, "ws"), ws: true },
    },
  },
});
