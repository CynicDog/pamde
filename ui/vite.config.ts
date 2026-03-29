import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  // During development, proxy API requests to the pamde FastAPI server.
  server: {
    proxy: {
      "/api": "http://localhost:2971",
    },
  },
  // Build static files into the Python package so they are served at runtime.
  build: {
    outDir: path.resolve(
      __dirname,
      "../py-pamde/src/pamde/server/static"
    ),
    emptyOutDir: true,
  },
});
