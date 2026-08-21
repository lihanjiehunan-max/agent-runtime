import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const target = env.VITE_RUNTIME_PROXY_TARGET || "http://127.0.0.1:8000";

  return {
    plugins: [react()],
    server: {
      proxy: {
        "/api/v1/runtime": {
          target,
          changeOrigin: false,
        },
      },
    },
    test: {
      environment: "jsdom",
      setupFiles: "./src/testSetup.ts",
      css: true,
    },
  };
});
