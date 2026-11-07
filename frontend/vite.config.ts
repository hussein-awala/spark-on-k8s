import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";

export default defineConfig({
  base: "./",
  define: { "process.env.NODE_ENV": JSON.stringify("production") },
  build: {
    outDir: "../spark_on_k8s/api/webserver/static/ui",
    emptyOutDir: true,
    sourcemap: false,
    cssCodeSplit: false,
    minify: true,
    license: { fileName: "licenses.md" },
    lib: {
      entry: fileURLToPath(new URL("./src/standalone.tsx", import.meta.url)),
      formats: ["es"],
      fileName: "console",
      cssFileName: "console",
    },
  },
});
