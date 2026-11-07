import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";

export default defineConfig({
  base: "./",
  define: { "process.env.NODE_ENV": JSON.stringify("production") },
  build: {
    outDir: "../spark_on_k8s/airflow/ui/static",
    emptyOutDir: true,
    sourcemap: false,
    cssCodeSplit: false,
    minify: true,
    license: { fileName: "licenses.md" },
    lib: {
      entry: fileURLToPath(new URL("./src/airflow.tsx", import.meta.url)),
      formats: ["iife"],
      name: "SparkOnK8sAirflow",
      fileName: () => "airflow.js",
      cssFileName: "console",
    },
    rolldownOptions: {
      external: ["react", "react/jsx-runtime"],
      output: {
        globals: { react: "React", "react/jsx-runtime": "ReactJSXRuntime" },
      },
    },
  },
});
