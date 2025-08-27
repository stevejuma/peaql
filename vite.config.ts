import { defineConfig } from "vitest/config";
import { lezer } from "@lezer/generator/rollup";
import dts from "vite-plugin-dts";
import peerDepsExternal from "rollup-plugin-peer-deps-external";

export default defineConfig({
  plugins: [lezer(), peerDepsExternal(), dts({ rollupTypes: true })],
  esbuild: {
    minifyIdentifiers: false,
  },
  test: {
    coverage: {
      reporter: ["html"],
      provider: "v8",
    },
  },
  build: {
    sourcemap: "inline",
    lib: {
      name: "beanquery",
      fileName: "index",
      entry: "src/lib/index.ts",
      formats: ["es"],
    },
  },
});
