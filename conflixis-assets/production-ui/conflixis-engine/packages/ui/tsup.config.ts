import { defineConfig } from "tsup";

// Manually specify your entry points
const entryObject: Record<string, string> = {
  "index": "src/index.tsx",
  "button/index": "src/button/index.tsx",
  "card/index": "src/card/index.tsx",
  // Add more entries as needed
};

export default defineConfig({
  entry: entryObject,
  format: ["esm", "cjs"],
  dts: {
    compilerOptions: {
      incremental: false
    }
  },
  external: ["react", "react-dom"],
  splitting: false,
  sourcemap: true,
  clean: true,
  outExtension({ format }) {
    return {
      js: format === "esm" ? ".js" : ".cjs",
    };
  },
});
