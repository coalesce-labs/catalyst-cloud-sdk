import { defineConfig } from "vitest/config";

// CTC-6150 — the latency suite, kept out of the default `npm test` (vitest.config.ts includes only
// `*.test.*`). Wall-clock bounds on a shared CI runner flake, so these run on demand: `npm run test:perf`.
export default defineConfig({
  test: {
    include: ["test/perf/**/*.perf.ts"],
    environment: "node",
    // The point of this run is the printed p50 and p95 lines, so use the reporter that shows console
    // output, whatever environment runs it (the minimal reporter vitest picks for some agents hides it).
    reporters: ["default"],
    // Seeding 20,000 issues through the real worker path dominates; the measured calls are milliseconds.
    testTimeout: 120_000,
    hookTimeout: 120_000,
  },
});
