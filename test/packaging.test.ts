// test/packaging.test.ts — the export map is PUBLIC API that `tsc` never reads (CTC-114 review, KtE).
//
// Nothing else in the build validates it: a renamed or moved source file still typechecks, still
// builds, and still passes every unit test, while the published package resolves to a path that does
// not exist. The failure only surfaces in a consumer's bundler, after publish, on a specifier that is
// frozen the moment it ships. These assertions are that missing check.
//
// Deliberately uses only `node:fs` and cwd-relative paths: this repo typechecks with `types: []` and
// no `@types/node`, so `node:path` / `node:url` do not resolve here (`node:fs` does, via the driver
// shims). Vitest runs with the package root as cwd, which is the only base these paths need.

import { describe, it, expect } from "vitest";
import * as fs from "node:fs";
import pkg from "../package.json";

/** `./dist/replica/browser/db.worker.js` → the `src/**.ts` that compiles to it. Mirrors
 *  tsconfig.build.json's `rootDir: "src"` / `outDir: "dist"`. */
function distTargetToSource(target: string): string {
  return target
    .replace(/^\.\/dist\//, "src/")
    .replace(/\.d\.ts$/, ".ts")
    .replace(/\.js$/, ".ts");
}

type ConditionMap = Record<string, string>;
const exportsMap = pkg.exports as unknown as Record<string, ConditionMap>;

describe("package exports map", () => {
  it("publishes the db.worker entry the documented createWorker escape hatch needs", () => {
    // The specifier string itself is the contract — renaming it after publish is a breaking change,
    // so it is asserted literally rather than derived.
    expect(exportsMap["./browser/db-worker"]).toEqual({
      types: "./dist/replica/browser/db.worker.d.ts",
      import: "./dist/replica/browser/db.worker.js",
      default: "./dist/replica/browser/db.worker.js",
    });
  });

  it("maps every export target to a source file that exists (anti-dangle)", () => {
    const dangling: string[] = [];
    for (const [subpath, conditions] of Object.entries(exportsMap)) {
      for (const [condition, target] of Object.entries(conditions)) {
        const source = distTargetToSource(target);
        if (!fs.existsSync(source)) {
          dangling.push(`${subpath} [${condition}] → ${target} (no ${source})`);
        }
      }
    }
    expect(dangling).toEqual([]);
  });

  it("keeps the worker out of sideEffects pruning", () => {
    // `sideEffects: false` is a license for webpack to elide any import whose bindings go unused —
    // and db.worker.js has NO exports at all: it calls createWorkerCore() and registers a message
    // listener. Pruned, the worker still loads and still registers nothing, so every RPC hangs with
    // no error anywhere. The export without this narrowing is worse than no export.
    expect(pkg.sideEffects).not.toBe(false);
    expect(pkg.sideEffects).toContain("./dist/replica/browser/db.worker.js");
  });
});
