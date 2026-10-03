// release-train-skill.test.mjs — this repository carries the release-train skill unchanged from
// coalesce-labs/catalyst-cloud-skills, so an agent releasing the SDK loads the same train rules as
// the CLI and catalyst-cloud. scripts/release-train-skill.mjs writes the copy and its lock; this test
// fails on a hand edit and runs the skill's own model-free tests.
import { spawnSync } from "node:child_process";
import { appendFileSync, cpSync, mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const node = (args, cwd = root) => spawnSync(process.execPath, args, { cwd, encoding: "utf8" });

describe("release-train skill", () => {
  test("every carried file matches .agents/release-train.lock.json", () => {
    const r = node(["scripts/release-train-skill.mjs", "--check"]);
    expect(r.stdout + r.stderr).toContain("0 problem(s)");
    expect(r.status).toBe(0);
  });

  test("the lock names the source repository and the commit it was copied from", () => {
    const lock = JSON.parse(readFileSync(join(root, ".agents/release-train.lock.json"), "utf8"));
    expect(lock.source).toBe("coalesce-labs/catalyst-cloud-skills");
    expect(lock.commit).toMatch(/^[0-9a-f]{40}$/);
  });

  test("control: a hand edit to the copy fails the check", () => {
    const copy = mkdtempSync(join(tmpdir(), "release-train-lock-"));
    for (const p of [".agents", "scripts"]) cpSync(join(root, p), join(copy, p), { recursive: true });
    appendFileSync(join(copy, ".agents/skills/release-train/SKILL.md"), "\nlocal tweak\n");
    const r = node(["scripts/release-train-skill.mjs", "--check"], copy);
    expect(r.status).toBe(1);
    expect(r.stderr).toContain("SKILL.md: differs");
  });

  test("the skill's model-free tests pass", () => {
    // Explicit files: Node 24 reads a directory argument to --test as a module path.
    const dir = join(root, ".agents/skills/release-train");
    const files = ["scripts", "evals"].flatMap((d) => readdirSync(join(dir, d)).filter((f) => f.endsWith(".test.mjs")).map((f) => join(dir, d, f)));
    expect(files.length).toBeGreaterThanOrEqual(2);
    const r = node(["--test", ...files]);
    expect(r.stdout).toMatch(/ℹ fail 0|# fail 0/);
    expect(r.status).toBe(0);
  });
});
