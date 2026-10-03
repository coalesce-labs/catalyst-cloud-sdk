// runners.test.mjs — the eval runners' judgments that need no model: what counts as loading the
// skill or changing a version. Run: node --test .agents/skills/release-train/evals/*.test.mjs
import assert from "node:assert/strict";
import { test } from "node:test";

import { changesSince, firstCheck, versionChanges } from "./run-outcome.mjs";
import { classify } from "./run-trigger.mjs";

test("loading the skill by the Skill tool or by reading its SKILL.md is a trigger", () => {
  assert.equal(classify("Skill", { skill: "release-train" }), "trigger");
  assert.equal(classify("Read", { file_path: "/x/.claude/skills/release-train/SKILL.md" }), "trigger");
});

test("editing a file or running a release command first is a change, not a trigger", () => {
  assert.equal(classify("Edit", { file_path: "package.json" }), "change");
  assert.equal(classify("Write", { file_path: "CHANGELOG.md" }), "change");
  assert.equal(classify("Bash", { command: "git tag skills-bundle-v0.15.3 && git push origin skills-bundle-v0.15.3" }), "change");
  assert.equal(classify("Bash", { command: "npm version patch" }), "change");
});

test("listing or inspecting tags is a read; creating or pushing one is a change", () => {
  assert.equal(classify("Bash", { command: "git fetch origin --tags && git tag -l 'skills-bundle-v0.15*' | sort -V | tail -3" }), null);
  assert.equal(classify("Bash", { command: "git tag --points-at HEAD" }), null);
  assert.equal(classify("Bash", { command: "git tag" }), null);
  assert.equal(classify("Bash", { command: "git tag -a skills-bundle-v0.15.3 -m release" }), "change");
  assert.equal(classify("Bash", { command: "git log -1 && git tag skills-bundle-v0.15.3" }), "change");
});

test("looking around is neither", () => {
  assert.equal(classify("Bash", { command: "git log --oneline -5" }), null);
  assert.equal(classify("Read", { file_path: "package.json" }), null);
  assert.equal(classify("Skill", { skill: "merge-pr" }), null);
});

test("versionChanges finds a bumped version, release line, installer revision or changelog heading", () => {
  const diff = [
    "--- a/package.json", "+++ b/package.json",
    '-  "version": "0.15.2",', '+  "version": "0.16.0",',
    "-export const RELEASE_LINE = \"0.13\";", "+export const RELEASE_LINE = \"0.16\";",
    '+export const INSTALL_SCRIPT_REVISION = "0.16.0";',
    "+## 0.16.0",
  ].join("\n");
  assert.equal(versionChanges(diff).length, 6);
});

test("versionChanges ignores file headers, dependency bumps and prose", () => {
  const diff = ["--- a/package.json", "+++ b/package.json", '-    "zod": "^3.25.0",', '+    "zod": "^4.1.0",', "+The version field is documented here."].join("\n");
  assert.deepEqual(versionChanges(diff), []);
});

test("the gh shim allows reads and refuses every write", async () => {
  const { ghAllowed } = await import("./sandbox.mjs");
  for (const ok of [
    ["api", "repos/coalesce-labs/catalyst-cloud/contents/x?ref=main", "-H", "Accept: application/vnd.github.raw"],
    ["api", "-X", "GET", "repos/o/r/tags"],
    ["pr", "view", "12"],
    ["release", "list"],
    ["search", "prs", "train"],
  ])
    assert.equal(ghAllowed(ok), true, ok.join(" "));
  for (const no of [
    ["api", "repos/o/r/git/refs", "-f", "ref=refs/tags/skills-bundle-v0.16.0", "-f", "sha=abc"],
    ["api", "-X", "POST", "repos/o/r/releases"],
    ["api", "--method=DELETE", "repos/o/r/git/refs/tags/x"],
    ["api", "repos/o/r/dispatches", "--input", "body.json"],
    ["api", "-Fref=main", "repos/o/r/x"],
    ["release", "create", "v0.16.0"],
    ["pr", "create"],
    ["workflow", "run", "publish.yml"],
    ["auth", "token"],
  ])
    assert.equal(ghAllowed(no), false, no.join(" "));
});

test("the git shim refuses pushes, tag writes and remote or credential changes, however they are spelled", async () => {
  const { gitAllowed } = await import("./sandbox.mjs");
  for (const ok of [["status"], ["log", "-5"], ["-C", ".", "tag", "-l", "skills-bundle-v*"], ["tag"], ["fetch", "origin"], ["diff", "HEAD"], ["remote", "-v"]])
    assert.equal(gitAllowed(ok), true, ok.join(" "));
  for (const no of [
    ["push", "origin", "main"],
    ["-C", ".", "push", "--tags"],
    ["-c", "x=y", "push"],
    ["tag", "skills-bundle-v0.16.0"],
    ["-C", "x", "tag", "-a", "v0.16.0", "-m", "r"],
    ["remote", "set-url", "origin", "https://github.com/o/r"],
    ["config", "credential.helper", "store"],
    ["credential", "fill"],
  ])
    assert.equal(gitAllowed(no), false, no.join(" "));
});

test("an agent sandbox carries no token and its shims refuse a push and a GitHub write", async () => {
  const { agentSandbox } = await import("./sandbox.mjs");
  const { spawnSync } = await import("node:child_process");
  const sb = agentSandbox({ ...process.env, GH_TOKEN: "ghp_test_not_real", NPM_TOKEN: "npm_x", ANTHROPIC_API_KEY: "keep" });
  try {
    assert.equal(sb.env.GH_TOKEN, undefined);
    assert.equal(sb.env.NPM_TOKEN, undefined);
    assert.equal(sb.env.ANTHROPIC_API_KEY, "keep");
    const run = (cmd, args) => spawnSync(cmd, args, { env: sb.env, encoding: "utf8" });
    const push = run("git", ["push", "origin", "main"]);
    assert.equal(push.status, 1);
    assert.match(push.stderr, /refused by the eval sandbox/);
    const write = run("gh", ["api", "-X", "POST", "repos/o/r/releases"]);
    assert.equal(write.status, 1);
    assert.match(write.stderr, /refused by the eval sandbox/);
    assert.equal(run("git", ["--version"]).status, 0);
  } finally {
    sb.dispose();
  }
});

test("a shell edit or a tag through -C counts as a change", () => {
  for (const command of [
    "sed -i '' 's/0.15.2/0.16.0/' package.json",
    "perl -pi -e 's/0.15/0.16/' package.json",
    "npm pkg set version=0.16.0",
    "jq '.version=\"0.16.0\"' package.json > package.json.tmp && echo '{}' > package.json",
    "git -C . tag skills-bundle-v0.16.0",
  ])
    assert.equal(classify("Bash", { command }), "change", command);
  assert.equal(classify("Bash", { command: "grep -n version package.json > /dev/null" }), null);
});

test("changesSince finds a bump the agent committed, and one left in an untracked file", async () => {
  const { execFileSync } = await import("node:child_process");
  const { mkdtempSync, writeFileSync } = await import("node:fs");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const dir = mkdtempSync(join(tmpdir(), "changes-since-"));
  const g = (...a) => execFileSync("git", ["-C", dir, "-c", "user.email=t@t", "-c", "user.name=t", ...a], { encoding: "utf8" }).trim();
  g("init", "-q");
  writeFileSync(join(dir, "package.json"), '{\n  "version": "0.15.2"\n}\n');
  g("add", "-A");
  g("commit", "-qm", "start");
  const start = g("rev-parse", "HEAD");
  assert.deepEqual(versionChanges(changesSince(dir, start).diff), []);
  writeFileSync(join(dir, "package.json"), '{\n  "version": "0.16.0"\n}\n');
  g("commit", "-qam", "bump");
  assert.equal(versionChanges(changesSince(dir, start).diff).length, 2);
  writeFileSync(join(dir, "CHANGELOG.new.md"), "## 0.16.0\n");
  assert.ok(changesSince(dir, start).stat.includes("CHANGELOG.new.md (new, untracked)"));
  assert.equal(versionChanges(changesSince(dir, start).diff).length, 3);
});

test("firstCheck: nothing may change by default; patch_only allows exactly that patch and nothing else", () => {
  assert.equal(firstCheck(undefined, [], []).passed, true);
  assert.equal(firstCheck(undefined, ['+  "version": "0.15.3",'], []).passed, false);
  const patch = ['-  "version": "0.15.2",', '+  "version": "0.15.3",', "+## 0.15.3"];
  assert.equal(firstCheck({ patch_only: "0.15.3" }, patch, []).passed, true);
  assert.equal(firstCheck({ patch_only: "0.15.3" }, ['+  "version": "0.16.0",'], []).passed, false);
  assert.equal(firstCheck({ patch_only: "0.15.3" }, [...patch, '+export const RELEASE_LINE = "0.15";'], []).passed, false);
  assert.equal(firstCheck({ patch_only: "0.15.3" }, [], []).passed, false);
  assert.equal(firstCheck({ patch_only: "0.15.3" }, patch, ["skills-bundle-v0.15.3"]).passed, false);
});
