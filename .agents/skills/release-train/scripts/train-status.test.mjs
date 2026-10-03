// train-status.test.mjs — the verdict on whether Catalyst's released members share one MAJOR.MINOR.
// Run: node --test .agents/skills/release-train/scripts/*.test.mjs
import assert from "node:assert/strict";
import { test } from "node:test";

import { MEMBERS, lineOf, parseReleaseLine, verdict } from "./train-status.mjs";

const all = (version) => Object.fromEntries(MEMBERS.map((m) => [m.id, version]));

test("lineOf takes MAJOR.MINOR and refuses what is not a version", () => {
  assert.equal(lineOf("0.15.2"), "0.15");
  assert.equal(lineOf("2.11.0"), "2.11");
  assert.equal(lineOf("0.13.0-rc.1"), "0.13");
  assert.equal(lineOf("2026-09-28.4"), null);
  assert.equal(lineOf(undefined), null);
});

test("parseReleaseLine reads the declared line from install-block.ts", () => {
  assert.equal(parseReleaseLine('// x\nexport const RELEASE_LINE = "0.13";\n'), "0.13");
  assert.equal(parseReleaseLine("export const OTHER = 1;"), null);
});

test("every member on the declared line is on the train", () => {
  const v = verdict("0.16", all("0.16.3"));
  assert.equal(v.state, "on-line");
  assert.deepEqual(v.off, []);
});

test("control: one member on a newer MINOR splits the train and is named", () => {
  const versions = { ...all("0.16.1"), cli: "0.17.0" };
  const v = verdict("0.16", versions);
  assert.equal(v.state, "split");
  assert.deepEqual(v.off.map((o) => o.id), ["cli"]);
  assert.deepEqual(v.lines, ["0.16", "0.17"]);
});

test("today's shape: three lines at once, every off-line member named", () => {
  const versions = { ...all("0.13.1"), cli: "0.15.2", forwarder: "0.15.2", sdk: "0.14.1", "sdk-replica-node": "0.14.1", "sdk-replica-browser": "0.14.1", installer: "0.13.16" };
  const v = verdict("0.13", versions);
  assert.equal(v.state, "split");
  assert.deepEqual(v.off.map((o) => o.id).sort(), ["cli", "forwarder", "sdk", "sdk-replica-browser", "sdk-replica-node"]);
  assert.deepEqual(v.lines, ["0.13", "0.14", "0.15"]);
});

test("a member whose version could not be read makes the verdict unknown, never on-line", () => {
  const versions = { ...all("0.16.0"), installer: null };
  const v = verdict("0.16", versions);
  assert.equal(v.state, "unknown");
  assert.deepEqual(v.unread, ["installer"]);
});

test("an unreadable declared line is unknown too", () => {
  assert.equal(verdict(null, all("0.16.0")).state, "unknown");
});

test("an empty member list can never pass", () => {
  assert.equal(verdict("0.16", {}).state, "unknown");
});

test("run through a symlinked skill directory, the script still runs (no silent exit 0)", async () => {
  const { mkdtempSync, symlinkSync } = await import("node:fs");
  const { tmpdir } = await import("node:os");
  const { dirname, join } = await import("node:path");
  const { fileURLToPath } = await import("node:url");
  const { spawnSync } = await import("node:child_process");
  const link = join(mkdtempSync(join(tmpdir(), "train-status-link-")), "scripts");
  symlinkSync(dirname(fileURLToPath(import.meta.url)), link);
  const r = spawnSync(process.execPath, [join(link, "train-status.mjs"), "--help"], { encoding: "utf8" });
  assert.match(r.stdout, /usage: node train-status\.mjs/);
  assert.equal(r.status, 0);
});

test("before its first line, an internal member that is not published yet is reported and does not block the verdict", async () => {
  const { UNPUBLISHED } = await import("./train-status.mjs");
  const versions = { ...all("0.15.3"), design: UNPUBLISHED };
  const v = verdict("0.15", versions);
  assert.equal(v.state, "on-line");
  assert.deepEqual(v.unpublished, ["design"]);
});

test("control: only an internal member may be unpublished; a public one reads as unread", async () => {
  const { UNPUBLISHED } = await import("./train-status.mjs");
  const v = verdict("0.16", { ...all("0.16.0"), cli: UNPUBLISHED });
  assert.equal(v.state, "unknown");
  assert.deepEqual(v.unread, ["cli"]);
});

test("the design package is a member, read from GitHub Packages", () => {
  const design = MEMBERS.find((m) => m.id === "design");
  assert.equal(design?.githubPackage, "catalyst-design");
  assert.equal(design?.internal, true);
});

test("githubPackageLatest: newest version; not published only when the org listing proves it", async () => {
  const { githubPackageLatest, UNPUBLISHED } = await import("./train-status.mjs");
  const notFound = () => Object.assign(new Error("gh: Not Found (HTTP 404)"), { stderr: "gh: Not Found (HTTP 404)" });
  const gh = (routes) => (path) => {
    const r = Object.entries(routes).find(([k]) => path.startsWith(k))?.[1];
    if (r instanceof Error) throw r;
    return r;
  };
  assert.equal(githubPackageLatest("catalyst-design", gh({ "orgs/coalesce-labs/packages/npm/catalyst-design/versions": [{ name: "0.16.0" }, { name: "0.16.2" }, { name: "0.16.10" }] })), "0.16.10");
  assert.equal(githubPackageLatest("catalyst-design", gh({ "orgs/coalesce-labs/packages/npm/": notFound(), "orgs/coalesce-labs/packages?": [] })), UNPUBLISHED);
  // Controls: a listing that fails, or one that names the package, means no access, not unpublished.
  assert.throws(() => githubPackageLatest("catalyst-design", gh({ "orgs/coalesce-labs/packages/npm/": notFound(), "orgs/coalesce-labs/packages?": new Error("HTTP 401") })));
  assert.throws(() => githubPackageLatest("catalyst-design", gh({ "orgs/coalesce-labs/packages/npm/": notFound(), "orgs/coalesce-labs/packages?": [{ name: "catalyst-design" }] })));
  assert.throws(() => githubPackageLatest("catalyst-design", gh({ "orgs/coalesce-labs/packages/npm/": new Error("HTTP 500") })));
});

test("control: once the declared line reaches the design package's first line, an unpublished package is unread, never exempt", async () => {
  const { UNPUBLISHED } = await import("./train-status.mjs");
  for (const line of ["0.16", "0.17", "1.0"]) {
    const v = verdict(line, { ...all(`${line}.0`), design: UNPUBLISHED });
    assert.equal(v.state, "unknown", line);
    assert.deepEqual(v.unread, ["design"], line);
    assert.deepEqual(v.unpublished, [], line);
  }
  assert.equal(verdict("0.15", { ...all("0.15.3"), design: UNPUBLISHED }).state, "on-line");
});
