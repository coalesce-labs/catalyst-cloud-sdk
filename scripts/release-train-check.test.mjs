import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile, mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  validateDeclaration,
  validateVersion,
  validateTag,
  checkRelease,
} from "./release-train-check.mjs";

const plan = JSON.parse(await readFile(new URL("../release-train.json", import.meta.url), "utf8"));

test("the approved complete cohort is readable, but a missing member cannot authorize a release", () => {
  assert.equal(validateDeclaration(plan).releaseLine, "0.16");
  const missing = structuredClone(plan);
  delete missing.members.forwarder;
  assert.throws(() => validateDeclaration(missing), /every member/);
});

test("a coordinated transition allows its source and target, but only the exact target may publish", () => {
  validateVersion(plan, "cli", "0.15.6");
  validateVersion(plan, "cli", "0.16.0", true);
  assert.throws(() => validateVersion(plan, "cli", "0.15.6", true), /exact approved/);
  assert.throws(() => validateVersion(plan, "sdk", "0.16.1", true), /exact approved/);
  assert.throws(() => validateVersion(plan, "design", "0.16.4", true), /No publication approved/);
  validateVersion(plan, "design", "0.16.5");
  assert.throws(() => validateVersion(plan, "design", "0.16.3"), /exact approved/);
});

test("source identities, tag prefixes and answered approvals fail closed", () => {
  validateTag(plan, "sdk", "0.16.0", "v0.16.0");
  validateTag(plan, "cli", "0.16.0", "skills-bundle-v0.16.0");
  assert.throws(() => validateTag(plan, "sdk", "0.16.0", "v0.15.0"), /does not match/);
  assert.throws(() => validateTag(plan, "cli", "0.16.0", "v0.16.0"), /does not match/);
  for (const corrupt of [
    (p) => delete p.approval,
    (p) => (p.members.sdk.path = "other.json"),
    (p) => (p.members.sdk.version = "0.14.0"),
  ]) {
    const changed = structuredClone(plan);
    corrupt(changed);
    assert.throws(() => validateDeclaration(changed));
  }
});

test("publication requires a readable matching cloud declaration before a pack is cut", async () => {
  const root = await mkdtemp(join(tmpdir(), "release-train-test-"));
  try {
    await writeFile(join(root, "release-train.json"), JSON.stringify(plan));
    await writeFile(
      join(root, "package.json"),
      JSON.stringify({
        name: "@catalyst-cloud/cli",
        version: "0.16.0",
        dependencies: { "@catalyst-cloud/sdk": "0.16.0" },
      }),
    );
    await mkdir(join(root, "packages/catalyst-skills"), { recursive: true });
    await writeFile(
      join(root, "packages/catalyst-skills/package.json"),
      JSON.stringify({ version: "0.16.0", dependencies: { "@catalyst-cloud/cli": "0.16.0" } }),
    );
    await mkdir(join(root, ".claude-plugin"));
    await writeFile(
      join(root, ".claude-plugin/plugin.json"),
      JSON.stringify({ version: "0.16.0" }),
    );
    const options = { root, publish: "cli", tag: "skills-bundle-v0.16.0" };
    const fetcher = async (url) =>
      url.endsWith("/install.sh")
        ? new Response("", { headers: { "x-catalyst-install-script-revision": "0.16.0" } })
        : new Response(JSON.stringify(plan));
    assert.equal((await checkRelease({ ...options, fetcher })).publishing.length, 3);
    await assert.rejects(checkRelease({ ...options, tag: "v0.16.0", fetcher }), /does not match/);
    await assert.rejects(checkRelease({ ...options, tag: "main", fetcher }), /does not match/);
    assert.equal(
      (await checkRelease({ ...options, tag: "main", dryRun: true, fetcher })).dryRun,
      true,
    );
    await assert.rejects(
      checkRelease({ ...options, fetcher: async () => new Response("", { status: 503 }) }),
      /HTTP 503/,
    );
    const stale = structuredClone(plan);
    stale.approval.answeredAt = "2026-10-06T20:00:00Z";
    await assert.rejects(
      checkRelease({ ...options, fetcher: async () => new Response(JSON.stringify(stale)) }),
      /differs from canonical/,
    );
    await assert.rejects(
      checkRelease({
        ...options,
        fetcher: async (url) =>
          url.endsWith("/install.sh")
            ? new Response("", { headers: { "x-catalyst-install-script-revision": "0.13.19" } })
            : fetcher(url),
      }),
      /not deployed/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("both published replicas need the approved direct library pins", async () => {
  const root = await mkdtemp(join(tmpdir(), "release-train-sdk-test-"));
  try {
    await writeFile(join(root, "release-train.json"), JSON.stringify(plan));
    const pins = {
      "@catalyst-cloud/schema": "0.16.0",
      "@catalyst-cloud/replicate": "0.16.0",
      "@catalyst-cloud/read-model": "^0.16.0",
    };
    await writeFile(
      join(root, "package.json"),
      JSON.stringify({
        name: "@catalyst-cloud/sdk",
        version: "0.16.0",
        devDependencies: pins,
        peerDependencies: pins,
      }),
    );
    for (const module of ["replica-node", "replica-browser"]) {
      await mkdir(join(root, "modules", module), { recursive: true });
      await writeFile(
        join(root, "modules", module, "package.json"),
        JSON.stringify({
          version: "0.16.0",
          peerDependencies: { "@catalyst-cloud/sdk": "^0.16.0" },
          dependencies: pins,
        }),
      );
    }
    const options = {
      root,
      publish: "sdk",
      tag: "v0.16.0",
      fetcher: async (url) =>
        url.endsWith("/install.sh")
          ? new Response("", { headers: { "x-catalyst-install-script-revision": "0.16.0" } })
          : new Response(JSON.stringify(plan)),
    };
    assert.equal((await checkRelease(options)).publishing.length, 3);
    for (const module of ["replica-node", "replica-browser"]) {
      for (const [id, version] of [
        ["schema", "0.13.2"],
        ["replicate", "0.13.2"],
        ["read-model", "^0.13.0"],
      ]) {
        const file = join(root, "modules", module, "package.json");
        const entry = JSON.parse(await readFile(file, "utf8"));
        entry.dependencies[`@catalyst-cloud/${id}`] = version;
        await writeFile(file, JSON.stringify(entry));
        await assert.rejects(checkRelease(options), /must pin|declared line/);
        entry.dependencies = { ...pins };
        await writeFile(file, JSON.stringify(entry));
      }
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
