import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile, mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  declarationIdentity,
  validateDeclaration,
  validateVersion,
  validateTag,
  checkRelease,
} from "./release-train-check.mjs";

const plan = JSON.parse(await readFile(new URL("../release-train.json", import.meta.url), "utf8"));
const sdkIds = ["sdk", "sdk-replica-node", "sdk-replica-browser"];
// CTC-5137's approved transition, as a line move: members may sit on 0.15 or 0.16, publish only 0.16.
const coordinated = { ...structuredClone(plan), state: "coordinated", sourceLine: "0.15" };
const aligned = { ...structuredClone(plan), state: "aligned" };
// A copy written before CTC-6446, which still names each member's exact patch.
const legacy = structuredClone(aligned);
for (const entry of Object.values(legacy.members))
  Object.assign(entry, { sourceVersion: "0.13.0", version: "0.16.9" });

const installAt = (revision) =>
  new Response("", { headers: { "x-catalyst-install-script-revision": revision } });
/** The deployed cloud: its declaration, its installer revision, and the npm versions it knows. */
const cloud =
  (declaration, published = new Set(), revision = "0.16.0") =>
  async (url) => {
    if (url.endsWith("/install.sh")) return installAt(revision);
    if (url.startsWith("https://registry.npmjs.org/")) {
      const [, name, version] = url.match(/registry\.npmjs\.org\/(.+)\/([^/]+)$/);
      const known = published.has(`${decodeURIComponent(name)}@${version}`);
      return new Response(known ? "{}" : "not found", { status: known ? 200 : 404 });
    }
    return new Response(JSON.stringify(declaration));
  };

test("the declaration names the line and every member, never a member's exact patch", () => {
  assert.equal(validateDeclaration(plan).releaseLine, "0.16");
  for (const entry of Object.values(plan.members)) {
    assert.equal("version" in entry, false);
    assert.equal("sourceVersion" in entry, false);
  }
  assert.equal(validateDeclaration(coordinated).state, "coordinated");
  const missing = structuredClone(plan);
  delete missing.members.forwarder;
  assert.throws(() => validateDeclaration(missing), /every member/);
});

test("a copy that still carries exact patches reads as the same declaration", () => {
  assert.equal(validateDeclaration(legacy).releaseLine, "0.16");
  assert.deepEqual(declarationIdentity(legacy), declarationIdentity(aligned));
  const moved = { ...structuredClone(aligned), releaseLine: "0.17" };
  assert.notDeepEqual(declarationIdentity(moved), declarationIdentity(aligned));
});

test("an aligned train accepts any patch on its line, with no declaration change", () => {
  for (const version of ["0.16.0", "0.16.10", "0.16.999"]) {
    validateVersion(aligned, "schema", version, true);
    for (const id of sdkIds) validateVersion(aligned, id, version, true);
  }
  validateVersion(aligned, "design", "0.16.6");
  assert.throws(
    () => validateVersion(aligned, "design", "0.16.6", true),
    /No publication approved/,
  );
});

test("mixing lines still refuses: 0.15 and 0.17 are off an aligned 0.16 train", () => {
  // Positive control on the same call: a 0.16 patch passes, so the refusals below are the line.
  validateVersion(aligned, "schema", "0.16.10");
  assert.throws(() => validateVersion(aligned, "schema", "0.15.7"), /off release line 0\.16/);
  assert.throws(() => validateVersion(aligned, "sdk", "0.17.0", true), /off release line 0\.16/);
  assert.throws(() => validateVersion(aligned, "cli", "0.13.0"), /off release line/);
});

test("a coordinated move allows its source line in source, but only the release line publishes", () => {
  validateVersion(coordinated, "cli", "0.15.6");
  validateVersion(coordinated, "cli", "0.16.0", true);
  validateVersion(coordinated, "sdk", "0.16.3", true);
  assert.throws(() => validateVersion(coordinated, "cli", "0.15.6", true), /off release line/);
  assert.throws(() => validateVersion(coordinated, "sdk", "0.14.2"), /off release line/);
  for (const sourceLine of [undefined, "0.16", "0.17", "x"])
    assert.throws(
      () => validateDeclaration({ ...structuredClone(coordinated), sourceLine }),
      /source line below/,
    );
});

test("source identities, tag prefixes and answered approvals fail closed", () => {
  validateTag(plan, "sdk", "0.16.0", "v0.16.0");
  validateTag(plan, "cli", "0.16.15", "skills-bundle-v0.16.15");
  assert.throws(() => validateTag(plan, "sdk", "0.16.0", "v0.15.0"), /does not match/);
  assert.throws(
    () => validateTag(plan, "cli", "0.16.15", "skills-bundle-v0.16.14"),
    /does not match cli@0\.16\.15/,
  );
  assert.throws(() => validateTag(plan, "cli", "0.16.0", "v0.16.0"), /does not match/);
  for (const corrupt of [
    (p) => delete p.approval,
    (p) => (p.members.sdk.path = "other.json"),
    (p) => (p.members.sdk.tag = null),
    (p) => delete p.members.sdk.publish,
  ]) {
    const changed = structuredClone(coordinated);
    corrupt(changed);
    assert.throws(() => validateDeclaration(changed));
  }
});

async function cliCheckout(root, version, sdkPin) {
  await writeFile(join(root, "release-train.json"), JSON.stringify(aligned));
  await writeFile(
    join(root, "package.json"),
    JSON.stringify({
      name: "@catalyst-cloud/cli",
      version,
      dependencies: { "@catalyst-cloud/sdk": sdkPin },
    }),
  );
  await mkdir(join(root, "packages/catalyst-skills"), { recursive: true });
  await writeFile(
    join(root, "packages/catalyst-skills/package.json"),
    JSON.stringify({ version, dependencies: { "@catalyst-cloud/cli": version } }),
  );
  await mkdir(join(root, ".claude-plugin"), { recursive: true });
  await writeFile(join(root, ".claude-plugin/plugin.json"), JSON.stringify({ version }));
}

test("publication requires a readable matching cloud declaration before a pack is cut", async () => {
  const root = await mkdtemp(join(tmpdir(), "release-train-test-"));
  try {
    await cliCheckout(root, "0.16.15", "0.16.12");
    const published = new Set(["@catalyst-cloud/sdk@0.16.12"]);
    const options = { root, publish: "cli", tag: "skills-bundle-v0.16.15" };
    const fetcher = cloud(aligned, published);
    assert.equal((await checkRelease({ ...options, fetcher })).publishing.length, 3);
    // A deployed copy that still names exact patches is the same declaration.
    assert.equal(
      (await checkRelease({ ...options, fetcher: cloud(legacy, published) })).publishing.length,
      3,
    );
    await assert.rejects(checkRelease({ ...options, tag: "v0.16.15", fetcher }), /does not match/);
    await assert.rejects(checkRelease({ ...options, tag: "main", fetcher }), /does not match/);
    assert.equal(
      (await checkRelease({ ...options, tag: "main", dryRun: true, fetcher })).dryRun,
      true,
    );
    await assert.rejects(
      checkRelease({ ...options, fetcher: async () => new Response("", { status: 503 }) }),
      /HTTP 503/,
    );
    const stale = structuredClone(aligned);
    stale.approval.answeredAt = "2026-10-06T20:00:00Z";
    await assert.rejects(
      checkRelease({ ...options, fetcher: cloud(stale, published) }),
      /differs from canonical/,
    );
    await assert.rejects(
      checkRelease({ ...options, fetcher: cloud(aligned, published, "0.13.19") }),
      /not deployed/,
    );
    await assert.rejects(checkRelease({ ...options, fetcher: cloud(aligned) }), /not on npm/);
    await cliCheckout(root, "0.16.15", "^0.16.12");
    await assert.rejects(checkRelease({ ...options, fetcher }), /must pin @catalyst-cloud\/sdk/);
    await cliCheckout(root, "0.15.7", "0.16.12");
    await assert.rejects(
      checkRelease({ ...options, tag: "skills-bundle-v0.15.7", fetcher }),
      /off release line/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("both published replicas pin one schema and replicate on the line, and npm has them", async () => {
  const root = await mkdtemp(join(tmpdir(), "release-train-sdk-test-"));
  try {
    await writeFile(join(root, "release-train.json"), JSON.stringify(aligned));
    // 0.16.10 is a patch no declaration names: the check passes on the line and the registry.
    const pins = {
      "@catalyst-cloud/schema": "0.16.10",
      "@catalyst-cloud/replicate": "0.16.10",
      "@catalyst-cloud/read-model": "^0.16.0",
    };
    await writeFile(
      join(root, "package.json"),
      JSON.stringify({
        name: "@catalyst-cloud/sdk",
        version: "0.16.12",
        devDependencies: pins,
        peerDependencies: pins,
      }),
    );
    for (const module of ["replica-node", "replica-browser"]) {
      await mkdir(join(root, "modules", module), { recursive: true });
      await writeFile(
        join(root, "modules", module, "package.json"),
        JSON.stringify({
          version: "0.16.12",
          peerDependencies: { "@catalyst-cloud/sdk": "^0.16.12" },
          dependencies: pins,
        }),
      );
    }
    const published = new Set([
      "@catalyst-cloud/schema@0.16.10",
      "@catalyst-cloud/replicate@0.16.10",
    ]);
    const options = { root, publish: "sdk", tag: "v0.16.12", fetcher: cloud(aligned, published) };
    assert.equal((await checkRelease(options)).publishing.length, 3);
    await assert.rejects(
      checkRelease({
        ...options,
        fetcher: cloud(aligned, new Set(["@catalyst-cloud/replicate@0.16.10"])),
      }),
      /schema@0\.16\.10, which is not on npm/,
    );
    // Every pin set must name the pin: a missing one in the first set is refused, not adopted.
    const core = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
    for (const field of ["devDependencies", "peerDependencies"]) {
      const without = structuredClone(core);
      delete without[field]["@catalyst-cloud/schema"];
      await writeFile(join(root, "package.json"), JSON.stringify(without));
      await assert.rejects(checkRelease(options), /must pin schema/);
    }
    await writeFile(join(root, "package.json"), JSON.stringify(core));
    for (const module of ["replica-node", "replica-browser"]) {
      for (const [id, version] of [
        ["schema", "0.15.2"],
        ["schema", "0.16.9"],
        ["replicate", "^0.16.10"],
        ["read-model", "^0.15.0"],
        ["read-model", "0.16.0"],
      ]) {
        const file = join(root, "modules", module, "package.json");
        const entry = JSON.parse(await readFile(file, "utf8"));
        entry.dependencies[`@catalyst-cloud/${id}`] = version;
        await writeFile(file, JSON.stringify(entry));
        await assert.rejects(checkRelease(options), /must pin|caret range|off release line/);
        entry.dependencies = { ...pins };
        await writeFile(file, JSON.stringify(entry));
      }
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
