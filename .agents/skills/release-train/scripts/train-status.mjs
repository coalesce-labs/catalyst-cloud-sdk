#!/usr/bin/env node
// train-status.mjs — where every member of Catalyst's release train stands right now, read-only.
//
//   node train-status.mjs [--json] [--catalyst-cloud <checkout>] [--base-url <url>]
//
// It reads each npm package's `latest`, the installer revision the live install.sh serves, and the
// declared train line (RELEASE_LINE in catalyst-cloud's packages/types/src/install-block.ts, read from
// a local checkout's origin/main after fetching it when --catalyst-cloud is given, otherwise through
// `gh api`). The installer is read from --base-url, else CATALYST_CLOUD_BASE_URL, else staging. Then it
// says whether every member's MAJOR.MINOR equals the declared line.
//
// Exit 0: every member is on the declared line. 1: the train is split (the off-line members are
// named). 2: something could not be read, so no verdict; it never reports "on-line" without reading
// every member. Nothing here writes, publishes or changes a version.

import { execFileSync } from "node:child_process";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

/** The released members. `npm` members are read from the registry; the installer from its header. */
export const MEMBERS = [
  { id: "cli", label: "CLI (@catalyst-cloud/cli)", npm: "@catalyst-cloud/cli", repo: "catalyst-cloud-skills" },
  { id: "forwarder", label: "forwarder (@catalyst-cloud/catalyst-skills, deprecated)", npm: "@catalyst-cloud/catalyst-skills", repo: "catalyst-cloud-skills" },
  { id: "sdk", label: "SDK (@catalyst-cloud/sdk)", npm: "@catalyst-cloud/sdk", repo: "catalyst-cloud-sdk" },
  { id: "sdk-replica-node", label: "SDK replica, Node", npm: "@catalyst-cloud/sdk-replica-node", repo: "catalyst-cloud-sdk" },
  { id: "sdk-replica-browser", label: "SDK replica, browser", npm: "@catalyst-cloud/sdk-replica-browser", repo: "catalyst-cloud-sdk" },
  { id: "schema", label: "schema (@catalyst-cloud/schema)", npm: "@catalyst-cloud/schema", repo: "catalyst-cloud" },
  { id: "replicate", label: "replicate (@catalyst-cloud/replicate)", npm: "@catalyst-cloud/replicate", repo: "catalyst-cloud" },
  { id: "read-model", label: "read-model (@catalyst-cloud/read-model)", npm: "@catalyst-cloud/read-model", repo: "catalyst-cloud" },
  { id: "installer", label: "installer (install.sh revision)", repo: "catalyst-cloud" },
  // Internal: private GitHub Packages, published from catalyst-cloud main on a version bump. Its first
  // publish is 0.16.0, at the coordinated release. Before the declared line reaches its firstLine it may
// be unpublished; from that line on it is a required member like any other.
  { id: "design", label: "design package (@coalesce-labs/catalyst-design, private)", githubPackage: "catalyst-design", internal: true, firstLine: "0.16", repo: "catalyst-cloud" },
];

/** The version recorded for an internal member that has never been published. */
export const UNPUBLISHED = "not-published";

export const DEFAULT_BASE_URL = "https://staging.catalystcloud.dev";
const INSTALL_BLOCK = "packages/types/src/install-block.ts";

/** "0.15.2" → "0.15"; anything that is not MAJOR.MINOR.PATCH → null. */
export function lineOf(version) {
  const m = /^(\d+)\.(\d+)\.\d+(?:[-+].*)?$/.exec(String(version ?? ""));
  return m ? `${m[1]}.${m[2]}` : null;
}

export function parseReleaseLine(source) {
  return /export const RELEASE_LINE\s*=\s*"(\d+\.\d+)"/.exec(source)?.[1] ?? null;
}

/**
 * verdict(declared, versions) → { state, declared, lines, off, unread }.
 * state is "on-line" only when the declared line is known and every member was read and sits on it.
 */
export function verdict(declared, versions) {
  const beforeFirstLine = (m) => Boolean(declared && m.firstLine) && declared.localeCompare(m.firstLine, undefined, { numeric: true }) < 0;
  const isUnpublished = (m) => m.internal === true && versions[m.id] === UNPUBLISHED && beforeFirstLine(m);
  const counted = MEMBERS.filter((m) => !isUnpublished(m));
  const unpublished = MEMBERS.filter(isUnpublished).map((m) => m.id);
  const unread = counted.filter((m) => lineOf(versions[m.id]) === null).map((m) => m.id);
  const off = counted
    .filter((m) => lineOf(versions[m.id]) !== null && lineOf(versions[m.id]) !== declared)
    .map((m) => ({ id: m.id, version: versions[m.id], line: lineOf(versions[m.id]) }));
  const lines = [...new Set(counted.map((m) => lineOf(versions[m.id])).filter(Boolean))].sort((a, b) =>
    a.localeCompare(b, undefined, { numeric: true }),
  );
  const state = !declared || unread.length > 0 || counted.length === 0 ? "unknown" : off.length > 0 ? "split" : "on-line";
  return { state, declared, lines, off, unread, unpublished };
}

const semverDesc = (a, b) => b.localeCompare(a, undefined, { numeric: true });

/**
 * The newest version of a private GitHub Packages npm package in coalesce-labs, through `gh api`.
 * A missing package counts as not published only when the same token can list the org's npm
 * packages (the positive control); any other failure throws, so the member reads as unread.
 */
const ghApi = (path) => JSON.parse(execFileSync("gh", ["api", path], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }));

export function githubPackageLatest(name, gh = ghApi) {
  try {
    const names = gh(`orgs/coalesce-labs/packages/npm/${name}/versions?per_page=100`).map((v) => v.name).filter((n) => lineOf(n));
    return names.sort(semverDesc)[0] ?? UNPUBLISHED;
  } catch (e) {
    if (!/404|Not Found/i.test(String(e.stderr ?? e.message))) throw e;
    const listed = gh("orgs/coalesce-labs/packages?package_type=npm&per_page=100");
    if (!Array.isArray(listed)) throw e;
    if (listed.some((p) => p.name === name)) throw e;
    return UNPUBLISHED;
  }
}

async function npmLatest(name) {
  const res = await fetch(`https://registry.npmjs.org/${name.replace("/", "%2f")}`, { headers: { accept: "application/vnd.npm.install-v1+json" } });
  if (!res.ok) throw new Error(`npm ${name}: HTTP ${res.status}`);
  return (await res.json())["dist-tags"]?.latest ?? null;
}

async function installerRevision(baseUrl) {
  // GET, not HEAD: the route only answers GET. The body is discarded.
  const res = await fetch(`${baseUrl}/install.sh`);
  await res.arrayBuffer();
  if (!res.ok) throw new Error(`install.sh: HTTP ${res.status}`);
  return res.headers.get("x-catalyst-install-script-revision");
}

function declaredLine(checkout) {
  // A stale origin/main would report the old line as current; fetch first, and give no verdict if
  // the fetch fails.
  if (checkout) execFileSync("git", ["-C", checkout, "fetch", "-q", "origin", "main"], { stdio: ["ignore", "ignore", "pipe"] });
  const text = checkout
    ? execFileSync("git", ["-C", checkout, "show", `origin/main:${INSTALL_BLOCK}`], { encoding: "utf8" })
    : execFileSync("gh", ["api", `repos/coalesce-labs/catalyst-cloud/contents/${INSTALL_BLOCK}?ref=main`, "-H", "Accept: application/vnd.github.raw"], {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
      });
  return parseReleaseLine(text);
}

const USAGE = "usage: node train-status.mjs [--json] [--catalyst-cloud <checkout>] [--base-url <url>]";

async function main(argv) {
  if (argv.includes("--help") || argv.includes("-h")) {
    console.log(USAGE);
    return 0;
  }
  const arg = (n) => (argv.indexOf(`--${n}`) === -1 ? undefined : argv[argv.indexOf(`--${n}`) + 1]);
  const errors = [];
  const versions = {};
  const baseUrl = arg("base-url") ?? process.env.CATALYST_CLOUD_BASE_URL ?? DEFAULT_BASE_URL;
  await Promise.all(
    MEMBERS.map(async (m) => {
      try {
        versions[m.id] = m.npm ? await npmLatest(m.npm) : m.githubPackage ? githubPackageLatest(m.githubPackage) : await installerRevision(baseUrl);
      } catch (e) {
        versions[m.id] = null;
        errors.push(e.message);
      }
    }),
  );
  let declared = null;
  try {
    declared = declaredLine(arg("catalyst-cloud"));
  } catch (e) {
    errors.push(`declared line: ${String(e.message).split("\n")[0]} (pass --catalyst-cloud <checkout> or sign in with gh)`);
  }
  const v = verdict(declared, versions);
  if (argv.includes("--json")) console.log(JSON.stringify({ ...v, versions, errors }, null, 2));
  else {
    console.log(`declared train line: ${declared ?? "UNREAD"}   (installer read from ${baseUrl})`);
    for (const m of MEMBERS) {
      const ver = versions[m.id];
      const mark = m.internal && ver === UNPUBLISHED ? "--" : lineOf(ver) === null ? "??" : lineOf(ver) === declared ? "ok" : "OFF";
      console.log(`  ${mark.padEnd(3)} ${(ver ?? "unread").padEnd(10)} ${m.label}  [${m.repo}]`);
    }
    for (const e of errors) console.log(`  error: ${e}`);
    if (v.unpublished.length) console.log(`  not published yet (internal, does not block the verdict): ${v.unpublished.join(", ")}`);
    if (v.state === "on-line") console.log(`TRAIN: every member is on ${declared}`);
    else if (v.state === "split")
      console.log(`TRAIN: SPLIT across ${v.lines.join(", ")}; off the declared ${declared}: ${v.off.map((o) => `${o.id} ${o.version}`).join(", ")}`);
    else console.log(`TRAIN: UNKNOWN, could not read ${[...(declared ? [] : ["the declared line"]), ...v.unread].join(", ")}`);
  }
  return v.state === "on-line" ? 0 : v.state === "split" ? 1 : 2;
}

// Compare real paths: agents run these through the .claude/skills symlink, and a plain path
// comparison would skip main() there and exit 0 having done nothing.
const isMain = (() => {
  try {
    return Boolean(process.argv[1]) && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
})();
if (isMain) {
  main(process.argv.slice(2)).then((code) => process.exit(code));
}
