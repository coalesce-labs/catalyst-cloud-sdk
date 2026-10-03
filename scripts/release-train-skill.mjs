#!/usr/bin/env node
// release-train-skill.mjs — the release-train skill is written once and carried, byte for byte, by
// every repository that releases a member of Catalyst's shared release train. An agent working in
// one of those repositories only sees the skills committed there, so each carries a copy, and this
// script keeps the copies equal to the source.
//
//   node scripts/release-train-skill.mjs --check                   every file matches the lock
//   node scripts/release-train-skill.mjs --check --source <dir>    ...and matches the source repo
//   node scripts/release-train-skill.mjs --write --source <dir>    copy from the source, rewrite the lock
//   node scripts/release-train-skill.mjs --lock                    (source repo only) rewrite the lock
//
// <dir> is a checkout of the source repository (SOURCE_REPO below). Files are read from its
// origin/main with `git show`, so its working tree and branch do not matter; fetch it first.
// --ref <ref> reads another ref. The script has no dependencies, so it runs the same in every repo.

import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync, realpathSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const SOURCE_REPO = "coalesce-labs/catalyst-cloud-skills";
export const SKILL_DIR = ".agents/skills/release-train";
export const TOOL = "scripts/release-train-skill.mjs";
export const LOCK = ".agents/release-train.lock.json";
const IGNORED = new Set([".DS_Store"]);

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const sha256 = (buf) => createHash("sha256").update(buf).digest("hex");

/** Every carried file in a checkout's working tree: the skill directory, recursively, plus this tool. */
export function carriedFiles(root) {
  const out = [];
  const walk = (rel) => {
    const full = join(root, rel);
    if (!existsSync(full)) return;
    for (const n of readdirSync(full).sort()) {
      if (IGNORED.has(n)) continue;
      const r = `${rel}/${n}`;
      if (statSync(join(root, r)).isDirectory()) walk(r);
      else out.push(r);
    }
  };
  walk(SKILL_DIR);
  if (existsSync(join(root, TOOL))) out.push(TOOL);
  return out.sort();
}

export function lockFor(root, source, commit) {
  return {
    comment: `The release-train skill's carried files. Written by ${TOOL}; never edit by hand. The source is ${source}.`,
    source,
    commit,
    files: carriedFiles(root).map((p) => ({ path: p, sha256: sha256(readFileSync(join(root, p))) })),
  };
}

export function readLock(root) {
  const p = join(root, LOCK);
  if (!existsSync(p)) throw new Error(`${LOCK} is missing`);
  return JSON.parse(readFileSync(p, "utf8"));
}

/** checkLock(root) → problems: an edited file, a missing one, or a file the lock does not name. */
export function checkLock(root, lock = readLock(root)) {
  const problems = [];
  const named = new Set(lock.files.map((f) => f.path));
  for (const f of lock.files) {
    const p = join(root, f.path);
    if (!existsSync(p)) problems.push(`${f.path}: in ${LOCK} but missing`);
    else if (sha256(readFileSync(p)) !== f.sha256)
      problems.push(`${f.path}: differs from ${LOCK}; change it in ${lock.source} and copy it here with --write`);
  }
  for (const p of carriedFiles(root)) if (!named.has(p)) problems.push(`${p}: not in ${LOCK}`);
  return problems;
}

const git = (dir, args) => execFileSync("git", ["-C", dir, ...args], { encoding: "buffer", maxBuffer: 64 << 20 });

/** sourceFiles(dir, ref) → [{ path, body }] for every carried file at `ref` in the source checkout. */
export function sourceFiles(dir, ref) {
  const listed = git(dir, ["ls-tree", "-r", "--name-only", ref, "--", SKILL_DIR, TOOL]).toString("utf8").split("\n").filter(Boolean);
  if (!listed.some((p) => p.startsWith(`${SKILL_DIR}/`))) throw new Error(`${dir} has no ${SKILL_DIR} at ${ref}`);
  return listed.filter((p) => !IGNORED.has(p.split("/").pop())).sort().map((p) => ({ path: p, body: git(dir, ["show", `${ref}:${p}`]) }));
}

/** checkSource(root, files) → problems where the source differs from the copy here. */
export function checkSource(root, files) {
  const problems = [];
  const want = new Set(files.map((f) => f.path));
  for (const f of files) {
    const p = join(root, f.path);
    if (!existsSync(p)) problems.push(`${f.path}: in the source but not here`);
    else if (!readFileSync(p).equals(f.body)) problems.push(`${f.path}: differs from the source; copy it with --write`);
  }
  for (const p of carriedFiles(root)) if (!want.has(p)) problems.push(`${p}: not in the source`);
  return problems;
}

function write(root, dir, ref) {
  const files = sourceFiles(dir, ref);
  for (const p of carriedFiles(root)) rmSync(join(root, p));
  for (const f of files) {
    mkdirSync(dirname(join(root, f.path)), { recursive: true });
    writeFileSync(join(root, f.path), f.body);
  }
  const commit = git(dir, ["rev-parse", `${ref}^{commit}`]).toString("utf8").trim();
  writeFileSync(join(root, LOCK), `${JSON.stringify(lockFor(root, SOURCE_REPO, commit), null, 2)}\n`);
  console.log(`copied ${files.length} files from ${SOURCE_REPO}@${commit.slice(0, 12)}`);
}

function arg(argv, name) {
  const i = argv.indexOf(`--${name}`);
  return i === -1 ? undefined : argv[i + 1];
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
  const argv = process.argv.slice(2);
  const dir = arg(argv, "source");
  const ref = arg(argv, "ref") ?? "origin/main";
  if (argv.includes("--lock")) {
    // In the source repository the lock names no commit: it is the working tree's own record.
    writeFileSync(join(repoRoot, LOCK), `${JSON.stringify(lockFor(repoRoot, SOURCE_REPO, null), null, 2)}\n`);
    console.log(`wrote ${LOCK} for ${carriedFiles(repoRoot).length} files`);
    process.exit(0);
  }
  if (argv.includes("--write")) {
    // In the source repository the lock names no commit. --write there would replace the working
    // tree's skill, uncommitted edits included, with the ref it reads.
    if (existsSync(join(repoRoot, LOCK)) && readLock(repoRoot).commit === null) {
      console.error(`--write refused: this is the source repository (${SOURCE_REPO}); edit the skill here and run --lock`);
      process.exit(2);
    }
    if (!dir) {
      console.error("--write needs --source <checkout of the source repository>");
      process.exit(2);
    }
    write(repoRoot, resolve(dir), ref);
    process.exit(0);
  }
  if (!argv.includes("--check")) {
    console.error(`usage: node ${TOOL} --check [--source <dir>] | --write --source <dir> | --lock`);
    process.exit(2);
  }
  const problems = checkLock(repoRoot);
  if (dir) problems.push(...checkSource(repoRoot, sourceFiles(resolve(dir), ref)));
  for (const p of problems) console.error(`RELEASE-TRAIN-SKILL  ${p}`);
  console.log(`RELEASE-TRAIN-SKILL: ${readLock(repoRoot).files.length} files checked against ${LOCK}${dir ? ` and ${SOURCE_REPO}@${ref}` : ""}; ${problems.length} problem(s)`);
  process.exit(problems.length ? 1 : 0);
}

