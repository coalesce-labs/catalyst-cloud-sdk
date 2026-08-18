// scripts/deployed-entity-drift.ts — CTC-672.
//
// ⛔ THE GAP THIS CLOSES, AND WHY THE EXISTING CHECK COULD NOT CLOSE IT.
// `test/entity-names-drift.test.ts` (CTC-643 / sdk#24) compares this package's published
// `ENTITY_NAMES` against `@catalyst-cloud/schema`'s feed list — but that dependency is pinned
// EXACTLY and CI installs frozen, so it compares two artifacts that are BOTH frozen in this repo. If
// the mirror gains a feed entity and nobody bumps the pin, it stays green. That check is still the
// right one for the moment the bump lands (all three historical drifts entered here at a bump); it
// simply cannot see the service moving first.
//
// ⭐ THE CONTROL THE TICKET WRITES INTO ITS OWN ACCEPTANCE CRITERIA, and the reason this file reaches
// the network at all:
//
//     "the detector reads something the SDK repo cannot edit … editing only files in the SDK repo
//      cannot make it pass"
//
// A detector whose expected value lives in the same repo as the thing it checks is the CTC-643
// defect one level up — which is exactly how sdk#24's first version overclaimed. So the expected set
// is fetched from the LATEST PUBLISHED `@catalyst-cloud/schema` on npm. Nothing in this repo — not
// `package.json`, not `bun.lock`, not this file — can change what that fetch returns.
//
// ⚠️ WHAT THIS STILL DOES NOT PROVE, stated plainly rather than left for a reviewer to find.
// "Latest published on npm" is NOT "what the service deployed". A window remains between the mirror
// deploying an entity and the schema package being published, and this cannot see into it. It is a
// strictly tighter bound than the pinned comparison — the publish is a REQUIRED step for a mirror
// entity to exist at all (a migration needs a schema version bump and a real npm publish), so the
// publish happens at or before the deploy — but it is a bound, not the deployed truth. The only
// thing that would be deployed truth is a live endpoint on the mirror reporting its own feed set,
// which does not exist today. Do not read this file as closing that.
//
// ⛔ WHY SCHEDULED AND NOT PER-PR. It installs from the network, which a per-PR gate must not depend
// on (this repo's CI is offline and frozen-lockfile by design). The per-PR job stays
// `entity-names-drift.test.ts`; this one runs on a cron and fails loudly, which is the right shape
// for "something OUTSIDE this repo changed".

/** What the two lists disagree about. Empty-and-empty is the only passing state. */
export interface EntityDrift {
  /** In the schema's feed list, absent from the SDK's `ENTITY_NAMES` — the SDK is BEHIND. */
  missing: string[];
  /** Claimed by the SDK, absent from the schema's feed list — the SDK is AHEAD or wrong. */
  extra: string[];
}

/**
 * Compare the SDK's published entity list against a schema's feed list.
 *
 * Pure, and separate from every I/O concern above it, so the interesting cases (behind, ahead, both
 * at once) are testable without a network install.
 */
export function diffEntityNames(
  sdkEntityNames: readonly string[],
  schemaFeedEntityNames: readonly string[],
): EntityDrift {
  const sdk = new Set(sdkEntityNames);
  const schema = new Set(schemaFeedEntityNames);
  return {
    missing: [...schema].filter((n) => !sdk.has(n)).sort(),
    extra: [...sdk].filter((n) => !schema.has(n)).sort(),
  };
}

/** True when the two lists agree exactly. */
export function isClean(drift: EntityDrift): boolean {
  return drift.missing.length === 0 && drift.extra.length === 0;
}

/**
 * Render the verdict for a human reading a failed cron job.
 *
 * ⭐ It NAMES the entities, which is the ticket's acceptance criterion ("Then it FAILS … naming the
 * new entity"). A count would reproduce the `toHaveLength(15)` failure that started CTC-643.
 */
export function formatDrift(drift: EntityDrift, schemaVersion: string, sdkVersion: string): string {
  if (isClean(drift)) {
    return `✅ @catalyst-cloud/sdk@${sdkVersion} ENTITY_NAMES matches @catalyst-cloud/schema@${schemaVersion} FEED_ENTITY_NAMES.`;
  }
  const lines = [
    `❌ ENTITY DRIFT — @catalyst-cloud/sdk@${sdkVersion} does not match the LATEST PUBLISHED @catalyst-cloud/schema@${schemaVersion}.`,
    "",
  ];
  if (drift.missing.length > 0) {
    lines.push(
      `The schema has ${drift.missing.length} feed entit${drift.missing.length === 1 ? "y" : "ies"} the SDK cannot name:`,
      ...drift.missing.map((n) => `  + ${n}`),
      "",
      "  → bump @catalyst-cloud/schema in package.json and re-derive ENTITY_NAMES.",
      "",
    );
  }
  if (drift.extra.length > 0) {
    lines.push(
      `The SDK claims ${drift.extra.length} entit${drift.extra.length === 1 ? "y" : "ies"} the schema's feed list does not have:`,
      ...drift.extra.map((n) => `  - ${n}`),
      "",
      "  → an entity was REMOVED from the feed (or moved to nonFeedEntityTables). A consumer",
      "    subscribing to it is subscribing to something that will never arrive.",
      "",
    );
  }
  return lines.join("\n");
}

/**
 * The feed list as exported by a schema module.
 *
 * ⛔ `FEED_ENTITY_NAMES` IS REQUIRED AND THERE IS DELIBERATELY NO FALLBACK TO `mirrorEntityTables`.
 * They are different questions, and the difference is a live defect class: before CTC-671 the only
 * list available was `mirrorEntityTables`, which INCLUDES `push_subscriptions` — a table that must
 * never be broadcast account-wide. Falling back to it would silently compare against a list that
 * means "everything replicable" while reporting on "everything feed-safe", so a schema too old to
 * answer the question would produce a confident WRONG answer. A named throw is the only safe reading.
 */
export function readFeedEntityNames(mod: unknown): readonly string[] {
  const names = (mod as { FEED_ENTITY_NAMES?: unknown }).FEED_ENTITY_NAMES;
  if (!Array.isArray(names) || names.some((n) => typeof n !== "string")) {
    throw new Error(
      "@catalyst-cloud/schema does not export FEED_ENTITY_NAMES as a string[]. " +
        "A schema predating CTC-671 cannot answer 'which entities are feed-safe' — " +
        "mirrorEntityTables is NOT a substitute (it includes push_subscriptions). " +
        "Refusing to guess.",
    );
  }
  return names as readonly string[];
}

// ── the I/O half ────────────────────────────────────────────────────────────────────────────────
//
// Kept below the pure half and behind injectable seams, so every branch above is tested without a
// network round-trip and this section stays thin enough to read in one go.

import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const run = promisify(execFile);

/** Ask npm for the version currently tagged `latest`. */
export async function resolveLatestSchemaVersion(): Promise<string> {
  const { stdout } = await run("npm", ["view", "@catalyst-cloud/schema", "version"], {
    timeout: 60_000,
  });
  const version = stdout.trim();
  if (!/^\d+\.\d+\.\d+/.test(version)) {
    throw new Error(`npm returned an unusable version for @catalyst-cloud/schema: ${version}`);
  }
  return version;
}

/**
 * Install one published schema version into a scratch directory and read its feed list.
 *
 * ⛔ A SCRATCH DIRECTORY, NOT THIS REPO'S `node_modules`. Installing into the repo would mutate the
 * pinned tree the OTHER check depends on — and would make this detector's answer a function of the
 * repo's own state, which is the one thing the acceptance criteria forbid.
 */
export async function fetchFeedEntityNames(version: string): Promise<readonly string[]> {
  const dir = await mkdtemp(join(tmpdir(), "sdk-entity-drift-"));
  try {
    await writeFile(
      join(dir, "package.json"),
      JSON.stringify({ name: "entity-drift-probe", private: true }),
    );
    await run("bun", ["add", `@catalyst-cloud/schema@${version}`], { cwd: dir, timeout: 300_000 });
    // The package source-exports `./src/index.ts`, so this needs a TS-capable runtime — which is why
    // the workflow runs it under `bun` rather than `node`.
    const mod = await import(join(dir, "node_modules", "@catalyst-cloud", "schema", "src", "index.ts"));
    return readFeedEntityNames(mod);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

/**
 * The cron entry point. Injectable so the test can drive every branch — including the failing one —
 * without touching the network.
 */
export async function checkDeployedEntityDrift(deps: {
  sdkEntityNames: readonly string[];
  sdkVersion: string;
  resolveVersion?: () => Promise<string>;
  fetchNames?: (version: string) => Promise<readonly string[]>;
  log?: (line: string) => void;
}): Promise<{ drift: EntityDrift; schemaVersion: string; ok: boolean }> {
  const resolveVersion = deps.resolveVersion ?? resolveLatestSchemaVersion;
  const fetchNames = deps.fetchNames ?? fetchFeedEntityNames;
  const log = deps.log ?? ((l: string) => console.log(l));

  const schemaVersion = await resolveVersion();
  const schemaNames = await fetchNames(schemaVersion);
  const drift = diffEntityNames(deps.sdkEntityNames, schemaNames);
  log(formatDrift(drift, schemaVersion, deps.sdkVersion));
  return { drift, schemaVersion, ok: isClean(drift) };
}
