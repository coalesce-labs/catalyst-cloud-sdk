// deployed-entity-drift.test.ts — CTC-672.
//
// ⛔ WHAT THIS FILE IS NOT. It is not a second copy of `entity-names-drift.test.ts`. That one asks
// "does the SDK agree with the schema it SHIPS AGAINST" (both frozen in this repo). This one covers
// the detector that asks "does the SDK agree with the schema the world has PUBLISHED" — a question
// this repo cannot answer from its own files, which is precisely the acceptance criterion:
//
//     "the detector reads something the SDK repo cannot edit … editing only files in the SDK repo
//      cannot make it pass"
//
// ⚠️ SO THE NETWORK FETCH ITSELF IS NOT UNIT-TESTED, DELIBERATELY. A test that stubbed the fetch and
// then asserted "we fetched" would prove nothing about the property that matters, and a test that
// really hit npm would make this suite fail on a plane. What IS tested is every decision the
// detector makes ONCE it has an answer — including the one that must fail — plus the refusal to
// guess when the answer is unreadable.

// ── ⭐ MEASURED AGAINST REAL PUBLISHED ARTIFACTS, 2026-08-18 ──────────────────────────────────────
//
// The unit tests below drive the detector's decisions with injected data. That is necessary but not
// sufficient: it says nothing about whether the NETWORK path works or whether the thing actually
// detects. So the detector was also run for real — full `bun add` of each published version, real
// import, no stubbing except which version to ask for:
//
//   vs schema@0.1.18 (latest) → ✅ clean                    ← the steady state
//   vs schema@0.1.17          → ✅ clean                    ← correct: 0.1.17→0.1.18 added a COLUMN,
//                                                              not an entity
//   vs schema@0.1.16          → ❌ FIRES, naming `check_suites` and `push_events`
//   vs schema@0.1.15          → THROWS the named refusal (predates CTC-671, no FEED_ENTITY_NAMES)
//
// ⛔ The 0.1.16 row is the one that matters. A detector that only ever passes is indistinguishable
// from a detector that cannot fail, and every version-based check in this repo's history has had
// that problem at least once. It fires, on a real artifact, naming real entities.

import { describe, expect, it } from "vitest";
import {
  checkDeployedEntityDrift,
  diffEntityNames,
  formatDrift,
  isClean,
  readFeedEntityNames,
} from "../scripts/deployed-entity-drift";
import { ENTITY_NAMES } from "../src/index";

describe("diffEntityNames", () => {
  it("agrees when the two lists match, regardless of order", () => {
    const drift = diffEntityNames(["b", "a"], ["a", "b"]);
    expect(drift).toEqual({ missing: [], extra: [] });
    expect(isClean(drift)).toBe(true);
  });

  it("⭐ THE SCENARIO — the schema gained an entity and the SDK has not: it is reported as MISSING, by name", () => {
    const drift = diffEntityNames(ENTITY_NAMES, [...ENTITY_NAMES, "brand_new_entity"]);
    expect(drift.missing).toEqual(["brand_new_entity"]);
    expect(drift.extra).toEqual([]);
    expect(isClean(drift)).toBe(false);
  });

  it("reports an entity the SDK claims but the schema's feed list dropped, as EXTRA", () => {
    // Not hypothetical: CTC-671 moved `push_subscriptions` OUT of the feed. A consumer subscribing
    // to an entity the feed no longer carries waits forever for a delta that cannot arrive.
    const drift = diffEntityNames([...ENTITY_NAMES, "push_subscriptions"], ENTITY_NAMES);
    expect(drift.extra).toEqual(["push_subscriptions"]);
    expect(drift.missing).toEqual([]);
  });

  it("reports BOTH directions at once, because a rename is both", () => {
    const drift = diffEntityNames(["a", "old_name"], ["a", "new_name"]);
    expect(drift).toEqual({ missing: ["new_name"], extra: ["old_name"] });
  });
});

describe("formatDrift", () => {
  it("⭐ NAMES the entities — a count is what let CTC-643 drift for four ticket generations", () => {
    const out = formatDrift({ missing: ["deployments", "pushes"], extra: [] }, "0.2.0", "0.8.14");
    expect(out).toContain("deployments");
    expect(out).toContain("pushes");
    expect(out).toContain("0.2.0");
    expect(out).toContain("0.8.14");
  });

  it("a clean verdict says so and names both versions it compared", () => {
    const out = formatDrift({ missing: [], extra: [] }, "0.1.18", "0.8.14");
    expect(out).toContain("✅");
    expect(out).toContain("0.1.18");
  });
});

describe("readFeedEntityNames", () => {
  it("reads FEED_ENTITY_NAMES when the schema exports it", () => {
    expect(readFeedEntityNames({ FEED_ENTITY_NAMES: ["issues", "labels"] })).toEqual([
      "issues",
      "labels",
    ]);
  });

  it("⛔ REFUSES to fall back to mirrorEntityTables — they answer DIFFERENT questions", () => {
    // A schema predating CTC-671 has only `mirrorEntityTables`, which INCLUDES `push_subscriptions`
    // — a table that must never be broadcast account-wide. Falling back would compare against
    // "everything replicable" while claiming to report on "everything feed-safe": a confident wrong
    // answer, which is worse than no answer.
    expect(() =>
      readFeedEntityNames({ mirrorEntityTables: { issues: {}, push_subscriptions: {} } }),
    ).toThrow(/FEED_ENTITY_NAMES/);
  });

  it("⛔ refuses a non-array and a non-string array rather than coercing", () => {
    expect(() => readFeedEntityNames({ FEED_ENTITY_NAMES: "issues" })).toThrow(/FEED_ENTITY_NAMES/);
    expect(() => readFeedEntityNames({ FEED_ENTITY_NAMES: [1, 2] })).toThrow(/FEED_ENTITY_NAMES/);
  });
});

describe("checkDeployedEntityDrift — the cron entry point", () => {
  it("⭐ returns ok:false and names the entity when the published schema is AHEAD", async () => {
    const lines: string[] = [];
    const res = await checkDeployedEntityDrift({
      sdkEntityNames: ["issues"],
      sdkVersion: "0.8.14",
      resolveVersion: async () => "0.9.0",
      fetchNames: async () => ["issues", "sparkling_new_table"],
      log: (l) => lines.push(l),
    });

    expect(res.ok).toBe(false);
    expect(res.schemaVersion).toBe("0.9.0");
    expect(res.drift.missing).toEqual(["sparkling_new_table"]);
    expect(lines.join("\n")).toContain("sparkling_new_table");
  });

  it("⛔ POSITIVE CONTROL — the same call with an AGREEING schema returns ok:true", async () => {
    // Without this, the test above passes just as well against a detector hard-wired to fail.
    const res = await checkDeployedEntityDrift({
      sdkEntityNames: ["issues"],
      sdkVersion: "0.8.14",
      resolveVersion: async () => "0.9.0",
      fetchNames: async () => ["issues"],
      log: () => {},
    });
    expect(res.ok).toBe(true);
    expect(res.drift).toEqual({ missing: [], extra: [] });
  });

  it("⛔ it asks for the version it was TOLD is latest — not the one this repo pins", async () => {
    // The acceptance criterion in one assertion: the expected set is keyed off an external answer.
    // If this ever read `package.json`'s pin instead, editing that file alone would silence the
    // detector — the CTC-643 defect one level up.
    const asked: string[] = [];
    await checkDeployedEntityDrift({
      sdkEntityNames: ["issues"],
      sdkVersion: "0.8.14",
      resolveVersion: async () => "99.99.99",
      fetchNames: async (v) => {
        asked.push(v);
        return ["issues"];
      },
      log: () => {},
    });
    expect(asked).toEqual(["99.99.99"]);
  });

  it("propagates a fetch failure rather than reporting a clean run", async () => {
    // ⛔ A detector that swallowed a failed fetch and exited 0 would report "no drift" for every
    // outage — the silent-green failure this whole ticket family is about.
    await expect(
      checkDeployedEntityDrift({
        sdkEntityNames: ["issues"],
        sdkVersion: "0.8.14",
        resolveVersion: async () => "0.9.0",
        fetchNames: async () => {
          throw new Error("npm is down");
        },
        log: () => {},
      }),
    ).rejects.toThrow(/npm is down/);
  });
});
