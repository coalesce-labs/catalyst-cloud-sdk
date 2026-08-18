// entity-names-drift.test.ts — CTC-643's actual acceptance criterion, which the shipped fix left open.
//
// ⛔ WHAT WAS STILL MISSING AFTER 0.8.11. That release added the nine entities the union had been
// stale by — correctly — but it added them as a hand-written union plus a hand-written array, and
// changed the contract test's `toHaveLength(15)` to `toHaveLength(24)`. CTC-643's own ticket forbids
// exactly that:
//
//   ⚠️ `toHaveLength(15)` must not simply become `toHaveLength(24)` — a hand-typed count is what let
//      this drift for four ticket generations.
//
// and asks for: "when the mirror gains a replicated entity table, then the SDK's list is either
// correct automatically or a test FAILS". Nothing in the suite reads the mirror's own table set, so
// nothing could report the next drift — and CTC-667 is about to add four more entities.
//
// This file is that check. It does NOT re-declare the list (a second copy is the defect, not the
// fix): it DERIVES the expected set from `@catalyst-cloud/schema`'s `MIRROR_TABLE_META`, which is
// itself derived from the Drizzle table definitions via `getTableConfig` — so it reads the mirror's
// real schema, not a transcription of it.

import { describe, expect, it } from "vitest";
import { MIRROR_TABLE_META } from "@catalyst-cloud/schema";
import { ENTITY_NAMES, type EntityName } from "../src/index";

/**
 * Tables that are in the mirror's schema but must NEVER appear on the ACCOUNT-WIDE change feed.
 *
 * ⛔ `push_subscriptions` IS A SECURITY EXCLUSION, NOT AN OVERSIGHT — and it is the reason this file
 * derives-minus-a-list rather than deriving wholesale. A row carries `endpoint` + `p256dh` + `auth`,
 * which together are the complete capability to push arbitrary notifications to that person's
 * browser. CTC-641 found them being appended to `change_log` and broadcast account-wide to every
 * host replica and every OTHER member's browser OPFS DB, and fixed it by narrowing
 * `MirrorDO.appendAndBroadcast` to the service's `EntityName` — the union whose whole job is "this
 * row is safe for every reader of the account's feed".
 *
 * ⚠️ SO THE NAIVE DERIVATION IS WRONG, AND CTC-643's PREFERRED WORDING ASKS FOR IT. `MIRROR_TABLE_META`
 * still contains `push_subscriptions` (CTC-641 narrowed the seam's type and left the schema's own
 * membership — and a comment there asserting it "rides the change feed like any other entity" —
 * untouched). Deriving `ENTITY_NAMES` straight from it would republish that table as a public feed
 * entity, re-asserting at the type level exactly the claim CTC-641 removed. The two lists disagreeing
 * in writing is filed as CTC-671; until that is decided, the exclusion is named HERE, once, with the
 * reason, and the control below fails if anyone quietly drops it.
 */
const NOT_ON_THE_ACCOUNT_FEED = ["push_subscriptions"] as const;

/** The entity set the SDK should publish, derived from the mirror's own schema. */
function derivedFeedEntities(): string[] {
  const excluded = new Set<string>(NOT_ON_THE_ACCOUNT_FEED);
  return Object.keys(MIRROR_TABLE_META).filter((name) => !excluded.has(name));
}

describe("CTC-643 — the published entity list tracks the mirror's schema", () => {
  it("⭐ ENTITY_NAMES is exactly the schema-derived feed set (order-independent)", () => {
    // Order-independent on purpose: the schema declares `workflow_states`/`team_workflow_mapping`
    // mid-list while the SDK appends them, and neither order is wire-significant. Pinning order here
    // would make a harmless reordering in the mirror a red SDK build — a false failure teaches people
    // to edit the test.
    expect([...ENTITY_NAMES].sort()).toEqual(derivedFeedEntities().sort());
  });

  it("⭐ the count is DERIVED, never hand-typed — CTC-643's own warning", () => {
    // Deliberately not `toHaveLength(24)`. A literal here is the exact mechanism that let the union
    // drift through four ticket generations while its test stayed green.
    expect(ENTITY_NAMES).toHaveLength(derivedFeedEntities().length);
    expect(new Set(ENTITY_NAMES).size).toBe(ENTITY_NAMES.length); // no duplicates
  });

  it("⛔⛔ SECURITY CONTROL — push_subscriptions is in the schema and must NOT be published", () => {
    // Both halves matter. The first proves the exclusion is still DOING something: if
    // push_subscriptions ever leaves MIRROR_TABLE_META, this fires and whoever removed it is told to
    // come here and delete the exclusion deliberately, rather than leaving a dead guard behind.
    expect(Object.keys(MIRROR_TABLE_META)).toContain("push_subscriptions");
    expect([...ENTITY_NAMES]).not.toContain("push_subscriptions");
  });

  it("⛔⛔ NEGATIVE CONTROL — the NAIVE derivation is detectably wrong, and differs by exactly the exclusions", () => {
    // This is the assertion that would have caught CTC-643's preferred fix being implemented
    // literally. If it ever passes trivially (because the two sets became equal), the exclusion has
    // stopped mattering and this file needs revisiting — which is why it asserts the DIFFERENCE, not
    // merely "they differ".
    const naive = Object.keys(MIRROR_TABLE_META);
    const published = new Set<string>(ENTITY_NAMES);
    expect(naive.filter((n) => !published.has(n)).sort()).toEqual([...NOT_ON_THE_ACCOUNT_FEED].sort());
    expect(naive).not.toEqual([...ENTITY_NAMES]);
  });

  it("⭐ every derived name is assignable to EntityName (the type and the runtime list agree)", () => {
    // A runtime list that matches the schema is worth nothing if the published TYPE lags it: a
    // consumer narrows against the type, not the array. `satisfies` in src/types.ts proves the array
    // is assignable to the union; this proves the union is not WIDER, by round-tripping every derived
    // name through it.
    for (const name of derivedFeedEntities()) {
      const asEntity = name as EntityName;
      expect([...ENTITY_NAMES]).toContain(asEntity);
    }
  });
});
