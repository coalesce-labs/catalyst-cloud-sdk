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
//
// ⛔ EXACTLY WHAT THIS DETECTS, AND WHAT IT DOES NOT — stated precisely because the first version of
// this header claimed the CTC-643 gap was "now closed", and a reviewer was right that it is not
// (Codex round-1 P1 on sdk#24). `@catalyst-cloud/schema` is pinned EXACTLY (`0.1.15`), locked in
// `bun.lock`, and CI installs frozen. So this compares the published list against the schema version
// the SDK SHIPS AGAINST — not against whatever the service deployed five minutes ago. If the mirror
// gains an entity and nobody bumps the dependency, this stays green.
//
// ⭐ WHY IT IS STILL THE RIGHT CHECK: that is precisely where all three historical drifts happened.
// The bump commit is the moment the new entity enters this repo, and it named the entity every time:
//
//     0.8.8  — bump replicate/schema for catalyst-cloud CTC-308  (workflow_states)
//     0.8.9  — bump replicate/schema for catalyst-cloud CTC-619  (push_subscriptions)
//     0.8.10 — bump replicate/schema for catalyst-cloud CTC-624  (team_workflow_mapping)
//
// Through all three the union sat at 15 and the contract test stayed green. THIS FILE WOULD HAVE
// GONE RED AT EACH OF THOSE THREE COMMITS, naming the entity. It converts a silent bump into a loud
// one, which is the failure mode that actually produced CTC-643.
//
// ⚠️ The residual — "the service moved and nobody bumped" — needs something that reads the deployed
// contract (a published fixture or a live fetch). Filed as CTC-672 rather than papered over here,
// with the control that matters written into it: a detector whose expected value lives in the same
// repo as the thing it checks is the CTC-643 defect one level up. Do not read this file as covering
// it.

import { describe, expect, it } from "vitest";
import {
  FEED_ENTITY_NAMES,
  MIRROR_TABLE_META,
  NON_FEED_ENTITY_NAMES,
} from "@catalyst-cloud/schema";
import { ENTITY_NAMES, type EntityName } from "../src/index";

/**
 * Compile-time exact type equality. Resolves to `true` only when `A` and `B` are the same type —
 * mutual assignability alone is not enough (a wider union is assignable in one direction).
 */
type TypesAreEqual<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

/** A compile error HERE is the failure: `EntityName` and `ENTITY_NAMES` have gone out of lockstep. */
const exactEquality: TypesAreEqual<EntityName, (typeof ENTITY_NAMES)[number]> = true;

/**
 * ⭐ CTC-671 — THE EXCLUSION LIST THAT USED TO LIVE HERE IS GONE, and that is the fix.
 *
 * This file previously declared `NOT_ON_THE_ACCOUNT_FEED = ["push_subscriptions"]` — a THIRD
 * hand-maintained list, added because the mirror's two lists disagreed. `mirrorEntityTables` (which
 * derives `MIRROR_TABLE_META`) contained `push_subscriptions` under a comment saying it "rides the
 * change feed like any other entity", while the service's own `EntityName` deliberately excluded it:
 * CTC-641 had found those rows — `endpoint` + `p256dh` + `auth`, together the complete capability to
 * push arbitrary notifications to a person's browser — being broadcast account-wide to every host
 * replica and every OTHER member's browser OPFS DB. Deriving this SDK's published list from
 * `MIRROR_TABLE_META` wholesale would have re-published exactly what CTC-641 removed, in a diff that
 * looked like one added string.
 *
 * ⛔ CTC-671 collapsed that into ONE definition, structurally: `@catalyst-cloud/schema` now splits
 * `feedEntityTables` from `nonFeedEntityTables`, exports `FEED_ENTITY_NAMES` as the single feed
 * contract, and `MIRROR_TABLE_META` still covers BOTH (so `@catalyst-cloud/replicate`'s
 * forward-compat `knownColumns` path is unchanged). So this file no longer names the exclusion — it
 * READS it, which means it can no longer be wrong about it, and the naive-derivation control below
 * still proves the distinction is doing something.
 */

/** The entity set the SDK should publish — the mirror's own feed contract, read not transcribed. */
function derivedFeedEntities(): string[] {
  return [...FEED_ENTITY_NAMES];
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
    // come here and delete the guard deliberately, rather than leaving a dead one behind.
    expect(Object.keys(MIRROR_TABLE_META)).toContain("push_subscriptions");
    expect([...ENTITY_NAMES]).not.toContain("push_subscriptions");
    // ⭐ CTC-671: and it is excluded STRUCTURALLY, at the schema, not by a filter this file applies.
    // If someone moves it into `feedEntityTables`, `FEED_ENTITY_NAMES` gains it and the assertion
    // above fires here — the SDK can no longer silently disagree with the mirror about this table.
    expect([...NON_FEED_ENTITY_NAMES]).toContain("push_subscriptions");
    expect([...FEED_ENTITY_NAMES]).not.toContain("push_subscriptions");
  });

  it("⛔⛔ NEGATIVE CONTROL — the NAIVE derivation is detectably wrong, and differs by exactly the exclusions", () => {
    // This is the assertion that would have caught CTC-643's preferred fix being implemented
    // literally. If it ever passes trivially (because the two sets became equal), the exclusion has
    // stopped mattering and this file needs revisiting — which is why it asserts the DIFFERENCE, not
    // merely "they differ".
    const naive = Object.keys(MIRROR_TABLE_META);
    const published = new Set<string>(ENTITY_NAMES);
    expect(naive.filter((n) => !published.has(n)).sort()).toEqual([...NON_FEED_ENTITY_NAMES].sort());
    expect(naive).not.toEqual([...ENTITY_NAMES]);
    // ⛔ And the exclusion must be NON-EMPTY. If `nonFeedEntityTables` were ever emptied, every
    // assertion in this file would still pass while the feed list quietly became the naive one.
    expect(NON_FEED_ENTITY_NAMES.length).toBeGreaterThan(0);
  });

  it("⭐ EntityName and ENTITY_NAMES are EXACTLY equal — a compile-time check, not a cast", () => {
    // ⛔ THE FIRST VERSION OF THIS TEST WAS INERT (Codex round-1 P2 on sdk#24, correct). It did
    // `const asEntity = name as EntityName` — and a cast SUPPRESSES assignability checking, so the
    // "the union is not wider" claim was never checked at all; the runtime line under it merely
    // re-asserted that schema-derived names appear in the array, which the first test already covers.
    // A test whose stated invariant is enforced by a cast is asserting nothing.
    //
    // `TypesAreEqual` below is the standard conditional-type identity trick: two generic function
    // types are mutually assignable only when their conditional branches resolve identically, which
    // holds iff A and B are the SAME type — so this fires when the union is wider AND when it is
    // narrower, neither of which `satisfies` in src/types.ts can catch on its own (`satisfies` proves
    // the array is assignable to the union, i.e. one direction only).
    expect(exactEquality).toBe(true);
  });
});
