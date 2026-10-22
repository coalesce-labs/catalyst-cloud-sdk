import { describe, expect, it } from "vitest";
import {
  checkEventFilter,
  eventFilterQueryParams,
  eventFilterSocketParams,
  eventScopeKeys,
  matchesEventFilter,
  matchesEventType,
  type EventScopeFilterInput,
} from "../src/event-filter.js";

const BRANCH_MOVED_931894 = {
  type: "branch.moved",
  entity: { type: "pull_request", id: "coalesce-labs/catalyst-cloud#3847" },
  payload: {
    repo: "coalesce-labs/catalyst-cloud",
    branch: "CTC-1175",
    pr: 3847,
    ticket: "CTC-1175",
  },
};
const CONFLICT = {
  type: "pr.conflict.detected",
  entity: { type: "pull_request", id: "coalesce-labs/catalyst-cloud#9001" },
  payload: {
    repo: "coalesce-labs/catalyst-cloud",
    branch: "ryan/stack-a",
    pr: 9001,
    ticket: "CTC-1234",
    paths: ["src/main.ts"],
  },
};

describe("checkEventFilter", () => {
  it("accepts every key in its documented form and normalizes it", () => {
    expect(
      checkEventFilter({
        type: ["pr.conflict.detected", "phase.*"],
        ticket: "ctc-1234",
        repo: "coalesce-labs/catalyst-cloud",
        ref: "coalesce-labs/catalyst-cloud:ryan/stack-a",
        pr: "coalesce-labs/catalyst-cloud#9001",
        session: "CTC-1234/implement#7",
        to: "me",
      }),
    ).toEqual({
      ok: true,
      filter: {
        type: ["pr.conflict.detected", "phase.*"],
        ticket: ["CTC-1234"],
        repo: ["coalesce-labs/catalyst-cloud"],
        ref: ["coalesce-labs/catalyst-cloud:ryan/stack-a"],
        pr: ["coalesce-labs/catalyst-cloud#9001"],
        session: ["CTC-1234/implement#7"],
        to: "me",
      },
    });
  });
  it.each<[EventScopeFilterInput, string | null, string | null, string]>([
    [{}, null, null, "at least one"],
    [{ type: "PR.Conflict" }, "type", "PR.Conflict", "event type"],
    [{ type: "*" }, "type", "*", "event type"],
    [{ ticket: "CTC1234" }, "ticket", "CTC1234", "ticket"],
    [{ repo: "repo" }, "repo", "repo", "owner/name"],
    [
      { ref: "catalyst-cloud:main" },
      "ref",
      "catalyst-cloud:main",
      "owner/name:branch",
    ],
    [{ ref: "o/r:" }, "ref", "o/r:", "owner/name:branch"],
    [{ ref: "o/r:a,b" }, "ref", "o/r:a,b", "comma"],
    [{ ref: "o/r:a..b" }, "ref", "o/r:a..b", "owner/name:branch"],
    [{ ref: "o/r:a b" }, "ref", "o/r:a b", "owner/name:branch"],
    [{ pr: "o/r#0" }, "pr", "o/r#0", "owner/name#n"],
    [{ pr: "o/r#x" }, "pr", "o/r#x", "owner/name#n"],
    [
      { session: "CTC-1/implement" },
      "session",
      "CTC-1/implement",
      "TICKET/phase#n",
    ],
    [{ to: "you" }, "to", "you", "me"],
    [
      { ticket: Array.from({ length: 51 }, (_, i) => `CTC-${i + 1}`) },
      "ticket",
      null,
      "50",
    ],
  ])("refuses %j naming the key and value", (input, key, value, reasonPart) => {
    const result = checkEventFilter(input);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.key).toBe(key);
      expect(result.value).toBe(value);
      expect(result.reason).toContain(reasonPart);
    }
  });
  it("trims and deduplicates values before enforcing the 50-value cap", () => {
    expect(
      checkEventFilter({ ticket: Array.from({ length: 51 }, () => " ctc-1 ") }),
    ).toEqual({ ok: true, filter: { ticket: ["CTC-1"] } });
    expect(checkEventFilter({ type: [] })).toMatchObject({
      ok: false,
      key: null,
    });
  });
  it("serializes socket values together and query values separately in a fixed order", () => {
    const filter = {
      type: ["pr.*"],
      ticket: ["CTC-1", "CTC-2"],
      ref: ["o/r:a", "o/r:b"],
      to: "me",
    } as const;
    expect(eventFilterQueryParams(filter)).toEqual([
      ["ticket", "CTC-1"],
      ["ticket", "CTC-2"],
      ["type", "pr.*"],
      ["ref", "o/r:a"],
      ["ref", "o/r:b"],
      ["to", "me"],
    ]);
    expect(eventFilterSocketParams(filter)).toEqual({
      ticket: "CTC-1,CTC-2",
      type: "pr.*",
      ref: "o/r:a,o/r:b",
      to: "me",
    });
  });
});

describe("eventScopeKeys", () => {
  it("derives ticket, repo, ref and pr from a real branch.moved (cache seq 931894)", () => {
    expect(eventScopeKeys(BRANCH_MOVED_931894)).toEqual({
      tickets: ["CTC-1175"],
      repos: ["coalesce-labs/catalyst-cloud"],
      refs: ["coalesce-labs/catalyst-cloud:CTC-1175"],
      prs: ["coalesce-labs/catalyst-cloud#3847"],
      sessions: [],
    });
  });
  it("derives them from a pr.conflict.detected, a github.pr.synchronize, a github.push and a phase event", () => {
    expect(eventScopeKeys(CONFLICT)).toEqual({
      tickets: ["CTC-1234"],
      repos: ["coalesce-labs/catalyst-cloud"],
      refs: ["coalesce-labs/catalyst-cloud:ryan/stack-a"],
      prs: ["coalesce-labs/catalyst-cloud#9001"],
      sessions: [],
    });
    expect(
      eventScopeKeys({
        type: "github.pr.synchronize",
        entity: { type: "ticket", id: "CTC-1175" },
        payload: {
          number: 3847,
          pull_request: { head: { ref: "CTC-1175" } },
          repository: { full_name: "coalesce-labs/catalyst-cloud" },
        },
      }),
    ).toEqual(eventScopeKeys(BRANCH_MOVED_931894));
    expect(
      eventScopeKeys({
        type: "github.push",
        payload: { ref: "refs/heads/main", repository: { full_name: "o/r" } },
      }).refs,
    ).toEqual(["o/r:main"]);
    expect(
      eventScopeKeys({
        type: "phase.plan.started",
        payload: { ticket: "CTC-5520", phase: "plan", nonce: 1 },
      }).sessions,
    ).toEqual(["CTC-5520/plan#1"]);
    expect(
      eventScopeKeys({
        type: "github.pr.opened",
        payload: { repo: "o/r", pull_request: { number: 9 } },
      }).prs,
    ).toEqual(["o/r#9"]);
  });
  it("ignores null payload fields (daytona sandbox events)", () => {
    expect(
      eventScopeKeys({
        type: "sandbox.started",
        payload: { ticket: null, repo: null, pr: null },
      }),
    ).toEqual({ tickets: [], repos: [], refs: [], prs: [], sessions: [] });
  });
});

describe("matchesEventFilter", () => {
  it("ANDs keys and ORs values", () => {
    expect(
      matchesEventFilter(CONFLICT, {
        type: ["pr.*"],
        ref: ["o/r:wrong", "coalesce-labs/catalyst-cloud:ryan/stack-a"],
      }),
    ).toBe(true);
    expect(
      matchesEventFilter(CONFLICT, {
        type: ["phase.*"],
        ref: ["coalesce-labs/catalyst-cloud:ryan/stack-a"],
      }),
    ).toBe(false);
    expect(
      matchesEventFilter(CONFLICT, {
        ticket: ["ctc-1234"],
        pr: ["COALESCE-LABS/CATALYST-CLOUD#9001"],
      }),
    ).toBe(true);
  });
  it("matches a family prefix on segment boundaries only", () => {
    expect(matchesEventType("phase.plan.started", ["phase.*"])).toBe(true);
    expect(matchesEventType("phasex.plan", ["phase.*"])).toBe(false);
    expect(matchesEventType("phase", ["phase.*"])).toBe(false);
  });
  it("compares repo case-insensitively and branch exactly", () => {
    expect(
      matchesEventFilter(CONFLICT, {
        repo: ["Coalesce-Labs/Catalyst-Cloud"],
        ref: ["COALESCE-LABS/CATALYST-CLOUD:ryan/stack-a"],
      }),
    ).toBe(true);
    expect(
      matchesEventFilter(CONFLICT, {
        ref: ["coalesce-labs/catalyst-cloud:ryan/Stack-A"],
      }),
    ).toBe(false);
  });
});
