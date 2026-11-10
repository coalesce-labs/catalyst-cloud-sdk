// ticket-execution-card.test.ts — CTC-10373. The two execution-card reads and the wire forms a
// client must handle: each cost form (measured, partial, running, restricted, unavailable), an
// unknown ticket, a ticket with no runs, and a body carrying fields the SDK does not declare.

import { describe, expect, it } from "vitest";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
import {
  CONDITION_REASON_CODES,
  costIsComplete,
  createTenantClient,
  EXECUTION_CONDITIONS,
  isTicketExecutionCard,
  type CostPart,
  type TicketCost,
  type TicketCostRead,
  type TicketExecutionCard,
} from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";
const SERVER_TIME = "2026-11-10T04:30:00.000Z";

function client(script: Parameters<typeof scriptedFetch>[0]) {
  const net = scriptedFetch(script);
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

function part(over: Partial<CostPart> = {}): CostPart {
  return {
    usd: 1.25,
    coverage: "complete",
    state: "measured",
    byBasis: { measured: 1.25, modelled: 0, estimated: 0 },
    missing: { count: 0, kinds: [] },
    ...over,
  };
}

function grantedCost(over: Partial<TicketCost> = {}): TicketCost {
  return {
    access: "granted",
    state: "ok",
    asOfMs: Date.parse(SERVER_TIME),
    lastUsageAtMs: Date.parse(SERVER_TIME) - 60_000,
    stale: false,
    currency: "USD",
    total: { usd: 2.5, coverage: "complete" },
    model: part(),
    compute: part(),
    runs: { stage: 2, sessions: 2, helpers: 0, open: 0, notLaunched: 0 },
    runtime: { firstStartedAtMs: Date.parse(SERVER_TIME) - 3_600_000, summedRunMs: 1_800_000 },
    ...over,
  };
}

function card(cost: TicketCostRead, over: Partial<TicketExecutionCard> = {}): TicketExecutionCard {
  return {
    ticket: { identifier: "ENG-1", title: "Owners see one card per ticket", teamKey: "ENG" },
    linear: { state: "In Progress", stateType: "started", asOf: SERVER_TIME },
    condition: "running",
    conditionReason: "runner_heartbeat",
    claim: { holderLabel: "runner", holderKind: "runner", expiresAt: "2026-11-10T05:00:00.000Z", lastProgressAt: SERVER_TIME },
    current: { stage: "implement", attempt: 1, startedAt: "2026-11-10T04:00:00.000Z", lastActivityAt: SERVER_TIME, helpers: [] },
    queue: null,
    blocker: null,
    completion: null,
    history: [
      {
        attemptId: "plan:n1",
        stage: "plan",
        attempt: 1,
        startedAt: "2026-11-10T03:30:00.000Z",
        endedAt: "2026-11-10T03:55:00.000Z",
        outcome: "succeeded",
        cost: null,
      },
    ],
    cost,
    freshness: [{ source: "fleet_activity", asOf: SERVER_TIME, stale: false }],
    ...over,
  };
}

describe("issues.executionCard", () => {
  it("reads one card from its own path", async () => {
    const body = { serverTime: SERVER_TIME, card: card(grantedCost()) };
    const { net, c } = client([() => json(200, body)]);
    const r = await c.issues.executionCard("ENG-1");
    expect(r).toEqual({ outcome: "ok", status: 200, serverTime: SERVER_TIME, card: body.card });
    expect(new URL(net.calls[0]!.url).pathname).toBe("/api/v1/issues/ENG-1/execution-card");
  });

  it("lifts a 404 for an unknown ticket to its own not-found arm", async () => {
    const { c } = client([() => json(404, { error: "not_found" })]);
    expect(await c.issues.executionCard("ENG-999")).toEqual({ outcome: "not-found", status: 404 });
  });

  it("returns a ticket with no runs as a card in its empty states, not a not-found", async () => {
    const empty = card(
      grantedCost({
        total: { usd: null, coverage: "missing" },
        model: part({ usd: null, coverage: "missing", state: "not_recorded", byBasis: { measured: 0, modelled: 0, estimated: 0 } }),
        compute: part({ usd: null, coverage: "missing", state: "not_recorded", byBasis: { measured: 0, modelled: 0, estimated: 0 } }),
        runs: { stage: 0, sessions: 0, helpers: 0, open: 0, notLaunched: 0 },
        runtime: { firstStartedAtMs: null, summedRunMs: null },
        lastUsageAtMs: null,
      }),
      { condition: "idle", conditionReason: null, claim: null, current: null, history: [] },
    );
    const r = await c_read(empty);
    expect(r.outcome).toBe("ok");
    if (r.outcome !== "ok") return;
    expect(r.card.condition).toBe("idle");
    // Missing is never zero.
    expect(r.card.cost.access === "granted" && "total" in r.card.cost && r.card.cost.total.usd).toBeNull();
  });

  it("carries cost.detail lines on the single-ticket read", async () => {
    const detail = [
      {
        phase: "implement",
        nonce: "n2",
        kind: "stage" as const,
        provider: "anthropic",
        harness: "claude",
        model: "claude-opus-5-5",
        resolvedModel: "claude-opus-5-5",
        tokens: { input: 10, output: 20, cacheRead: 0, cacheWrite: 0 },
        durationMs: 60_000,
        outcome: "succeeded",
        headlineUsd: 1.25,
        costSource: "reported",
        priceSource: null,
        computeUsd: 1.25,
        computeSource: "sandbox",
        sessionFirst: true,
      },
    ];
    const r = await c_read(card(grantedCost({ detail })));
    expect(r.outcome === "ok" && r.card.cost.access === "granted" && "detail" in r.card.cost && r.card.cost.detail).toEqual(detail);
  });

  it("hands a restricted cost to the caller with no amount in it", async () => {
    const r = await c_read(card({ access: "restricted", reason: "account_admin_required" }));
    expect(r.outcome).toBe("ok");
    if (r.outcome !== "ok") return;
    expect(r.card.cost).toEqual({ access: "restricted", reason: "account_admin_required" });
    expect(costIsComplete(r.card.cost)).toBe(false);
  });

  it("hands an unavailable cost to the caller with its last good time", async () => {
    const unavailable = { access: "granted" as const, state: "unavailable" as const, reason: "usage_store_unreachable", lastGoodAtMs: 1 };
    const r = await c_read(card(unavailable));
    expect(r.outcome === "ok" && r.card.cost).toEqual(unavailable);
    expect(costIsComplete(unavailable)).toBe(false);
  });

  it("keeps a partial and a running cost distinct from complete", async () => {
    const partial = grantedCost({
      total: { usd: 1.25, coverage: "partial" },
      compute: part({ usd: null, coverage: "missing", state: "not_attributable", missing: { count: 1, kinds: ["compute_not_measured"] } }),
    });
    const running = grantedCost({
      total: { usd: 0.4, coverage: "partial" },
      model: part({ usd: 0.4, coverage: "partial", state: "running" }),
      runs: { stage: 1, sessions: 1, helpers: 0, open: 1, notLaunched: 0 },
    });
    expect(costIsComplete(partial)).toBe(false);
    expect(costIsComplete(running)).toBe(false);
    expect(costIsComplete(grantedCost())).toBe(true);
    const r = await c_read(card(partial));
    expect(r.outcome === "ok" && r.card.cost).toEqual(partial);
  });

  it("accepts a body carrying fields the SDK does not declare", async () => {
    const extended = { ...card(grantedCost()), brandNewCloudField: { any: 1 } };
    const { c } = client([() => json(200, { serverTime: SERVER_TIME, card: extended, alsoNew: true })]);
    const r = await c.issues.executionCard("ENG-1");
    expect(r.outcome).toBe("ok");
    expect(r.outcome === "ok" && (r.card as unknown as Record<string, unknown>)["brandNewCloudField"]).toEqual({ any: 1 });
  });

  it("refuses a body with no card as a shape failure", async () => {
    const { c } = client([() => json(200, { serverTime: SERVER_TIME })]);
    expect((await c.issues.executionCard("ENG-1")).outcome).toBe("shape");
  });

  it("refuses a card whose condition is outside the closed list", async () => {
    const { c } = client([() => json(200, { serverTime: SERVER_TIME, card: { ...card(grantedCost()), condition: "sleeping" } })]);
    expect((await c.issues.executionCard("ENG-1")).outcome).toBe("shape");
  });

  it("folds a 403 through the shared failure union", async () => {
    const { c } = client([() => json(403, { error: "nope", required: "mirror:read" })]);
    expect((await c.issues.executionCard("ENG-1")).outcome).toBe("forbidden");
  });
});

async function c_read(one: TicketExecutionCard) {
  const { c } = client([() => json(200, { serverTime: SERVER_TIME, card: one })]);
  return c.issues.executionCard(one.ticket.identifier);
}

describe("executionCards.list", () => {
  it("sends the team and filters as comma-joined query values", async () => {
    const body = { serverTime: SERVER_TIME, cards: [card({ access: "restricted", reason: "account_admin_required" })], nextCursor: "c2" };
    const { net, c } = client([() => json(200, body)]);
    const r = await c.executionCards.list({
      team: "ENG",
      condition: ["running", "queued"],
      linearStateType: ["started"],
      limit: 25,
      cursor: "c1",
    });
    expect(r).toEqual({ outcome: "ok", status: 200, serverTime: SERVER_TIME, cards: body.cards, nextCursor: "c2" });
    const u = new URL(net.calls[0]!.url);
    expect(u.pathname).toBe("/api/v1/execution-cards");
    expect(u.searchParams.get("team")).toBe("ENG");
    expect(u.searchParams.get("condition")).toBe("running,queued");
    expect(u.searchParams.get("linearStateType")).toBe("started");
    expect(u.searchParams.get("limit")).toBe("25");
    expect(u.searchParams.get("cursor")).toBe("c1");
  });

  it("leaves unset filters off the query", async () => {
    const { net, c } = client([() => json(200, { serverTime: SERVER_TIME, cards: [], nextCursor: null })]);
    const r = await c.executionCards.list({ team: "ENG" });
    expect(r.outcome === "ok" && r.nextCursor).toBeNull();
    const u = new URL(net.calls[0]!.url);
    expect([...u.searchParams.keys()]).toEqual(["team"]);
  });

  it("refuses an empty team and an out-of-range limit before any request", async () => {
    const { net, c } = client([]);
    expect((await c.executionCards.list({ team: " " })).outcome).toBe("rejected");
    expect((await c.executionCards.list({ team: "ENG", limit: 0 })).outcome).toBe("rejected");
    expect((await c.executionCards.list({ team: "ENG", limit: 101 })).outcome).toBe("rejected");
    expect(net.calls).toHaveLength(0);
  });

  it("folds the cloud's 400 for an unknown condition into rejected", async () => {
    const { c } = client([() => json(400, { error: "unknown_condition" })]);
    expect((await c.executionCards.list({ team: "ENG" })).outcome).toBe("rejected");
  });

  it("refuses the whole page when one row is not a card", async () => {
    const { c } = client([() => json(200, { serverTime: SERVER_TIME, cards: [card(grantedCost()), { nope: 1 }], nextCursor: null })]);
    expect((await c.executionCards.list({ team: "ENG" })).outcome).toBe("shape");
  });

  it("accepts rows and an envelope carrying fields the SDK does not declare", async () => {
    const row = { ...card(grantedCost()), brandNewCloudField: 1 };
    const { c } = client([() => json(200, { serverTime: SERVER_TIME, cards: [row], nextCursor: null, total: 1 })]);
    expect((await c.executionCards.list({ team: "ENG" })).outcome).toBe("ok");
  });
});

describe("the closed code lists", () => {
  it("name every condition and reason the cloud's contract names", () => {
    expect(EXECUTION_CONDITIONS).toEqual([
      "unknown",
      "complete",
      "paused",
      "running",
      "claimed",
      "blocked",
      "queued",
      "interrupted",
      "failed",
      "cancelled",
      "waiting",
      "idle",
    ]);
    expect(CONDITION_REASON_CODES).toContain("source_stale");
    expect(CONDITION_REASON_CODES).toContain("no_source_read");
    expect(CONDITION_REASON_CODES).toContain("branch_push");
    expect(new Set(CONDITION_REASON_CODES).size).toBe(CONDITION_REASON_CODES.length);
  });

  it("the container check accepts a minimal card and rejects a non-card", () => {
    expect(isTicketExecutionCard(card(grantedCost()))).toBe(true);
    expect(isTicketExecutionCard(null)).toBe(false);
    expect(isTicketExecutionCard([])).toBe(false);
    expect(isTicketExecutionCard({ ...card(grantedCost()), cost: { access: "maybe" } })).toBe(false);
  });
});
