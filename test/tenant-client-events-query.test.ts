// tenant-client-events-query.test.ts — CTC-4549. `events.query` reads one page of a tenant's event
// history filtered by ticket and type (GET /api/v1/events/query), and `events.pages` follows `next`
// until the history is drained. A server without the route answers 404, which comes back as
// `unsupported`, so a caller can fall back instead of reading "no events".

import { describe, expect, expectTypeOf, it } from "vitest";
import { json, scriptedFetch, text } from "./helpers/scripted-fetch";
import { createTenantClient, type EventQueryResult, type QueriedEvent } from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";
const COVERAGE = { indexedFromSeq: 1, indexedToSeq: 90 };

function client(script: Parameters<typeof scriptedFetch>[0]) {
  const net = scriptedFetch(script);
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

function event(sequence: number) {
  return { sequence, type: "relay.phase.completed", ticket: "ENG-1", brandNewCloudField: true };
}

describe("events.query", () => {
  it("sends only the params given, with the server's own names", async () => {
    const { net, c } = client([() => json(200, { events: [], next: null, coverage: COVERAGE })]);
    await c.events.query({});
    await c.events.query({ ticket: "ENG-1", type: ["relay.phase.completed", "lease.claimed"], limit: 20, order: "asc", afterSeq: 5, beforeSeq: 80 });
    expect(net.calls[0]!.url).toBe(`${BASE}/api/v1/events/query`);
    const sent = new URL(net.calls[1]!.url);
    expect(sent.pathname).toBe("/api/v1/events/query");
    expect(sent.searchParams.get("ticket")).toBe("ENG-1");
    expect(sent.searchParams.getAll("type")).toEqual(["relay.phase.completed", "lease.claimed"]);
    expect(sent.searchParams.get("limit")).toBe("20");
    expect(sent.searchParams.get("order")).toBe("asc");
    expect(sent.searchParams.get("afterSeq")).toBe("5");
    expect(sent.searchParams.get("beforeSeq")).toBe("80");
  });

  it("returns the typed page, events verbatim", async () => {
    const next = { param: "beforeSeq", value: 41 };
    const { c } = client([() => json(200, { events: [event(42)], next, coverage: COVERAGE })]);
    const r = await c.events.query({ ticket: "ENG-1", type: "relay.phase.completed" });
    expect(r).toEqual({ outcome: "ok", status: 200, events: [event(42)], next, coverage: COVERAGE });
  });

  it("reports a 404 as unsupported by this server", async () => {
    const { c } = client([() => text(404, "Not Found")]);
    const r = await c.events.query({ ticket: "ENG-1" });
    expect(r.outcome).toBe("unsupported");
    expect(r).toMatchObject({ outcome: "unsupported", status: 404 });
  });

  it("refuses a body that is not the documented page", async () => {
    const { c } = client([
      () => json(200, { events: "nope", next: null, coverage: COVERAGE }),
      () => json(200, { events: [], next: { param: "sideways", value: 1 }, coverage: COVERAGE }),
      () => json(200, { events: [7], next: null, coverage: COVERAGE }),
      () => json(200, { events: [], next: null }),
    ]);
    for (let i = 0; i < 4; i++) expect((await c.events.query({})).outcome).toBe("shape");
  });

  it("folds another named 400 through the shared failure union", async () => {
    const { c } = client([() => json(400, { error: "invalid_limit", min: 1, max: 200 })]);
    const r = await c.events.query({ limit: 500 });
    expect(r).toMatchObject({ outcome: "rejected", status: 400, reason: "invalid_limit" });
  });
});

describe("events.pages", () => {
  it("follows next until it is null, keeping the other params", async () => {
    const { net, c } = client([
      () => json(200, { events: [event(9), event(8)], next: { param: "beforeSeq", value: 8 }, coverage: COVERAGE }),
      () => json(200, { events: [event(5)], next: null, coverage: COVERAGE }),
    ]);
    const pages: EventQueryResult[] = [];
    for await (const page of c.events.pages({ ticket: "ENG-1", afterSeq: 2 })) pages.push(page);
    expect(pages.map((p) => (p.outcome === "ok" ? p.events.length : p.outcome))).toEqual([2, 1]);
    const second = new URL(net.calls[1]!.url).searchParams;
    expect(Object.fromEntries(second)).toEqual({ ticket: "ENG-1", afterSeq: "2", beforeSeq: "8" });
  });

  it("ends after the first failure, yielding it", async () => {
    const { net, c } = client([
      () => json(200, { events: [event(9)], next: { param: "beforeSeq", value: 9 }, coverage: COVERAGE }),
      () => json(500, { error: "archive_object_missing", sequence: 4 }),
    ]);
    const outcomes: string[] = [];
    for await (const page of c.events.pages({})) outcomes.push(page.outcome);
    expect(outcomes).toEqual(["ok", "http"]);
    expect(net.calls.length).toBe(2);
  });

  it("stops with a shape failure when next does not advance", async () => {
    const { net, c } = client([
      () => json(200, { events: [event(9)], next: { param: "beforeSeq", value: 9 }, coverage: COVERAGE }),
    ]);
    const outcomes: string[] = [];
    for await (const page of c.events.pages({})) outcomes.push(page.outcome);
    expect(outcomes).toEqual(["ok", "ok", "shape"]);
    expect(net.calls.length).toBe(2);
  });

  it("yields unsupported once on an older server", async () => {
    const { c } = client([() => text(404, "Not Found")]);
    const outcomes: string[] = [];
    for await (const page of c.events.pages({})) outcomes.push(page.outcome);
    expect(outcomes).toEqual(["unsupported"]);
  });
});

// ── Server refusals and stubs, with bodies copied from catalyst-cloud main's
//    apps/mirror/src/event-index/query-route.ts and store.ts (bodyUnavailableStub). ────────────────

const MAX_BYTES = 1024 * 1024;

/** store.ts bodyUnavailableStub: an event whose archive write the r2 lane gave up on. */
const STUB = {
  tenantId: "tenant-0",
  sequence: 10,
  eventId: "tenant-0-e10",
  type: "phase.plan.failed",
  ticket: "ENG-1",
  bodyUnavailable: true,
  reason: "archive_write_failed",
} as const;

describe("events.query — a bodyUnavailable stub is its own typed arm", () => {
  it("returns the stub verbatim beside full events, and a caller can branch on it", async () => {
    const { c } = client([() => json(200, { events: [event(11), STUB, event(9)], next: null, coverage: COVERAGE })]);
    const r = await c.events.query({ ticket: "ENG-1" });
    if (r.outcome !== "ok") throw new Error(`expected ok, got ${r.outcome}`);
    expect(r.events[1]).toEqual(STUB);
    const reasons = r.events.map((e) => (e.bodyUnavailable === true ? e.reason : null));
    expect(reasons).toEqual([null, "archive_write_failed", null]);
  });

  it("types the union so the stub branch has its fields and the full branch has its sequence", () => {
    const e = {} as QueriedEvent;
    if (e.bodyUnavailable === true) {
      expectTypeOf(e.reason).toEqualTypeOf<string>();
      expectTypeOf(e.eventId).toEqualTypeOf<string>();
      expectTypeOf(e.sequence).toEqualTypeOf<number>();
    } else {
      expectTypeOf(e.sequence).toEqualTypeOf<number>();
      expectTypeOf(e.type).toEqualTypeOf<string>();
    }
  });

  it("refuses a stub missing its sequence", async () => {
    const { c } = client([
      () => json(200, { events: [{ ...STUB, sequence: "ten" }], next: null, coverage: COVERAGE }),
    ]);
    expect((await c.events.query({})).outcome).toBe("shape");
  });
});

describe("events.query — 413 event_too_large keeps the skip cursor", () => {
  const tooLarge = {
    error: "event_too_large",
    sequence: 77,
    maxBytes: MAX_BYTES,
    eventBytes: 1_300_000,
    next: { param: "beforeSeq", value: 77 },
  };

  it("is a too-large arm naming the event and where to resume past it", async () => {
    const { c } = client([() => json(413, tooLarge)]);
    expect(await c.events.query({ ticket: "ENG-1" })).toEqual({
      outcome: "too-large",
      status: 413,
      sequence: 77,
      maxBytes: MAX_BYTES,
      eventBytes: 1_300_000,
      next: { param: "beforeSeq", value: 77 },
    });
  });

  it("pages() yields it and carries on past the event", async () => {
    const { net, c } = client([
      () => json(200, { events: [event(80), event(79)], next: { param: "beforeSeq", value: 79 }, coverage: COVERAGE }),
      () => json(413, tooLarge),
      () => json(200, { events: [event(76)], next: null, coverage: COVERAGE }),
    ]);
    const outcomes: string[] = [];
    for await (const page of c.events.pages({ ticket: "ENG-1" })) outcomes.push(page.outcome);
    expect(outcomes).toEqual(["ok", "too-large", "ok"]);
    expect(new URL(net.calls[2]!.url).searchParams.get("beforeSeq")).toBe("77");
  });

  it("a 413 without a usable cursor is a shape failure, not a silent stop", async () => {
    const { c } = client([() => json(413, { ...tooLarge, next: null })]);
    expect((await c.events.query({})).outcome).toBe("shape");
  });
});

describe("events.query — a 503 is retryable, with the request's own resume cursor", () => {
  it.each([
    [{ error: "archive_read_failed", sequence: 12 }, 12],
    [{ error: "index_unavailable" }, null],
    [{ error: "archive_unavailable" }, null],
  ])("%o is unavailable and retryable", async (body, sequence) => {
    const { c } = client([() => json(503, body)]);
    expect(await c.events.query({ ticket: "ENG-1", order: "asc", afterSeq: 7 })).toEqual({
      outcome: "unavailable",
      status: 503,
      error: body.error,
      retryable: true,
      sequence,
      resume: { param: "afterSeq", value: 7 },
    });
  });

  it("the resume cursor is null for a first page sent without one", async () => {
    const { c } = client([() => json(503, { error: "index_unavailable" })]);
    expect(await c.events.query({ ticket: "ENG-1" })).toMatchObject({ outcome: "unavailable", resume: null });
  });

  it("pages() yields it and stops, resuming from the last page's next", async () => {
    const { c } = client([
      () => json(200, { events: [event(9)], next: { param: "beforeSeq", value: 9 }, coverage: COVERAGE }),
      () => json(503, { error: "archive_read_failed", sequence: 8 }),
    ]);
    const pages: EventQueryResult[] = [];
    for await (const page of c.events.pages({})) pages.push(page);
    expect(pages.map((p) => p.outcome)).toEqual(["ok", "unavailable"]);
    expect(pages[1]).toMatchObject({ retryable: true, resume: { param: "beforeSeq", value: 9 } });
  });
});

describe("events.query — 400 unknown_event_type keeps its types list", () => {
  it("is an event-type-refused arm naming every unknown type", async () => {
    const body = {
      error: "unknown_event_type",
      types: ["phase.completed", "phase.*.failed"],
      detail: "type must be a durable event type from the registry; globs are not supported yet",
    };
    const { c } = client([() => json(400, body)]);
    expect(await c.events.query({ type: ["phase.completed", "phase.*.failed"] })).toEqual({
      outcome: "event-type-refused",
      status: 400,
      types: ["phase.completed", "phase.*.failed"],
      reason: body.detail,
    });
  });
});
