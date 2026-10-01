// tenant-client-events-query.test.ts — CTC-4549. `events.query` reads one page of a tenant's event
// history filtered by ticket and type (GET /api/v1/events/query), and `events.pages` follows `next`
// until the history is drained. A server without the route answers 404, which comes back as
// `unsupported`, so a caller can fall back instead of reading "no events".

import { describe, expect, it } from "vitest";
import { json, scriptedFetch, text } from "./helpers/scripted-fetch";
import { createTenantClient, type EventQueryResult } from "../src/index";

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

  it("folds a named 400 through the shared failure union", async () => {
    const { c } = client([() => json(400, { error: "unknown_event_type", types: ["phase.completed"] })]);
    const r = await c.events.query({ type: "phase.completed" });
    expect(r).toMatchObject({ outcome: "rejected", status: 400, reason: "unknown_event_type" });
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
      () => json(503, { error: "index_unavailable" }),
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
