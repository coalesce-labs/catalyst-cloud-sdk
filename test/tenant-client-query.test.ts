// tenant-client-query.test.ts — CTC-2132 Phase 4. cycles, search, workflow-stages, snapshot head.
// `@catalyst-cloud/read-model` does not publish CycleView/SearchView (verified against the installed
// package on 2026-09-18) so the SDK declares its own structural row types (Decision 4).

import { describe, expect, it } from "vitest";
import { json, scriptedFetch, text } from "./helpers/scripted-fetch";
import { createTenantClient } from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";

function client(script: Parameters<typeof scriptedFetch>[0]) {
  const net = scriptedFetch(script);
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

describe("cycles.list", () => {
  it("returns the bare array the route answers", async () => {
    const { c } = client([() => json(200, [{ id: "cycle-1", number: 4 }])]);
    expect(await c.cycles.list()).toEqual({ outcome: "ok", status: 200, rows: [{ id: "cycle-1", number: 4 }] });
  });

  it("accepts rows carrying fields the SDK does not declare", async () => {
    const { c } = client([() => json(200, [{ id: "cycle-1", brandNewCloudField: 1 }])]);
    expect((await c.cycles.list()).outcome).toBe("ok");
  });
});

describe("search", () => {
  it("sends q (and limit when given) and returns the four result buckets", async () => {
    const { net, c } = client([() => json(200, { issues: [], pulls: [], projects: [], initiatives: [] })]);
    const r = await c.search({ q: "auth", limit: 10 });
    expect(new URL(net.calls[0]!.url).searchParams.get("q")).toBe("auth");
    expect(new URL(net.calls[0]!.url).searchParams.get("limit")).toBe("10");
    expect(r.outcome).toBe("ok");
  });

  it("accepts a body with a bucket the SDK does not declare", async () => {
    const { c } = client([() => json(200, { issues: [], somethingNew: [] })]);
    expect((await c.search({ q: "auth" })).outcome).toBe("ok");
  });
});

describe("workflowStages", () => {
  it("accepts the {stages, source} envelope", async () => {
    const { c } = client([() => json(200, { stages: [{ id: "s1" }], source: "linear" })]);
    expect(await c.workflowStages()).toEqual({ outcome: "ok", status: 200, stages: [{ id: "s1" }], source: "linear" });
  });

  it("accepts a BARE ARRAY and normalizes it to the same result shape", async () => {
    const { c } = client([() => json(200, [{ id: "s1" }])]);
    expect(await c.workflowStages()).toEqual({ outcome: "ok", status: 200, stages: [{ id: "s1" }], source: null });
  });

  it("refuses a body that is neither, as `shape` (negative control)", async () => {
    const { c } = client([() => json(200, { neither: true })]);
    expect(await c.workflowStages()).toMatchObject({ outcome: "shape", status: 200 });
  });
});

describe("snapshot.head", () => {
  it("sends head=1", async () => {
    const { net, c } = client([() => text(200, "", { "X-Mirror-Cursor": "42" })]);
    await c.snapshot.head();
    expect(new URL(net.calls[0]!.url).searchParams.get("head")).toBe("1");
  });

  it("reads the head from X-Mirror-Cursor and reports source:'header'", async () => {
    const { c } = client([() => text(200, "", { "X-Mirror-Cursor": "42" })]);
    expect(await c.snapshot.head()).toEqual({ outcome: "ok", status: 200, head: 42, source: "header" });
  });

  it("falls back to a JSON body's head/seq/cursor and reports source:'body'", async () => {
    const { c } = client([() => json(200, { head: 99 })]);
    expect(await c.snapshot.head()).toEqual({ outcome: "ok", status: 200, head: 99, source: "body" });
  });

  it("falls back to the first NDJSON line", async () => {
    const { c } = client([() => text(200, '{"seq":7}\n{"seq":8}\n', { "content-type": "application/x-ndjson" })]);
    expect(await c.snapshot.head()).toEqual({ outcome: "ok", status: 200, head: 7, source: "body" });
  });

  it("returns `shape` when no path yields a finite head", async () => {
    const { c } = client([() => text(200, "", { "content-type": "application/x-ndjson" })]);
    expect(await c.snapshot.head()).toMatchObject({ outcome: "shape", status: 200 });
  });
});
