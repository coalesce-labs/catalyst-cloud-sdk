// tenant-client-diagnostics.test.ts — CTC-2132 Phase 3. Seven literal-path open reads (no contract
// fetch), following the `issuesList`/`me()` pattern. Every route gets an "accepts undeclared fields"
// test — the executable form of Decision 4: a cloud-side field addition must NOT become a `shape`
// refusal for every caller.

import { describe, expect, it } from "vitest";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
import { createTenantClient } from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";

function client(script: Parameters<typeof scriptedFetch>[0]) {
  const net = scriptedFetch(script);
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

describe("issues.execution", () => {
  it("returns the report on 200", async () => {
    const { net, c } = client([() => json(200, { identifier: "ENG-1", steps: [] })]);
    const r = await c.issues.execution("ENG-1");
    expect(r).toEqual({ outcome: "ok", status: 200, report: { identifier: "ENG-1", steps: [] } });
    expect(new URL(net.calls[0]!.url).pathname).toBe("/api/v1/issues/ENG-1/execution");
  });

  it("lifts a 404 to its own not-found arm", async () => {
    const { c } = client([() => json(404, { error: "no such issue" })]);
    expect(await c.issues.execution("ENG-999")).toEqual({ outcome: "not-found", status: 404 });
  });

  it("accepts a body carrying fields the SDK does not declare", async () => {
    const { c } = client([() => json(200, { identifier: "ENG-1", brandNewCloudField: 1 })]);
    expect((await c.issues.execution("ENG-1")).outcome).toBe("ok");
  });

  it("folds a 403 through the shared failure union", async () => {
    const { c } = client([() => json(403, { error: "nope", required: "mirror:read" })]);
    const r = await c.issues.execution("ENG-1");
    expect(r.outcome).toBe("forbidden");
    if (r.outcome === "forbidden") expect(r.required).toBe("mirror:read");
  });
});

describe("diagnostics.workEligibility", () => {
  it("sends the route's own param names and returns the body", async () => {
    const { net, c } = client([() => json(200, { team: "ENG", eligibility: { rows: [] } })]);
    const r = await c.diagnostics.workEligibility({ team: "ENG" });
    expect(r.outcome).toBe("ok");
    const sent = new URL(net.calls[0]!.url);
    expect(sent.origin + sent.pathname).toBe(`${BASE}/api/v1/work-eligibility`);
    expect(Object.fromEntries(sent.searchParams)).toEqual({ team: "ENG" });
  });

  it("folds a refusal through the shared failure union", async () => {
    const { c } = client([() => json(403, { error: "nope", required: "mirror:read" })]);
    const r = await c.diagnostics.workEligibility({ team: "ENG" });
    expect(r.outcome).toBe("forbidden");
    if (r.outcome === "forbidden") expect(r.required).toBe("mirror:read");
  });

  it("accepts a body carrying fields the SDK does not declare", async () => {
    const { c } = client([() => json(200, { team: "ENG", eligibility: { rows: [] }, brandNewCloudField: 1 })]);
    expect((await c.diagnostics.workEligibility({ team: "ENG" })).outcome).toBe("ok");
  });
});

describe("diagnostics.dispatchQueue", () => {
  it("reads the ENVELOPE, not a bare array", async () => {
    const { c } = client([
      () => json(200, { team: "ENG", published_at: 1, age_ms: 2, stale: false, source: "x", entries: [{ position: 1, identifier: "ENG-1" }] }),
    ]);
    const r = await c.diagnostics.dispatchQueue({ team: "ENG" });
    expect(r.outcome === "ok" && r.queue.entries).toHaveLength(1);
  });

  it("refuses a BARE ARRAY body as `shape` (negative control for the envelope)", async () => {
    const { c } = client([() => json(200, [{ position: 1 }])]);
    expect(await c.diagnostics.dispatchQueue({ team: "ENG" })).toMatchObject({ outcome: "shape", status: 200 });
  });

  it("accepts a body carrying fields the SDK does not declare", async () => {
    const { c } = client([() => json(200, { entries: [], brandNewCloudField: 1 })]);
    expect((await c.diagnostics.dispatchQueue({ team: "ENG" })).outcome).toBe("ok");
  });
});

describe("diagnostics.fleetActivity", () => {
  it("returns the bare array", async () => {
    const { c } = client([() => json(200, [{ id: "a1" }])]);
    const r = await c.diagnostics.fleetActivity();
    expect(r).toEqual({ outcome: "ok", status: 200, rows: [{ id: "a1" }] });
  });

  it("accepts rows carrying fields the SDK does not declare", async () => {
    const { c } = client([() => json(200, [{ id: "a1", brandNewCloudField: 1 }])]);
    expect((await c.diagnostics.fleetActivity()).outcome).toBe("ok");
  });

  it("a non-array body is `shape`", async () => {
    const { c } = client([() => json(200, { not: "an array" })]);
    expect(await c.diagnostics.fleetActivity()).toMatchObject({ outcome: "shape", status: 200 });
  });
});

describe("diagnostics.agentRoster", () => {
  it("returns the bare array", async () => {
    const { c } = client([() => json(200, [{ id: "agent-1" }])]);
    expect(await c.diagnostics.agentRoster()).toEqual({ outcome: "ok", status: 200, rows: [{ id: "agent-1" }] });
  });

  it("accepts rows carrying fields the SDK does not declare", async () => {
    const { c } = client([() => json(200, [{ id: "agent-1", brandNewCloudField: 1 }])]);
    expect((await c.diagnostics.agentRoster()).outcome).toBe("ok");
  });
});

describe("diagnostics.leaseAttributions", () => {
  it("sends BOTH required params", async () => {
    const { net, c } = client([() => json(200, { rows: [] })]);
    await c.diagnostics.leaseAttributions({ ticket: "ENG-1", phase: "implement" });
    const sent = new URL(net.calls[0]!.url);
    expect(Object.fromEntries(sent.searchParams)).toEqual({ ticket: "ENG-1", phase: "implement" });
  });

  it("is tolerant of an object OR array body", async () => {
    const { c: cObj } = client([() => json(200, { rows: [] })]);
    expect((await cObj.diagnostics.leaseAttributions({ ticket: "ENG-1", phase: "implement" })).outcome).toBe("ok");
    const { c: cArr } = client([() => json(200, [{ ticket: "ENG-1" }])]);
    expect((await cArr.diagnostics.leaseAttributions({ ticket: "ENG-1", phase: "implement" })).outcome).toBe("ok");
  });

  it("a 400 (missing param) folds through the shared failure union", async () => {
    const { c } = client([() => json(400, { error: "ticket and phase are both required" })]);
    expect(await c.diagnostics.leaseAttributions({ ticket: "ENG-1", phase: "implement" })).toMatchObject({
      outcome: "rejected",
      status: 400,
    });
  });
});

describe("diagnostics.codingAccounts", () => {
  it("returns an OBJECT with an accounts array", async () => {
    const { c } = client([() => json(200, { accounts: [{ id: "acc-1" }] })]);
    const r = await c.diagnostics.codingAccounts();
    expect(r).toEqual({ outcome: "ok", status: 200, report: { accounts: [{ id: "acc-1" }] } });
  });

  it("refuses a body with no accounts array as `shape`", async () => {
    const { c } = client([() => json(200, { notAccounts: [] })]);
    expect(await c.diagnostics.codingAccounts()).toMatchObject({ outcome: "shape", status: 200 });
  });

  it("accepts a body carrying fields the SDK does not declare", async () => {
    const { c } = client([() => json(200, { accounts: [], observedAtMs: 7 })]);
    expect((await c.diagnostics.codingAccounts()).outcome).toBe("ok");
  });
});
