// tenant-client-select.test.ts — CTC-4549. `issues.get` and `issues.execution` can ask the server for
// only the top-level keys a caller needs (`?fields=a,b`, `?projection=<name>`). A call without options
// must send exactly the request it sent before this ticket, and the server's 400 for a name outside
// its closed catalog must come back as its own arm, with the allowed names, not the generic bucket.

import { describe, expect, expectTypeOf, it } from "vitest";
import type { IssueDetailView } from "@catalyst-cloud/read-model";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
import {
  createTenantClient,
  type IssueGetResult,
  type TicketExecutionResult,
  type TicketRepairLoop,
} from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";

function client(script: Parameters<typeof scriptedFetch>[0]) {
  const net = scriptedFetch(script);
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

describe("a default call sends no selection params", () => {
  it("issues.get(id) sends the same request as before", async () => {
    const { net, c } = client([() => json(200, { identifier: "ENG-1", title: "t" })]);
    await c.issues.get("ENG-1");
    expect(net.calls[0]!.url).toBe(`${BASE}/api/v1/issues/ENG-1`);
  });

  it("issues.execution(id) sends the same request as before", async () => {
    const { net, c } = client([() => json(200, { ticket: "ENG-1" })]);
    await c.issues.execution("ENG-1");
    expect(net.calls[0]!.url).toBe(`${BASE}/api/v1/issues/ENG-1/execution`);
  });

  it("keeps the unprojected return types", () => {
    const { c } = client([() => json(200, {})]);
    expectTypeOf(c.issues.get("ENG-1")).resolves.toEqualTypeOf<IssueGetResult>();
    expectTypeOf(c.issues.execution("ENG-1")).resolves.toEqualTypeOf<TicketExecutionResult>();
  });
});

describe("issues.get with a selection", () => {
  it("sends fields joined by commas", async () => {
    const { net, c } = client([() => json(200, { identifier: "ENG-1", title: "t", state: "Todo" })]);
    const r = await c.issues.get("ENG-1", { fields: ["title", "state"] });
    const sent = new URL(net.calls[0]!.url);
    expect(sent.pathname).toBe("/api/v1/issues/ENG-1");
    expect(sent.searchParams.get("fields")).toBe("title,state");
    expect(sent.searchParams.has("projection")).toBe(false);
    expect(r).toEqual({ outcome: "ok", issue: { identifier: "ENG-1", title: "t", state: "Todo" }, head: null });
  });

  it("sends a projection by name, and both together", async () => {
    const { net, c } = client([() => json(200, { identifier: "ENG-1" })]);
    await c.issues.get("ENG-1", { projection: "status" });
    await c.issues.get("ENG-1", { projection: "brief", fields: ["comments"] });
    const first = new URL(net.calls[0]!.url).searchParams;
    const second = new URL(net.calls[1]!.url).searchParams;
    expect(Object.fromEntries(first)).toEqual({ projection: "status" });
    expect(Object.fromEntries(second)).toEqual({ fields: "comments", projection: "brief" });
  });

  it("types a projected issue as partial with the identifier kept", async () => {
    const { c } = client([() => json(200, { identifier: "ENG-1" })]);
    const r = await c.issues.get("ENG-1", { projection: "status" });
    if (r.outcome === "ok") {
      expectTypeOf(r.issue.identifier).toEqualTypeOf<IssueDetailView["identifier"]>();
      expectTypeOf(r.issue.title).toEqualTypeOf<IssueDetailView["title"] | undefined>();
    }
    expect(r.outcome).toBe("ok");
  });

  it("lifts unknown_field to the select-refused arm with the allowed names", async () => {
    const { c } = client([
      () => json(400, { error: "unknown_field", field: "titel", allowed: ["identifier", "title"] }),
    ]);
    const r = await c.issues.get("ENG-1", { fields: ["titel"] });
    expect(r).toEqual({
      outcome: "select-refused",
      status: 400,
      error: "unknown_field",
      name: "titel",
      allowed: ["identifier", "title"],
      reason: "unknown_field",
    });
  });

  it("lifts unknown_projection and invalid_field the same way", async () => {
    const { c } = client([
      () => json(400, { error: "unknown_projection", projection: "full", allowed: ["status", "brief"] }),
      () => json(400, { error: "invalid_field", field: "fields", allowed: ["identifier"] }),
    ]);
    const projection = await c.issues.get("ENG-1", { projection: "full" as "status" });
    const empty = await c.issues.get("ENG-1", { fields: [] });
    expect(projection).toMatchObject({ outcome: "select-refused", error: "unknown_projection", name: "full", allowed: ["status", "brief"] });
    expect(empty).toMatchObject({ outcome: "select-refused", error: "invalid_field", name: "fields" });
  });

  it("leaves any other 400 in the shared failure union", async () => {
    const { c } = client([() => json(400, { error: "bad_identifier" })]);
    const r = await c.issues.get("ENG-1", { fields: ["title"] });
    expect(r.outcome).toBe("rejected");
  });

  it("keeps not-found as its own arm", async () => {
    const { c } = client([() => json(404, { error: "no such issue" })]);
    expect(await c.issues.get("ENG-9", { projection: "status" })).toEqual({ outcome: "not-found", status: 404 });
  });
});

describe("issues.execution with a selection", () => {
  it("sends fields and projection", async () => {
    const { net, c } = client([() => json(200, { ticket: "ENG-1", lease: null, unreadable: [] })]);
    const r = await c.issues.execution("ENG-1", { projection: "failures", fields: ["runs"] });
    const sent = new URL(net.calls[0]!.url);
    expect(sent.pathname).toBe("/api/v1/issues/ENG-1/execution");
    expect(Object.fromEntries(sent.searchParams)).toEqual({ fields: "runs", projection: "failures" });
    expect(r).toEqual({ outcome: "ok", status: 200, report: { ticket: "ENG-1", lease: null, unreadable: [] } });
  });

  it("lifts the catalog 400 to the select-refused arm", async () => {
    const { c } = client([
      () => json(400, { error: "unknown_projection", projection: "brief", allowed: ["status", "failures"] }),
    ]);
    const r = await c.issues.execution("ENG-1", { projection: "brief" as "status" });
    expect(r).toMatchObject({ outcome: "select-refused", status: 400, error: "unknown_projection", allowed: ["status", "failures"] });
  });
});

// ── repairLoop (CTC-1889): the server's detail handler adds it after the view builder, and both
//    projections (`status`, `brief`) name it, so it is typed on the full and the selected issue. ──

const REPAIR_LOOP = {
  remediateRounds: 2,
  validateAttempts: 3,
  hold: { reason: "validate-budget", headSha: "abc123", heldAtMs: 1_700_000_000_000 },
  parks: [{ phase: "validate", sentinel: "repeated_failure", label: "repeated failures" }],
};

describe("repairLoop is typed on the issue", () => {
  it("a projected read carries it", async () => {
    const { c } = client([() => json(200, { identifier: "ENG-1", repairLoop: REPAIR_LOOP })]);
    const r = await c.issues.get("ENG-1", { projection: "status" });
    if (r.outcome !== "ok") throw new Error(r.outcome);
    expect(r.issue.repairLoop).toEqual(REPAIR_LOOP);
    expectTypeOf(r.issue.repairLoop).toEqualTypeOf<TicketRepairLoop | undefined>();
  });

  it("the full read carries it too", async () => {
    const { c } = client([() => json(200, { identifier: "ENG-1", title: "t", repairLoop: REPAIR_LOOP })]);
    const r = await c.issues.get("ENG-1");
    if (r.outcome !== "ok") throw new Error(r.outcome);
    expect(r.issue.repairLoop?.remediateRounds).toBe(2);
    expectTypeOf(r.issue.repairLoop).toEqualTypeOf<TicketRepairLoop | undefined>();
    expectTypeOf<TicketRepairLoop["hold"]>().toEqualTypeOf<{
      reason: string;
      headSha: string | null;
      heldAtMs: number;
    } | null>();
  });
});
