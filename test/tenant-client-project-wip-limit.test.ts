import { describe, expect, it } from "vitest";
import { createTenantClient } from "../src/index";
import { json, scriptedFetch } from "./helpers/scripted-fetch";

// CTC-3780 — a project's WIP limit, read by any member and set by an admin or owner, over
// GET|POST /api/v1/agent/team-wip-limit with the person's own key.
const BASE = "https://cloud.example";
const KEY = "ctc_user_personal";
const TEAM = { id: "3f2a8c1e-0000-4a11-9a55-0d1f2b3c4d5e", key: "CTC" };
const view = (limit: number, source: "project" | "flag" | "default", stored: number | null) => ({
  team: TEAM,
  limit,
  source,
  stored,
  inProgress: 2,
  countedAt: 1_790_000_000_000,
});

function client(responses: ReturnType<typeof json>[]) {
  const net = scriptedFetch(responses.map((response) => () => response));
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

describe("project WIP limit", () => {
  it("an admin sets 8, then reads back 8 with source project", async () => {
    const { c, net } = client([json(200, view(8, "project", 8)), json(200, view(8, "project", 8))]);
    expect(await c.setProjectWipLimit("CTC", 8)).toEqual({ outcome: "ok", status: 200, ...view(8, "project", 8) });
    expect(await c.getProjectWipLimit("CTC")).toMatchObject({ outcome: "ok", limit: 8, source: "project", stored: 8, inProgress: 2 });
    expect(net.calls.map((call) => [call.method, call.url])).toEqual([
      ["POST", `${BASE}/api/v1/agent/team-wip-limit`],
      ["GET", `${BASE}/api/v1/agent/team-wip-limit?team=CTC`],
    ]);
    expect(net.calls[0]?.body).toEqual({ team: "CTC", limit: 8 });
    expect(net.calls.every((call) => call.headers.authorization === `Bearer ${KEY}`)).toBe(true);
  });

  it("a member reads the value but gets a permission error on set", async () => {
    const { c } = client([
      json(200, view(8, "project", 8)),
      json(403, { error: "forbidden", message: "changing a project's WIP limit needs an admin or owner of this workspace" }),
    ]);
    expect(await c.getProjectWipLimit("CTC")).toMatchObject({ outcome: "ok", limit: 8 });
    expect(await c.setProjectWipLimit("CTC", 3)).toMatchObject({ outcome: "forbidden", status: 403, error: "forbidden" });
  });

  it("null sets the project back to the default", async () => {
    const { c, net } = client([json(200, view(12, "default", null))]);
    expect(await c.setProjectWipLimit("CTC", null)).toMatchObject({ outcome: "ok", limit: 12, source: "default", stored: null });
    expect(net.calls[0]?.body).toEqual({ team: "CTC", limit: null });
  });

  it("encodes the team reference and names the cloud's refusals", async () => {
    const { c, net } = client([
      json(401, { error: "unauthorized" }),
      json(404, { error: "team-unknown", message: "no team matches that reference" }),
      json(400, { error: "invalid-limit", message: "body must be JSON {limit: number} or {limit: null} for the default" }),
    ]);
    expect(await c.getProjectWipLimit("A B")).toMatchObject({ outcome: "unauthorized", status: 401 });
    expect(await c.getProjectWipLimit("NOPE")).toMatchObject({ outcome: "rejected", status: 404, error: "team-unknown" });
    expect(await c.setProjectWipLimit("CTC", 8)).toMatchObject({ outcome: "rejected", status: 400, error: "invalid-limit" });
    expect(net.calls[0]?.url).toBe(`${BASE}/api/v1/agent/team-wip-limit?team=A%20B`);
  });

  it("refuses a non-finite limit locally: JSON would send NaN as null, which resets to the default", async () => {
    const { c, net } = client([]);
    for (const bad of [Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(await c.setProjectWipLimit("CTC", bad)).toMatchObject({ outcome: "rejected", status: 0, error: "invalid-limit" });
    }
    expect(net.calls).toHaveLength(0);
  });

  it("a body missing a pinned field is a shape failure, not ok", async () => {
    const { inProgress: _dropped, ...partial } = view(8, "project", 8);
    const { c } = client([json(200, partial), json(200, { ...view(8, "project", 8), source: "somewhere" })]);
    expect(await c.getProjectWipLimit("CTC")).toMatchObject({ outcome: "shape" });
    expect(await c.getProjectWipLimit("CTC")).toMatchObject({ outcome: "shape" });
  });
});
