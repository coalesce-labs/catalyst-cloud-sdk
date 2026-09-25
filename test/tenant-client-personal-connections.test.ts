import { describe, expect, it } from "vitest";
import { createTenantClient } from "../src/index";
import { json, scriptedFetch } from "./helpers/scripted-fetch";

const BASE = "https://cloud.example";
const KEY = "ctc_user_personal";

function client(responses: ReturnType<typeof json>[]) {
  const net = scriptedFetch(responses.map((response) => () => response));
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

describe("personal connections", () => {
  it("starts consent with the caller's bearer and returns a short lived same-origin URL", async () => {
    const authorizationUrl = `${BASE}/connect/linear/personal/start?handoff=signed`;
    const { c, net } = client([json(200, { authorizationUrl, expiresAt: 1_800_000_000_000 })]);

    expect(await c.personalConnections.start("linear")).toEqual({
      outcome: "ok", status: 200, authorizationUrl, expiresAt: 1_800_000_000_000,
    });
    expect(net.calls).toHaveLength(1);
    expect(net.calls[0]).toMatchObject({
      method: "GET", url: `${BASE}/connect/linear/personal/start`, body: null,
      headers: { authorization: `Bearer ${KEY}` },
    });
  });

  it("does not return a consent URL for another origin or provider", async () => {
    const { c } = client([
      json(200, { authorizationUrl: "https://other.example/connect/linear/personal/start?handoff=x", expiresAt: 1_800_000_000_000 }),
      json(200, { authorizationUrl: `${BASE}/connect/github/personal/start?handoff=x`, expiresAt: 1_800_000_000_000 }),
      json(200, { authorizationUrl: `${BASE}/connect/linear/personal/start`, expiresAt: 1_800_000_000_000 }),
    ]);
    expect(await c.personalConnections.start("linear")).toMatchObject({ outcome: "shape", status: 200 });
    expect(await c.personalConnections.start("linear")).toMatchObject({ outcome: "shape", status: 200 });
    expect(await c.personalConnections.start("linear")).toMatchObject({ outcome: "shape", status: 200 });
  });

  it("reports absence, expiry, and a usable personal grant as distinct states", async () => {
    const { c, net } = client([
      json(200, { connected: false }),
      json(200, { connected: false, reason: "lapsed", lapsedAt: 1_700_000_000_000 }),
      json(200, { connected: true, linearUserId: "lin-1", grantedScope: null, updatedAt: 1_700_000_000_000, expiresAt: null }),
      json(200, { connected: true, githubUserId: "42", githubLogin: "ryan", updatedAt: 1_700_000_000_000, expiresAt: null }),
    ]);
    expect(await c.personalConnections.status("linear")).toEqual({ outcome: "absent", status: 200 });
    expect(await c.personalConnections.status("linear")).toEqual({ outcome: "lapsed", status: 200, lapsedAt: 1_700_000_000_000 });
    expect(await c.personalConnections.status("linear")).toEqual({
      outcome: "connected", status: 200, provider: "linear", linearUserId: "lin-1",
      grantedScope: null, updatedAt: 1_700_000_000_000, expiresAt: null,
    });
    expect(await c.personalConnections.status("github")).toEqual({
      outcome: "connected", status: 200, provider: "github", githubUserId: "42",
      githubLogin: "ryan", updatedAt: 1_700_000_000_000, expiresAt: null,
    });
    expect(net.calls.map((call) => call.url)).toEqual([
      `${BASE}/me/connections/linear/personal`,
      `${BASE}/me/connections/linear/personal`,
      `${BASE}/me/connections/linear/personal`,
      `${BASE}/me/connections/github/personal`,
    ]);
  });

  it("keeps auth, missing workspace, and uncertain provider checks distinct", async () => {
    const { c } = client([
      json(401, { error: "unauthorized" }),
      json(403, { error: "personal_principal_required" }),
      json(409, { error: "linear_workspace_required" }),
      json(503, { error: "github_grant_check_unavailable" }),
    ]);
    expect(await c.personalConnections.status("linear")).toMatchObject({ outcome: "unauthorized", status: 401 });
    expect(await c.personalConnections.status("linear")).toMatchObject({ outcome: "forbidden", status: 403 });
    expect(await c.personalConnections.start("linear")).toEqual({ outcome: "workspace-required", status: 409 });
    expect(await c.personalConnections.status("github")).toEqual({ outcome: "unavailable", status: 503, provider: "github" });
  });

  it("refuses malformed 200 bodies rather than treating them as disconnected", async () => {
    const { c } = client([
      json(200, { connected: true, linearUserId: "lin-1" }),
      json(200, { connected: false, reason: "mystery" }),
      json(200, { authorizationUrl: `${BASE}/connect/linear/personal/start?handoff=x`, expiresAt: "soon" }),
    ]);
    expect(await c.personalConnections.status("linear")).toMatchObject({ outcome: "shape", status: 200 });
    expect(await c.personalConnections.status("linear")).toMatchObject({ outcome: "shape", status: 200 });
    expect(await c.personalConnections.start("linear")).toMatchObject({ outcome: "shape", status: 200 });
  });
});
