import { describe, expect, it } from "vitest";
import fixture from "./fixtures/tenant-contract.fixture.json";
import { createTenantClient } from "../src/index";
import { json, scriptedFetch, type Scripted } from "./helpers/scripted-fetch";

const routes = [
  { method: "GET", path: "/api/v1/agent/portal-servers", takesWriteBudgetUnit: false, since: "1.18.0" },
  { method: "POST", path: "/api/v1/agent/portal-servers/register", takesWriteBudgetUnit: false, since: "1.18.0" },
  { method: "POST", path: "/api/v1/agent/portal-servers/remove", takesWriteBudgetUnit: false, since: "1.18.0" },
];
const input = { name: "linear", url: "https://mcp.linear.app/mcp", auth: { kind: "bearer", secretName: "LINEAR_API_KEY" } } as const;
const server = { id: "server-1", ...input, status: "pending" as const };

function client(script: Scripted[], advertised = routes) {
  const net = scriptedFetch([() => json(200, { ...fixture, routes: advertised }, { etag: '"mcp"' }), ...script]);
  return { net, c: createTenantClient({ key: "ctc_acct_test", baseUrl: "https://cloud.example", fetch: net.fetch }) };
}

describe("portal server registration client", () => {
  it("preserves pending approval and sends only vault reference names", async () => {
    const { c, net } = client([() => json(201, { outcome: "registered", server })]);
    expect(await c.agent.portalServerRegister(input)).toEqual({ outcome: "registered", server, status: 201 });
    expect(net.calls[1]).toMatchObject({ method: "POST", url: "https://cloud.example/api/v1/agent/portal-servers/register", body: input });
  });

  it("follows moved contract paths for register, list and remove", async () => {
    const moved = routes.map((r) => ({ ...r, path: r.path.replace("/api/v1/agent/", "/api/v2/agent/") }));
    const { c, net } = client([
      () => json(200, { outcome: "registered", server }),
      () => json(200, { outcome: "ok", servers: [server] }),
      () => json(200, { outcome: "removed", removed: true }),
    ], moved);
    await c.agent.portalServerRegister(input);
    expect(await c.agent.portalServers()).toEqual({ outcome: "ok", servers: [server], status: 200 });
    expect(await c.agent.portalServerRemove({ name: "linear" })).toEqual({ outcome: "removed", removed: true, status: 200 });
    expect(net.calls.slice(1).map((r) => [r.method, r.url])).toEqual([
      ["POST", "https://cloud.example/api/v2/agent/portal-servers/register"],
      ["GET", "https://cloud.example/api/v2/agent/portal-servers"],
      ["POST", "https://cloud.example/api/v2/agent/portal-servers/remove"],
    ]);
    expect(net.calls[3]?.body).toEqual({ name: "linear" });
  });

  it.each(["none", "headers"] as const)("supports %s authentication without values", async (kind) => {
    const auth = kind === "none" ? { kind } : { kind, headers: [{ name: "X-API-Key", secretName: "API_KEY" }] };
    const { c, net } = client([() => json(201, { outcome: "registered", server: { ...server, auth } })]);
    await c.agent.portalServerRegister({ ...input, auth });
    expect(net.calls[1]?.body).toEqual({ ...input, auth });
  });

  it("keeps a named missing-secret refusal", async () => {
    const { c } = client([() => json(400, { error: "secret_not_found", reason: "vault secret NOPE is absent" })]);
    expect(await c.agent.portalServerRegister(input)).toMatchObject({ outcome: "rejected", reason: "vault secret NOPE is absent" });
  });

  it("does not submit a registration when the route is absent", async () => {
    const { c, net } = client([], []);
    expect(await c.agent.portalServerRegister(input)).toMatchObject({ outcome: "route-unknown", route: "portal-servers/register" });
    expect(net.calls).toHaveLength(1);
  });

  it("preserves idempotent removal and rejects malformed success bodies", async () => {
    const { c } = client([
      () => json(200, { outcome: "removed", removed: false }),
      () => json(200, { outcome: "ok", servers: "not-an-array" }),
      () => json(200, { outcome: "registered", server: { id: "bad" } }),
    ]);
    expect(await c.agent.portalServerRemove({ name: "gone" })).toEqual({ outcome: "removed", removed: false, status: 200 });
    expect(await c.agent.portalServers()).toMatchObject({ outcome: "shape" });
    expect(await c.agent.portalServerRegister(input)).toMatchObject({ outcome: "shape" });
  });
  it("does not treat a success-shaped error response as success", async () => {
    const { c } = client([() => json(503, { outcome: "registered", server })]);
    expect(await c.agent.portalServerRegister(input)).toMatchObject({ outcome: "http", status: 503 });
  });

  it("rejects malformed status arrays instead of treating them as pending", async () => {
    const { c } = client([() => json(200, { outcome: "ok", servers: [{ ...server, status: ["pending"] }] })]);
    expect(await c.agent.portalServers()).toMatchObject({ outcome: "shape" });
  });

  it("lists an approved server held for the public egress guard", async () => {
    const held = { ...server, status: "pending_egress_guard" };
    const { c } = client([() => json(200, { outcome: "ok", servers: [held] })]);
    expect(await c.agent.portalServers()).toMatchObject({ outcome: "ok", servers: [held] });
  });

});
