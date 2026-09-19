// tenant-client-request.test.ts — CTC-2132 Phase 5. The generic authed-request escape hatch, with the
// origin guard: `new URL(origin + path)` does not throw on a path like "@evil.example/steal" — it
// reads the origin as USERINFO and the host becomes the attacker's, silently sending this client's
// bearer credential to that host. Reproduced independently of this suite:
//   $ bun -e 'const u = new URL("https://cloud.example" + "@evil.example/steal"); console.log(u.host)'
//   evil.example
// The guard below refuses all four hostile inputs BEFORE anything is sent.

import { describe, expect, it } from "vitest";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
import { createTenantClient } from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";

describe("request()", () => {
  it("carries the credential and returns the raw body", async () => {
    const net = scriptedFetch([() => json(200, { accounts: [], observedAtMs: 7 })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const r = await client.request({ path: "/api/v1/coding-accounts" });
    expect(r.outcome).toBe("ok");
    if (r.outcome === "ok") expect(r.json).toEqual({ accounts: [], observedAtMs: 7 });
    expect(net.calls[0]!.headers["authorization"]).toBe(`Bearer ${KEY}`);
  });

  it("⛔ refuses a path that would escape the tenant origin, WITHOUT SENDING ANYTHING", async () => {
    const net = scriptedFetch([() => json(200, {})]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    for (const path of ["@evil.example/steal", "https://evil.example/steal", "//evil.example/x", "api/v1/me"]) {
      const r = await client.request({ path });
      expect([path, r.outcome]).toEqual([path, "rejected"]);
    }
    expect(net.calls).toHaveLength(0); // the credential never left the process
  });

  it("resolves a bearer AuthStrategy per request, like every other method", async () => {
    let n = 0;
    const net = scriptedFetch([() => json(200, {}), () => json(200, {})]);
    const client = createTenantClient({
      auth: { kind: "bearer", getToken: async () => `tok${++n}` },
      baseUrl: BASE,
      fetch: net.fetch,
    });
    await client.request({ path: "/api/v1/coding-accounts" });
    await client.request({ path: "/api/v1/coding-accounts" });
    expect(net.calls.map((c) => c.headers["authorization"])).toEqual(["Bearer tok1", "Bearer tok2"]);
  });

  it("folds a non-2xx through classify()", async () => {
    const net = scriptedFetch([() => json(403, { error: "nope", required: "mirror:read" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const r = await client.request({ path: "/api/v1/coding-accounts" });
    expect(r.outcome).toBe("forbidden");
    if (r.outcome === "forbidden") expect(r.required).toBe("mirror:read");
  });

  it("POSTs a JSON body with the content-type header", async () => {
    const net = scriptedFetch([() => json(200, { outcome: "succeeded" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    await client.request({ path: "/api/v1/agent/x/issue-comment", method: "POST", body: { issueId: "i1" } });
    expect(net.calls[0]!.method).toBe("POST");
    expect(net.calls[0]!.headers["content-type"]).toBe("application/json");
    expect(net.calls[0]!.body).toEqual({ issueId: "i1" });
  });
});
