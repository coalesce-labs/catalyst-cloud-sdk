// tenant-client-auth.test.ts — CTC-2132 Phase 1. `createTenantClient` accepts the CTC-2111
// `AuthStrategy` alongside the existing `key`, resolved PER REQUEST (a bearer token rotates ~15 min
// and must never be captured once — the same rule CatalystReplica.feedHeaders() follows).

import { describe, expect, it } from "vitest";
import fixture from "./fixtures/tenant-contract.fixture.json";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
import { createTenantClient } from "../src/index";

const BASE = "https://cloud.example";
const ME = { account: "acct_1", slug: "acme", name: "Acme", permissions: null, principal: "service" };

describe("AuthStrategy on the tenant client", () => {
  it("bearer resolves a FRESH token per request", async () => {
    let n = 0;
    const net = scriptedFetch([() => json(200, ME), () => json(200, ME)]);
    const client = createTenantClient({
      auth: { kind: "bearer", getToken: async () => `tok${++n}` },
      baseUrl: BASE,
      fetch: net.fetch,
    });
    await client.me();
    await client.me();
    expect(net.calls.map((c) => c.headers["authorization"])).toEqual(["Bearer tok1", "Bearer tok2"]);
  });

  it("cookie sends NO authorization header", async () => {
    const net = scriptedFetch([() => json(200, ME)]);
    const client = createTenantClient({ auth: { kind: "cookie" }, baseUrl: BASE, fetch: net.fetch });
    await client.me();
    expect(net.calls[0]!.headers["authorization"]).toBeUndefined();
  });

  it("a getToken() rejection is a typed unauthorized arm, not a throw", async () => {
    const net = scriptedFetch([() => json(200, ME)]);
    const client = createTenantClient({
      auth: {
        kind: "bearer",
        getToken: async () => {
          throw new Error("interaction required");
        },
      },
      baseUrl: BASE,
      fetch: net.fetch,
    });
    const r = await client.me();
    expect(r.outcome).toBe("unauthorized");
    expect(net.calls).toHaveLength(0); // nothing was sent
  });

  it("key still works and is unchanged (non-breaking)", async () => {
    const net = scriptedFetch([() => json(200, ME)]);
    const client = createTenantClient({ key: "ctc_user_x", baseUrl: BASE, fetch: net.fetch });
    await client.me();
    expect(net.calls[0]!.headers["authorization"]).toBe("Bearer ctc_user_x");
  });

  it("the bearer token is re-resolved for the contract fetch AND the agent write it precedes", async () => {
    let n = 0;
    const net = scriptedFetch([
      () => json(200, fixture, { etag: '"abc"', "x-catalyst-contract-version": "1.0.0" }),
      () => json(200, { outcome: "succeeded", attempts: 1 }),
    ]);
    const client = createTenantClient({
      auth: { kind: "bearer", getToken: async () => `tok${++n}` },
      baseUrl: BASE,
      fetch: net.fetch,
    });
    await client.agent.issueState({ issueId: "i1", stateId: "s1" });
    expect(net.calls).toHaveLength(2);
    expect(net.calls[0]!.headers["authorization"]).toBe("Bearer tok1");
    expect(net.calls[1]!.headers["authorization"]).toBe("Bearer tok2");
  });
});

describe("options exclusivity (compile-time)", () => {
  it("createTenantClient with `key` alone typechecks and is the runtime under test elsewhere", () => {
    // @ts-expect-error — neither credential
    createTenantClient({ baseUrl: "https://x" });
    // @ts-expect-error — both credentials at once
    createTenantClient({ key: "k", auth: { kind: "cookie" as const }, baseUrl: "https://x" });
  });
});
