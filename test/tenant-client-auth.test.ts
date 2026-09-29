// tenant-client-auth.test.ts — CTC-2132 Phase 1. `createTenantClient` accepts the CTC-2111
// `AuthStrategy` alongside the existing `key`, resolved PER REQUEST (a bearer token rotates ~15 min
// and must never be captured once — the same rule CatalystReplica.feedHeaders() follows).

import { describe, expect, it } from "vitest";
import fixture from "./fixtures/tenant-contract.fixture.json";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
import { createTenantClient, memoryContractCache } from "../src/index";

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

  // ⭐ Regression, CTC-2132 validate attempt 1 / code-review Finding 3. Widening `Sent`'s failure arm
  // to `network | unauthorized` made `contract()`'s `unavailable(sent.failure)` swallow a CREDENTIAL
  // REFUSAL into the stale-cache tolerance, so a client whose OAuth refresh had died was answered
  // `{outcome:"ok",source:"cache"}` — and `contract()` is precisely the probe a credential provider
  // uses to decide whether to prompt for re-login. The cache covers the server being away, never the
  // credential having been revoked.
  it("⭐ a getToken() rejection is `unauthorized` from contract(), NOT a cached-contract `ok`", async () => {
    const net = scriptedFetch([() => json(200, fixture, { etag: '"abc"', "x-catalyst-contract-version": "1.0.0" })]);
    const store = memoryContractCache();
    let live = true;
    const client = createTenantClient({
      auth: {
        kind: "bearer",
        getToken: async () => {
          if (!live) throw new Error("refresh token revoked");
          return "tok1";
        },
      },
      baseUrl: BASE,
      fetch: net.fetch,
      contractCache: store,
    });
    expect((await client.contract()).outcome).toBe("ok"); // seeds the cache over a live credential
    live = false;
    const r = await client.contract({ refresh: true });
    expect(r.outcome).toBe("unauthorized");
    if (r.outcome === "unauthorized") expect(r.reason).toContain("refresh token revoked");
    expect(net.calls).toHaveLength(1); // the refusal never reached the wire
  });

  it("a transport failure still falls back to the cached contract (the tolerance the refusal must not borrow)", async () => {
    const net = scriptedFetch([
      () => json(200, fixture, { etag: '"abc"', "x-catalyst-contract-version": "1.0.0" }),
      () => new Error("ECONNREFUSED"),
    ]);
    const store = memoryContractCache();
    const client = createTenantClient({ key: "ctc_user_x", baseUrl: BASE, fetch: net.fetch, contractCache: store });
    expect((await client.contract()).outcome).toBe("ok");
    expect(await client.contract({ refresh: true })).toMatchObject({ outcome: "ok", source: "cache" });
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
