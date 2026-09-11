// tenant-client-contract.test.ts — CTC-2004 Tier 1, criterion 1: `createTenantClient({key, baseUrl})
// .contract()` returns the typed TenantContract with the cloud's own ETag cache policy applied.
//
// The policy under test is docs/agent-contract.md "Caching and staleness", as the bundle's
// `loadContract` implements it today: before `cache.maxAgeSeconds` the cached document is used as-is;
// past it a conditional GET revalidates (304 refreshes the clock, 200 replaces the document); past
// `cache.staleRefusalSeconds` with no successful revalidation the client REFUSES by name — there is no
// literal fallback for any field the document serves. Both bounds are read from the fixture, never
// typed here.

import { describe, expect, it } from "vitest";
import fixture from "./fixtures/tenant-contract.fixture.json";
import { empty, json, scriptedFetch, text } from "./helpers/scripted-fetch";
import {
  CONTRACT_ROUTE,
  createTenantClient,
  memoryContractCache,
  type ContractCacheEntry,
} from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_acct_test";
const ETAG = '"0123456789abcdef0123456789abcdef"';
const MAX_AGE = fixture.cache.maxAgeSeconds;
const STALE_AFTER = fixture.cache.staleRefusalSeconds;

function clock(startMs = 1_700_000_000_000) {
  let t = startMs;
  return { now: () => t, advanceSeconds: (s: number) => (t += s * 1000) };
}

describe("contract() — first fetch", () => {
  it("⭐ GETs the contract route with the bearer, no If-None-Match, stores etag+doc, and answers source:network", async () => {
    const net = scriptedFetch([() => json(200, fixture, { etag: ETAG, "x-catalyst-contract-version": "1.0.0" })]);
    const store = memoryContractCache();
    const c = clock();
    const client = createTenantClient({ key: KEY, baseUrl: `${BASE}/`, fetch: net.fetch, contractCache: store, now: c.now });

    const res = await client.contract();
    expect(res.outcome).toBe("ok");
    if (res.outcome !== "ok") return;
    expect(res.source).toBe("network");
    expect(res.doc.account.id).toBe(fixture.account.id);
    expect(res.etag).toBe(ETAG);
    expect(res.contractVersion).toBe("1.0.0");
    expect(res.ageSeconds).toBe(0);

    expect(net.calls).toHaveLength(1);
    const call = net.calls[0]!;
    expect(call.url).toBe(`${BASE}${CONTRACT_ROUTE}`); // the trailing slash on baseUrl is trimmed
    expect(call.method).toBe("GET");
    expect(call.headers["authorization"]).toBe(`Bearer ${KEY}`);
    expect(call.headers["accept"]).toBe("application/json");
    expect(call.headers["if-none-match"]).toBeUndefined();

    const entry = await store.get();
    expect(entry).not.toBeNull();
    expect(entry?.etag).toBe(ETAG);
    expect(entry?.contractVersion).toBe("1.0.0");
    expect(entry?.fetchedAt).toBe(c.now());
  });

  it("the version comes from the x-catalyst-contract-version header, falling back to the body", async () => {
    const net = scriptedFetch([() => json(200, fixture, { etag: ETAG })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const res = await client.contract();
    expect(res.outcome === "ok" && res.contractVersion).toBe(fixture.contractVersion);
  });
});

describe("contract() — the cache policy", () => {
  async function primed() {
    const c = clock();
    const store = memoryContractCache();
    const net = scriptedFetch([() => json(200, fixture, { etag: ETAG, "x-catalyst-contract-version": "1.0.0" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch, contractCache: store, now: c.now });
    const first = await client.contract();
    expect(first.outcome).toBe("ok");
    return { c, store, net, client };
  }

  it("inside maxAgeSeconds: no fetch, source:cache, ageSeconds measured", async () => {
    const { c, net, client } = await primed();
    c.advanceSeconds(MAX_AGE - 1);
    const res = await client.contract();
    expect(net.calls).toHaveLength(1);
    expect(res).toMatchObject({ outcome: "ok", source: "cache", ageSeconds: MAX_AGE - 1 });
  });

  it("⭐ past maxAgeSeconds: a conditional GET with If-None-Match; a 304 answers source:revalidated and refreshes fetchedAt", async () => {
    const { c, store, net, client } = await primed();
    net.calls.length = 0;
    // Re-script: the next answer is a 304 carrying the same validator.
    const net2 = scriptedFetch([() => empty(304, { etag: ETAG, "x-catalyst-contract-version": "1.0.0" })]);
    const client2 = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net2.fetch, contractCache: store, now: c.now });
    c.advanceSeconds(MAX_AGE);
    const res = await client2.contract();
    expect(net2.calls).toHaveLength(1);
    expect(net2.calls[0]!.headers["if-none-match"]).toBe(ETAG);
    expect(res).toMatchObject({ outcome: "ok", source: "revalidated", ageSeconds: 0, etag: ETAG });
    expect((await store.get())?.fetchedAt).toBe(c.now());
    void client;
  });

  it("past maxAgeSeconds: a 200 with a new etag replaces the document and the stored etag", async () => {
    const { c, store, client: _client } = await primed();
    const newEtag = '"ffffffffffffffffffffffffffffffff"';
    const changed = { ...fixture, account: { ...fixture.account, name: "Renamed" } };
    const net2 = scriptedFetch([() => json(200, changed, { etag: newEtag, "x-catalyst-contract-version": "1.1.0" })]);
    const client2 = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net2.fetch, contractCache: store, now: c.now });
    c.advanceSeconds(MAX_AGE);
    const res = await client2.contract();
    expect(res).toMatchObject({ outcome: "ok", source: "network", etag: newEtag, contractVersion: "1.1.0" });
    expect(res.outcome === "ok" && res.doc.account.name).toBe("Renamed");
    expect((await store.get())?.etag).toBe(newEtag);
  });

  it("refresh:true inside maxAgeSeconds still fetches (conditionally)", async () => {
    const { c, store } = await primed();
    const net2 = scriptedFetch([() => empty(304, { etag: ETAG })]);
    const client2 = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net2.fetch, contractCache: store, now: c.now });
    const res = await client2.contract({ refresh: true });
    expect(net2.calls).toHaveLength(1);
    expect(res).toMatchObject({ outcome: "ok", source: "revalidated" });
  });

  it("a network failure inside staleRefusalSeconds serves the cache", async () => {
    const { c, store } = await primed();
    const net2 = scriptedFetch([() => new Error("ECONNREFUSED")]);
    const client2 = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net2.fetch, contractCache: store, now: c.now });
    c.advanceSeconds(STALE_AFTER - 1);
    const res = await client2.contract();
    expect(res).toMatchObject({ outcome: "ok", source: "cache", ageSeconds: STALE_AFTER - 1 });
  });

  it("⛔ a network failure PAST staleRefusalSeconds refuses by name — no literal fallback", async () => {
    const { c, store } = await primed();
    const net2 = scriptedFetch([() => new Error("ECONNREFUSED")]);
    const client2 = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net2.fetch, contractCache: store, now: c.now });
    c.advanceSeconds(STALE_AFTER);
    const res = await client2.contract();
    expect(res).toMatchObject({
      outcome: "stale",
      ageSeconds: STALE_AFTER,
      staleRefusalSeconds: STALE_AFTER,
      cause: { outcome: "network" },
    });
    expect(res.outcome === "stale" && res.cause.outcome === "network" && res.cause.reason).toContain("ECONNREFUSED");
  });

  it("offline:true serves the cache when there is one and answers `missing` when there is none", async () => {
    const { c, store } = await primed();
    const net2 = scriptedFetch([() => new Error("must not be called")]);
    const client2 = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net2.fetch, contractCache: store, now: c.now });
    c.advanceSeconds(STALE_AFTER * 2);
    expect(await client2.contract({ offline: true })).toMatchObject({ outcome: "ok", source: "cache" });
    const cold = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net2.fetch, contractCache: memoryContractCache(), now: c.now });
    expect(await cold.contract({ offline: true })).toMatchObject({ outcome: "missing" });
    expect(net2.calls).toHaveLength(0);
  });

  it("a 304 with no cache to revalidate is a `shape` outcome (the server answered a question we did not ask)", async () => {
    const net = scriptedFetch([() => empty(304, { etag: ETAG })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.contract()).toMatchObject({ outcome: "shape", status: 304 });
  });

  it("a 200 that does not read as a TenantContract is a `shape` outcome and is NOT cached", async () => {
    const store = memoryContractCache();
    const net = scriptedFetch([() => json(200, { contractVersion: "1.0.0", teams: [] }, { etag: ETAG })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch, contractCache: store });
    expect(await client.contract()).toMatchObject({ outcome: "shape", status: 200 });
    expect(await store.get()).toBeNull();
  });

  it("a custom store is honoured: a pre-seeded entry is served without a fetch", async () => {
    const entry: ContractCacheEntry = {
      etag: ETAG,
      fetchedAt: 1_700_000_000_000,
      contractVersion: "1.0.0",
      doc: JSON.parse(JSON.stringify(fixture)) as ContractCacheEntry["doc"],
    };
    let written: ContractCacheEntry | null = null;
    const store = { get: () => entry, set: (e: ContractCacheEntry) => void (written = e) };
    const net = scriptedFetch([() => new Error("must not be called")]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch, contractCache: store, now: () => entry.fetchedAt + 1000 });
    expect(await client.contract()).toMatchObject({ outcome: "ok", source: "cache", ageSeconds: 1 });
    expect(net.calls).toHaveLength(0);
    expect(written).toBeNull();
  });
});

describe("contract() — the server's refusals, as typed arms", () => {
  it("403 not-machine-principal (a workstation key) → forbidden", async () => {
    const net = scriptedFetch([() => json(403, { error: "forbidden", reason: "not-machine-principal" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.contract()).toEqual({ outcome: "forbidden", status: 403, reason: "not-machine-principal", required: null, account: null });
  });

  it("403 missing-scope carries `required`; 403 account-mismatch carries `account`", async () => {
    const net = scriptedFetch([
      () => json(403, { error: "forbidden", reason: "missing-scope", required: "mirror:read" }),
      () => json(403, { error: "forbidden", reason: "account-mismatch", account: "acct-other" }),
    ]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.contract()).toMatchObject({ outcome: "forbidden", required: "mirror:read" });
    expect(await client.contract()).toMatchObject({ outcome: "forbidden", account: "acct-other" });
  });

  it("401 credential-not-accepted → unauthorized with the correlation ref", async () => {
    const net = scriptedFetch([() => json(401, { error: "unauthorized", reason: "credential-not-accepted", ref: "abcd1234" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.contract()).toEqual({ outcome: "unauthorized", status: 401, reason: "credential-not-accepted", ref: "abcd1234" });
  });

  it("⛔ 502 {outcome:failed} (a partial document) → failed, and the store is NOT written", async () => {
    const store = memoryContractCache();
    const net = scriptedFetch([() => json(502, { outcome: "failed", reason: "teams_unreadable" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch, contractCache: store });
    expect(await client.contract()).toEqual({ outcome: "failed", status: 502, reason: "teams_unreadable" });
    expect(await store.get()).toBeNull();
  });

  it("an unmapped status with a text body → http with the body's head as the reason", async () => {
    const net = scriptedFetch([() => text(500, "internal error")]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.contract()).toEqual({ outcome: "http", status: 500, reason: "internal error" });
  });

  it("a fetch that throws with no cache → network", async () => {
    const net = scriptedFetch([() => new Error("getaddrinfo ENOTFOUND")]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const res = await client.contract();
    expect(res.outcome).toBe("network");
    expect(res.outcome === "network" && res.reason).toContain("ENOTFOUND");
  });
});
