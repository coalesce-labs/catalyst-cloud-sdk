import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  LiveSyncClient,
  buildConnectUrl,
  parseFrame,
  toWsOrigin,
  type WebSocketLike,
  type WebSocketFactory,
  type LiveSyncStatus,
  type ChangeFrame,
} from "../src/index";

// Covers src/live-sync-client.ts — the isomorphic WS push client. The transport is driven by a
// FakeWebSocket so every branch (seed-on-first-connect, sync-on-open, live apply, underflow resync,
// backoff reconnect, stop, auth strategy) is deterministic and offline. Storage is injected — these
// tests use an in-memory cursor + an onChange spy in place of any real store.

// ── A scriptable WebSocket the test drives by hand (no network, no timers of its own). ──
class FakeWebSocket implements WebSocketLike {
  sent: string[] = [];
  closed = false;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;

  send(data: string): void {
    this.sent.push(data);
  }
  close(): void {
    this.closed = true;
  }

  // ── test drivers (simulate the server side) ──
  fireOpen(): void {
    this.onopen?.({});
  }
  deliver(frame: unknown): void {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }
  deliverRaw(data: unknown): void {
    this.onmessage?.({ data });
  }
  fireServerClose(): void {
    this.onclose?.({});
  }
  /** The last frame this socket sent, parsed. */
  lastSent(): unknown {
    return JSON.parse(this.sent[this.sent.length - 1]!);
  }
}

function recordingFactory(): {
  sockets: FakeWebSocket[];
  urls: string[];
  factory: WebSocketFactory;
} {
  const sockets: FakeWebSocket[] = [];
  const urls: string[] = [];
  const factory: WebSocketFactory = (url: string) => {
    urls.push(url);
    const ws = new FakeWebSocket();
    sockets.push(ws);
    return ws;
  };
  return { sockets, urls, factory };
}

/** A tiny injected store: a mutable cursor + recorded applied frames + a reseed counter. */
function makeStore(initialCursor: number | null) {
  let cursor = initialCursor;
  const applied: ChangeFrame[] = [];
  const reseedCalls = { count: 0 };
  return {
    getCursor: () => cursor,
    setCursor: (n: number) => {
      cursor = n;
    },
    onChange: (frame: ChangeFrame) => {
      applied.push(frame);
      if (frame.seq > (cursor ?? -1)) cursor = frame.seq;
    },
    applied,
    reseedCalls,
    reseedTo(n: number) {
      return async () => {
        reseedCalls.count += 1;
        cursor = n;
        return n;
      };
    },
  };
}

const BASE = "https://api.example.test";

describe("toWsOrigin", () => {
  it("swaps http→ws and https→wss, preserving the rest of the origin + path", () => {
    expect(toWsOrigin("https://h.example/api/v1")).toBe("wss://h.example/api/v1");
    expect(toWsOrigin("http://localhost:8787")).toBe("ws://localhost:8787");
  });
});

describe("buildConnectUrl auth strategy", () => {
  it("appends ?token= for token kind (token ordered first, then account)", () => {
    const url = buildConnectUrl({
      baseUrl: "https://h.example/api/v1",
      connectPath: "/connect",
      accountId: "tenant-7",
      auth: { kind: "token", token: "svc-tok" },
    });
    const u = new URL(url);
    expect(u.protocol).toBe("wss:");
    expect(u.pathname).toBe("/api/v1/connect");
    expect(u.searchParams.get("token")).toBe("svc-tok");
    expect(u.searchParams.get("account")).toBe("tenant-7");
    // token first so a truncated log still shows account
    expect(url.indexOf("token=")).toBeLessThan(url.indexOf("account="));
  });

  it("appends NOTHING for cookie kind — no token can ever leak from the browser path", () => {
    const url = buildConnectUrl({
      baseUrl: "https://app.example/api/v1",
      connectPath: "/connect",
      accountId: "tenant-0",
      auth: { kind: "cookie" },
    });
    const u = new URL(url);
    expect(u.searchParams.has("token")).toBe(false);
    expect(u.searchParams.get("account")).toBe("tenant-0");
    expect(url).not.toContain("token");
  });
});

describe("parseFrame", () => {
  it("accepts a {type:'change'} frame", () => {
    const frame = parseFrame(
      JSON.stringify({ type: "change", accountId: "t", seq: 1, entity: "issues", entityId: "i", op: "upsert" }),
    );
    expect(frame?.type).toBe("change");
  });

  it("accepts a {type:'resync'} frame", () => {
    expect(parseFrame(JSON.stringify({ type: "resync" }))?.type).toBe("resync");
  });

  it("accepts an ArrayBuffer-encoded frame", () => {
    const buf = new TextEncoder().encode(JSON.stringify({ type: "resync" })).buffer;
    expect(parseFrame(buf)?.type).toBe("resync");
  });

  it("rejects a bare payload (no type), unknown type, non-JSON, and non-string/buffer", () => {
    expect(parseFrame(JSON.stringify({ hello: "world" }))).toBeNull();
    expect(parseFrame(JSON.stringify({ type: "bogus" }))).toBeNull();
    expect(parseFrame("not json at all")).toBeNull();
    expect(parseFrame(JSON.stringify(42))).toBeNull();
    expect(parseFrame(null)).toBeNull();
    expect(parseFrame(123)).toBeNull();
  });
});

describe("LiveSyncClient", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("seeds via reseed when there is no cursor, then opens the WS and syncs from the seeded cursor", async () => {
    const store = makeStore(null);
    const { sockets, factory } = recordingFactory();
    const statuses: LiveSyncStatus[] = [];
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "token", token: "tok" },
      reseed: store.reseedTo(5),
      getCursor: store.getCursor,
      onChange: store.onChange,
      onStatus: (s) => statuses.push(s),
      wsFactory: factory,
    });

    void client.start();
    await vi.waitFor(() => expect(sockets).toHaveLength(1));

    expect(store.reseedCalls.count).toBe(1); // seeded once
    expect(store.getCursor()).toBe(5);

    sockets[0]!.fireOpen();
    expect(sockets[0]!.lastSent()).toEqual({ type: "sync", after: 5 });
    expect(statuses).toContain("resyncing");
    expect(statuses).toContain("live");

    client.stop();
    expect(statuses[statuses.length - 1]).toBe("stopped");
  });

  it("with an existing cursor, opens the WS immediately (no reseed) and syncs from it", () => {
    const store = makeStore(7);
    const { sockets, factory } = recordingFactory();
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(0),
      getCursor: store.getCursor,
      onChange: store.onChange,
      wsFactory: factory,
    });

    void client.start(); // existing cursor → openSocket path is synchronous
    expect(sockets).toHaveLength(1);
    expect(store.reseedCalls.count).toBe(0); // no seed

    sockets[0]!.fireOpen();
    expect(sockets[0]!.lastSent()).toEqual({ type: "sync", after: 7 });

    client.stop();
  });

  it("sends {type:'sync', after:-1} when getCursor returns null/undefined on open", () => {
    // start() reseeds when cursor is null; simulate a store that still returns null at sync time
    // (e.g. a reseed that returns 0 but getCursor not yet wired) to prove the -1 fallback.
    let cursor: number | null = null;
    const { sockets, factory } = recordingFactory();
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: async () => {
        cursor = null; // pathological: reseed leaves cursor null
        return -1;
      },
      getCursor: () => cursor,
      onChange: () => {},
      wsFactory: factory,
    });
    void client.start();
    return vi.waitFor(() => expect(sockets).toHaveLength(1)).then(() => {
      sockets[0]!.fireOpen();
      expect(sockets[0]!.lastSent()).toEqual({ type: "sync", after: -1 });
      client.stop();
    });
  });

  it("delivers a live change frame to onChange (and onFrame)", () => {
    const store = makeStore(0);
    const { sockets, factory } = recordingFactory();
    const seen: string[] = [];
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(0),
      getCursor: store.getCursor,
      onChange: store.onChange,
      onFrame: (f) => seen.push(f.type),
      wsFactory: factory,
    });

    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 3,
      entity: "issues",
      entityId: "i1",
      op: "upsert",
      row: { id: "i1", title: "Live", updated_at: 10 },
    });

    expect(store.applied).toHaveLength(1);
    expect(store.applied[0]!.seq).toBe(3);
    expect(seen).toEqual(["change"]);

    client.stop();
  });

  it("ignores malformed / unknown frames without throwing or calling onChange", () => {
    const store = makeStore(4);
    const { sockets, factory } = recordingFactory();
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(0),
      getCursor: store.getCursor,
      onChange: store.onChange,
      wsFactory: factory,
    });

    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliverRaw("not json at all");
    sockets[0]!.deliver({ type: "bogus" });
    sockets[0]!.deliver({ hello: "world" });

    expect(store.applied).toHaveLength(0);
    expect(store.getCursor()).toBe(4);
    client.stop();
  });

  it("on a {type:'resync'} frame calls reseed, then reopens and syncs from the fresh cursor", async () => {
    const store = makeStore(7);
    const { sockets, factory } = recordingFactory();
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(12),
      getCursor: store.getCursor,
      onChange: store.onChange,
      wsFactory: factory,
    });

    void client.start();
    expect(sockets).toHaveLength(1);
    sockets[0]!.fireOpen();
    expect(sockets[0]!.lastSent()).toEqual({ type: "sync", after: 7 });

    sockets[0]!.deliver({ type: "resync", accountId: "tenant-0" });
    await vi.waitFor(() => expect(sockets).toHaveLength(2));

    expect(store.reseedCalls.count).toBe(1); // re-seeded exactly once
    expect(sockets[0]!.closed).toBe(true); // old socket torn down (close before reseed)
    expect(store.getCursor()).toBe(12);

    sockets[1]!.fireOpen();
    expect(sockets[1]!.lastSent()).toEqual({ type: "sync", after: 12 });

    client.stop();
  });

  it("the client's connectUrl reflects the /api/v1 base, path preservation, and auth strategy", () => {
    const store = makeStore(7);
    const { factory } = recordingFactory();
    const client = new LiveSyncClient({
      baseUrl: "https://h.example/api/v1",
      accountId: "tenant-0",
      auth: { kind: "token", token: "svc-tok" },
      reseed: store.reseedTo(0),
      getCursor: store.getCursor,
      onChange: store.onChange,
      wsFactory: factory,
    });
    const u = new URL(client.connectUrl());
    expect(u.protocol).toBe("wss:");
    expect(u.pathname).toBe("/api/v1/connect");
    expect(u.searchParams.get("token")).toBe("svc-tok");
    expect(u.searchParams.get("account")).toBe("tenant-0");
  });

  it("honors a custom connectPath", () => {
    const store = makeStore(7);
    const { sockets, urls, factory } = recordingFactory();
    const client = new LiveSyncClient({
      baseUrl: "https://h.example",
      accountId: "tenant-0",
      connectPath: "/api/v1/connect",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(0),
      getCursor: store.getCursor,
      onChange: store.onChange,
      wsFactory: factory,
    });
    void client.start();
    expect(sockets).toHaveLength(1);
    expect(urls[0]).toBe("wss://h.example/api/v1/connect?account=tenant-0");
    client.stop();
  });

  it("reconnects with capped exponential backoff 1s,2s,4s…30s, and stop() halts the loop", () => {
    vi.useFakeTimers();
    const store = makeStore(7);
    const { sockets, factory } = recordingFactory();
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(0),
      getCursor: store.getCursor,
      onChange: store.onChange,
      backoffMs: 1000,
      maxBackoffMs: 30_000,
      wsFactory: factory,
    });

    void client.start();
    expect(sockets).toHaveLength(1);
    sockets[0]!.fireOpen();

    // Server drops → reconnect scheduled at 1000ms, not immediate.
    sockets[0]!.fireServerClose();
    expect(sockets).toHaveLength(1);
    vi.advanceTimersByTime(999);
    expect(sockets).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(sockets).toHaveLength(2);

    // socket[1] fails before it ever opens → backoff doubles to 2000ms (no successful-open reset).
    sockets[1]!.fireServerClose();
    vi.advanceTimersByTime(1999);
    expect(sockets).toHaveLength(2);
    vi.advanceTimersByTime(1);
    expect(sockets).toHaveLength(3);

    // 4000ms next.
    sockets[2]!.fireServerClose();
    vi.advanceTimersByTime(4000);
    expect(sockets).toHaveLength(4);

    // stop() closes the live socket and cancels any further reconnects.
    client.stop();
    sockets[3]!.fireServerClose();
    vi.advanceTimersByTime(60_000);
    expect(sockets).toHaveLength(4);
    expect(sockets[3]!.closed).toBe(true);
  });

  it("caps the backoff at maxBackoffMs (30s) after enough failures", () => {
    vi.useFakeTimers();
    const store = makeStore(7);
    const { sockets, factory } = recordingFactory();
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(0),
      getCursor: store.getCursor,
      onChange: store.onChange,
      backoffMs: 1000,
      maxBackoffMs: 30_000,
      wsFactory: factory,
    });

    void client.start();
    // Drive failures: delays should be 1s,2s,4s,8s,16s,30s(capped),30s…
    const expectedDelays = [1000, 2000, 4000, 8000, 16000, 30000, 30000];
    let idx = 0;
    sockets[idx]!.fireServerClose();
    for (const delay of expectedDelays) {
      const before = sockets.length;
      vi.advanceTimersByTime(delay - 1);
      expect(sockets).toHaveLength(before); // not yet
      vi.advanceTimersByTime(1);
      expect(sockets).toHaveLength(before + 1); // opened after the full delay
      idx = sockets.length - 1;
      sockets[idx]!.fireServerClose();
    }
    client.stop();
  });
});
