import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  LiveSyncClient,
  buildConnectUrl,
  parseFrame,
  toWsOrigin,
  PING_FRAME,
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
  /** When set, the next send() throws — simulates writing to an already-dead socket (CTC-135). */
  throwOnSend = false;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;

  send(data: string): void {
    if (this.throwOnSend) throw new Error("send on a dead socket");
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

// ── CTC-135 liveness watchdog: half-open detection via app-level ping/pong ──
describe("LiveSyncClient liveness watchdog (CTC-135)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  /** Fake timers with a fixed epoch so lastFrameAt / pingSentAt are exact and deterministic. */
  function useFakeClock(): void {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  }

  function makeClient(opts: {
    pingIntervalMs?: number;
    pongTimeoutMs?: number;
    initialCursor?: number | null;
  }) {
    const store = makeStore(opts.initialCursor ?? 7);
    const { sockets, factory } = recordingFactory();
    const statuses: LiveSyncStatus[] = [];
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(0),
      getCursor: store.getCursor,
      onChange: store.onChange,
      onStatus: (s) => statuses.push(s),
      backoffMs: 1000,
      maxBackoffMs: 30_000,
      pingIntervalMs: opts.pingIntervalMs,
      pongTimeoutMs: opts.pongTimeoutMs,
      wsFactory: factory,
    });
    return { client, sockets, statuses, store };
  }

  const change = (seq: number): ChangeFrame => ({
    type: "change",
    accountId: "tenant-0",
    seq,
    entity: "issues",
    entityId: `i${seq}`,
    op: "upsert",
    row: { id: `i${seq}` },
  });
  const pingsOn = (ws: FakeWebSocket) => ws.sent.filter((s) => s === PING_FRAME);
  const lastSent = (ws: FakeWebSocket) => ws.sent[ws.sent.length - 1];

  it("pings after an idle interval and stays connected when the server pongs", () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen();

    // No ping until a full idle interval elapses.
    vi.advanceTimersByTime(999);
    expect(pingsOn(sockets[0]!)).toHaveLength(0);
    vi.advanceTimersByTime(1);
    expect(lastSent(sockets[0]!)).toBe(PING_FRAME); // ping at t=1000, deadline armed at t=1200

    // Each pong clears the deadline and re-arms the next ping — three clean cycles, no reconnect.
    for (let i = 0; i < 3; i++) {
      sockets[0]!.deliver({ type: "pong" });
      vi.advanceTimersByTime(1000);
      expect(lastSent(sockets[0]!)).toBe(PING_FRAME);
    }
    expect(pingsOn(sockets[0]!)).toHaveLength(4);
    expect(sockets).toHaveLength(1); // a ponging server keeps the one socket alive

    client.stop();
  });

  it("force-reconnects when a previously-ponging connection stops answering", () => {
    useFakeClock();
    const { client, sockets, statuses } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen();

    // Prove capability: the first ping is ponged.
    vi.advanceTimersByTime(1000);
    sockets[0]!.deliver({ type: "pong" });

    // Then go silent: the next ping's deadline elapses with no frame → liveness timeout.
    vi.advanceTimersByTime(1000); // ping (pingSentAt=2000)
    expect(lastSent(sockets[0]!)).toBe(PING_FRAME);
    vi.advanceTimersByTime(200); // deadline: lastFrameAt(1000) < pingSentAt(2000) → not answered
    expect(sockets[0]!.closed).toBe(true);
    expect(statuses).toContain("reconnecting");

    vi.advanceTimersByTime(1000); // backoff reconnect
    expect(sockets).toHaveLength(2);

    client.stop();
  });

  it("sends no pings while change frames keep arriving", () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen();

    // A change every 500ms (< the 1000ms interval) postpones the ping indefinitely.
    for (let i = 0; i < 5; i++) {
      vi.advanceTimersByTime(500);
      sockets[0]!.deliver(change(10 + i));
    }
    expect(pingsOn(sockets[0]!)).toHaveLength(0);
    expect(sockets).toHaveLength(1);

    client.stop();
  });

  it("does not reconnect when a frame answers just before the pong deadline", () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen();

    vi.advanceTimersByTime(1000); // ping at t=1000, deadline armed for t=1200
    expect(lastSent(sockets[0]!)).toBe(PING_FRAME);
    vi.advanceTimersByTime(150); // t=1150
    sockets[0]!.deliver(change(50)); // answers before the deadline → deadline cleared
    vi.advanceTimersByTime(100); // t=1250, PAST the original deadline — but it was cancelled
    expect(sockets[0]!.closed).toBe(false);
    expect(sockets).toHaveLength(1);

    client.stop();
  });

  it("disables the watchdog after 3 consecutive unanswered probes (old server)", () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen();

    // One probe cycle against a server that never pongs: ping → deadline → force-reconnect → reopen.
    const probeFailAndReopen = () => {
      vi.advanceTimersByTime(1000); // ping fires
      vi.advanceTimersByTime(200); // deadline elapses (never ponged) → force-reconnect
      vi.advanceTimersByTime(1000); // backoff reconnect opens the next socket
      sockets[sockets.length - 1]!.fireOpen();
    };

    probeFailAndReopen(); // failure 1 → conn2
    expect(sockets).toHaveLength(2);
    probeFailAndReopen(); // failure 2 → conn3
    expect(sockets).toHaveLength(3);
    probeFailAndReopen(); // failure 3 → watchdog disabled → conn4
    expect(sockets).toHaveLength(4);

    // conn4 opened with the watchdog disabled: it never pings and never reconnects again.
    vi.advanceTimersByTime(10_000);
    expect(pingsOn(sockets[3]!)).toHaveLength(0);
    expect(sockets).toHaveLength(4); // bounded to exactly 3 reconnects against an old server

    client.stop();
  });

  it("a pong resets the consecutive-probe-failure counter", () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen();

    const probeFailAndReopen = () => {
      vi.advanceTimersByTime(1000);
      vi.advanceTimersByTime(200);
      vi.advanceTimersByTime(1000);
      sockets[sockets.length - 1]!.fireOpen();
    };

    probeFailAndReopen(); // probeFailures = 1 → conn2
    probeFailAndReopen(); // probeFailures = 2 → conn3

    // conn3 pongs its first probe → probeFailures resets to 0.
    vi.advanceTimersByTime(1000);
    expect(lastSent(sockets[2]!)).toBe(PING_FRAME);
    sockets[2]!.deliver({ type: "pong" });

    // A server-initiated close moves us to conn4 without a probe failure.
    sockets[2]!.fireServerClose();
    vi.advanceTimersByTime(1000);
    sockets[3]!.fireOpen();
    expect(sockets).toHaveLength(4);

    // conn4 still pings — proof the counter reset (were it at 3, the watchdog would be disabled).
    vi.advanceTimersByTime(1000);
    expect(pingsOn(sockets[3]!)).toHaveLength(1);

    client.stop();
  });

  it("clears liveness timers on stop (no ping fires afterward)", () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen();
    client.stop(); // closeSocket → clearLivenessTimers
    vi.advanceTimersByTime(10_000);
    expect(pingsOn(sockets[0]!)).toHaveLength(0);
  });

  it("clears the ping timer on resync teardown (the old socket never pings mid-reseed)", async () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen(); // ping armed on conn1
    sockets[0]!.deliver({ type: "resync", accountId: "tenant-0" }); // closeSocket → clears timers, reopen
    await vi.advanceTimersByTimeAsync(2000); // flush the async reseed + reopen, past conn1's interval
    expect(pingsOn(sockets[0]!)).toHaveLength(0); // conn1's ping timer was cleared with its socket
    expect(sockets.length).toBeGreaterThanOrEqual(2);
    client.stop();
  });

  it("never pings when pingIntervalMs is 0", () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 0, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen();
    vi.advanceTimersByTime(1_000_000);
    expect(pingsOn(sockets[0]!)).toHaveLength(0);
    expect(sockets).toHaveLength(1);
    client.stop();
  });

  it("exposes lastFrameAt, updated by ANY inbound frame (change, malformed, or pong)", () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen();
    expect(client.lastFrameAt).toBeNull(); // null before the first inbound frame

    vi.advanceTimersByTime(500);
    sockets[0]!.deliver(change(10));
    expect(client.lastFrameAt).toBe(500);

    vi.advanceTimersByTime(300);
    sockets[0]!.deliverRaw("not json at all"); // malformed bytes still prove liveness
    expect(client.lastFrameAt).toBe(800);

    vi.advanceTimersByTime(100);
    sockets[0]!.deliver({ type: "pong" }); // a transport-internal pong counts too
    expect(client.lastFrameAt).toBe(900);

    client.stop();
  });

  it("force-reconnects when sending a ping throws (a dead socket surfaces synchronously)", () => {
    useFakeClock();
    const { client, sockets } = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 200 });
    void client.start();
    sockets[0]!.fireOpen(); // the {type:sync} frame sends fine
    sockets[0]!.throwOnSend = true; // the ping send will throw

    vi.advanceTimersByTime(1000); // ping fires → send throws → immediate force-reconnect
    expect(sockets[0]!.closed).toBe(true);
    vi.advanceTimersByTime(1000); // backoff reconnect
    expect(sockets).toHaveLength(2);

    client.stop();
  });
});
