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

// ── CTL-1402 gap detection + self-healing re-request ──
// The live push is at-most-once (the mirror's broadcastChange swallows send failures), and before this
// fix the client advanced its cursor to any seq it SAW — an undelivered frame was sealed over
// permanently by the next delivered one, with no telemetry (an unarrived frame lands in no apply
// bucket). These tests pin the new contract: a frame beyond deliveredSeq+1 is never applied; the
// client re-requests the hole via {type:"sync", after:<deliveredSeq>} (the mirror's replaySince has
// served client-requested replay since CTC-63); the replayed frames heal the gap through the SAME
// apply path; bounded retries escalate to the full re-seed; {type:"resync"} is always honoured.
describe("LiveSyncClient gap detection + self-healing re-request (CTL-1402)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  interface LogLine {
    level: string;
    msg: string;
    extra?: unknown;
  }

  function makeGapClient(opts: {
    initialCursor?: number | null;
    gapTimeoutMs?: number;
    gapRetryLimit?: number;
    reseedTo?: number;
  }) {
    const store = makeStore(opts.initialCursor ?? 7);
    const { sockets, factory } = recordingFactory();
    const statuses: LiveSyncStatus[] = [];
    const logs: LogLine[] = [];
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(opts.reseedTo ?? 100),
      getCursor: store.getCursor,
      onChange: store.onChange,
      onStatus: (s) => statuses.push(s),
      backoffMs: 1000,
      maxBackoffMs: 30_000,
      pingIntervalMs: 0, // liveness watchdog off — isolate the gap machinery
      gapTimeoutMs: opts.gapTimeoutMs,
      gapRetryLimit: opts.gapRetryLimit,
      wsFactory: factory,
      log: (level, msg, extra) => logs.push({ level, msg, extra }),
    });
    return { client, store, sockets, statuses, logs };
  }

  const change = (seq: number): ChangeFrame => ({
    type: "change",
    accountId: "tenant-0",
    seq,
    entity: "issues",
    entityId: `i${seq}`,
    op: "upsert",
    row: { id: `i${seq}`, updated_at: seq },
  });
  /** All {type:"sync"} control frames this socket has sent, parsed. */
  const syncsSent = (ws: FakeWebSocket): Array<{ type: string; after: number }> =>
    ws.sent.map((s) => JSON.parse(s) as { type: string; after: number }).filter((f) => f.type === "sync");
  /** All catalyst.replica.gap log lines, flattened. */
  const gapLines = (logs: LogLine[]): Record<string, unknown>[] =>
    logs
      .filter((l) => l.msg === "catalyst.replica.gap")
      .map((l) => ({ level: l.level, ...(l.extra as Record<string, unknown>) }));

  it("contiguous frames apply with no gap re-request and no gap telemetry", () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();

    sockets[0]!.deliver(change(8));
    sockets[0]!.deliver(change(9));
    sockets[0]!.deliver(change(10));

    expect(store.applied.map((f) => f.seq)).toEqual([8, 9, 10]);
    expect(store.getCursor()).toBe(10);
    expect(syncsSent(sockets[0]!)).toEqual([{ type: "sync", after: 7 }]); // only the on-open sync
    expect(gapLines(logs)).toEqual([]);
    client.stop();
  });

  it("a gap sends exactly one {type:'sync', after:highWater}, holds the cursor at the hole, and the replay heals it", () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8)); // contiguous — applied

    sockets[0]!.deliver(change(11)); // seqs 9,10 were never delivered → GAP

    // The gapped frame is NOT applied and the cursor did NOT advance past the hole.
    expect(store.applied.map((f) => f.seq)).toEqual([8]);
    expect(store.getCursor()).toBe(8);
    // Exactly one re-request, from the last delivered seq.
    expect(syncsSent(sockets[0]!)).toEqual([
      { type: "sync", after: 7 },
      { type: "sync", after: 8 },
    ]);
    expect(gapLines(logs)).toContainEqual(
      expect.objectContaining({ level: "info", event: "detected", seq_from: 9, seq_to: 10, size: 2 }),
    );

    // The mirror answers the re-request by replaying 9..head as ORDINARY change frames — the same
    // apply path — which closes the hole; live frames then resume contiguously.
    sockets[0]!.deliver(change(9));
    sockets[0]!.deliver(change(10));
    sockets[0]!.deliver(change(11)); // the dropped trigger frame is redelivered by the replay
    sockets[0]!.deliver(change(12)); // live resumes

    expect(store.applied.map((f) => f.seq)).toEqual([8, 9, 10, 11, 12]);
    expect(store.getCursor()).toBe(12);
    expect(syncsSent(sockets[0]!)).toHaveLength(2); // no further re-request
    expect(gapLines(logs)).toContainEqual(
      expect.objectContaining({ level: "info", event: "healed", seq_from: 9, seq_to: 10 }),
    );
    client.stop();
  });

  it("while a re-request is pending, further beyond-gap frames are dropped WITHOUT another sync (no spam)", () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8));

    sockets[0]!.deliver(change(11)); // gap detected → one re-request
    sockets[0]!.deliver(change(13)); // in-flight live pushes beyond the gap…
    sockets[0]!.deliver(change(14)); // …are dropped; the replay covers them

    expect(store.applied.map((f) => f.seq)).toEqual([8]);
    expect(syncsSent(sockets[0]!)).toHaveLength(2); // on-open + ONE gap re-request
    expect(gapLines(logs).filter((l) => l["event"] === "detected")).toHaveLength(1);

    // Replay walks the hole and everything beyond, in order.
    for (const seq of [9, 10, 11, 12, 13, 14]) sockets[0]!.deliver(change(seq));
    expect(store.applied.map((f) => f.seq)).toEqual([8, 9, 10, 11, 12, 13, 14]);
    expect(store.getCursor()).toBe(14);
    expect(gapLines(logs).filter((l) => l["event"] === "healed")).toHaveLength(1);
    client.stop();
  });

  it("duplicates (seq <= highWater) still pass through to onChange (stale-guard dedup semantics preserved)", () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8));
    sockets[0]!.deliver(change(8)); // duplicate — e.g. a live push racing a replay of the same seq

    // Passed through unchanged (the consumer's stale-guard dedups); cursor never moves backward.
    expect(store.applied.map((f) => f.seq)).toEqual([8, 8]);
    expect(store.getCursor()).toBe(8);
    expect(syncsSent(sockets[0]!)).toHaveLength(1); // no gap machinery involved
    expect(gapLines(logs)).toEqual([]);
    client.stop();
  });

  it("an unanswered gap re-request retries on the deadline and ESCALATES to the full re-seed after gapRetryLimit", async () => {
    vi.useFakeTimers();
    const { client, store, sockets, statuses, logs } = makeGapClient({
      initialCursor: 7,
      gapTimeoutMs: 1000,
      gapRetryLimit: 3,
      reseedTo: 100,
    });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8));
    sockets[0]!.deliver(change(11)); // gap → re-request #1 (after 8)

    await vi.advanceTimersByTimeAsync(1000); // deadline → re-request #2
    await vi.advanceTimersByTimeAsync(1000); // deadline → re-request #3
    expect(syncsSent(sockets[0]!)).toEqual([
      { type: "sync", after: 7 },
      { type: "sync", after: 8 },
      { type: "sync", after: 8 },
      { type: "sync", after: 8 },
    ]);
    expect(store.reseedCalls.count).toBe(0); // still trying replay

    await vi.advanceTimersByTimeAsync(1000); // budget spent → escalate to reseed
    await vi.advanceTimersByTimeAsync(0); // flush the async reseed → reopen

    expect(store.reseedCalls.count).toBe(1);
    expect(statuses).toContain("resyncing");
    expect(gapLines(logs)).toContainEqual(
      expect.objectContaining({ level: "error", event: "escalated", seq_from: 9, seq_to: 10, retries: 3 }),
    );
    expect(sockets).toHaveLength(2); // reopened after the re-seed
    sockets[1]!.fireOpen();
    expect(sockets[1]!.lastSent()).toEqual({ type: "sync", after: 100 }); // fresh post-seed cursor
    // The old gap is gone: the next live frame from the fresh cursor applies cleanly.
    sockets[1]!.deliver(change(101));
    expect(store.getCursor()).toBe(101);
    client.stop();
  });

  it("{type:'resync'} answering a gap re-request triggers the existing full re-seed (never a silent no-op)", async () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 7, reseedTo: 50 });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8));
    sockets[0]!.deliver(change(11)); // gap → re-request
    expect(syncsSent(sockets[0]!)).toHaveLength(2);

    // The hole predates the mirror's retained window → replaySince answers resync → full re-seed.
    sockets[0]!.deliver({ type: "resync", accountId: "tenant-0" });
    await vi.waitFor(() => expect(sockets).toHaveLength(2));

    expect(store.reseedCalls.count).toBe(1);
    expect(store.getCursor()).toBe(50);
    sockets[1]!.fireOpen();
    expect(sockets[1]!.lastSent()).toEqual({ type: "sync", after: 50 });
    // The pending gap was superseded by the re-seed — no stray heal/escalate afterwards.
    expect(gapLines(logs).map((l) => l["event"])).toEqual(["detected"]);
    client.stop();
  });

  it("replay-to-live boundary: catch-up replay then the first live frame do NOT false-positive", () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 5 });
    void client.start();
    sockets[0]!.fireOpen(); // sync after 5 → server replays 6..8, then live 9 arrives
    for (const seq of [6, 7, 8]) sockets[0]!.deliver(change(seq)); // the replay
    sockets[0]!.deliver(change(9)); // first LIVE frame — legitimately highWater+1

    expect(store.applied.map((f) => f.seq)).toEqual([6, 7, 8, 9]);
    expect(syncsSent(sockets[0]!)).toHaveLength(1); // never re-requested
    expect(gapLines(logs)).toEqual([]);
    client.stop();
  });

  it("no baseline (cursor 0): the first frame is accepted at any seq; detection arms from then on", () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 0 });
    void client.start();
    sockets[0]!.fireOpen();

    sockets[0]!.deliver(change(5)); // highWater 0 = nothing to be contiguous with → accepted
    expect(store.applied.map((f) => f.seq)).toEqual([5]);
    expect(gapLines(logs)).toEqual([]);

    sockets[0]!.deliver(change(8)); // baseline now 5 → 8 skips 6,7 → detected
    expect(store.applied.map((f) => f.seq)).toEqual([5]);
    expect(gapLines(logs)).toContainEqual(
      expect.objectContaining({ event: "detected", seq_from: 6, seq_to: 7 }),
    );
    client.stop();
  });

  it("a reconnect during an unhealed gap re-syncs from the durable cursor — the hole is never sealed", () => {
    vi.useFakeTimers();
    const { client, store, sockets } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8));
    sockets[0]!.deliver(change(11)); // gap: 9,10 missing; cursor held at 8

    sockets[0]!.fireServerClose(); // socket dies mid-gap
    vi.advanceTimersByTime(1000); // backoff reconnect
    expect(sockets).toHaveLength(2);
    sockets[1]!.fireOpen();

    // The on-open sync re-requests from the durable cursor (8) — NOT from the gapped 11.
    expect(sockets[1]!.lastSent()).toEqual({ type: "sync", after: 8 });
    // The replay on the new socket heals the hole through the same path.
    for (const seq of [9, 10, 11]) sockets[1]!.deliver(change(seq));
    expect(store.getCursor()).toBe(11);
    client.stop();
  });

  it("lastChangeFrameAt moves only on change frames — auto-pongs stamp lastFrameAt but not it", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const { client, sockets } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();
    expect(client.lastChangeFrameAt).toBeNull();

    vi.advanceTimersByTime(500);
    sockets[0]!.deliver({ type: "pong" }); // socket alive — but the feed pushed nothing
    expect(client.lastFrameAt).toBe(500);
    expect(client.lastChangeFrameAt).toBeNull();

    vi.advanceTimersByTime(100);
    sockets[0]!.deliver(change(8)); // the feed actually delivered
    expect(client.lastChangeFrameAt).toBe(600);
    client.stop();
  });

  // ── FINDING 1: the heal deadline measures NO PROGRESS, not time-since-request ──
  // A big first heal (the steady-state case replays thousands of keyset-paginated frames) can take far
  // longer than one gapTimeoutMs while still advancing. The deadline must re-arm on every delivered
  // frame so such a heal never retries (overlapping replays) or escalates to a full /snapshot WHILE it
  // is progressing. The retry budget still bounds a genuinely STALLED heal (no frames at all).

  /** Inclusive integer range [from..to]. */
  const range = (from: number, to: number): number[] =>
    Array.from({ length: to - from + 1 }, (_, i) => from + i);

  it("a big heal that keeps making progress (each frame slower than one deadline) never retries or escalates", async () => {
    vi.useFakeTimers();
    const { client, store, sockets, statuses, logs } = makeGapClient({
      initialCursor: 7,
      gapTimeoutMs: 1000,
      gapRetryLimit: 3,
      reseedTo: 100,
    });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8)); // contiguous, baseline 8
    sockets[0]!.deliver(change(30)); // seqs 9..29 missing → GAP, one re-request (after 8)
    expect(syncsSent(sockets[0]!)).toEqual([
      { type: "sync", after: 7 },
      { type: "sync", after: 8 },
    ]);

    // The replay lands the hole frame-by-frame, each 800ms apart (< the 1000ms deadline). The whole
    // heal spans ~17s — many deadline widths — yet every frame refunds the deadline, so it never fires.
    for (let seq = 9; seq <= 30; seq++) {
      await vi.advanceTimersByTimeAsync(800);
      sockets[0]!.deliver(change(seq));
    }

    expect(store.getCursor()).toBe(30);
    expect(store.applied.map((f) => f.seq)).toEqual(range(8, 30));
    expect(syncsSent(sockets[0]!)).toHaveLength(2); // NO extra re-request while progress continued
    expect(store.reseedCalls.count).toBe(0); // NO escalation to /snapshot
    expect(statuses).not.toContain("resyncing");
    expect(gapLines(logs).map((l) => l["event"])).toEqual(["detected", "healed"]);
    client.stop();
  });

  it("progress refunds the deadline, but once the heal STALLS it still retries and escalates on the no-progress schedule", async () => {
    vi.useFakeTimers();
    const { client, store, sockets, statuses, logs } = makeGapClient({
      initialCursor: 7,
      gapTimeoutMs: 1000,
      gapRetryLimit: 3,
      reseedTo: 100,
    });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8));
    sockets[0]!.deliver(change(20)); // gap 9..19 → re-request (after 8)

    // Two frames of progress, each within a deadline window: the deadline keeps refunding, no retry.
    await vi.advanceTimersByTimeAsync(800);
    sockets[0]!.deliver(change(9));
    await vi.advanceTimersByTimeAsync(800);
    sockets[0]!.deliver(change(10));
    expect(syncsSent(sockets[0]!)).toHaveLength(2); // still only the initial re-request
    expect(store.reseedCalls.count).toBe(0);

    // Then the replay stalls at seq 10 (nothing more arrives). The no-progress deadline now fires on
    // the existing schedule: two retries from the advanced baseline (10), then escalate on the third.
    await vi.advanceTimersByTimeAsync(1000); // retry #1
    await vi.advanceTimersByTimeAsync(1000); // retry #2
    expect(syncsSent(sockets[0]!)).toEqual([
      { type: "sync", after: 7 },
      { type: "sync", after: 8 }, // initial gap re-request
      { type: "sync", after: 10 }, // retry from the baseline the progress advanced to
      { type: "sync", after: 10 },
    ]);
    expect(store.reseedCalls.count).toBe(0); // budget not yet spent

    await vi.advanceTimersByTimeAsync(1000); // third no-progress window → budget spent → escalate
    await vi.advanceTimersByTimeAsync(0); // flush the async reseed
    expect(store.reseedCalls.count).toBe(1);
    expect(statuses).toContain("resyncing");
    expect(gapLines(logs)).toContainEqual(
      expect.objectContaining({ level: "error", event: "escalated", retries: 3 }),
    );
    client.stop();
  });

  // ── FINDING 2: gap lifecycle log levels (a gap is steady-state; alert on `escalated` only) ──
  it("logs `detected` and `healed` at INFO and only `escalated` at ERROR", async () => {
    vi.useFakeTimers();
    const { client, sockets, logs } = makeGapClient({
      initialCursor: 7,
      gapTimeoutMs: 1000,
      gapRetryLimit: 2,
      reseedTo: 100,
    });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8));
    sockets[0]!.deliver(change(11)); // gap → detected (INFO)
    sockets[0]!.deliver(change(9));
    sockets[0]!.deliver(change(10)); // heal → healed (INFO)
    expect(gapLines(logs)).toContainEqual(expect.objectContaining({ level: "info", event: "detected" }));
    expect(gapLines(logs)).toContainEqual(expect.objectContaining({ level: "info", event: "healed" }));

    // A second, unanswered gap escalates → escalated (ERROR).
    sockets[0]!.deliver(change(11)); // baseline 10 → 11 contiguous, applied
    sockets[0]!.deliver(change(14)); // gap 12,13 → re-request, then stall
    await vi.advanceTimersByTimeAsync(1000); // retry #1
    await vi.advanceTimersByTimeAsync(1000); // budget (2) spent → escalate
    await vi.advanceTimersByTimeAsync(0);
    const escalated = gapLines(logs).filter((l) => l["event"] === "escalated");
    expect(escalated).toHaveLength(1);
    expect(escalated[0]!["level"]).toBe("error");
    // No gap line ever logs at warn.
    expect(gapLines(logs).some((l) => l["level"] === "warn")).toBe(false);
    client.stop();
  });

  // ── ADDITION: {type:"head", seq:N} end-of-pass nudge ──
  // A reconcile pass appends change_log rows the mirror never broadcasts individually; without a later
  // change frame the client has nothing to detect the hole FROM. The mirror broadcasts one end-of-pass
  // {type:"head", seq:<feed head>}; the client re-requests a head beyond its baseline exactly as a
  // beyond-gap change frame — but the head is never applied and never advances a cursor.

  const head = (seq: number) => ({ type: "head" as const, accountId: "tenant-0", seq });

  it("a {type:'head'} frame ahead of the baseline starts one re-request that heals via replay", () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8)); // contiguous, baseline 8

    sockets[0]!.deliver(head(10)); // reconcile appended 9,10 (un-broadcast) → head nudges

    // The head frame is NOT applied and advances no cursor — but it triggers exactly one re-request.
    expect(store.applied.map((f) => f.seq)).toEqual([8]);
    expect(store.getCursor()).toBe(8);
    expect(syncsSent(sockets[0]!)).toEqual([
      { type: "sync", after: 7 },
      { type: "sync", after: 8 },
    ]);
    // The hole spans up to AND INCLUDING the head seq — 10 is itself a real, un-broadcast row.
    expect(gapLines(logs)).toContainEqual(
      expect.objectContaining({ level: "info", event: "detected", seq_from: 9, seq_to: 10, size: 2 }),
    );

    // The replay redelivers 9,10 as ordinary change frames → heal.
    sockets[0]!.deliver(change(9));
    sockets[0]!.deliver(change(10));
    expect(store.applied.map((f) => f.seq)).toEqual([8, 9, 10]);
    expect(store.getCursor()).toBe(10);
    expect(gapLines(logs)).toContainEqual(
      expect.objectContaining({ event: "healed", seq_from: 9, seq_to: 10 }),
    );
    client.stop();
  });

  it("a {type:'head'} frame at or below the baseline is a no-op (no sync, no telemetry)", () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8)); // baseline 8

    sockets[0]!.deliver(head(8)); // caught up
    sockets[0]!.deliver(head(5)); // a stale head

    expect(syncsSent(sockets[0]!)).toEqual([{ type: "sync", after: 7 }]); // only the on-open sync
    expect(gapLines(logs)).toEqual([]);
    expect(store.applied.map((f) => f.seq)).toEqual([8]);
    client.stop();
  });

  it("a {type:'head'} frame while a gap is already pending is ignored (no extra sync)", () => {
    const { client, sockets, logs } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8));
    sockets[0]!.deliver(change(11)); // gap 9,10 → one re-request pending
    expect(syncsSent(sockets[0]!)).toHaveLength(2);

    sockets[0]!.deliver(head(20)); // the in-flight replay already covers up to (past) the head

    expect(syncsSent(sockets[0]!)).toHaveLength(2); // no extra re-request
    expect(gapLines(logs).filter((l) => l["event"] === "detected")).toHaveLength(1);
    client.stop();
  });

  it("a {type:'head'} frame is transport-internal — surfaced to neither onFrame nor onChange", () => {
    const store = makeStore(7);
    const { sockets, factory } = recordingFactory();
    const framesSeen: string[] = [];
    const client = new LiveSyncClient({
      baseUrl: BASE,
      accountId: "tenant-0",
      auth: { kind: "cookie" },
      reseed: store.reseedTo(0),
      getCursor: store.getCursor,
      onChange: store.onChange,
      onFrame: (f) => framesSeen.push(f.type),
      pingIntervalMs: 0,
      wsFactory: factory,
    });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8)); // a real change → surfaced to onFrame
    sockets[0]!.deliver(head(20)); // internal nudge → NOT surfaced (triggers a gap re-request instead)

    expect(framesSeen).toEqual(["change"]); // head never reached onFrame
    expect(store.applied.map((f) => f.seq)).toEqual([8]); // head never reached onChange
    client.stop();
  });

  it("an unknown/future frame type is still ignored gracefully (old-SDK-vs-new-mirror forward compat)", () => {
    const { client, store, sockets, logs } = makeGapClient({ initialCursor: 7 });
    void client.start();
    sockets[0]!.fireOpen();
    sockets[0]!.deliver(change(8));

    sockets[0]!.deliver({ type: "future-nudge", seq: 99 }); // a frame type this SDK version predates
    sockets[0]!.deliver({ type: "bogus" });

    expect(store.applied.map((f) => f.seq)).toEqual([8]);
    expect(syncsSent(sockets[0]!)).toEqual([{ type: "sync", after: 7 }]); // nothing re-requested
    expect(gapLines(logs)).toEqual([]);
    client.stop();
  });
});
