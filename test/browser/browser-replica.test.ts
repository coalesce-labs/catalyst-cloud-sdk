import { describe, it, expect, vi, afterEach } from "vitest";
import { BrowserReplica, type ReplicaStatus } from "../../src/replica/browser/browser-replica.js";
import type { Envelope, ReplicaResponse } from "../../src/replica/browser/protocol.js";

// CTC-114 — the main-thread client shell, driven against a FAKE worker (the real worker logic is
// covered by worker-core.test.ts over real sqlite-wasm). What is pinned here is the client's own
// contract: lock-loss surfaces "secondary" without ever constructing a worker; a warm cursor skips
// the seed; close() sends a cooperative `close` RPC BEFORE terminate and releases the lock; and a
// call after close rejects rather than hanging.

/** A scriptable fake Worker: answers each request type with a canned result. */
class FakeWorker {
  listeners: ((e: MessageEvent<ReplicaResponse>) => void)[] = [];
  received: Envelope[] = [];
  terminated = false;
  constructor(private readonly answers: Record<string, unknown>) {}
  addEventListener(_type: string, fn: (e: MessageEvent<ReplicaResponse>) => void): void {
    this.listeners.push(fn);
  }
  postMessage(envelope: Envelope): void {
    this.received.push(envelope);
    const result = this.answers[envelope.request.type];
    const reply: ReplicaResponse = { id: envelope.id, ok: true, result };
    // Deliver async, like a real worker.
    queueMicrotask(() => {
      for (const fn of this.listeners) fn({ data: reply } as MessageEvent<ReplicaResponse>);
    });
  }
  terminate(): void {
    this.terminated = true;
  }
}

function collect() {
  const statuses: ReplicaStatus[] = [];
  let changed = 0;
  return {
    statuses,
    changedCount: () => changed,
    handlers: {
      onChanged: () => {
        changed++;
      },
      onStatus: (s: ReplicaStatus) => {
        statuses.push(s);
      },
    },
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("BrowserReplica (CTC-114)", () => {
  it("surfaces 'secondary' on lock loss and never constructs the worker", async () => {
    // navigator.locks that always reports the lock as taken.
    vi.stubGlobal("navigator", {
      locks: {
        request: (_n: string, _o: unknown, cb: (l: null) => void) =>
          Promise.resolve(cb(null)),
      },
    });
    let constructed = 0;
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "/api/v1",
      createWorker: () => {
        constructed++;
        return new FakeWorker({}) as unknown as Worker;
      },
    });
    await replica.start();
    expect(c.statuses).toEqual(["loading", "secondary"]);
    expect(constructed).toBe(0);
    replica.close(); // must be safe with nothing booted
  });

  it("warm cursor skips the seed: open → getCursor → live, no fetch", async () => {
    vi.stubGlobal("navigator", {}); // no Web Locks → no-op gate, proceeds
    vi.stubGlobal("WebSocket", undefined); // node 22+ HAS a global WebSocket — keep subscribe() out
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const worker = new FakeWorker({ open: undefined, getCursor: 42, close: undefined });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "/api/v1",
      createWorker: () => worker as unknown as Worker,
    });
    await replica.start();

    expect(worker.received.map((e) => e.request.type)).toEqual(["open", "getCursor"]);
    expect(fetchSpy).not.toHaveBeenCalled(); // warm replica must NOT re-download /snapshot
    expect(c.statuses).toEqual(["loading", "live"]);
    expect(c.changedCount()).toBe(1);
    replica.close();
  });

  it("close() sends a cooperative close RPC, then terminates on the reply", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    const worker = new FakeWorker({ open: undefined, getCursor: 42, close: undefined });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "/api/v1",
      createWorker: () => worker as unknown as Worker,
    });
    await replica.start();

    replica.close();
    expect(worker.received.at(-1)?.request.type).toBe("close");
    expect(worker.terminated).toBe(false); // not yet — the grace window is open
    await new Promise((r) => setTimeout(r, 0)); // the fake replies on a microtask
    expect(worker.terminated).toBe(true); // …and the reply, not the timeout, triggered terminate
  });

  it("a query after close rejects instead of hanging", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    const worker = new FakeWorker({ open: undefined, getCursor: 42, close: undefined });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "/api/v1",
      createWorker: () => worker as unknown as Worker,
    });
    await replica.start();
    replica.close();
    await expect(replica.queryIssues()).rejects.toThrow(/closed/);
  });

  it("a query before start rejects instead of hanging", async () => {
    const c = collect();
    const replica = new BrowserReplica(c.handlers, { baseUrl: "/api/v1" });
    await expect(replica.queryIssues()).rejects.toThrow(/closed/);
  });
});
