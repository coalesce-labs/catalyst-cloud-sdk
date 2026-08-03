import { describe, it, expect, vi, afterEach } from "vitest";
import {
  BrowserReplica,
  type BrowserReplicaOptions,
  type ReplicaHandlers,
  type ReplicaStatus,
} from "../../src/replica/browser/browser-replica.js";
import type {
  Envelope,
  ReplicaResponse,
} from "../../src/replica/browser/protocol.js";

// CTC-114 — the main-thread client shell, driven against a FAKE worker (the real worker logic is
// covered by worker-core.test.ts over real sqlite-wasm). What is pinned here is the client's own
// contract: lock-loss surfaces "secondary" without ever constructing a worker; a warm cursor skips
// the seed; close() sends a cooperative `close` RPC BEFORE terminate and releases the lock; a call
// after close rejects rather than hanging; and (CTC-114 review) every way the boot, the seed, or the
// worker itself can fail releases the origin lock instead of wedging every tab on the origin.
//
// NOTE ON `location`: it is undefined under vitest's node environment, so resolveBase falls back to
// http://localhost and the same-origin guard returns early. Tests that depend on the guard stub
// `location` explicitly.

/**
 * A scriptable fake Worker.
 *
 * Listeners are keyed BY TYPE. This is not incidental: the client registers `error` and
 * `messageerror` listeners alongside `message`, and a single shared listener array delivers every RPC
 * reply to the error handler too. Since a MessageEvent carries `.data` and not `.message`, the
 * client's error describer would then latch on the first successful reply and fail the boot.
 */
class FakeWorker {
  private readonly listeners = new Map<string, ((e: unknown) => void)[]>();
  received: Envelope[] = [];
  terminated = false;
  /** When true, never answer — models a worker that loaded but is wedged. */
  silent = false;

  constructor(
    private readonly answers: Record<string, unknown> = {},
    /** Per-request-type reply delay in ms — models a SLOW worker (e.g. an OPFS commit on throttled
     *  storage) as distinct from a wedged one (`silent`). */
    private readonly delays: Record<string, number> = {},
  ) {}

  addEventListener(type: string, fn: (e: unknown) => void): void {
    const list = this.listeners.get(type) ?? [];
    list.push(fn);
    this.listeners.set(type, list);
  }

  private emit(type: string, event: unknown): void {
    for (const fn of this.listeners.get(type) ?? []) fn(event);
  }

  postMessage(envelope: Envelope): void {
    this.received.push(envelope);
    if (this.silent) return;
    const answer = this.answers[envelope.request.type];
    // An Error-valued answer models the worker rejecting THAT request (the err envelope).
    const reply: ReplicaResponse =
      answer instanceof Error
        ? { id: envelope.id, ok: false, error: answer.message }
        : { id: envelope.id, ok: true, result: answer };
    const send = (): void => {
      this.emit("message", { data: reply } as MessageEvent<ReplicaResponse>);
    };
    const delay = this.delays[envelope.request.type] ?? 0;
    if (delay > 0) setTimeout(send, delay);
    else queueMicrotask(send);
  }

  /** A module-script load failure: a BARE Event, no `.message`, no `.error`. */
  failToLoad(): void {
    this.emit("error", new Event("error"));
  }

  /** A runtime error inside the worker: ErrorEvent-shaped, carries a message. */
  failWith(message: string): void {
    this.emit("error", { message } as unknown as Event);
  }

  terminate(): void {
    this.terminated = true;
  }
}

/**
 * A Web Locks stub with REAL `ifAvailable` semantics — a `held` set, not the always-null stub the
 * lock tests used to share. The always-null version is precisely why a non-one-shot `start()` went
 * unnoticed: every request looked like a loss, so no test could observe a double claim.
 */
function fakeLockManager() {
  const held = new Set<string>();
  return {
    held,
    isHeld: (name: string) => held.has(name),
    locks: {
      request: (
        name: string,
        opts: { ifAvailable?: boolean; signal?: AbortSignal } | undefined,
        cb: (lock: unknown) => unknown,
      ): Promise<unknown> => {
        if (opts?.ifAvailable && held.has(name)) return Promise.resolve(cb(null));
        held.add(name);
        // THE WEB LOCKS IDIOM: the lock lives exactly as long as the promise the callback RETURNS —
        // it is not released by calling a method on the lock object. browser-lock.ts relies on this
        // (its `release()` resolves that inner promise), so a stub that ignores the return value
        // never observes a release and reports every lock as permanently held.
        const result = cb({ name });
        return Promise.resolve(result).finally(() => {
          held.delete(name);
        });
      },
    },
  };
}

/** A recording WebSocket global: every constructed socket, its URL, and whether it was closed. */
class FakeSocket {
  static sockets: FakeSocket[] = [];
  sent: string[] = [];
  closed = false;
  onopen: ((e: unknown) => void) | null = null;
  onmessage: ((e: { data: unknown }) => void) | null = null;
  onclose: ((e: unknown) => void) | null = null;
  onerror: ((e: unknown) => void) | null = null;
  constructor(readonly url: string) {
    FakeSocket.sockets.push(this);
  }
  send(data: string): void {
    this.sent.push(data);
  }
  close(): void {
    this.closed = true;
  }
  fireOpen(): void {
    this.onopen?.({});
  }
  deliver(frame: unknown): void {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }
  lastSent(): unknown {
    return JSON.parse(this.sent[this.sent.length - 1]!);
  }
}

function recordingSocketGlobal(): typeof FakeSocket {
  FakeSocket.sockets = [];
  vi.stubGlobal("WebSocket", FakeSocket);
  return FakeSocket;
}

/** ONE fetch-stub factory. `vi.stubGlobal` is last-write-wins within a test, so several ad-hoc
 *  stubs in one test silently discard all but the last. */
function stubFetch(
  impl: (url: string, init?: RequestInit) => Promise<Response> | Response,
) {
  const spy = vi.fn(impl as (...a: unknown[]) => unknown);
  vi.stubGlobal("fetch", spy);
  return spy;
}

/**
 * Teardown is asynchronous BY CONTRACT: close()/releaseResources() send a cooperative `close` RPC and
 * terminate on the reply, or at CLOSE_GRACE_MS (250ms) if the worker never answers. So a silent worker
 * releases the lock only at the grace deadline — waiting is not test slop, it is the bound under test.
 */
const CLOSE_GRACE_MS = 250;
const settleTeardown = (ms = CLOSE_GRACE_MS + 20) =>
  new Promise((r) => setTimeout(r, ms));

/**
 * A body stream that never delivers, and ERRORS when `signal` aborts.
 *
 * Real `fetch` propagates an abort into the body stream; a naive fake that only rejects the outer
 * promise leaves `reader.read()` pending forever, so the test times out even though the client did
 * abort correctly — it would look like a product bug and it is not.
 */
function stalledResponse(signal: AbortSignal | null | undefined): Response {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      signal?.addEventListener("abort", () => {
        try {
          controller.error(new Error("The operation was aborted"));
        } catch {
          // already closed/errored
        }
      });
    },
  });
  return { ok: true, status: 200, body } as unknown as Response;
}

/** An NDJSON /snapshot body: `rows` data lines then the terminal cursor line. */
function snapshotBody(rows: unknown[], cursor: number): ReadableStream<Uint8Array> {
  const lines = [
    ...rows.map((r) => JSON.stringify(r)),
    JSON.stringify({ accountId: "t", cursor }),
  ].join("\n");
  const bytes = new TextEncoder().encode(lines);
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

function okSnapshot(rows: unknown[], cursor: number): Response {
  return {
    ok: true,
    status: 200,
    body: snapshotBody(rows, cursor),
  } as unknown as Response;
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

/** The options every test shares; `identity` is required, so it cannot be omitted here. */
function opts(over: Partial<BrowserReplicaOptions> = {}): BrowserReplicaOptions {
  return { baseUrl: "/api/v1", identity: "u1", ...over };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("BrowserReplica (CTC-114)", () => {
  it("surfaces 'secondary' on lock loss and never constructs the worker", async () => {
    const locks = fakeLockManager();
    locks.held.add("catalyst-replica:.catalyst-replica"); // someone else owns it
    vi.stubGlobal("navigator", locks);
    let constructed = 0;
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "/api/v1",
      identity: "u1",
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
    const fetchSpy = stubFetch(() => {
      throw new Error("must not fetch");
    });
    const worker = new FakeWorker({
      open: undefined,
      getCursor: 42,
      close: undefined,
    });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "/api/v1",
      identity: "u1",
      createWorker: () => worker as unknown as Worker,
    });
    await replica.start();

    expect(worker.received.map((e) => e.request.type)).toEqual([
      "open",
      "getCursor",
    ]);
    expect(fetchSpy).not.toHaveBeenCalled(); // warm replica must NOT re-download /snapshot
    expect(c.statuses).toEqual(["loading", "live"]);
    expect(c.changedCount()).toBe(1);
    replica.close();
  });

  it("sends the tenant identity with `open`", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    const worker = new FakeWorker({ open: undefined, getCursor: 42 });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "/api/v1",
      identity: "user-a",
      accountId: "tenant-7",
      createWorker: () => worker as unknown as Worker,
    });
    await replica.start();
    const open = worker.received[0]?.request as { identity?: string };
    // accountId is folded in so a tenant switcher cannot reuse another account's rows even when the
    // consumer's identity only names the signed-in user.
    expect(open.identity).toBe("user-a\0tenant-7");
    replica.close();
  });

  it("close() sends a cooperative close RPC, then terminates on the reply", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    const worker = new FakeWorker({
      open: undefined,
      getCursor: 42,
      close: undefined,
    });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      createWorker: () => worker as unknown as Worker,
    }));
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
    const worker = new FakeWorker({
      open: undefined,
      getCursor: 42,
      close: undefined,
    });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      createWorker: () => worker as unknown as Worker,
    }));
    await replica.start();
    replica.close();
    await expect(replica.queryIssues()).rejects.toThrow(/closed/);
  });

  it("a query before start rejects instead of hanging", async () => {
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts());
    await expect(replica.queryIssues()).rejects.toThrow(/closed/);
  });
});

describe("identity is a required fence (CTC-114 review, Ks5)", () => {
  it("throws at construction when identity is empty", () => {
    const c = collect();
    // An untyped-JS consumer following the old README produced "undefined\0" for EVERY user — a fence
    // that looks isolated and is not.
    expect(
      () =>
        new BrowserReplica(c.handlers, {
          baseUrl: "/api/v1",
          identity: "",
        }),
    ).toThrow(/identity` is required/);
    expect(
      () =>
        new BrowserReplica(c.handlers, {
          baseUrl: "/api/v1",
        } as never),
    ).toThrow(/identity` is required/);
  });

  it("constructs with a real identity (negative control)", () => {
    const c = collect();
    expect(() => new BrowserReplica(c.handlers, opts())).not.toThrow();
  });
});

describe("start() is one-shot (CTC-114 review, KtK)", () => {
  it("rejects a second start() and constructs only ONE worker", async () => {
    const locks = fakeLockManager();
    vi.stubGlobal("navigator", locks);
    vi.stubGlobal("WebSocket", undefined);
    let constructed = 0;
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      createWorker: () => {
        constructed++;
        return new FakeWorker({ open: undefined, getCursor: 42 }) as unknown as Worker;
      },
    }));
    await replica.start();
    await expect(replica.start()).rejects.toThrow(/already called/);
    // The real damage of a second start is a second worker over the same OPFS handles — or, with the
    // lock enabled, a spurious "secondary" against this instance's OWN lock.
    expect(constructed).toBe(1);
    expect(c.statuses).not.toContain("secondary");
    replica.close();
  });

  it("rejects start() after close()", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      createWorker: () =>
        new FakeWorker({ open: undefined, getCursor: 42 }) as unknown as Worker,
    }));
    replica.close();
    await expect(replica.start()).rejects.toThrow(/after close/);
  });
});

describe("baseUrl must resolve to this origin (CTC-114 review, KtD)", () => {
  it("rejects a cross-origin baseUrl BEFORE requesting the lock", async () => {
    const locks = fakeLockManager();
    vi.stubGlobal("navigator", locks);
    vi.stubGlobal("location", { origin: "https://app.example" });
    vi.stubGlobal("WebSocket", undefined);
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "https://other.example/api/v1",
      identity: "u1",
      createWorker: () => new FakeWorker({}) as unknown as Worker,
    });
    await expect(replica.start()).rejects.toThrow(/must resolve to this origin/);
    // The ordering is the point: a misconfigured tab that claimed the origin-wide lock would hold
    // every OTHER tab in "secondary" for the life of the document.
    expect(locks.held.size).toBe(0);
    expect(c.statuses).toEqual([]);
  });

  it("accepts an absolute SAME-origin baseUrl (negative control against an over-broad guard)", async () => {
    const locks = fakeLockManager();
    vi.stubGlobal("navigator", locks);
    vi.stubGlobal("location", { origin: "https://app.example" });
    vi.stubGlobal("WebSocket", undefined);
    const worker = new FakeWorker({ open: undefined, getCursor: 42 });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "https://app.example/api/v1",
      identity: "u1",
      createWorker: () => worker as unknown as Worker,
    });
    await replica.start();
    expect(c.statuses.at(-1)).toBe("live");
    replica.close();
  }, 15_000);
});

describe("a failed boot releases the worker AND the origin lock (CTC-114 review, Ks-)", () => {
  it("frees the lock so a SECOND instance can boot", async () => {
    const locks = fakeLockManager();
    vi.stubGlobal("navigator", locks);
    vi.stubGlobal("WebSocket", undefined);
    const bad = new FakeWorker({ open: new Error("OPFS unavailable") });
    const c1 = collect();
    const first = new BrowserReplica(c1.handlers, opts({
      createWorker: () => bad as unknown as Worker,
    }));
    await expect(first.start()).rejects.toThrow(/OPFS unavailable/);
    await settleTeardown();

    expect(c1.statuses.at(-1)).toBe("error");
    expect(bad.terminated).toBe(true);
    expect(locks.held.size).toBe(0);

    // start() is one-shot, so the recovery path is a NEW instance — which is what the consuming hook
    // does anyway (a fresh replica per effect run). This is the assertion that matters: the failure
    // did not wedge the origin.
    const good = new FakeWorker({ open: undefined, getCursor: 42 });
    const c2 = collect();
    const second = new BrowserReplica(c2.handlers, opts({
      createWorker: () => good as unknown as Worker,
    }));
    await second.start();
    expect(c2.statuses.at(-1)).toBe("live");
    second.close();
  });
});

describe("a worker that never loads fails loudly (CTC-114 review, KtA)", () => {
  it("rejects start(), releases the lock, and reports the real cause on later reads", async () => {
    const locks = fakeLockManager();
    vi.stubGlobal("navigator", locks);
    vi.stubGlobal("WebSocket", undefined);
    const worker = new FakeWorker({});
    worker.silent = true; // loaded nothing, answers nothing
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      createWorker: () => {
        // The load failure lands asynchronously, after the client has posted `open`.
        queueMicrotask(() => worker.failToLoad());
        return worker as unknown as Worker;
      },
    }));

    // Without the error listener this hangs forever holding the origin lock — the single most likely
    // first-integration failure for a third party.
    await expect(replica.start()).rejects.toThrow(/bundler|worker failed/i);
    await settleTeardown();
    expect(locks.held.size).toBe(0);
    expect(c.statuses.at(-1)).toBe("error");

    // And the CAUSE survives — not the generic "replica client closed" that teardown would produce.
    await expect(replica.queryIssues()).rejects.toThrow(/db.worker chunk/);
  });

  it("prefers a real error message when the event carries one", async () => {
    const locks = fakeLockManager();
    vi.stubGlobal("navigator", locks);
    vi.stubGlobal("WebSocket", undefined);
    const worker = new FakeWorker({});
    worker.silent = true;
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      createWorker: () => {
        queueMicrotask(() => worker.failWith("Cannot find module 'sqlite-wasm'"));
        return worker as unknown as Worker;
      },
    }));
    await expect(replica.start()).rejects.toThrow(/sqlite-wasm/);
  });
});

describe("close() from inside a status callback opens no socket (CTC-114 review, KtG)", () => {
  it("does not construct a WebSocket when the consumer closes during onStatus", async () => {
    vi.stubGlobal("navigator", {});
    const Sock = recordingSocketGlobal();
    const worker = new FakeWorker({ open: undefined, getCursor: 42 });
    const statuses: ReplicaStatus[] = [];
    let replica: BrowserReplica;
    const handlers = {
      onChanged: () => {},
      onStatus: (s: ReplicaStatus) => {
        statuses.push(s);
        // The warm path fires "live" synchronously from start(), BEFORE subscribe() runs — and at
        // that moment `this.live` is still null, so this close() has no transport to stop.
        if (s === "live") replica.close();
      },
    };
    replica = new BrowserReplica(handlers, opts({
      createWorker: () => worker as unknown as Worker,
    }));
    await replica.start();
    // An unstoppable self-reconnecting socket re-fetching /snapshot forever from a torn-down document.
    expect(Sock.sockets).toHaveLength(0);
  });

  it("DOES open a socket on a normal warm start (negative control)", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("location", { origin: "https://app.example" });
    const Sock = recordingSocketGlobal();
    const worker = new FakeWorker({ open: undefined, getCursor: 42 });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, {
      baseUrl: "https://app.example/api/v1",
      identity: "u1",
      createWorker: () => worker as unknown as Worker,
    });
    await replica.start();
    expect(Sock.sockets).toHaveLength(1);
    // No account named → the parameter is omitted entirely, not serialized as `?account=`.
    expect(Sock.sockets[0]!.url).toBe("wss://app.example/api/v1/connect");
    replica.close();
  }, 15_000);
});

describe("the seed is bounded and supersedable (CTC-114 review, Ks9)", () => {
  it("sends credentials with the /snapshot fetch", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    const fetchSpy = stubFetch(() => okSnapshot([], 5));
    const worker = new FakeWorker({
      open: undefined,
      getCursor: null,
      seedBegin: undefined,
      seedBatch: undefined,
      seedCommit: 5,
    });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      createWorker: () => worker as unknown as Worker,
    }));
    await replica.start();
    const init = fetchSpy.mock.calls[0]?.[1] as RequestInit;
    expect(init.credentials).toBe("include");
    expect(init.signal).toBeDefined();
    replica.close();
  });

  it("aborts a stalled body via the idle timeout instead of hanging start()", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    // Headers arrive, then the body delivers nothing, ever.
    stubFetch((_url, init) => stalledResponse((init as RequestInit).signal));
    const worker = new FakeWorker({
      open: undefined,
      getCursor: null,
      seedBegin: undefined,
      seedAbort: undefined,
    });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      snapshotIdleTimeoutMs: 20,
      createWorker: () => worker as unknown as Worker,
    }));
    // Without the idle bound this never settles, and every read queues behind the worker's armed
    // SeedReadGate. The bound is IDLE-only — a slow-but-progressing 100 MB seed is not killed.
    await expect(replica.start()).rejects.toThrow();
    expect(c.statuses.at(-1)).toBe("error");
  });

  it("close() aborts an in-flight seed", async () => {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    let aborted = false;
    stubFetch((_url, init) => {
      const signal = (init as RequestInit).signal;
      signal?.addEventListener("abort", () => {
        aborted = true;
      });
      return stalledResponse(signal);
    });
    const worker = new FakeWorker({
      open: undefined,
      getCursor: null,
      seedBegin: undefined,
      seedAbort: undefined,
      close: undefined,
    });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      snapshotIdleTimeoutMs: 0, // disabled — close() must be what aborts it
      createWorker: () => worker as unknown as Worker,
    }));
    const booting = replica.start();
    await new Promise((r) => setTimeout(r, 5));
    replica.close();
    await expect(booting).rejects.toThrow();
    // The ~100 MB response must stop draining server-side, not just be ignored here.
    expect(aborted).toBe(true);
  });
});

describe("a queue overflow quiesces the socket and never stays latched (CTC-114 review, Ks8 + Wl0)", () => {
  /**
   * Drive the replica to an "apply-failed" overflow: the worker rejects every applyChanges, so after
   * MAX_APPLY_RETRIES the queue drops its backlog and asks the owner to re-seed. Cheaper and more
   * deterministic than the depth path, which needs 20k frames, and it exercises the identical
   * onOverflow contract.
   */
  async function bootToOverflow(
    snapshot: () => Response | Promise<Response>,
    /** Swap in handlers that misbehave (e.g. an onStatus that throws) — defaults to the recorder. */
    handlersOverride?: ReplicaHandlers,
  ) {
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("location", { origin: "https://app.example" });
    const Sock = recordingSocketGlobal();
    const fetchSpy = stubFetch(snapshot);
    const worker = new FakeWorker({
      open: undefined,
      getCursor: 42,
      // Every live apply rejects → the queue escalates after its bounded retries.
      applyChanges: new Error("SQLITE_IOERR"),
      seedBegin: undefined,
      seedBatch: undefined,
      seedCommit: 99,
      seedAbort: undefined,
      close: undefined,
    });
    const c = collect();
    const replica = new BrowserReplica(handlersOverride ?? c.handlers, {
      baseUrl: "https://app.example/api/v1",
      identity: "u1",
      createWorker: () => worker as unknown as Worker,
    });
    await replica.start();
    expect(Sock.sockets).toHaveLength(1);
    Sock.sockets[0]!.fireOpen();
    // One live frame is enough — the same batch is requeued and retried until the cap.
    Sock.sockets[0]!.deliver({
      type: "change",
      seq: 43,
      entity: "issues",
      entityId: "i1",
      op: "upsert",
      row: { id: "i1" },
    });
    return { Sock, fetchSpy, worker, replica, c };
  }

  it("closes the live socket BEFORE fetching the replacement snapshot", async () => {
    let closedAtFetch: boolean | null = null;
    const { Sock, replica } = await bootToOverflow(() => {
      // Sampled AT the fetch, not after — the ordering is the invariant, and an after-the-fact read
      // would pass even if the close happened later.
      closedAtFetch = Sock0()?.closed ?? null;
      return okSnapshot([], 99);
    });
    function Sock0() {
      return Sock.sockets[0];
    }

    await vi.waitFor(() => expect(closedAtFetch).not.toBeNull(), { timeout: 4000 });
    // Re-seeding while the socket still delivers drops every frame written during the multi-second
    // /snapshot into a window that is in neither the snapshot nor the DB — and the transport counts
    // them as delivered, so the gap detector is structurally blind to them.
    expect(closedAtFetch).toBe(true);
    replica.close();
  });

  it("resumes the queue even when the replacement snapshot FAILS", async () => {
    // The overflow latch gates every subsequent push. Resuming only on success left a transient
    // /snapshot error discarding every later frame while the socket stayed up — a silently stale
    // replica reporting "live" until the tab was reloaded.
    const { Sock, worker, replica } = await bootToOverflow(() => {
      throw new Error("snapshot 503");
    });

    await vi.waitFor(
      () => expect(Sock.sockets.length).toBeGreaterThanOrEqual(2),
      { timeout: 4000 },
    );

    const appliesBefore = worker.received.filter(
      (e) => e.request.type === "applyChanges",
    ).length;
    Sock.sockets.at(-1)!.fireOpen();
    // seq 43, NOT 44. The overflow rolled `acceptedSeq` back to the durable cursor (42), so the
    // reconnect re-requests from 42 — and the transport correctly WITHHOLDS a frame at 44 as a gap.
    // That rollback is itself the fix for a failed reseed sealing a second hole; delivering 44 here
    // would test the gap detector rather than the overflow latch.
    Sock.sockets.at(-1)!.deliver({
      type: "change",
      seq: 43,
      entity: "issues",
      entityId: "i2",
      op: "upsert",
      row: { id: "i2" },
    });

    // The OUTCOME that matters: a post-overflow frame still reaches the worker. A latched queue would
    // silently drop it and nothing else would observe the difference.
    await vi.waitFor(
      () =>
        expect(
          worker.received.filter((e) => e.request.type === "applyChanges").length,
        ).toBeGreaterThan(appliesBefore),
      { timeout: 4000 },
    );
    replica.close();
  }, 15_000);
  it("rolls the transport high-water back to the DURABLE cursor", async () => {
    // The queue just discarded its inbox, so `acceptedSeq` describes frames that were received and
    // then thrown away. Without the rollback, a FAILED replacement seed leaves the next connect
    // resuming ABOVE those frames — sealing a second hole that nothing ever re-requests, because the
    // transport counts them as delivered and the gap detector is structurally blind to them.
    const { Sock, replica } = await bootToOverflow(() => {
      throw new Error("snapshot 503");
    });

    await vi.waitFor(
      () => expect(Sock.sockets.length).toBeGreaterThanOrEqual(2),
      { timeout: 4000 },
    );
    Sock.sockets.at(-1)!.fireOpen();

    // 42 = the durable cursor. 43 (the discarded frame's seq) would be the bug.
    expect(Sock.sockets.at(-1)!.lastSent()).toEqual({ type: "sync", after: 42 });
    replica.close();
  }, 15_000);
  it("recovers even when the consumer's onStatus THROWS during the overflow", async () => {
    // CTC-114 review round 4 (P2). `onStatus("reconnecting")` is raised at the TOP of the overflow
    // handler — before `acceptedSeq` is rolled back and `requestResync()` dispatched. Consumer
    // handlers are arbitrary app code (a React setState that throws lands right here), and an
    // uncaught throw left the queue OVERFLOW-LATCHED: `push()` then refuses every subsequent frame
    // while the socket stays happily open. DeltaQueue.notify() catches the exception, so nothing
    // reports an error — the replica just silently stops applying while still claiming to be live.
    let threw = 0;
    const handlers: ReplicaHandlers = {
      onChanged: () => {},
      onStatus: (s) => {
        if (s === "reconnecting") {
          threw++;
          throw new Error("consumer setState blew up");
        }
      },
    };
    // A FAILING replacement snapshot, like the two tests above: it keeps the durable cursor at 42 so
    // the rollback is observable, and it is the arm where a latched queue is unrecoverable.
    const { Sock, worker, replica } = await bootToOverflow(() => {
      throw new Error("snapshot 503");
    }, handlers);

    // The throwing handler really did fire on the overflow — otherwise this test proves nothing.
    await vi.waitFor(() => expect(threw).toBeGreaterThan(0), { timeout: 4000 });

    await vi.waitFor(
      () => expect(Sock.sockets.length).toBeGreaterThanOrEqual(2),
      { timeout: 4000 },
    );
    Sock.sockets.at(-1)!.fireOpen();
    // OUTCOME 1: recovery ran past the throw — the high-water was rolled back to the DURABLE cursor,
    // so the reconnect re-requests from 42 rather than sealing the discarded frame over.
    expect(Sock.sockets.at(-1)!.lastSent()).toEqual({ type: "sync", after: 42 });

    const appliesBefore = worker.received.filter(
      (e) => e.request.type === "applyChanges",
    ).length;
    Sock.sockets.at(-1)!.deliver({
      type: "change",
      seq: 43,
      entity: "issues",
      entityId: "i2",
      op: "upsert",
      row: { id: "i2" },
    });
    // OUTCOME 2: the queue is UNLATCHED — a post-overflow frame still reaches the worker.
    await vi.waitFor(
      () =>
        expect(
          worker.received.filter((e) => e.request.type === "applyChanges")
            .length,
        ).toBeGreaterThan(appliesBefore),
      { timeout: 4000 },
    );
    replica.close();
  }, 15_000);

  it("a SLOW worker commit does not trip the snapshot idle bound", async () => {
    // CTC-114 review round 5. The idle timer re-armed on body chunks only, so it stayed armed across
    // every seedBatch RPC and — the sharp case — across the final seedCommit, which runs AFTER the
    // body is fully consumed and so can never be re-armed by progress. A ~100 MB commit on slow or
    // background-throttled OPFS that crossed the bound aborted a seed whose stream was perfectly
    // healthy, and the post-stream re-check then reported an ALREADY-COMMITTED snapshot as "seed
    // superseded" — so the client discarded it and re-fetched, potentially never converging.
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    stubFetch(() => okSnapshot([{ entity: "issues", op: "upsert", row: { id: "a" } }], 77));
    const worker = new FakeWorker(
      {
        open: undefined,
        getCursor: null, // cold → take the seed path
        seedBegin: undefined,
        seedBatch: undefined,
        seedCommit: 77,
        seedAbort: undefined,
        close: undefined,
      },
      // The commit takes far longer than the idle bound below, with the stream long since finished.
      { seedCommit: 120 },
    );
    const c = collect();
    const replica = new BrowserReplica(
      c.handlers,
      opts({
        snapshotIdleTimeoutMs: 30,
        createWorker: () => worker as unknown as Worker,
      }),
    );

    // THE OUTCOME: the seed completes. Unfixed, the timer fires mid-commit and start() rejects.
    await expect(replica.start()).resolves.toBeUndefined();
    expect(c.statuses).toContain("live");
    expect(worker.received.map((e) => e.request.type)).not.toContain("seedAbort");
    replica.close();
  }, 15_000);

  it("a THROWING lock manager boots to 'error', never to 'secondary'", async () => {
    // CTC-114 review round 5. A denied / malfunctioning Web Locks API was collapsed into the same
    // `null` that means "another tab owns the replica", so start() resolved into the clean, terminal
    // "secondary" state — the consumer stayed on the fallback path for the life of the document,
    // believing a sibling tab held the lock when none did, with nothing to diagnose.
    vi.stubGlobal("navigator", {
      locks: { request: () => Promise.reject(new Error("locks denied")) },
    });
    vi.stubGlobal("WebSocket", undefined);
    let constructed = 0;
    const c = collect();
    const replica = new BrowserReplica(
      c.handlers,
      opts({
        createWorker: () => {
          constructed++;
          return new FakeWorker({}) as unknown as Worker;
        },
      }),
    );

    await expect(replica.start()).rejects.toThrow("locks denied");
    expect(c.statuses).toContain("error");
    expect(c.statuses).not.toContain("secondary");
    expect(constructed).toBe(0); // the lock is claimed before any worker/wasm cost
  });

  it("still sends seedAbort to the worker when the idle timeout fires", async () => {
    // The idle bound aborts the FETCH — but the worker has already run `seedBegin`, so it is sitting
    // in an open transaction with its SeedReadGate armed. `streamSeedIntoWorker`'s catch issues the
    // cleanup `seedAbort` that rolls it back and settles the gate; if the supersede guard rejects
    // that call, the wedge is merely relocated from the fetch to the worker — every subsequent read
    // waits forever and delta applies fail on a nested transaction until a reload.
    vi.stubGlobal("navigator", {});
    vi.stubGlobal("WebSocket", undefined);
    stubFetch((_url, init) => stalledResponse((init as RequestInit).signal));
    const worker = new FakeWorker({
      open: undefined,
      getCursor: null,
      seedBegin: undefined,
      seedAbort: undefined,
      close: undefined,
    });
    const c = collect();
    const replica = new BrowserReplica(c.handlers, opts({
      snapshotIdleTimeoutMs: 20,
      createWorker: () => worker as unknown as Worker,
    }));

    await expect(replica.start()).rejects.toThrow();

    const sent = worker.received.map((e) => e.request.type);
    expect(sent).toContain("seedBegin");
    expect(sent).toContain("seedAbort");
    // Ordering matters: the abort must come after the begin it is rolling back.
    expect(sent.lastIndexOf("seedAbort")).toBeGreaterThan(sent.indexOf("seedBegin"));
  });
});
