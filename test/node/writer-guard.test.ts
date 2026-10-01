import { afterEach, describe, expect, it, vi } from "vitest";
import * as fs from "node:fs";
import {
  CatalystReplica,
  claimWriterLock,
  nodeSqliteEngine,
  type WebSocketLike,
  type WebSocketFactory,
} from "../../src/node";

// SINGLE-WRITER GUARD (CTC-113). Two writers on one replica file would race the cursor + reseed and
// silently diverge, so a writer's start() best-effort claims a sidecar `<dbPath>.writer.lock`. These
// tests cover the guard at the function level (claim/override/stale/release/disabled) and end-to-end
// (a second concurrent CatalystReplica writer rejects; a fresh writer can claim after close). Advisory
// by design — see src/replica/writer-lock.ts.

const BASE = "https://api.example.test";

// `process` isn't typed here (tsconfig `types: []`, no @types/node) — reach it via globalThis, the
// same way the SDK source does. PID = this process; PPID = the parent, which is alive for the whole
// test run and always distinct from PID (so it stands in for a different, live writer).
const { pid: PID, ppid: PPID } = (
  globalThis as unknown as { process: { pid: number; ppid: number } }
).process;

const dirs: string[] = [];
function tmpDbPath(): string {
  const dir = fs.mkdtempSync("catalyst-replica-guard-");
  dirs.push(dir);
  return `${dir}/replica.db`;
}

// ── A scriptable WebSocket + buffered snapshot, mirroring catalyst-replica.test.ts, so an integration
//    writer can reach 'live' deterministically and offline. ──
class FakeWebSocket implements WebSocketLike {
  closed = false;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  send(): void {}
  close(): void {
    this.closed = true;
  }
  fireOpen(): void {
    this.onopen?.({});
  }
}
function recordingFactory(): { sockets: FakeWebSocket[]; factory: WebSocketFactory } {
  const sockets: FakeWebSocket[] = [];
  const factory: WebSocketFactory = () => {
    const ws = new FakeWebSocket();
    sockets.push(ws);
    return ws;
  };
  return { sockets, factory };
}
function emptySnapshotFetch(): typeof fetch {
  return (async () =>
    ({ ok: true, status: 200, text: async () => `${JSON.stringify({ cursor: 0 })}\n` }) as unknown as Response) as unknown as typeof fetch;
}

const replicas: CatalystReplica[] = [];
function newWriter(
  dbPath: string,
  extra?: Partial<ConstructorParameters<typeof CatalystReplica>[0]>,
): { replica: CatalystReplica; sockets: FakeWebSocket[] } {
  const { sockets, factory } = recordingFactory();
  const replica = new CatalystReplica({
    baseUrl: BASE,
    account: "tenant-0",
    auth: { kind: "cookie" },
    dbPath,
    engine: nodeSqliteEngine,
    fetchImpl: emptySnapshotFetch(),
    wsFactory: factory,
    ...extra,
  });
  replicas.push(replica);
  return { replica, sockets };
}
async function startToLive(replica: CatalystReplica, sockets: FakeWebSocket[]): Promise<void> {
  const started = replica.start();
  await vi.waitFor(() => expect(sockets.length).toBeGreaterThan(0));
  sockets[0]!.fireOpen();
  await started;
}

afterEach(async () => {
  while (replicas.length) {
    try {
      await replicas.pop()!.close();
    } catch {
      /* ignore */
    }
  }
  while (dirs.length) {
    try {
      fs.rmSync(dirs.pop()!, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  }
  vi.useRealTimers();
});

describe("claimWriterLock — the guard mechanism", () => {
  it("a second claim on the same path throws a clear 'another writer owns' error", () => {
    const dbPath = tmpDbPath();
    const lock = claimWriterLock(dbPath, {});
    expect(lock).not.toBeNull();
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true);
    try {
      expect(() => claimWriterLock(dbPath, {})).toThrow(/another writer owns this replica/);
    } finally {
      lock!.release();
    }
  });

  it("override:true steals a live lock instead of throwing", () => {
    const dbPath = tmpDbPath();
    const first = claimWriterLock(dbPath, {});
    let second: ReturnType<typeof claimWriterLock> = null;
    try {
      second = claimWriterLock(dbPath, { override: true });
      expect(second).not.toBeNull();
    } finally {
      second?.release();
      first?.release();
    }
  });

  it("release() frees the path for a later writer", () => {
    const dbPath = tmpDbPath();
    claimWriterLock(dbPath, {})!.release();
    const second = claimWriterLock(dbPath, {}); // no longer held → succeeds
    expect(second).not.toBeNull();
    second!.release();
  });

  it("reclaims a STALE lock (heartbeat older than staleMs)", () => {
    const dbPath = tmpDbPath();
    const lockPath = `${dbPath}.writer.lock`;
    // A dead writer's leftover lock: heartbeat long in the past.
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 999999, owner: "dead", heartbeat: Date.now() - 60_000 }));
    const lock = claimWriterLock(dbPath, { staleMs: 15_000 }); // stale → reclaimed, no throw
    expect(lock).not.toBeNull();
    lock!.release();
  });

  it("disabled:true and ':memory:' are no-ops (return null, claim nothing)", () => {
    const dbPath = tmpDbPath();
    expect(claimWriterLock(dbPath, { disabled: true })).toBeNull();
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(false);
    expect(claimWriterLock(":memory:", {})).toBeNull();
  });
});

describe("claimWriterLock — ownerKey fast-reclaim (kill -9 + fast relaunch)", () => {
  // A live lock the normal gate WOULD treat as held: an alive pid (the parent process is alive for the
  // duration of the test, and is always distinct from our own pid) + a fresh heartbeat.
  function writeLiveLock(lockPath: string, ownerKey?: string): void {
    fs.writeFileSync(
      lockPath,
      JSON.stringify({ pid: PPID, owner: "predecessor", heartbeat: Date.now(), ownerKey }),
    );
  }

  it("a relaunch with the SAME ownerKey but a different pid reclaims IMMEDIATELY (no throw)", () => {
    const dbPath = tmpDbPath();
    const lockPath = `${dbPath}.writer.lock`;
    writeLiveLock(lockPath, "host-a-tenant-0"); // our own crashed predecessor (alive-looking lock)

    const lock = claimWriterLock(dbPath, { ownerKey: "host-a-tenant-0" });
    expect(lock).not.toBeNull();
    // Reclaimed: the lock file is now ours (our pid, our ownerKey), bypassing the staleMs/pid gate.
    const rec = JSON.parse(fs.readFileSync(lockPath, "utf8")) as { pid: number; ownerKey?: string };
    expect(rec.pid).toBe(PID);
    expect(rec.ownerKey).toBe("host-a-tenant-0");
    lock!.release();
  });

  it("WITHOUT ownerKey, the SAME live lock still throws (the bug it fixes is real)", () => {
    const dbPath = tmpDbPath();
    writeLiveLock(`${dbPath}.writer.lock`); // no ownerKey → falls through to the normal held gate
    expect(() => claimWriterLock(dbPath, {})).toThrow(/another writer owns this replica/);
  });

  it("a DIFFERENT ownerKey does NOT fast-reclaim — a live lock still throws", () => {
    const dbPath = tmpDbPath();
    writeLiveLock(`${dbPath}.writer.lock`, "host-OTHER-tenant-9");
    expect(() => claimWriterLock(dbPath, { ownerKey: "host-a-tenant-0" })).toThrow(
      /another writer owns this replica/,
    );
  });
});

describe("CatalystReplica writer guard — end to end", () => {
  it("holds the writer guard through a pending engine open and cancels before migration", async () => {
    const dbPath = tmpDbPath();
    let releaseOpen!: () => void;
    const openGate = new Promise<void>((resolve) => {
      releaseOpen = resolve;
    });
    let engineClosed = false;
    let migrationWrites = 0;
    const replica = new CatalystReplica({
      baseUrl: BASE,
      account: "tenant-0",
      auth: { kind: "cookie" },
      dbPath,
      engine: async (path) => {
        await openGate;
        const engine = await nodeSqliteEngine(path);
        const instrumented: typeof engine = {
          ...engine,
          exec: (sql) => { migrationWrites += 1; engine.exec(sql); },
          run: (sql, ...bindings) => { migrationWrites += 1; return engine.run(sql, ...bindings); },
          close: () => { engineClosed = true; engine.close(); },
        };
        return instrumented;
      },
      fetchImpl: emptySnapshotFetch(),
    });
    replicas.push(replica);

    const start = replica.start();
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true);
    const shutdown = replica.closeAndWait();
    await Promise.resolve();
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true);

    releaseOpen();
    await expect(start).rejects.toThrow(/cancelled by close/);
    await shutdown;
    expect(engineClosed).toBe(true);
    expect(migrationWrites).toBe(0);
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(false);
  });

  it("keeps the writer guard until the cancelled snapshot reader acknowledges cleanup", async () => {
    const dbPath = tmpDbPath();
    let releaseCancel!: () => void;
    let cancelSeen = false;
    let readPending = false;
    let engineClosed = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('{"cursor":0}\n'));
      },
      pull() {
        readPending = true;
        return new Promise<void>(() => {}); // hold the second read until close() cancels the reader
      },
      cancel() {
        cancelSeen = true;
        return new Promise<void>((resolve) => { releaseCancel = resolve; });
      },
    });
    const replica = new CatalystReplica({
      baseUrl: BASE,
      account: "tenant-0",
      auth: { kind: "cookie" },
      dbPath,
      engine: async (path) => {
        const engine = await nodeSqliteEngine(path);
        return { ...engine, close: () => { engineClosed = true; engine.close(); } };
      },
      fetchImpl: (async () => new Response(body, { status: 200 })) as typeof fetch,
    });
    replicas.push(replica);

    const start = replica.start();
    await vi.waitFor(() => expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true));
    await vi.waitFor(() => expect(readPending).toBe(true));
    const shutdown = replica.closeAndWait();
    await vi.waitFor(() => expect(cancelSeen).toBe(true));
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true);
    expect(engineClosed).toBe(false);

    releaseCancel();
    await expect(start).rejects.toThrow();
    await shutdown;
    expect(engineClosed).toBe(true);
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(false);
  });

  it("reports a real reader-cancel AbortError and keeps ownership after closing the engine", async () => {
    const dbPath = tmpDbPath();
    const failedCancel = new Error("reader cleanup aborted independently");
    failedCancel.name = "AbortError";
    let readPending = false;
    let cancelSeen = false;
    let engineClosed = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('{"cursor":0}\n'));
      },
      pull() {
        readPending = true;
        return new Promise<void>(() => {});
      },
      cancel() {
        cancelSeen = true;
        return Promise.reject(failedCancel);
      },
    });
    const fetchImpl: typeof fetch = async () => new Response(body, { status: 200 });
    const replica = new CatalystReplica({
      baseUrl: BASE,
      account: "tenant-0",
      auth: { kind: "cookie" },
      dbPath,
      engine: async (path) => {
        const engine = await nodeSqliteEngine(path);
        return { ...engine, close: () => { engineClosed = true; engine.close(); } };
      },
      fetchImpl,
    });
    replicas.push(replica);
    const starting = replica.start();
    const startFailure = starting.catch((error: unknown) => error);
    await vi.waitFor(() => expect(readPending).toBe(true));
    const failure: unknown = await replica.closeAndWait().catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(AggregateError);
    if (!(failure instanceof AggregateError)) throw new Error("shutdown did not preserve failed cancel");
    const seedFailure: unknown = failure.errors[0];
    expect(seedFailure).toBeInstanceOf(AggregateError);
    if (!(seedFailure instanceof AggregateError)) throw new Error("missing transport cleanup failure");
    expect(seedFailure.errors).toEqual([failedCancel]);
    expect(cancelSeen).toBe(true);
    expect(engineClosed).toBe(true);
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true);
    expect(() => claimWriterLock(dbPath, {})).toThrow(/another writer owns this replica/);
    expect(await startFailure).toBeInstanceOf(Error);
  });

  it("surfaces engine-close failure and keeps the writer guard held", async () => {
    const dbPath = tmpDbPath();
    const { sockets, factory } = recordingFactory();
    const replica = new CatalystReplica({
      baseUrl: BASE,
      account: "tenant-0",
      auth: { kind: "cookie" },
      dbPath,
      engine: async (path) => {
        const engine = await nodeSqliteEngine(path);
        return { ...engine, close: () => { throw new Error("close sentinel"); } };
      },
      fetchImpl: emptySnapshotFetch(),
      wsFactory: factory,
    });
    replicas.push(replica);
    await startToLive(replica, sockets);

    await expect(replica.closeAndWait()).rejects.toThrow(/shutdown failed/);
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true);
  });

  it("rejects a second concurrent writer on the same file", async () => {
    const dbPath = tmpDbPath();
    const w1 = newWriter(dbPath);
    await startToLive(w1.replica, w1.sockets);

    const w2 = newWriter(dbPath);
    // The guard fires at the top of start(), before any socket is opened.
    await expect(w2.replica.start()).rejects.toThrow(/another writer owns this replica/);
    expect(w2.sockets.length).toBe(0);
  });

  it("a fresh writer can claim the file after the first writer closes (releases the lock)", async () => {
    const dbPath = tmpDbPath();
    const w1 = newWriter(dbPath);
    await startToLive(w1.replica, w1.sockets);
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true);
    await w1.replica.close();
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(false); // released on close

    const w2 = newWriter(dbPath);
    await startToLive(w2.replica, w2.sockets); // claims the now-free file
    expect(w2.replica.status).toBe("live");
  });

  it("writerGuard.disabled lets a second writer start without the guard throwing", async () => {
    const dbPath = tmpDbPath();
    const w1 = newWriter(dbPath);
    await startToLive(w1.replica, w1.sockets);

    const w2 = newWriter(dbPath, { writerGuard: { disabled: true } });
    await startToLive(w2.replica, w2.sockets); // guard skipped → no throw
    expect(w2.replica.status).toBe("live");
  });
});

describe("CatalystReplica startTimeoutMs — fail-fast on a wedged start", () => {
  it("consumes timeout cleanup rejection and retains its actual cause", async () => {
    const dbPath = tmpDbPath();
    const closeFailure = new Error("timeout engine close failed");
    const opened: { close: (() => void) | null } = { close: null };
    const { sockets, factory } = recordingFactory();
    const replica = new CatalystReplica({
      baseUrl: BASE,
      account: "tenant-0",
      auth: { kind: "cookie" },
      dbPath,
      startTimeoutMs: 100,
      engine: async (path) => {
        const engine = await nodeSqliteEngine(path);
        opened.close = () => engine.close();
        return { ...engine, close: () => { throw closeFailure; } };
      },
      fetchImpl: emptySnapshotFetch(),
      wsFactory: factory,
    });
    replicas.push(replica);
    try {
      // Never open the socket. The real deadline initiates the rejecting close path.
      const failure: unknown = await replica.start().catch((error: unknown) => error);
      expect(failure).toBeInstanceOf(Error);
      if (!(failure instanceof Error)) throw new Error("start did not report its timeout");
      expect(failure.message).toMatch(/did not reach 'live' within 100ms/);
      expect(failure.cause).toBeInstanceOf(AggregateError);
      if (!(failure.cause instanceof AggregateError)) throw new Error("timeout lost cleanup cause");
      expect(failure.cause.errors).toContain(closeFailure);
      expect(sockets).toHaveLength(1);
      expect(sockets[0]?.closed).toBe(true);
      expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true);
      // Let the runtime emit any ignored derived rejection; Vitest treats it as an unhandled error.
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    } finally {
      // The intentionally failing engine port cannot clean its native handle; the test owns it.
      opened.close?.();
    }
  });

  it("rejects within the timeout when start() never reaches 'live', then leaves no held lock / open socket", async () => {
    const dbPath = tmpDbPath();
    const w = newWriter(dbPath, { startTimeoutMs: 100 });
    // Seed completes (empty snapshot) and the socket is constructed, but we NEVER fire it open, so the
    // status is stuck at 'connecting' and never reaches 'live'.
    await expect(w.replica.start()).rejects.toThrow(/did not reach 'live' within 100ms/);

    // The timeout tore down the SAME way close() does: the writer-lock sidecar is released and the
    // socket was closed — no held lock, no dangling open handle.
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(false);
    expect(w.sockets[0]?.closed ?? true).toBe(true);
  });

  it("a normal fast start with a generous startTimeoutMs still resolves to 'live' (timer cancelled)", async () => {
    const dbPath = tmpDbPath();
    const w = newWriter(dbPath, { startTimeoutMs: 30_000 });
    await startToLive(w.replica, w.sockets); // reaches 'live' → the deadline timer is cleared
    expect(w.replica.status).toBe("live");
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(true); // still the live writer
  });
});
