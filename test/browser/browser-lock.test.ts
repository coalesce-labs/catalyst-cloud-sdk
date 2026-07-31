import { describe, it, expect, vi, afterEach } from "vitest";
import { acquireReplicaLock } from "../../src/replica/browser/browser-lock.js";

// CTC-114 / CTC-118 — the Web Locks single-owner gate. A second tab must learn "secondary"
// IMMEDIATELY (ifAvailable never queues), a release must hand the lock to the next acquirer, and a
// runtime with no Web Locks at all must degrade to a no-op handle rather than block the replica.

/** A minimal in-process fake of the Web Locks manager: exclusive, ifAvailable-only semantics. */
function fakeLockManager() {
  const held = new Map<string, () => void>(); // name -> forced-release hook
  return {
    request: (
      name: string,
      _opts: LockOptions,
      cb: (lock: Lock | null) => Promise<void> | void,
    ): Promise<void> => {
      if (held.has(name)) return Promise.resolve(cb(null)).then(() => undefined);
      const lockDone = Promise.resolve(cb({ name, mode: "exclusive" } as Lock));
      held.set(name, () => undefined);
      return lockDone.finally(() => held.delete(name));
    },
    isHeld: (name: string) => held.has(name),
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("acquireReplicaLock (CTC-118)", () => {
  it("first tab acquires; second tab gets null without queueing", async () => {
    const mgr = fakeLockManager();
    vi.stubGlobal("navigator", { locks: mgr });

    const first = await acquireReplicaLock("catalyst-replica:test", { retries: 0 });
    expect(first?.held).toBe(true);
    expect(mgr.isHeld("catalyst-replica:test")).toBe(true);

    const second = await acquireReplicaLock("catalyst-replica:test", { retries: 0 });
    expect(second).toBeNull();
  });

  it("release hands the lock to the next acquirer, and is idempotent", async () => {
    const mgr = fakeLockManager();
    vi.stubGlobal("navigator", { locks: mgr });

    const first = await acquireReplicaLock("catalyst-replica:test", { retries: 0 });
    first!.release();
    first!.release(); // idempotent
    // the held-lock promise settles on a microtask — let it drain.
    await new Promise((r) => setTimeout(r, 0));
    expect(mgr.isHeld("catalyst-replica:test")).toBe(false);

    const second = await acquireReplicaLock("catalyst-replica:test", { retries: 0 });
    expect(second?.held).toBe(true);
  });

  it("retries cover the reload race — a lock released mid-wait is acquired on the second attempt", async () => {
    const mgr = fakeLockManager();
    vi.stubGlobal("navigator", { locks: mgr });

    const first = await acquireReplicaLock("catalyst-replica:test", { retries: 0 });
    // Simulate the predecessor document releasing during the newcomer's retry delay.
    setTimeout(() => first!.release(), 5);
    const second = await acquireReplicaLock("catalyst-replica:test", {
      retries: 1,
      retryDelayMs: 25,
    });
    expect(second?.held).toBe(true);
  });

  it("degrades to a NO-OP handle (held:false) when the runtime has no Web Locks API", async () => {
    vi.stubGlobal("navigator", {}); // navigator exists, locks does not
    const handle = await acquireReplicaLock("catalyst-replica:test");
    expect(handle).not.toBeNull();
    expect(handle?.held).toBe(false);
    handle?.release(); // must not throw
  });

  it("a throwing lock manager reads as not-acquired, never as a crash", async () => {
    vi.stubGlobal("navigator", {
      locks: { request: () => Promise.reject(new Error("boom")) },
    });
    const handle = await acquireReplicaLock("catalyst-replica:test", { retries: 0 });
    expect(handle).toBeNull();
  });
});
