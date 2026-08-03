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

  it("PROPAGATES a throwing lock manager rather than reporting it as contention", async () => {
    // INVERTED in CTC-114 review round 5. This used to assert `null` — the very same value
    // `ifAvailable` returns for "another live tab holds the lock". Collapsing the two meant a Web
    // Locks API that was denied by policy or simply malfunctioning resolved BrowserReplica.start()
    // into "secondary": a clean, terminal, non-error state claiming another tab owned the replica when
    // none did. The consumer then sat on the fallback path for the life of the document with nothing
    // to diagnose from. A throw is a boot failure and must reach the documented boot-error path.
    vi.stubGlobal("navigator", {
      locks: { request: () => Promise.reject(new Error("boom")) },
    });
    await expect(
      acquireReplicaLock("catalyst-replica:test", { retries: 0 }),
    ).rejects.toThrow("boom");
  });

  it("REJECTS a non-finite retries count instead of probing forever", async () => {
    // CTC-114 review round 12. Exported from `@catalyst-cloud/sdk/browser`, so `retries` is
    // consumer-supplied. With `NaN`/`Infinity` the `attempt >= retries` termination test never
    // succeeds, so ORDINARY contention — another tab simply holding the lock — probes forever instead
    // of resolving null, and start() never settles. Same class as the snapshot batchSize and the
    // queue's maxDepth; all three now share one validator.
    const mgr = fakeLockManager();
    vi.stubGlobal("navigator", { locks: mgr });
    // Put the lock under genuine contention — the condition that would otherwise loop forever.
    const owner = await acquireReplicaLock("catalyst-replica:test", { retries: 0 });
    expect(owner?.held).toBe(true);

    for (const bad of [NaN, Infinity, -1, 1.5]) {
      await expect(
        acquireReplicaLock("catalyst-replica:test", { retries: bad }),
      ).rejects.toThrow(/retries must be a non-negative integer/);
    }
    // Negative control: 0 retries is legitimate (probe once) and still resolves null under contention
    // rather than throwing — so the guard rejects only the values that break termination.
    await expect(
      acquireReplicaLock("catalyst-replica:test", { retries: 0 }),
    ).resolves.toBeNull();
  });
});
