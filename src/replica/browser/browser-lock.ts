// replica/browser/browser-lock.ts — the single-owner gate for the browser OPFS replica (CTC-114,
// resolving CTC-118). Browser twin of the node writer-lock (src/replica/writer-lock.ts), built on the
// Web Locks API instead of a sidecar file.
//
// WHY: OPFS SAHPool is SINGLE-CONNECTION-PER-ORIGIN — it takes exclusive SyncAccessHandles on the pool
// files, so a second tab booting the same directory dies inside the worker with
// `NoModificationAllowedError` and (pre-CTC-118) surfaced as a red "Replica error" for a situation
// that is neither an error nor actionable. This gate moves the contention to the MAIN thread, BEFORE
// any worker or wasm is loaded: exactly one tab per origin acquires the lock and boots the replica;
// every other tab is told "secondary" and cleanly stays on its fetch+live path.
//
// PROPERTIES the Web Locks API gives us for free (and the node lock had to build by hand):
//   • Automatic release — a lock dies with its document (close, reload, crash). No heartbeat, no
//     staleness window, no reclaim protocol.
//   • steal-free fairness — `ifAvailable: true` never queues; a losing tab knows IMMEDIATELY.
//
// The one race that needs care: a RELOAD. The old document's lock releases during unload, so the new
// document's first probe can lose to its own predecessor. One short retry covers it. (The SAHPool
// handles themselves can also outlive the lock by a beat — the worker-side open remains the last line
// of defense; see sqlite-db.ts.)
//
// Wrapped so a runtime without Web Locks (or a non-browser test runtime) degrades to a no-op handle:
// the caller proceeds exactly as pre-CTC-114, where the worker-side collision is the only guard.

import {
  requireNonNegativeFinite,
  requireNonNegativeInt,
} from "./validate.js";

/** A held (or no-op) replica lock. `release()` is idempotent. */
export interface ReplicaLockHandle {
  /** False when Web Locks is unavailable and the gate degraded to a no-op (no exclusion happened). */
  readonly held: boolean;
  release(): void;
}

export interface ReplicaLockOptions {
  /** Extra acquisition attempts after the first (covers the reload race). Default 1. */
  retries?: number;
  /** Delay between attempts, ms. Default 1000. */
  retryDelayMs?: number;
}

const NOOP_HANDLE: ReplicaLockHandle = { held: false, release: () => {} };

/** One `ifAvailable` probe: resolve a handle when the lock is ours, else null. Never queues. */
function tryAcquire(name: string): Promise<ReplicaLockHandle | null> {
  return new Promise((resolve, reject) => {
    // The held-lock idiom: the lock lives exactly as long as the promise the callback returns. We
    // resolve the OUTER promise from inside the callback (so the caller learns the outcome), and park
    // the callback's own promise until release() settles it.
    void navigator.locks
      .request(name, { ifAvailable: true, mode: "exclusive" }, (lock) => {
        if (lock === null) {
          resolve(null); // someone else holds it — ifAvailable never queues.
          return;
        }
        return new Promise<void>((releaseLock) => {
          let released = false;
          resolve({
            held: true,
            release: () => {
              if (released) return;
              released = true;
              releaseLock();
            },
          });
        });
      })
      // A REJECTING lock manager is not contention (CTC-114 review round 5). Collapsing it to the same
      // `null` that `ifAvailable` uses for "another tab holds it" made a denied or malfunctioning Web
      // Locks API resolve start() as "secondary" — a clean, terminal, NON-error state — when in fact no
      // other tab owned the replica and the documented boot-error path was never reached. The consumer
      // silently sat on the fallback path forever with nothing to diagnose from. Propagate instead, so
      // the caller can tell "someone else has it" (null) from "the lock API is broken" (throw).
      .catch((err: unknown) =>
        reject(err instanceof Error ? err : new Error(String(err))),
      );
  });
}

/**
 * Claim sole ownership of the origin's OPFS replica. Resolves a held handle when THIS tab should boot
 * the replica; `null` when another live tab owns it (caller should surface "secondary", not an error);
 * a no-op handle (`held: false`) when the runtime has no Web Locks API at all.
 *
 * REJECTS when the Web Locks API is present but throws — denied by policy, or malfunctioning. That is
 * a boot failure, NOT contention, and the caller must not report it as "secondary": no other tab owns
 * the replica, so "secondary" would be both wrong and terminal, leaving the consumer on the fallback
 * path with nothing to diagnose. Not retried either — a denied API does not recover, and retrying only
 * delays the diagnosis (CTC-114 review round 5).
 */
export async function acquireReplicaLock(
  name: string,
  options: ReplicaLockOptions = {},
): Promise<ReplicaLockHandle | null> {
  if (typeof navigator === "undefined" || typeof navigator.locks?.request !== "function") {
    return NOOP_HANDLE;
  }
  // Validate before entering the loop (CTC-114 review round 12). This helper is exported from
  // `@catalyst-cloud/sdk/browser`, so both values are consumer-supplied. With `retries: NaN` or
  // `Infinity` the `attempt >= retries` termination test never succeeds, so ordinary contention —
  // another tab simply holding the lock — makes this probe FOREVER instead of resolving `null`, and
  // `start()` never settles. Zero retries is legitimate (probe once), hence non-negative.
  const retries = requireNonNegativeInt(
    "acquireReplicaLock",
    "retries",
    options.retries ?? 1,
  );
  const delayMs = requireNonNegativeFinite(
    "acquireReplicaLock",
    "retryDelayMs",
    options.retryDelayMs ?? 1000,
  );
  for (let attempt = 0; ; attempt++) {
    const handle = await tryAcquire(name);
    if (handle) return handle;
    if (attempt >= retries) return null;
    await new Promise((r) => setTimeout(r, delayMs));
  }
}
