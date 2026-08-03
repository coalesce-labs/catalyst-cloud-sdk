import { describe, it, expect, vi } from "vitest";
import {
  DeltaQueue,
  MAX_APPLY_BATCH_ROWS,
  MAX_INBOX_DEPTH,
  type SeqChange,
} from "../../src/replica/browser/delta-queue.js";

// CTC-318 — the replica used to answer every live ChangeFrame on its own: one structured clone, one
// worker RPC, one OPFS transaction and one full view rebuild PER ROW, with no backpressure between a
// socket delivering at network speed and a worker draining at one fsync per row. A stale-cursor
// reconnect replays up to CHANGE_LOG_MAX_ROWS = 200,000 frames through that path.
//
// These pin the two properties that fix it: frames arriving while an apply is in flight are COALESCED
// into the next batch, and the caller is notified ONCE per drain rather than once per frame.

function change(seq: number): SeqChange {
  return {
    seq,
    entity: "issues",
    op: "upsert",
    row: { id: `i${seq}` },
    entityId: `i${seq}`,
  };
}

/** A controllable apply: each call parks until the test releases it. */
function deferredApply() {
  const releases: (() => void)[] = [];
  const batches: SeqChange[][] = [];
  const apply = (changes: SeqChange[]): Promise<{ cursor: number }> => {
    batches.push(changes);
    return new Promise((resolve) => {
      releases.push(() =>
        resolve({ cursor: changes[changes.length - 1]?.seq ?? 0 }),
      );
    });
  };
  return { apply, batches, releases };
}

describe("DeltaQueue — coalescing (CTC-318)", () => {
  it("coalesces every frame that arrives while an apply is in flight into ONE next batch", async () => {
    const { apply, batches, releases } = deferredApply();
    const onDrained = vi.fn();
    const q = new DeltaQueue({ apply, onDrained, onError: vi.fn() });

    // First push starts an apply immediately — an idle tab pays no added latency.
    q.push(change(1));
    await Promise.resolve();
    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(1);

    // 99 more arrive while that apply is parked. NONE of them start their own apply.
    for (let i = 2; i <= 100; i++) q.push(change(i));
    expect(batches).toHaveLength(1);
    expect(q.depth).toBe(99);

    // Releasing the first apply lets the loop take all 99 in a single batch.
    releases[0]!();
    await vi.waitFor(() => expect(batches).toHaveLength(2));
    expect(batches[1]).toHaveLength(99);

    releases[1]!();
    await vi.waitFor(() => expect(onDrained).toHaveBeenCalledTimes(1));
    // 100 frames → 2 worker RPCs → exactly ONE view rebuild.
    expect(onDrained).toHaveBeenCalledWith(100);
  });

  it("caps a batch at maxBatch so one clone cannot carry a whole replay", async () => {
    const { apply, batches, releases } = deferredApply();
    const q = new DeltaQueue({
      apply,
      onDrained: vi.fn(),
      onError: vi.fn(),
      maxBatch: 10,
    });

    // The first push applies immediately with whatever is queued at that instant — just itself. That
    // is deliberate: an idle tab must not wait for a batch to fill.
    for (let i = 1; i <= 35; i++) q.push(change(i));
    await Promise.resolve();
    expect(batches[0]).toHaveLength(1);
    expect(q.depth).toBe(34);

    // Every subsequent batch is capped, so no single postMessage carries the whole backlog.
    releases[0]!();
    await vi.waitFor(() => expect(batches).toHaveLength(2));
    expect(batches[1]).toHaveLength(10);

    releases[1]!();
    await vi.waitFor(() => expect(batches).toHaveLength(3));
    expect(batches[2]).toHaveLength(10);

    releases[2]!();
    await vi.waitFor(() => expect(batches).toHaveLength(4));
    expect(batches[3]).toHaveLength(10);

    releases[3]!();
    await vi.waitFor(() => expect(batches).toHaveLength(5));
    expect(batches[4]).toHaveLength(4); // the remainder
    expect(batches.flat()).toHaveLength(35); // nothing dropped
  });

  it("notifies ONCE per drain, not once per batch and never once per frame", async () => {
    const onDrained = vi.fn();
    // A resolved-immediately apply, so the whole queue drains in one loop.
    const q = new DeltaQueue({
      apply: (c) => Promise.resolve({ cursor: c[c.length - 1]!.seq }),
      onDrained,
      onError: vi.fn(),
      maxBatch: 10,
    });

    for (let i = 1; i <= 100; i++) q.push(change(i));
    await vi.waitFor(() => expect(q.depth).toBe(0));
    await vi.waitFor(() => expect(onDrained).toHaveBeenCalled());
    expect(onDrained).toHaveBeenCalledTimes(1);
    expect(onDrained).toHaveBeenCalledWith(100);
  });

  it("reports the highest seq seen even when the worker cursor lags it", async () => {
    const onDrained = vi.fn();
    // The worker advances its cursor to the max seq SEEN; pin that we never regress below a frame seq.
    const q = new DeltaQueue({
      apply: () => Promise.resolve({ cursor: 0 }),
      onDrained,
      onError: vi.fn(),
    });
    q.push(change(42));
    await vi.waitFor(() => expect(onDrained).toHaveBeenCalledWith(42));
  });

  it("surfaces an apply failure and REQUEUES the failed batch, losing nothing", async () => {
    // Driven on FAKE timers since CTC-114 review round 5. The second half used to rely on
    // `q.push(change(3))` re-entering the drain — "the next arrival re-enters the drain rather than
    // wedging forever" — which is exactly the behaviour the review flagged: push() bypassed the armed
    // backoff entirely, so on a busy feed a handful of frames could burn the whole maxApplyRetries
    // budget in milliseconds and escalate to a full /snapshot for a transient error the delay would
    // have ridden out. The OUTCOME this test exists for — nothing is lost, and 1/2/3 apply IN ORDER —
    // is unchanged and still asserted; only the mechanism that resumes the drain moves, from "any
    // incoming frame" to "the scheduled retry".
    vi.useFakeTimers();
    try {
      const onError = vi.fn();
      const onDrained = vi.fn();
      const applied: number[] = [];
      let calls = 0;
      const q = new DeltaQueue({
        apply: (c) => {
          calls++;
          if (calls === 1) return Promise.reject(new Error("boom"));
          for (const ch of c) applied.push(ch.seq);
          return Promise.resolve({ cursor: c[c.length - 1]!.seq });
        },
        onDrained,
        onError,
        maxBatch: 1,
        retryDelayMs: 1000,
      });

      q.push(change(1));
      q.push(change(2));
      await vi.advanceTimersByTimeAsync(0); // let the failing apply settle
      expect(onError).toHaveBeenCalledTimes(1);
      // BOTH frames are buffered: the failed batch went back to the front, and #2 is still behind it.
      // This previously read `1` — the failed batch had been spliced out and dropped, and the
      // assertion was pinning that loss while its comment claimed the opposite.
      expect(q.depth).toBe(2);

      // A new arrival must NOT bypass the armed backoff — it buffers and waits its turn.
      q.push(change(3));
      await vi.advanceTimersByTimeAsync(0);
      expect(q.depth).toBe(3);
      expect(calls).toBe(1); // no second apply attempt yet: the delay is still running

      // The scheduled RETRY is what resumes the drain.
      await vi.advanceTimersByTimeAsync(1000);
      expect(q.depth).toBe(0);
      expect(onDrained).toHaveBeenCalled();
      // The whole point: seq 1 is APPLIED, in order, not skipped. Depth alone cannot see this.
      expect(applied).toEqual([1, 2, 3]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("stop() drops the buffer and refuses further work", async () => {
    const onDrained = vi.fn();
    const { apply, releases } = deferredApply();
    const q = new DeltaQueue({ apply, onDrained, onError: vi.fn() });

    q.push(change(1));
    await Promise.resolve();
    for (let i = 2; i <= 50; i++) q.push(change(i));
    expect(q.depth).toBe(49);

    q.stop();
    expect(q.depth).toBe(0);

    releases[0]!();
    await Promise.resolve();
    q.push(change(99));
    expect(q.depth).toBe(0);
    expect(onDrained).not.toHaveBeenCalled();
  });

  it("defaults to a batch cap that keeps one clone bounded", () => {
    expect(MAX_APPLY_BATCH_ROWS).toBe(500);
  });
});

// Batching alone does NOT bound retention — it collapses the per-frame copies and drains far faster,
// but the buffer is still O(frames received) and each entry retains a fully parsed row. Past a certain
// depth a snapshot is cheaper than the replay, so the queue escalates rather than growing.
describe("DeltaQueue — bounded retention (CTC-318)", () => {
  it("escalates to the owner ONCE and drops the buffer when the backlog passes maxDepth", async () => {
    const { apply, releases } = deferredApply();
    const onOverflow = vi.fn();
    const q = new DeltaQueue({
      apply,
      onDrained: vi.fn(),
      onError: vi.fn(),
      onOverflow,
      maxDepth: 10,
    });

    // Park the first apply so nothing drains while we pile frames in.
    q.push(change(1));
    await Promise.resolve();
    for (let i = 2; i <= 40; i++) q.push(change(i));

    expect(onOverflow).toHaveBeenCalledTimes(1);
    expect(onOverflow.mock.calls[0]?.[0]).toBe(11); // the depth that tripped it
    // The buffer is released rather than retained — that IS the fix.
    expect(q.depth).toBe(0);

    releases[0]!();
    await Promise.resolve();
    // Latched: the remaining 29 pushes did not each raise their own escalation.
    expect(onOverflow).toHaveBeenCalledTimes(1);
  });

  it("refuses further deltas until resume() — so a re-seed is not racing the feed", async () => {
    const onOverflow = vi.fn();
    const q = new DeltaQueue({
      apply: (c) => Promise.resolve({ cursor: c[c.length - 1]!.seq }),
      onDrained: vi.fn(),
      onError: vi.fn(),
      onOverflow,
      maxDepth: 2,
    });

    for (let i = 1; i <= 10; i++) q.push(change(i));
    await vi.waitFor(() => expect(onOverflow).toHaveBeenCalled());

    q.push(change(99));
    expect(q.depth).toBe(0);

    // After the owner's re-seed lands it re-opens, and normal service resumes.
    q.resume();
    q.push(change(100));
    await vi.waitFor(() => expect(q.depth).toBe(0));
  });

  it("stop() wins over resume() — a torn-down queue never accepts again", () => {
    const q = new DeltaQueue({
      apply: () => Promise.resolve({ cursor: 0 }),
      onDrained: vi.fn(),
      onError: vi.fn(),
    });
    q.stop();
    q.resume();
    q.push(change(1));
    expect(q.depth).toBe(0);
  });

  it("defaults to a depth cap well under the change_log ring", () => {
    // CHANGE_LOG_MAX_ROWS is 200,000; at ~19.5 KB of parsed row per issues frame an unbounded buffer
    // would be multi-GB. 20k caps it around 390 MB worst case.
    expect(MAX_INBOX_DEPTH).toBe(20_000);
  });

  it("escalates to a re-seed when apply keeps rejecting — it does not retry forever", async () => {
    // `applyChanges` can reject DETERMINISTICALLY (schema drift, a row this schema cannot accept).
    // An unbounded requeue-and-retry on that is a hot loop that never converges, so the queue gives up
    // after MAX_APPLY_RETRIES and asks the owner to re-seed — the same contract as a depth overflow.
    const onError = vi.fn();
    const onOverflow = vi.fn();
    const q = new DeltaQueue({
      apply: () => Promise.reject(new Error("schema drift")),
      onDrained: vi.fn(),
      onError,
      onOverflow,
      maxBatch: 1,
      retryDelayMs: 1,
    });

    q.push(change(1));

    await vi.waitFor(() => expect(onOverflow).toHaveBeenCalledTimes(1));
    expect(onOverflow.mock.calls[0]?.[1]).toBe("apply-failed");
    expect(onError).toHaveBeenCalledTimes(3);
    // Latched: the queue refuses further work until the owner's re-seed calls resume().
    expect(q.depth).toBe(0);
    q.push(change(2));
    expect(q.depth).toBe(0);

    q.resume();
    expect(q.depth).toBe(0);
  });

  it("a depth overflow reports its reason too", async () => {
    const onOverflow = vi.fn();
    const { apply } = deferredApply();
    const q = new DeltaQueue({
      apply,
      onDrained: vi.fn(),
      onError: vi.fn(),
      onOverflow,
      maxDepth: 3,
    });

    for (let i = 1; i <= 5; i++) q.push(change(i));

    expect(onOverflow).toHaveBeenCalledTimes(1);
    expect(onOverflow.mock.calls[0]?.[1]).toBe("depth");
  });
  it("rejects a non-positive maxBatch at construction (public option, unbounded spin)", async () => {
    // `splice(0, 0)` removes nothing, so `while (inbox.length > 0)` never advances: awaiting
    // `apply([])` and iterating forever, an unbounded stream of empty worker transactions that never
    // applies the queued change. Both types are public exports of `./browser`, so this is reachable.
    const base = { apply: vi.fn(), onDrained: vi.fn(), onError: vi.fn() };
    expect(() => new DeltaQueue({ ...base, maxBatch: 0 })).toThrow(
      /maxBatch must be a positive integer/,
    );
    expect(() => new DeltaQueue({ ...base, maxBatch: -1 })).toThrow(
      /maxBatch must be a positive integer/,
    );
    expect(() => new DeltaQueue({ ...base, maxBatch: 1.5 })).toThrow(
      /maxBatch must be a positive integer/,
    );
    // Negative control: a legal batch size still constructs, and the default is untouched.
    expect(() => new DeltaQueue({ ...base, maxBatch: 1 })).not.toThrow();
    expect(() => new DeltaQueue({ ...base })).not.toThrow();
  });
  it("drops an in-flight batch whose apply rejects AFTER a depth overflow latched", async () => {
    // The exact interleaving (CTC-114 review): apply(batch) is in flight when push() crosses maxDepth.
    // push() clears the inbox, latches `overflowed`, and the owner starts a replacement snapshot. THEN
    // the in-flight apply rejects. Requeueing that batch resurrects pre-snapshot work — and because
    // scheduleRetry() is inert while overflowed, it simply waits in the inbox until the next live push
    // drains it ON TOP of the fresh DB, where an old delete removes a row the snapshot legitimately has.
    const onOverflow = vi.fn();
    const onError = vi.fn();
    const applied: SeqChange[][] = [];
    let rejectFirst: ((e: Error) => void) | null = null;
    let call = 0;
    const apply = (changes: SeqChange[]): Promise<{ cursor: number }> => {
      call += 1;
      applied.push(changes);
      if (call === 1) {
        return new Promise((_resolve, reject) => {
          rejectFirst = reject;
        });
      }
      return Promise.resolve({ cursor: changes[changes.length - 1]?.seq ?? 0 });
    };

    const q = new DeltaQueue({
      apply,
      onDrained: vi.fn(),
      onError,
      onOverflow,
      maxBatch: 1,
      maxDepth: 3,
    });

    q.push(change(1)); // starts the (parked) apply of [1]
    await Promise.resolve();
    expect(applied[0]?.map((c) => c.seq)).toEqual([1]);

    // Overflow WHILE that apply is still in flight.
    for (let i = 2; i <= 6; i++) q.push(change(i));
    expect(onOverflow).toHaveBeenCalledTimes(1);
    expect(onOverflow.mock.calls[0]?.[1]).toBe("depth");

    // Now the in-flight apply fails.
    rejectFirst!(new Error("SQLITE_IOERR"));
    await new Promise((r) => setTimeout(r, 0));

    // It must NOT be sitting in the inbox waiting to be replayed over the snapshot.
    expect(q.depth).toBe(0);
    expect(onError).toHaveBeenCalledTimes(1);
    // And it must not have re-escalated — the owner is already re-seeding.
    expect(onOverflow).toHaveBeenCalledTimes(1);

    // After the owner's reseed lands, only POST-reseed frames may reach the worker.
    q.resume();
    q.push(change(7));
    await new Promise((r) => setTimeout(r, 0));
    const seqsAfter = applied.slice(1).flat().map((c) => c.seq);
    expect(seqsAfter).toEqual([7]);
    expect(seqsAfter).not.toContain(1);
  });

  it("resume() discards anything still buffered from before the reseed", async () => {
    // Belt-and-braces on the same invariant, asserted at the resume() boundary rather than the catch.
    const { apply, batches, releases } = deferredApply();
    const q = new DeltaQueue({
      apply,
      onDrained: vi.fn(),
      onError: vi.fn(),
      onOverflow: vi.fn(),
      maxBatch: 1,
      maxDepth: 100,
    });
    q.push(change(1));
    await Promise.resolve();
    q.push(change(2)); // queued behind the in-flight apply of [1]
    expect(q.depth).toBe(1);

    q.resume(); // the owner re-seeded — everything buffered is superseded
    expect(q.depth).toBe(0);

    releases[0]?.();
    await new Promise((r) => setTimeout(r, 0));
    expect(batches.slice(1).flat().map((c) => c.seq)).not.toContain(2);
  });
  it("keeps retrying when the consumer's onError handler THROWS", async () => {
    // These handlers are arbitrary app code — the replica forwards them to onStatus/onChanged, so a
    // React setState that throws lands here. Uncaught, it propagated out of the failure path BEFORE
    // scheduleRetry() ran: the batch was already requeued and the transport's high-water had already
    // advanced past it, so on a quiet feed nothing re-drained it and a reconnect resumed ABOVE data
    // that was never applied.
    let calls = 0;
    const applied: SeqChange[][] = [];
    const apply = (changes: SeqChange[]): Promise<{ cursor: number }> => {
      calls += 1;
      applied.push(changes);
      if (calls === 1) return Promise.reject(new Error("SQLITE_IOERR"));
      return Promise.resolve({ cursor: changes[changes.length - 1]?.seq ?? 0 });
    };
    const onDrained = vi.fn();
    const q = new DeltaQueue({
      apply,
      onDrained,
      onError: () => {
        throw new Error("consumer setState blew up");
      },
      onOverflow: vi.fn(),
      maxBatch: 10,
      retryDelayMs: 5,
    });

    q.push(change(1));
    // The self-driven retry must still fire and land the batch.
    await vi.waitFor(() => expect(calls).toBeGreaterThanOrEqual(2), { timeout: 2000 });
    await vi.waitFor(() => expect(onDrained).toHaveBeenCalled(), { timeout: 2000 });
    expect(applied[1]?.map((c) => c.seq)).toEqual([1]);
    expect(q.depth).toBe(0);
  });

  it("still escalates to a re-seed when onError throws on every failure", async () => {
    const onOverflow = vi.fn();
    const q = new DeltaQueue({
      apply: () => Promise.reject(new Error("SQLITE_IOERR")),
      onDrained: vi.fn(),
      onError: () => {
        throw new Error("consumer setState blew up");
      },
      onOverflow,
      maxBatch: 10,
      maxApplyRetries: 2,
      retryDelayMs: 5,
    });

    q.push(change(1));
    await vi.waitFor(() => expect(onOverflow).toHaveBeenCalledTimes(1), { timeout: 2000 });
    expect(onOverflow.mock.calls[0]?.[1]).toBe("apply-failed");
  });

  it("a throwing onDrained leaves the queue usable", async () => {
    // HONEST SCOPE: this passes with or without the notify() wrap today, because `draining` is already
    // cleared before onDrained runs — verified by negative control. It is kept as a guard against a
    // future reordering that moved the clear AFTER the callback, which would wedge the queue on the
    // first consumer render error. The wrap's other benefit — drain() still resolving, so the
    // `void this.drain()` call sites cannot raise an unhandled rejection — is not observable through
    // the public API, since push() has already consumed the inbox by the time a test can call drain().
    const applied: SeqChange[][] = [];
    const q = new DeltaQueue({
      apply: (changes) => {
        applied.push(changes);
        return Promise.resolve({ cursor: changes[changes.length - 1]?.seq ?? 0 });
      },
      onDrained: () => {
        throw new Error("consumer render blew up");
      },
      onError: vi.fn(),
      maxBatch: 10,
    });
    q.push(change(1));
    await new Promise((r) => setTimeout(r, 0));
    q.push(change(2));
    await new Promise((r) => setTimeout(r, 0));
    expect(applied.flat().map((c) => c.seq)).toEqual([1, 2]);
  });
});
