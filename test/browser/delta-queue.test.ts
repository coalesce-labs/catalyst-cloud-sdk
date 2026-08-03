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
      // Keep the self-retry inert so this test observes the requeue directly rather than racing it.
      retryDelayMs: 10_000,
    });

    q.push(change(1));
    q.push(change(2));
    await vi.waitFor(() => expect(onError).toHaveBeenCalledTimes(1));
    // BOTH frames are buffered: the failed batch went back to the front, and #2 is still behind it.
    // This previously read `1` — the failed batch had been spliced out and dropped, and the assertion
    // was pinning that loss while its comment claimed the opposite.
    expect(q.depth).toBe(2);

    // …and the next arrival re-enters the drain rather than wedging forever.
    q.push(change(3));
    await vi.waitFor(() => expect(q.depth).toBe(0));
    expect(onDrained).toHaveBeenCalled();
    // The whole point: seq 1 is APPLIED, in order, not skipped. A depth-only assertion cannot see this.
    expect(applied).toEqual([1, 2, 3]);
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
});
