// replica/delta-queue.ts — the coalescing buffer between the live socket and the OPFS worker (CTC-318).
//
// The SDK delivers ONE onChange per change_log row, and a stale-cursor reconnect replays up to
// CHANGE_LOG_MAX_ROWS = 200,000 of them back-to-back (apps/mirror/src/do/ws.ts replaySince sends one
// frame per row in a tight loop). The replica client used to answer each frame on its own:
//
//     onChange: (frame) => { void this.applyDelta(frame) }   →   call({ applyChanges, changes: [wire] })
//
// which is fire-and-forget with no backpressure anywhere. Every frame became its own structured clone,
// its own worker RPC, its own OPFS transaction and its own FULL view rebuild — while the gap between
// arrival rate (network) and drain rate (one fsync per row) accumulated without bound, holding a fully
// parsed row (issues carry the provider's `raw` JSON) per undrained frame.
//
// This module buffers frames and drains them in bounded batches under a SINGLE-FLIGHT loop. The
// coalescing needs no timer, which is the important trick: each WebSocket message arrives in its OWN
// task, so a microtask/queueMicrotask flush would batch nothing. Instead, while one apply is awaited
// every frame that arrives piles into the inbox and the next iteration takes the lot — so the batch
// size self-tunes to the lag. An idle tab applies one row at a time with no added latency; a replay
// applies them `maxBatch` at a time.
//
// Pure over an injected `apply` (the seam streamSeedIntoWorker established in CTC-132), so the
// batching is unit-testable without the wasm/OPFS worker.

import type { WireChange } from "./protocol.js";

/** A live delta as it rides into the worker: the wire record plus its change_log seq. */
export type SeqChange = WireChange & { seq: number };

/**
 * Largest number of deltas handed to the worker in ONE applyChanges RPC.
 *
 * The cap is about the CLONE, not the transaction — the worker already wraps a whole `changes` array
 * in a single transaction. `changes` is structured-cloned across the worker boundary and `issues` /
 * `issue_history` rows carry the provider's full `raw` JSON (measured avg ~9 KB, max ~94 KB), so an
 * uncapped batch would let a 200k-row replay serialize itself into one message. 500 keeps a batch in
 * the low single-digit MB while still amortizing the OPFS commit ~500×.
 */
export const MAX_APPLY_BATCH_ROWS = 500;

/**
 * Depth at which we stop buffering and ask for a fresh snapshot instead.
 *
 * Batching alone does NOT bound retention — it collapses the three retained copies per frame into one
 * and drains far faster, but the buffer itself is still O(frames received). At ~19.5 KB of parsed V8
 * objects per issues frame (measured), a 200k-frame replay would be ~3.9 GB of inbox on the main
 * thread. Past this depth a snapshot is simply cheaper than the replay — /snapshot is one streamed
 * body, not N individually-cloned frames — so the queue escalates instead of growing.
 *
 * 20,000 × ~19.5 KB ≈ 390 MB worst case, which is a bad-but-survivable ceiling rather than an
 * unbounded one. The real remedy is to stop the server creating the condition (pace/cap `replaySince`
 * in apps/mirror/src/do/ws.ts) — this is the client-side backstop for when it does.
 */
export const MAX_INBOX_DEPTH = 20_000;

/** What the queue needs from its owner. `apply` resolves with the worker's post-apply cursor. */
export interface DeltaQueueOptions {
  apply: (changes: SeqChange[]) => Promise<{ cursor: number }>;
  /** Called ONCE per completed drain (not per batch, and never per frame) with the highest cursor. */
  onDrained: (cursor: number) => void;
  /** Called when an apply rejects. Whatever is still queued is left in place for the next push. */
  onError: (err: unknown) => void;
  /**
   * Called ONCE when the buffer passes {@link MAX_INBOX_DEPTH} — the backlog is deeper than it is
   * worth replaying, so the owner should drop the socket and re-seed from /snapshot. The queue drops
   * what it is holding at that point: those frames are about to be superseded by the snapshot, and
   * keeping them is exactly the retention this bound exists to prevent.
   */
  onOverflow?: (depth: number) => void;
  maxBatch?: number;
  maxDepth?: number;
}

export class DeltaQueue {
  private readonly inbox: SeqChange[] = [];
  private draining = false;
  private stopped = false;
  private overflowed = false;
  private readonly opts: DeltaQueueOptions;
  private readonly maxBatch: number;
  private readonly maxDepth: number;

  constructor(opts: DeltaQueueOptions) {
    this.opts = opts;
    this.maxBatch = opts.maxBatch ?? MAX_APPLY_BATCH_ROWS;
    this.maxDepth = opts.maxDepth ?? MAX_INBOX_DEPTH;
  }

  /** How many deltas are buffered but not yet applied (observability + tests). */
  get depth(): number {
    return this.inbox.length;
  }

  /** Buffer one delta and make sure a drain is running. */
  push(change: SeqChange): void {
    if (this.stopped || this.overflowed) return;
    this.inbox.push(change);
    if (this.inbox.length > this.maxDepth) {
      // Deeper than it is worth replaying — drop the backlog and let the owner re-seed. Latched, so
      // a burst raises exactly one escalation rather than one per subsequent frame.
      this.overflowed = true;
      const depth = this.inbox.length;
      this.inbox.length = 0;
      this.opts.onOverflow?.(depth);
      return;
    }
    void this.drain();
  }

  /** Clear the overflow latch so the queue accepts deltas again (after the owner's re-seed lands). */
  resume(): void {
    if (this.stopped) return;
    this.overflowed = false;
  }

  /**
   * Drain the inbox into `apply` in bounded batches. SINGLE-FLIGHT — a re-entrant call is a no-op, so
   * at most one apply is outstanding at a time and everything arriving meanwhile is coalesced into the
   * next batch.
   */
  async drain(): Promise<void> {
    if (this.draining || this.stopped) return;
    this.draining = true;
    let applied = false;
    let highest = 0;
    try {
      while (this.inbox.length > 0 && !this.stopped) {
        const batch = this.inbox.splice(0, this.maxBatch);
        const { cursor } = await this.opts.apply(batch);
        if (cursor > highest) highest = cursor;
        for (const c of batch) if (c.seq > highest) highest = c.seq;
        applied = true;
      }
    } catch (err) {
      // Leave whatever is still queued in place — the next push() re-enters the drain.
      this.draining = false;
      if (!this.stopped) this.opts.onError(err);
      return;
    }
    this.draining = false;
    // ONE notification per drain rather than one per frame. The caller's reaction is a full view
    // rebuild + a structured clone of a whole page back across the boundary, and a burst of deltas has
    // exactly one visible outcome — so firing it per frame was pure waste.
    if (applied && !this.stopped) this.opts.onDrained(highest);
  }

  /** Drop everything buffered and refuse further work (teardown). Each queued delta retains a fully
   *  parsed row, so leaving them behind would pin that memory until the queue itself is collected. */
  stop(): void {
    this.stopped = true;
    this.inbox.length = 0;
  }
}
