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

/**
 * Consecutive apply rejections tolerated before the queue stops retrying and asks for a re-seed.
 *
 * Bounded deliberately. A transient OPFS/SQLite error deserves a retry, but `applyChanges` can also
 * reject DETERMINISTICALLY (schema drift, a row the current schema cannot accept) — and an unbounded
 * retry on that is a hot loop that never converges. After this many failures the batch is no longer
 * replayable in place, so escalating to a snapshot is both the correct answer and the terminating one.
 */
export const MAX_APPLY_RETRIES = 3;

/** Base delay between apply retries; grows linearly with the failure count. */
export const APPLY_RETRY_DELAY_MS = 250;

/** Why the owner is being asked to re-seed. */
export type OverflowReason = "depth" | "apply-failed";

/** What the queue needs from its owner. `apply` resolves with the worker's post-apply cursor. */
export interface DeltaQueueOptions {
  apply: (changes: SeqChange[]) => Promise<{ cursor: number }>;
  /** Called ONCE per completed drain (not per batch, and never per frame) with the highest cursor. */
  onDrained: (cursor: number) => void;
  /** Called when an apply rejects. The failed batch is REQUEUED, so this is a warning, not a loss. */
  onError: (err: unknown) => void;
  /**
   * Called ONCE when the owner must drop the socket and re-seed from /snapshot, with the depth at the
   * moment of escalation and the reason:
   *
   *   • `"depth"`        — the buffer passed {@link MAX_INBOX_DEPTH}. The backlog is deeper than it is
   *     worth replaying, so the queue drops what it holds: those frames are about to be superseded by
   *     the snapshot, and keeping them is exactly the retention this bound exists to prevent.
   *   • `"apply-failed"` — {@link MAX_APPLY_RETRIES} consecutive applies rejected. The buffered rows
   *     cannot be written, so the cursor cannot advance past them safely; only a re-seed recovers.
   *
   * `reason` is a SECOND parameter rather than a changed first one so existing one-arg handlers keep
   * working unchanged.
   *
   * REQUIRED (CTC-114 review round 6). It was optional, and omitting it was silently terminal: an
   * overflow latches `overflowed` permanently, empties the inbox, and makes every later `push()` a
   * no-op — while `depth` reads 0 and no error is raised. This callback is the ONLY signal that the
   * owner must re-seed and call `resume()`, so without it a `DeltaQueue` stops syncing forever with
   * nothing to observe. `DeltaQueue` is publicly exported (`@catalyst-cloud/sdk/browser`), so that
   * was a reachable third-party configuration, not a theoretical one. Made required rather than
   * non-latching because the latch is load-bearing — dropping it would let a stale batch land on top
   * of a fresh snapshot. Free to tighten now: `./browser` is new and unpublished in 0.8.0, so there
   * is no install base to break.
   */
  onOverflow: (depth: number, reason: OverflowReason) => void;
  maxBatch?: number;
  maxDepth?: number;
  maxApplyRetries?: number;
  retryDelayMs?: number;
}

export class DeltaQueue {
  private readonly inbox: SeqChange[] = [];
  private draining = false;
  private stopped = false;
  private overflowed = false;
  private readonly opts: DeltaQueueOptions;
  private readonly maxBatch: number;
  private readonly maxDepth: number;
  private readonly maxApplyRetries: number;
  private readonly retryDelayMs: number;
  /** Consecutive apply rejections. Reset by any successful batch and by resume(). */
  private applyFailures = 0;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(opts: DeltaQueueOptions) {
    this.opts = opts;
    this.maxBatch = opts.maxBatch ?? MAX_APPLY_BATCH_ROWS;
    this.maxDepth = opts.maxDepth ?? MAX_INBOX_DEPTH;
    this.maxApplyRetries = opts.maxApplyRetries ?? MAX_APPLY_RETRIES;
    this.retryDelayMs = opts.retryDelayMs ?? APPLY_RETRY_DELAY_MS;

    // `DeltaQueue` and `DeltaQueueOptions` are both public exports of `./browser`, so every one of
    // these is consumer-supplied and may arrive from unchecked runtime config. Validate them ALL —
    // not just the one that happened to be reported (CTC-114 review round 10). `maxBatch` was guarded
    // in round 5 and `maxDepth` was not, though it is the same kind of value from the same object,
    // and its failure modes are worse. Fixing the class rather than the instance is the point.
    //
    //   • maxBatch        — `splice(0, 0)` removes NOTHING, so the drain loop's `while (length > 0)`
    //     never advances: it awaits `apply([])` and iterates again forever, an unbounded stream of
    //     empty worker transactions that never applies the queued change.
    //   • maxDepth        — 0/negative/fractional makes the FIRST frame overflow and force a
    //     snapshot; `NaN` makes `inbox.length > maxDepth` permanently FALSE, so the inbox grows
    //     without bound and the OOM backstop this whole module exists for is silently gone.
    //   • maxApplyRetries — below 1, the first transient apply error escalates straight to a full
    //     re-seed instead of retrying at all.
    //
    // Fail at construction rather than degrade silently at the first delta.
    const positiveInt: [string, number, unknown][] = [
      ["maxBatch", this.maxBatch, opts.maxBatch],
      ["maxDepth", this.maxDepth, opts.maxDepth],
      ["maxApplyRetries", this.maxApplyRetries, opts.maxApplyRetries],
    ];
    for (const [name, value, raw] of positiveInt) {
      if (!Number.isInteger(value) || value < 1) {
        throw new Error(
          `DeltaQueue: ${name} must be a positive integer (got ${String(raw)})`,
        );
      }
    }
    // Delay may legitimately be 0 (retry on the next tick) but must be finite and non-negative — NaN
    // makes setTimeout fire immediately, turning the bounded backoff into a hot loop.
    if (!Number.isFinite(this.retryDelayMs) || this.retryDelayMs < 0) {
      throw new Error(
        `DeltaQueue: retryDelayMs must be a non-negative finite number (got ${String(opts.retryDelayMs)})`,
      );
    }
    // Same reasoning, at RUNTIME, for the same reason (CTC-114 review round 6): the type now requires
    // `onOverflow`, but an untyped-JS consumer of the public export gets no help from that — which is
    // exactly the case the maxBatch guard above exists for. Omitting it is silently TERMINAL: the
    // first overflow latches the queue, empties the inbox, and turns every later push() into a no-op,
    // while `depth` reads 0 and nothing is raised. This callback is the only signal that the owner
    // must re-seed and call resume(), so failing at construction beats syncing forever-nothing.
    if (typeof opts.onOverflow !== "function") {
      throw new Error(
        "DeltaQueue: onOverflow is required — it is the ONLY signal that the queue has latched and " +
          "the owner must re-seed and call resume(); without it an overflow stops syncing silently.",
      );
    }
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
      this.clearRetry();
      this.notify("onOverflow", () => this.opts.onOverflow(depth, "depth"));
      return;
    }
    void this.drain();
  }

  /**
   * Latch the queue for an owner-driven re-seed, WITHOUT escalating back to the owner (CTC-114 review
   * round 9).
   *
   * The same latch `onOverflow` sets, minus the callback — the owner is the one re-seeding, so telling
   * it to re-seed would be circular. Without this, a reseed that began while buffered work remained
   * (a gap escalating mid-replay, say) left the queue live across `seedBegin`: its retry timer then
   * posted an `applyChanges` INTO the seed's open transaction, whose nested `BEGIN` rejects, and the
   * repeated failures walked the queue into its apply-failed overflow during an otherwise healthy
   * re-seed. The overflow's `requestResync()` then returned immediately — the transport is already
   * resyncing — so the escalation bought no recovery at all and simply discarded live frames.
   *
   * Dropping the inbox is correct for the same reason `resume()` drops it: everything buffered is
   * pre-snapshot by definition, and the snapshot supersedes it. `resume()` clears this.
   */
  pause(): void {
    if (this.stopped) return;
    this.overflowed = true;
    this.inbox.length = 0;
    this.clearRetry();
  }

  /** Clear the overflow latch so the queue accepts deltas again (after the owner's re-seed lands). */
  resume(): void {
    if (this.stopped) return;
    // Anything still buffered here is PRE-RESEED work by definition — while `overflowed` is latched
    // push() refuses every frame, so nothing new can have arrived — and the snapshot supersedes it.
    // Dropping it is the same reasoning the overflow paths already apply to the backlog they discard;
    // resuming without it would let a stale batch be applied on top of the fresh DB by the next live
    // push, where an old delete removes a row the snapshot legitimately has.
    this.inbox.length = 0;
    this.overflowed = false;
    // The re-seed supersedes whatever could not be applied, so the failure streak starts over.
    this.applyFailures = 0;
  }

  /**
   * Invoke a CONSUMER callback without letting it break the queue's state machine.
   *
   * These handlers are arbitrary app code — the replica forwards them straight to `onStatus` /
   * `onChanged`, so a React setState that throws lands here. Uncaught, the exception propagates out of
   * the failure path BEFORE `scheduleRetry()` or the overflow escalation runs: the batch is already
   * requeued and the transport's high-water has already advanced past it, so on a quiet feed nothing
   * ever re-drains it and a reconnect resumes ABOVE data that was never applied. The transport applies
   * the same discipline to its own user handlers.
   */
  private notify(label: string, fn: () => void): void {
    try {
      fn();
    } catch (err) {
      console.error(`[delta-queue] ${label} handler threw`, err);
    }
  }

  /** Cancel a pending self-retry. */
  private clearRetry(): void {
    if (this.retryTimer !== null) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
  }

  /**
   * Re-enter the drain on a timer after a failed apply.
   *
   * Self-driven on purpose: `push()` is the only other thing that starts a drain, so on a quiet feed a
   * requeued batch would otherwise sit in the inbox indefinitely — while `acceptedSeq` on the socket
   * has already advanced past it, meaning a reconnect would never replay it either.
   */
  private scheduleRetry(): void {
    if (this.stopped || this.overflowed || this.retryTimer !== null) return;
    const delay = this.retryDelayMs * this.applyFailures;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      void this.drain();
    }, delay);
  }

  /**
   * Drain the inbox into `apply` in bounded batches. SINGLE-FLIGHT — a re-entrant call is a no-op, so
   * at most one apply is outstanding at a time and everything arriving meanwhile is coalesced into the
   * next batch.
   */
  async drain(): Promise<void> {
    // A PENDING RETRY owns the next attempt (CTC-114 review round 5). `push()` calls drain() on every
    // incoming frame and only `draining` was checked — but the failure path clears `draining` before
    // arming the backoff, so the very next live frame re-entered immediately and the 250/500 ms delay
    // never elapsed. On a busy feed three frames could burn the whole `maxApplyRetries` budget in
    // milliseconds and escalate to a full /snapshot re-seed for a transient OPFS error that the
    // documented delay would have ridden out. Frames arriving meanwhile are NOT lost — they buffer in
    // the inbox, and the retry drains the whole of it. scheduleRetry()'s own timer nulls this field
    // before calling drain(), so the retry itself is never blocked by this guard.
    if (this.draining || this.stopped || this.retryTimer !== null) return;
    this.draining = true;
    let applied = false;
    let highest = 0;
    while (this.inbox.length > 0 && !this.stopped) {
      // `splice` REMOVES the batch before the await, so the failure path below must put it back. It
      // previously did not, and the loss was silent and permanent: worker-core.applyChanges advances
      // the durable OPFS cursor to the highest seq it has SEEN, so the next successful batch sealed
      // the hole below the cursor. A reload does not heal that, and the socket's high-water mark
      // stops a reconnect from replaying it — up to maxBatch rows simply ceased to exist locally.
      const batch = this.inbox.splice(0, this.maxBatch);
      try {
        const { cursor } = await this.opts.apply(batch);
        if (cursor > highest) highest = cursor;
        for (const c of batch) if (c.seq > highest) highest = c.seq;
        applied = true;
        this.applyFailures = 0;
      } catch (err) {
        this.draining = false;
        if (this.stopped) return;
        // A DEPTH overflow can latch WHILE this apply is in flight: push() cleared the inbox and the
        // owner is already re-seeding. Requeueing here would resurrect work the incoming snapshot
        // supersedes — and since scheduleRetry() is inert while overflowed, the batch would simply sit
        // in the inbox until the next live push drained it ON TOP of the fresh DB, where an old delete
        // removes a row the snapshot legitimately has. Drop it, and do not escalate again: the owner
        // has already been told to re-seed.
        if (this.overflowed) {
          this.notify("onError", () => this.opts.onError(err));
          return;
        }
        // Back at the FRONT: these seqs are strictly older than anything still queued behind them,
        // and the worker applies in order.
        this.inbox.unshift(...batch);
        this.applyFailures += 1;
        this.notify("onError", () => this.opts.onError(err));
        if (this.applyFailures >= this.maxApplyRetries) {
          // Not replayable in place. Drop the backlog and let the owner re-seed — same contract as a
          // depth overflow, so the owner needs no new branch.
          this.overflowed = true;
          const depth = this.inbox.length;
          this.inbox.length = 0;
          this.clearRetry();
          this.notify("onOverflow", () => this.opts.onOverflow(depth, "apply-failed"));
        } else {
          this.scheduleRetry();
        }
        return;
      }
    }
    this.draining = false;
    // ONE notification per drain rather than one per frame. The caller's reaction is a full view
    // rebuild + a structured clone of a whole page back across the boundary, and a burst of deltas has
    // exactly one visible outcome — so firing it per frame was pure waste.
    if (applied && !this.stopped)
      this.notify("onDrained", () => this.opts.onDrained(highest));
  }

  /** Drop everything buffered and refuse further work (teardown). Each queued delta retains a fully
   *  parsed row, so leaving them behind would pin that memory until the queue itself is collected. */
  stop(): void {
    this.stopped = true;
    this.clearRetry();
    this.inbox.length = 0;
  }
}
