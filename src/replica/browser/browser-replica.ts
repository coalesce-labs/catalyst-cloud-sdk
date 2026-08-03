// replica/browser/browser-replica.ts — the MAIN-THREAD client for the OPFS replica (CTC-51 → CTC-114,
// ADR-0002). Owns the Worker (db.worker.ts), drives it over the protocol.ts RPC, and wires the cloud
// transport:
//
//   • LOCK   — claim single-ownership of the origin's replica via Web Locks (browser-lock.ts) BEFORE
//              loading any worker or wasm. A losing tab surfaces "secondary" — a clean state, not an
//              error — and stays on its fetch+live path (CTC-118).
//   • SEED   — STREAM the /snapshot NDJSON (same endpoint the node CatalystReplica seeds from): decode
//              it incrementally into bounded batches (snapshot-stream.ts) and drive the worker over
//              seedBegin→seedBatch*→seedCommit so no layer buffers the whole (~100 MB) body (CTC-132 —
//              peak memory is one batch).
//   • LIVE   — the SAME `LiveSyncClient` that feeds the node replica feeds this in-browser OPFS DB,
//              through a coalescing, single-flight, depth-bounded DeltaQueue (CTC-318) — never one RPC
//              per frame.
//
// AUTH: the browser rides the httpOnly session cookie on same-origin requests — there is NO Bearer
// token here (that's the node path). `auth:{kind:"cookie"}` appends NOTHING to the connect URL (the
// type system + the SDK's single URL-construction point make a browser token leak impossible).
//
// PACKAGING (CTC-114): the worker is constructed with `new Worker(new URL("./db.worker.js",
// import.meta.url), { type: "module" })` — the idiom Vite, webpack 5 and Rollup statically detect and
// split into its own chunk (the wasm + worker code never touch the consumer's main bundle). Consumers
// with an exotic bundler can inject `createWorker` instead.

import { LiveSyncClient } from "../../live-sync-client.js";
import type {
  IssueView,
  IssueDetailView,
  PullView,
} from "@catalyst-cloud/read-model";
import type {
  Envelope,
  ReplicaRequest,
  ReplicaResponse,
  ResultMap,
} from "./protocol.js";
import { streamSnapshotBatches } from "./snapshot-stream.js";
import { DeltaQueue } from "./delta-queue.js";
import { acquireReplicaLock, type ReplicaLockHandle } from "./browser-lock.js";
import { requireNonNegativeFinite } from "./validate.js";

/** Where the replica DB file + its OPFS dir live by default. Absolute path (SAHPool requires a leading
 *  slash); one directory per origin-app so engines don't collide. */
const DEFAULT_DB_PATH = "/catalyst-replica.sqlite3";
const DEFAULT_OPFS_DIR = ".catalyst-replica";

/** How long a cooperative `close` RPC gets to release the OPFS handles before the Worker is
 *  terminated anyway — teardown must be bounded (a wedged worker can't be allowed to leak forever). */
const CLOSE_GRACE_MS = 250;

/**
 * How long the seed tolerates NO PROGRESS before aborting. IDLE, never total duration: a legitimate
 * ~100 MB snapshot streams for minutes, and the node replica rejects a total cap for exactly that
 * reason. The timer re-arms before the fetch, on response headers, and on every body chunk — so this
 * only fires when the server has genuinely stopped sending.
 */
const DEFAULT_SNAPSHOT_IDLE_TIMEOUT_MS = 30_000;

/**
 * How long a SEED's worker RPC may go unanswered before the worker is declared dead (CTC-114 review
 * round 6).
 *
 * Separate from the snapshot idle bound above, because they measure different things and one number
 * cannot serve both. That bound watches the NETWORK and is deliberately down while the worker works —
 * which left a worker that accepts a `postMessage` and never replies unbounded, since `call()` has no
 * deadline of its own. That is not merely a hang: the worker is sitting in an OPEN seed transaction
 * with its `SeedReadGate` armed, so every later read waits forever and delta applies fail as nested
 * transactions. On a live reseed it is worse — `LiveSyncClient.boundedReseed` abandons the callback
 * after its own deadline while that transaction and gate stay open, so the client carries on against a
 * replica that can no longer answer anything.
 *
 * GENEROUS BY DESIGN: this exists to convert a WEDGED worker into a diagnosable error, not to police
 * slow-but-progressing storage. A `seedCommit` of a ~100 MB snapshot on throttled OPFS is legitimately
 * slow, and aborting one that is about to succeed is the exact failure the round-5 fix removed. Four
 * times the network idle bound, and overridable.
 */
const DEFAULT_WORKER_RPC_TIMEOUT_MS = 120_000;

/**
 * Request types whose deadline PAUSES while a seed is in flight — the reads.
 *
 * They are the one family whose pending time is not all the worker's own work: the `SeedReadGate`
 * defers a read for as long as an open seed lasts (CTC-132), and a legitimate ~100 MB snapshot
 * streams for minutes, so a flat deadline would abort healthy reads during exactly the operation the
 * gate exists to protect them from.
 *
 * Round 9 EXEMPTED them outright, on the reasoning that some other bounded RPC would notice a dead
 * worker first. Round 10 showed that to be unfounded: on a quiet feed no apply and no seed need ever
 * follow, so a UI awaiting `queryIssues()` hung forever. The exemption is gone — reads are bounded
 * like everything else, but their window only counts time in which the worker could actually have
 * answered. This is the same principle the snapshot idle bound already follows: measure the time the
 * operation is able to progress, not wall-clock. We can do it precisely because THIS client drives
 * the seed, so it knows exactly when a read is legitimately gated.
 */
const SEED_GATED_WORKER_RPCS: ReadonlySet<string> = new Set([
  "queryIssues",
  "queryIssueDetail",
  "queryPulls",
]);

/**
 * Resolve the API base to a URL — the SINGLE point of construction.
 *
 * `snapshotUrl` and `subscribe` each derived this independently, which is a real hazard once a
 * same-origin guard exists: the guard can pass on one derivation while the fetch is built from the
 * other. One helper, used by both, makes that impossible.
 */
function resolveBase(baseUrl: string): URL {
  const isAbsolute = /^https?:\/\//i.test(baseUrl);
  const origin =
    typeof location !== "undefined" ? location.origin : "http://localhost";
  return isAbsolute ? new URL(baseUrl) : new URL(baseUrl, origin);
}

/**
 * Reject a `baseUrl` that resolves off-origin.
 *
 * Cross-origin cannot work from the client side at all, so accepting it only produces an
 * undiagnosable failure much later: `/snapshot` is served under the mirror's credential-free CORS
 * posture (no `Access-Control-Allow-Credentials`), and the session cookie is host-only, so a
 * cross-origin seed is unauthenticated no matter what this client sends. Failing here, before any
 * resource is claimed, turns that into one actionable message. An absolute-but-same-origin base stays
 * legal — it is the documented form. Returns early when `location` is undefined (SSR / node tests).
 */
function assertSameOriginBase(baseUrl: string): void {
  if (typeof location === "undefined") return;
  const resolved = resolveBase(baseUrl);
  if (resolved.origin !== location.origin) {
    throw new Error(
      `BrowserReplica: baseUrl must resolve to this origin (${location.origin}); got ${resolved.origin}. ` +
        "Cross-origin replicas cannot authenticate — /snapshot is served without Access-Control-Allow-Credentials and the session cookie is host-only.",
    );
  }
}

/**
 * Turn a worker `error` / `messageerror` event into an actionable Error.
 *
 * A module-script load failure fires a BARE `Event` with no `.message` and no `.error` — so the
 * natural `event.message` read yields `undefined` and the consumer gets nothing at all. This is the
 * single most likely first-integration failure for a third party (the bundler didn't emit the worker
 * chunk, CSP blocks `worker-src`, or the sqlite-wasm peer is missing), so it is worth spelling out.
 */
function describeWorkerError(event: Event): Error {
  const withError = event as { error?: unknown; message?: unknown };
  if (withError.error instanceof Error) return withError.error;
  if (typeof withError.message === "string" && withError.message.length > 0) {
    return new Error(`replica worker failed: ${withError.message}`);
  }
  return new Error(
    "replica worker failed to load. The bundler may not have emitted the db.worker chunk — " +
      "pass createWorker with the '@catalyst-cloud/sdk/browser/db-worker' specifier — or a Content-Security-Policy " +
      "worker-src directive is blocking it, or the '@sqlite.org/sqlite-wasm' peer dependency is not installed.",
  );
}

/**
 * Status of the replica (superset of the transport's vocabulary):
 *   • "secondary" — another live tab owns the origin's replica (Web Locks); THIS tab should read via
 *     its normal fetch path. Not an error, not retryable from here (CTC-118).
 *   • "unsupported" — the engine lacks OPFS/worker support (see isBrowserReplicaSupported).
 */
export type ReplicaStatus =
  "loading" | "live" | "reconnecting" | "error" | "unsupported" | "secondary";

/** Handlers the consumer supplies to react to replica state changes. */
export interface ReplicaHandlers {
  /** Fired after the initial seed AND after every applied delta DRAIN — re-run the relevant view query. */
  onChanged: () => void;
  /** Connection / load status for the live affordance. */
  onStatus: (status: ReplicaStatus) => void;
}

export interface BrowserReplicaOptions {
  /**
   * The mirror API base the replica seeds + subscribes against — e.g. "/api/v1" (same-origin,
   * cookie-authed) or an absolute "https://host/api/v1". Relative values resolve against
   * `location.origin`.
   *
   * MUST resolve to this origin. An absolute URL is permitted, but only when its origin matches —
   * `start()` rejects anything else, because a cross-origin replica cannot authenticate at all
   * (`/snapshot` is served without `Access-Control-Allow-Credentials`, and the session cookie is
   * host-only). Supporting it needs a server-side change, not a client one.
   */
  baseUrl: string;
  /** Target a specific account the user is a member of; omit for the session's own tenant. */
  accountId?: string;
  /**
   * Who this replica belongs to — REQUIRED (CTC-114 review).
   *
   * The persisted OPFS database is shared by every replica on the origin (`dbPath`/`directory` default
   * to constants), and the warm-start path skips `/snapshot` whenever a cursor is persisted. Without a
   * fence, a change of signed-in user left the PREVIOUS tenant's rows readable — and unremovable,
   * since deltas only carry changes. Pass a stable per-user (or per-user-per-tenant) string, e.g. the
   * WorkOS user id. It is compared at `open`, and a mismatch wipes the replica and forces a re-seed.
   *
   * It is opaque and never sent to the server — only compared against the value in `sync_meta`.
   */
  identity: string;
  /** Override the OPFS db path (absolute, leading slash required). */
  dbPath?: string;
  /** Override the OPFS directory the SAHPool manages. */
  directory?: string;
  /**
   * Override worker construction for bundlers that can't statically analyze
   * `new Worker(new URL(...))`. The worker must run this package's db.worker module, published at the
   * `@catalyst-cloud/sdk/browser/db-worker` subpath — e.g. Vite's
   * `import DbWorker from "@catalyst-cloud/sdk/browser/db-worker?worker"`.
   */
  createWorker?: () => Worker;
  /** Disable the Web Locks single-owner gate (NOT recommended — see browser-lock.ts). */
  disableLock?: boolean;
  /**
   * Abort the seed after this many ms with NO PROGRESS. Default 30s; `0` disables.
   *
   * Idle, never total duration — a legitimate ~100 MB snapshot streams for minutes, so a total cap
   * would kill real seeds. Without any bound a stalled `/snapshot` never settles `start()`, every read
   * hangs behind the worker's armed SeedReadGate, and the transport's own 10-minute reseed deadline
   * then abandons a seed that is still running — whose late `seedCommit` can persist a stale cursor
   * over a truncated DB, which the next cold start reads as warm and goes live over.
   */
  snapshotIdleTimeoutMs?: number;
  /**
   * Declare the worker dead if a SEED RPC goes unanswered this long. Default 120s; `0` disables.
   *
   * Distinct from `snapshotIdleTimeoutMs`, which watches the network and is deliberately down while a
   * worker RPC is in flight. See {@link DEFAULT_WORKER_RPC_TIMEOUT_MS} — generous by design: it turns a
   * WEDGED worker into a diagnosable error, and is not meant to police slow-but-progressing storage.
   */
  workerRpcTimeoutMs?: number;
  /**
   * Total deadline the TRANSPORT allows one re-seed before cancelling it. Default: the SDK's 600s;
   * `0` disables.
   *
   * Distinct again from the two bounds above, which watch the network and each worker RPC. This one
   * bounds the whole operation, and since round 10 it CANCELS rather than merely abandons — the
   * signal is forwarded into the seed, which aborts its fetch and rolls the worker transaction back.
   * Exposed because the right value depends on corpus size: a very large snapshot on slow storage can
   * legitimately outlive the default, and before cancellation was linked that produced two writers.
   */
  reseedTimeoutMs?: number;
}

/** Resolve `${base}/snapshot` preserving an absolute base's path (mirror of the reads' apiUrl). */
function snapshotUrl(baseUrl: string, account?: string): string {
  const base = resolveBase(baseUrl);
  const basePath = base.pathname.replace(/\/$/, "");
  const url = new URL(base.toString());
  url.pathname = `${basePath}/snapshot`;
  // The OPEN read plane scopes to the session's own tenant when ?account= is omitted; only pass it for
  // an explicit tenant switcher.
  if (account) url.searchParams.set("account", account);
  return url.toString();
}

/**
 * Drive a streamed /snapshot body into the worker over the batched seed protocol: post `seedBegin`,
 * decode the body into bounded batches (streamSnapshotBatches), post each as a `seedBatch`, capture the
 * terminal cursor, then post `seedCommit`. On any error post `seedAbort` (best-effort) and rethrow so
 * the whole seed rolls back. Exported (and pure over an injected `call`) so it is unit-testable without
 * the wasm/OPFS worker (CTC-132).
 */
export async function streamSeedIntoWorker(
  call: (req: ReplicaRequest) => Promise<unknown>,
  body: ReadableStream<Uint8Array>,
  batchSize?: number,
  /** Fired whenever the body delivers bytes — the seed's idle-timeout re-arm. Optional, so every
   *  existing caller and test compiles unchanged. */
  onProgress?: () => void,
): Promise<number> {
  try {
    // seedBegin is INSIDE the try so a mid-begin failure (e.g. truncate threw with the txn already
    // open) still triggers the rollback below (CTC-132 review finding B).
    await call({ type: "seedBegin" });
    let cursor = 0;
    for await (const item of streamSnapshotBatches(body, batchSize, onProgress)) {
      if (item.kind === "batch")
        await call({ type: "seedBatch", rows: item.rows });
      else cursor = item.cursor;
    }
    await call({ type: "seedCommit", cursor });
    return cursor;
  } catch (err) {
    try {
      await call({ type: "seedAbort" });
    } catch {
      // worker already torn down / txn already aborted — surface the original error.
    }
    throw err;
  }
}

/**
 * BrowserReplica — claim the origin lock, open the Worker, seed from /snapshot, then keep the OPFS
 * replica live off the SDK's `LiveSyncClient` (the SAME client that feeds the node replica). The
 * consumer reads views via queryIssues/queryIssueDetail/queryPulls (each a Worker round-trip that runs
 * the shared read-model builder over the local DB). `close()` tears down the SDK client + the Worker.
 */
export class BrowserReplica {
  private worker: Worker | null = null;
  private readonly options: BrowserReplicaOptions;
  private live: LiveSyncClient | null = null;
  private lock: ReplicaLockHandle | null = null;
  /** The DURABLE cursor — advanced only once the worker has committed, and what a cold start reads. */
  private lastSeq = 0;
  /**
   * The TRANSPORT high-water: the highest seq ACCEPTED off the socket, advanced synchronously on
   * arrival (CTC-318).
   *
   * The SDK re-baselines from `getCursor()` on EVERY socket open (`deliveredSeq = getCursor()`, then
   * `{type:"sync", after}`), so reporting the durable cursor — which lags by the whole undrained
   * buffer — made a reconnect mid-drain re-request everything already received-but-not-committed, and
   * the DO replayed it into the same buffer on top of what was there. K reconnects meant K copies.
   *
   * In-memory only, deliberately: a page reload reads the durable cursor from `sync_meta`, so a tab
   * that dies between accept and commit still re-requests those frames on its next cold start.
   */
  private acceptedSeq = 0;
  private nextId = 1;
  private readonly pending = new Map<
    number,
    { resolve: (v: unknown) => void; reject: (e: Error) => void }
  >();
  private disposed = false;
  /** Has start() been called? One-shot, like the node CatalystReplica — see start(). */
  private started = false;
  /** Latched worker load/runtime failure, so every later call reports the CAUSE, not "closed". */
  private workerError: Error | null = null;
  /** Aborts the in-flight seed (close, supersede, or idle timeout). */
  private seedAbort: AbortController | null = null;
  /** Monotonic seed id — a superseded seed must not publish its cursor over a newer one's. */
  private seedGeneration = 0;
  private readonly handlers: ReplicaHandlers;
  /** Coalescing buffer between the socket and the worker (CTC-318) — see delta-queue.ts. */
  private readonly deltas: DeltaQueue;

  constructor(handlers: ReplicaHandlers, options: BrowserReplicaOptions) {
    // The fence is only a fence if it holds a value. An untyped-JS consumer following the old README
    // produced the shared key "undefined\0" for EVERY user, which is worse than no fence at all: it
    // looks isolated and is not.
    if (!options.identity) {
      throw new Error(
        "BrowserReplica: `identity` is required — pass a stable per-user id (e.g. the session user id). " +
          "The OPFS database is shared across the origin and a warm cursor skips the snapshot, so without it a change of signed-in user serves the previous user's rows.",
      );
    }
    // ISOLATE the consumer's callbacks ONCE, here, rather than at each of the ~16 call sites (CTC-114
    // review round 4). These are arbitrary app code — a React setState that throws lands in whatever
    // internal path happened to be notifying. The concrete finding: `onStatus("reconnecting")` is
    // raised at the TOP of the overflow handler, so a throw there escaped before `acceptedSeq` was
    // rolled back and `requestResync()` dispatched. `DeltaQueue.notify()` catches it, but the queue
    // stays overflow-LATCHED — `push()` then refuses every subsequent frame while the socket happily
    // stays up, i.e. a silently frozen replica still reporting itself connected.
    //
    // Wrapping at the boundary (not per-site) is deliberate: it cannot be forgotten by a later call
    // site, and it holds for `onChanged` too. This is the same discipline DeltaQueue and the transport
    // already apply to their own user handlers.
    this.handlers = {
      onStatus: (status) => {
        try {
          handlers.onStatus(status);
        } catch (err) {
          console.error(`[replica] onStatus("${status}") handler threw`, err);
        }
      },
      onChanged: () => {
        try {
          handlers.onChanged();
        } catch (err) {
          console.error("[replica] onChanged handler threw", err);
        }
      },
    };
    // Validate the numeric knobs ONCE, here (CTC-114 review round 13) — the FOURTH appearance of this
    // class, and the first inside an option I added myself. `NaN` defeats each of these the same way:
    // `ms <= 0` is false, so the guard that disables the bound does not fire, and `setTimeout(fn, NaN)`
    // is then coerced to a ZERO-delay timer. A NaN `workerRpcTimeoutMs` therefore declares a healthy
    // worker wedged before its own `open` reply can arrive, terminates it, and rejects `start()` —
    // the opposite of the "generous by design" contract documented on the constant.
    //
    // Round 12 introduced validate.ts for exactly this and I applied it to the exported helpers while
    // walking past the options object of the main public class. All three of these accept 0 to disable
    // the bound, so non-negative rather than positive.
    for (const [label, value] of [
      ["snapshotIdleTimeoutMs", options.snapshotIdleTimeoutMs],
      ["workerRpcTimeoutMs", options.workerRpcTimeoutMs],
      ["reseedTimeoutMs", options.reseedTimeoutMs],
    ] as const) {
      if (value !== undefined) {
        requireNonNegativeFinite("BrowserReplica", label, value);
      }
    }
    this.options = options;
    this.deltas = new DeltaQueue({
      // Bounded by `call()` itself (round 9) — a wedged apply used to leave the queue permanently
      // `draining`, reaching NEITHER its retry nor its overflow recovery, while arriving frames kept
      // advancing `acceptedSeq` so a reconnect resumed above changes that were never committed.
      apply: (changes) => this.call({ type: "applyChanges", changes }),
      onDrained: (cursor) => {
        this.lastSeq = Math.max(this.lastSeq, cursor);
        if (this.disposed) return;
        this.handlers.onStatus("live");
        this.handlers.onChanged();
      },
      onError: (err) => {
        if (this.disposed) return;
        console.error("[replica] applying live deltas failed:", err);
        this.handlers.onStatus("error");
      },
      // THE COMPENSATION FOR A DISCARD, paired with it at the queue's seam (CTC-114 review round 12).
      //
      // Whatever the queue just dropped was accepted off the socket and is now in neither the store
      // nor the buffer. `acceptedSeq` still describes it and `getCursor()` reports the max of the two,
      // so without this the next `{type:"sync", after}` resumes ABOVE those frames, the gap detector
      // re-baselines from the same poisoned value and sees contiguity, and the next applied batch
      // carries the durable cursor past the hole — permanent, re-seed-only recovery.
      //
      // This lived in `onOverflow` and therefore covered ONE of the queue's six discard sites. Round
      // 9's `pause()` was the second, and it lost data on every failed re-seed that began with frames
      // still buffered. Rolling back to the DURABLE cursor is deliberately conservative: an apply
      // still in flight may re-deliver rows the worker already has, and worker-core's
      // `rec.seq <= maxSeq` stale-guard drops those. Re-requesting a few frames is free; missing one
      // is not.
      onDiscard: () => {
        this.acceptedSeq = this.lastSeq;
      },
      // The queue has given up on what it holds and needs the owner to replace the DB wholesale —
      // either the backlog outgrew replaying it ("depth") or applies kept rejecting ("apply-failed").
      onOverflow: (depth, reason) => {
        if (reason === "apply-failed") {
          console.error(
            `[replica] ${depth} buffered frames could not be applied — re-seeding from /snapshot`,
          );
        } else {
          console.warn(
            `[replica] live backlog reached ${depth} frames — re-seeding from /snapshot instead of replaying`,
          );
        }
        if (this.disposed) return;
        this.handlers.onStatus("reconnecting");

        // Roll the TRANSPORT high-water back to the durable cursor. The queue just discarded its
        // inbox, so `acceptedSeq` now describes frames that were received and then thrown away. If
        // the replacement seed FAILS, the next connect would resume above those frames and seal a
        // second hole that nothing ever re-requests.
        this.acceptedSeq = this.lastSeq;

        // Route through the TRANSPORT, not straight to reseed(). requestResync() closes the socket
        // FIRST, which is the whole point: a bare reseed() runs while the socket is still delivering,
        // and every frame written during the multi-second /snapshot lands in a window that is in
        // neither the snapshot nor the DB — while the transport counts them as delivered, so the gap
        // detector is structurally blind to them.
        //
        // Resuming ON SETTLE is the fix for the queue staying latched forever. `overflowed` gates
        // every subsequent push, so when resume() ran only after a SUCCESSFUL re-seed, a transient
        // snapshot error discarded every later frame while the socket stayed up — a silently stale
        // replica reporting "live" until the tab was reloaded. requestResync() itself re-enters the
        // backoff/reconnect path on a failed reseed, so the retry is the transport's.
        //
        // `.finally` rather than `.then` is belt-and-braces, NOT the mechanism: requestResync() never
        // rejects by contract, so the two are equivalent today (verified — swapping them changes no
        // test). It is written this way so the unlatch survives that contract changing.
        const live = this.live;
        if (live) {
          void live.requestResync().finally(() => {
            // Only unlatch if NO seed is in flight (round 9). requestResync() can return without
            // re-seeding at all — the transport may already be resyncing, or the boot may have failed
            // — and in the already-resyncing case a seed is running right now with the queue paused by
            // its own preamble. Resuming here would clear that pause and let stale frames apply into
            // its open transaction. When a seed IS running, its `finally` owns the unlatch; this arm
            // exists for the case where requestResync did nothing and nobody else will.
            if (!this.seedAbort) this.deltas.resume();
          });
          return;
        }
        // No transport yet (overflow during boot, before subscribe()) — there is no socket to
        // quiesce, so seed directly, but keep the same unlatch-on-either-arm discipline.
        void this.reseed()
          .catch((err: unknown) => {
            console.error(
              "[replica] re-seed after backlog overflow failed:",
              err,
            );
            if (!this.disposed) this.handlers.onStatus("error");
          })
          .finally(() => this.deltas.resume());
      },
    });
  }

  /**
   * The tenant fence sent with `open`.
   *
   * `accountId` is folded in alongside `identity` so a TENANT SWITCHER cannot reuse another account's
   * rows even when the consumer's `identity` only identifies the signed-in user. NUL-joined because it
   * cannot occur in either component, so no pair of values can collide by concatenation.
   */
  private identityKey(): string {
    return `${this.options.identity}\0${this.options.accountId ?? ""}`;
  }

  /**
   * Send a typed request to the Worker and resolve with its result (cast by the ResultMap entry).
   *
   * BOUNDED HERE, for every request type, rather than at individual call sites (CTC-114 review round
   * 9). Rounds 6 and 7 wrapped the seed RPCs and then the apply RPC one at a time, and round 9 found
   * the ones still missed — `open` and `getCursor`, where a wedged `installOpfsSAHPoolVfs()` left
   * `start()` pending forever while holding the origin-wide Web Lock, with every sibling tab reporting
   * "secondary" and no worker `error` event to notice. Bounding at the seam instead makes that class
   * of miss structural: a new request type is bounded by construction, not by remembering.
   *
   * EVERY type is bounded, reads included. Their window merely pauses while a seed is in flight —
   * see {@link SEED_GATED_WORKER_RPCS}.
   */
  private call<K extends ReplicaRequest["type"]>(
    request: Extract<ReplicaRequest, { type: K }>,
  ): Promise<ResultMap[K]> {
    // A latched worker failure comes FIRST — above the disposed check — so the caller gets the real
    // cause ("the bundler didn't emit the worker chunk") instead of the generic "replica client
    // closed" that failWorker's teardown would otherwise produce.
    if (this.workerError) return Promise.reject(this.workerError);
    // Once closed (or before start), there is no Worker to reply — reject synchronously rather than
    // register a pending entry that would hang forever (CTC-132 review finding). This closes the
    // teardown race: close() rejects an in-flight seedBatch, whose catch fires a fresh seedAbort call;
    // without this guard that abort would postMessage a dead Worker and leave streamSeedIntoWorker /
    // reseed() / start() pending indefinitely, retaining the client and a stranded pending-map entry.
    if (this.disposed || !this.worker) {
      return Promise.reject(new Error("replica client closed"));
    }
    const worker = this.worker;
    const id = this.nextId++;
    const envelope: Envelope = { id, request };
    const sent = new Promise<ResultMap[K]>((resolve, reject) => {
      this.pending.set(id, {
        resolve: (v) => resolve(v as ResultMap[K]),
        reject,
      });
      worker.postMessage(envelope);
    });
    return this.withWorkerDeadline(request.type, sent);
  }

  /**
   * Boot the replica: claim the origin lock, open the OPFS DB (applyMigrations runs inside the
   * worker), seed from /snapshot, then subscribe to the live SDK feed. Resolves once the seed lands
   * (the first view query is valid after) — or immediately with status "secondary" when another tab
   * owns the replica.
   */
  async start(): Promise<void> {
    // BEFORE any status change and before the lock: a tab pointed at the wrong origin must never
    // claim the origin's Web Lock. If it did, it would win the election, fail its own seed, and hold
    // every OTHER tab on the origin in "secondary" — a misconfiguration in one place taking out the
    // replica everywhere.
    assertSameOriginBase(this.options.baseUrl);
    // One-shot, matching the node CatalystReplica. A second start() would otherwise construct a second
    // worker while the first still holds the OPFS SyncAccessHandles, or (with the lock enabled) report
    // a spurious "secondary" against this instance's own lock. Consumers construct a fresh instance
    // per effect run, which is also what makes the failed-boot retry path work.
    if (this.disposed) {
      throw new Error("BrowserReplica: start() after close()");
    }
    if (this.started) {
      throw new Error("BrowserReplica: start() already called");
    }
    this.started = true;
    this.handlers.onStatus("loading");
    // The lock comes FIRST — before the worker, before the wasm chunk — so a losing tab pays nothing.
    if (!this.options.disableLock) {
      let lock: ReplicaLockHandle | null;
      try {
        lock = await acquireReplicaLock(
          `catalyst-replica:${this.options.directory ?? DEFAULT_OPFS_DIR}`,
        );
      } catch (err) {
        // A REJECTING lock manager is a BOOT FAILURE, not contention (CTC-114 review round 5). It used
        // to be folded into the same `null` that means "another tab owns the replica", so start()
        // resolved with the clean, terminal "secondary" state and the consumer sat on the fallback
        // path forever with nothing to diagnose. Route it to the documented boot-error path instead.
        // Nothing to release: the lock is claimed BEFORE the worker, so none exists yet.
        console.error("[replica] boot failed: the Web Locks API rejected:", err);
        if (!this.disposed) this.handlers.onStatus("error");
        throw err;
      }
      if (this.disposed) {
        lock?.release();
        return;
      }
      if (lock === null) {
        this.handlers.onStatus("secondary");
        return;
      }
      this.lock = lock;
    }
    try {
      this.worker = this.options.createWorker
        ? this.options.createWorker()
        : new Worker(new URL("./db.worker.js", import.meta.url), {
            type: "module",
          });
      // Register the FAILURE listeners immediately after construction and BEFORE the first
      // postMessage. A module-script load failure is asynchronous and can land before `open` is even
      // answered; without a listener there is nothing to reject against, so `start()` and every read
      // behind it hang forever with no diagnostic anywhere.
      this.worker.addEventListener("error", (e) => {
        this.failWorker(describeWorkerError(e));
      });
      this.worker.addEventListener("messageerror", (e) => {
        this.failWorker(describeWorkerError(e));
      });
      this.worker.addEventListener(
        "message",
        (e: MessageEvent<ReplicaResponse>) => {
          const reply = e.data;
          const slot = this.pending.get(reply.id);
          if (!slot) return;
          this.pending.delete(reply.id);
          if (reply.ok) slot.resolve(reply.result);
          else slot.reject(new Error(reply.error));
        },
      );

      await this.call({
        type: "open",
        dbPath: this.options.dbPath ?? DEFAULT_DB_PATH,
        directory: this.options.directory ?? DEFAULT_OPFS_DIR,
        identity: this.identityKey(),
      });
      // Prime the cursors from the PERSISTED OPFS cursor (a prior session survives reloads — SAHPool
      // clearOnInit:false). The SDK reads this via getCursor on its first connect so a warm replica
      // resumes the live feed from where it left off instead of re-seeding.
      const persisted = await this.call({ type: "getCursor" });
      if (persisted != null) {
        this.lastSeq = persisted;
        this.acceptedSeq = persisted;
      }
      // Seed only if we have never completed a snapshot (cold OPFS); a warm replica skips straight to
      // live and lets {type:"sync", after:<persisted>} catch it up.
      if (persisted == null) {
        await this.reseed();
      } else if (!this.disposed) {
        this.handlers.onStatus("live");
        this.handlers.onChanged();
      }
      this.subscribe();
    } catch (err) {
      // Surface the REAL cause (the worker preserves err.message → reply.error → here). Without this
      // the failure was swallowed and the UI showed a bare "Replica error" with no diagnosable reason.
      console.error("[replica] boot failed:", err);
      // Release the worker AND the Web Lock. A failed boot used to leak both, which is far worse than
      // it sounds: the lock is origin-wide, so one bad boot wedged EVERY other tab into "secondary"
      // for the life of the document, and the worker kept the OPFS SyncAccessHandles.
      //
      // releaseResources(), NOT close() — close() sets `disposed`, which would silence the very next
      // onStatus("error") line. It is deliberately non-terminal: its job is freeing the lock and the
      // worker for sibling tabs and for the NEXT instance (this one stays one-shot).
      this.releaseResources();
      if (!this.disposed) this.handlers.onStatus("error");
      throw err;
    }
  }

  /**
   * Pull a fresh snapshot, replace the replica (seed), and return the snapshot cursor. Used on first
   * start (cold OPFS) and on every SDK resync (cursor underflow).
   *
   * Bounded and supersedable. A stalled `/snapshot` must not leave `start()` unsettled with every read
   * queued behind the worker's armed SeedReadGate, and a superseded seed must never publish its cursor
   * or post another worker message — a late `seedCommit` from an abandoned seed can otherwise persist
   * a stale cursor over a truncated DB, which the next cold start reads as WARM and goes live over.
   */
  private async reseed(cancel?: AbortSignal): Promise<number> {
    // QUIESCE THE DELTA QUEUE FIRST (CTC-114 review round 9) — before anything can open the worker's
    // seed transaction. A reseed can begin while the queue still holds buffered work (a gap escalating
    // mid-replay is the concrete case), and a live queue across `seedBegin` posts `applyChanges` into
    // that open transaction from its retry timer; the nested `BEGIN` rejects, and repeated failures
    // walk the queue into its apply-failed overflow during an otherwise healthy re-seed. That
    // escalation then buys nothing: its `requestResync()` returns immediately because the transport is
    // already resyncing, so live frames are discarded with no recovery. Everything buffered here is
    // pre-snapshot by definition and the snapshot supersedes it, so dropping it is also correct.
    // Unlatched in the `finally` below, on BOTH arms.
    this.deltas.pause();
    // Supersede any seed already running: abort its fetch, then release the worker-side seed session
    // so its SeedReadGate stops holding reads. `seedAbort` is idempotent worker-side.
    this.seedAbort?.abort();
    if (this.seedAbort) {
      try {
        await this.call({ type: "seedAbort" });
      } catch {
        // Worker already gone or no session open — the fresh seedBegin below is what matters.
      }
    }

    const gen = ++this.seedGeneration;
    const abort = new AbortController();
    // LINK the transport's cancellation to this seed (CTC-114 review round 10, P1).
    //
    // LiveSyncClient bounds the reseed callback with a TOTAL deadline (default 10 min) that this
    // integration never overrode, while the seed's own bounds — network idleness and per-RPC worker
    // deadlines — are both satisfied indefinitely by a legitimately slow ~100 MB snapshot. So the
    // transport could declare the attempt over and RECONNECT while this method kept streaming into
    // the worker: two writers, with the socket resuming from a cursor the seed was still moving.
    // Frames past that point were accepted by the transport and then dropped by our own paused queue,
    // and if the abandoned seed later committed, the socket sat advanced over a hole that the next
    // delta sealed permanently — unrecoverable without another full re-seed.
    //
    // Honouring the signal is what makes the transport's "give up" mean something here. It aborts the
    // fetch and trips `guardedCall`, so the streaming stops and the cleanup `seedAbort` rolls the
    // worker transaction back. `{ once: true }` because this controller is per-attempt.
    if (cancel) {
      if (cancel.aborted) abort.abort();
      else cancel.addEventListener("abort", () => abort.abort(), { once: true });
    }
    this.seedAbort = abort;

    const idleMs =
      this.options.snapshotIdleTimeoutMs ?? DEFAULT_SNAPSHOT_IDLE_TIMEOUT_MS;
    let idleTimer: ReturnType<typeof setTimeout> | null = null;
    const clearIdle = (): void => {
      if (idleTimer !== null) {
        clearTimeout(idleTimer);
        idleTimer = null;
      }
    };
    // Re-armed on every sign of life, so this measures IDLENESS, not elapsed time — a legitimate
    // ~100 MB snapshot streams for minutes and must not be killed for being big.
    const armIdle = (): void => {
      if (idleMs <= 0) return;
      clearIdle();
      idleTimer = setTimeout(() => abort.abort(), idleMs);
    };

    /** A superseded or aborted seed must post ZERO further worker messages — otherwise its batches
     *  interleave with the newer seed's transaction. */
    const guardedCall = (req: ReplicaRequest): Promise<unknown> => {
      if (this.disposed) return Promise.reject(new Error("seed superseded"));
      // A NEWER seed now owns the worker's session. Post NOTHING — not even the cleanup abort, which
      // would roll back the successor's transaction. Its own preamble already aborted this session.
      if (gen !== this.seedGeneration) {
        return Promise.reject(new Error("seed superseded"));
      }
      // Aborted with NO successor (idle timeout, or close() before teardown): refuse everything
      // EXCEPT `seedAbort`. That one MUST still reach the worker — it is what rolls back the open
      // seed transaction and settles the SeedReadGate. Blocking it left the gate armed forever, so
      // every subsequent read hung: precisely the wedge the idle bound exists to prevent, merely
      // relocated from the fetch to the worker. (The boot path masked this — its catch terminates the
      // worker — but a transport-driven resync leaves the worker alive.)
      if (abort.signal.aborted && req.type !== "seedAbort") {
        return Promise.reject(new Error("seed superseded"));
      }
      return this.call(req as never);
    };

    /**
     * The idle bound measures the NETWORK, and only the network (CTC-114 review round 5).
     *
     * It re-armed on body chunks alone, so it stayed armed across every `seedBatch` RPC and — the
     * sharp case — across the final `seedCommit`, which runs after the body is fully consumed and so
     * can never be re-armed by progress. A commit of a ~100 MB snapshot on slow or background-
     * throttled OPFS that crossed the 30s bound aborted a seed whose stream was perfectly healthy;
     * the post-stream re-check then reported a snapshot that had ALREADY COMMITTED as "seed
     * superseded", so the client threw it away and re-fetched. On a big corpus that can fail to
     * converge at all. This restores what DEFAULT_SNAPSHOT_IDLE_TIMEOUT_MS already documents: "this
     * only fires when the server has genuinely stopped sending."
     *
     * Taking the network timer down leaves the WORKER unbounded on its own — which is why `call()`
     * now applies a separate, much longer deadline to every RPC (rounds 6-9). One number could not
     * serve both: 30s of network silence means a dead server, while 30s inside a `seedCommit` can be
     * honest work. See {@link DEFAULT_WORKER_RPC_TIMEOUT_MS}.
     */
    const seedCall = async (req: ReplicaRequest): Promise<unknown> => {
      clearIdle();
      try {
        return await guardedCall(req);
      } finally {
        // Back to waiting on the body — start a FRESH window rather than resuming a partly-spent one.
        armIdle();
      }
    };

    try {
      armIdle();
      const res = await fetch(
        snapshotUrl(this.options.baseUrl, this.options.accountId),
        {
          headers: { accept: "application/x-ndjson" },
          // Same-origin already sends the session cookie, but `baseUrl` may legitimately be an
          // ABSOLUTE same-origin URL, and the default "same-origin" policy is evaluated per request —
          // being explicit costs nothing and removes a class of "works relative, 401s absolute".
          credentials: "include",
          signal: abort.signal,
        },
      );
      armIdle(); // headers arrived
      if (!res.ok) throw new Error(`/snapshot ${res.status}`);
      if (!res.body) throw new Error("/snapshot returned no body stream");
      // Stream the body into the worker in bounded batches — no whole-body buffer anywhere (CTC-132).
      // `req as never`: `this.call` is generic over the request `type` discriminant, so a widened
      // `ReplicaRequest` isn't assignable to its `Extract<…, {type:K}>` parameter; the worker dispatches
      // on `request.type` at runtime, and every request the helper builds is a genuine ReplicaRequest.
      const cursor = await streamSeedIntoWorker(
        seedCall,
        res.body,
        undefined,
        armIdle,
      );

      // Re-check before publishing: between the last chunk and here, a close() or a newer reseed may
      // have superseded this one, and writing these cursors would roll a newer baseline backwards.
      if (this.disposed || abort.signal.aborted || gen !== this.seedGeneration) {
        throw new Error("seed superseded");
      }
      this.lastSeq = cursor;
      // The snapshot IS the new baseline for both cursors — anything accepted before it is superseded.
      this.acceptedSeq = cursor;
      this.handlers.onStatus("live");
      this.handlers.onChanged();
      return cursor;
    } finally {
      clearIdle();
      if (this.seedAbort === abort) this.seedAbort = null;
      // UNLATCH on both arms (round 9), pairing the `pause()` this method opened with. It lives in the
      // `finally` so a FAILED seed also releases the queue — the worker rolled back to the prior
      // complete snapshot and the durable cursor never moved, so live frames may resume against it.
      // Still before reseed() returns, which is what matters: this is the callback the transport
      // awaits BEFORE reopening the socket, so the latch clears before any frame can be delivered —
      // on the server-`{type:"resync"}` path too, which never passes through the overflow handler.
      //
      // GENERATION-GUARDED: a superseded seed must NOT unlatch, or it would clear the pause the
      // successor's preamble just set and let stale frames apply into ITS open transaction.
      if (gen === this.seedGeneration) this.deltas.resume();
    }
  }

  /**
   * Open the live subscription via the SDK `LiveSyncClient` (same-origin ws(s) `<baseUrl>/connect`,
   * cookie auth). Each ChangeFrame is buffered into the DeltaQueue; a resync re-seeds. The SDK owns
   * reconnect/backoff; the worker side is transport-agnostic.
   */
  private subscribe(): void {
    // A consumer can synchronously call close() from inside an onStatus/onChanged callback — and
    // start() invokes those callbacks on the warm-start path and from reseed(), both of which run
    // BEFORE this. Without this guard that close() lands while `this.live` is still null, so it has
    // nothing to stop, and we then open a self-reconnecting socket that re-fetches /snapshot forever
    // from a document the consumer already tore down.
    if (this.disposed) return;
    if (typeof WebSocket === "undefined") return; // SSR/tests — no live feed.

    const base = resolveBase(this.options.baseUrl);
    const baseUrl = `${base.origin}${base.pathname.replace(/\/$/, "")}`;

    this.live = new LiveSyncClient({
      baseUrl,
      // Pass `undefined` through, never "". The DO scopes a cookie-authed socket to the session
      // user's own tenant when no account is named; an explicit account is the tenant-switcher path.
      accountId: this.options.accountId,
      // connectPath defaults to "/connect" in the SDK → "…/connect" under baseUrl.
      auth: { kind: "cookie" }, // NEVER a token in the browser (type system forbids leaking one).
      // Resume from the highest seq we have ACCEPTED, not the highest we have COMMITTED (CTC-318) —
      // otherwise a reconnect re-requests the entire undrained buffer and the DO replays it on top of
      // itself. Falls back to the durable OPFS cursor, which is what a cold start has.
      getCursor: () => Math.max(this.acceptedSeq, this.lastSeq),
      // reseed re-runs the snapshot→OPFS seed and returns the fresh (post-reseed) cursor, which the SDK
      // uses for the next {type:"sync"} — so a resync resumes from the fresh head, never 0.
      // Forward the transport's cancellation signal — see reseed()'s preamble (round 10 P1).
      reseed: (signal) => this.reseed(signal),
      // The transport must stay disconnected until THIS seed has unwound (CTC-114 review round 14).
      //
      // Our cleanup is not instantaneous and not short: aborting mid-seed has to unwind a `seedBatch`
      // or `seedCommit` that is already in flight and then land a `seedAbort`, each bounded by
      // `workerRpcTimeoutMs` (120s by default) — a slow OPFS commit legitimately uses that budget. The
      // transport's own default grace is 250ms, so it would have reconnected long before, into a
      // replica whose delta queue is still PAUSED: replayed frames get refused while the transport's
      // `deliveredSeq` advances over them, and the next frame we do accept carries the durable cursor
      // past the gap. Permanent, and invisible.
      //
      // Derived rather than hard-coded, so raising the RPC deadline cannot silently un-fix this. The
      // margin covers the abort round-trip that follows the RPC being unwound.
      cancelCleanupGraceMs:
        (this.options.workerRpcTimeoutMs ?? DEFAULT_WORKER_RPC_TIMEOUT_MS) +
        CLOSE_GRACE_MS,
      ...(this.options.reseedTimeoutMs === undefined
        ? {}
        : { reseedTimeoutMs: this.options.reseedTimeoutMs }),
      // Buffer + single-flight drain (CTC-318) — NOT a per-frame apply, which had no backpressure and
      // turned each of up to 200k replayed rows into its own clone, RPC, OPFS transaction and full
      // view rebuild.
      onChange: (frame) => {
        this.enqueueDelta(frame);
      },
      onStatus: (status) => {
        if (this.disposed) return;
        // SDK lifecycle → the replica's UI signal. "live" → live; "stopped" is our own teardown
        // (no signal); "error" → error; everything else mid-flight → reconnecting.
        if (status === "live") this.handlers.onStatus("live");
        else if (status === "error") {
          console.error("[replica] SDK live feed entered error state");
          this.handlers.onStatus("error");
        } else if (status !== "stopped") this.handlers.onStatus("reconnecting");
      },
    });
    // start() resolves only on stop(); the open socket keeps things alive between deltas. We never await
    // it in the browser — fire it and stop() on teardown.
    void this.live.start();
  }

  /**
   * Buffer one live SDK ChangeFrame for application (CTC-318 — see delta-queue.ts for why it is
   * buffered rather than applied on arrival). The SDK frame's `row` is OPTIONAL (absent/partial on a
   * delete) — coerce to `{}` so the worker's WireChange always has an object (load-bearing for the
   * delete path, which keys off entityId).
   */
  private enqueueDelta(frame: {
    seq: number;
    entity: string;
    entityId: string;
    op: "upsert" | "delete";
    row?: Record<string, unknown>;
  }): void {
    if (this.disposed) return;
    // Advance the TRANSPORT high-water the instant the frame is accepted — before it is APPLIED — so a
    // reconnect resumes from what we received rather than from what the worker has committed.
    //
    // But only if the queue actually KEPT it (CTC-114 review round 11). This advanced unconditionally,
    // one line above the push that decides, so a frame arriving while the queue was latched — paused
    // for a re-seed, or overflowed — was counted as delivered and then refused. `getCursor()` reports
    // max(acceptedSeq, lastSeq), so the next `{type:"sync", after}` would resume ABOVE it, the gap
    // detector would re-baseline from the same poisoned value and see contiguity, and the next applied
    // batch would carry the durable cursor past the hole. Silent, permanent, re-seed-only recovery.
    //
    // Reachability today rests on `closeSocket()` running before `pause()` on every path that latches
    // while a socket is open — which held when I traced it, but is a proof by call ordering across
    // three modules, and this PR has now had five consecutive rounds where exactly that kind of
    // reasoning was wrong. Following the queue's own decision costs one boolean and needs no proof.
    const kept = this.deltas.push({
      seq: frame.seq,
      entity: frame.entity,
      op: frame.op,
      row: frame.row ?? {},
      entityId: frame.entityId,
    });
    if (kept && frame.seq > this.acceptedSeq) this.acceptedSeq = frame.seq;
  }

  /** Read the issues list view from the local replica (buildIssuesView over OPFS). */
  queryIssues(limit?: number, offset?: number): Promise<IssueView[]> {
    return this.call({ type: "queryIssues", limit, offset });
  }

  /** Read one issue's detail view from the local replica (buildIssueDetail over OPFS). */
  queryIssueDetail(identifier: string): Promise<IssueDetailView | null> {
    return this.call({ type: "queryIssueDetail", identifier });
  }

  /** Read the pull-requests view from the local replica (buildPullsView over OPFS). */
  queryPulls(limit?: number, offset?: number): Promise<PullView[]> {
    return this.call({ type: "queryPulls", limit, offset });
  }

  /**
   * Tear down: stop the SDK live client, reject any in-flight calls, cooperatively close the DB
   * (releasing the OPFS SyncAccessHandles), then terminate the Worker and release the origin lock.
   * Synchronous by contract (React effect cleanups can't await); the close→terminate tail runs
   * detached but BOUNDED (CLOSE_GRACE_MS), so teardown can neither hang nor leak.
   */
  close(): void {
    this.disposed = true;
    this.releaseResources();
  }

  /** Reject every in-flight call with `err` and clear the map. */
  private rejectAllPending(err: Error): void {
    for (const slot of this.pending.values()) slot.reject(err);
    this.pending.clear();
  }

  /**
   * Latch a worker load/runtime failure: stop the transport, surface the cause to every in-flight and
   * every future call, release the worker + lock, and report "error".
   *
   * Without this a worker that dies before installing its message handler leaves `start()` and every
   * read pending forever, holding the origin's Web Lock — the single most likely first-integration
   * failure for a third party, presenting as an unbounded silent hang.
   */
  /**
   * Bound one in-flight worker RPC. On expiry the worker is declared DEAD, not merely slow.
   *
   * Tearing it down is the point (`failWorker` → `releaseResources`): a worker that has stopped
   * answering is holding an open seed transaction with its `SeedReadGate` armed, and nothing else can
   * settle either — the cleanup `seedAbort` is itself a worker message, so it would join the same
   * queue of replies that are never coming. Releasing the worker also frees the origin-wide Web Lock
   * and the OPFS handles, which would otherwise wedge every sibling tab into "secondary".
   *
   * Double-settle is harmless: failWorker() rejects the pending map entry too, so the underlying call
   * rejects moments later against an already-settled promise.
   */
  private withWorkerDeadline<T>(label: string, call: Promise<T>): Promise<T> {
    const ms = this.options.workerRpcTimeoutMs ?? DEFAULT_WORKER_RPC_TIMEOUT_MS;
    if (ms <= 0) return call;
    const seedGated = SEED_GATED_WORKER_RPCS.has(label);
    return new Promise<T>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout>;
      const expire = (): void => {
        // A READ legitimately waits behind the SeedReadGate for as long as a seed runs. Re-arm rather
        // than fire, so the window measures time the worker could actually have answered in. The seed
        // itself is bounded (network idle + its own RPC deadlines), so this cannot re-arm forever.
        if (seedGated && this.seedAbort) {
          timer = setTimeout(expire, ms);
          return;
        }
        const err = new Error(
          `replica worker did not answer '${label}' within ${ms}ms — treating it as wedged`,
        );
        this.failWorker(err);
        reject(err);
      };
      timer = setTimeout(expire, ms);
      call.then(
        (v) => {
          clearTimeout(timer);
          resolve(v);
        },
        (e: unknown) => {
          clearTimeout(timer);
          reject(e instanceof Error ? e : new Error(String(e)));
        },
      );
    });
  }

  private failWorker(err: Error): void {
    if (this.workerError) return; // first cause wins; teardown is idempotent but the message is not
    this.workerError = err;
    console.error("[replica] worker failed:", err);
    this.releaseResources();
    if (!this.disposed) this.handlers.onStatus("error");
  }

  /**
   * Release the worker, the origin lock, the transport and the queue — WITHOUT marking the client
   * disposed.
   *
   * Deliberately non-terminal. `start()` is one-shot, so this is not a "retry on this instance" hook;
   * its job is to make sure a failed boot does not hold the origin-wide Web Lock or the OPFS handles
   * hostage from SIBLING TABS and from the next instance the consumer constructs.
   */
  private releaseResources(): void {
    this.live?.stop();
    this.live = null;
    // Abort an in-flight seed so its fetch stops draining the ~100 MB body server-side.
    this.seedAbort?.abort();
    this.seedAbort = null;
    // Drop any deltas still buffered — each retains a full parsed row (issues carry the provider's
    // `raw` JSON), so a queue left behind on teardown would be retained until the client is collected.
    this.deltas.stop();
    this.rejectAllPending(this.workerError ?? new Error("replica client closed"));

    const worker = this.worker;
    this.worker = null;
    const lock = this.lock;
    this.lock = null;
    if (!worker) {
      lock?.release();
      return;
    }
    // Cooperative close first (frees the SAHPool handles), terminate as the bounded backstop. The lock
    // is held until the worker is gone so a sibling tab's boot can't race the not-yet-released pool;
    // browser-lock's retry covers the residual gap on abrupt teardown.
    let settled = false;
    const finish = (): void => {
      if (settled) return;
      settled = true;
      worker.terminate();
      lock?.release();
    };
    try {
      const id = this.nextId++;
      worker.addEventListener("message", (e: MessageEvent<ReplicaResponse>) => {
        if (e.data.id === id) finish();
      });
      const envelope: Envelope = { id, request: { type: "close" } };
      worker.postMessage(envelope);
    } catch {
      finish();
      return;
    }
    setTimeout(finish, CLOSE_GRACE_MS);
  }
}
