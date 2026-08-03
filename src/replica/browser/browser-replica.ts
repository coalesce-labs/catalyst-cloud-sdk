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
    this.handlers = handlers;
    this.options = options;
    this.deltas = new DeltaQueue({
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
          void live.requestResync().finally(() => this.deltas.resume());
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

  /** Send a typed request to the Worker and resolve with its result (cast by the ResultMap entry). */
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
    return new Promise<ResultMap[K]>((resolve, reject) => {
      this.pending.set(id, {
        resolve: (v) => resolve(v as ResultMap[K]),
        reject,
      });
      worker.postMessage(envelope);
    });
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
      const lock = await acquireReplicaLock(
        `catalyst-replica:${this.options.directory ?? DEFAULT_OPFS_DIR}`,
      );
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
  private async reseed(): Promise<number> {
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
        guardedCall,
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
      // Unlatch the queue HERE, not only in the overflow handler's `.finally`. reseed() is also the
      // callback the transport awaits BEFORE reopening the socket, so resuming at this point clears
      // the latch before any frame can be delivered — on the server-`{type:"resync"}` path too, which
      // never passes through the overflow handler at all.
      this.deltas.resume();
      this.handlers.onStatus("live");
      this.handlers.onChanged();
      return cursor;
    } finally {
      clearIdle();
      if (this.seedAbort === abort) this.seedAbort = null;
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
      reseed: () => this.reseed(),
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
    // Advance the TRANSPORT high-water the instant the frame is accepted — before it is applied — so a
    // reconnect resumes from what we received rather than from what the worker has committed.
    if (frame.seq > this.acceptedSeq) this.acceptedSeq = frame.seq;
    this.deltas.push({
      seq: frame.seq,
      entity: frame.entity,
      op: frame.op,
      row: frame.row ?? {},
      entityId: frame.entityId,
    });
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
