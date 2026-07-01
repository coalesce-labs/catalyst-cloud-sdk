// @catalyst-cloud/sdk/node — CatalystReplica: host-sync's writer + read seam behind ONE import.
//
// Composed from the published @catalyst-cloud/{schema,read-model,replicate} packages + the SDK's own
// LiveSyncClient. It is the drop-in replacement for the hand-rolled writer loop in apps/host-sync
// (apply.ts + sync-client.ts + live-client.ts + read-adapter.ts), now engine-generic:
//
//   start()  = open DB (injected engine, default bun:sqlite/node:sqlite auto-detect)
//            → applyMigrations(MIRROR_MIGRATIONS) + sync_meta DDL
//            → open LiveSyncClient: if cursor==null it STREAM-seeds /snapshot (the injected reseed),
//              then replays {type:"sync", after:cursor}, applyDelta + cursor per frame; {type:"resync"}
//              re-seeds. RESOLVES WHEN FIRST 'live' (caught-up + ready to read); background sync
//              continues until close(). This is the deliberate fork from the raw transport's
//              resolve-on-stop.
//   reads    = SYNCHRONOUS over the read-model SqlExecutor (node/bun sqlite is sync). The
//              `buildIssuesView(replica.sql, …)` calls are LITERALLY unchanged.
//
// Reads, writes, migrations, cursor and wire semantics are byte-identical to host-sync because this
// runs the SAME @catalyst-cloud/read-model builders over the SAME @catalyst-cloud/replicate write path
// and the SAME @catalyst-cloud/schema MIRROR_MIGRATIONS.

import { applyMigrations, MIRROR_MIGRATIONS, type MigrationDb } from "@catalyst-cloud/schema";
import {
  applyDelta,
  truncateReplica,
  getCursor,
  setCursor,
  type ReplicaWriteDb,
} from "@catalyst-cloud/replicate";
import {
  buildIssuesView,
  buildIssueDetail,
  buildPullsView,
  buildProjectsView,
  buildProjectDetail,
  buildInitiativesView,
  buildInitiativeDetail,
  type SqlExecutor,
  type SqlValue,
  type IssueView,
  type IssueDetailView,
  type PullView,
  type ProjectView,
  type ProjectDetailView,
  type InitiativeView,
  type InitiativeDetailView,
} from "@catalyst-cloud/read-model";

import {
  LiveSyncClient,
  stripTrailingSlashes,
  type AuthStrategy,
  type LiveSyncStatus,
  type LogLevel,
  type WebSocketFactory,
} from "../live-sync-client.js";
import type { ChangeFrame } from "../types.js";
import {
  autoDetectEngine,
  autoDetectReadonlyEngine,
  type EngineFactory,
  type ReplicaEngine,
} from "./engine.js";
import {
  claimWriterLock,
  type WriterGuardOptions,
  type WriterLockHandle,
} from "./writer-lock.js";
import {
  NOOP_TELEMETRY,
  createTelemetry,
  CATALYST_ATTR,
  REPLICA_METRIC,
  REPLICA_LOG,
  REPLICA_SPAN,
  REPLICA_STATUS_CODE,
  DEFAULT_SCOPE_NAME,
  entitySource,
  type ReplicaApplyResult,
  type Telemetry,
  type TelemetryConfig,
  type Counter,
  type Histogram,
  type GaugeRegistration,
} from "../otel.js";

/** Host-sync bookkeeping table (NOT part of the DO mirror schema, so not in MIRROR_MIGRATIONS): the
 *  change-feed cursor, so a restart resumes from the live `{type:"sync", after}` replay (or /changes)
 *  instead of a full /snapshot every boot. */
const SYNC_META_DDL = `CREATE TABLE IF NOT EXISTS sync_meta (
  key TEXT PRIMARY KEY, value TEXT
);`;

/** Rows per streamed seed transaction — bounds memory + fsync so a large tenant never OOMs the node/bun
 *  process the way host-sync's buffered `await res.text()` + `split("\n")` snapshot would. */
const SEED_BATCH_ROWS = 1000;

export interface CatalystReplicaOptions {
  /** http(s) origin incl. any path prefix (…/api/v1); the scheme is swapped to ws(s) for /connect. */
  baseUrl: string;
  /** Tenant id = mirror name → `?account=` on every feed request. */
  account: string;
  /** How to authorize: {kind:'token',token} (host bearer rides /connect as ?token= and /snapshot as
   *  Authorization) | {kind:'cookie'} (same-origin session cookie). */
  auth: AuthStrategy;
  /** File path or ':memory:'. */
  dbPath: string;
  /** INJECTED sqlite engine, or a factory over dbPath. Default = auto-detect (bun:sqlite, else node:sqlite). */
  engine?: ReplicaEngine | EngineFactory;
  /** The connect route. Default '/connect'. */
  connectPath?: string;
  /** Snapshot/changes fetch. Default global fetch. */
  fetchImpl?: typeof fetch;
  /** Fired after each applied delta (live frame OR a completed seed) — a refetch hook. */
  onChange?: () => void;
  /** Connection lifecycle, for UI/logging. */
  onStatus?: (status: LiveSyncStatus) => void;
  /** Base reconnect backoff in ms. Default 1000. */
  backoffMs?: number;
  /** Reconnect backoff ceiling in ms. Default 30_000. */
  maxBackoffMs?: number;
  /** Injectable WebSocket factory (tests). Defaults to the runtime global WebSocket. */
  wsFactory?: WebSocketFactory;
  /** Optional structured logger; defaults to console. */
  log?: (level: LogLevel, msg: string, extra?: unknown) => void;
  /**
   * Single-writer guard (a sidecar `dbPath + '.writer.lock'`). On `start()` the writer best-effort
   * claims sole ownership of the file; a second LIVE writer on the same path makes `start()` throw a
   * clear error (single-writer/many-reader, ADR-0008). Advisory, not a hard OS lock; a no-op for
   * `:memory:`. Default: enabled. Set `{ disabled: true }` to skip, `{ override: true }` to steal.
   */
  writerGuard?: WriterGuardOptions;
  /**
   * If set, `start()` rejects with a clear error if it doesn't reach `live` within this many ms (e.g.
   * a wedged cold /snapshot), AFTER cleaning up — so a supervisor can fail-fast/restart instead of
   * hanging. Off by default.
   */
  startTimeoutMs?: number;
  /**
   * Opt-in OpenTelemetry (CTC-138). `false`/absent = OFF (strictly zero overhead; no
   * `@opentelemetry/api` import is even attempted). `true` = ON with the default `@catalyst-cloud/sdk`
   * instrumentation scope; an object overrides the tracer/meter name. When ON the replica emits:
   *   • SPANS  — `catalyst.replica.seed` (snapshot pull+apply), `catalyst.replica.apply_batch` (per
   *     seed batch), plus the transport's `catalyst.replica.resync` + `catalyst.replica.reconnect`.
   *   • METRICS — observable gauges `catalyst.replica.cursor`, `catalyst.replica.lag_seq` (only when
   *     the mirror head_seq is known from the `x-catalyst-head-seq` /snapshot response header),
   *     `catalyst.replica.freshness_ms`, `catalyst.replica.status`; a `catalyst.replica.applied`
   *     counter (per live frame) and a `catalyst.replica.apply_batch_rows` histogram.
   * All carry `catalyst.tenant` (+ `catalyst.source` where a single entity is involved). `@opentelemetry/api`
   * is an OPTIONAL peer dep: enabling this without it installed logs once and runs no-op (never throws).
   * Spans/metrics route through the consumer's GLOBAL TracerProvider/MeterProvider so their Resource
   * (service.name) flows through.
   */
  telemetry?: TelemetryConfig;
}

/**
 * Options for a READ-ONLY reader ({@link CatalystReplica.openReadOnly}). A reader needs only the file
 * path (+ optional injected engine / logger): no `baseUrl`, `account`, or `auth`, because it opens NO
 * socket and pulls NO snapshot.
 */
export interface CatalystReplicaReadOnlyOptions {
  /** File path to an EXISTING writer-seeded replica. `:memory:` makes no sense for a reader (a fresh,
   *  empty, per-connection DB) and is rejected. */
  dbPath: string;
  /** INJECTED read-only sqlite engine or a factory over dbPath. Default = auto-detect READ-ONLY
   *  (bun:sqlite/node:sqlite opened `{ readonly: true }`). Pass a writable engine at your own risk. */
  engine?: ReplicaEngine | EngineFactory;
  /** Optional structured logger; defaults to console. */
  log?: (level: LogLevel, msg: string, extra?: unknown) => void;
}

/** A snapshot NDJSON line: a data line `{accountId, entity, op, row}` or the final `{accountId, cursor}`. */
interface SnapshotLine {
  entity?: string;
  op?: "upsert" | "delete";
  row?: Record<string, unknown>;
  cursor?: number;
}

export class CatalystReplica {
  private readonly opts: CatalystReplicaOptions;
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly log: NonNullable<CatalystReplicaOptions["log"]>;

  private engine: ReplicaEngine | null = null;
  private writeDb: ReplicaWriteDb<unknown> | null = null;
  private sqlExecutor: SqlExecutor | null = null;
  private client: LiveSyncClient | null = null;

  /** In-memory high-water of the last seq persisted, so each frame avoids re-reading the cursor row. */
  private highWater = 0;
  private _status: LiveSyncStatus = "connecting";
  private started = false;
  private closed = false;

  /** READ-ONLY mode (opened via {@link CatalystReplica.openReadOnly}): no migrations, no seed, no
   *  socket; `start()` is a no-op and the write path is unreachable. */
  private readonlyMode = false;
  /** The claimed single-writer lock (writers only; null for readers / `:memory:` / disabled). */
  private writerLock: WriterLockHandle | null = null;

  /** Resolved on the first 'live' status (start() = caught-up + ready to read); rejected on close /
   *  an initial seed failure. */
  private liveResolve: (() => void) | null = null;
  private liveReject: ((err: unknown) => void) | null = null;

  // ── OpenTelemetry (CTC-138, opt-in via opts.telemetry) ──────────────────────────────────────────
  /** Resolved once in start(); the no-op until then so any early metric/span call is safe. */
  private telemetry: Telemetry = NOOP_TELEMETRY;
  /** Observable-gauge unregister handles, torn down in close() so a closed replica stops being scraped. */
  private metricRegs: GaugeRegistration[] = [];
  /** Per-live-frame applied counter + per-seed-batch row histogram (no-op until telemetry is wired). */
  private appliedCounter: Counter = NOOP_TELEMETRY.counter(REPLICA_METRIC.applied);
  private applyBatchHistogram: Histogram = NOOP_TELEMETRY.histogram(REPLICA_METRIC.applyBatchRows);
  /** Last mirror head_seq + server clock observed from the `x-catalyst-head-seq` /snapshot headers
   *  (CTC-137), or null until seen. Drives the `lag_seq` gauge (emitted ONLY when non-null). */
  private lastHeadSeq: number | null = null;
  private lastServerTimeMs: number | null = null;
  /** Wall-clock ms of the last applied delta (live frame or seed), or null. Drives `freshness_ms`. */
  private lastAppliedAtMs: number | null = null;

  constructor(opts: CatalystReplicaOptions) {
    this.opts = opts;
    this.baseUrl = stripTrailingSlashes(opts.baseUrl);
    this.fetchImpl = opts.fetchImpl ?? fetch;
    this.log =
      opts.log ??
      ((lvl, msg, extra) =>
        console[lvl === "error" ? "error" : "log"](`[catalyst-replica] ${msg}`, extra ?? ""));
  }

  /**
   * Open a CatalystReplica as a READ-ONLY READER over a file another process owns as the WRITER
   * (single-writer/many-reader, ADR-0008). The sqlite handle is opened read-only (`{ readonly: true }`
   * + `busy_timeout`), so the reader runs NO migrations, pulls NO /snapshot, opens NO LiveSyncClient,
   * and the write path is unreachable — it only serves the synchronous `build*View` reads, `.sql`,
   * `.handle`, and `.cursor` off whatever the writer has already persisted. There is no live tailing:
   * the reader sees the file's committed state at read time (a fresh `issues()` re-queries, so it
   * reflects the writer's latest committed rows).
   *
   * Async because the default engine dynamic-imports its sqlite driver; once it resolves the replica
   * is already open, so `start()` on the returned reader is a no-op.
   */
  static async openReadOnly(opts: CatalystReplicaReadOnlyOptions): Promise<CatalystReplica> {
    if (!opts.dbPath || opts.dbPath === ":memory:" || opts.dbPath.startsWith("file::memory:")) {
      throw new Error(
        "CatalystReplica.openReadOnly: a reader needs a FILE path to a writer-seeded replica; " +
          "':memory:' is a fresh per-connection DB with nothing to read.",
      );
    }
    // Reuse the writer constructor with placeholders for the connect-only fields (never touched in
    // read-only mode), then open the read-only engine eagerly.
    const replica = new CatalystReplica({
      baseUrl: "readonly://local",
      account: "",
      auth: { kind: "cookie" },
      dbPath: opts.dbPath,
      engine: opts.engine,
      log: opts.log,
    });
    await replica.openReadOnlyInternal(opts);
    return replica;
  }

  /**
   * Open + migrate the replica, then open the live socket. Resolves when the replica is caught-up and
   * ready to read (first 'live'); background sync continues until close(). A cold tenant stream-seeds
   * /snapshot first (via the injected reseed inside LiveSyncClient.start), so 'live' implies
   * seed-complete. NOTE: there is no built-in timeout — a stalled /snapshot or unreachable host can
   * delay 'live'; drive progress via onStatus.
   */
  async start(): Promise<void> {
    // A reader is already open (openReadOnly opened the engine); start() is an inert no-op.
    if (this.readonlyMode) return;
    if (this.closed) throw new Error("CatalystReplica: start() after close()");
    if (this.started) throw new Error("CatalystReplica: start() already called");
    this.started = true;

    // Single-writer guard: claim sole ownership of the file BEFORE opening it, so a second concurrent
    // writer rejects here instead of racing the cursor/seed. A no-op for ':memory:' or when disabled.
    this.writerLock = claimWriterLock(this.opts.dbPath, this.opts.writerGuard ?? {}, this.log);

    const engine = await this.resolveEngine();
    this.engine = engine;
    this.writeDb = {
      run: (sql, ...bindings) => engine.run(sql, ...bindings),
      get: (sql, ...bindings) => engine.get(sql, ...bindings),
    };
    this.sqlExecutor = {
      exec: (query: string, ...bindings: SqlValue[]) => ({
        toArray: () =>
          engine.all(query, ...bindings.map(engine.toBindable)) as Record<string, SqlValue>[],
      }),
    };

    // Migrate via the ~3-line MigrationDb adapter, then add the host-only cursor table.
    const migrationDb: MigrationDb = {
      exec: (sql) => engine.exec(sql),
      query: (sql) => engine.all(sql),
    };
    applyMigrations(migrationDb, MIRROR_MIGRATIONS);
    engine.exec(SYNC_META_DDL);

    this.highWater = getCursor(this.writeDb) ?? 0;

    // Resolve the OTel seam ONCE (guarded dynamic import; no-op when off / api absent) BEFORE wiring
    // the transport, then cache the tracer/meter-backed instruments on the instance so the synchronous
    // applyFrame/flush hot paths never go async, and SHARE the resolved instance with the transport so
    // the seed span nests under the resync span and both layers use one tracer/meter.
    this.telemetry = await createTelemetry(this.opts.telemetry, {
      tracerName: DEFAULT_SCOPE_NAME,
      meterName: DEFAULT_SCOPE_NAME,
    });
    this.registerMetrics();

    this.client = new LiveSyncClient({
      baseUrl: this.baseUrl,
      accountId: this.opts.account,
      connectPath: this.opts.connectPath,
      auth: this.opts.auth,
      reseed: () => this.seedFromSnapshot(),
      getCursor: () => getCursor(this.writeDb as ReplicaWriteDb<unknown>),
      onChange: (frame) => this.applyFrame(frame),
      onStatus: (status) => this.handleStatus(status),
      backoffMs: this.opts.backoffMs,
      maxBackoffMs: this.opts.maxBackoffMs,
      wsFactory: this.opts.wsFactory,
      log: this.log,
      telemetry: this.telemetry,
    });

    const livePromise = new Promise<void>((resolve, reject) => {
      this.liveResolve = resolve;
      this.liveReject = reject;
      // The transport's start() resolves only on stop() ("runs forever"); we fork on first 'live'.
      // Run it in the background and surface an initial seed/connect failure as a start() rejection.
      void this.client!.start().catch((err) => {
        const rej = this.liveReject;
        this.clearLiveDeferred();
        rej?.(err);
      });
    });

    const timeoutMs = this.opts.startTimeoutMs;
    if (timeoutMs == null) return livePromise;

    // Race the start sequence (seed + connect-to-live) against a deadline. On timeout, tear down the
    // SAME way close() does (stop the socket, release the writer lock, close the engine) so nothing
    // leaks, then reject — a supervisor fails fast instead of hanging on a wedged /snapshot. Reaching
    // 'live' first cancels the (unref'd) timer, so there's no late rejection and the timer never holds
    // the event loop open on its own.
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        void this.close().finally(() => {
          reject(
            new Error(
              `CatalystReplica.start() did not reach 'live' within ${timeoutMs}ms ` +
                `(account=${this.opts.account}, dbPath=${this.opts.dbPath})`,
            ),
          );
        });
      }, timeoutMs);
      (timer as unknown as { unref?: () => void }).unref?.();

      livePromise.then(
        () => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          resolve();
        },
        (err) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          reject(err);
        },
      );
    });
  }

  /** Stop the socket, release the writer lock, close the DB. Idempotent. Rejects a still-pending
   *  start(). On a reader: no socket/lock to release — just closes the read-only handle. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.client?.stop();
    const rej = this.liveReject;
    this.clearLiveDeferred();
    rej?.(new Error("CatalystReplica: closed before first 'live'"));
    try {
      this.writerLock?.release();
    } catch (err) {
      this.log("warn", "writer-lock release threw", err);
    }
    this.writerLock = null;
    // Unregister the observable-gauge callbacks so a closed replica stops being scraped + can be GC'd.
    for (const reg of this.metricRegs) {
      try {
        reg.remove();
      } catch (err) {
        this.log("warn", "metric unregister threw", err);
      }
    }
    this.metricRegs = [];
    try {
      this.engine?.close();
    } catch (err) {
      this.log("warn", "engine close threw", err);
    }
  }

  // ── Reads ───────────────────────────────────────────────────────────────────────────────────────

  /** The read-model SqlExecutor over the replica. `buildIssuesView(replica.sql, …)` is unchanged. */
  get sql(): SqlExecutor {
    if (!this.sqlExecutor) throw new Error("CatalystReplica: call start() before reading");
    return this.sqlExecutor;
  }

  issues(opts?: { limit?: number; offset?: number }): IssueView[] {
    return buildIssuesView(this.sql, opts?.limit, opts?.offset);
  }
  issue(identifier: string): IssueDetailView | null {
    return buildIssueDetail(this.sql, identifier);
  }
  pulls(opts?: { limit?: number; offset?: number }): PullView[] {
    return buildPullsView(this.sql, opts?.limit, opts?.offset);
  }
  projects(opts?: { limit?: number; offset?: number }): ProjectView[] {
    return buildProjectsView(this.sql, opts?.limit, opts?.offset);
  }
  project(id: string): ProjectDetailView | null {
    return buildProjectDetail(this.sql, id);
  }
  initiatives(opts?: { limit?: number; offset?: number }): InitiativeView[] {
    return buildInitiativesView(this.sql, opts?.limit, opts?.offset);
  }
  initiative(id: string): InitiativeDetailView | null {
    return buildInitiativeDetail(this.sql, id);
  }

  /** The durable change-feed cursor (sync_meta), or null before the first seed. */
  get cursor(): number | null {
    return this.writeDb ? getCursor(this.writeDb) : null;
  }

  /** The connection lifecycle status. */
  get status(): LiveSyncStatus {
    return this._status;
  }

  /** The raw driver Database the SDK owns — for `drizzle(replica.handle, { schema: mirrorSchema })`. */
  get handle(): unknown {
    return (this.engine as Partial<{ handle: unknown }> | null)?.handle;
  }

  /** Ms since the last applied delta (live frame or seed), or null before the first apply. The basis
   *  of the `catalyst.replica.freshness_ms` gauge — "how stale is the local copy right now". */
  get freshnessMs(): number | null {
    return this.lastAppliedAtMs == null ? null : Date.now() - this.lastAppliedAtMs;
  }

  /** The last mirror head_seq read from the `x-catalyst-head-seq` /snapshot response header (CTC-137),
   *  or null if the header has never been observed. `headSeq − cursor` is the replica's lag in seqs. */
  get headSeq(): number | null {
    return this.lastHeadSeq;
  }

  /** The mirror server clock (ms) read from the `x-catalyst-server-time-ms` /snapshot response header
   *  (CTC-137), or null if never observed — for a consumer that wants to estimate clock skew. */
  get serverTimeMs(): number | null {
    return this.lastServerTimeMs;
  }

  // ── Internals ─────────────────────────────────────────────────────────────────────────────────

  private async resolveEngine(): Promise<ReplicaEngine> {
    const e = this.opts.engine;
    if (e === undefined) return autoDetectEngine(this.opts.dbPath);
    if (typeof e === "function") return e(this.opts.dbPath);
    return e;
  }

  /**
   * Open the read-only engine and wire ONLY the read ports — no migrations, no `sync_meta` DDL, no
   * snapshot, no LiveSyncClient. The write-facing `writeDb` is a read-capable shim whose `run` throws,
   * so reads (`sql`/`build*View`/`cursor`) work but any accidental write path is unreachable AND the
   * underlying sqlite handle is itself read-only.
   */
  private async openReadOnlyInternal(opts: CatalystReplicaReadOnlyOptions): Promise<void> {
    this.readonlyMode = true;
    this.started = true; // already open → start() is a no-op

    const e = opts.engine;
    const engine =
      e === undefined
        ? await autoDetectReadonlyEngine(opts.dbPath)
        : typeof e === "function"
          ? await e(opts.dbPath)
          : e;
    this.engine = engine;

    this.sqlExecutor = {
      exec: (query: string, ...bindings: SqlValue[]) => ({
        toArray: () =>
          engine.all(query, ...bindings.map(engine.toBindable)) as Record<string, SqlValue>[],
      }),
    };
    // Read-capable cursor access; `run` is poisoned so the write path can never fire on a reader.
    this.writeDb = {
      get: (sql, ...bindings) => engine.get(sql, ...bindings),
      run: () => {
        throw new Error("CatalystReplica: this replica is READ-ONLY (opened via openReadOnly)");
      },
    };
    this._status = "live"; // a reader is "live" the moment it is open (it has no socket)
  }

  private handleStatus(status: LiveSyncStatus): void {
    this._status = status;
    if (status === "live" && this.liveResolve) {
      const res = this.liveResolve;
      this.clearLiveDeferred();
      res();
    }
    try {
      this.opts.onStatus?.(status);
    } catch (err) {
      this.log("warn", "onStatus handler threw", err);
    }
  }

  private clearLiveDeferred(): void {
    this.liveResolve = null;
    this.liveReject = null;
  }

  /**
   * Register the four `catalyst.replica.*` observable gauges (whose async callbacks read live instance
   * state) + create the per-frame counter and per-batch histogram. A no-op telemetry returns no-op
   * instruments + empty registrations, so this is safe (and free) when telemetry is OFF. Idempotent
   * per start(); the registrations are torn down in close().
   */
  private registerMetrics(): void {
    if (!this.telemetry.enabled) return; // skip the gauge churn entirely on the no-op path
    const tenant = (): Record<string, string> => ({ [CATALYST_ATTR.tenant]: this.opts.account });

    this.metricRegs.push(
      this.telemetry.observableGauge(
        REPLICA_METRIC.cursor,
        { description: "Durable change-feed cursor (last applied seq).", unit: "{seq}" },
        (result) => result.observe(this.highWater, tenant()),
      ),
      // lag_seq is emitted ONLY when the mirror head_seq is known (CTC-137 header observed at seed).
      this.telemetry.observableGauge(
        REPLICA_METRIC.lagSeq,
        { description: "Mirror lag in seqs (head_seq − cursor); only when head_seq is known.", unit: "{seq}" },
        (result) => {
          if (this.lastHeadSeq != null) result.observe(this.lastHeadSeq - this.highWater, tenant());
        },
      ),
      this.telemetry.observableGauge(
        REPLICA_METRIC.freshnessMs,
        { description: "Ms since the last applied delta (live frame or seed).", unit: "ms" },
        (result) => {
          if (this.lastAppliedAtMs != null) result.observe(Date.now() - this.lastAppliedAtMs, tenant());
        },
      ),
      this.telemetry.observableGauge(
        REPLICA_METRIC.status,
        { description: "Connection lifecycle status as a numeric code (see REPLICA_STATUS_CODE)." },
        (result) =>
          result.observe(REPLICA_STATUS_CODE[this._status], {
            [CATALYST_ATTR.tenant]: this.opts.account,
            [CATALYST_ATTR.status]: this._status,
          }),
      ),
    );

    this.appliedCounter = this.telemetry.counter(REPLICA_METRIC.applied, {
      description: "Live change frames applied to the replica.",
      unit: "{frame}",
    });
    this.applyBatchHistogram = this.telemetry.histogram(REPLICA_METRIC.applyBatchRows, {
      description: "Rows applied per seed batch transaction.",
      unit: "{row}",
    });
  }

  /** Land one delta + advance the durable cursor atomically (a crash can't skip a seq), then signal. */
  private applyFrame(frame: ChangeFrame): void {
    const engine = this.engine;
    const writeDb = this.writeDb;
    if (!engine || !writeDb) return;
    try {
      // `applyDelta` returns whether the row was actually written (`true`) or dropped by the replica's
      // stale-guard (`false`) — the applied-vs-skipped signal (CTL-1402).
      let written = false;
      engine.transaction(() => {
        written = applyDelta(
          writeDb,
          { entity: frame.entity, op: frame.op, row: frame.row ?? {}, entityId: frame.entityId },
          engine.toBindable,
        );
        // Advance to the seq we SAW (not just applied), so a stale-but-newer-seq delta still moves the
        // cursor forward and a reconnect doesn't re-request it.
        if (frame.seq > this.highWater) setCursor(writeDb, frame.seq, engine.toBindable);
      });
      if (frame.seq > this.highWater) this.highWater = frame.seq;
      // Freshness stamp. The cursor + freshness_ms gauges read this.highWater / this.lastAppliedAtMs on
      // the next scrape.
      this.lastAppliedAtMs = Date.now();
      this.recordApplyResult(written ? "applied" : "skipped", frame);
      try {
        this.opts.onChange?.();
      } catch (err) {
        this.log("warn", "onChange handler threw", err);
      }
    } catch (err) {
      this.recordApplyResult("failed", frame, err);
    }
  }

  /**
   * CTL-1402: record ONE apply outcome per live frame. Two coherent emit paths, both carrying
   * `result: applied|skipped|failed`:
   *
   *  1. A structured `catalyst.replica.apply` LOG line (via the consumer's `log` callback) — the
   *     fleet's primary signal, since it runs NO in-process MeterProvider: the daemon serializes
   *     `extra` into fields the Loki→metrics connector materializes. `seq` is the join key to the
   *     mirror's `ingest.committed.headSeq`; the failure branch carries the untruncated `err_message`
   *     the #127 drift needs. This REPLACES the old string-interpolated `"apply failed for … seq="`
   *     line — one emit path, no double-count (the interim in-repo bridge retires on the SDK bump).
   *  2. A `result`-tagged bump of the `catalyst.replica.applied` OTLP counter — durable for when a
   *     MeterProvider is adopted; a no-op when telemetry is off.
   *
   * Cardinality: only `source` + `result` are counter labels (both low-card); `seq` / `entity` /
   * `err_message` ride the log line as VALUES, never labels.
   */
  private recordApplyResult(result: ReplicaApplyResult, frame: ChangeFrame, err?: unknown): void {
    const source = entitySource(frame.entity);
    this.appliedCounter.add(1, {
      [CATALYST_ATTR.tenant]: this.opts.account,
      [CATALYST_ATTR.source]: source,
      [CATALYST_ATTR.result]: result,
    });
    const errMessage =
      err instanceof Error ? err.message : err !== undefined ? String(err) : undefined;
    this.log(result === "failed" ? "error" : "info", REPLICA_LOG.apply, {
      result,
      seq: frame.seq,
      entity: frame.entity,
      source,
      ...(errMessage !== undefined ? { err_message: errMessage } : {}),
    });
  }

  /**
   * Stream-seed the replica from /snapshot and return the fresh cursor. Streams `response.body` as
   * NDJSON (chunked, batched transactions) so a large tenant never materializes the whole snapshot in
   * memory — the OOM fix vs host-sync's buffered seed. The cursor row is DELETED up front so an
   * interrupted seed self-heals (getCursor → null → re-seed on the next start), preserving host-sync's
   * atomic truncate+apply+setCursor safety without holding one giant transaction.
   */
  private async seedFromSnapshot(): Promise<number> {
    // The whole seed (fetch + truncate + batched apply + setCursor) runs inside ONE active span so it
    // nests under the resync span when triggered by a {type:"resync"} frame, and stands alone on a cold
    // start. Active so the per-batch apply spans auto-parent under it.
    return this.telemetry.withActiveSpan(
      REPLICA_SPAN.seed,
      { [CATALYST_ATTR.tenant]: this.opts.account },
      async (span) => {
        const engine = this.engine as ReplicaEngine;
        const writeDb = this.writeDb as ReplicaWriteDb<unknown>;

        const url = `${this.baseUrl}/snapshot?account=${encodeURIComponent(this.opts.account)}`;
        const res = await this.fetchImpl(url, { headers: this.feedHeaders() });
        if (!res.ok) throw new Error(`/snapshot ${res.status}`);

        // CTC-137: the /snapshot response is the SINGLE HTTP Response the SDK reads, so it is the one
        // place to learn the mirror's head_seq (+ server clock). Stashed for the lag_seq gauge, which
        // only emits while head_seq is known. Guarded: test fetch stand-ins return no `headers`.
        this.readHeadSeqHeaders(res);

        // Invalidate the cursor BEFORE truncating so a crash mid-seed re-seeds rather than going live over
        // an empty replica from a stale cursor.
        engine.run("DELETE FROM sync_meta WHERE key = 'cursor'");
        engine.transaction(() => truncateReplica(writeDb));

        let cursor = 0;
        let batch: SnapshotLine[] = [];
        const flush = (): void => {
          if (batch.length === 0) return;
          const rows = batch;
          batch = [];
          // The literal batched-apply seam: one transaction per ≤SEED_BATCH_ROWS rows → its own span.
          this.telemetry.withActiveSpanSync(
            REPLICA_SPAN.applyBatch,
            { [CATALYST_ATTR.tenant]: this.opts.account, [CATALYST_ATTR.batchRows]: rows.length },
            () => {
              engine.transaction(() => {
                for (const rec of rows) {
                  if (rec.entity === undefined) continue;
                  applyDelta(
                    writeDb,
                    { entity: rec.entity, op: rec.op ?? "upsert", row: rec.row ?? {} },
                    engine.toBindable,
                  );
                }
              });
            },
          );
          this.applyBatchHistogram.record(rows.length, { [CATALYST_ATTR.tenant]: this.opts.account });
        };

        let rowCount = 0;
        for await (const line of iterateNdjson(res)) {
          const rec = JSON.parse(line) as SnapshotLine;
          if (typeof rec.cursor === "number") {
            cursor = rec.cursor; // the FINAL line carries the cursor
            continue;
          }
          batch.push(rec);
          rowCount++;
          if (batch.length >= SEED_BATCH_ROWS) flush();
        }
        flush();

        engine.transaction(() => setCursor(writeDb, cursor, engine.toBindable));
        this.highWater = cursor;
        this.lastAppliedAtMs = Date.now(); // a completed seed refreshes freshness_ms
        span.setAttribute(CATALYST_ATTR.rowCount, rowCount);
        span.setAttribute(CATALYST_ATTR.cursor, cursor);
        this.log("info", `snapshot seeded (${rowCount} rows), cursor=${cursor}`);
        try {
          this.opts.onChange?.();
        } catch (err) {
          this.log("warn", "onChange handler threw", err);
        }
        return cursor;
      },
    );
  }

  /** Read CTC-137's `x-catalyst-head-seq` (+ `x-catalyst-server-time-ms`) off the /snapshot response,
   *  if present. Guarded so a header-less test fetch stand-in is a no-op. */
  private readHeadSeqHeaders(res: Response): void {
    const headers = (res as { headers?: { get?(name: string): string | null } }).headers;
    const get = headers?.get;
    if (typeof get !== "function") return;
    const headSeq = get.call(headers, "x-catalyst-head-seq");
    if (headSeq != null && headSeq !== "") {
      const n = Number(headSeq);
      if (Number.isFinite(n)) this.lastHeadSeq = n;
    }
    const serverTime = get.call(headers, "x-catalyst-server-time-ms");
    if (serverTime != null && serverTime !== "") {
      const n = Number(serverTime);
      if (Number.isFinite(n)) this.lastServerTimeMs = n;
    }
  }

  private feedHeaders(): Record<string, string> {
    const h: Record<string, string> = { accept: "application/x-ndjson" };
    if (this.opts.auth.kind === "token") h["authorization"] = `Bearer ${this.opts.auth.token}`;
    return h;
  }
}

/**
 * Iterate an NDJSON Response as non-empty lines. Streams `response.body` (chunked + partial-line
 * buffered) when present — the production path that never buffers the whole snapshot; falls back to a
 * buffered `await res.text()` when the body is absent (e.g. a test fetch stand-in).
 */
async function* iterateNdjson(res: Response): AsyncGenerator<string> {
  const body = res.body;
  if (body && typeof body.getReader === "function") {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let nl: number;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (line.length > 0) yield line;
      }
    }
    buf += decoder.decode();
    if (buf.length > 0) {
      for (const line of buf.split("\n")) if (line.length > 0) yield line;
    }
    return;
  }
  const text = await res.text();
  for (const line of text.split("\n")) if (line.length > 0) yield line;
}
