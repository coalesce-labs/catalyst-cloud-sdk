// @catalyst-cloud/sdk — LiveSyncClient: the live-sync client.
//
// The push transport for the catalyst-cloud change feed. It opens an OUTBOUND WebSocket to the
// tenant's mirror (`{baseUrl}{connectPath}`) and lets the service push each change the instant it
// lands. On every (re)connect it sends `{type:"sync", after:<cursor>}` so the service replays — in
// seq order — everything the consumer missed; a `{type:"resync"}` frame (cursor underflow) triggers a
// full re-seed via the injected `reseed` callback. It reconnects with capped exponential backoff and
// never throws out of the event loop.
//
// It is deliberately transport-only and storage-agnostic: it does NOT know about a specific storage
// engine, snapshot endpoint, or auth token — any host-only assumption is INJECTED:
//
//   • auth      — {kind:"token", token} (host → ?token=) OR {kind:"cookie"} (browser → nothing; the
//                 same-origin cookie rides the upgrade). A cookie-kind client can never leak a token.
//   • reseed    — async callback returning the fresh cursor. The host pulls /snapshot into bun:sqlite;
//                 the browser re-runs its own OPFS seed(). The class never hardcodes a snapshot path.
//   • onChange  — each applied change frame (the consumer lands it into its own store).
//   • onStatus  — connection lifecycle ("connecting"/"live"/"reconnecting"/"resyncing"/"error"/
//                 "stopped"), so a UI can render "live"/"reconnecting".
//   • wsFactory — injectable WebSocket constructor (tests / a node polyfill). Defaults to the
//                 platform global `WebSocket` (browser, Bun, Node >=22) so the shared core has NO
//                 node-only import like 'ws'.
//
// Liveness (CTC-135): a pure-receive socket cannot tell a quiet feed from a HALF-OPEN one. A laptop
// sleep or network roam can kill the TCP path with no FIN/RST, so `onclose`/`onerror` never fire and
// the client would report "live" forever while frozen (a mini ran 3+ days, a laptop 6.8 days stuck at
// one cursor). The premise that "the platform handles ping/pong" is false here — WHATWG platforms
// *respond* to protocol pings but nothing in this stack ever *sends* one, and browsers can't send RFC
// 6455 pings at all. So this client runs an app-level watchdog: after `pingIntervalMs` of inbound
// silence it sends the pinned `{"type":"ping"}` frame; the mirror answers via `setWebSocketAutoResponse`
// (which replies WITHOUT waking a hibernated DO — ADR-0009's cost model is preserved). If no frame
// arrives within `pongTimeoutMs`, the socket is force-reconnected through the existing backoff path.
// Traffic postpones pings (no keepalive on a busy stream), and a 3-probe feature-detect disables the
// watchdog against an old server that never pongs — so it degrades to exactly today's behavior.

import type { ChangeFrame, PongFrame, ResyncFrame, ServerFrame, SyncFrame } from "./types.js";
import { PING_FRAME } from "./types.js";
import {
  NOOP_TELEMETRY,
  createTelemetry,
  CATALYST_ATTR,
  REPLICA_SPAN,
  DEFAULT_SCOPE_NAME,
  type Telemetry,
  type TelemetryConfig,
  type ManualSpan,
} from "./otel.js";

/**
 * The minimal WHATWG-WebSocket surface LiveSyncClient drives. Declared structurally (not via a DOM /
 * Node lib type) so (a) the package needs no `@types/ws` / DOM lib and (b) tests inject a fake. The
 * browser, Bun, and Node (>=22) global `WebSocket` are all structurally assignable to this.
 */
export interface WebSocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
}

/** Opens a WebSocket to `url`. Defaults to the runtime global `WebSocket`; tests/node inject one. */
export type WebSocketFactory = (url: string) => WebSocketLike;

/**
 * How the consumer proves it may open `/connect`.
 *
 *  • `token`  — a service bearer token. The WHATWG WebSocket constructor
 *    cannot set an Authorization header, so the token rides the URL as `?token=`. The service
 *    constant-time compares it and STRIPS it before forwarding internally. Use this ONLY on a
 *    trusted backend — never the browser.
 *  • `cookie` — append NOTHING to the URL. The browser's same-origin session cookie rides the
 *    WebSocket upgrade automatically. This makes it impossible to leak a token from the browser path.
 */
export type AuthStrategy = { kind: "token"; token: string } | { kind: "cookie" };

/** Connection lifecycle, surfaced via `onStatus` so a consumer can drive UI. */
export type LiveSyncStatus =
  | "connecting"
  | "live"
  | "reconnecting"
  | "resyncing"
  | "error"
  | "stopped";

/** Structured log levels. */
export type LogLevel = "info" | "warn" | "error";

/** Configuration for a {@link LiveSyncClient}. */
export interface LiveSyncClientOptions {
  /**
   * The service's public origin, http(s), INCLUDING any versioned path prefix (e.g.
   * "https://api.example/api/v1"). The scheme is swapped to ws(s) and `connectPath` is appended
   * verbatim (path-preserving). A trailing slash is trimmed.
   */
  baseUrl: string;
  /** The tenant id = the mirror's name; sent as `?account=` on the connect URL. */
  accountId: string;
  /**
   * The connect route. Default "/connect"; the service dual-serves it at "/api/v1/connect" too, so a
   * path-prefixed `baseUrl` ("…/api/v1") with the default "/connect" resolves to "…/api/v1/connect".
   */
  connectPath?: string;
  /** How to authorize the upgrade. token → ?token= (host); cookie → nothing (browser). */
  auth: AuthStrategy;
  /**
   * Re-seed the consumer's store from a full snapshot and resolve to the FRESH cursor. Called on a
   * `{type:"resync"}` underflow frame and (optionally) before the first connect. The host pulls
   * /snapshot into bun:sqlite; the browser re-runs its OPFS seed(). The client closes the socket
   * before calling this so no live frame interleaves with the seed.
   */
  reseed: () => Promise<number>;
  /**
   * Read the consumer's durable cursor (the last applied feed seq), or null/undefined if it has
   * never seeded. Drives the `{type:"sync", after}` catch-up request. Return `null` to send
   * `after: -1` (replay from the start).
   */
  getCursor: () => number | null | undefined;
  /**
   * Land one applied change frame into the consumer's store. The client does NOT persist for you —
   * this is where the host upserts into bun:sqlite and advances its cursor, or the browser writes
   * OPFS. Errors thrown here are caught and logged (one bad frame won't wedge the stream).
   */
  onChange: (frame: ChangeFrame) => void;
  /** Optional: every parsed server frame (change OR resync), before the type-specific handling. */
  onFrame?: (frame: ServerFrame) => void;
  /** Optional: connection lifecycle, for UI ("live"/"reconnecting"/…). */
  onStatus?: (status: LiveSyncStatus) => void;
  /** Base reconnect backoff in ms; doubles each failed attempt up to maxBackoffMs. Default 1000. */
  backoffMs?: number;
  /** Reconnect backoff ceiling in ms. Default 30_000. */
  maxBackoffMs?: number;
  /**
   * Liveness watchdog (CTC-135): after this many ms of inbound silence, send one `{"type":"ping"}` and
   * expect a frame back within {@link pongTimeoutMs}. Any inbound frame (change OR pong) postpones the
   * next ping, so a busy stream never pings. Default `90_000`; set `0` to disable the watchdog entirely
   * (back to close/error-only detection).
   */
  pingIntervalMs?: number;
  /**
   * How long (ms) to wait for ANY inbound frame after sending a liveness ping before declaring the
   * socket half-open and force-reconnecting through the normal backoff path. Default `15_000`.
   */
  pongTimeoutMs?: number;
  /** Injectable WebSocket factory (tests / a node polyfill). Defaults to `globalThis.WebSocket`. */
  wsFactory?: WebSocketFactory;
  /** Optional structured logger; defaults to console. */
  log?: (level: LogLevel, msg: string, extra?: unknown) => void;
  /**
   * Opt-in OpenTelemetry (CTC-138). `false`/absent = OFF (zero overhead, no `@opentelemetry/api`
   * import attempted). `true` = ON with the default instrumentation scope; an object overrides the
   * tracer/meter name. This transport emits the `catalyst.replica.reconnect` (per connect attempt) and
   * `catalyst.replica.resync` spans; the replica layer adds the seed/apply spans + the metrics. When a
   * {@link CatalystReplica} owns this client it injects its already-resolved {@link Telemetry} so both
   * layers share ONE tracer/meter (and the seed span nests under the resync span). Spans route through
   * the consumer's global TracerProvider when one is registered.
   */
  telemetry?: TelemetryConfig | Telemetry;
}

/** Resolve the runtime global WebSocket, or fail with an actionable message. */
function defaultWsFactory(url: string): WebSocketLike {
  const Ctor = (globalThis as { WebSocket?: new (u: string) => WebSocketLike }).WebSocket;
  if (!Ctor) {
    throw new Error(
      "global WebSocket unavailable; pass wsFactory (browser, Bun, or Node >=22 expose one)",
    );
  }
  return new Ctor(url);
}

/** http(s)://host → ws(s)://host, leaving the rest of the origin + path intact (https→wss, http→ws). */
export function toWsOrigin(baseUrl: string): string {
  return baseUrl.replace(/^http/i, "ws");
}

/**
 * Build the `/connect` URL with `?account=` and, for token auth, `?token=`. For cookie auth NO token
 * is ever appended (the type system + this single construction point make a browser token leak
 * impossible). Token is ordered FIRST so a truncated log line still reveals the account.
 */
/** Strip trailing "/" without a backtracking regex (ReDoS-safe vs `/\/+$/`, CodeQL js/polynomial-redos). */
export function stripTrailingSlashes(s: string): string {
  let end = s.length;
  while (end > 0 && s.charCodeAt(end - 1) === 47 /* "/" */) end--;
  return end === s.length ? s : s.slice(0, end);
}

export function buildConnectUrl(opts: {
  baseUrl: string;
  connectPath: string;
  accountId: string;
  auth: AuthStrategy;
}): string {
  const origin = toWsOrigin(stripTrailingSlashes(opts.baseUrl));
  const params = new URLSearchParams();
  if (opts.auth.kind === "token") params.set("token", opts.auth.token);
  params.set("account", opts.accountId);
  return `${origin}${opts.connectPath}?${params.toString()}`;
}

/**
 * Consecutive opened-then-never-ponged connections after which the watchdog disables itself for the
 * client's lifetime (feature-detect for a server without auto-pong). Bounds worst-case reconnect
 * churn against an old server to exactly this many attempts, making mirror/SDK deploy order harmless.
 */
const PROBE_FAILURE_LIMIT = 3;

export class LiveSyncClient {
  private readonly baseUrl: string;
  private readonly accountId: string;
  private readonly connectPath: string;
  private readonly auth: AuthStrategy;
  private readonly reseed: () => Promise<number>;
  private readonly getCursor: () => number | null | undefined;
  private readonly onChange: (frame: ChangeFrame) => void;
  private readonly onFrame?: (frame: ServerFrame) => void;
  private readonly onStatus?: (status: LiveSyncStatus) => void;
  private readonly backoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly pingIntervalMs: number;
  private readonly pongTimeoutMs: number;
  private readonly wsFactory: WebSocketFactory;
  private readonly log: NonNullable<LiveSyncClientOptions["log"]>;
  private readonly telemetryConfig: TelemetryConfig | Telemetry | undefined;

  private ws: WebSocketLike | null = null;
  private stopped = false;
  private resyncing = false;
  private backoff: number;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private resolveDone: (() => void) | null = null;
  /** Resolved once in start(); the no-op until then so any early call is safe. */
  private telemetry: Telemetry = NOOP_TELEMETRY;
  /** The in-flight connect-attempt span (ended OK on open, ERROR on construct-fail / close-before-open). */
  private connectSpan: ManualSpan | null = null;

  // ── Liveness watchdog state (CTC-135) ──
  /** Epoch ms of the last inbound frame (any bytes: change, pong, even malformed). Null before the
   *  first frame. Public via {@link lastFrameAt} — the per-frame timestamp the catalyst daemon's
   *  stall classifier lacked (it can now distinguish a quiet feed from a dead socket). Client-lifetime. */
  private _lastFrameAt: number | null = null;
  /** Per-CONNECTION: has this socket answered at least one ping? Gates a real liveness timeout vs a
   *  never-ponged feature-detect probe. Reset on every (re)open. */
  private pongObserved = false;
  /** Per-CONNECTION: epoch ms the last ping was sent, for the deadline's late-timer re-check. */
  private pingSentAt = 0;
  /** Client-lifetime: consecutive opened-then-never-ponged connections. Reset to 0 by ANY pong. */
  private probeFailures = 0;
  /** Client-lifetime: after PROBE_FAILURE_LIMIT never-ponged connections we stop pinging for good
   *  (an old server without auto-pong), degrading to close/error-only detection. */
  private watchdogDisabled = false;
  private pingTimer: ReturnType<typeof setTimeout> | null = null;
  private pongDeadline: ReturnType<typeof setTimeout> | null = null;

  constructor(opts: LiveSyncClientOptions) {
    this.baseUrl = stripTrailingSlashes(opts.baseUrl);
    this.accountId = opts.accountId;
    this.connectPath = opts.connectPath ?? "/connect";
    this.auth = opts.auth;
    this.reseed = opts.reseed;
    this.getCursor = opts.getCursor;
    this.onChange = opts.onChange;
    this.onFrame = opts.onFrame;
    this.onStatus = opts.onStatus;
    this.backoffMs = opts.backoffMs ?? 1000;
    this.maxBackoffMs = opts.maxBackoffMs ?? 30_000;
    this.pingIntervalMs = opts.pingIntervalMs ?? 90_000;
    this.pongTimeoutMs = opts.pongTimeoutMs ?? 15_000;
    this.wsFactory = opts.wsFactory ?? defaultWsFactory;
    this.log =
      opts.log ??
      ((lvl, msg, extra) =>
        console[lvl === "error" ? "error" : "log"](`[catalyst-sdk:live] ${msg}`, extra ?? ""));
    this.telemetryConfig = opts.telemetry;
    this.backoff = this.backoffMs;
  }

  /**
   * Start the client: seed first if the consumer has no cursor, then open the live socket and keep it
   * open. Returns a Promise that resolves ONLY when stop() is called (mirrors the host daemon's "runs
   * forever" contract) — the open WebSocket keeps the process alive between deltas. In a browser the
   * returned Promise is simply never awaited; call stop() on teardown.
   */
  async start(): Promise<void> {
    this.stopped = false;
    // Resolve the OTel seam ONCE up front (before the first reseed, so the seed span exists on the
    // cold-start path too). Keep the OFF path FULLY SYNCHRONOUS — no `await`, so a caller that opens
    // the socket and inspects it in the same tick still sees it; only pay the async resolution (guarded
    // dynamic import, or a CatalystReplica passing its already-resolved instance) when telemetry is on.
    this.telemetry =
      this.telemetryConfig === undefined || this.telemetryConfig === false
        ? NOOP_TELEMETRY
        : await createTelemetry(this.telemetryConfig, {
            tracerName: DEFAULT_SCOPE_NAME,
            meterName: DEFAULT_SCOPE_NAME,
          });
    const saved = this.getCursor();
    if (saved == null) {
      this.setStatus("resyncing");
      await this.reseed();
    }
    this.openSocket();
    return new Promise<void>((resolve) => {
      this.resolveDone = resolve;
    });
  }

  /** Stop the client: close the socket, cancel any pending reconnect, resolve start(). Idempotent. */
  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer != null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.closeSocket();
    this.setStatus("stopped");
    const done = this.resolveDone;
    this.resolveDone = null;
    done?.();
  }

  /** The ws(s):// URL this client opens, for diagnostics/tests. Re-derived from the options. */
  connectUrl(): string {
    return buildConnectUrl({
      baseUrl: this.baseUrl,
      connectPath: this.connectPath,
      accountId: this.accountId,
      auth: this.auth,
    });
  }

  private setStatus(status: LiveSyncStatus): void {
    try {
      this.onStatus?.(status);
    } catch (err) {
      this.log("warn", "onStatus handler threw", err);
    }
  }

  private openSocket(): void {
    if (this.stopped) return;
    this.setStatus("connecting");
    // One span per connect attempt: started here, ended OK in onopen, ERROR on construct-fail / a close
    // before open. Manual (not active) because the lifecycle spans onopen…onclose callbacks.
    this.connectSpan = this.telemetry.startSpan(REPLICA_SPAN.reconnect, {
      [CATALYST_ATTR.tenant]: this.accountId,
    });
    const wsUrl = this.connectUrl();
    let ws: WebSocketLike;
    try {
      ws = this.wsFactory(wsUrl);
    } catch (err) {
      this.log("error", "ws construction failed; scheduling reconnect", err);
      this.setStatus("error");
      this.endConnectSpan(err);
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.onopen = () => {
      this.backoff = this.backoffMs; // a successful open resets the backoff ramp
      this.setStatus("live");
      this.endConnectSpan();
      // Fresh connection: reset per-connection watchdog state, then start the idle-ping countdown.
      this.pongObserved = false;
      this.pingSentAt = 0;
      this.sendSync();
      this.armPing();
    };
    ws.onmessage = (ev) => {
      // ANY inbound bytes prove the socket is alive: stamp lastFrameAt, clear a pending pong deadline,
      // and postpone the next ping — BEFORE parsing, so even a malformed frame counts as liveness.
      this.onInboundFrame();
      void this.handleFrame(ev.data);
    };
    ws.onclose = () => {
      if (this.ws === ws) this.ws = null;
      this.clearLivenessTimers(); // this connection's ping/deadline die with its socket
      if (!this.stopped && !this.resyncing) this.setStatus("reconnecting");
      // No-op if onopen already ended it (a normal disconnect of a healthy socket isn't a connect error).
      this.endConnectSpan(new Error("socket closed before open"));
      this.scheduleReconnect();
    };
    ws.onerror = (err) => {
      // Some implementations fire error THEN close; close() here is best-effort and onclose drives the
      // reconnect so we never double-schedule.
      this.log("warn", "ws error", err);
      this.setStatus("error");
      try {
        ws.close();
      } catch {
        // already closing/closed
      }
    };
  }

  /** End the in-flight connect span exactly once (idempotent — nulls the handle). */
  private endConnectSpan(error?: unknown): void {
    const span = this.connectSpan;
    if (!span) return;
    this.connectSpan = null;
    span.end(error);
  }

  /** Detach handlers BEFORE closing so a programmatic close can't re-enter scheduleReconnect. */
  private closeSocket(): void {
    // A deliberate teardown of an in-flight attempt (stop/resync): end the connect span neutrally.
    this.endConnectSpan();
    // The single choke point for liveness-timer teardown — covers stop/resync/forceReconnect. (The
    // server-close path clears them in onclose; both routes null this.ws, so no timer outlives a socket.)
    this.clearLivenessTimers();
    const ws = this.ws;
    this.ws = null;
    if (!ws) return;
    ws.onopen = null;
    ws.onmessage = null;
    ws.onclose = null;
    ws.onerror = null;
    try {
      ws.close();
    } catch {
      // already closed
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.resyncing || this.reconnectTimer != null) return;
    const delay = this.backoff;
    this.backoff = Math.min(this.backoff * 2, this.maxBackoffMs);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.openSocket();
    }, delay);
  }

  /** Ask the service to replay everything after our durable cursor. */
  private sendSync(): void {
    const after = this.getCursor() ?? -1;
    const frame: SyncFrame = { type: "sync", after };
    try {
      this.ws?.send(JSON.stringify(frame));
    } catch (err) {
      this.log("error", "sync send failed", err);
    }
  }

  private async handleFrame(data: unknown): Promise<void> {
    const frame = parseFrame(data);
    if (!frame) return;
    if (frame.type === "pong") {
      // Transport-internal liveness ack: the socket is alive and the server speaks auto-pong. Record
      // capability and reset the feature-detect counter; a pong is NEVER surfaced to onFrame/onChange.
      // (lastFrameAt + the pending-deadline clear already happened synchronously in onInboundFrame.)
      this.pongObserved = true;
      this.probeFailures = 0;
      return;
    }
    try {
      this.onFrame?.(frame);
    } catch (err) {
      this.log("warn", "onFrame handler threw", err);
    }
    if (frame.type === "resync") {
      await this.handleResync();
      return;
    }
    try {
      this.onChange(frame);
    } catch (err) {
      this.log("error", `onChange failed for ${frame.entity} seq=${frame.seq}`, err);
    }
  }

  /**
   * Cursor underflow: the deltas we need were evicted from the service's retained change buffer. Close the socket
   * (so no live frame interleaves with the re-seed), re-seed via the injected callback, then reconnect
   * — which re-sends {type:"sync"} from the fresh cursor. `resyncing` guards against a second resync
   * frame and suppresses scheduleReconnect for the duration so we reopen exactly once.
   */
  private async handleResync(): Promise<void> {
    if (this.resyncing) return;
    this.resyncing = true;
    this.setStatus("resyncing");
    this.closeSocket();
    try {
      // The reseed runs inside an ACTIVE span so the replica's seed span (the injected reseed IS
      // seedFromSnapshot) auto-parents under this resync span.
      await this.telemetry.withActiveSpan(
        REPLICA_SPAN.resync,
        { [CATALYST_ATTR.tenant]: this.accountId },
        async () => {
          const cursor = await this.reseed();
          this.log("info", `resynced, cursor=${cursor}`);
        },
      );
    } catch (err) {
      this.log("error", "resync reseed failed; will retry on reconnect", err);
    } finally {
      this.resyncing = false;
    }
    if (!this.stopped) this.openSocket();
  }

  // ── Liveness watchdog (CTC-135) ──

  /** Epoch ms of the last inbound frame (change, pong, or malformed) — null before the first frame.
   *  Any inbound bytes prove the socket is alive; the catalyst daemon's stall classifier reads this to
   *  tell a quiet feed from a half-open socket (the per-frame timestamp it previously lacked). */
  get lastFrameAt(): number | null {
    return this._lastFrameAt;
  }

  /** Any inbound frame: stamp liveness, cancel a pending pong deadline (it was answered), and postpone
   *  the next ping so a busy stream never sends one. Runs synchronously in onmessage before parsing. */
  private onInboundFrame(): void {
    this._lastFrameAt = Date.now();
    this.clearPongDeadline();
    this.armPing();
  }

  /** (Re)arm the idle-ping timer. No-op when the watchdog is disabled/off or there is no live socket,
   *  so it is safe to call on every frame. A setTimeout chain (not setInterval): each frame resets it. */
  private armPing(): void {
    this.clearPingTimer();
    if (this.watchdogDisabled || this.pingIntervalMs <= 0 || this.stopped || !this.ws) return;
    this.pingTimer = setTimeout(() => this.sendPing(), this.pingIntervalMs);
  }

  /** The feed has been idle for a full interval: send one liveness ping and start the pong deadline. A
   *  synchronous send throw means the socket is already dead — treat it as an unanswered probe now. */
  private sendPing(): void {
    this.pingTimer = null; // this timer just fired
    if (this.stopped || this.watchdogDisabled || !this.ws) return;
    this.pingSentAt = Date.now();
    try {
      this.ws.send(PING_FRAME);
    } catch (err) {
      this.log("warn", "liveness ping send failed (dead socket); forcing reconnect", err);
      this.onProbeUnanswered();
      return;
    }
    this.pongDeadline = setTimeout(() => this.onPongDeadline(), this.pongTimeoutMs);
  }

  /** The pong deadline elapsed. Late-timer guard first: in a throttled background tab this callback can
   *  fire long after a frame actually arrived, so if ANYTHING landed at/after the ping we sent, liveness
   *  is proven — re-arm and move on (detection latency grows; correctness holds). Otherwise: half-open. */
  private onPongDeadline(): void {
    this.pongDeadline = null; // this timer just fired
    if (this.stopped || !this.ws) return;
    if (this._lastFrameAt != null && this._lastFrameAt >= this.pingSentAt) {
      this.armPing();
      return;
    }
    this.onProbeUnanswered();
  }

  /** A ping went unanswered (deadline elapsed or the send threw). If this connection had already proven
   *  pong capability it is a genuine liveness timeout; otherwise it counts toward the feature-detect —
   *  after PROBE_FAILURE_LIMIT never-ponged connections the watchdog disables itself for good (an old
   *  server without auto-pong). Either way, force-reconnect through the existing backoff path. */
  private onProbeUnanswered(): void {
    if (this.pongObserved) {
      this.log("warn", "liveness timeout: no frame within the pong deadline; reconnecting");
    } else {
      this.probeFailures += 1;
      if (this.probeFailures >= PROBE_FAILURE_LIMIT) {
        this.watchdogDisabled = true;
        this.log(
          "warn",
          `liveness watchdog disabled after ${PROBE_FAILURE_LIMIT} unanswered probes (server lacks auto-pong); relying on close/error detection`,
        );
      }
    }
    this.forceReconnect();
  }

  /** Tear the socket down and reconnect through the normal backoff. closeSocket detaches handlers (so
   *  onclose won't also fire) and clears the liveness timers, so we schedule the reopen ourselves. */
  private forceReconnect(): void {
    this.closeSocket();
    if (this.stopped) return;
    this.setStatus("reconnecting");
    this.scheduleReconnect();
  }

  private clearPingTimer(): void {
    if (this.pingTimer != null) {
      clearTimeout(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private clearPongDeadline(): void {
    if (this.pongDeadline != null) {
      clearTimeout(this.pongDeadline);
      this.pongDeadline = null;
    }
  }

  private clearLivenessTimers(): void {
    this.clearPingTimer();
    this.clearPongDeadline();
  }
}

/** Parse a WS frame (string or ArrayBuffer) into a known server frame, or null for anything malformed. */
export function parseFrame(data: unknown): ServerFrame | null {
  const text =
    typeof data === "string"
      ? data
      : data instanceof ArrayBuffer
        ? new TextDecoder().decode(data)
        : null;
  if (text == null) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const type = (parsed as { type?: unknown }).type;
  if (type === "resync") return parsed as ResyncFrame;
  if (type === "change") return parsed as ChangeFrame;
  if (type === "pong") return parsed as PongFrame;
  return null;
}
