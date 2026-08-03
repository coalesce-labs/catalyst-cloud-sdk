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
// Traffic postpones pings (no keepalive on a busy stream), and a 3-probe feature-detect DEGRADES the
// watchdog against an old server that never pongs — re-probing at 10x the interval instead of never
// (CTC-281: detection can be slowed, but never permanently lost).
//
// Gap detection (CTL-1402): the server's live push (`broadcastChange`) is at-most-once — a send into a
// half-open socket is silently swallowed, and the dropped frame used to be sealed over permanently the
// moment the next delivered frame advanced the cursor. The change_log itself is durable, so every lost
// frame is recoverable — the client just never asked again. This client now tracks the contiguity of
// delivered seqs: a frame arriving at `seq > deliveredSeq + 1` is NOT delivered to `onChange`; instead
// the client re-requests the hole with the SAME `{type:"sync", after:<deliveredSeq>}` control frame it
// already sends on every (re)connect, and the mirror replays the missing rows as ordinary change
// frames through this same path (server support has existed since CTC-63 — `replaySince` is keyset-
// paginated and answers `{type:"resync"}` on underflow). Bounded: after `gapRetryLimit` no-PROGRESS
// windows (the heal deadline re-arms on every delivered frame, so a big-but-advancing heal never
// escalates) the client escalates to the full re-seed path rather than spinning — a gap is never
// silently accepted.
//
// Wedge-proofing (CTC-281): the Jul 17-23 fleet incident (6 windows of server-side half-opens with no
// FIN/RST; cursors frozen 28-215 min while clients said "live") exposed four restart-only states this
// client could reach. The invariant now enforced: EVERY state that is not "stopped" holds either a
// pending timer or a socket whose events re-enter the machine — there is no state only a process
// restart clears. Concretely: (1) a connect attempt whose ws impl never fires open/close/error is
// bounded by `openTimeoutMs`; (2) `onerror` without a follow-up `onclose` (real undici bugs #3697/
// #3546) arms a one-shot fallback reconnect; (3) the watchdog feature-detect can only DEGRADE itself
// (a slow re-probe every DEGRADED_PROBE_MULTIPLIER x pingIntervalMs), never disable itself outright —
// and only while pong capability is UNPROVEN; once ANY pong has ever been observed, silence is always
// treated as a liveness failure (during the incident, 3 open-then-silent sockets used to disable
// detection for the client's lifetime ~6 min into a window — and because the pong latch is per-process,
// a client RESTARTED mid-window would have re-latched the disable, so the degrade-not-disable shape is
// what actually guarantees convergence); (4) a FAILED reseed re-enters the backoff path instead
// of hot-reopening — and the reseed await itself is bounded by `reseedTimeoutMs` (the injected
// callback is a trust boundary like the ws impl: both first-party reseeds self-bound (the node
// replica's seedFromSnapshot and the browser replica's OPFS seed both abort on an idle body), but an
// arbitrary consumer-supplied reseed can still hang, and "resyncing" holds no socket and suppresses
// scheduleReconnect, so without this bound
// it was the one remaining zero-timer state; a timed-out reseed is ABANDONED, its late settle
// discarded, and the client re-enters backoff); (5) closeSocket() escalates past `close()` to a duck-typed `terminate()` (Bun /
// the 'ws' package expose one; undici does not — its close-handshake wait is why teardown must not
// depend on a graceful close against a half-open peer). Every transition emits the `catalyst.replica.gap` log/counter signal, the
// detector the per-frame apply telemetry is structurally blind to (an undelivered frame lands in no
// apply bucket). Gaps are the STEADY-STATE path here — the mirror's reconcile pass appends change_log
// rows it never broadcasts, so every pass punches a hole that heals via re-request — hence `detected`
// /`healed` log at INFO and only `escalated` alerts. A reconcile pass with NO webhook frame after it
// would leave nothing to detect the hole FROM, so the mirror also broadcasts one end-of-pass
// `{type:"head", seq:<feed head>}` nudge; the client treats a head beyond its baseline exactly like a
// beyond-gap change frame (re-request the hole `deliveredSeq+1..head`) but never applies it.

import type { ChangeFrame, HeadFrame, PongFrame, ResyncFrame, ServerFrame, SyncFrame } from "./types.js";
import { PING_FRAME } from "./types.js";
import {
  NOOP_TELEMETRY,
  createTelemetry,
  CATALYST_ATTR,
  REPLICA_LOG,
  REPLICA_METRIC,
  REPLICA_SPAN,
  DEFAULT_SCOPE_NAME,
  type Counter,
  type ReplicaGapEvent,
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
  /**
   * The tenant id = the mirror's name; sent as `?account=` on the connect URL.
   *
   * OPTIONAL, and omitting it is the browser default: the OPEN read plane scopes a cookie-authed
   * connection to the session user's own tenant, and an explicit account is the tenant-SWITCHER path.
   * When absent the parameter is omitted entirely rather than serialized as `?account=` — an empty
   * value is not a mirror name, and freezing it into the wire contract would leave the one nearby
   * server-side fallback that a bare empty value can reach (`adminAccountScope`'s `|| "tenant-0"`)
   * cross-tenant-shaped.
   *
   * REQUIRED for `auth: {kind:"token"}` — a host has no session to fall back to, so the constructor
   * throws rather than letting it fail later as an opaque server 401.
   */
  accountId?: string;
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
   * before calling this so no live frame interleaves with the seed. The await is bounded by
   * {@link reseedTimeoutMs} (CTC-281) — a hanging reseed is abandoned (its late settle discarded)
   * and the client re-enters the backoff path, so this callback can never wedge the transport; still,
   * bound your own I/O (as the replica's seedFromSnapshot does) so a dead fetch fails fast, not at
   * the backstop.
   *
   * `signal` ABORTS when the transport gives up on this attempt — the {@link reseedTimeoutMs}
   * deadline expiring, or `stop()` (CTC-114 review round 10). Honour it: the transport considers the
   * attempt over the moment it fires, and RECONNECTS. A callback that keeps writing after that is a
   * second writer racing the socket the transport just reopened — the browser replica's concrete
   * failure was frames delivered past the abandoned seed's cursor being dropped by its paused queue,
   * after which a late seed completion left the socket advanced over a hole that later deltas sealed
   * permanently.
   *
   * This is why "abandon" alone was not enough. Two layers bounding one operation with NO
   * cancellation link between them is the defect; the signal is the link. Optional parameter, so
   * existing zero-arg callbacks still typecheck — but a consumer that ignores it keeps the old
   * two-writer hazard.
   */
  reseed: (signal?: AbortSignal) => Promise<number>;
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
  /**
   * Connect/open deadline (CTC-281): how long (ms) after constructing a socket to wait for `onopen`
   * before treating the attempt as failed (tear the socket down, status "reconnecting", backoff
   * reconnect). Without it, a TCP-connect or HTTP-upgrade stall that never fires open/close/error
   * (undici sets no upgrade deadline; injected impls are trusted blindly) wedges the client in
   * "connecting" with ZERO pending timers — restart-only. Default `20_000`; `0` disables.
   */
  openTimeoutMs?: number;
  /**
   * Re-seed deadline (CTC-281): how long (ms) to wait for the injected {@link reseed} callback to
   * settle before abandoning the attempt (status "reconnecting", backoff reconnect; the abandoned
   * reseed's late settle is discarded). While resyncing the client holds NO socket and suppresses
   * scheduleReconnect, so an unbounded reseed hang was a zero-timer restart-only wedge — the exact
   * ticket-ask-3 state. A TOTAL-duration backstop, deliberately generous: the replica's
   * seedFromSnapshot already fails fast on a dead stream via its own IDLE abort
   * (`snapshotIdleTimeoutMs`); this bound exists for reseed impls with no bound of their own (e.g. a
   * browser OPFS seed over a default fetch). Raise it if a legitimate full seed can stream longer.
   * Default `600_000` (10 min); `0` disables.
   */
  reseedTimeoutMs?: number;
  /**
   * Gap detection (CTL-1402): how long (ms) to wait for a `{type:"sync"}` gap re-request to finish
   * redelivering the detected hole before retrying (or, once {@link gapRetryLimit} re-requests have
   * been spent, escalating to the full re-seed path). Default `10_000`; set `0` to never time out
   * (the gap then heals only via the replay itself or the next reconnect — not recommended).
   */
  gapTimeoutMs?: number;
  /**
   * Gap detection (CTL-1402): total `{type:"sync"}` re-requests to spend on one gap before escalating
   * to the resync/re-seed path. Default `3`. Escalation — never silent acceptance — is deliberate: a
   * gap the replay cannot close (evicted rows, or seqs that never existed) must end in a /snapshot
   * re-seed, not a sealed hole.
   */
  gapRetryLimit?: number;
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

/**
 * How long a CANCELLED reseed gets to unwind before the transport settles anyway.
 *
 * Cancelling is asynchronous on the consumer's side — the browser seed aborts a fetch, trips its
 * supersede guard, posts `seedAbort` and resumes its delta queue — and the transport reconnects the
 * moment it settles, so it must wait for that unwind or it reconnects into a still-paused consumer.
 * Bounded because the deadline that triggered this exists precisely for an unresponsive callback: a
 * cleanup that also hangs must not wedge the transport (CTC-114 review round 12).
 */
const CANCEL_CLEANUP_GRACE_MS = 500;

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
  accountId?: string;
  auth: AuthStrategy;
}): string {
  const origin = toWsOrigin(stripTrailingSlashes(opts.baseUrl));
  const params = new URLSearchParams();
  if (opts.auth.kind === "token") params.set("token", opts.auth.token);
  // Only when a tenant was actually named. `?account=` is NOT the same as no account: the server's
  // consumers are truthiness checks, so empty takes the omitted path anyway — but it would freeze a
  // contract in which "" is a legal mirror name, and it puts `catalyst.tenant=""` on every span.
  if (opts.accountId) params.set("account", opts.accountId);
  const query = params.toString();
  // Guard the dangling "?" — with cookie auth and no account there are no params at all.
  return `${origin}${opts.connectPath}${query ? `?${query}` : ""}`;
}

/**
 * Consecutive opened-then-never-ponged connections after which the watchdog DEGRADES itself
 * (feature-detect for a server without auto-pong). Bounds worst-case reconnect churn against an old
 * server, making mirror/SDK deploy order harmless.
 */
const PROBE_FAILURE_LIMIT = 3;

/**
 * Degraded-watchdog probe interval, as a multiple of `pingIntervalMs` (CTC-281). After
 * {@link PROBE_FAILURE_LIMIT} never-ponged connections the watchdog does NOT turn off — it re-probes
 * at this heavily backed-off cadence (stock: every 15 min instead of 90 s). A hard lifetime disable
 * was the incident's restart-only residual: the `pongEverObserved` latch is per-PROCESS, so a client
 * (re)started inside an incident window (supervisors restarted processes mid-window) came up
 * unproven, burned its 3 probes against open-but-silent sockets, and went permanently blind — the
 * next half-open then froze it as "live" forever, and only another restart (which repeats the cycle)
 * cleared it. Degrading instead keeps a probe pending in EVERY non-stopped state: a half-open socket
 * under a degraded watchdog is still detected within ~this multiple of the interval, and the first
 * pong after recovery re-arms full-speed detection (and latches capability as proven). Against a
 * genuinely old server the cost is one bounded reconnect per degraded window — churn, never wedge.
 */
const DEGRADED_PROBE_MULTIPLIER = 10;

/**
 * How long (ms) after `onerror` to wait for the spec-mandated follow-up `onclose` before forcing the
 * reconnect ourselves (CTC-281). WHATWG requires close-after-error, but real impls have shipped
 * violations (undici #3697 "close not emitted on error", #3546 "close not fired if the connection
 * failed to be established") — and `WebSocketLike` is structural, so an injected impl is trusted
 * blindly. Pre-open, a missing onclose used to be a ZERO-timer permanent-"error" wedge.
 */
const ERROR_CLOSE_GRACE_MS = 5_000;

export class LiveSyncClient {
  private readonly baseUrl: string;
  private readonly accountId: string | undefined;
  private readonly connectPath: string;
  private readonly auth: AuthStrategy;
  private readonly reseed: (signal?: AbortSignal) => Promise<number>;
  private readonly getCursor: () => number | null | undefined;
  private readonly onChange: (frame: ChangeFrame) => void;
  private readonly onFrame?: (frame: ServerFrame) => void;
  private readonly onStatus?: (status: LiveSyncStatus) => void;
  private readonly backoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly pingIntervalMs: number;
  private readonly pongTimeoutMs: number;
  private readonly openTimeoutMs: number;
  private readonly reseedTimeoutMs: number;
  private readonly gapTimeoutMs: number;
  private readonly gapRetryLimit: number;
  private readonly wsFactory: WebSocketFactory;
  private readonly log: NonNullable<LiveSyncClientOptions["log"]>;
  private readonly telemetryConfig: TelemetryConfig | Telemetry | undefined;

  private ws: WebSocketLike | null = null;
  private stopped = false;
  /**
   * Has start() been entered? `stopped` alone cannot answer this — it is false BEFORE the first
   * start() as well as during a run, so every "am I running?" guard read true on a client that had
   * never booted. Only the public `requestResync()` can reach that window (CTC-114 review round 5):
   * it would reseed and open a socket with no lifecycle deferred and no telemetry resolved, and the
   * later real start() would then openSocket() again — overwriting `this.ws`, so the first socket
   * kept delivering duplicate frames and could no longer be closed through the stored reference.
   */
  private started = false;
  /** The in-flight boot task, or null once it settles — `requestResync()` serializes behind it. */
  private bootTask: Promise<void> | null = null;
  /** Settles an in-flight `boundedReseed` wrapper on stop(), so an awaited resync cannot hang. */
  private abandonReseed: (() => void) | null = null;
  /**
   * The in-flight resync, so a concurrent caller AWAITS it instead of being handed an
   * already-resolved promise and acting on a store still being rebuilt (round 10).
   */
  private activeResync: Promise<void> | null = null;
  /** Did the boot this request waited on perform a COLD re-seed? Only then may it be absorbed. */
  private bootColdSeeded = false;
  /**
   * Did the last boot REJECT? Latched until the next `start()` (CTC-114 review round 9).
   *
   * Round 8 read the failure from the awaited `bootTask`, but that handle is nulled once the boot
   * settles — so a `requestResync()` arriving after that microtask found no record of the failure,
   * with `started` still true and `stopped` still false, and sailed past the guard into a reseed that
   * could open a live socket under an application already told startup had failed. The outcome has to
   * outlive the handle.
   */
  private bootFailed = false;
  private resyncing = false;
  private backoff: number;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private resolveDone: (() => void) | null = null;
  /** Resolved once in start(); the no-op until then so any early call is safe. */
  private telemetry: Telemetry = NOOP_TELEMETRY;
  /** The in-flight connect-attempt span (ended OK on open, ERROR on construct-fail / close-before-open). */
  private connectSpan: ManualSpan | null = null;

  // ── Gap-detection state (CTL-1402) ──
  /** Transport high-water: the highest seq DELIVERED to onChange — the contiguity baseline. Re-seeded
   *  from getCursor() on every (re)open (the durable cursor never advanced past an undelivered frame,
   *  so the on-open `{type:"sync"}` replay covers any previously-pending hole). `-1` = no baseline yet
   *  (fresh cursor / cursorless store) — gap checks are suspended until a baseline exists. */
  private deliveredSeq = -1;
  /** The gap currently being re-requested, or null when the stream is contiguous. `seqFrom..seqTo` is
   *  the detected hole (fixed at detection); `retries` counts the sync re-requests spent on it. */
  private gap: { seqFrom: number; seqTo: number; retries: number } | null = null;
  private gapTimer: ReturnType<typeof setTimeout> | null = null;
  /** Epoch ms of the last inbound CHANGE frame (live or replayed, delivered or gap-dropped). Unlike
   *  {@link lastFrameAt} this ignores pongs, so it only moves when the feed actually pushes data. */
  private _lastChangeFrameAt: number | null = null;
  /** The `catalyst.replica.gaps` counter (no-op until telemetry resolves in start()). */
  private gapCounter: Counter = NOOP_TELEMETRY.counter(REPLICA_METRIC.gaps);

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
  /** Client-lifetime until a pong: after PROBE_FAILURE_LIMIT never-ponged connections the watchdog
   *  DEGRADES to a {@link DEGRADED_PROBE_MULTIPLIER}x-slower re-probe (an old server without auto-pong)
   *  — it never turns off outright, so detection is never a restart-only casualty (CTC-281). Only
   *  reachable while pong capability is UNPROVEN ({@link pongEverObserved}); the first pong clears it. */
  private watchdogDegraded = false;
  /** Client-lifetime pong latch (CTC-281): has ANY connection EVER answered a ping? Once true, the
   *  server's auto-pong capability is PROVEN for good — a later never-ponged connection is a liveness
   *  failure (the incident's open-but-silent socket), never feature-detect evidence, so the watchdog
   *  can no longer even degrade itself. PER-PROCESS by design — which is exactly why the degrade must
   *  be soft (see {@link DEGRADED_PROBE_MULTIPLIER}): a restart mid-incident resets this latch. */
  private pongEverObserved = false;
  private pingTimer: ReturnType<typeof setTimeout> | null = null;
  private pongDeadline: ReturnType<typeof setTimeout> | null = null;
  /** Per-CONNECTION connect/open deadline (CTC-281): armed when the socket is constructed, cleared on
   *  open/close/teardown. The only timer pending between openSocket() and onopen — the guarantee that
   *  a never-firing ws impl cannot leave the client wedged in "connecting" with nothing scheduled. */
  private connectTimer: ReturnType<typeof setTimeout> | null = null;
  /** Per-CONNECTION onerror→onclose fallback (CTC-281): armed by onerror, fires forceReconnect once
   *  if the impl never follows error with close (undici #3697/#3546). Cleared on open/close/teardown. */
  private errorFallbackTimer: ReturnType<typeof setTimeout> | null = null;
  /** Per-CONNECTION: has THIS socket fired onopen? The connect-deadline's late-timer guard (a
   *  throttled background tab can fire the deadline after onopen already ran and cleared it). */
  private socketOpened = false;
  /** The pending reseed deadline (CTC-281) — the timer that makes "resyncing" (no socket, reconnect
   *  suppressed) a bounded state instead of a restart-only wedge. Cleared when the reseed settles in
   *  time and by stop() (ask 4: stop() leaves NOTHING pending). At most one reseed is ever in flight:
   *  `resyncing` guards the resync path, and the boot cold seed SETS that same flag for its duration.
   *  (It used to rely on "the boot seed runs before any socket exists" — true only while a resync
   *  needed a server frame. The public `requestResync()` added in 0.8.0 needs no socket.) */
  private reseedTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(opts: LiveSyncClientOptions) {
    // Fail fast, and fail HERE. A token-authed client has no session to fall back to, so an omitted
    // account is a misconfiguration, not a default. It is checked in the constructor rather than in
    // buildConnectUrl because `connectUrl()` is called from `openSocket()` OUTSIDE its try/catch — a
    // throw down there escapes the reconnect machinery entirely instead of surfacing to the caller.
    if (opts.auth.kind === "token" && !opts.accountId) {
      throw new Error(
        "LiveSyncClient: accountId is required with token auth (only cookie auth can fall back to the session's own tenant)",
      );
    }
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
    this.openTimeoutMs = opts.openTimeoutMs ?? 20_000;
    this.reseedTimeoutMs = opts.reseedTimeoutMs ?? 600_000;
    this.gapTimeoutMs = opts.gapTimeoutMs ?? 10_000;
    this.gapRetryLimit = opts.gapRetryLimit ?? 3;
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
  start(): Promise<void> {
    this.stopped = false;
    this.started = true;
    // RESET per boot. `start()` is restartable after `stop()`, and a stale `true` from a previous
    // cold boot would make the NEXT boot — warm, and therefore re-seeding nothing — absorb a resync
    // it should have honoured. Found while re-reading this path rather than reported; the same class
    // of staleness as the `bootTask` handle being nulled when it settles.
    this.bootColdSeeded = false;
    this.bootFailed = false;
    // The done deferred is created BEFORE the boot body runs (CTC-281 N2): stop() during the cold-seed
    // await used to find resolveDone still null and leave the returned promise pending forever — a
    // contract violation for a consumer awaiting start(). The boot body below is deliberately its OWN
    // async task raced against this deferred, because an `async start()` suspended at `await reseed()`
    // can never reach a `return done` — stop() must be able to resolve the caller regardless of the
    // boot phase (openSocket() already no-ops on stopped, so a late-settling seed is harmless).
    const done = new Promise<void>((resolve) => {
      this.resolveDone = resolve;
    });
    const boot = (async () => {
      // The WHOLE boot is an in-flight resync, not just the cold seed (CTC-114 review rounds 4 + 6).
      //
      // `requestResync()` — public as of 0.8.0 — is callable the moment start() returns its promise,
      // which is before ANY of this settles. Without the latch, `handleResync()`'s re-entrancy guard
      // read false and started a SECOND concurrent reseed: two seeds interleaving writes through a
      // non-reentrant consumer callback, then each completion calling openSocket() — and since
      // openSocket() overwrites `this.ws`, the first socket was orphaned, still delivering duplicate
      // frames and unreachable by stop().
      //
      // Round 4 latched only the cold seed. That was not enough: `createTelemetry()` below is awaited
      // BEFORE the seed, so with telemetry enabled the boot suspends in a window where `started` is
      // already true and the latch is not yet set. The latch therefore has to cover the entire body.
      //
      // Until this release the invariant held for free — a resync could only be driven by a server
      // frame, and a frame needs a socket, which does not exist until openSocket() below.
      //
      // The latch makes the request WAIT; whether it is then absorbed or honoured is decided in
      // requestResync() from `bootColdSeeded`, once this task has settled.
      //
      // Round 6 absorbed it on BOTH arms, arguing that a warm boot's `{type:"sync", after:<cursor>}`
      // is itself the catch-up. That was wrong (round 7), and wrong against this method's whole
      // reason for existing: a consumer calls requestResync() when it has discovered ON ITS OWN SIDE
      // that deltas can no longer catch its store up — the browser replica's dropped overflow buffer
      // is the motivating case. Replaying from the cursor cannot rebuild rows the consumer already
      // lost, so silently swallowing the request left it permanently inconsistent. Only a COLD boot
      // may absorb it, because that boot really is a full re-seed from /snapshot.
      this.resyncing = true;
      try {
        // Resolve the OTel seam ONCE up front (before the first reseed, so the seed span exists on the
        // cold-start path too). Keep the OFF path FULLY SYNCHRONOUS — no `await`, so a caller that opens
        // the socket and inspects it in the same tick still sees it (the boot body runs synchronously up
        // to its first await); only pay the async resolution (guarded dynamic import, or a
        // CatalystReplica passing its already-resolved instance) when telemetry is on.
        this.telemetry =
          this.telemetryConfig === undefined || this.telemetryConfig === false
            ? NOOP_TELEMETRY
            : await createTelemetry(this.telemetryConfig, {
                tracerName: DEFAULT_SCOPE_NAME,
                meterName: DEFAULT_SCOPE_NAME,
              });
        this.gapCounter = this.telemetry.counter(REPLICA_METRIC.gaps, {
          description: "Change-feed seq-gap lifecycle events (detected/healed/escalated).",
          unit: "{gap}",
        });
        const saved = this.getCursor();
        if (saved == null) {
          this.setStatus("resyncing");
          // Bounded like the resync-path reseed (CTC-281): a hanging COLD seed surfaces as a start()
          // rejection (the boot arm rejects) instead of a silent forever-"resyncing" start().
          await this.boundedReseed();
          // Only NOW may a request that waited on this boot be absorbed — this really was a full
          // re-seed from /snapshot. A warm boot sets nothing, so the waiter is honoured instead.
          this.bootColdSeeded = true;
        }
      } finally {
        // Must clear on the FAILURE arm too, or a failed boot latches the client into a state where
        // every later resync — and scheduleReconnect — is suppressed forever.
        this.resyncing = false;
      }
      this.openSocket();
    })();
    this.bootTask = boot;
    // Clear the handle once boot settles, so a resync arriving LONG after startup is never mistaken
    // for one that raced it — otherwise `bootColdSeeded` would absorb legitimate later requests
    // forever. The catch keeps a boot rejection from surfacing as an unhandled one on this arm; the
    // race below is what actually reports it.
    void boot
      .catch(() => {
        // LATCH the failure before the handle is dropped — `bootTask` is the transient record, this is
        // the durable one, and requestResync() has to be able to see it afterwards (round 9).
        this.bootFailed = true;
      })
      .then(() => {
        if (this.bootTask === boot) this.bootTask = null;
      });
    // Settles when stop() resolves the deferred, OR rejects if the boot (cold seed) fails — a boot
    // SUCCESS deliberately keeps waiting on `done` (the "runs forever" contract). Promise.race
    // attaches handlers to both arms, so a boot rejection after stop() is never an unhandled one.
    return Promise.race([done, boot.then(() => done)]);
  }

  /** Stop the client: close the socket, cancel any pending reconnect, resolve start(). Idempotent. */
  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer != null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    // Ask 4 (bounded teardown): the reseed deadline must not hold process exit for up to
    // reseedTimeoutMs. With it cleared a still-hanging reseed simply never settles its (now
    // irrelevant) await — every post-await path in handleResync/boot checks `stopped` first.
    this.clearReseedTimer();
    this.closeSocket();
    this.setStatus("stopped");
    const done = this.resolveDone;
    this.resolveDone = null;
    done?.();
    // AFTER resolving start()'s deferred, so the race below settles on `done` and a boot arm that
    // rejects from this abandon lands on an already-settled race rather than surfacing as the
    // outcome of start(). Settles a `requestResync()` a consumer is awaiting — see boundedReseed.
    const abandon = this.abandonReseed;
    this.abandonReseed = null;
    abandon?.();
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

  /**
   * The tenant label for telemetry. Attributes are `Record<string, string>`, and an empty string reads
   * as a MISSING attribute in Loki/Tempo — which silently merges every session-scoped browser client
   * into one unlabelled bucket. `"session"` names the case instead: this client is scoped to whatever
   * tenant the cookie resolves to.
   */
  private get tenantAttr(): string {
    return this.accountId ?? "session";
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
      [CATALYST_ATTR.tenant]: this.tenantAttr,
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
    this.socketOpened = false;
    this.clearConnectTimers(); // never stack deadlines across attempts (every teardown clears too)
    // Connect/open deadline (CTC-281): from here until onopen, THIS timer is the client's only
    // guaranteed pending work (the reconnectTimer that led here was already nulled). If the impl
    // never fires open/close/error — a stalled upgrade with no FIN/RST, or a buggy injected ws —
    // this converts the dead attempt into an ordinary backoff retry instead of a permanent
    // "connecting" wedge.
    if (this.openTimeoutMs > 0) {
      this.connectTimer = setTimeout(() => {
        this.connectTimer = null;
        // Late-timer guard (same discipline as onPongDeadline): a throttled tab can fire this after
        // onopen already ran, or after this socket was already replaced/torn down.
        if (this.stopped || this.ws !== ws || this.socketOpened) return;
        this.log("warn", `ws open timed out after ${this.openTimeoutMs}ms; forcing reconnect`);
        this.endConnectSpan(new Error("open timeout"));
        this.forceReconnect();
      }, this.openTimeoutMs);
    }
    ws.onopen = () => {
      this.socketOpened = true;
      this.clearConnectTimers(); // the attempt succeeded — the open deadline + error fallback die here
      this.backoff = this.backoffMs; // a successful open resets the backoff ramp
      this.setStatus("live");
      this.endConnectSpan();
      // Fresh connection: reset per-connection watchdog state, then start the idle-ping countdown.
      this.pongObserved = false;
      this.pingSentAt = 0;
      // Re-baseline gap detection from the durable cursor: it never advanced past an undelivered
      // frame, so the sync we are about to send re-requests any previously-pending hole anyway.
      this.clearGapState();
      this.deliveredSeq = this.getCursor() ?? -1;
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
      // Fallback (CTC-281): if the impl violates the spec and never follows error with close (undici
      // #3697/#3546), force the reconnect ourselves after a short grace. One-shot per socket, guarded
      // on identity — a spec-conforming onclose lands first, clears this timer, and reconnects
      // normally (scheduleReconnect's reconnectTimer check also prevents any double-schedule).
      if (this.errorFallbackTimer == null) {
        this.errorFallbackTimer = setTimeout(() => {
          this.errorFallbackTimer = null;
          if (this.stopped || this.ws !== ws) return; // onclose (or a teardown) already handled it
          this.log("warn", "ws error was never followed by close; forcing reconnect (CTC-281)");
          this.forceReconnect();
        }, ERROR_CLOSE_GRACE_MS);
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
    // Escalate past the graceful close (CTC-281): against a half-open peer the Close frame goes into
    // a black hole, and undici waits on the never-answered handshake with NO timeout — the ref'd TCP
    // handle then holds a supervised process's exit hostage for up to the OS retransmission timeout
    // (~minutes). Bun's WebSocket and the node 'ws' package both expose a non-standard `terminate()`
    // that destroys the connection immediately; duck-type it (structurally, never `as any`) and call
    // it when present. Handlers are already detached above, so a hard kill is behaviorally safe;
    // native/undici sockets simply lack the member and keep today's behavior (documented gap — on
    // Node, inject a 'ws'-package wsFactory if bounded process exit matters).
    const t = ws as WebSocketLike & { terminate?: () => void };
    if (typeof t.terminate === "function") {
      try {
        t.terminate();
      } catch {
        // best-effort — already destroyed
      }
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
      this.pongEverObserved = true; // CTC-281: capability proven for the client's LIFETIME
      this.probeFailures = 0;
      if (this.watchdogDegraded) {
        // The degraded slow re-probe just paid off (the server pongs after all — recovered mid-window
        // or upgraded): re-arm full-speed detection immediately (CTC-281).
        this.watchdogDegraded = false;
        this.log("info", "pong observed on a degraded watchdog; full-speed liveness detection re-armed (CTC-281)");
        this.armPing();
      }
      return;
    }
    if (frame.type === "head") {
      // Transport-internal end-of-pass nudge (CTL-1402): a reconcile pass appended change_log rows it
      // never broadcast individually, so this head seq may sit beyond our contiguous baseline with no
      // change frame to detect the hole FROM. Treat that as a gap to re-request — but the head frame
      // is itself NEVER delivered/applied and advances NO cursor. Not surfaced to onFrame/onChange
      // (same convention as pong); onInboundFrame already stamped it as liveness bytes.
      this.onHeadFrame(frame);
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
    // A change frame: live pushes and `{type:"sync"}` replays arrive through this one path by design.
    this._lastChangeFrameAt = Date.now();
    // Gap check (CTL-1402): only with a real baseline (deliveredSeq > 0 — a fresh/cursorless store has
    // nothing to be contiguous WITH). A frame beyond deliveredSeq+1 means the frames in between were
    // never delivered — re-request them instead of applying it (the replay will redeliver it in order).
    if (this.deliveredSeq > 0 && frame.seq > this.deliveredSeq + 1) {
      this.onGapFrame(frame);
      return;
    }
    // Contiguous — or a duplicate/out-of-order oldie (seq <= deliveredSeq), which is passed through
    // unchanged: the consumer's stale-guard already dedups it and its cursor never moves backward.
    try {
      this.onChange(frame);
    } catch (err) {
      this.log("error", `onChange failed for ${frame.entity} seq=${frame.seq}`, err);
    }
    if (frame.seq > this.deliveredSeq) {
      this.deliveredSeq = frame.seq;
      if (this.gap) {
        if (this.deliveredSeq >= this.gap.seqTo) {
          // The replay walked the whole detected hole: contiguity is restored, live frames resume.
          this.onGapHealed();
        } else {
          // Progress WITHIN the hole: refund the heal deadline. It measures "no progress for
          // gapTimeoutMs", NOT "not fully healed within gapTimeoutMs" — the steady-state first heal
          // replays thousands of frames (keyset-paginated server-side), and a slow consumer / heavy
          // first heal must not retry (overlapping replays on the same socket) or escalate to a full
          // /snapshot WHILE frames are actively landing. The retry budget still bounds a genuinely
          // STALLED heal (no frames at all for a full window).
          this.armGapDeadline();
        }
      }
    }
  }

  // ── Gap detection + self-healing re-request (CTL-1402) ──

  /**
   * A change frame arrived BEYOND the contiguous next seq: every frame in `(deliveredSeq, frame.seq)`
   * was silently dropped by the at-most-once live push. The frame is NOT applied and the baseline is
   * NOT advanced past the hole — the mirror's change_log is durable, so the client re-requests the
   * gap with the same `{type:"sync", after}` control frame it sends on every (re)connect, and the
   * mirror replays the missing rows as ordinary change frames through the same apply path. While a
   * re-request is in flight, further beyond-the-gap frames (in-flight live pushes the replay will
   * cover) are dropped the same way WITHOUT sending another sync — one request per gap episode.
   */
  private onGapFrame(frame: ChangeFrame): void {
    if (this.gap) return; // a re-request is already in flight; the replay redelivers this frame too
    this.gap = { seqFrom: this.deliveredSeq + 1, seqTo: frame.seq - 1, retries: 0 };
    this.recordGap("detected", this.gap);
    this.sendGapRequest();
  }

  /**
   * An end-of-pass head nudge (CTL-1402): `{type:"head", seq:N}` reports the mirror's current feed
   * head after a reconcile pass whose change_log rows were never individually broadcast. If N sits
   * beyond our contiguous baseline and no gap episode is pending, start one exactly as a beyond-gap
   * change frame would — the hole is `deliveredSeq+1..N` (N itself is a real, un-broadcast row, so —
   * unlike a change frame's trigger seq — it IS part of the hole) — and re-request it with the same
   * bounded retry/escalation machinery and telemetry. The head frame is never applied and never
   * advances a cursor. A head at/below the baseline is a no-op (already caught up); a head arriving
   * while a gap is already pending is ignored (the in-flight replay already covers up to the head).
   */
  private onHeadFrame(frame: HeadFrame): void {
    if (this.gap) return; // a re-request is pending; its replay already walks up to (past) this head
    if (this.deliveredSeq <= 0) return; // no baseline yet — nothing to be contiguous with (see gap check)
    if (frame.seq <= this.deliveredSeq) return; // caught up (or a stale head): no hole to re-request
    this.gap = { seqFrom: this.deliveredSeq + 1, seqTo: frame.seq, retries: 0 };
    this.recordGap("detected", this.gap);
    this.sendGapRequest();
  }

  /** Send (or re-send) the gap re-request from the current baseline and arm the heal deadline. */
  private sendGapRequest(): void {
    const req: SyncFrame = { type: "sync", after: this.deliveredSeq };
    try {
      this.ws?.send(JSON.stringify(req));
    } catch (err) {
      // A dead socket: leave the deadline armed — it retries/escalates, and a reconnect re-baselines.
      this.log("error", "gap re-request send failed", err);
    }
    this.armGapDeadline();
  }

  /** (Re)arm the gap heal deadline. Armed when a re-request is sent AND refreshed on every frame of
   *  heal progress (the delivery path), so the deadline measures "no PROGRESS for gapTimeoutMs" rather
   *  than "not fully healed within gapTimeoutMs". `gapTimeoutMs === 0` disables it (heals only via the
   *  replay itself or the next reconnect). Only ever called with a gap episode pending. */
  private armGapDeadline(): void {
    this.clearGapTimer();
    if (this.gapTimeoutMs > 0) {
      this.gapTimer = setTimeout(() => this.onGapTimeout(), this.gapTimeoutMs);
    }
  }

  /** The heal deadline elapsed with the hole still open: retry within budget, else escalate to the
   *  full re-seed path. Escalation — never silent acceptance — is what keeps a drop from becoming a
   *  permanent hole one level up. */
  private onGapTimeout(): void {
    this.gapTimer = null;
    const gap = this.gap;
    if (!gap || this.stopped || !this.ws) return;
    gap.retries += 1;
    if (gap.retries >= this.gapRetryLimit) {
      this.recordGap("escalated", gap);
      this.gap = null;
      void this.handleResync();
      return;
    }
    this.sendGapRequest();
  }

  /** The replay redelivered the whole detected hole (deliveredSeq reached seqTo contiguously). */
  private onGapHealed(): void {
    const gap = this.gap;
    if (!gap) return;
    this.gap = null;
    this.clearGapTimer();
    this.recordGap("healed", gap);
  }

  /** Emit the gap lifecycle signal: a structured `catalyst.replica.gap` log line (the fleet's primary,
   *  Loki-materialized channel — same convention as `catalyst.replica.apply`) + a `result`-style
   *  low-cardinality bump of the `catalyst.replica.gaps` counter. seq_from/seq_to/size ride the log
   *  line as VALUES, never labels. Levels: `detected` and `healed` log at INFO — a gap is the
   *  steady-state path (every reconcile pass punches one that heals via re-request), so ALERTING MUST
   *  KEY ON `escalated` ONLY (logged at ERROR); a gap that heals is routine and boring. */
  private recordGap(event: ReplicaGapEvent, gap: { seqFrom: number; seqTo: number; retries: number }): void {
    this.gapCounter.add(1, {
      [CATALYST_ATTR.tenant]: this.tenantAttr,
      [CATALYST_ATTR.gapEvent]: event,
    });
    this.log(event === "escalated" ? "error" : "info", REPLICA_LOG.gap, {
      event,
      seq_from: gap.seqFrom,
      seq_to: gap.seqTo,
      size: gap.seqTo - gap.seqFrom + 1,
      retries: gap.retries,
    });
  }

  private clearGapTimer(): void {
    if (this.gapTimer != null) {
      clearTimeout(this.gapTimer);
      this.gapTimer = null;
    }
  }

  /** Drop all gap state (pending hole + timer) — on (re)open and on the resync/re-seed path, both of
   *  which re-request/rebuild from the durable cursor and so supersede any pending re-request. */
  private clearGapState(): void {
    this.gap = null;
    this.clearGapTimer();
  }

  private clearReseedTimer(): void {
    if (this.reseedTimer != null) {
      clearTimeout(this.reseedTimer);
      this.reseedTimer = null;
    }
  }

  /**
   * Run the injected reseed() bounded by {@link LiveSyncClientOptions.reseedTimeoutMs} (CTC-281).
   * The injected callback is a trust boundary like the ws impl: while it runs there is NO socket and
   * scheduleReconnect is suppressed, so an unbounded await here was the last zero-timer wedge — the
   * deadline below is the pending timer that upholds the header invariant for the "resyncing" state.
   * On timeout the attempt is both CANCELLED and abandoned (CTC-114 review round 10). It used to be
   * abandoned only, justified as "the callback owns its own I/O bounds" — but the callback owning
   * bounds is exactly what makes a SECOND, independent deadline here dangerous. The browser seed is
   * bounded by network idleness and by per-RPC worker deadlines, both of which a legitimately slow
   * ~100 MB snapshot satisfies indefinitely; this total deadline could therefore fire on a seed that
   * was making honest progress, whereupon the transport reconnected while the callback kept writing.
   * Frames past the abandoned seed's cursor were then accepted by the socket and discarded by the
   * replica's paused queue, and a late seed completion left the socket advanced over a hole that
   * later deltas sealed for good.
   *
   * So the deadline now fires an AbortSignal FIRST and rejects second: whoever is told the attempt is
   * over is also told to stop. A late settle is still discarded via the `settled` latch, and a late
   * REJECTION is still swallowed so it can never surface as an unhandled rejection.
   */
  private boundedReseed(): Promise<number> {
    // Cancellation is scoped to THIS attempt. Aborting it must not disturb a successor.
    const cancel = new AbortController();
    const seed = this.reseed(cancel.signal);
    void seed.catch(() => {}); // an abandoned attempt's late rejection must never go unhandled
    // ALWAYS wrapped, even with the deadline disabled (CTC-114 review round 8). This used to
    // early-return the raw seed promise when `reseedTimeoutMs <= 0` — the documented way to turn the
    // deadline off — which skipped installing `abandonReseed` and so bypassed round 7's stop() fix
    // entirely on that path. Disabling the DEADLINE must not also disable teardown: the two are
    // independent, and `stop()` has to be able to settle an awaited `requestResync()` either way.
    return new Promise<number>((resolve, reject) => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | null = null;
      const finish = (fn: () => void): void => {
        if (settled) return; // stale settle: the deadline or stop() already took this attempt
        settled = true;
        if (timer !== null) {
          clearTimeout(timer);
          if (this.reseedTimer === timer) this.reseedTimer = null;
        }
        this.abandonReseed = null;
        fn();
      };
      /**
       * Give up on this attempt: tell it to stop, WAIT for it to unwind, then settle.
       *
       * Signalling alone was not enough (CTC-114 review round 12, P1). `abort()` only *initiates* the
       * consumer's cleanup — the browser seed still has to abort its fetch, let the supersede guard
       * trip, post its `seedAbort`, and run the `finally` that resumes its delta queue. Settling
       * immediately let `runResync()` reconnect after one backoff while that queue was still PAUSED,
       * so arriving frames were counted as delivered and then discarded: exactly the hole the queue's
       * discard/rollback pairing exists to prevent, re-opened from the other side.
       *
       * Bounded, because the whole point of this deadline is that the callback may be unresponsive: a
       * cleanup that itself hangs must not wedge the transport, so we settle anyway after a grace.
       */
      const giveUp = (err: Error): void => {
        if (settled) return;
        cancel.abort();
        // Only WAIT for a callback that can actually act on the signal. Arity is the honest test:
        // a zero-arg `reseed` (the node replica's `() => this.seedFromSnapshot()`, and every consumer
        // written before 0.8.0) cannot observe the abort, so there is no unwind to wait for and
        // pausing here would only add latency to the failure path. This also keeps the pre-round-12
        // behaviour exactly for those consumers.
        if (this.reseed.length === 0) {
          finish(() => reject(err));
          return;
        }
        const grace = setTimeout(
          () => finish(() => reject(err)),
          CANCEL_CLEANUP_GRACE_MS,
        );
        void seed
          .catch(() => undefined)
          .then(() => {
            clearTimeout(grace);
            finish(() => reject(err));
          });
      };
      if (this.reseedTimeoutMs > 0) {
        timer = setTimeout(
          () =>
            giveUp(
              new Error(
                `reseed did not settle within ${this.reseedTimeoutMs}ms; cancelled (CTC-281)`,
              ),
            ),
          this.reseedTimeoutMs,
        );
        this.reseedTimer = timer;
      }
      // stop() settles this wrapper (CTC-114 review round 7). Clearing the deadline is not enough:
      // if the injected reseed() never settles, nothing else ever settles THIS promise, and while
      // that was merely "an irrelevant await nobody holds" when boundedReseed was internal, the
      // public `requestResync()` is now awaited by consumers — so teardown or recovery code holding
      // that await hung forever on stop(). Rejecting rather than resolving keeps the outcome honest;
      // requestResync() catches it and still upholds its never-rejects contract.
      this.abandonReseed = () =>
        giveUp(new Error("client stopped while re-seeding"));
      seed.then(
        (cursor) => finish(() => resolve(cursor)),
        (err: unknown) => finish(() => reject(err instanceof Error ? err : new Error(String(err)))),
      );
    });
  }

  /**
   * Ask the client to drop the socket, re-seed, and reconnect from the fresh cursor — the same path a
   * server `{type:"resync"}` frame drives, exposed for a consumer that discovers ON ITS OWN SIDE that
   * its store can no longer be caught up by deltas.
   *
   * The browser replica is the motivating caller: when its delta queue overflows (or its applies keep
   * rejecting) the buffered frames are dropped, so the transport's notion of what has been delivered is
   * now ahead of what the store actually holds. Re-seeding through here — rather than calling the
   * `reseed` callback directly — is what makes that safe: this CLOSES THE SOCKET FIRST, so no live
   * frame interleaves with the snapshot and lands in the window that is in neither the snapshot nor the
   * store.
   *
   * Delegates to the existing resync path in full — same `resyncing` re-entrancy guard, same
   * `boundedReseed` deadline, same span, same failure→backoff behaviour. NEVER rejects: a failed
   * re-seed is already handled internally by re-entering the reconnect path, and this is called from
   * event handlers and `void` contexts where a rejection would surface as an unhandled promise.
   */
  async requestResync(): Promise<void> {
    if (this.stopped) return;
    // BEFORE start() there is nothing to resync (CTC-114 review round 5). `stopped` is false on a
    // never-started client, so it cannot carry this guard by itself. Reseeding here would run with no
    // lifecycle deferred and no telemetry resolved, and — worse — open a socket that the later real
    // start() would orphan: openSocket() overwrites `this.ws`, so the first socket keeps delivering
    // frames and stop() can no longer reach it. Ignore rather than throw: the contract above is that
    // this never rejects, and it is called from `void` contexts.
    if (!this.started) {
      this.log("warn", "requestResync() before start() — ignored");
      return;
    }
    // SERIALIZE behind an in-flight boot (CTC-114 review rounds 6 + 7). Running concurrently with it
    // meant two reseeds and two openSocket() calls, the second overwriting `this.ws` and orphaning the
    // first socket. Waiting — rather than dropping — is what keeps the WARM path correct: a warm boot
    // performs no re-seed, and this method's whole purpose is a consumer that has discovered its store
    // can no longer be caught up by deltas, which replaying from the cursor cannot fix.
    const boot = this.bootTask;
    if (boot) {
      await boot.catch(() => undefined); // the outcome is read from `bootFailed`, latched below
      if (this.stopped) return;
    }
    // A FAILED boot must not be recovered from here (CTC-114 review rounds 8 + 9). Round 7 swallowed
    // the rejection and carried straight on into handleResync — so a cold start whose /snapshot failed
    // would reject the caller's start(), sending the application into its boot-error path, and then a
    // later successful reseed here would quietly open a live socket underneath it. Round 8 read the
    // failure from the awaited task, which round 9 showed is not enough: `bootTask` is nulled when the
    // boot settles, so a request arriving after that microtask saw no failure at all. Checked OUTSIDE
    // the `if (boot)` for exactly that reason — the latch outlives the handle, until the next start().
    if (this.bootFailed) {
      this.log("warn", "requestResync() ignored — startup failed");
      return;
    }
    // A COLD boot re-seeded from /snapshot while we waited, which IS what was being asked for.
    if (boot && this.bootColdSeeded) {
      this.log("info", "requestResync() absorbed by the boot's cold seed");
      return;
    }
    // Unlike the frame path, this entry point can be called MID-BACKOFF: the queue overflowed while the
    // client was already waiting to reconnect. handleResync would then reopen the socket itself and the
    // pending timer would open a second one on top of it.
    if (this.reconnectTimer != null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    try {
      await this.handleResync();
    } catch (err) {
      // handleResync already catches its own reseed failure; this is the belt-and-braces guard for the
      // contract above (never reject) against a throw from a consumer callback it invokes.
      this.log("error", "requestResync failed", err);
    }
  }

  /**
   * Cursor underflow: the deltas we need were evicted from the service's retained change buffer. Close the socket
   * (so no live frame interleaves with the re-seed), re-seed via the injected callback, then reconnect
   * — which re-sends {type:"sync"} from the fresh cursor. `resyncing` guards against a second resync
   * frame and suppresses scheduleReconnect for the duration so we reopen exactly once.
   */
  private async handleResync(): Promise<void> {
    // RETURN THE RUNNING ONE, do not resolve immediately (CTC-114 review round 10). The re-entrancy
    // guard used to `return` bare, so a caller awaiting the public `requestResync()` while a
    // server-driven resync was already re-seeding got a promise that resolved AT ONCE — and then read
    // the store while the replacement snapshot was still being written into it. Awaiting recovery has
    // to mean recovery finished, whoever started it.
    //
    // This is the same seam as the boot serialization, which covered only `bootTask`: one handle for
    // "a re-seed is in flight", awaited by everything that needs it to be done.
    // `return await`, not `return` (CodeQL, round 11). Behaviour is identical today, but returning a
    // bare promise from an async function means a later `try`/`finally` added around this line would
    // settle BEFORE the awaited work — a foot-gun this file has been bitten by often enough to be
    // worth foreclosing, and it keeps this frame in the stack trace when the resync rejects.
    if (this.activeResync) return await this.activeResync;
    const run = this.runResync();
    this.activeResync = run;
    try {
      await run;
    } finally {
      if (this.activeResync === run) this.activeResync = null;
    }
  }

  /**
   * The resync body. Never call directly — `handleResync()` owns the in-flight handle.
   *
   * TWO FLAGS, ONE FACT — documented rather than consolidated here, deliberately. `resyncing` and
   * `activeResync` both mean "a re-seed is in flight", and they can disagree: the boot path sets
   * `resyncing` directly (so a resync cannot race startup) without ever creating an `activeResync`
   * handle. In that window a call landing here would hit the guard below and resolve silently, which
   * is the very shape round 10 fixed for the live path.
   *
   * It is NOT reachable today, and both reasons are load-bearing: `requestResync()` serialises behind
   * `bootTask` before it can get here, and the server-frame path needs a socket, which does not exist
   * until the boot opens one. Consolidating the two into a single handle is the right fix and belongs
   * with the wider lifecycle rework — not in a release candidate at round eleven.
   */
  private async runResync(): Promise<void> {
    if (this.resyncing) return;
    this.resyncing = true;
    // A full re-seed supersedes any pending gap re-request (it rebuilds from /snapshot wholesale) —
    // this also covers the server answering a gap re-request with {type:"resync"} (window eviction).
    this.clearGapState();
    this.setStatus("resyncing");
    this.closeSocket();
    let reseeded = false;
    try {
      // The reseed runs inside an ACTIVE span so the replica's seed span (the injected reseed IS
      // seedFromSnapshot) auto-parents under this resync span.
      await this.telemetry.withActiveSpan(
        REPLICA_SPAN.resync,
        { [CATALYST_ATTR.tenant]: this.tenantAttr },
        async () => {
          const cursor = await this.boundedReseed();
          this.log("info", `resynced, cursor=${cursor}`);
        },
      );
      reseeded = true;
    } catch (err) {
      this.log("error", "resync reseed failed; will retry on reconnect", err);
    } finally {
      this.resyncing = false;
    }
    if (this.stopped) return;
    if (reseeded) {
      // A completed re-seed reopens immediately — the store is fresh and the endpoint just served us.
      this.openSocket();
      return;
    }
    // A FAILED reseed re-enters the BACKOFF path (CTC-281): the old unconditional reopen made each
    // gap-escalate → /snapshot-fail → reopen cycle run hot (~30-40s of upgrade + replays + /snapshot
    // per client, fleet-wide, backoff reset on every open) against exactly the sick server the ticket
    // covers. scheduleReconnect converges identically once the endpoint recovers — just politely.
    this.setStatus("reconnecting");
    this.scheduleReconnect();
  }

  // ── Liveness watchdog (CTC-135) ──

  /** Epoch ms of the last inbound frame (change, pong, or malformed) — null before the first frame.
   *  Any inbound bytes prove the socket is alive; the catalyst daemon's stall classifier reads this to
   *  tell a quiet feed from a half-open socket (the per-frame timestamp it previously lacked). */
  get lastFrameAt(): number | null {
    return this._lastFrameAt;
  }

  /** Epoch ms of the last inbound CHANGE frame (live or replayed; delivered OR gap-dropped), or null
   *  before the first. Unlike {@link lastFrameAt} — which any bytes stamp, INCLUDING the mirror's
   *  watchdog auto-pongs — this only moves when the feed actually pushes data. Pairing the two lets a
   *  stall supervisor separate the three states CTL-1402 conflated: dead socket (both stale),
   *  healthy-but-unpushed socket (lastFrameAt fresh via pongs, lastChangeFrameAt stale while the
   *  mirror head advances), and a genuinely quiet feed (same signature, distinguished server-side). */
  get lastChangeFrameAt(): number | null {
    return this._lastChangeFrameAt;
  }

  /** Any inbound frame: stamp liveness, cancel a pending pong deadline (it was answered), and postpone
   *  the next ping so a busy stream never sends one. Runs synchronously in onmessage before parsing. */
  private onInboundFrame(): void {
    this._lastFrameAt = Date.now();
    this.clearPongDeadline();
    this.armPing();
  }

  /** (Re)arm the idle-ping timer. No-op when the watchdog is off or there is no live socket, so it is
   *  safe to call on every frame. A setTimeout chain (not setInterval): each frame resets it. A
   *  DEGRADED watchdog still arms — at {@link DEGRADED_PROBE_MULTIPLIER}x the interval — so detection
   *  is never permanently off (CTC-281): every live socket always has a probe pending. */
  private armPing(): void {
    this.clearPingTimer();
    if (this.pingIntervalMs <= 0 || this.stopped || !this.ws) return;
    const interval = this.watchdogDegraded
      ? this.pingIntervalMs * DEGRADED_PROBE_MULTIPLIER
      : this.pingIntervalMs;
    this.pingTimer = setTimeout(() => this.sendPing(), interval);
  }

  /** The feed has been idle for a full interval: send one liveness ping and start the pong deadline. A
   *  synchronous send throw means the socket is already dead — treat it as an unanswered probe now. */
  private sendPing(): void {
    this.pingTimer = null; // this timer just fired
    if (this.stopped || !this.ws) return;
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
   *  pong capability it is a genuine liveness timeout. If pong capability was proven EARLIER in this
   *  client's lifetime (CTC-281), a never-ponged connection is the incident signature — a half-open
   *  socket against a server we KNOW auto-pongs — so it too is a liveness failure and must NEVER count
   *  toward the feature-detect (during the Jul 17-23 windows, 3 such sockets permanently disabled
   *  detection). Only while capability is UNPROVEN does the failure count toward the DEGRADE — after
   *  PROBE_FAILURE_LIMIT never-ponged connections the watchdog backs its probes off to
   *  DEGRADED_PROBE_MULTIPLIER x pingIntervalMs (an old server without auto-pong costs one bounded
   *  reconnect per degraded window; a mid-incident restart — per-process latch reset — still detects
   *  the next half-open within one degraded window, never restart-only; CTC-281). Every path
   *  force-reconnects through the existing backoff. */
  private onProbeUnanswered(): void {
    if (this.pongObserved) {
      this.log("warn", "liveness timeout: no frame within the pong deadline; reconnecting");
    } else if (this.pongEverObserved) {
      // A distinct signal from the plain liveness timeout: a PROVEN-pong server delivered zero frames
      // on a whole connection — the fleet-incident shape (server accepts upgrades, feed is dead).
      this.log(
        "warn",
        "liveness timeout on a never-ponged connection against a proven-pong server (half-open or dead feed); reconnecting — watchdog stays armed (CTC-281)",
      );
    } else {
      this.probeFailures += 1;
      if (this.probeFailures >= PROBE_FAILURE_LIMIT && !this.watchdogDegraded) {
        this.watchdogDegraded = true;
        this.log(
          "warn",
          `liveness watchdog degraded after ${PROBE_FAILURE_LIMIT} unanswered probes (server may lack auto-pong); re-probing every ${DEGRADED_PROBE_MULTIPLIER}x pingIntervalMs (CTC-281)`,
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
    // A pending gap re-request dies with its socket: the timer must not fire against the next one
    // (whose onopen re-baselines and re-requests from the durable cursor anyway).
    this.clearGapTimer();
    // The connect deadline + onerror fallback are per-connection too (CTC-281) — they die with the
    // socket on both teardown routes (closeSocket and the server-close path), same as the pair above.
    this.clearConnectTimers();
  }

  /** Clear the per-connection connect/open deadline + onerror→onclose fallback (CTC-281). */
  private clearConnectTimers(): void {
    if (this.connectTimer != null) {
      clearTimeout(this.connectTimer);
      this.connectTimer = null;
    }
    if (this.errorFallbackTimer != null) {
      clearTimeout(this.errorFallbackTimer);
      this.errorFallbackTimer = null;
    }
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
  if (type === "head") return parsed as HeadFrame;
  return null;
}
