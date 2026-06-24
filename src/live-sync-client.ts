// @catalyst-cloud/sdk — LiveSyncClient: the isomorphic read-layer-over-WebSocket client.
//
// The push transport for the catalyst-cloud change-feed (ADR-0008/0009). It opens an OUTBOUND
// WebSocket to a tenant's Mirror Durable Object (`{baseUrl}{connectPath}`) and lets the DO push each
// change_log delta the instant it lands. On every (re)connect it sends `{type:"sync", after:<cursor>}`
// so the DO replays — in seq order — everything the consumer missed; a `{type:"resync"}` frame
// (cursor underflow) triggers a full re-seed via the injected `reseed` callback. It reconnects with
// capped exponential backoff and never throws out of the event loop.
//
// This is the STANDALONE extraction of apps/host-sync/src/live-client.ts. It is deliberately
// transport-only and storage-agnostic: it does NOT know about bun:sqlite, /snapshot, ADMIN_TOKEN, or
// any host-only assumption. Those are INJECTED:
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
// There is intentionally NO setInterval keepalive: the WHATWG WebSocket ping/pong is handled by the
// platform and a re-implemented heartbeat would re-pin the hibernating DO (ADR-0009), defeating the
// whole point of moving off SSE.

import type { ChangeFrame, ResyncFrame, ServerFrame, SyncFrame } from "./types.js";

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
 *  • `token`  — a SERVICE bearer (e.g. the host's ADMIN_TOKEN). The WHATWG WebSocket constructor
 *    cannot set an Authorization header, so the token rides the URL as `?token=`. The Worker
 *    constant-time compares it against the configured secret and STRIPS it before forwarding to the
 *    DO. Use this ONLY on a trusted backend (the host daemon) — never the browser.
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

/** Structured log levels (matches the host-sync logger contract). */
export type LogLevel = "info" | "warn" | "error";

/** Configuration for a {@link LiveSyncClient}. */
export interface LiveSyncClientOptions {
  /**
   * The Worker public origin, http(s), INCLUDING any versioned path prefix (e.g.
   * "https://api.example/api/v1"). The scheme is swapped to ws(s) and `connectPath` is appended
   * verbatim (path-preserving). A trailing slash is trimmed.
   */
  baseUrl: string;
  /** The tenant id = the Mirror DO name; sent as `?account=` on the connect URL. */
  accountId: string;
  /**
   * The connect route. Default "/connect"; the Worker dual-serves it at "/api/v1/connect" too, so a
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
   * Read the consumer's durable cursor (the last applied change_log.seq), or null/undefined if it has
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
  /** Injectable WebSocket factory (tests / a node polyfill). Defaults to `globalThis.WebSocket`. */
  wsFactory?: WebSocketFactory;
  /** Optional structured logger; defaults to console. */
  log?: (level: LogLevel, msg: string, extra?: unknown) => void;
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
export function buildConnectUrl(opts: {
  baseUrl: string;
  connectPath: string;
  accountId: string;
  auth: AuthStrategy;
}): string {
  const origin = toWsOrigin(opts.baseUrl.replace(/\/+$/, ""));
  const params = new URLSearchParams();
  if (opts.auth.kind === "token") params.set("token", opts.auth.token);
  params.set("account", opts.accountId);
  return `${origin}${opts.connectPath}?${params.toString()}`;
}

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
  private readonly wsFactory: WebSocketFactory;
  private readonly log: NonNullable<LiveSyncClientOptions["log"]>;

  private ws: WebSocketLike | null = null;
  private stopped = false;
  private resyncing = false;
  private backoff: number;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private resolveDone: (() => void) | null = null;

  constructor(opts: LiveSyncClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
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
    this.wsFactory = opts.wsFactory ?? defaultWsFactory;
    this.log =
      opts.log ??
      ((lvl, msg, extra) =>
        console[lvl === "error" ? "error" : "log"](`[catalyst-sdk:live] ${msg}`, extra ?? ""));
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
    const wsUrl = this.connectUrl();
    let ws: WebSocketLike;
    try {
      ws = this.wsFactory(wsUrl);
    } catch (err) {
      this.log("error", "ws construction failed; scheduling reconnect", err);
      this.setStatus("error");
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.onopen = () => {
      this.backoff = this.backoffMs; // a successful open resets the backoff ramp
      this.setStatus("live");
      this.sendSync();
    };
    ws.onmessage = (ev) => {
      void this.handleFrame(ev.data);
    };
    ws.onclose = () => {
      if (this.ws === ws) this.ws = null;
      if (!this.stopped && !this.resyncing) this.setStatus("reconnecting");
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

  /** Detach handlers BEFORE closing so a programmatic close can't re-enter scheduleReconnect. */
  private closeSocket(): void {
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

  /** Ask the DO to replay everything after our durable cursor. */
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
   * Cursor underflow: the deltas we need were evicted from the DO's change_log ring. Close the socket
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
      const cursor = await this.reseed();
      this.log("info", `resynced, cursor=${cursor}`);
    } catch (err) {
      this.log("error", "resync reseed failed; will retry on reconnect", err);
    } finally {
      this.resyncing = false;
    }
    if (!this.stopped) this.openSocket();
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
  return null;
}
