// events-socket.ts — CTC-5295. The client half of the events socket (`/api/v1/connect?channels=events`,
// catalyst-cloud CTC-4562): the tenant pushes every backbone event over a hibernatable WebSocket, so
// an event follower hears about an event when it happens instead of polling `/events/backbone`.
//
// This class only carries frames. It opens the socket, buffers each `events` frame, keeps the socket
// alive, and reconnects. Deciding what a frame means for the cache (apply it, or catch up through the
// backbone first) is `CatalystEventSync`'s job, in events.ts.
//
// ⭐ THE FRAME. `{type:"events", after, through, events[], gap?}`. Every backbone sequence in
// (after, through] was scanned for this socket, and on an unfiltered `fields=full` socket the events
// listed are all of them, as backbone lines. The next frame's `after` is this frame's `through`.
//
// ⭐ GENERATION. Bumped each time a socket confirms its subscription (the `channels` frame). Frames
// from before a reconnect stay buffered (they are real events), but the follower must catch up
// through the backbone once per generation, because nothing was pushed while the socket was down.
import { buildConnectUrl, type AuthStrategy, type WebSocketFactory, type WebSocketLike } from "./live-sync-client.js";
import { PING_FRAME, PONG_FRAME } from "./types.js";

/** One `events` frame as the cloud sent it. `events` is unparsed: the follower validates each one. */
export interface EventsFrame {
  after: number;
  through: number;
  gap: boolean;
  events: unknown[];
}

export type EventsSocketState = "idle" | "connecting" | "live" | "down" | "stopped";

export interface EventsSocketOptions {
  /** The cloud origin, e.g. `https://staging.catalystcloud.dev` (no `/api/v1`). */
  baseUrl: string;
  auth: Exclude<AuthStrategy, { kind: "cookie" }>;
  tenantId: string;
  /** Opens the socket. Defaults to the runtime's global `WebSocket`. */
  wsFactory?: WebSocketFactory;
  /** Silence after which the client sends a ping. Default 90 s. */
  pingAfterMs?: number;
  /** How long a ping, or a new socket's subscription, may go unanswered before reconnecting. Default 15 s. */
  pongTimeoutMs?: number;
  /** First reconnect delay after a failure, doubling to `reconnectMaxMs`. Default 1 s. */
  reconnectMinMs?: number;
  reconnectMaxMs?: number;
  /** Buffered frames past this count, or past `maxBufferedBytes`, are dropped and reported as an overflow. */
  maxBufferedFrames?: number;
  maxBufferedBytes?: number;
}

export interface EventsSocketBatch {
  frames: EventsFrame[];
  /** Frames were dropped since the last take; the follower must catch up through the backbone. */
  overflowed: boolean;
  /** The generation the frames were taken under. */
  generation: number;
}

const DEFAULT_PING_AFTER_MS = 90_000;
const DEFAULT_PONG_TIMEOUT_MS = 15_000;
const DEFAULT_RECONNECT_MIN_MS = 1_000;
const DEFAULT_RECONNECT_MAX_MS = 30_000;
const DEFAULT_MAX_BUFFERED_FRAMES = 1_000;
const DEFAULT_MAX_BUFFERED_BYTES = 8 * 1024 * 1024;
/** The mirror's reauthenticate close codes: the credential bound passed, so reconnect at once. */
const REAUTH_CLOSE_CODES = new Set([4401, 4410]);
/** A socket that lived shorter than this before a reauth close reconnects with backoff, not at once. */
const MIN_HEALTHY_LIFETIME_MS = 1_000;

/** The runtime's global WebSocket, or null when it has none (then a follower polls). */
export function globalWebSocketFactory(): WebSocketFactory | null {
  const Ctor = (globalThis as { WebSocket?: new (url: string) => WebSocketLike }).WebSocket;
  return typeof Ctor === "function" ? (url) => new Ctor(url) : null;
}

/** The events socket URL. The token rides `?token=` because a WebSocket cannot set a header. */
export function eventsSocketUrl(options: Pick<EventsSocketOptions, "baseUrl" | "auth" | "tenantId">, token?: string): string {
  return buildConnectUrl({
    baseUrl: options.baseUrl,
    connectPath: "/api/v1/connect",
    accountId: options.tenantId,
    auth: options.auth,
    ...(token !== undefined ? { bearerToken: token } : {}),
    extraParams: { channels: "events", fields: "full" },
  });
}

function closeCode(event: unknown): number | undefined {
  const code = (event as { code?: unknown } | null | undefined)?.code;
  return typeof code === "number" ? code : undefined;
}

function parseEventsFrame(value: Record<string, unknown>): EventsFrame | null {
  const { after, through, events } = value;
  if (
    !Number.isSafeInteger(after) ||
    !Number.isSafeInteger(through) ||
    (through as number) < (after as number) ||
    !Array.isArray(events)
  )
    return null;
  return { after: after as number, through: through as number, gap: value.gap === true, events };
}

export class EventsChannelClient {
  private readonly factory: WebSocketFactory;
  private ws: WebSocketLike | null = null;
  private current: EventsSocketState = "idle";
  private gen = 0;
  private frames: EventsFrame[] = [];
  private bufferedBytes = 0;
  private overflow = false;
  private backoff: number;
  private openedAt = 0;
  /** Bumped on every teardown, so a bearer token resolving late cannot open a superseded socket. */
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private silenceTimer: ReturnType<typeof setTimeout> | null = null;
  private answerTimer: ReturnType<typeof setTimeout> | null = null;
  private waiters: Array<() => void> = [];

  constructor(private readonly options: EventsSocketOptions) {
    const factory = options.wsFactory ?? globalWebSocketFactory();
    if (!factory) throw new Error("events socket: no WebSocket available; pass wsFactory");
    this.factory = factory;
    this.backoff = options.reconnectMinMs ?? DEFAULT_RECONNECT_MIN_MS;
  }

  get state(): EventsSocketState {
    return this.current;
  }

  /** Bumped each time a socket confirms its subscription. */
  get generation(): number {
    return this.gen;
  }

  hasPending(): boolean {
    return this.frames.length > 0 || this.overflow;
  }

  /** Take every buffered frame, and whether any were dropped, in arrival order. */
  take(): EventsSocketBatch {
    const batch = { frames: this.frames, overflowed: this.overflow, generation: this.gen };
    this.frames = [];
    this.bufferedBytes = 0;
    this.overflow = false;
    return batch;
  }

  /**
   * Resolves on the next buffered frame or the next time a socket goes live. Never rejects; an abort
   * only unregisters the waiter, so a long outage does not pile them up.
   */
  changed(signal?: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      if (signal?.aborted) return;
      this.waiters.push(resolve);
      signal?.addEventListener(
        "abort",
        () => {
          this.waiters = this.waiters.filter((waiter) => waiter !== resolve);
        },
        { once: true },
      );
    });
  }

  start(): void {
    if (this.current !== "idle" && this.current !== "stopped") return;
    this.current = "connecting";
    void this.open();
  }

  stop(): void {
    this.current = "stopped";
    this.teardown();
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.notify();
  }

  private notify(): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const resolve of waiters) resolve();
  }

  private async open(): Promise<void> {
    const attempt = ++this.attempt;
    let token: string | undefined;
    if (this.options.auth.kind === "bearer") {
      try {
        token = await this.options.auth.getToken();
      } catch {
        if (attempt === this.attempt && this.current !== "stopped") this.fail(undefined);
        return;
      }
      if (attempt !== this.attempt || this.current === "stopped") return;
    }
    let ws: WebSocketLike;
    try {
      ws = this.factory(eventsSocketUrl(this.options, token));
    } catch {
      this.fail(undefined);
      return;
    }
    this.ws = ws;
    this.openedAt = Date.now();
    // The subscription must be confirmed within the answer window, or the socket is presumed dead.
    this.armAnswer();
    ws.onmessage = (event) => this.onMessage(event.data);
    ws.onclose = (event) => this.fail(closeCode(event));
    // Some runtimes fire error without a close; treat it as one.
    ws.onerror = () => this.fail(undefined);
  }

  private onMessage(data: unknown): void {
    this.clearAnswer();
    this.armSilence();
    if (typeof data !== "string") return;
    if (data === PONG_FRAME) return;
    let value: unknown;
    try {
      value = JSON.parse(data);
    } catch {
      return;
    }
    if (!value || typeof value !== "object") return;
    const frame = value as Record<string, unknown>;
    if (frame.type === "channels") {
      this.current = "live";
      this.gen++;
      this.backoff = this.options.reconnectMinMs ?? DEFAULT_RECONNECT_MIN_MS;
      this.notify();
      return;
    }
    if (frame.type !== "events") return;
    const parsed = parseEventsFrame(frame);
    if (!parsed) {
      // A frame the client cannot read is a hole in the chain; the follower must catch up.
      this.overflow = true;
      this.notify();
      return;
    }
    const bytes = data.length;
    const maxFrames = this.options.maxBufferedFrames ?? DEFAULT_MAX_BUFFERED_FRAMES;
    const maxBytes = this.options.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES;
    if (this.frames.length + 1 > maxFrames || this.bufferedBytes + bytes > maxBytes) {
      // Everything buffered is in the backbone already, so dropping it costs one catch-up, never an event.
      this.frames = [];
      this.bufferedBytes = 0;
      this.overflow = true;
    } else {
      this.frames.push(parsed);
      this.bufferedBytes += bytes;
    }
    this.notify();
  }

  /** The socket closed or failed: tear it down and schedule the next attempt. */
  private fail(code: number | undefined): void {
    const lived = this.openedAt > 0 ? Date.now() - this.openedAt : 0;
    this.teardown();
    if (this.current === "stopped") return;
    this.current = "down";
    const immediate = code !== undefined && REAUTH_CLOSE_CODES.has(code) && lived >= MIN_HEALTHY_LIFETIME_MS;
    const delay = immediate ? 0 : this.backoff;
    if (!immediate)
      this.backoff = Math.min(this.options.reconnectMaxMs ?? DEFAULT_RECONNECT_MAX_MS, this.backoff * 2);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.current === "stopped") return;
      this.current = "connecting";
      void this.open();
    }, delay);
  }

  private teardown(): void {
    this.attempt++;
    this.clearAnswer();
    if (this.silenceTimer) clearTimeout(this.silenceTimer);
    this.silenceTimer = null;
    this.openedAt = 0;
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
      // Already closed.
    }
    const killable = ws as WebSocketLike & { terminate?: () => void };
    if (typeof killable.terminate === "function") {
      try {
        killable.terminate();
      } catch {
        // Already destroyed.
      }
    }
  }

  private armSilence(): void {
    if (this.silenceTimer) clearTimeout(this.silenceTimer);
    this.silenceTimer = setTimeout(() => {
      this.silenceTimer = null;
      const ws = this.ws;
      if (!ws) return;
      try {
        ws.send(PING_FRAME);
      } catch {
        this.fail(undefined);
        return;
      }
      this.armAnswer();
    }, this.options.pingAfterMs ?? DEFAULT_PING_AFTER_MS);
  }

  private armAnswer(): void {
    this.clearAnswer();
    this.answerTimer = setTimeout(() => {
      this.answerTimer = null;
      this.fail(undefined);
    }, this.options.pongTimeoutMs ?? DEFAULT_PONG_TIMEOUT_MS);
  }

  private clearAnswer(): void {
    if (this.answerTimer) clearTimeout(this.answerTimer);
    this.answerTimer = null;
  }
}
