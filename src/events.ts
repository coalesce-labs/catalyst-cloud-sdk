import {
  EventHistoryGapError,
  EventSyncHttpError,
  parseEvent,
  readCurrentHead,
  readPage,
} from "./events-backbone.js";
export { EventHistoryGapError, eventIdentity } from "./events-backbone.js";
import {
  cacheSegmentIndex,
  compareCacheSegments,
  isCacheSegment,
} from "./events-cache-reader.js";
import {
  mkdir,
  open,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  truncate,
  watch,
  writeFile,
} from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import type { CatalystEvent } from "@catalyst-cloud/schema";
import type { AuthStrategy, WebSocketFactory } from "./live-sync-client.js";
import {
  EventsChannelClient,
  globalWebSocketFactory,
  type EventsFrame,
  type EventsSocketOptions,
} from "./events-socket.js";
import {
  claimWriterLock,
  type WriterGuardOptions,
  type WriterLockHandle,
} from "./replica/writer-lock.js";

export type { CatalystEvent } from "@catalyst-cloud/schema";
export {
  EventsChannelClient,
  eventsSocketUrl,
  type EventsFrame,
  type EventsSocketBatch,
  type EventsSocketOptions,
  type EventsSocketState,
} from "./events-socket.js";

const MAX_EVENT_LINE_BYTES = 1024 * 1024;
const DEFAULT_IDLE_MIN_MS = 5_000;
const DEFAULT_IDLE_MAX_MS = 30_000;
const DEFAULT_RETAIN_DAYS = 7;
const DEFAULT_MAX_CACHE_BYTES = 256 * 1024 * 1024;
/** Backbone pages one catch-up may read before handing back to the loop. */
const MAX_CATCH_UP_PAGES = 20;

export interface EventCachePaths {
  directory: string;
  cursor: string;
  lock: string;
}

export interface EventSyncOptions {
  baseUrl: string;
  auth: Exclude<AuthStrategy, { kind: "cookie" }>;
  tenantId: string;
  directory?: string;
  fetch?: typeof fetch;
  requestTimeoutMs?: number;
  idleMinMs?: number;
  idleMaxMs?: number;
  retainDays?: number;
  maxCacheBytes?: number;
  writerGuard?: WriterGuardOptions;
  now?: () => Date;
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
  /**
   * Take events from the tenant's events socket instead of polling (CTC-5295). Default true when the
   * runtime has a global `WebSocket` or `wsFactory` is given. The socket opens on `start()` or
   * `waitForChange()`, never on a bare `syncOnce()`, and closes on `stop()`.
   */
  push?: boolean;
  /** Opens the events socket. Defaults to the runtime's global `WebSocket`. */
  wsFactory?: WebSocketFactory;
  /** Events socket timing and buffer bounds; the defaults suit a long-running follower. */
  socket?: Pick<
    EventsSocketOptions,
    | "pingAfterMs"
    | "pongTimeoutMs"
    | "reconnectMinMs"
    | "reconnectMaxMs"
    | "maxBufferedFrames"
    | "maxBufferedBytes"
  >;
}

export interface EventSyncStatus {
  state: "idle" | "syncing" | "stopped" | "failed";
  cursor: number | null;
  head: number | null;
  lastSyncAt: string | null;
  error: string | null;
  /** How new events arrive: `push` while the events socket is live, else `poll`. */
  transport?: "push" | "poll";
}

export interface EventSyncResult {
  appended: number;
  cursor: number;
  head: number;
}

export function defaultEventCacheDirectory(tenantId: string): string {
  const state =
    (globalThis as { process?: { env?: Record<string, string | undefined> } })
      .process?.env?.XDG_STATE_HOME || join(homedir(), ".local", "state");
  return join(state, "catalyst", "events", tenantId, "backbone");
}

export function eventCachePaths(
  tenantId: string,
  directory?: string,
): EventCachePaths {
  const root = directory ?? defaultEventCacheDirectory(tenantId);
  return {
    directory: root,
    cursor: join(root, "cursor.json"),
    lock: join(root, ".sync"),
  };
}

function segmentName(date: Date, index = 0): string {
  const day = date.toISOString().slice(0, 10);
  return index === 0
    ? `${day}.jsonl`
    : `${day}-${String(index).padStart(3, "0")}.jsonl`;
}

async function writeJsonAtomic(path: string, value: unknown): Promise<void> {
  const pid = (globalThis as { process?: { pid?: number } }).process?.pid ?? 0;
  const temporary = `${path}.${pid}.tmp`;
  await writeFile(temporary, JSON.stringify(value) + "\n", { mode: 0o600 });
  await rename(temporary, path);
}

interface CursorState {
  cursor: number;
  floor: number;
}

async function readCursor(path: string): Promise<CursorState | null> {
  try {
    const value = JSON.parse(await readFile(path, "utf8")) as {
      cursor?: unknown;
    };
    return typeof value.cursor === "number" &&
      Number.isSafeInteger(value.cursor) &&
      value.cursor >= 0
      ? {
          cursor: value.cursor,
          floor:
            typeof (value as { floor?: unknown }).floor === "number" &&
            Number.isSafeInteger((value as { floor: number }).floor)
              ? (value as { floor: number }).floor
              : -1,
        }
      : null;
  } catch (error) {
    if ((error as { code?: string }).code === "ENOENT") return null;
    throw error;
  }
}

async function repairTail(directory: string): Promise<number | null> {
  const segments = (await readdir(directory))
    .filter(isCacheSegment)
    .sort(compareCacheSegments);
  for (const name of segments.reverse()) {
    const path = join(directory, name);
    const bytes = await readFile(path);
    if (bytes.length === 0) continue;
    const lastNewline = bytes.lastIndexOf(10);
    if (lastNewline !== bytes.length - 1)
      await truncate(path, lastNewline < 0 ? 0 : lastNewline + 1);
    const complete = bytes
      .subarray(0, lastNewline < 0 ? 0 : lastNewline)
      .toString("utf8")
      .trimEnd();
    const line = complete.slice(complete.lastIndexOf("\n") + 1);
    if (line) return parseEvent(JSON.parse(line)).sequence;
  }
  return null;
}

async function retainBoundedSegments(
  directory: string,
  now: Date,
  retainDays: number,
  maxBytes: number,
  current: string,
  floor: number,
  publishFloor: (floor: number) => Promise<void>,
): Promise<number> {
  const segments = (await readdir(directory))
    .filter(isCacheSegment)
    .sort(compareCacheSegments);
  const sizes = new Map<string, number>();
  for (const name of segments)
    sizes.set(name, (await stat(join(directory, name))).size);
  let total = [...sizes.values()].reduce((sum, size) => sum + size, 0);
  const cutoff = new Date(now);
  cutoff.setUTCDate(cutoff.getUTCDate() - retainDays);
  const remove: string[] = [];
  for (const name of segments) {
    if (name === current) continue;
    const expired = name.slice(0, 10) < cutoff.toISOString().slice(0, 10);
    if (!expired && total <= maxBytes) continue;
    remove.push(name);
    total -= sizes.get(name) ?? 0;
  }
  for (const name of remove) {
    const body = await readFile(join(directory, name), "utf8");
    const lines = body.trimEnd().split("\n");
    const last = lines.at(-1);
    if (last) floor = Math.max(floor, parseEvent(JSON.parse(last)).sequence);
  }
  // A reader may open segments while retention runs. Publish the conservative floor before
  // removing any bytes, so its checkpoint recheck cannot prove a range whose files disappeared.
  if (remove.length > 0) await publishFloor(floor);
  await Promise.all(
    remove.map((name) => rm(join(directory, name), { force: true })),
  );
  return floor;
}

async function appendBoundedRows(
  directory: string,
  date: Date,
  rows: Array<{ line: string }>,
  maxBytes: number,
): Promise<string> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < MAX_EVENT_LINE_BYTES)
    throw new Error("event sync: maxCacheBytes must be at least 1 MiB");
  const day = date.toISOString().slice(0, 10);
  const existing = (await readdir(directory))
    .filter((name) => name.startsWith(day) && isCacheSegment(name))
    .sort(compareCacheSegments);
  const latest = existing.at(-1);
  // Retention removes old files, so their count cannot identify the current segment.
  let index = latest === undefined ? 0 : cacheSegmentIndex(latest);
  let name = segmentName(date, index);
  let size = 0;
  try {
    size = (await stat(join(directory, name))).size;
  } catch (error) {
    if ((error as { code?: string }).code !== "ENOENT") throw error;
  }
  let handle = await open(join(directory, name), "a", 0o600);
  try {
    for (const row of rows) {
      const line = `${row.line}\n`;
      const bytes = new TextEncoder().encode(line).byteLength;
      if (size > 0 && size + bytes > maxBytes) {
        await handle.sync();
        await handle.close();
        name = segmentName(date, ++index);
        handle = await open(join(directory, name), "a", 0o600);
        size = 0;
      }
      await handle.write(line);
      size += bytes;
    }
    await handle.sync();
  } finally {
    await handle.close();
  }
  return name;
}

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    const finish = () => {
      signal.removeEventListener("abort", abort);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    const abort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
      reject(signal.reason);
    };
    signal.addEventListener("abort", abort, { once: true });
  });
}

export class CatalystEventSync {
  readonly paths: EventCachePaths;
  private readonly now: () => Date;
  private readonly sleeper: (ms: number, signal: AbortSignal) => Promise<void>;
  private lock: WriterLockHandle | null = null;
  private controller: AbortController | null = null;
  private running: Promise<void> | null = null;
  private recoveredCursor: number | null = null;
  private floor = -1;
  private socket: EventsChannelClient | null = null;
  /** The socket generation the cache last caught up under; a new one needs a backbone catch-up. */
  private syncedGeneration = -1;
  private catchUpPending = false;
  private current: EventSyncStatus = {
    state: "stopped",
    cursor: null,
    head: null,
    lastSyncAt: null,
    error: null,
  };

  constructor(private readonly options: EventSyncOptions) {
    this.paths = eventCachePaths(options.tenantId, options.directory);
    this.now = options.now ?? (() => new Date());
    this.sleeper = options.sleep ?? abortableSleep;
  }

  status(): EventSyncStatus {
    return { ...this.current };
  }

  async syncOnce(
    signal: AbortSignal = new AbortController().signal,
  ): Promise<EventSyncResult> {
    await mkdir(this.paths.directory, { recursive: true, mode: 0o700 });
    if (!this.lock) {
      this.lock = claimWriterLock(
        this.paths.lock,
        this.options.writerGuard ?? {},
      );
      this.recoveredCursor = await repairTail(this.paths.directory);
    }
    this.current = { ...this.current, state: "syncing", error: null };
    const checkpoint = await readCursor(this.paths.cursor);
    this.floor = Math.max(this.floor, checkpoint?.floor ?? -1);
    let cursor = Math.max(
      checkpoint?.cursor ?? -1,
      this.recoveredCursor ?? -1,
      this.current.cursor ?? -1,
    );
    if (cursor < 0) {
      cursor = await this.readCurrentHead(signal);
      this.floor = cursor;
    }
    const live = this.socket?.state === "live";
    const result = live
      ? await this.readPushed(cursor, signal)
      : { ...(await this.readPage(cursor, signal)), through: null };
    const now = this.now();
    const maxCacheBytes = this.options.maxCacheBytes ?? DEFAULT_MAX_CACHE_BYTES;
    let segment = segmentName(now);
    if (result.rows.length > 0) {
      segment = await appendBoundedRows(
        this.paths.directory,
        now,
        result.rows,
        maxCacheBytes,
      );
      cursor = result.rows[result.rows.length - 1]!.event.sequence;
      this.recoveredCursor = cursor;
    }
    // Every sequence through a pushed frame's `through` was scanned, events or not.
    if (result.through !== null && result.through > cursor)
      cursor = result.through;
    this.floor = await retainBoundedSegments(
      this.paths.directory,
      now,
      this.options.retainDays ?? DEFAULT_RETAIN_DAYS,
      maxCacheBytes,
      segment,
      this.floor,
      (floor) =>
        writeJsonAtomic(this.paths.cursor, { version: 1, cursor, floor }),
    );
    await writeJsonAtomic(this.paths.cursor, {
      version: 1,
      cursor,
      floor: this.floor,
    });
    this.current = {
      state: "idle",
      cursor,
      head: result.head,
      lastSyncAt: this.now().toISOString(),
      error: null,
      transport: live ? "push" : "poll",
    };
    return { appended: result.rows.length, cursor, head: result.head };
  }

  start(): Promise<void> {
    if (this.running) return this.running;
    this.controller = new AbortController();
    this.running = this.loop(this.controller.signal).finally(() => {
      this.closeSocket();
      if (this.current.state !== "failed")
        this.current = { ...this.current, state: "stopped" };
      this.lock?.release();
      this.lock = null;
      this.recoveredCursor = null;
      this.running = null;
      this.controller = null;
    });
    return this.running;
  }

  async stop(): Promise<void> {
    const controller = this.controller;
    if (!this.running) {
      this.closeSocket();
      this.lock?.release();
      this.lock = null;
      this.current = { ...this.current, state: "stopped" };
      return;
    }
    controller?.abort(new Error("event sync stopped"));
    try {
      await this.running;
    } catch (error) {
      if (!controller?.signal.aborted) throw error;
    }
  }

  private async loop(signal: AbortSignal): Promise<void> {
    let idle = this.options.idleMinMs ?? DEFAULT_IDLE_MIN_MS;
    const maxIdle = this.options.idleMaxMs ?? DEFAULT_IDLE_MAX_MS;
    try {
      while (!signal.aborted) {
        let failed = false;
        try {
          const live = this.socket?.state === "live";
          const result = await this.syncOnce(signal);
          idle =
            result.appended > 0
              ? (this.options.idleMinMs ?? DEFAULT_IDLE_MIN_MS)
              : Math.min(maxIdle, idle * 2);
          // A partial catch-up goes straight on to the next page.
          if (live && this.catchUpPending) continue;
        } catch (error) {
          if (
            error instanceof EventHistoryGapError ||
            (error instanceof EventSyncHttpError && error.status < 500)
          )
            throw error;
          this.current = {
            ...this.current,
            state: "idle",
            error: error instanceof Error ? error.message : String(error),
          };
          idle = Math.min(maxIdle, idle * 2);
          failed = true;
        }
        if (failed) {
          // A failed read backs off whatever the socket says: a pending catch-up would otherwise
          // end every wait at once and retry the backbone with no pause.
          this.ensureSocket();
          await this.sleeper(idle, signal);
        } else {
          // While the socket is live a wait costs no request, so wait the longest; a frame ends it early.
          await this.waitForChange(
            this.socket?.state === "live" ? maxIdle : idle,
            signal,
          );
        }
      }
    } catch (error) {
      if (signal.aborted) return;
      this.current = {
        ...this.current,
        state: "failed",
        error: error instanceof Error ? error.message : String(error),
      };
      throw error;
    }
  }

  /**
   * Wait up to `maxMs` for something to sync: a pushed frame, or the events socket going live (which
   * needs a catch-up). Opens the events socket on first call when push is on. Resolves true when a
   * change ended the wait, false on timeout; rejects only when `signal` aborts. Without a socket it
   * is a plain sleep, so a caller's own loop can use it either way.
   */
  async waitForChange(maxMs: number, signal: AbortSignal): Promise<boolean> {
    const socket = this.ensureSocket();
    if (!socket) {
      await this.sleeper(maxMs, signal);
      return false;
    }
    if (socket.hasPending() || this.needsCatchUp(socket)) return true;
    const waker = new AbortController();
    const woke = socket.changed(waker.signal).then(() => true);
    const slept = this.sleeper(
      maxMs,
      AbortSignal.any([signal, waker.signal]),
    ).then(
      () => false,
      (error: unknown) => {
        if (signal.aborted) throw error;
        return true;
      },
    );
    try {
      return await Promise.race([woke, slept]);
    } finally {
      waker.abort();
    }
  }

  private ensureSocket(): EventsChannelClient | null {
    if (this.socket) return this.socket;
    if (this.options.push === false) return null;
    const wsFactory = this.options.wsFactory ?? globalWebSocketFactory();
    if (!wsFactory) return null;
    this.socket = new EventsChannelClient({
      baseUrl: this.options.baseUrl.replace(/\/$/, ""),
      auth: this.options.auth,
      tenantId: this.options.tenantId,
      wsFactory,
      ...this.options.socket,
    });
    this.socket.start();
    return this.socket;
  }

  private closeSocket(): void {
    this.socket?.stop();
    this.socket = null;
    this.syncedGeneration = -1;
    this.catchUpPending = false;
  }

  private needsCatchUp(socket: EventsChannelClient): boolean {
    return (
      socket.state === "live" &&
      (socket.generation !== this.syncedGeneration || this.catchUpPending)
    );
  }

  /**
   * The push path. Apply buffered frames that chain from the cursor with no request. Read the backbone
   * from the cursor instead when this socket generation has not caught up yet (subscribe first: the
   * frames buffered meanwhile are deduped by sequence after), when a frame starts past the cursor, is
   * flagged `gap`, or carries an event without its payload, or when the buffer overflowed.
   */
  private async readPushed(
    cursor: number,
    signal: AbortSignal,
  ): Promise<{
    head: number;
    through: number;
    rows: Array<{ event: CatalystEvent; line: string }>;
  }> {
    const socket = this.socket!;
    const batch = socket.take();
    let catchUp = batch.overflowed || this.needsCatchUp(socket);
    // Until this read succeeds, the next one must catch up: the frames taken here are gone.
    this.catchUpPending = true;
    const rows: Array<{ event: CatalystEvent; line: string }> = [];
    let head = this.current.head ?? cursor;
    let pending: EventsFrame[] = batch.frames;
    let pages = 0;
    for (;;) {
      const applied = this.applyFrames(cursor, pending);
      rows.push(...applied.rows);
      cursor = applied.cursor;
      head = Math.max(head, cursor);
      pending = applied.rest;
      if (!catchUp && !applied.broken) break;
      catchUp = false;
      // Everything a frame carried is already in the backbone, so reading from the cursor covers the
      // frame that broke the chain too; the frames after it are then deduped by sequence.
      for (;;) {
        const page = await this.readPage(cursor, signal);
        pages++;
        rows.push(...page.rows);
        if (page.rows.length > 0)
          cursor = page.rows[page.rows.length - 1]!.event.sequence;
        head = Math.max(head, page.head);
        if (page.rows.length === 0 || cursor >= page.head) break;
        if (pages >= MAX_CATCH_UP_PAGES) {
          // Leave the rest to the next sync; the loop runs it at once.
          return { head, through: cursor, rows };
        }
      }
    }
    this.syncedGeneration = batch.generation;
    this.catchUpPending = false;
    return { head, through: cursor, rows };
  }

  /** Apply frames that chain from `cursor`; stop at the first one that does not. */
  private applyFrames(
    cursor: number,
    frames: EventsFrame[],
  ): {
    cursor: number;
    rows: Array<{ event: CatalystEvent; line: string }>;
    broken: boolean;
    rest: EventsFrame[];
  } {
    const rows: Array<{ event: CatalystEvent; line: string }> = [];
    for (let index = 0; index < frames.length; index++) {
      const frame = frames[index]!;
      if (frame.through <= cursor) continue;
      const fresh = frame.events.filter(
        (value) =>
          ((value as { sequence?: unknown } | null)?.sequence as number) >
          cursor,
      );
      if (
        frame.gap ||
        frame.after > cursor ||
        fresh.some(
          (value) =>
            (value as { payloadOmitted?: unknown }).payloadOmitted === true,
        )
      )
        return { cursor, rows, broken: true, rest: frames.slice(index + 1) };
      for (const value of fresh) {
        const event = parseEvent(value);
        if (event.tenantId !== this.options.tenantId)
          throw new Error("event sync: tenant mismatch");
        if (event.sequence <= cursor || event.sequence > frame.through)
          throw new Error("event sync: non-monotonic sequence");
        rows.push({ event, line: JSON.stringify(event) });
        cursor = event.sequence;
      }
      cursor = frame.through;
    }
    return { cursor, rows, broken: false, rest: [] };
  }

  private readCurrentHead(signal: AbortSignal): Promise<number> {
    return readCurrentHead(this.options, signal);
  }

  private readPage(cursor: number, signal: AbortSignal) {
    return readPage(this.options, cursor, signal);
  }
}

export interface ReadCachedEventsOptions {
  tenantId: string;
  directory?: string;
  after?: number;
}

export async function readCachedEvents(
  options: ReadCachedEventsOptions,
): Promise<CatalystEvent[]> {
  const { directory } = eventCachePaths(options.tenantId, options.directory);
  const checkpoint = await readCursor(
    eventCachePaths(options.tenantId, options.directory).cursor,
  );
  if (
    options.after !== undefined &&
    checkpoint &&
    options.after < checkpoint.floor
  )
    throw new EventHistoryGapError(
      "event cache: requested sequence precedes retained history",
      checkpoint.cursor,
      checkpoint.floor,
      "local-retention",
    );
  let names: string[];
  try {
    names = (await readdir(directory))
      .filter(isCacheSegment)
      .sort(compareCacheSegments);
  } catch (error) {
    if ((error as { code?: string }).code === "ENOENT") return [];
    throw error;
  }
  const events: CatalystEvent[] = [];
  for (const name of names) {
    const body = await readFile(join(directory, name), "utf8");
    const complete = body.endsWith("\n")
      ? body.slice(0, -1)
      : body.slice(0, body.lastIndexOf("\n") + 1);
    for (const line of complete.split("\n")) {
      if (!line) continue;
      const event = parseEvent(JSON.parse(line));
      if (event.sequence > (options.after ?? -1)) events.push(event);
    }
  }
  return events;
}

export async function* tailCachedEvents(
  options: ReadCachedEventsOptions & { signal: AbortSignal },
): AsyncGenerator<CatalystEvent> {
  const paths = eventCachePaths(options.tenantId, options.directory);
  const { directory } = paths;
  let cursor = options.after ?? (await readCursor(paths.cursor))?.cursor ?? -1;
  const emit = async () => {
    const rows = await readCachedEvents({ ...options, after: cursor });
    if (rows.length > 0) cursor = rows[rows.length - 1]!.sequence;
    return rows;
  };
  const changes = watch(directory, { signal: options.signal })[
    Symbol.asyncIterator
  ]();
  const nextChange = () =>
    changes.next().then(
      (value) => ({ value }),
      (error: unknown) => ({ error }),
    );
  let pending = nextChange();
  try {
    for (const event of await emit()) yield event;
    for (;;) {
      const outcome = await pending;
      if ("error" in outcome) throw outcome.error;
      const change = outcome.value;
      if (change.done) return;
      pending = nextChange();
      for (const event of await emit()) yield event;
    }
  } finally {
    await changes.return?.();
    await pending;
  }
}

export {
  checkEventFilter,
  matchesEventFilter,
  matchesEventType,
  eventScopeKeys,
  eventFilterSocketParams,
  eventFilterQueryParams,
  EVENT_FILTER_MAX_VALUES,
  type EventScopeFilter,
  type EventScopeFilterInput,
  type EventFilterKey,
  type EventFilterCheck,
  type EventScopeKeys,
} from "./event-filter.js";
export {
  waitForEvents,
  watchEvents,
  DEFAULT_EVENT_WAIT_TIMEOUT_MS,
  EVENT_WAIT_EXIT_CODES,
  type EventWaitOptions,
  type EventWatchOptions,
  type EventWaitResult,
  type WaitedEvent,
} from "./events-wait.js";

export {
  waitForCachedEvents,
  type CachedEventWaitOptions,
} from "./events-cache-wait.js";
