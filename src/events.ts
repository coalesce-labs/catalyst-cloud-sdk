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
import type { AuthStrategy } from "./live-sync-client.js";
import {
  claimWriterLock,
  type WriterGuardOptions,
  type WriterLockHandle,
} from "./replica/writer-lock.js";

export type { CatalystEvent } from "@catalyst-cloud/schema";

const HEAD_HEADER = "x-catalyst-event-backbone-head-seq";
const MAX_EVENT_LINE_BYTES = 1024 * 1024;
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;
const DEFAULT_IDLE_MIN_MS = 1_000;
const DEFAULT_IDLE_MAX_MS = 30_000;
const DEFAULT_RETAIN_DAYS = 7;
const DEFAULT_MAX_CACHE_BYTES = 256 * 1024 * 1024;
const SEGMENT_PATTERN = /^\d{4}-\d{2}-\d{2}(?:-\d{3})?\.jsonl$/;

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
}

export interface EventSyncStatus {
  state: "idle" | "syncing" | "stopped" | "failed";
  cursor: number | null;
  head: number | null;
  lastSyncAt: string | null;
  error: string | null;
}

export interface EventSyncResult {
  appended: number;
  cursor: number;
  head: number;
}

export class EventHistoryGapError extends Error {
  constructor(
    message: string,
    readonly head: number,
    readonly resumeFrom: number,
    readonly reason: string,
  ) {
    super(message);
    this.name = "EventHistoryGapError";
  }
}

class EventSyncHttpError extends Error {
  constructor(readonly status: number) {
    super(`event sync: backbone returned ${status}`);
    this.name = "EventSyncHttpError";
  }
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

function positiveInteger(value: unknown, name: string): number {
  if (
    (typeof value !== "string" && typeof value !== "number") ||
    String(value).trim() === ""
  )
    throw new Error(`event sync: invalid ${name}`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0)
    throw new Error(`event sync: invalid ${name}`);
  return parsed;
}

function parseEvent(value: unknown): CatalystEvent {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("event sync: invalid event");
  const event = value as Partial<CatalystEvent>;
  if (
    typeof event.tenantId !== "string" ||
    !Number.isSafeInteger(event.sequence) ||
    (event.sequence ?? -1) < 0 ||
    typeof event.eventId !== "string" ||
    typeof event.type !== "string" ||
    !Number.isSafeInteger(event.schemaVersion) ||
    typeof event.recordedAt !== "string" ||
    !("payload" in event)
  )
    throw new Error("event sync: invalid event");
  return event as CatalystEvent;
}

async function readNdjson(
  response: Response,
): Promise<Array<{ event: CatalystEvent; line: string }>> {
  if (!response.body) return [];
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = "";
  const rows: Array<{ event: CatalystEvent; line: string }> = [];
  const consume = (line: string) => {
    if (!line) return;
    if (new TextEncoder().encode(line).byteLength > MAX_EVENT_LINE_BYTES)
      throw new Error("event sync: event line exceeds 1 MiB");
    rows.push({ event: parseEvent(JSON.parse(line)), line });
  };
  for (;;) {
    const { done, value } = await reader.read();
    pending += decoder.decode(value, { stream: !done });
    let newline = pending.indexOf("\n");
    while (newline >= 0) {
      consume(pending.slice(0, newline));
      pending = pending.slice(newline + 1);
      newline = pending.indexOf("\n");
    }
    if (pending.length > MAX_EVENT_LINE_BYTES)
      throw new Error("event sync: event line exceeds 1 MiB");
    if (done) break;
  }
  if (pending) consume(pending);
  return rows;
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
    .filter((name) => SEGMENT_PATTERN.test(name))
    .sort();
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
): Promise<number> {
  const segments = (await readdir(directory))
    .filter((name) => SEGMENT_PATTERN.test(name))
    .sort();
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
    .filter((name) => name.startsWith(day) && SEGMENT_PATTERN.test(name))
    .sort();
  let index = existing.length === 0 ? 0 : existing.length - 1;
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
  private readonly fetchImpl: typeof fetch;
  private readonly now: () => Date;
  private readonly sleeper: (ms: number, signal: AbortSignal) => Promise<void>;
  private lock: WriterLockHandle | null = null;
  private controller: AbortController | null = null;
  private running: Promise<void> | null = null;
  private recoveredCursor: number | null = null;
  private floor = -1;
  private current: EventSyncStatus = {
    state: "stopped",
    cursor: null,
    head: null,
    lastSyncAt: null,
    error: null,
  };

  constructor(private readonly options: EventSyncOptions) {
    this.paths = eventCachePaths(options.tenantId, options.directory);
    this.fetchImpl = options.fetch ?? fetch;
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
    const result = await this.readPage(cursor, signal);
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
    this.floor = await retainBoundedSegments(
      this.paths.directory,
      now,
      this.options.retainDays ?? DEFAULT_RETAIN_DAYS,
      maxCacheBytes,
      segment,
      this.floor,
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
    };
    return { appended: result.rows.length, cursor, head: result.head };
  }

  start(): Promise<void> {
    if (this.running) return this.running;
    this.controller = new AbortController();
    this.running = this.loop(this.controller.signal).finally(() => {
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
        try {
          const result = await this.syncOnce(signal);
          idle =
            result.appended > 0
              ? (this.options.idleMinMs ?? DEFAULT_IDLE_MIN_MS)
              : Math.min(maxIdle, idle * 2);
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
        }
        await this.sleeper(idle, signal);
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

  private async request(since: number, signal: AbortSignal): Promise<Response> {
    const timeout = AbortSignal.timeout(
      this.options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
    );
    const token =
      this.options.auth.kind === "token"
        ? this.options.auth.token
        : await this.options.auth.getToken();
    return this.fetchImpl(
      `${this.options.baseUrl.replace(/\/$/, "")}/api/v1/events/backbone?since=${since}`,
      {
        headers: { authorization: `Bearer ${token}` },
        signal: AbortSignal.any([signal, timeout]),
      },
    );
  }

  private async readCurrentHead(signal: AbortSignal): Promise<number> {
    const response = await this.request(Number.MAX_SAFE_INTEGER, signal);
    if (response.status !== 409) throw new EventSyncHttpError(response.status);
    const body = (await response.json()) as Record<string, unknown>;
    if (body.error !== "cursor_ahead_of_head")
      throw new Error("event sync: head bootstrap contract mismatch");
    return positiveInteger(body.resumeFrom, "bootstrap cursor");
  }

  private async readPage(
    cursor: number,
    signal: AbortSignal,
  ): Promise<{
    head: number;
    rows: Array<{ event: CatalystEvent; line: string }>;
  }> {
    const response = await this.request(cursor, signal);
    if (!response.ok && response.status !== 409)
      throw new EventSyncHttpError(response.status);
    const head = positiveInteger(
      response.headers.get(HEAD_HEADER),
      "head header",
    );
    if (response.status === 409) {
      const body = (await response.json()) as Record<string, unknown>;
      throw new EventHistoryGapError(
        "event sync: durable history has a gap",
        head,
        positiveInteger(body.resumeFrom, "resume cursor"),
        typeof body.reason === "string"
          ? body.reason
          : String(body.error ?? "cursor_refused"),
      );
    }
    const rows = await readNdjson(response);
    let previous = cursor;
    for (const row of rows) {
      if (row.event.tenantId !== this.options.tenantId)
        throw new Error("event sync: tenant mismatch");
      if (row.event.sequence <= previous)
        throw new Error("event sync: non-monotonic sequence");
      previous = row.event.sequence;
    }
    return { head, rows };
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
      .filter((name) => SEGMENT_PATTERN.test(name))
      .sort();
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

export function eventIdentity(event: CatalystEvent): string {
  return JSON.stringify([event.tenantId, event.sequence, event.eventId]);
}
