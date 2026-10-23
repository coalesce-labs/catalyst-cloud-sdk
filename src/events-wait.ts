/**
 * Direct event waits and the cursor contract (CTC-5520).
 * 1. Validate the filter before any request (S5b).
 * 2. Check reach over HTTP before subscribing; retry transient failures (S5/S5c).
 * 3. Start at `after`, otherwise the backbone head; a refused probe uses the first
 *    frame's `after`, with preflight coverage as a conservative timeout cursor (S9b).
 * 4. Hold one filtered socket, using its keepalive (S1/S2/S6).
 * 5. Re-probe and replay on every live socket generation (S3).
 * 6. Chained frames prove their range. Gaps, omitted bodies and overflow require replay.
 *    Invalid events and workspace mismatches fail explicitly (hole controls).
 * 7. Replay until index coverage reaches the target, polling 2 s up to 30 s. After
 *    30 s use the backbone where permitted; never skip an unindexed range (S3b-d).
 * 8. A safety query runs every 10 min live / 30 s down, never beside a replay (S2).
 * 9. Deliver sorted unique matches. A capped batch ends its cursor at the last
 *    delivery; a watch retains its remaining batch on the same socket (S8/S8b/S8c).
 * 10. Timeout and abort return the proven cursor and stop all owned work (S9).
 */
import type { CatalystEvent } from "@catalyst-cloud/schema";
import {
  EventsChannelClient,
  type EventsSocketOptions,
} from "./events-socket.js";
import {
  EventHistoryGapError,
  EventSyncHttpError,
  eventIdentity,
  parseEvent,
  readCurrentHead,
  readPage,
} from "./events-backbone.js";
import {
  checkEventFilter,
  matchesEventFilter,
  matchesEventType,
  type EventFilterKey,
  type EventScopeFilter,
  type EventScopeFilterInput,
} from "./event-filter.js";
import {
  createTenantClient,
  type EventQueryParams,
  type EventQueryResult,
  type QueriedEvent,
  type QueriedEventStub,
  type TenantClient,
} from "./tenant-client.js";
import type { AuthStrategy, WebSocketFactory } from "./live-sync-client.js";

export const DEFAULT_EVENT_WAIT_TIMEOUT_MS = 4 * 60 * 60 * 1000;
export const EVENT_WAIT_EXIT_CODES = {
  matched: 0,
  failed: 1,
  refused: 3,
  timeout: 124,
  aborted: 130,
} as const;
export type WaitedEvent = CatalystEvent | QueriedEventStub;
export type EventWaitResult =
  | { outcome: "matched"; events: WaitedEvent[]; cursor: number }
  | { outcome: "timeout"; cursor: number | null; waitedMs: number }
  | {
      outcome: "refused";
      source: "filter" | "cloud";
      status: number | null;
      key: EventFilterKey | null;
      value: string | null;
      reason: string;
      filter: EventScopeFilterInput;
      types?: string[];
    }
  | {
      outcome: "failed";
      code:
        | "history-gap"
        | "unsupported"
        | "shape"
        | "cache-missing"
        | "cache-writer-stopped";
      reason: string;
      cursor: number | null;
      resumeFrom?: number;
    }
  | { outcome: "aborted"; cursor: number | null };
export interface EventWaitOptions {
  baseUrl: string;
  auth: Exclude<AuthStrategy, { kind: "cookie" }>;
  tenantId: string;
  filter: EventScopeFilterInput;
  after?: number;
  timeoutMs?: number;
  maxEvents?: number;
  signal?: AbortSignal;
  fetch?: typeof fetch;
  wsFactory?: WebSocketFactory;
  requestTimeoutMs?: number;
  /** Replay from the cursor this often while live. Default 10 min. */
  safetyNetMs?: number;
  /** Replay from the cursor this often while down. Default 30 s. */
  safetyNetDownMs?: number;
  /** Wait this long for index coverage before trying the backbone. Default 30 s. */
  indexWaitMs?: number;
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
export type EventWatchOptions = Omit<EventWaitOptions, "timeoutMs"> & {
  signal: AbortSignal;
};
type Matched = Extract<EventWaitResult, { outcome: "matched" }>;
type Terminal = Exclude<EventWaitResult, Matched>;
type QueryPage = Extract<EventQueryResult, { outcome: "ok" | "too-large" }>;

class EndWait extends Error {
  constructor(readonly result: Terminal) {
    super(result.outcome);
  }
}

/** One follower owns its socket, deadline, request cancellation and proven cursor. */
class Follower {
  cursor: number | null;
  private fallback: number | null = null;
  private socket: EventsChannelClient | null = null;
  private readonly controller = new AbortController();
  private readonly client: TenantClient;
  private readonly started = Date.now();
  private readonly timer: ReturnType<typeof setTimeout> | undefined;
  private timedOut = false;
  private backboneAvailable = true;
  private generation = -1;
  private lastReplay = this.started;
  private readonly maxEvents: number;
  private readonly fetch: typeof globalThis.fetch;
  private readonly externalAbort = () => {
    this.controller.abort();
    this.socket?.stop();
    clearTimeout(this.timer);
  };

  constructor(
    private readonly options: EventWaitOptions,
    private readonly filter: EventScopeFilter,
    timeoutMs: number | undefined,
  ) {
    this.cursor = options.after ?? null;
    this.maxEvents = options.maxEvents ?? 100;
    const fetchImpl = options.fetch ?? globalThis.fetch;
    this.fetch = (input, init) => {
      this.controller.signal.throwIfAborted();
      return fetchImpl(input, {
        ...init,
        signal: init?.signal
          ? AbortSignal.any([init.signal, this.controller.signal])
          : this.controller.signal,
      });
    };
    this.client = createTenantClient({
      baseUrl: options.baseUrl,
      auth: options.auth,
      fetch: this.fetch,
      timeoutMs: options.requestTimeoutMs,
    });
    options.signal?.addEventListener("abort", this.externalAbort, {
      once: true,
    });
    if (options.signal?.aborted) this.externalAbort();
    if (timeoutMs !== undefined)
      this.timer = setTimeout(() => {
        this.timedOut = true;
        this.externalAbort();
      }, timeoutMs);
  }

  private end(): Terminal {
    return this.timedOut
      ? {
          outcome: "timeout",
          cursor: this.cursor ?? this.fallback,
          waitedMs: Date.now() - this.started,
        }
      : { outcome: "aborted", cursor: this.cursor ?? this.fallback };
  }
  private check(): void {
    if (this.controller.signal.aborted) throw new EndWait(this.end());
  }
  private failed(
    code: Extract<Terminal, { outcome: "failed" }>["code"],
    reason: string,
    resumeFrom?: number,
  ): EndWait {
    return new EndWait({
      outcome: "failed",
      code,
      reason,
      cursor: this.cursor,
      ...(resumeFrom === undefined ? {} : { resumeFrom }),
    });
  }
  private async untilStopped<T>(promise: Promise<T>): Promise<T> {
    this.check();
    const signal = this.controller.signal;
    let onAbort: () => void = () => {};
    const stopped = new Promise<never>((_, reject) => {
      onAbort = () => reject(new EndWait(this.end()));
      signal.addEventListener("abort", onAbort, { once: true });
    });
    try {
      return await Promise.race([promise, stopped]);
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }
  private async sleep(ms: number, listen = false): Promise<void> {
    this.check();
    const waker = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const slept = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, Math.max(0, ms));
    });
    try {
      await this.untilStopped(
        listen && this.socket
          ? Promise.race([slept, this.socket.changed(waker.signal)])
          : slept,
      );
    } finally {
      clearTimeout(timer);
      waker.abort();
    }
  }
  private async query(params: EventQueryParams): Promise<QueryPage> {
    let backoff = 1000;
    for (;;) {
      this.check();
      const result = await this.untilStopped(this.client.events.query(params));
      this.check();
      if (result.outcome === "ok" || result.outcome === "too-large")
        return result;
      if (
        result.outcome === "forbidden" ||
        result.outcome === "unauthorized" ||
        result.outcome === "rejected" ||
        result.outcome === "event-type-refused"
      ) {
        throw new EndWait({
          outcome: "refused",
          source: "cloud",
          status: result.status,
          key: null,
          value: null,
          reason: result.reason,
          filter: this.filter,
          ...(result.outcome === "event-type-refused"
            ? { types: result.types }
            : {}),
        });
      }
      if (result.outcome === "unsupported" || result.outcome === "shape")
        throw this.failed(result.outcome, result.reason);
      if (result.outcome === "http" && result.status < 500)
        throw this.failed(
          "shape",
          `event query returned ${result.status}: ${result.reason}`,
        );
      await this.sleep(backoff);
      backoff = Math.min(backoff * 2, 30000);
    }
  }
  private backboneOptions() {
    return { ...this.options, fetch: this.fetch };
  }
  private forbiddenBackbone(error: unknown): boolean {
    if (
      error instanceof EventSyncHttpError &&
      [401, 403, 404].includes(error.status)
    ) {
      this.backboneAvailable = false;
      return true;
    }
    return false;
  }
  private async head(): Promise<number | null> {
    if (!this.backboneAvailable) return null;
    let delay = 1000;
    for (;;) {
      try {
        return await this.untilStopped(
          readCurrentHead(this.backboneOptions(), this.controller.signal),
        );
      } catch (error) {
        this.check();
        if (this.forbiddenBackbone(error)) return null;
        if (error instanceof EventSyncHttpError && error.status < 500)
          throw error;
        if (
          error instanceof Error &&
          error.message.includes("contract mismatch")
        )
          throw error;
        await this.sleep(delay);
        delay = Math.min(delay * 2, 30000);
      }
    }
  }
  private parsed(value: QueriedEvent): WaitedEvent {
    if (value.bodyUnavailable === true) {
      if (
        value.tenantId !== this.options.tenantId ||
        !Number.isSafeInteger(value.sequence) ||
        value.sequence < 0
      )
        throw this.failed(
          "shape",
          "event query: invalid unavailable event or workspace mismatch",
        );
      return value;
    }
    const event = parseEvent(value);
    if (event.tenantId !== this.options.tenantId)
      throw this.failed("shape", "event query: workspace mismatch");
    return event;
  }
  private async *deliver(
    events: WaitedEvent[],
    through: number,
  ): AsyncGenerator<Matched> {
    const seen = new Set<string>();
    const rows = events
      .filter((event) => {
        // Both full events and unavailable-body stubs have the same identity fields.
        const id = eventIdentity(event);
        if (
          event.sequence <= (this.cursor ?? -1) ||
          event.sequence > through ||
          seen.has(id)
        )
          return false;
        seen.add(id);
        return true;
      })
      .sort((a, b) => a.sequence - b.sequence);
    for (let index = 0; index < rows.length; index += this.maxEvents) {
      this.check();
      const batch = rows.slice(index, index + this.maxEvents);
      const last = batch.at(-1);
      if (!last) break;
      this.cursor =
        index + this.maxEvents < rows.length
          ? last.sequence
          : Math.max(this.cursor ?? -1, through);
      yield { outcome: "matched", events: batch, cursor: this.cursor };
    }
    this.cursor = Math.max(this.cursor ?? -1, through);
  }
  /** Returns null only when this credential cannot read the backbone. */
  private async backbonePage(cursor: number) {
    if (!this.backboneAvailable || this.filter.to) return null;
    let backoff = 1000;
    for (;;) {
      try {
        return await this.untilStopped(
          readPage(this.backboneOptions(), cursor, this.controller.signal),
        );
      } catch (error) {
        this.check();
        if (this.forbiddenBackbone(error)) return null;
        if (error instanceof EventHistoryGapError)
          throw this.failed("history-gap", error.message, error.resumeFrom);
        if (
          (error instanceof EventSyncHttpError && error.status >= 500) ||
          error instanceof TypeError ||
          (error instanceof Error &&
            (error.name === "TimeoutError" || error.name === "AbortError"))
        ) {
          await this.sleep(backoff);
          backoff = Math.min(backoff * 2, 30000);
          continue;
        }
        throw error;
      }
    }
  }
  private async *backboneReplay(
    target: number,
  ): AsyncGenerator<Matched, boolean> {
    for (;;) {
      const start = this.cursor ?? this.fallback ?? 0;
      const page = await this.backbonePage(start);
      if (!page) return false;
      const last = page.rows.at(-1);
      const through = Math.min(target, last?.event.sequence ?? page.head);
      if (through <= start) return true;
      const matches = page.rows
        .map((row) => row.event)
        .filter(
          (event) =>
            event.sequence <= through && matchesEventFilter(event, this.filter),
        );
      yield* this.deliver(matches, through);
      if (through >= target || through >= page.head) return true;
    }
  }
  private async *replay(target: number | null): AsyncGenerator<Matched> {
    const began = Date.now();
    let backoff = 2000;
    let triedBackbone = false;
    for (;;) {
      this.check();
      const start = this.cursor ?? this.fallback ?? 0;
      let pageCursor = start;
      let covered = start;
      let floor: number | null = null;
      const matches: WaitedEvent[] = [];
      for (;;) {
        const page = await this.query({
          ...this.filter,
          order: "asc",
          afterSeq: pageCursor,
          ...(target !== null && target < Number.MAX_SAFE_INTEGER
            ? { beforeSeq: target + 1 }
            : {}),
        });
        if (page.outcome === "too-large") {
          if (
            page.sequence <= pageCursor ||
            page.next.param !== "afterSeq" ||
            page.next.value !== page.sequence
          )
            throw this.failed(
              "shape",
              "event query: oversized event cursor did not advance",
            );
          // Re-read from the preceding proven page position, so this cannot skip earlier matches.
          let readThrough = pageCursor;
          let found = false;
          while (!found) {
            const raw = await this.backbonePage(readThrough);
            const last = raw?.rows.at(-1)?.event.sequence;
            if (!raw || last === undefined)
              throw this.failed(
                "shape",
                `cannot read oversized event at sequence ${page.sequence}`,
              );
            for (const row of raw.rows) {
              if (row.event.sequence === page.sequence) found = true;
              if (
                row.event.sequence <= page.sequence &&
                matchesEventFilter(row.event, this.filter)
              )
                matches.push(row.event);
            }
            if (!found && last >= page.sequence)
              throw this.failed(
                "shape",
                `backbone omitted oversized event at sequence ${page.sequence}`,
              );
            readThrough = last;
          }
          pageCursor = page.sequence;
          covered = Math.max(covered, page.sequence);
          continue;
        }
        floor = page.coverage.indexedFromSeq;
        covered = Math.max(covered, page.coverage.indexedToSeq ?? start);
        for (const value of page.events) {
          const event = this.parsed(value);
          if (event.sequence <= pageCursor)
            throw this.failed(
              "shape",
              "event query: non-advancing event sequence",
            );
          if (
            !this.filter.type ||
            matchesEventType(event.type, this.filter.type)
          )
            matches.push(event);
        }
        if (page.next === null) break;
        if (page.next.param !== "afterSeq" || page.next.value <= pageCursor)
          throw this.failed("shape", "event query: non-advancing page cursor");
        pageCursor = page.next.value;
      }
      if (floor !== null && floor > start + 1) {
        if (triedBackbone || !(yield* this.backboneReplay(target ?? covered)))
          throw this.failed(
            "history-gap",
            `the workspace's event history no longer holds sequence ${start + 1}`,
            floor - 1,
          );
        triedBackbone = true;
        if ((this.cursor ?? start) < floor - 1)
          throw this.failed(
            "history-gap",
            `the workspace's event history no longer holds sequence ${start + 1}`,
            floor - 1,
          );
      } else {
        yield* this.deliver(matches, Math.min(covered, target ?? Infinity));
      }
      if (target === null || (this.cursor ?? start) >= target) {
        this.lastReplay = Date.now();
        return;
      }
      const indexWait = this.options.indexWaitMs ?? 30000;
      if (!triedBackbone && Date.now() - began >= indexWait) {
        triedBackbone = true;
        yield* this.backboneReplay(target);
        if ((this.cursor ?? start) >= target) {
          this.lastReplay = Date.now();
          return;
        }
      }
      const beforeFallback = triedBackbone
        ? backoff
        : Math.max(0, indexWait - (Date.now() - began));
      await this.sleep(Math.min(backoff, beforeFallback));
      backoff = Math.min(backoff * 2, 30000);
    }
  }
  private async *follow(): AsyncGenerator<Matched> {
    const preflight = await this.query({
      ...this.filter,
      order: "desc",
      limit: 1,
    });
    if (preflight.outcome === "ok") {
      const newest = preflight.events.reduce(
        (max, event) => Math.max(max, event.sequence),
        -1,
      );
      const fallback = Math.max(preflight.coverage.indexedToSeq ?? -1, newest);
      this.fallback = fallback >= 0 ? fallback : null;
    }
    if (this.cursor === null) this.cursor = await this.head();
    this.check();
    this.socket = new EventsChannelClient({
      ...this.options,
      filter: this.filter,
      ...this.options.socket,
    });
    this.socket.start();
    for (;;) {
      this.check();
      if (
        this.socket.state === "live" &&
        this.socket.generation !== this.generation
      ) {
        this.generation = this.socket.generation;
        const target = await this.head();
        // A successful later probe must retain the connect window from the preflight.
        if (this.cursor === null && target !== null)
          this.cursor = this.fallback ?? target;
        if (this.cursor !== null) yield* this.replay(target);
      }
      const batch = this.socket.take();
      if (batch.overflowed) yield* this.replay(await this.head());
      for (const frame of batch.frames) {
        // Without a head probe, start from the preflight's proven position so the connect window
        // (fallback, frame.after] is replayed through the index instead of skipped.
        this.cursor ??= this.fallback ?? frame.after;
        if (frame.through <= this.cursor) continue;
        if (
          frame.gap ||
          frame.after > this.cursor ||
          frame.events.some(
            (value) =>
              typeof value === "object" &&
              value !== null &&
              "payloadOmitted" in value &&
              value.payloadOmitted === true,
          )
        ) {
          yield* this.replay(frame.through);
          continue;
        }
        const events = frame.events.map((value) => parseEvent(value));
        for (const event of events) {
          if (
            event.tenantId !== this.options.tenantId ||
            event.sequence > frame.through ||
            event.sequence <= frame.after
          )
            throw this.failed(
              "shape",
              "events frame: invalid sequence or workspace mismatch",
            );
        }
        yield* this.deliver(
          events.filter(
            (event) =>
              !this.filter.type ||
              matchesEventType(event.type, this.filter.type),
          ),
          frame.through,
        );
      }
      // Frames can arrive while replay is awaiting HTTP. Never sleep on buffered work.
      if (
        this.socket.hasPending() ||
        (this.socket.generation !== this.generation &&
          this.socket.state === "live")
      )
        continue;
      const interval =
        this.socket.state === "live"
          ? (this.options.safetyNetMs ?? 600000)
          : (this.options.safetyNetDownMs ?? 30000);
      const due = this.lastReplay + interval - Date.now();
      if (due <= 0) {
        yield* this.replay(null);
        continue;
      }
      // Transport close does not emit changed(); checking state locally costs no request.
      await this.sleep(
        Math.min(due, this.options.safetyNetDownMs ?? 30000),
        true,
      );
    }
  }
  async *run(): AsyncGenerator<EventWaitResult> {
    let terminal: Terminal;
    try {
      yield* this.follow();
      terminal = this.end();
    } catch (error) {
      terminal = this.controller.signal.aborted
        ? this.end()
        : error instanceof EndWait
          ? error.result
          : {
              outcome: "failed",
              code: "shape",
              reason: error instanceof Error ? error.message : String(error),
              cursor: this.cursor,
            };
    } finally {
      clearTimeout(this.timer);
      this.options.signal?.removeEventListener("abort", this.externalAbort);
      this.controller.abort();
      this.socket?.stop();
    }
    yield terminal;
  }
}

async function* follow(
  options: EventWaitOptions,
  timeoutMs: number | undefined,
): AsyncGenerator<EventWaitResult> {
  const checked = checkEventFilter(options.filter);
  if (!checked.ok) {
    yield {
      outcome: "refused",
      source: "filter",
      status: null,
      key: checked.key,
      value: checked.value,
      reason: checked.reason,
      filter: options.filter,
    };
    return;
  }
  if (
    (options.after !== undefined &&
      (!Number.isSafeInteger(options.after) || options.after < 0)) ||
    (options.maxEvents !== undefined &&
      (!Number.isSafeInteger(options.maxEvents) || options.maxEvents < 1)) ||
    (timeoutMs !== undefined && (!Number.isFinite(timeoutMs) || timeoutMs < 0))
  ) {
    yield {
      outcome: "failed",
      code: "shape",
      reason:
        "after must be a non-negative integer, maxEvents a positive integer, and timeoutMs non-negative",
      cursor: null,
    };
    return;
  }
  yield* new Follower(options, checked.filter, timeoutMs).run();
}

/** Wait silently on one cloud subscription; no local event cache is opened. */
export async function waitForEvents(
  options: EventWaitOptions,
): Promise<EventWaitResult> {
  const follower = follow(
    options,
    options.timeoutMs ?? DEFAULT_EVENT_WAIT_TIMEOUT_MS,
  );
  try {
    const next = await follower.next();
    return (
      next.value ?? {
        outcome: "failed",
        code: "shape",
        reason: "event follower ended without an outcome",
        cursor: null,
      }
    );
  } finally {
    await follower.return(undefined);
  }
}

/** Keep yielding batches on the same subscription until aborted or refused. */
export async function* watchEvents(
  options: EventWatchOptions,
): AsyncGenerator<EventWaitResult, void, undefined> {
  yield* follow(options, undefined);
}
