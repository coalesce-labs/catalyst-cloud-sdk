/** Local waits share the replica writer's upstream connection and never modify its cache. */
import { watch, type FSWatcher } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import { CacheSegmentReader } from "./events-cache-reader.js";
import { eventCachePaths } from "./events.js";
import { eventIdentity } from "./events-backbone.js";
import {
  checkEventFilter,
  matchesEventFilter,
  type EventScopeFilterInput,
} from "./event-filter.js";
import {
  DEFAULT_EVENT_WAIT_TIMEOUT_MS,
  type EventWaitResult,
} from "./events-wait.js";
import { readWriterLockHeartbeat } from "./replica/writer-lock.js";

export interface CachedEventWaitOptions {
  tenantId: string;
  directory?: string;
  filter: EventScopeFilterInput;
  after?: number;
  timeoutMs?: number;
  maxEvents?: number;
  signal?: AbortSignal;
  /** A writer heartbeat older than this means no writer is running. Default 15 s. */
  writerStaleMs?: number;
  /** Re-check the writer this often. Default 30 s; stale for 4 × writerStaleMs ends the wait. */
  writerCheckMs?: number;
  /** Backstop scan interval beside the directory watch. Default 2 s. */
  pollMs?: number;
}

async function checkpoint(
  path: string,
): Promise<{ cursor: number; floor: number } | null> {
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT")
      return null;
    throw error;
  }
  const value: unknown = JSON.parse(text);
  if (
    !value ||
    typeof value !== "object" ||
    !("cursor" in value) ||
    typeof value.cursor !== "number" ||
    !Number.isSafeInteger(value.cursor) ||
    value.cursor < 0
  ) {
    throw new Error("event cache: invalid checkpoint cursor");
  }
  const floor =
    "floor" in value &&
    typeof value.floor === "number" &&
    Number.isSafeInteger(value.floor)
      ? value.floor
      : -1;
  return { cursor: value.cursor, floor };
}

export async function waitForCachedEvents(
  options: CachedEventWaitOptions,
): Promise<EventWaitResult> {
  const checked = checkEventFilter(options.filter);
  if (!checked.ok)
    return {
      outcome: "refused",
      source: "filter",
      status: null,
      key: checked.key,
      value: checked.value,
      reason: checked.reason,
      filter: options.filter,
    };
  const filter = checked.filter;
  if (filter.to)
    return {
      outcome: "refused",
      source: "filter",
      status: null,
      key: "to",
      value: "me",
      reason: "to=me is resolved by the cloud; a cache wait cannot evaluate it",
      filter,
    };

  const paths = eventCachePaths(options.tenantId, options.directory);
  const started = Date.now();
  const timeoutMs = options.timeoutMs ?? DEFAULT_EVENT_WAIT_TIMEOUT_MS;
  const maxEvents = options.maxEvents ?? 100;
  const staleMs = options.writerStaleMs ?? 15_000;
  const checkMs = options.writerCheckMs ?? 30_000;
  const pollMs = options.pollMs ?? 2_000;
  let cursor: number | null = options.after ?? null;
  let watcher: FSWatcher | undefined;
  let wake: (() => void) | undefined;
  let changed = false;
  let watchError: Error | undefined;
  const notify = () => {
    changed = true;
    wake?.();
  };
  const stopped = (): EventWaitResult => ({
    outcome: "failed",
    code: "cache-writer-stopped",
    cursor,
    reason: `no event cache writer has run for ${paths.directory} in the last ${staleMs / 1000}s`,
  });
  const gap = (floor: number): EventWaitResult => ({
    outcome: "failed",
    code: "history-gap",
    cursor,
    resumeFrom: floor,
    reason: "event cache: requested sequence precedes retained history",
  });
  try {
    if (
      !Number.isFinite(timeoutMs) ||
      timeoutMs < 0 ||
      !Number.isSafeInteger(maxEvents) ||
      maxEvents < 1 ||
      [staleMs, checkMs, pollMs].some(
        (value) => !Number.isFinite(value) || value <= 0,
      ) ||
      (options.after !== undefined &&
        (!Number.isSafeInteger(options.after) || options.after < 0))
    ) {
      throw new Error("event cache: invalid wait options");
    }
    if (options.signal?.aborted) return { outcome: "aborted", cursor };
    let exists: boolean;
    try {
      exists = (await stat(paths.directory)).isDirectory();
    } catch (error) {
      if (!(
        error instanceof Error &&
        "code" in error &&
        error.code === "ENOENT"
      ))
        throw error;
      exists = false;
    }
    if (!exists)
      return {
        outcome: "failed",
        code: "cache-missing",
        reason: `no event cache at ${paths.directory}`,
        cursor,
      };
    const initial = await checkpoint(paths.cursor);
    cursor = options.after ?? initial?.cursor ?? null;
    let heartbeat = readWriterLockHeartbeat(`${paths.lock}.writer.lock`);
    if (heartbeat === null || Date.now() - heartbeat >= staleMs)
      return stopped();
    if (initial && cursor !== null && cursor < initial.floor)
      return gap(initial.floor);

    // Install the watch before positioning so appends during the first scan are not missed.
    watcher = watch(paths.directory, notify);
    watcher.on("error", (error) => {
      watchError = error;
      notify();
    });
    options.signal?.addEventListener("abort", notify, { once: true });
    const reader = await CacheSegmentReader.positionAt(
      paths.directory,
      cursor ?? -1,
    );
    let nextWriterCheck = Date.now() + checkMs;
    for (;;) {
      if (options.signal?.aborted) return { outcome: "aborted", cursor };
      if (watchError) throw watchError;
      const position = cursor ?? -1;
      // The checkpoint is read BEFORE the bytes: the writer flushes the rows before moving it.
      const current = await checkpoint(paths.cursor);
      if (current && position < current.floor) return gap(current.floor);
      const rows = await reader.read();
      // Retention can remove a segment between the checkpoint read and opening that segment.
      // Recheck only its floor; the newer cursor may cover bytes this scan has not read yet.
      const retained = await checkpoint(paths.cursor);
      if (retained && position < retained.floor) return gap(retained.floor);
      if (options.signal?.aborted) return { outcome: "aborted", cursor };
      const matches = [];
      const seen = new Set<string>();
      let proven = Math.max(position, current?.cursor ?? -1);
      for (const event of rows) {
        if (event.tenantId !== options.tenantId)
          throw new Error("event cache: event belongs to another workspace");
        proven = Math.max(proven, event.sequence);
        const identity = eventIdentity(event);
        if (
          event.sequence > position &&
          matchesEventFilter(event, filter) &&
          !seen.has(identity)
        ) {
          seen.add(identity);
          matches.push(event);
        }
      }
      matches.sort((a, b) => a.sequence - b.sequence);
      if (matches.length) {
        const events = matches.slice(0, maxEvents);
        const last = events.at(-1);
        if (!last) throw new Error("event cache: empty match batch");
        return {
          outcome: "matched",
          events,
          cursor: matches.length > maxEvents ? last.sequence : proven,
        };
      }
      cursor = proven < 0 ? null : proven;
      const now = Date.now();
      if (now >= nextWriterCheck) {
        heartbeat =
          readWriterLockHeartbeat(`${paths.lock}.writer.lock`) ?? heartbeat;
        if (now - heartbeat >= 4 * staleMs) return stopped();
        nextWriterCheck = now + checkMs;
      }
      if (now - started >= timeoutMs)
        return { outcome: "timeout", cursor, waitedMs: now - started };
      if (changed) {
        changed = false;
        continue;
      }
      await new Promise<void>((resolve) => {
        const timer = setTimeout(
          done,
          Math.min(pollMs, nextWriterCheck - now, timeoutMs - (now - started)),
        );
        function done() {
          clearTimeout(timer);
          wake = undefined;
          resolve();
        }
        wake = done;
        if (options.signal?.aborted) done();
      });
      changed = false;
    }
  } catch (error) {
    return {
      outcome: "failed",
      code: "shape",
      cursor,
      reason: error instanceof Error ? error.message : String(error),
    };
  } finally {
    watcher?.close();
    options.signal?.removeEventListener("abort", notify);
  }
}
