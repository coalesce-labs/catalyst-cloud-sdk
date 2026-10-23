/** Incremental, read-only access to the writer's ordered JSONL segments. */
import { open, readdir } from "node:fs/promises";
import { join } from "node:path";
import type { CatalystEvent } from "@catalyst-cloud/schema";
import { parseEvent } from "./events-backbone.js";

const SEGMENT_PATTERN = /^\d{4}-\d{2}-\d{2}(?:-\d{3,})?\.jsonl$/;
const CHUNK_BYTES = 64 * 1024;
const decoder = new TextDecoder();

function concatenate(left: Uint8Array, right: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(left.length + right.length);
  bytes.set(left);
  bytes.set(right, left.length);
  return bytes;
}

function isMissing(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
}

export function isCacheSegment(name: string): boolean {
  return SEGMENT_PATTERN.test(name);
}

export function cacheSegmentIndex(name: string): number {
  return name.length === 16 ? 0 : Number(name.slice(11, -6));
}

/** The unnumbered segment is first; numeric suffixes keep sorting after 999. */
export function compareCacheSegments(left: string, right: string): number {
  return (
    left.slice(0, 10).localeCompare(right.slice(0, 10)) ||
    cacheSegmentIndex(left) - cacheSegmentIndex(right)
  );
}

async function segments(directory: string): Promise<string[]> {
  return (await readdir(directory))
    .filter(isCacheSegment)
    .sort(compareCacheSegments);
}

async function firstSequence(path: string): Promise<number> {
  const handle = await open(path, "r");
  try {
    let pending: Uint8Array = new Uint8Array(0);
    for (;;) {
      const chunk = new Uint8Array(CHUNK_BYTES);
      const { bytesRead } = await handle.read(chunk, 0, chunk.length, null);
      if (!bytesRead) return Infinity; // a newly opened, empty segment
      pending = concatenate(pending, chunk.subarray(0, bytesRead));
      const newline = pending.indexOf(10);
      if (newline >= 0)
        return parseEvent(
          JSON.parse(decoder.decode(pending.subarray(0, newline))),
        ).sequence;
    }
  } finally {
    await handle.close();
  }
}

export class CacheSegmentReader {
  segment: string | null = null;
  offset = 0;
  private pending: Uint8Array = new Uint8Array(0);
  private queued: CatalystEvent[] = [];

  private constructor(private readonly directory: string) {}

  /** Locate the containing segment from its first line, then scan that segment once. */
  static async positionAt(
    directory: string,
    after: number,
  ): Promise<CacheSegmentReader> {
    const reader = new CacheSegmentReader(directory);
    const names = await segments(directory);
    let low = 0;
    let high = names.length;
    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      const name = names[mid];
      if (name === undefined) break;
      try {
        if ((await firstSequence(join(directory, name))) <= after)
          low = mid + 1;
        else high = mid;
      } catch (error) {
        if (!isMissing(error)) throw error;
        // Retention raced the search. A fresh listing avoids trusting the vanished boundary.
        return CacheSegmentReader.positionAt(directory, after);
      }
    }
    reader.segment = names[Math.max(0, low - 1)] ?? null;
    reader.queued = (await reader.read()).filter(
      (event) => event.sequence > after,
    );
    return reader;
  }

  async read(): Promise<CatalystEvent[]> {
    const events = this.queued.splice(0);
    const names = await segments(this.directory);
    for (const name of names) {
      if (
        this.segment !== null &&
        compareCacheSegments(name, this.segment) < 0
      )
        continue;
      if (name !== this.segment) {
        this.segment = name;
        this.offset = 0;
        this.pending = new Uint8Array(0);
      }
      let handle;
      try {
        handle = await open(join(this.directory, name), "r");
      } catch (error) {
        if (isMissing(error)) continue;
        throw error;
      }
      try {
        // Capture the size: concurrent appends belong to the next read, keeping each read bounded.
        const size = (await handle.stat()).size;
        if (size < this.offset)
          throw new Error(`event cache: segment shrank: ${name}`);
        while (this.offset < size) {
          const chunk = new Uint8Array(
            Math.min(CHUNK_BYTES, size - this.offset),
          );
          const { bytesRead } = await handle.read(
            chunk,
            0,
            chunk.length,
            this.offset,
          );
          if (!bytesRead) break;
          this.offset += bytesRead;
          this.pending = concatenate(
            this.pending,
            chunk.subarray(0, bytesRead),
          );
          let newline = this.pending.indexOf(10);
          while (newline >= 0) {
            const line = decoder.decode(this.pending.subarray(0, newline));
            this.pending = this.pending.subarray(newline + 1);
            if (line) events.push(parseEvent(JSON.parse(line)));
            newline = this.pending.indexOf(10);
          }
        }
      } finally {
        await handle.close();
      }
    }
    return events;
  }
}
