// replica/snapshot-stream.ts — a pure, unit-testable streaming decoder for the /snapshot NDJSON feed
// (CTC-132). It turns the response `ReadableStream<Uint8Array>` into BOUNDED batches of parsed
// `WireChange` rows plus the terminal cursor, decoding incrementally with `TextDecoder({stream:true})`
// and buffering only the current partial line — never the whole body, never a whole-array parse.
//
// This is the browser consumer's OOM fix: the old path did `await res.text()` on the ~74 MB body, then
// `body.split("\n")` + a `JSON.parse` per line into a 44.5k-element array, then structured-cloned that
// whole array across `postMessage`. Here at most `batchSize` rows (plus the current partial line) are
// resident at once, so peak memory is O(batch), not O(snapshot).
//
// No OPFS / Worker import lives here on purpose — the module stays runtime-agnostic and tree-shakes
// cleanly, and is fully covered in vitest/jsdom (test/snapshot-stream.test.ts).

import type { WireChange } from "./protocol.js";
import { requirePositiveInt } from "./validate.js";

/** One server /snapshot page; the memory/round-trip knob (mirrors the server-side PAGE_SIZE, CTC-63). */
export const SEED_BATCH_ROWS = 1000;

/** One NDJSON data line off /snapshot — `{accountId, entity, op:"upsert", row}`. */
interface SnapshotDataLine {
  accountId: string;
  entity: string;
  op: "upsert";
  row: Record<string, unknown>;
}
/** The FINAL /snapshot line — `{accountId, cursor, server_time_ms?}`. */
interface SnapshotCursorLine {
  accountId: string;
  cursor: number;
}

/** A yielded item: a bounded batch of rows, or the terminal cursor (always the LAST item). */
export type SnapshotItem =
  | { kind: "batch"; rows: WireChange[] }
  | { kind: "cursor"; cursor: number };

/** Discriminate the two wire line shapes on a numeric `cursor` field (the terminal line carries it). */
function isCursorLine(o: SnapshotDataLine | SnapshotCursorLine): o is SnapshotCursorLine {
  return typeof (o as SnapshotCursorLine).cursor === "number";
}

/**
 * Decode a /snapshot NDJSON body stream into bounded batches of `WireChange` rows, yielding a final
 * `{kind:"cursor"}` item once the terminal cursor line is seen. Throws if the stream ends before a
 * terminal cursor line (a truncated snapshot must not be mistaken for a complete one).
 *
 * At most `batchSize` rows plus the current partial line are held in memory at any moment.
 */
export async function* streamSnapshotBatches(
  body: ReadableStream<Uint8Array>,
  batchSize: number = SEED_BATCH_ROWS,
  /** Fired after every non-terminal `read()` — i.e. whenever the body actually delivered bytes. The
   *  seed's idle timeout re-arms on this, which is what distinguishes a slow-but-live ~100 MB snapshot
   *  from a stalled one. Optional, so every existing caller compiles unchanged. */
  onChunk?: () => void,
): AsyncGenerator<SnapshotItem> {
  // Validate BEFORE reading a byte (CTC-114 review round 12). This helper is exported from
  // `@catalyst-cloud/sdk/browser`, so `batchSize` is consumer-supplied. `NaN`/`Infinity` make
  // `batch.length >= batchSize` permanently false, so no batch is ever yielded: every parsed row of a
  // ~100 MB snapshot accumulates and is finally structured-cloned across the worker boundary in one
  // message. The bounded-memory guarantee this module exists for would be gone, silently — and an OOM
  // is exactly what CTC-132 introduced streaming to prevent.
  requirePositiveInt("streamSnapshotBatches", "batchSize", batchSize);
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  let batch: WireChange[] = [];
  let sawCursor = false;
  let cursor = 0;

  // Parse one complete line. Data lines accumulate into `batch`; the terminal cursor line records the
  // cursor. Returns a full batch to yield when the batch reaches `batchSize`, else null.
  const handleLine = (line: string): SnapshotItem | null => {
    if (line.length === 0) return null; // skip blank lines
    const o = JSON.parse(line) as SnapshotDataLine | SnapshotCursorLine;
    if (isCursorLine(o)) {
      sawCursor = true;
      cursor = o.cursor;
      return null;
    }
    batch.push({ entity: o.entity, op: o.op, row: o.row });
    if (batch.length >= batchSize) {
      const full = batch;
      batch = [];
      return { kind: "batch", rows: full };
    }
    return null;
  };

  let completed = false;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      onChunk?.();
      buf += decoder.decode(value, { stream: true });
      let nl: number;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        const emit = handleLine(line);
        if (emit) yield emit;
      }
    }

    // Flush any final line with no trailing newline (plus any bytes the decoder was still holding).
    const tail = (buf + decoder.decode()).trim();
    if (tail.length > 0) {
      const emit = handleLine(tail);
      if (emit) yield emit;
    }
    // Emit the last partial batch before the cursor so the cursor is always the FINAL yielded item.
    if (batch.length > 0) yield { kind: "batch", rows: batch };
    if (!sawCursor) throw new Error("/snapshot stream ended without a terminal cursor line");
    yield { kind: "cursor", cursor };
    completed = true;
  } finally {
    // On any ABNORMAL exit — a parse throw, a downstream `seedBatch` rejection, or an early consumer
    // `return`/`break` — CANCEL the body so the ~74 MB /snapshot response and its Durable Object
    // generator stop draining server-side; releaseLock alone leaves the underlying fetch stream live
    // (CTC-132 review finding C). On normal completion the stream is already drained, so we only
    // release the lock. reader.cancel() keeps the lock, so releaseLock() still runs on every path.
    if (!completed) {
      try {
        await reader.cancel();
      } catch {
        // The stream already errored/closed — nothing left to cancel; fall through to releaseLock.
      }
    }
    reader.releaseLock();
  }
}
