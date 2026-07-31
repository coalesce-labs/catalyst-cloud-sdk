import { describe, expect, it } from "vitest";
import { streamSnapshotBatches } from "../../src/replica/browser/snapshot-stream.js";

// Build a ReadableStream<Uint8Array> from arbitrary byte chunks (to exercise split-boundary handling).
function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
  const enc = new TextEncoder();
  return new ReadableStream({
    start(c) {
      for (const s of chunks) c.enqueue(enc.encode(s));
      c.close();
    },
  });
}

// A ReadableStream that records whether the consumer CANCELLED it — the browser twin is the ~74 MB
// /snapshot fetch body we must abort on early exit (CTC-132 review finding C). Chunks are enqueued and
// the controller is closed, so a full drain completes normally; an early break/throw cancels instead.
function cancellableStream(chunks: string[]): {
  stream: ReadableStream<Uint8Array>;
  cancelled: () => boolean;
} {
  const enc = new TextEncoder();
  let wasCancelled = false;
  const stream = new ReadableStream({
    start(c) {
      for (const s of chunks) c.enqueue(enc.encode(s));
      c.close();
    },
    cancel() {
      wasCancelled = true;
    },
  });
  return { stream, cancelled: () => wasCancelled };
}

describe("streamSnapshotBatches", () => {
  it("yields data rows in bounded batches and returns the terminal cursor", async () => {
    const body =
      `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"1"}}\n` +
      `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"2"}}\n` +
      `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"3"}}\n` +
      `{"accountId":"a","cursor":42,"server_time_ms":1}\n`;
    const batches: unknown[][] = [];
    let cursor = -1;
    for await (const b of streamSnapshotBatches(streamOf([body]), 2)) {
      if (b.kind === "batch") batches.push(b.rows);
      else cursor = b.cursor;
    }
    expect(batches.map((r) => r.length)).toEqual([2, 1]); // batchSize=2 → [2,1]
    expect(cursor).toBe(42);
    // rows are WireChange-shaped: {entity, op, row}
    expect(batches[0][0]).toMatchObject({ entity: "issue", op: "upsert", row: { id: "1" } });
  });

  it("splits lines correctly when JSON is fragmented across chunk boundaries", async () => {
    const line = `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"1"}}\n`;
    const mid = Math.floor(line.length / 2);
    const stream = streamOf([
      line.slice(0, mid),
      line.slice(mid),
      `{"accountId":"a","cursor":7}\n`,
    ]);
    const rows: unknown[] = [];
    let cursor = -1;
    for await (const b of streamSnapshotBatches(stream, 1000)) {
      if (b.kind === "batch") rows.push(...b.rows);
      else cursor = b.cursor;
    }
    expect(rows).toHaveLength(1);
    expect(cursor).toBe(7);
  });

  it("handles a final line with no trailing newline", async () => {
    const stream = streamOf([
      `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"1"}}\n`,
      `{"accountId":"a","cursor":9}`, // no trailing \n
    ]);
    let cursor = -1;
    let count = 0;
    for await (const b of streamSnapshotBatches(stream, 1000)) {
      if (b.kind === "batch") count += b.rows.length;
      else cursor = b.cursor;
    }
    expect(count).toBe(1);
    expect(cursor).toBe(9);
  });

  it("skips blank lines and yields an empty-snapshot cursor with zero rows", async () => {
    const stream = streamOf([`\n`, `{"accountId":"a","cursor":0}\n`]);
    let cursor = -1;
    let count = 0;
    for await (const b of streamSnapshotBatches(stream, 1000)) {
      if (b.kind === "batch") count += b.rows.length;
      else cursor = b.cursor;
    }
    expect(count).toBe(0);
    expect(cursor).toBe(0);
  });

  it("throws if the stream ends before a terminal cursor line", async () => {
    const stream = streamOf([
      `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"1"}}\n`,
    ]);
    await expect(async () => {
      for await (const _ of streamSnapshotBatches(stream, 1000)) void _;
    }).rejects.toThrow(/cursor/i);
  });

  it("cancels the body when the consumer returns early (CTC-132 finding C)", async () => {
    // Rows in chunk 0, the terminal cursor in chunk 1 — so an early break leaves chunk 1 UNREAD and the
    // body still open, which is exactly when cancel must fire (a fully-drained body is already closed).
    const { stream, cancelled } = cancellableStream([
      `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"1"}}\n` +
        `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"2"}}\n` +
        `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"3"}}\n`,
      `{"accountId":"a","cursor":42}\n`,
    ]);
    for await (const b of streamSnapshotBatches(stream, 2)) {
      if (b.kind === "batch") break; // early exit after the first bounded batch (a downstream failure)
    }
    expect(cancelled()).toBe(true);
  });

  it("cancels the body when a line fails to parse", async () => {
    // Bad line in chunk 0, more data queued in chunk 1 — the throw happens before chunk 1 is read, so
    // the still-open body must be cancelled.
    const { stream, cancelled } = cancellableStream([
      `{ not valid json }\n`,
      `{"accountId":"a","cursor":1}\n`,
    ]);
    await expect(async () => {
      for await (const _ of streamSnapshotBatches(stream, 1000)) void _;
    }).rejects.toThrow();
    expect(cancelled()).toBe(true);
  });

  it("does NOT cancel the body on a clean full drain", async () => {
    const { stream, cancelled } = cancellableStream([
      `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"1"}}\n` +
        `{"accountId":"a","cursor":9}\n`,
    ]);
    let cursor = -1;
    for await (const b of streamSnapshotBatches(stream, 1000)) {
      if (b.kind === "cursor") cursor = b.cursor;
    }
    expect(cursor).toBe(9);
    expect(cancelled()).toBe(false); // fully drained → releaseLock only, no cancel
  });
});
