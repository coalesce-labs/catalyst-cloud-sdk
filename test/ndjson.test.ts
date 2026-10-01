// ndjson.test.ts — CTC-2132 Phase 2. Direct coverage of the leaf module extracted from
// catalyst-replica.ts, over the paths the tenant client's `/changes` stream newly depends on. The
// existing replica tests (test/node/catalyst-replica.test.ts, test/browser/replica-seed-stream.test.ts,
// test/browser/snapshot-stream.test.ts) already cover this function through `/snapshot` — unchanged.

import { describe, expect, it } from "vitest";
import { iterateNdjson } from "../src/ndjson";

function streamedResponse(chunks: string[], status = 200): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
  return new Response(stream, { status });
}

describe("iterateNdjson", () => {
  it("yields non-empty lines from a streamed body, buffering partial lines across chunks", async () => {
    const res = streamedResponse(['{"a":1}\n{"a"', ':2}\n', '{"a":3}\n']);
    const got: string[] = [];
    for await (const line of iterateNdjson(res)) got.push(line);
    expect(got).toEqual(['{"a":1}', '{"a":2}', '{"a":3}']);
  });

  it("yields lines from a buffered body when response.body is absent (injected fetch stand-in)", async () => {
    const res = { body: null, text: async () => '{"a":1}\n{"a":2}\n' } as unknown as Response;
    const got: string[] = [];
    for await (const line of iterateNdjson(res)) got.push(line);
    expect(got).toEqual(['{"a":1}', '{"a":2}']);
  });

  it("cancels the reader and throws the abort reason when the signal fires mid-stream", async () => {
    const controller = new AbortController();
    let cancelled = false;
    const encoder = new TextEncoder();
    const stream = new ReadableStream<Uint8Array>({
      pull(c) {
        c.enqueue(encoder.encode('{"a":1}\n'));
        controller.abort(new Error("stop now"));
      },
      cancel() {
        cancelled = true;
      },
    });
    const res = new Response(stream, { status: 200 });
    await expect(async () => {
      for await (const _line of iterateNdjson(res, { signal: controller.signal })) {
        // drain until the abort fires
      }
    }).rejects.toThrow("stop now");
    expect(cancelled).toBe(true);
  });

  it("waits for the first underlying reader-cancel acknowledgement before finishing abort", async () => {
    const controller = new AbortController();
    let acknowledgeCancel!: () => void;
    let cancelStarted = false;
    let settled = false;
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new TextEncoder().encode('{"a":1}\n'));
      },
      pull() {
        return new Promise<void>(() => {});
      },
      cancel() {
        cancelStarted = true;
        return new Promise<void>((resolve) => { acknowledgeCancel = resolve; });
      },
    });
    const consuming = (async () => {
      for await (const _line of iterateNdjson(new Response(stream), { signal: controller.signal })) {
        // The next read is held open so abort must cancel the reader.
      }
    })().finally(() => { settled = true; });
    await Promise.resolve();
    controller.abort(new DOMException("stop now", "AbortError"));
    await Promise.resolve();
    expect(cancelStarted).toBe(true);
    expect(settled).toBe(false);

    acknowledgeCancel();
    await expect(consuming).rejects.toThrow("stop now");
    expect(settled).toBe(true);
  });

  it("surfaces a failed underlying cancellation instead of acknowledging cleanup", async () => {
    const controller = new AbortController();
    let failCancel!: (error: Error) => void;
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new TextEncoder().encode('{"a":1}\n'));
      },
      pull() {
        return new Promise<void>(() => {});
      },
      cancel() {
        return new Promise<void>((_resolve, reject) => { failCancel = reject; });
      },
    });
    const consuming = (async () => {
      for await (const _line of iterateNdjson(new Response(stream), { signal: controller.signal })) {
        // Hold the body until the signal cancels its reader.
      }
    })();
    await Promise.resolve();
    controller.abort(new DOMException("stop now", "AbortError"));
    await Promise.resolve();
    failCancel(new Error("cancel cleanup failed"));
    await expect(consuming).rejects.toThrow("cancel cleanup failed");
  });

  it("emits a trailing line with no final newline", async () => {
    const res = streamedResponse(['{"a":1}\n{"a":2}']);
    const got: string[] = [];
    for await (const line of iterateNdjson(res)) got.push(line);
    expect(got).toEqual(['{"a":1}', '{"a":2}']);
  });
});
