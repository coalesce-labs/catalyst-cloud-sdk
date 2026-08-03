import { describe, expect, it, vi } from "vitest";
import { streamSeedIntoWorker } from "../../src/replica/browser/browser-replica.js";

function bodyOf(ndjson: string): ReadableStream<Uint8Array> {
  const enc = new TextEncoder();
  return new ReadableStream({
    start(c) {
      c.enqueue(enc.encode(ndjson));
      c.close();
    },
  });
}

describe("streamSeedIntoWorker", () => {
  it("drives begin → batch(es) → commit and returns the cursor", async () => {
    const calls: string[] = [];
    const call = vi.fn(async (req: { type: string; cursor?: number }) => {
      calls.push(req.type);
      return req.type === "seedCommit" ? req.cursor : undefined;
    });
    const ndjson =
      `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"1"}}\n` +
      `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"2"}}\n` +
      `{"accountId":"a","cursor":50}\n`;
    const cursor = await streamSeedIntoWorker(call as never, bodyOf(ndjson), 1);
    expect(cursor).toBe(50);
    expect(calls).toEqual(["seedBegin", "seedBatch", "seedBatch", "seedCommit"]);
  });

  it("posts seedAbort when a batch apply fails", async () => {
    const calls: string[] = [];
    const call = vi.fn(async (req: { type: string }) => {
      calls.push(req.type);
      if (req.type === "seedBatch") throw new Error("boom");
      return undefined;
    });
    const ndjson = `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"1"}}\n{"accountId":"a","cursor":1}\n`;
    await expect(streamSeedIntoWorker(call as never, bodyOf(ndjson), 1000)).rejects.toThrow(/boom/);
    expect(calls).toContain("seedAbort");
  });

  it("posts seedAbort and rethrows when seedBegin itself fails", async () => {
    // CTC-132 verify (review finding B): seedBegin is INSIDE the try, so a mid-begin
    // failure (e.g. truncate threw with the txn already open) must still roll back.
    const calls: string[] = [];
    const call = vi.fn(async (req: { type: string }) => {
      calls.push(req.type);
      if (req.type === "seedBegin") throw new Error("begin boom");
      return undefined;
    });
    const ndjson = `{"accountId":"a","cursor":1}\n`;
    await expect(streamSeedIntoWorker(call as never, bodyOf(ndjson), 1000)).rejects.toThrow(
      /begin boom/,
    );
    expect(calls).toEqual(["seedBegin", "seedAbort"]);
  });

  it("surfaces the original error even when seedAbort also throws", async () => {
    // CTC-132 verify: the inner `catch {}` around seedAbort must not mask the primary error.
    const call = vi.fn(async (req: { type: string }) => {
      if (req.type === "seedBatch") throw new Error("primary boom");
      if (req.type === "seedAbort") throw new Error("abort boom");
      return undefined;
    });
    const ndjson = `{"accountId":"a","entity":"issue","op":"upsert","row":{"id":"1"}}\n{"accountId":"a","cursor":1}\n`;
    await expect(streamSeedIntoWorker(call as never, bodyOf(ndjson), 1000)).rejects.toThrow(
      /primary boom/,
    );
  });
});
