// tenant-client-changes.test.ts — CTC-2132 Phase 4. `/api/v1/changes` is the ONE route in the client
// where success and failure bodies use different content types: 200 is NDJSON, a refusal is JSON. The
// 409-resync and 403-refusal tests below prove the branch is on `status`, not on `content-type`.

import { describe, expect, it } from "vitest";
import { json, scriptedFetch, text } from "./helpers/scripted-fetch";
import { createTenantClient } from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";

function client(script: Parameters<typeof scriptedFetch>[0]) {
  const net = scriptedFetch(script);
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

describe("changes.stream", () => {
  it("streams NDJSON rows on 200 and reads the head header", async () => {
    const nd = '{"seq":1,"entity":"issues"}\n{"seq":2,"entity":"pulls"}\n';
    const { net, c } = client([() => text(200, nd, { "content-type": "application/x-ndjson", "X-Mirror-Cursor": "2" })]);
    const r = await c.changes.stream({ since: 0 });
    expect(r.outcome).toBe("ok");
    if (r.outcome !== "ok") return;
    expect(r.head).toBe(2);
    const got: unknown[] = [];
    for await (const row of r.rows) got.push(row);
    expect(got).toEqual([
      { seq: 1, entity: "issues" },
      { seq: 2, entity: "pulls" },
    ]);
    expect(net.calls[0]!.headers["accept"]).toBe("application/x-ndjson");
  });

  it("⭐ a 409 underflow is a typed resync arm carrying the head, NOT a throw", async () => {
    const { c } = client([() => json(409, { type: "resync" }, { "X-Mirror-Cursor": "900" })]);
    const r = await c.changes.stream({ since: 1 });
    expect(r.outcome).toBe("resync");
    if (r.outcome === "resync") expect(r.head).toBe(900);
  });

  it("⭐ a JSON refusal on the NDJSON route folds into the shared failure union", async () => {
    const { c } = client([() => json(403, { error: "missing scope", required: "mirror:feed" })]);
    const r = await c.changes.stream({ since: 1 });
    expect(r.outcome).toBe("forbidden");
    if (r.outcome === "forbidden") expect(r.required).toBe("mirror:feed");
  });

  it("since:'head' is sent verbatim, not coerced to a number", async () => {
    const { net, c } = client([() => text(200, "", { "content-type": "application/x-ndjson" })]);
    await c.changes.stream({ since: "head" });
    expect(new URL(net.calls[0]!.url).searchParams.get("since")).toBe("head");
  });

  it("an abort signal stops the stream", async () => {
    const controller = new AbortController();
    const lines = ['{"seq":1}\n', '{"seq":2}\n', '{"seq":3}\n'];
    let i = 0;
    const encoder = new TextEncoder();
    const stream = new ReadableStream<Uint8Array>({
      pull(ctrl) {
        if (i < lines.length) ctrl.enqueue(encoder.encode(lines[i++]!));
        else ctrl.close();
      },
    });
    const net = scriptedFetch([() => new Response(stream, { status: 200, headers: { "content-type": "application/x-ndjson" } })]);
    const c = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const r = await c.changes.stream({ since: 0, signal: controller.signal });
    if (r.outcome !== "ok") throw new Error("expected ok");
    const got: unknown[] = [];
    await expect(async () => {
      for await (const row of r.rows) {
        got.push(row);
        if (got.length === 1) controller.abort(new Error("stop"));
      }
    }).rejects.toThrow();
    expect(got).toHaveLength(1);
  });
});

describe("changes.list", () => {
  it("buffers the same rows and never throws", async () => {
    const nd = '{"seq":1}\n{"seq":2}\n';
    const { c } = client([() => text(200, nd, { "content-type": "application/x-ndjson", "X-Mirror-Cursor": "2" })]);
    const r = await c.changes.list({ since: 0 });
    expect(r).toEqual({ outcome: "ok", status: 200, head: 2, rows: [{ seq: 1 }, { seq: 2 }] });
  });

  it("returns the `network` arm when the stream faults mid-body", async () => {
    const { c } = client([
      () =>
        new Response('{"seq":1}\nnot-json\n', {
          status: 200,
          headers: { "content-type": "application/x-ndjson" },
        }),
    ]);
    const r = await c.changes.list({ since: 0 });
    expect(r.outcome).toBe("network");
  });

  it("passes a resync through unchanged", async () => {
    const { c } = client([() => json(409, { type: "resync" }, { "X-Mirror-Cursor": "50" })]);
    const r = await c.changes.list({ since: 1 });
    expect(r).toMatchObject({ outcome: "resync", head: 50 });
  });
});

// ⭐ Regression, CTC-2132 validate attempt 1 / code-review Finding 2. `sendRaw` attached
// `AbortSignal.timeout(timeoutMs)` — a WALL-CLOCK deadline that keeps governing the response BODY
// after the headers arrive — so any `/changes` replay slower than `timeoutMs` was truncated and lost
// every row already read, defeating the whole point of `stream()`. The deadline is now an IDLE one,
// rearmed per chunk via `iterateNdjson`'s `onProgress` (the same refund the replica's snapshot seed
// uses). The two tests below pin BOTH halves: a feed that keeps delivering must never expire, and a
// feed that STALLS must still expire — the fix must not have simply removed the deadline.
//
// The stand-in below honours `init.signal` on the BODY the way a real transport does; the recording
// `scriptedFetch` helper ignores it, which is why this defect could land under a green suite.
function dribbleFetch(rows: string[], gapMs: number, stallAtEnd = false): typeof fetch {
  const impl = async (_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const signal = init?.signal ?? null;
    const encoder = new TextEncoder();
    let i = 0;
    const stream = new ReadableStream<Uint8Array>({
      async pull(ctrl) {
        const aborted = (): Error => (signal?.reason instanceof Error ? signal.reason : new Error("aborted"));
        if (i < rows.length) {
          await new Promise((r) => setTimeout(r, gapMs));
          if (signal?.aborted === true) return void ctrl.error(aborted());
          ctrl.enqueue(encoder.encode(rows[i++]!));
          return;
        }
        if (!stallAtEnd) return void ctrl.close();
        await new Promise<void>((resolve) => {
          if (signal === null || signal.aborted) return resolve();
          signal.addEventListener("abort", () => resolve(), { once: true });
        });
        ctrl.error(aborted());
      },
    });
    return new Response(stream, { status: 200, headers: { "content-type": "application/x-ndjson", "X-Mirror-Cursor": "12" } });
  };
  return impl as typeof fetch;
}

describe("the /changes deadline is IDLE, not wall-clock", () => {
  it("⭐ a replay that takes longer than timeoutMs overall — but never stalls — completes in full", async () => {
    const rows = Array.from({ length: 12 }, (_, n) => `{"seq":${n + 1}}\n`);
    const c = createTenantClient({
      key: KEY,
      baseUrl: BASE,
      // 12 × 25 ms ≈ 300 ms of feed against a 120 ms deadline: it outlives the WALL CLOCK by 2.5×
      // while every individual gap sits ~5× inside it.
      timeoutMs: 120,
      fetch: dribbleFetch(rows, 25),
    });
    const r = await c.changes.list({ since: 0 });
    expect(r.outcome).toBe("ok");
    if (r.outcome !== "ok") return;
    expect(r.rows).toHaveLength(12);
    expect(r.rows[11]).toEqual({ seq: 12 });
  });

  it("⭐ a feed that STALLS past timeoutMs still expires — the deadline was rearmed, not removed", async () => {
    const c = createTenantClient({
      key: KEY,
      baseUrl: BASE,
      timeoutMs: 80,
      fetch: dribbleFetch(['{"seq":1}\n'], 5, true),
    });
    const r = await c.changes.list({ since: 0 });
    expect(r.outcome).toBe("network");
    if (r.outcome === "network") expect(r.reason).toContain("without progress");
  });
});
