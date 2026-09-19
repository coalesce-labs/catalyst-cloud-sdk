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
    const r = await c.changes.stream({ since: "head" });
    expect(new URL(net.calls[0]!.url).searchParams.get("since")).toBe("head");
    // This test asserts only the wire shape and never iterates — the exact abandonment shape
    // Finding 3 is about. `close()` is how such a caller releases the body it did not read.
    if (r.outcome === "ok") await r.close();
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

// ⭐ Regression, CTC-2132 validate attempt 14 / code-review Findings 1–4. All four defects lived in
// the `sendRaw()` + `answerOf()` + `idleDeadline()` hand-back, and they shared one root cause:
// handing the caller a raw `Response` plus a manually-driven deadline moved three responsibilities
// `send()` had discharged itself — body-read error handling, deadline LIFETIME, and signal
// propagation — onto callers that discharged them only partly.
//
// ⛔ The recording `scriptedFetch` helper IGNORES `init.signal`, which is precisely why three of the
// four landed under a green suite. Every stand-in below honours the signal the way a real transport
// does: it is the signal-awareness, not the assertion, that makes these tests bite.

/** A response whose body faults the moment it is read — a refusal whose socket resets mid-body. */
function faultingBody(status: number, headers: Record<string, string> = {}): Response {
  const stream = new ReadableStream<Uint8Array>({
    start(ctrl) {
      ctrl.error(new Error("socket reset mid-refusal-body"));
    },
  });
  return new Response(stream, { status, headers: { "content-type": "application/json", ...headers } });
}

/** Headers arrive with `status`; the body then STALLS until the signal aborts. */
function stalledBody(status: number, headers: Record<string, string> = {}): typeof fetch {
  const impl = async (_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const signal = init?.signal ?? null;
    const aborted = (): Error => (signal?.reason instanceof Error ? signal.reason : new Error("aborted"));
    const stream = new ReadableStream<Uint8Array>({
      async pull(ctrl) {
        await new Promise<void>((resolve) => {
          if (signal === null || signal.aborted) return resolve();
          signal.addEventListener("abort", () => resolve(), { once: true });
        });
        ctrl.error(aborted());
      },
    });
    return new Response(stream, { status, headers: { "content-type": "application/json", ...headers } });
  };
  return impl as typeof fetch;
}

/** An NDJSON 200 whose body honours the fetch signal — an abort ERRORS the stream, as a socket does. */
function signalAwareNdjson(rows: string[], headers: Record<string, string> = {}): { fetch: typeof fetch; cancelled: () => boolean } {
  let cancelled = false;
  const impl = async (_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const signal = init?.signal ?? null;
    const encoder = new TextEncoder();
    let i = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(ctrl) {
        if (signal?.aborted === true) {
          ctrl.error(signal.reason instanceof Error ? signal.reason : new Error("aborted"));
          return;
        }
        if (i < rows.length) {
          ctrl.enqueue(encoder.encode(rows[i++]!));
          return;
        }
        ctrl.close();
      },
      cancel() {
        cancelled = true;
      },
    });
    return new Response(stream, {
      status: 200,
      headers: { "content-type": "application/x-ndjson", "X-Mirror-Cursor": "7", ...headers },
    });
  };
  return { fetch: impl as typeof fetch, cancelled: () => cancelled };
}

/** A transport whose headers NEVER arrive: it settles only when the signal it was handed aborts. */
function neverAnswering(): { fetch: typeof fetch; signals: (AbortSignal | null)[] } {
  const signals: (AbortSignal | null)[] = [];
  const impl = async (_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const signal = init?.signal ?? null;
    signals.push(signal);
    return await new Promise<Response>((_resolve, reject) => {
      const fail = (): void => reject(signal?.reason instanceof Error ? signal.reason : new Error("aborted"));
      if (signal === null) return;
      if (signal.aborted) return fail();
      signal.addEventListener("abort", fail, { once: true });
    });
  };
  return { fetch: impl as typeof fetch, signals };
}

/** Race a call against a wall clock: `"HUNG"` means it never settled, which is the defect's shape. */
async function outcomeOrHang(call: Promise<{ outcome: string }>, ms = 1_200): Promise<string> {
  return await Promise.race([
    call.then(
      (r) => r.outcome,
      (err: unknown) => `THREW:${err instanceof Error ? err.message : String(err)}`,
    ),
    new Promise<string>((resolve) => setTimeout(() => resolve("HUNG"), ms)),
  ]);
}

describe("the /changes + /snapshot refusal path never throws (Finding 1)", () => {
  it("⭐ a refusal body that faults mid-read is changes.list's `network` arm, NOT a throw", async () => {
    const { c } = client([() => faultingBody(403)]);
    expect(await outcomeOrHang(c.changes.list({ since: 1 }))).toBe("network");
  });

  it("⭐ a refusal body that faults mid-read is changes.stream's `network` arm, NOT a throw", async () => {
    const { c } = client([() => faultingBody(500)]);
    expect(await outcomeOrHang(c.changes.stream({ since: 1 }))).toBe("network");
  });

  it("⭐ a 409 whose body faults is the `network` arm — the resync reason is read, not assumed", async () => {
    const { c } = client([() => faultingBody(409, { "X-Mirror-Cursor": "900" })]);
    expect(await outcomeOrHang(c.changes.stream({ since: 1 }))).toBe("network");
  });
});

describe("the refusal body is read UNDER the idle deadline (Finding 2)", () => {
  it("⭐ a 409 whose refusal body stalls expires on the deadline instead of wedging forever", async () => {
    const c = createTenantClient({ key: KEY, baseUrl: BASE, timeoutMs: 80, fetch: stalledBody(409, { "X-Mirror-Cursor": "900" }) });
    expect(await outcomeOrHang(c.changes.stream({ since: 1 }))).toBe("network");
  });

  it("⭐ a 503 whose refusal body stalls expires on the deadline too", async () => {
    const c = createTenantClient({ key: KEY, baseUrl: BASE, timeoutMs: 80, fetch: stalledBody(503) });
    expect(await outcomeOrHang(c.changes.list({ since: 1 }))).toBe("network");
  });
});

describe("an un-iterated stream is released, not leaked (Finding 3)", () => {
  it("⭐ close() cancels the body of a stream whose rows are never iterated", async () => {
    const net = signalAwareNdjson(['{"seq":1}\n']);
    const c = createTenantClient({ key: KEY, baseUrl: BASE, timeoutMs: 60, fetch: net.fetch });
    const r = await c.changes.stream({ since: 0 });
    if (r.outcome !== "ok") throw new Error(`expected ok, got ${r.outcome}`);
    expect(r.head).toBe(7);
    expect(net.cancelled()).toBe(false);
    await r.close();
    expect(net.cancelled()).toBe(true);
    await r.close(); // idempotent: a second release is not an error
  });

  it("⭐ the idle deadline starts at the first READ, so a caller slow to iterate is not aborted", async () => {
    const net = signalAwareNdjson(['{"seq":1}\n', '{"seq":2}\n']);
    const c = createTenantClient({ key: KEY, baseUrl: BASE, timeoutMs: 50, fetch: net.fetch });
    const r = await c.changes.stream({ since: 0 });
    if (r.outcome !== "ok") throw new Error(`expected ok, got ${r.outcome}`);
    // 3× the deadline with no read at all: an eagerly-armed timer aborts the fetch here.
    await new Promise((resolve) => setTimeout(resolve, 150));
    const got: unknown[] = [];
    for await (const row of r.rows) got.push(row);
    expect(got).toEqual([{ seq: 1 }, { seq: 2 }]);
  });
});

describe("the caller's signal reaches the fetch (Finding 4)", () => {
  it("⭐ an abort raised BEFORE the headers arrive cancels the in-flight request", async () => {
    const controller = new AbortController();
    const net = neverAnswering();
    // A deadline far beyond the race: only the CALLER's signal can settle this call.
    const c = createTenantClient({ key: KEY, baseUrl: BASE, timeoutMs: 30_000, fetch: net.fetch });
    const pending = c.changes.stream({ since: 0, signal: controller.signal });
    await new Promise((resolve) => setTimeout(resolve, 20));
    controller.abort(new Error("caller went away"));
    expect(await outcomeOrHang(pending)).toBe("network");
    expect(net.signals[0]?.aborted).toBe(true);
  });

  it("⭐ a signal already aborted before the call never leaves a request in flight", async () => {
    const net = neverAnswering();
    const c = createTenantClient({ key: KEY, baseUrl: BASE, timeoutMs: 30_000, fetch: net.fetch });
    expect(await outcomeOrHang(c.changes.stream({ since: 0, signal: AbortSignal.abort() }))).toBe("network");
  });
});
