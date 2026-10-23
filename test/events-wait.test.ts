import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CatalystEvent } from "@catalyst-cloud/schema";
import { checkEventFilter, matchesEventFilter } from "../src/event-filter.js";
import {
  DEFAULT_EVENT_WAIT_TIMEOUT_MS,
  EVENT_WAIT_EXIT_CODES,
  waitForEvents,
  watchEvents,
  type EventWaitOptions,
  type EventWaitResult,
} from "../src/events-wait.js";
import { recordingFactory } from "./helpers/fake-events-socket.js";
import { json } from "./helpers/scripted-fetch.js";

const REF = "coalesce-labs/catalyst-cloud:ryan/stack-a";
function conflict(sequence: number, branch = "ryan/stack-a"): CatalystEvent {
  return {
    tenantId: "tenant-1",
    sequence,
    eventId: `conflict-${sequence}`,
    type: "pr.conflict.detected",
    schemaVersion: 1,
    recordedAt: "2026-10-22T20:00:00.000Z",
    entity: { type: "pull_request", id: "coalesce-labs/catalyst-cloud#9001" },
    payload: {
      repo: "coalesce-labs/catalyst-cloud",
      branch,
      pr: 9001,
      ticket: "CTC-1234",
      paths: ["src/a.ts", "src/b.ts"],
      pathsKnown: true,
    },
  };
}
function fakeCloud(initialHead = 5000) {
  const calls: URL[] = [];
  const state = {
    head: initialHead,
    coverage: initialHead,
    from: 0,
    events: [] as CatalystEvent[],
    backboneStatus: 200,
    preflight: undefined as (() => Response) | undefined,
    queryOverride: undefined as
      ((url: URL) => Response | undefined) | undefined,
  };
  const fetch: typeof globalThis.fetch = async (input) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    calls.push(url);
    if (url.pathname.endsWith("/events/query")) {
      if (url.searchParams.get("order") === "desc" && state.preflight)
        return state.preflight();
      const overridden = state.queryOverride?.(url);
      if (overridden) return overridden;
      const inputFilter = Object.fromEntries(
        ["type", "ticket", "repo", "ref", "pr", "session"]
          .map((k) => [k, url.searchParams.getAll(k)])
          .filter(([, v]) => v.length),
      );
      const checked = checkEventFilter(inputFilter);
      if (!checked.ok) throw new Error(checked.reason);
      const asc = url.searchParams.get("order") === "asc";
      const after = Number(url.searchParams.get("afterSeq") ?? -1);
      const before = Number(
        url.searchParams.get("beforeSeq") ?? Number.MAX_SAFE_INTEGER,
      );
      const rows = state.events
        .filter(
          (e) =>
            e.sequence <= state.coverage &&
            e.sequence > after &&
            e.sequence < before &&
            matchesEventFilter(e, checked.filter),
        )
        .sort((a, b) =>
          asc ? a.sequence - b.sequence : b.sequence - a.sequence,
        );
      const limit = Number(url.searchParams.get("limit") ?? 50);
      const page = rows.slice(0, limit);
      const last = page.at(-1);
      return json(200, {
        events: page,
        next:
          rows.length > page.length && last
            ? { param: asc ? "afterSeq" : "beforeSeq", value: last.sequence }
            : null,
        coverage: { indexedFromSeq: state.from, indexedToSeq: state.coverage },
      });
    }
    if (!url.pathname.endsWith("/events/backbone"))
      throw new Error(`Unexpected route ${url}`);
    if (state.backboneStatus !== 200)
      return json(state.backboneStatus, { reason: "no backbone reach" });
    const since = Number(url.searchParams.get("since"));
    if (since === Number.MAX_SAFE_INTEGER)
      return json(409, {
        error: "cursor_ahead_of_head",
        resumeFrom: state.head,
      });
    return new Response(
      state.events
        .filter((e) => e.sequence > since)
        .map((e) => JSON.stringify(e) + "\n")
        .join(""),
      { headers: { "x-catalyst-event-backbone-head-seq": String(state.head) } },
    );
  };
  return { state, calls, fetch };
}
const active: Array<{
  controller: AbortController;
  result: Promise<EventWaitResult>;
}> = [];
beforeEach(() => vi.useFakeTimers());
afterEach(async () => {
  for (const run of active) run.controller.abort();
  await Promise.all(active.splice(0).map((run) => run.result));
  vi.useRealTimers();
  vi.unstubAllEnvs();
});
function setup(extra: Partial<EventWaitOptions> = {}, head = 5000) {
  const cloud = fakeCloud(head);
  const sockets = recordingFactory();
  const controller = new AbortController();
  const options: EventWaitOptions = {
    baseUrl: "https://cloud.invalid",
    tenantId: "tenant-1",
    auth: { kind: "token", token: "seat-key" },
    filter: { type: "pr.conflict.detected", ref: REF },
    signal: controller.signal,
    fetch: cloud.fetch,
    wsFactory: sockets.factory,
    ...extra,
  };
  return { cloud, ...sockets, controller, options };
}
function launch(run: ReturnType<typeof setup>) {
  const result = waitForEvents(run.options);
  active.push({ controller: run.controller, result });
  return result;
}
async function live(run: ReturnType<typeof setup>, index = 0) {
  await vi.advanceTimersByTimeAsync(0);
  const socket = run.sockets[index];
  if (!socket) throw new Error(`socket ${index} was not opened`);
  socket.answerPings = true;
  socket.subscribe();
  await vi.advanceTimersByTimeAsync(0);
  return socket;
}
function frame(
  after: number,
  through: number,
  events: unknown[] = [],
  gap = false,
) {
  return { type: "events", after, through, events, gap };
}
function sequences(result: EventWaitResult) {
  return result.outcome === "matched"
    ? result.events.map((e) => e.sequence)
    : [];
}

describe("direct event waits", () => {
  it("S1: wakes on a conflict for its branch and prints PR #9001 and the conflicting paths, well inside 10 s", async () => {
    const run = setup({}, 9100);
    const result = launch(run);
    const ws = await live(run);
    ws.deliver(frame(9100, 9101, [conflict(9101)]));
    await vi.advanceTimersByTimeAsync(1);
    expect(await result).toEqual({
      outcome: "matched",
      cursor: 9101,
      events: [conflict(9101)],
    });
    const url = new URL(run.urls[0] ?? "");
    expect(url.searchParams.get("type")).toBe("pr.conflict.detected");
    expect(url.searchParams.get("ref")).toBe(REF);
  });
  it("S2: a 4-hour wait with no match is still pending on one socket at 3 h, then times out at 4 h", async () => {
    const run = setup();
    const result = launch(run);
    let finished = false;
    void result.then(() => {
      finished = true;
    });
    await live(run);
    await vi.advanceTimersByTimeAsync(3 * 60 * 60 * 1000);
    expect(finished).toBe(false);
    expect(run.sockets).toHaveLength(1);
    expect(run.cloud.calls.length).toBeLessThanOrEqual(25);
    expect(
      run.cloud.calls.filter(
        (u) =>
          u.pathname.endsWith("backbone") &&
          u.searchParams.get("since") !== String(Number.MAX_SAFE_INTEGER),
      ),
    ).toEqual([]);
    await vi.advanceTimersByTimeAsync(60 * 60 * 1000);
    expect(await result).toEqual({
      outcome: "timeout",
      cursor: 5000,
      waitedMs: DEFAULT_EVENT_WAIT_TIMEOUT_MS,
    });
  });
  it("S3: a wait that lost its socket at 5000 delivers 5003 when it reconnects", async () => {
    const run = setup();
    const result = launch(run);
    const ws = await live(run);
    ws.serverClose(1006);
    run.cloud.state.events = [
      conflict(5001, "other"),
      conflict(5003),
      conflict(5004, "other"),
    ];
    run.cloud.state.head = 5004;
    run.cloud.state.coverage = 5004;
    await vi.advanceTimersByTimeAsync(1000);
    await live(run, 1);
    expect(await result).toMatchObject({ outcome: "matched", cursor: 5004 });
    expect(sequences(await result)).toEqual([5003]);
  });
  it("S3b: after a gap it keeps replaying until the index covers through, and delivers nothing early", async () => {
    const run = setup();
    const result = launch(run);
    const ws = await live(run);
    let finished = false;
    void result.then(() => {
      finished = true;
    });
    run.cloud.state.events = [conflict(5003)];
    run.cloud.state.head = 5004;
    run.cloud.state.coverage = 5001;
    ws.deliver(frame(5000, 5004, [], true));
    await vi.advanceTimersByTimeAsync(2000);
    expect(finished).toBe(false);
    run.cloud.state.coverage = 5004;
    await vi.advanceTimersByTimeAsync(4000);
    expect(sequences(await result)).toEqual([5003]);
    expect(await result).toMatchObject({ cursor: 5004 });
  });
  it("S3c: when the index stays behind for 30 s it falls back to the backbone and matches locally", async () => {
    const run = setup();
    const result = launch(run);
    const ws = await live(run);
    run.cloud.state.events = [
      conflict(5002, "other"),
      conflict(5003),
      conflict(5004, "other"),
    ];
    run.cloud.state.head = 5004;
    run.cloud.state.coverage = 5001;
    ws.deliver(frame(5000, 5004, [], true));
    await vi.advanceTimersByTimeAsync(30000);
    expect(sequences(await result)).toEqual([5003]);
    expect(
      run.cloud.calls.some(
        (u) =>
          u.pathname.endsWith("backbone") &&
          u.searchParams.get("since") === "5001",
      ),
    ).toBe(true);
  });
  it("S3d: when the backbone refuses the credential it keeps polling the index instead", async () => {
    const run = setup();
    const result = launch(run);
    const ws = await live(run);
    let finished = false;
    void result.then(() => {
      finished = true;
    });
    run.cloud.state.events = [conflict(5003)];
    run.cloud.state.head = 5004;
    run.cloud.state.coverage = 5001;
    run.cloud.state.backboneStatus = 403;
    ws.deliver(frame(5000, 5004, [], true));
    await vi.advanceTimersByTimeAsync(50000);
    expect(finished).toBe(false);
    run.cloud.state.coverage = 5004;
    await vi.advanceTimersByTimeAsync(10000);
    expect(sequences(await result)).toEqual([5003]);
  });
  it("S5: a ticket outside the seat's teams is refused before any socket opens", async () => {
    const run = setup({ filter: { ticket: "OPS-77" } });
    run.cloud.state.preflight = () =>
      json(403, {
        outcome: "rejected",
        reason: "your seat does not include team OPS",
      });
    expect(await launch(run)).toMatchObject({
      outcome: "refused",
      source: "cloud",
      status: 403,
      filter: { ticket: ["OPS-77"] },
      reason: expect.stringContaining("team OPS"),
    });
    expect(run.sockets).toEqual([]);
    expect(EVENT_WAIT_EXIT_CODES.refused).toBe(3);
  });
  it("S5b: a malformed filter is refused locally with no request at all", async () => {
    const run = setup({ filter: { ref: "catalyst-cloud:main" } });
    expect(await launch(run)).toMatchObject({
      outcome: "refused",
      source: "filter",
      key: "ref",
      value: "catalyst-cloud:main",
    });
    expect(run.cloud.calls).toEqual([]);
    expect(run.sockets).toEqual([]);
  });
  it("S5c: an unknown event type answers refused with the cloud's types list", async () => {
    const run = setup({ filter: { type: "unknown.event" } });
    run.cloud.state.preflight = () =>
      json(400, {
        error: "unknown_event_type",
        types: ["unknown.event"],
        detail: "unknown type",
      });
    expect(await launch(run)).toMatchObject({
      outcome: "refused",
      types: ["unknown.event"],
      reason: "unknown type",
    });
    expect(run.sockets).toEqual([]);
  });
  it("S6: a cloud-default wait holds one subscription of its own and creates or reads no cache", async () => {
    const directory = await mkdtemp(join(tmpdir(), "direct-wait-"));
    vi.stubEnv("XDG_STATE_HOME", directory);
    try {
      const run = setup();
      const result = launch(run);
      const ws = await live(run);
      ws.deliver(frame(5000, 5001, [conflict(5001)]));
      expect(sequences(await result)).toEqual([5001]);
      expect(run.sockets).toHaveLength(1);
      expect(await readdir(directory)).toEqual([]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  it("S6b: the direct-wait modules import nothing from node:fs", async () => {
    for (const path of [
      "src/events-wait.ts",
      "src/events-backbone.ts",
      "src/event-filter.ts",
    ])
      expect(await readFile(path, "utf8")).not.toMatch(/from ["']node:fs/);
  });
  it("S8: a wait from cursor 7000 delivers 7002 and then 7005, each once", async () => {
    const run = setup({ after: 7000 }, 7005);
    run.cloud.state.events = [conflict(7002), conflict(7005)];
    const result = launch(run);
    await live(run);
    expect(sequences(await result)).toEqual([7002, 7005]);
    expect(await result).toMatchObject({ cursor: 7005 });
    const next = setup({ after: 7005 }, 7005);
    const later = launch(next);
    const ws = await live(next);
    ws.deliver(frame(7004, 7008, [conflict(7005), conflict(7008)]));
    expect(sequences(await later)).toEqual([7008]);
  });
  it("S8b: maxEvents 1 delivers 7002, then a wait from 7002 delivers 7005", async () => {
    const run = setup({ after: 7000, maxEvents: 1 }, 7005);
    run.cloud.state.events = [conflict(7002), conflict(7005)];
    const result = launch(run);
    await live(run);
    expect(await result).toMatchObject({ cursor: 7002 });
    expect(sequences(await result)).toEqual([7002]);
    const next = setup({ after: 7002, maxEvents: 1 }, 7005);
    next.cloud.state.events = run.cloud.state.events;
    const later = launch(next);
    await live(next);
    expect(sequences(await later)).toEqual([7005]);
  });
  it("S8c: watchEvents yields [7002, 7005] then, across a reconnect with overlapping replay, [7008] once", async () => {
    const run = setup({ after: 7000 }, 7005);
    run.cloud.state.events = [conflict(7002), conflict(7005)];
    const watch = watchEvents({
      ...run.options,
      signal: run.controller.signal,
    });
    const first = watch.next();
    const ws = await live(run);
    expect(
      sequences((await first).value ?? { outcome: "aborted", cursor: null }),
    ).toEqual([7002, 7005]);
    const second = watch.next();
    ws.serverClose(1006);
    run.cloud.state.events.push(conflict(7008));
    run.cloud.state.head = 7008;
    run.cloud.state.coverage = 7008;
    await vi.advanceTimersByTimeAsync(1000);
    await live(run, 1);
    expect(
      sequences((await second).value ?? { outcome: "aborted", cursor: null }),
    ).toEqual([7008]);
    run.controller.abort();
    expect((await watch.next()).value).toMatchObject({
      outcome: "aborted",
      cursor: 7008,
    });
    await watch.return();
    expect(run.sockets).toHaveLength(2);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("S9: a 60 s wait with no match times out distinctly and reports the cursor it reached", async () => {
    const run = setup({ timeoutMs: 60000 }, 9100);
    const result = launch(run);
    const ws = await live(run);
    ws.deliver(frame(9100, 9200));
    await vi.advanceTimersByTimeAsync(60000);
    expect(await result).toEqual({
      outcome: "timeout",
      cursor: 9200,
      waitedMs: 60000,
    });
    expect(new Set(Object.values(EVENT_WAIT_EXIT_CODES)).size).toBe(5);
    expect(EVENT_WAIT_EXIT_CODES.timeout).toBe(124);
    const resumed = setup({ after: 9200 }, 9200);
    launch(resumed);
    await live(resumed);
    expect(
      resumed.cloud.calls.some(
        (u) => u.searchParams.get("afterSeq") === "9200",
      ),
    ).toBe(true);
  });
  it("S9b: with the head probe refused and no frame, the timeout cursor falls back to the preflight coverage", async () => {
    const run = setup({ timeoutMs: 60000 });
    run.cloud.state.backboneStatus = 403;
    const result = launch(run);
    await live(run);
    await vi.advanceTimersByTimeAsync(60000);
    expect(await result).toEqual({
      outcome: "timeout",
      cursor: 5000,
      waitedMs: 60000,
    });
  });
  it("re-probes the head and catches up on every new socket generation", async () => {
    const run = setup();
    launch(run);
    const ws = await live(run);
    const before = run.cloud.calls.filter(
      (u) => u.searchParams.get("since") === String(Number.MAX_SAFE_INTEGER),
    ).length;
    ws.serverClose(1006);
    await vi.advanceTimersByTimeAsync(1000);
    await live(run, 1);
    expect(
      run.cloud.calls.filter(
        (u) => u.searchParams.get("since") === String(Number.MAX_SAFE_INTEGER),
      ).length,
    ).toBe(before + 1);
  });
  it.each(["omitted payload", "overflow", "unreadable frame"])(
    "treats %s as a hole to replay",
    async (mode) => {
      const run = setup({ socket: { maxBufferedFrames: 1 } });
      const result = launch(run);
      const ws = await live(run);
      run.cloud.state.events = [conflict(5003)];
      run.cloud.state.head = 5004;
      run.cloud.state.coverage = 5004;
      if (mode === "omitted payload")
        ws.deliver(
          frame(5000, 5004, [{ sequence: 5003, payloadOmitted: true }]),
        );
      else if (mode === "overflow") {
        ws.deliver(frame(5000, 5001));
        ws.deliver(frame(5001, 5004));
      } else
        ws.deliver({ type: "events", after: "bad", through: 5004, events: [] });
      await vi.advanceTimersByTimeAsync(0);
      expect(sequences(await result)).toEqual([5003]);
    },
  );
  it.each(["network", "503"])(
    "retries a preflight that hits %s with backoff, and times out if it never answers",
    async (mode) => {
      const run = setup({ timeoutMs: 60000 });
      run.cloud.state.preflight = () => {
        if (mode === "network") throw new Error("offline");
        return json(503, { error: "index_unavailable" });
      };
      const result = launch(run);
      await vi.advanceTimersByTimeAsync(60000);
      expect(await result).toMatchObject({ outcome: "timeout", cursor: null });
      expect(run.cloud.calls.length).toBeLessThan(10);
      expect(run.sockets).toEqual([]);
    },
  );
  it("answers a server without /events/query as failed unsupported", async () => {
    const run = setup();
    run.cloud.state.preflight = () => json(404, {});
    expect(await launch(run)).toMatchObject({
      outcome: "failed",
      code: "unsupported",
    });
  });
  it("answers a backbone 409 history gap as failed history-gap with resumeFrom", async () => {
    const run = setup({ after: 7000 }, 9000);
    run.cloud.state.from = 8000;
    run.options.fetch = async (input, init) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      if (
        url.pathname.endsWith("backbone") &&
        url.searchParams.get("since") !== String(Number.MAX_SAFE_INTEGER)
      )
        return json(
          409,
          { error: "cursor_underflow", resumeFrom: 8000 },
          { "x-catalyst-event-backbone-head-seq": "9000" },
        );
      return run.cloud.fetch(input, init);
    };
    const result = launch(run);
    await live(run);
    expect(await result).toMatchObject({
      outcome: "failed",
      code: "history-gap",
      cursor: 7000,
      resumeFrom: 8000,
    });
  });
  it("an abort resolves aborted with the cursor and stops the socket and every timer", async () => {
    const run = setup();
    const result = launch(run);
    const ws = await live(run);
    run.controller.abort();
    expect(await result).toEqual({ outcome: "aborted", cursor: 5000 });
    expect(ws.closed).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("re-checks the type of pushed events and drops one the server should not have sent", async () => {
    const run = setup({ timeoutMs: 100 });
    const result = launch(run);
    const ws = await live(run);
    ws.deliver(
      frame(5000, 5001, [{ ...conflict(5001), type: "phase.plan.started" }]),
    );
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toMatchObject({ outcome: "timeout", cursor: 5001 });
  });
  it("watchEvents retains a capped batch on one socket and abort closes it while the consumer is paused", async () => {
    const run = setup({ after: 7000, maxEvents: 1 }, 7005);
    run.cloud.state.events = [conflict(7002), conflict(7005)];
    const watch = watchEvents({
      ...run.options,
      signal: run.controller.signal,
    });
    const first = watch.next();
    const ws = await live(run);
    expect((await first).value).toMatchObject({
      outcome: "matched",
      cursor: 7002,
      events: [conflict(7002)],
    });
    expect((await watch.next()).value).toMatchObject({
      outcome: "matched",
      cursor: 7005,
      events: [conflict(7005)],
    });
    run.controller.abort();
    expect(ws.closed).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    expect((await watch.next()).value).toMatchObject({
      outcome: "aborted",
      cursor: 7005,
    });
    await watch.return();
  });
  it("replays every page before advancing a capped cursor", async () => {
    const run = setup({ after: 7000, maxEvents: 60 }, 7120);
    run.cloud.state.events = Array.from({ length: 120 }, (_, i) =>
      conflict(7001 + i),
    );
    const result = launch(run);
    await live(run);
    expect(sequences(await result)).toEqual(
      Array.from({ length: 60 }, (_, i) => 7001 + i),
    );
    expect(await result).toMatchObject({ cursor: 7060 });
    expect(
      run.cloud.calls
        .filter((url) => url.searchParams.get("order") === "asc")
        .map((url) => url.searchParams.get("afterSeq")),
    ).toEqual(["7000", "7050", "7100"]);
  });
  it("delivers unavailable-body stubs without skipping their cursor", async () => {
    const run = setup({ after: 5000 }, 5003);
    const stub = {
      tenantId: "tenant-1",
      sequence: 5003,
      eventId: "lost-body",
      type: "pr.conflict.detected",
      bodyUnavailable: true,
      reason: "archive_write_failed",
    };
    run.cloud.state.queryOverride = (url) =>
      url.searchParams.get("order") === "asc"
        ? json(200, {
            events: [stub],
            next: null,
            coverage: { indexedFromSeq: 0, indexedToSeq: 5003 },
          })
        : undefined;
    const result = launch(run);
    await live(run);
    expect(await result).toEqual({
      outcome: "matched",
      cursor: 5003,
      events: [stub],
    });
  });
  it("reads an oversized event from the backbone before advancing past it", async () => {
    const run = setup({ after: 5000 }, 5004);
    run.cloud.state.events = [conflict(5001), conflict(5003), conflict(5004)];
    run.cloud.state.queryOverride = (url) =>
      url.searchParams.get("afterSeq") === "5000"
        ? json(413, {
            error: "event_too_large",
            sequence: 5003,
            next: { param: "afterSeq", value: 5003 },
          })
        : undefined;
    const result = launch(run);
    await live(run);
    expect(sequences(await result)).toEqual([5001, 5003, 5004]);
    expect(await result).toMatchObject({ cursor: 5004 });
  });
  it("fails loudly on an oversized event when the backbone refuses the credential", async () => {
    const run = setup({ after: 5000 }, 5003);
    run.cloud.state.backboneStatus = 403;
    run.cloud.state.queryOverride = (url) =>
      url.searchParams.get("order") === "asc"
        ? json(413, {
            error: "event_too_large",
            sequence: 5003,
            next: { param: "afterSeq", value: 5003 },
          })
        : undefined;
    const result = launch(run);
    await live(run);
    expect(await result).toMatchObject({
      outcome: "failed",
      code: "shape",
      cursor: 5000,
      reason: expect.stringContaining("5003"),
    });
  });
  it("to=me waits for server coverage without using locally guessed recipients", async () => {
    const run = setup({ filter: { type: "pr.*", to: "me" } });
    const result = launch(run);
    const ws = await live(run);
    let finished = false;
    void result.then(() => {
      finished = true;
    });
    run.cloud.state.events = [conflict(5003)];
    run.cloud.state.head = 5004;
    run.cloud.state.coverage = 5001;
    ws.deliver(frame(5000, 5004, [], true));
    await vi.advanceTimersByTimeAsync(30000);
    expect(finished).toBe(false);
    expect(
      run.cloud.calls.filter(
        (url) =>
          url.pathname.endsWith("backbone") &&
          url.searchParams.get("since") !== String(Number.MAX_SAFE_INTEGER),
      ),
    ).toEqual([]);
    run.cloud.state.coverage = 5004;
    await vi.advanceTimersByTimeAsync(30000);
    expect(sequences(await result)).toEqual([5003]);
    expect(new URL(run.urls[0] ?? "").searchParams.get("to")).toBe("me");
  });
  it("replays the connect window from the preflight when a credential cannot probe the backbone head", async () => {
    const run = setup();
    run.cloud.state.backboneStatus = 403;
    const result = launch(run);
    const ws = await live(run);
    run.cloud.state.events = [conflict(5003), conflict(5006)];
    run.cloud.state.coverage = 5006;
    ws.deliver(frame(5005, 5006, [conflict(5006)]));
    expect(await result).toMatchObject({ outcome: "matched", cursor: 5006 });
    expect(sequences(await result)).toEqual([5003, 5006]);
  });
  it("uses the first frame's after when there is neither a head probe nor a preflight position", async () => {
    const run = setup({}, -1);
    run.cloud.state.backboneStatus = 403;
    const result = launch(run);
    const ws = await live(run);
    ws.deliver(frame(5005, 5006, [conflict(5006)]));
    expect(await result).toMatchObject({ outcome: "matched", cursor: 5006 });
    expect(sequences(await result)).toEqual([5006]);
  });
  it("a retained index gap fails with its resume cursor when the backbone is unavailable", async () => {
    const run = setup({ after: 5000 }, 6000);
    run.cloud.state.from = 5501;
    run.cloud.state.backboneStatus = 403;
    const result = launch(run);
    await live(run);
    expect(await result).toMatchObject({
      outcome: "failed",
      code: "history-gap",
      cursor: 5000,
      resumeFrom: 5500,
    });
  });
  it.each(["bad event", "other workspace"])(
    "fails shape for %s instead of advancing the cursor",
    async (mode) => {
      const run = setup();
      const result = launch(run);
      const ws = await live(run);
      ws.deliver(
        frame(5000, 5001, [
          mode === "bad event"
            ? { sequence: 5001 }
            : { ...conflict(5001), tenantId: "tenant-2" },
        ]),
      );
      expect(await result).toMatchObject({
        outcome: "failed",
        code: "shape",
        cursor: 5000,
      });
    },
  );
  it("aborts an in-flight HTTP request at the wait deadline", async () => {
    const run = setup({ timeoutMs: 100 });
    let signal: AbortSignal | null | undefined;
    run.options.fetch = async (_input, init) => {
      signal = init?.signal;
      return new Promise<Response>((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(new Error("aborted")), {
          once: true,
        });
      });
    };
    const result = launch(run);
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toMatchObject({ outcome: "timeout", cursor: null });
    expect(signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("safety queries keep catching up while a socket cannot connect", async () => {
    const run = setup({
      wsFactory: () => {
        throw new Error("socket offline");
      },
    });
    const result = launch(run);
    await vi.advanceTimersByTimeAsync(0);
    run.cloud.state.events = [conflict(5003)];
    run.cloud.state.coverage = 5003;
    run.cloud.state.head = 5003;
    await vi.advanceTimersByTimeAsync(30000);
    expect(sequences(await result)).toEqual([5003]);
  });
  it("pages the backbone through an oversized event without dropping earlier matches", async () => {
    const run = setup({ after: 5000 }, 5004);
    run.cloud.state.events = [
      conflict(5001),
      conflict(5002),
      conflict(5003),
      conflict(5004),
    ];
    run.cloud.state.queryOverride = (url) =>
      url.searchParams.get("afterSeq") === "5000"
        ? json(413, {
            error: "event_too_large",
            sequence: 5003,
            next: { param: "afterSeq", value: 5003 },
          })
        : undefined;
    const backboneCursors: number[] = [];
    run.options.fetch = async (input, init) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      const since = Number(url.searchParams.get("since"));
      if (
        url.pathname.endsWith("backbone") &&
        since !== Number.MAX_SAFE_INTEGER
      ) {
        backboneCursors.push(since);
        const rows = run.cloud.state.events
          .filter((e) => e.sequence > since)
          .slice(0, 2);
        return new Response(
          rows.map((e) => JSON.stringify(e) + "\n").join(""),
          { headers: { "x-catalyst-event-backbone-head-seq": "5004" } },
        );
      }
      return run.cloud.fetch(input, init);
    };
    const result = launch(run);
    await live(run);
    expect(await result).toMatchObject({ outcome: "matched", cursor: 5004 });
    expect(sequences(await result)).toEqual([5001, 5002, 5003, 5004]);
    expect(backboneCursors).toEqual([5000, 5002]);
  });
  it.each(["TimeoutError", "AbortError"])(
    "retries a backbone request %s within the overall wait deadline",
    async (name) => {
      const run = setup({ indexWaitMs: 0 });
      let attempts = 0;
      run.options.fetch = async (input, init) => {
        const url = new URL(
          input instanceof Request ? input.url : String(input),
        );
        if (
          url.pathname.endsWith("backbone") &&
          url.searchParams.get("since") !== String(Number.MAX_SAFE_INTEGER)
        ) {
          attempts++;
          if (attempts === 1) throw new DOMException("request timed out", name);
        }
        return run.cloud.fetch(input, init);
      };
      const result = launch(run);
      const ws = await live(run);
      run.cloud.state.events = [conflict(5003)];
      run.cloud.state.head = 5004;
      run.cloud.state.coverage = 5001;
      ws.deliver(frame(5000, 5004, [], true));
      await vi.advanceTimersByTimeAsync(1000);
      expect(sequences(await result)).toEqual([5003]);
      expect(attempts).toBe(2);
    },
  );
});
