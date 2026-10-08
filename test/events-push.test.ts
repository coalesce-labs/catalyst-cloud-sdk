// CTC-5295 — the event follower takes events from the tenant's events socket and polls only while it
// is down. A scriptable FakeWebSocket plays the cloud's side; the backbone is a mocked fetch whose
// calls are counted, because "no request while the socket is live" is the whole point.
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  CatalystEventSync,
  readCachedEvents,
  type CatalystEvent,
  type EventSyncOptions,
} from "../src/events.js";
import { EventsChannelClient, eventsSocketUrl } from "../src/events-socket.js";
import type { WebSocketFactory, WebSocketLike } from "../src/live-sync-client.js";
import { PING_FRAME, PONG_FRAME } from "../src/types.js";

class FakeWebSocket implements WebSocketLike {
  sent: string[] = [];
  closed = false;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  send(data: string): void {
    this.sent.push(data);
  }
  close(): void {
    this.closed = true;
  }
  /** The cloud accepted the upgrade and confirmed the subscription. */
  subscribe(): void {
    this.onopen?.({});
    this.deliver({ type: "channels", channels: ["events"], fields: "full" });
  }
  deliver(frame: unknown): void {
    this.onmessage?.({ data: typeof frame === "string" ? frame : JSON.stringify(frame) });
  }
  serverClose(code: number): void {
    this.onclose?.({ code, reason: "" });
  }
}

function recordingFactory(): { sockets: FakeWebSocket[]; urls: string[]; factory: WebSocketFactory } {
  const sockets: FakeWebSocket[] = [];
  const urls: string[] = [];
  return {
    sockets,
    urls,
    factory: (url) => {
      urls.push(url);
      const ws = new FakeWebSocket();
      sockets.push(ws);
      return ws;
    },
  };
}

const directories: string[] = [];
afterEach(async () => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  await Promise.all(directories.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

/** A cache directory whose cursor already stands at `cursor`, so no head bootstrap runs. */
async function cacheAt(cursor: number): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), "catalyst-events-push-"));
  directories.push(path);
  await writeFile(join(path, "cursor.json"), `{"version":1,"cursor":${cursor},"floor":${cursor}}\n`);
  return path;
}

function event(sequence: number): CatalystEvent {
  return {
    tenantId: "tenant-1",
    sequence,
    eventId: `event-${sequence}`,
    type: "phase.outcome.reported",
    schemaVersion: 1,
    recordedAt: "2026-10-08T16:00:00.000Z",
    payload: { ok: true },
  };
}

function frame(after: number, through: number, seqs: number[], extra: Record<string, unknown> = {}) {
  return { type: "events", after, through, events: seqs.map(event), ...extra };
}

/** A backbone that serves `events` after `since`, with the head header at the newest. */
function backbone(events: () => CatalystEvent[]) {
  const since: number[] = [];
  const fetch = vi.fn<typeof globalThis.fetch>(async (input) => {
    const after = Number(new URL(String(input)).searchParams.get("since"));
    since.push(after);
    const all = events();
    const head = all.length > 0 ? all[all.length - 1]!.sequence : after;
    const body = all
      .filter((e) => e.sequence > after)
      .map((e) => `${JSON.stringify(e)}\n`)
      .join("");
    return new Response(body, {
      status: 200,
      headers: { "x-catalyst-event-backbone-head-seq": String(Math.max(head, after)) },
    });
  });
  return { fetch, since };
}

function follower(
  directory: string,
  fetch: typeof globalThis.fetch,
  extra: Partial<EventSyncOptions> = {},
): CatalystEventSync {
  return new CatalystEventSync({
    baseUrl: "https://cloud.invalid",
    auth: { kind: "token", token: "secret" },
    tenantId: "tenant-1",
    directory,
    fetch,
    now: () => new Date("2026-10-08T16:00:00.000Z"),
    sleep: async () => {},
    ...extra,
  });
}

/** Open the follower's socket (as `start()` would) and confirm the subscription. */
async function goLive(sync: CatalystEventSync, sockets: FakeWebSocket[]): Promise<FakeWebSocket> {
  await sync.waitForChange(0, new AbortController().signal);
  const ws = sockets.at(-1)!;
  ws.subscribe();
  return ws;
}

async function cached(directory: string): Promise<number[]> {
  return (await readCachedEvents({ tenantId: "tenant-1", directory })).map((e) => e.sequence);
}

describe("CatalystEventSync over the events socket", () => {
  it("opens the events socket with the token, the account, and the full unfiltered channel", () => {
    const url = new URL(
      eventsSocketUrl({
        baseUrl: "https://staging.catalystcloud.dev",
        auth: { kind: "token", token: "secret" },
        tenantId: "tenant-1",
      }),
    );
    expect(url.protocol).toBe("wss:");
    expect(url.pathname).toBe("/api/v1/connect");
    expect(Object.fromEntries(url.searchParams)).toEqual({
      token: "secret",
      account: "tenant-1",
      channels: "events",
      fields: "full",
    });
  });

  it("a bare syncOnce opens no socket, so a one-shot caller keeps its exit behavior", async () => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const { fetch } = backbone(() => []);
    const sync = follower(directory, fetch, { wsFactory: factory });
    await sync.syncOnce();
    expect(sockets).toEqual([]);
    expect(sync.status().transport).toBe("poll");
  });

  it("while live, N pushed frames are applied with zero backbone requests", async () => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const { fetch } = backbone(() => []);
    const sync = follower(directory, fetch, { wsFactory: factory });
    const ws = await goLive(sync, sockets);
    await sync.syncOnce(); // the one catch-up this socket generation needs
    expect(fetch).toHaveBeenCalledTimes(1);

    ws.deliver(frame(0, 2, [1, 2]));
    ws.deliver(frame(2, 3, [3]));
    await expect(sync.syncOnce()).resolves.toEqual({ appended: 3, cursor: 3, head: 3 });
    ws.deliver(frame(3, 5, [4, 5]));
    await sync.syncOnce();
    await sync.syncOnce(); // nothing buffered: still no request
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(await cached(directory)).toEqual([1, 2, 3, 4, 5]);
    expect(sync.status()).toMatchObject({ transport: "push", cursor: 5 });
  });

  it("a frame starting past the cursor makes one backbone read, and each seq lands once", async () => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const durable = [1, 2, 3].map(event);
    const { fetch, since } = backbone(() => durable);
    const sync = follower(directory, fetch, { wsFactory: factory });
    const ws = await goLive(sync, sockets);
    await sync.syncOnce();
    expect(await cached(directory)).toEqual([1, 2, 3]);

    // 4 and 5 were appended while the frame for them was lost; the next frame starts at 5.
    durable.push(event(4), event(5), event(6));
    ws.deliver(frame(5, 6, [6]));
    await sync.syncOnce();
    expect(since).toEqual([0, 3]);
    expect(await cached(directory)).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it("subscribe first: frames buffered during the catch-up are deduped by sequence", async () => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const { fetch, since } = backbone(() => [1, 2, 3].map(event));
    const sync = follower(directory, fetch, { wsFactory: factory });
    const ws = await goLive(sync, sockets);
    // Pushed before the follower caught up: 2 and 3 overlap what the backbone read returns.
    ws.deliver(frame(1, 3, [2, 3]));
    ws.deliver(frame(3, 4, [4]));
    await sync.syncOnce();
    expect(since).toEqual([0]);
    expect(await cached(directory)).toEqual([1, 2, 3, 4]);
  });

  it("an overlapping frame applies only the sequences past the cursor, with no request", async () => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const { fetch } = backbone(() => [1, 2, 3].map(event));
    const sync = follower(directory, fetch, { wsFactory: factory });
    const ws = await goLive(sync, sockets);
    await sync.syncOnce();
    ws.deliver(frame(2, 4, [3, 4]));
    ws.deliver(frame(1, 4, [2, 3, 4])); // a stale duplicate is skipped whole
    await sync.syncOnce();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(await cached(directory)).toEqual([1, 2, 3, 4]);
  });

  it.each([
    ["a gap frame", frame(0, 2, [], { gap: true })],
    [
      "an event sent without its payload",
      {
        type: "events",
        after: 0,
        through: 2,
        events: [event(1), { ...event(2), payload: undefined, payloadOmitted: true }],
      },
    ],
  ])("%s falls back to a backbone read", async (_name, pushed) => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const durable: CatalystEvent[] = [];
    const { fetch, since } = backbone(() => durable);
    const sync = follower(directory, fetch, { wsFactory: factory });
    const ws = await goLive(sync, sockets);
    await sync.syncOnce();
    durable.push(event(1), event(2));
    ws.deliver(pushed);
    await sync.syncOnce();
    expect(since).toEqual([0, 0]);
    const events = await readCachedEvents({ tenantId: "tenant-1", directory });
    expect(events).toEqual([event(1), event(2)]);
  });

  it("a buffer past its cap is dropped and caught up through one backbone read", async () => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const { fetch, since } = backbone(() => [1, 2, 3].map(event));
    const sync = follower(directory, fetch, {
      wsFactory: factory,
      socket: { maxBufferedFrames: 2 },
    });
    const ws = await goLive(sync, sockets);
    await sync.syncOnce();
    ws.deliver(frame(3, 4, [4]));
    ws.deliver(frame(4, 5, [5]));
    ws.deliver(frame(5, 6, [6])); // the third frame overflows the cap of two
    await sync.syncOnce();
    expect(since).toEqual([0, 3]);
    expect(await cached(directory)).toEqual([1, 2, 3]);
  });

  it("while the socket is down it polls at least 5 s apart, and a reconnect catches up from the last seq", async () => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const durable: CatalystEvent[] = [];
    const { fetch, since } = backbone(() => durable);
    const sleeps: Array<{ ms: number; transport: string | undefined }> = [];
    let sync!: CatalystEventSync;
    // Each wait runs one step of the cloud's side. A step that returns true lets the wait time out;
    // otherwise the socket's own event has to end it.
    const steps: Array<() => Promise<boolean> | boolean> = [
      () => (sockets[0]!.subscribe(), false), // the socket goes live: catch-up from 0
      () => (sockets[0]!.deliver(frame(0, 1, [1])), durable.push(event(1)), false),
      () => (sockets[0]!.serverClose(1006), true), // dropped; 5 s pass
      () => (durable.push(event(2)), true), // a poll while down picks up 2
      async () => {
        await vi.waitFor(() => expect(sockets).toHaveLength(2));
        durable.push(event(3));
        sockets[1]!.subscribe();
        return false;
      },
      () => (void sync.stop(), false),
    ];
    sync = follower(directory, fetch, {
      wsFactory: factory,
      socket: { reconnectMinMs: 0 },
      sleep: (ms, signal) => {
        sleeps.push({ ms, transport: sync.status().transport });
        const step = steps[sleeps.length - 1];
        return new Promise((resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
          void Promise.resolve(step?.()).then((elapse) => elapse && resolve());
        });
      },
    });
    await sync.start();
    // Polls: the first sync before the socket exists, the catch-up when it goes live, two polls
    // while it is down, then the reconnect's catch-up from the last delivered seq.
    expect(since).toEqual([0, 0, 1, 1, 2]);
    const downWaits = sleeps.filter((s) => s.transport === "poll").map((s) => s.ms);
    expect(downWaits.every((ms) => ms >= 5_000)).toBe(true);
    expect(await cached(directory)).toEqual([1, 2, 3]);
  });

  it("a failing catch-up while live backs off instead of retrying at once", async () => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        new Response("", { status: 200, headers: { "x-catalyst-event-backbone-head-seq": "0" } }),
      )
      .mockImplementation(async () => new Response("", { status: 503 }));
    const sleeps: number[] = [];
    let sync!: CatalystEventSync;
    sync = follower(directory, fetch, {
      wsFactory: factory,
      sleep: (ms, signal) => {
        sleeps.push(ms);
        if (sleeps.length === 1) sockets[0]!.subscribe(); // live: the catch-up read now fails
        if (sleeps.length === 4) void sync.stop();
        return new Promise((resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
          if (sleeps.length > 1) resolve();
        });
      },
    });
    await sync.start();
    // One poll before the socket, then one failed catch-up per backed-off wait, never a burst.
    expect(fetch).toHaveBeenCalledTimes(4);
    expect(sleeps.slice(1)).toEqual([20_000, 30_000, 30_000]);
  });

  it("with no global WebSocket the follower runs poll-only", async () => {
    vi.stubGlobal("WebSocket", undefined);
    const directory = await cacheAt(0);
    const { fetch } = backbone(() => [event(1)]);
    const sync = follower(directory, fetch);
    await expect(sync.waitForChange(1, new AbortController().signal)).resolves.toBe(false);
    await sync.syncOnce();
    expect(sync.status()).toMatchObject({ transport: "poll", cursor: 1 });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("waitForChange ends early on a pushed frame and times out without one", async () => {
    const directory = await cacheAt(0);
    const { sockets, factory } = recordingFactory();
    const { fetch } = backbone(() => []);
    let release: (() => void) | null = null;
    const sync = follower(directory, fetch, {
      wsFactory: factory,
      sleep: () => new Promise<void>((resolve) => (release = resolve)),
    });
    const opening = sync.waitForChange(30_000, new AbortController().signal);
    sockets[0]!.subscribe(); // going live needs a catch-up, so the wait ends
    await expect(opening).resolves.toBe(true);
    await sync.syncOnce();
    const waiting = sync.waitForChange(30_000, new AbortController().signal);
    sockets[0]!.deliver(frame(0, 1, [1]));
    await expect(waiting).resolves.toBe(true);
    await sync.syncOnce();
    const idle = sync.waitForChange(30_000, new AbortController().signal);
    release!();
    await expect(idle).resolves.toBe(false);
  });
});

describe("EventsChannelClient — liveness and reconnect", () => {
  function client(extra: Partial<ConstructorParameters<typeof EventsChannelClient>[0]> = {}) {
    const { sockets, factory } = recordingFactory();
    const socket = new EventsChannelClient({
      baseUrl: "https://cloud.invalid",
      auth: { kind: "token", token: "secret" },
      tenantId: "tenant-1",
      wsFactory: factory,
      ...extra,
    });
    return { socket, sockets };
  }

  it("a 4401 close reconnects at once and is not fatal; another close backs off", async () => {
    vi.useFakeTimers();
    // A ctc_host_ socket is closed 4401 about every 5 minutes; no pings in between here.
    const { socket, sockets } = client({ pingAfterMs: 10 * 60_000 });
    socket.start();
    await vi.advanceTimersByTimeAsync(0);
    sockets[0]!.subscribe();
    expect(socket.state).toBe("live");
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    sockets[0]!.serverClose(4401);
    expect(socket.state).toBe("down");
    await vi.advanceTimersByTimeAsync(0);
    expect(sockets).toHaveLength(2);
    sockets[1]!.subscribe();
    expect(socket.generation).toBe(2);

    await vi.advanceTimersByTimeAsync(5_000);
    sockets[1]!.serverClose(1006);
    await vi.advanceTimersByTimeAsync(999);
    expect(sockets).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(sockets).toHaveLength(3);
    socket.stop();
  });

  it("pings after 90 s of silence, keeps a socket that answers, and reconnects one that does not", async () => {
    vi.useFakeTimers();
    const { socket, sockets } = client();
    socket.start();
    await vi.advanceTimersByTimeAsync(0);
    sockets[0]!.subscribe();
    await vi.advanceTimersByTimeAsync(89_999);
    expect(sockets[0]!.sent).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(sockets[0]!.sent).toEqual([PING_FRAME]);
    sockets[0]!.deliver(PONG_FRAME);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(sockets).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(70_000); // 90 s since the pong: the second ping
    expect(sockets[0]!.sent).toEqual([PING_FRAME, PING_FRAME]);
    await vi.advanceTimersByTimeAsync(15_000); // unanswered
    expect(sockets[0]!.closed).toBe(true);
    expect(socket.state).toBe("down");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sockets).toHaveLength(2);
    socket.stop();
  });

  it("a socket that never confirms its subscription is replaced", async () => {
    vi.useFakeTimers();
    const { socket, sockets } = client();
    socket.start();
    await vi.advanceTimersByTimeAsync(15_000);
    expect(sockets[0]!.closed).toBe(true);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sockets).toHaveLength(2);
    socket.stop();
  });

  it("a bearer token is resolved fresh for each connect", async () => {
    vi.useFakeTimers();
    const { sockets, factory } = recordingFactory();
    const urls: string[] = [];
    let n = 0;
    const socket = new EventsChannelClient({
      baseUrl: "https://cloud.invalid",
      auth: { kind: "bearer", getToken: async () => `token-${++n}` },
      tenantId: "tenant-1",
      wsFactory: (url) => (urls.push(url), factory(url)),
    });
    socket.start();
    await vi.advanceTimersByTimeAsync(0);
    sockets[0]!.subscribe();
    await vi.advanceTimersByTimeAsync(2_000);
    sockets[0]!.serverClose(4401);
    await vi.advanceTimersByTimeAsync(0);
    expect(urls.map((u) => new URL(u).searchParams.get("token"))).toEqual(["token-1", "token-2"]);
    socket.stop();
  });

  it("caps the buffer by frames and reports the overflow once", () => {
    const { socket, sockets } = client({ maxBufferedFrames: 2 });
    socket.start();
    return Promise.resolve().then(() => {
      sockets[0]!.subscribe();
      for (let i = 0; i < 3; i++) sockets[0]!.deliver(frame(i, i + 1, [i + 1]));
      expect(socket.take()).toMatchObject({ frames: [], overflowed: true });
      sockets[0]!.deliver(frame(3, 4, [4]));
      expect(socket.take()).toMatchObject({ frames: [{ after: 3, through: 4 }], overflowed: false });
      socket.stop();
    });
  });
});
