import {
  appendFile,
  mkdtemp,
  open,
  readFile,
  readdir,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { CatalystEventSync, type CatalystEvent } from "../src/events.js";
import {
  waitForCachedEvents,
  type CachedEventWaitOptions,
} from "../src/events-cache-wait.js";
import { recordingFactory } from "./helpers/fake-events-socket.js";

vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return { ...actual, rm: vi.fn(actual.rm), open: vi.fn(actual.open) };
});

const directories: string[] = [];
afterEach(async () => {
  await Promise.all(
    directories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});
const pause = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));
function event(sequence: number, type = "comms.message.posted"): CatalystEvent {
  return {
    tenantId: "tenant-1",
    sequence,
    eventId: `event-${sequence}`,
    type,
    schemaVersion: 1,
    recordedAt: "2026-10-22T00:00:00.000Z",
    payload: { ticket: "CTC-1234" },
  };
}
async function cache(cursor = 7000, floor = 7000): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "event-cache-wait-"));
  directories.push(directory);
  await checkpoint(directory, cursor, floor);
  await heartbeat(directory);
  return directory;
}
async function checkpoint(directory: string, cursor: number, floor = 7000) {
  await writeFile(
    join(directory, "cursor.next"),
    JSON.stringify({ version: 1, cursor, floor }),
  );
  await rename(join(directory, "cursor.next"), join(directory, "cursor.json"));
}
async function heartbeat(directory: string, timestamp = Date.now()) {
  await writeFile(
    join(directory, ".sync.writer.lock"),
    JSON.stringify({ pid: process.pid, owner: "test", heartbeat: timestamp }),
  );
}
function wait(directory: string, extra: Partial<CachedEventWaitOptions> = {}) {
  return waitForCachedEvents({
    tenantId: "tenant-1",
    directory,
    filter: { type: "comms.message.posted", ticket: "CTC-1234" },
    timeoutMs: 100,
    pollMs: 5,
    ...extra,
  });
}
it("S7: two cache waits share the writer's one upstream connection; only the message wait wakes", async () => {
  const directory = await cache();
  await rm(join(directory, ".sync.writer.lock"));
  const { sockets, urls, factory } = recordingFactory();
  const fetch = vi.fn<typeof globalThis.fetch>(
    async () =>
      new Response("", {
        headers: { "x-catalyst-event-backbone-head-seq": "7000" },
      }),
  );
  const writer = new CatalystEventSync({
    tenantId: "tenant-1",
    directory,
    baseUrl: "https://cloud.invalid",
    auth: { kind: "token", token: "seat" },
    fetch,
    wsFactory: factory,
    now: () => new Date("2026-10-22T00:00:00Z"),
  });
  const abort = new AbortController();
  try {
    // Open the real writer's socket and establish its lock/checkpoint before either reader starts.
    await writer.waitForChange(0, new AbortController().signal);
    const socket = sockets[0];
    expect(socket).toBeDefined();
    if (!socket) throw new Error("writer did not connect");
    socket.subscribe();
    await writer.syncOnce();
    const running = writer.start();
    let conflictSettled = false;
    const conflict = wait(directory, {
      filter: { type: "pr.conflict.detected", ticket: "CTC-1234" },
      timeoutMs: 2000,
      signal: abort.signal,
    }).then((result) => {
      conflictSettled = true;
      return result;
    });
    const message = wait(directory, { timeoutMs: 2000 });
    await pause(30);
    const row = event(7001);
    socket.deliver({
      type: "events",
      after: 7000,
      through: 7001,
      events: [row],
    });
    expect(await message).toMatchObject({
      outcome: "matched",
      events: [row],
      cursor: 7001,
    });
    await pause(500);
    expect(conflictSettled).toBe(false);
    expect(urls).toHaveLength(1);
    expect(fetch).toHaveBeenCalledTimes(1);
    abort.abort();
    expect(await conflict).toEqual({ outcome: "aborted", cursor: 7001 });
    await writer.stop();
    await running;
  } finally {
    abort.abort();
    await writer.stop();
  }
});
it("fails cache-missing and creates nothing when the directory does not exist", async () => {
  const parent = await cache();
  const before = await readdir(parent);
  expect(await wait(join(parent, "absent"))).toMatchObject({
    outcome: "failed",
    code: "cache-missing",
    cursor: null,
  });
  expect(await readdir(parent)).toEqual(before);
});
it("fails cache-writer-stopped when the writer lock is missing or its heartbeat is older than 15 s", async () => {
  const directory = await cache();
  await rm(join(directory, ".sync.writer.lock"));
  expect(await wait(directory)).toMatchObject({
    outcome: "failed",
    code: "cache-writer-stopped",
    cursor: 7000,
  });
  await heartbeat(directory, Date.now() - 16_000);
  expect(await wait(directory)).toMatchObject({
    outcome: "failed",
    code: "cache-writer-stopped",
    cursor: 7000,
  });
});
it("fails cache-writer-stopped, with its cursor, when the writer stops during the wait for more than 60 s", async () => {
  const directory = await cache();
  const result = wait(directory, {
    writerStaleMs: 20,
    writerCheckMs: 5,
    timeoutMs: 1000,
  });
  await pause(30);
  await rm(join(directory, ".sync.writer.lock"));
  expect(await result).toMatchObject({
    outcome: "failed",
    code: "cache-writer-stopped",
    cursor: 7000,
  });
});
it("refuses to=me locally", async () => {
  expect(await wait("/does-not-exist", { filter: { to: "me" } })).toMatchObject(
    { outcome: "refused", source: "filter", key: "to", value: "me" },
  );
});
it("answers an after below the retention floor as failed history-gap with resumeFrom = floor", async () => {
  const directory = await cache();
  expect(await wait(directory, { after: 6999 })).toMatchObject({
    outcome: "failed",
    code: "history-gap",
    cursor: 6999,
    resumeFrom: 7000,
  });
});
it("from cursor 7000 delivers 7002 and 7005 each once, and a wait from 7005 does not repeat 7005", async () => {
  const directory = await cache(7005);
  await writeFile(
    join(directory, "2026-10-22.jsonl"),
    [event(7002), event(7005)]
      .map((row) => JSON.stringify(row) + "\n")
      .join(""),
  );
  expect(await wait(directory, { after: 7000 })).toMatchObject({
    outcome: "matched",
    cursor: 7005,
    events: [event(7002), event(7005)],
  });
  expect(await wait(directory, { after: 7005 })).toMatchObject({
    outcome: "timeout",
    cursor: 7005,
  });
  expect(await wait(directory, { after: 7000, maxEvents: 1 })).toMatchObject({
    outcome: "matched",
    cursor: 7002,
    events: [event(7002)],
  });
  expect(await wait(directory, { after: 7002, maxEvents: 1 })).toMatchObject({
    outcome: "matched",
    cursor: 7005,
    events: [event(7005)],
  });
});
it("times out with the cache writer's checkpoint cursor so the next wait resumes there", async () => {
  const directory = await cache();
  const pending = wait(directory);
  await pause(20);
  await checkpoint(directory, 7010);
  expect(await pending).toMatchObject({ outcome: "timeout", cursor: 7010 });
});
it("delivers an event only once its line is complete", async () => {
  const directory = await cache();
  const segment = join(directory, "2026-10-22.jsonl");
  const line = JSON.stringify(event(7001)) + "\n";
  await writeFile(segment, line.slice(0, -4));
  let settled = false;
  const pending = wait(directory, { timeoutMs: 500 }).then((result) => {
    settled = true;
    return result;
  });
  await pause(30);
  expect(settled).toBe(false);
  await appendFile(segment, line.slice(-4));
  expect(await pending).toMatchObject({
    outcome: "matched",
    events: [event(7001)],
    cursor: 7001,
  });
});
it("follows a segment rotation and a retention removal without losing or repeating events", async () => {
  const directory = await cache();
  const old = join(directory, "2026-10-21.jsonl");
  await writeFile(old, JSON.stringify(event(7000)) + "\n");
  const pending = wait(directory, { timeoutMs: 500 });
  await pause(25);
  await writeFile(
    join(directory, "2026-10-22.jsonl"),
    JSON.stringify(event(7001)) + "\n",
  );
  await rm(old);
  expect(await pending).toMatchObject({
    outcome: "matched",
    events: [event(7001)],
  });
});
it("a read-only wait leaves all cache bytes unchanged", async () => {
  const directory = await cache();
  const names = await readdir(directory);
  const before = await Promise.all(
    names.map((name) => readFile(join(directory, name), "utf8")),
  );
  await wait(directory, { timeoutMs: 15 });
  expect(await readdir(directory)).toEqual(names);
  expect(
    await Promise.all(
      names.map((name) => readFile(join(directory, name), "utf8")),
    ),
  ).toEqual(before);
});

it.each([1, 999])(
  "keeps cache waits resumable after retention and rotation from segment %i",
  async (suffix) => {
    const directory = await cache(7002, 7001);
    await rm(join(directory, ".sync.writer.lock"));
    const oldSegment = `2026-10-22-${String(suffix).padStart(3, "0")}.jsonl`;
    const largeEvent = (sequence: number) => ({
      ...event(sequence),
      payload: { ticket: "CTC-1234", body: "x".repeat(600_000) },
    });
    await writeFile(
      join(directory, oldSegment),
      JSON.stringify(largeEvent(7002)) + "\n",
    );
    let rows: ReturnType<typeof largeEvent>[] = [];
    let head = 7002;
    const writer = new CatalystEventSync({
      tenantId: "tenant-1",
      directory,
      baseUrl: "https://cloud.invalid",
      auth: { kind: "token", token: "seat" },
      push: false,
      maxCacheBytes: 1024 * 1024,
      now: () => new Date("2026-10-22T00:00:00Z"),
      fetch: async () =>
        new Response(rows.map((row) => JSON.stringify(row) + "\n").join(""), {
          headers: { "x-catalyst-event-backbone-head-seq": String(head) },
        }),
    });
    try {
      await writer.syncOnce();
      // The preceding size-retention pass has removed the unnumbered segment.
      await rm(join(directory, "2026-10-22.jsonl"), { force: true });
      for (const sequence of [7003, 7004]) {
        const current = `2026-10-22-${String(suffix + sequence - 7003).padStart(3, "0")}.jsonl`;
        const next = `2026-10-22-${String(suffix + sequence - 7002).padStart(3, "0")}.jsonl`;
        vi.mocked(open).mockClear();
        const result = wait(directory, { after: sequence - 1, timeoutMs: 2000 });
        // Wait until positioning and the first segment scan have opened the surviving file.
        await vi.waitFor(() =>
          expect(
            vi.mocked(open).mock.calls.filter(
              ([path, flags]) => path === join(directory, current) && flags === "r",
            ).length,
          ).toBeGreaterThanOrEqual(2),
        );
        rows = [largeEvent(sequence)];
        head = sequence;
        await writer.syncOnce();
        const matched = await result;
        expect(matched).toMatchObject({ outcome: "matched", cursor: sequence });
        expect(
          matched.outcome === "matched"
            ? matched.events.map((row) => row.sequence)
            : [],
        ).toEqual([sequence]);
        expect(
          (await readdir(directory)).filter((name) => name.endsWith(".jsonl")),
        ).toEqual([next]);
      }
    } finally {
      await writer.stop();
    }
  },
);

it("publishes the retention floor before unlink so a concurrent wait cannot skip a removed match", async () => {
  const directory = await cache(7002);
  await rm(join(directory, ".sync.writer.lock"));
  await writeFile(
    join(directory, "2026-10-01.jsonl"),
    JSON.stringify(event(7001)) + "\n",
  );
  await writeFile(
    join(directory, "2026-10-22.jsonl"),
    JSON.stringify(event(7002, "phase.plan.started")) + "\n",
  );
  const actual =
    await vi.importActual<typeof import("node:fs/promises")>(
      "node:fs/promises",
    );
  let floorAtRemoval: unknown;
  let duringRemoval: Awaited<ReturnType<typeof wait>> | undefined;
  vi.mocked(rm).mockImplementationOnce(async (path, options) => {
    const saved: unknown = JSON.parse(
      await readFile(join(directory, "cursor.json"), "utf8"),
    );
    floorAtRemoval =
      typeof saved === "object" && saved !== null && "floor" in saved
        ? saved.floor
        : undefined;
    await actual.rm(path, options);
    // Hold the writer between unlink and its final checkpoint to expose the reader's race.
    duringRemoval = await wait(directory, { after: 7000, timeoutMs: 30 });
  });
  const writer = new CatalystEventSync({
    tenantId: "tenant-1",
    directory,
    baseUrl: "https://cloud.invalid",
    auth: { kind: "token", token: "seat" },
    push: false,
    now: () => new Date("2026-10-22T00:00:00Z"),
    fetch: async () =>
      new Response("", {
        headers: { "x-catalyst-event-backbone-head-seq": "7002" },
      }),
  });
  try {
    await writer.syncOnce();
    expect(floorAtRemoval).toBe(7001);
    expect(duringRemoval).toMatchObject({
      outcome: "failed",
      code: "history-gap",
      cursor: 7000,
      resumeFrom: 7001,
    });
  } finally {
    await writer.stop();
  }
});
