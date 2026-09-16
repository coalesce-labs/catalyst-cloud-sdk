import {
  access,
  appendFile,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  CatalystEventSync,
  eventCachePaths,
  readCachedEvents,
  tailCachedEvents,
  type CatalystEvent,
} from "../src/events.js";

const directories: string[] = [];
afterEach(async () => {
  await Promise.all(
    directories
      .splice(0)
      .map((path) => rm(path, { recursive: true, force: true })),
  );
});

async function directory(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), "catalyst-events-"));
  directories.push(path);
  return path;
}

function event(sequence: number): CatalystEvent {
  return {
    tenantId: "tenant-1",
    sequence,
    eventId: `event-${sequence}`,
    type: "phase.outcome.reported",
    schemaVersion: 1,
    recordedAt: "2026-09-16T20:00:00.000Z",
    payload: { ok: true },
  };
}

function response(body: unknown, status = 200, head = 0): Response {
  return new Response(typeof body === "string" ? body : JSON.stringify(body), {
    status,
    headers: { "x-catalyst-event-backbone-head-seq": String(head) },
  });
}

describe("CatalystEventSync", () => {
  it("arms the real filesystem watcher before its startup scan", async () => {
    const root = await directory();
    const first = `${JSON.stringify(event(1))}\n`;
    await writeFile(join(root, "2026-09-16.jsonl"), first.repeat(10_000));
    await writeFile(
      join(root, "cursor.json"),
      '{"version":1,"cursor":1,"floor":0}\n',
    );
    const controller = new AbortController();
    const tail = tailCachedEvents({
      tenantId: "tenant-1",
      directory: root,
      after: 1,
      signal: controller.signal,
    });
    const next = tail.next();
    await appendFile(
      join(root, "2026-09-16.jsonl"),
      `${JSON.stringify(event(2))}\n`,
    );
    await expect(next).resolves.toEqual({ value: event(2), done: false });
    controller.abort();
    await tail.return(undefined);
  });

  it("lets concurrent readers ignore an in-progress final line without creating cache state", async () => {
    const root = await directory();
    await writeFile(
      join(root, "2026-09-16.jsonl"),
      `${JSON.stringify(event(1))}\n${JSON.stringify(event(2)).slice(0, 20)}`,
    );
    await expect(
      readCachedEvents({ tenantId: "tenant-1", directory: root }),
    ).resolves.toEqual([event(1)]);

    const missing = join(root, "missing");
    const tail = tailCachedEvents({
      tenantId: "tenant-1",
      directory: missing,
      signal: new AbortController().signal,
    });
    await expect(tail.next()).rejects.toMatchObject({ code: "ENOENT" });
    await expect(access(missing)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("bootstraps at the current head, appends before checkpointing, and recovers an append/checkpoint crash", async () => {
    const root = await directory();
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        response({ error: "cursor_ahead_of_head", resumeFrom: 4 }, 409, 4),
      )
      .mockResolvedValueOnce(response(`${JSON.stringify(event(5))}\n`, 200, 5))
      .mockResolvedValueOnce(response("", 200, 5));
    const sync = new CatalystEventSync({
      baseUrl: "https://cloud.invalid",
      auth: { kind: "token", token: "secret" },
      tenantId: "tenant-1",
      directory: root,
      fetch,
      now: () => new Date("2026-09-16T20:00:00.000Z"),
    });

    await expect(sync.syncOnce()).resolves.toEqual({
      appended: 1,
      cursor: 5,
      head: 5,
    });
    expect(
      await readCachedEvents({ tenantId: "tenant-1", directory: root }),
    ).toEqual([event(5)]);
    expect(
      JSON.parse(
        await readFile(eventCachePaths("tenant-1", root).cursor, "utf8"),
      ),
    ).toEqual({
      version: 1,
      cursor: 5,
      floor: 4,
    });

    // Model a crash after the fsynced append but before its cursor rename. The next owner derives
    // the durable cursor from the complete tail and asks after 5, so replay cannot duplicate it.
    await writeFile(
      eventCachePaths("tenant-1", root).cursor,
      '{"version":1,"cursor":4}\n',
    );
    await expect(sync.syncOnce()).resolves.toEqual({
      appended: 0,
      cursor: 5,
      head: 5,
    });
    expect(
      new URL(String(fetch.mock.calls[2]![0])).searchParams.get("since"),
    ).toBe("5");
    expect(
      await readCachedEvents({ tenantId: "tenant-1", directory: root }),
    ).toEqual([event(5)]);
  });

  it("keeps an explicit durable-history gap and a second live writer loud", async () => {
    const root = await directory();
    const first = new CatalystEventSync({
      baseUrl: "https://cloud.invalid",
      auth: { kind: "token", token: "secret" },
      tenantId: "tenant-1",
      directory: root,
      fetch: vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValueOnce(
          response({ error: "cursor_ahead_of_head", resumeFrom: 9 }, 409, 9),
        )
        .mockResolvedValueOnce(response("", 200, 9)),
    });
    await first.syncOnce();

    const second = new CatalystEventSync({
      baseUrl: "https://cloud.invalid",
      auth: { kind: "token", token: "secret" },
      tenantId: "tenant-1",
      directory: root,
      fetch: vi.fn<typeof globalThis.fetch>(),
    });
    await expect(second.syncOnce()).rejects.toThrow(
      "another writer owns this replica",
    );

    const gap = new CatalystEventSync({
      baseUrl: "https://cloud.invalid",
      auth: { kind: "token", token: "secret" },
      tenantId: "tenant-2",
      directory: await directory(),
      fetch: vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValueOnce(
          response({ error: "cursor_ahead_of_head", resumeFrom: 4 }, 409, 4),
        )
        .mockResolvedValueOnce(
          response(
            {
              error: "cursor_underflow",
              resumeFrom: 8,
              reason: "range-not-archived",
            },
            409,
            10,
          ),
        ),
    });
    await expect(gap.syncOnce()).rejects.toMatchObject({
      name: "EventHistoryGapError",
      head: 10,
      resumeFrom: 8,
      reason: "range-not-archived",
    });
  });

  it("retires closed daily segments at the seven-day floor and reports the local gap", async () => {
    const root = await directory();
    await writeFile(
      join(root, "2026-09-01.jsonl"),
      `${JSON.stringify(event(2))}\n`,
    );
    await writeFile(
      join(root, "cursor.json"),
      '{"version":1,"cursor":2,"floor":1}\n',
    );
    const sync = new CatalystEventSync({
      baseUrl: "https://cloud.invalid",
      auth: { kind: "token", token: "secret" },
      tenantId: "tenant-1",
      directory: root,
      fetch: vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValue(response("", 200, 2)),
      now: () => new Date("2026-09-16T20:00:00.000Z"),
    });
    await sync.syncOnce();
    await expect(
      readCachedEvents({ tenantId: "tenant-1", directory: root, after: 1 }),
    ).rejects.toMatchObject({
      name: "EventHistoryGapError",
      head: 2,
      resumeFrom: 2,
      reason: "local-retention",
    });
  });

  it("rotates the active daily segment before it can exceed the byte bound", async () => {
    const root = await directory();
    const first = `${JSON.stringify(event(1))}\n`;
    await writeFile(join(root, "2026-09-16.jsonl"), first.repeat(7_000));
    await writeFile(
      join(root, "cursor.json"),
      '{"version":1,"cursor":1,"floor":0}\n',
    );
    const sync = new CatalystEventSync({
      baseUrl: "https://cloud.invalid",
      auth: { kind: "token", token: "secret" },
      tenantId: "tenant-1",
      directory: root,
      maxCacheBytes: 1024 * 1024,
      fetch: vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValue(response(`${JSON.stringify(event(2))}\n`, 200, 2)),
      now: () => new Date("2026-09-16T20:00:00.000Z"),
    });
    await sync.syncOnce();
    const segments = (await readdir(root)).filter((name) =>
      name.endsWith(".jsonl"),
    );
    expect(segments).toEqual(["2026-09-16-001.jsonl"]);
    expect((await stat(join(root, segments[0]!))).size).toBeLessThanOrEqual(
      1024 * 1024,
    );
  });
});
