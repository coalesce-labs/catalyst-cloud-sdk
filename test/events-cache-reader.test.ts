import { appendFile, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { CacheSegmentReader } from "../src/events-cache-reader.js";

const directories: string[] = [];
afterEach(async () => {
  await Promise.all(
    directories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});
function line(sequence: number, body = "") {
  return (
    JSON.stringify({
      tenantId: "tenant-1",
      sequence,
      eventId: `event-${sequence}`,
      type: "comms.message.posted",
      schemaVersion: 1,
      recordedAt: "2026-10-22T00:00:00Z",
      payload: { body },
    }) + "\n"
  );
}
it("after positioning at the end, reads only appended bytes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "event-reader-"));
  directories.push(directory);
  const segment = join(directory, "2026-10-22.jsonl");
  await writeFile(
    segment,
    Array.from({ length: 100 }, (_, i) => line(i + 1, "x".repeat(52_000))).join(
      "",
    ),
  );
  const before = (await stat(segment)).size;
  expect(before).toBeGreaterThan(5_000_000);
  const reader = await CacheSegmentReader.positionAt(directory, 100);
  expect(reader.offset).toBe(before);
  expect(await reader.read()).toEqual([]);
  const next = line(101);
  await appendFile(segment, next);
  expect((await reader.read()).map((event) => event.sequence)).toEqual([101]);
  expect(reader.offset).toBe(before + Buffer.byteLength(next));
  expect(await reader.read()).toEqual([]);
});
it("positions by the first event of each segment and retains events after the requested cursor", async () => {
  const directory = await mkdtemp(join(tmpdir(), "event-reader-"));
  directories.push(directory);
  await writeFile(join(directory, "2026-10-20.jsonl"), line(1));
  await writeFile(join(directory, "2026-10-21.jsonl"), line(10) + line(12));
  await writeFile(join(directory, "2026-10-22.jsonl"), line(20));
  const reader = await CacheSegmentReader.positionAt(directory, 10);
  expect((await reader.read()).map((event) => event.sequence)).toEqual([
    12, 20,
  ]);
});
it("buffers an incomplete UTF-8 line and follows rotation and removal", async () => {
  const directory = await mkdtemp(join(tmpdir(), "event-reader-"));
  directories.push(directory);
  const first = join(directory, "2026-10-21.jsonl");
  await writeFile(first, line(1));
  const reader = await CacheSegmentReader.positionAt(directory, 1);
  const bytes = Buffer.from(line(2, "é"));
  const split = bytes.indexOf(Buffer.from("é")) + 1;
  await appendFile(first, bytes.subarray(0, split));
  expect(await reader.read()).toEqual([]);
  await appendFile(first, bytes.subarray(split));
  expect((await reader.read()).map((event) => event.sequence)).toEqual([2]);
  await rm(first);
  await writeFile(join(directory, "2026-10-22.jsonl"), line(3));
  expect((await reader.read()).map((event) => event.sequence)).toEqual([3]);
  expect(await reader.read()).toEqual([]);
});
it("orders same-day numbered segments after the unnumbered first segment", async () => {
  const directory = await mkdtemp(join(tmpdir(), "event-reader-"));
  directories.push(directory);
  await writeFile(join(directory, "2026-10-22.jsonl"), line(1));
  const reader = await CacheSegmentReader.positionAt(directory, 1);
  await writeFile(join(directory, "2026-10-22-001.jsonl"), line(2));
  expect((await reader.read()).map((event) => event.sequence)).toEqual([2]);
  await writeFile(join(directory, "2026-10-22-002.jsonl"), line(3));
  const resumed = await CacheSegmentReader.positionAt(directory, 1);
  expect((await resumed.read()).map((event) => event.sequence)).toEqual([2, 3]);
});
