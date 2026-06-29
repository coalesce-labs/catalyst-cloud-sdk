import { afterEach, describe, expect, it } from "vitest";
import * as fs from "node:fs";
import {
  applyMigrations,
  MIRROR_MIGRATIONS,
  type MigrationDb,
} from "@catalyst-cloud/schema";
import { applyDelta, setCursor, type ReplicaWriteDb } from "@catalyst-cloud/replicate";
import {
  CatalystReplica,
  nodeSqliteEngine,
  nodeSqliteReadonlyEngine,
  type ReplicaEngine,
} from "../../src/node";

// READ-ONLY READER MODE (CTC-113). A writer-seeded SQLite file is opened READ-ONLY via
// CatalystReplica.openReadOnly — NO migrations, NO snapshot, NO socket. The reader serves the same
// read-model views off the file the writer persisted, its start() is inert, and the write path is
// unreachable (the sqlite handle itself is read-only). Everything runs against a REAL node:sqlite
// engine on a temp file; no network.

const SYNC_META_DDL = "CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value TEXT)";

const dirs: string[] = [];
function tmpDbPath(): string {
  const dir = fs.mkdtempSync("catalyst-replica-reader-");
  dirs.push(dir);
  return `${dir}/replica.db`;
}
afterEach(() => {
  while (dirs.length) {
    try {
      fs.rmSync(dirs.pop()!, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  }
});

/** Seed a replica FILE exactly as the writer would leave it (migrate + applyDelta + cursor), then
 *  checkpoint + switch off WAL so a read-only handle never needs WAL shared memory. */
async function seedWriterFile(
  dbPath: string,
  rows: { entity: string; row: Record<string, unknown> }[],
  cursor: number,
): Promise<void> {
  const engine: ReplicaEngine = await nodeSqliteEngine(dbPath);
  const migrationDb: MigrationDb = { exec: (s) => engine.exec(s), query: (s) => engine.all(s) };
  applyMigrations(migrationDb, MIRROR_MIGRATIONS);
  engine.exec(SYNC_META_DDL);
  const writeDb: ReplicaWriteDb<unknown> = {
    run: (s, ...b) => engine.run(s, ...b),
    get: (s, ...b) => engine.get(s, ...b),
  };
  for (const r of rows) {
    applyDelta(writeDb, { entity: r.entity, op: "upsert", row: r.row }, engine.toBindable);
  }
  setCursor(writeDb, cursor, engine.toBindable);
  engine.exec("PRAGMA wal_checkpoint(TRUNCATE)");
  engine.exec("PRAGMA journal_mode = DELETE");
  engine.close();
}

describe("CatalystReplica.openReadOnly — the read-only reader", () => {
  it("reads the SAME rows a writer seeded into the file (issues/issue/cursor/handle)", async () => {
    const dbPath = tmpDbPath();
    await seedWriterFile(
      dbPath,
      [
        { entity: "issues", row: { id: "i1", identifier: "CTC-1", title: "older", updated_at: 100 } },
        { entity: "issues", row: { id: "i2", identifier: "CTC-2", title: "newer", updated_at: 200 } },
        { entity: "comments", row: { id: "c1", issue_id: "i1", body: "hello", updated_at: 100 } },
        { entity: "labels", row: { id: "l1", name: "bug", updated_at: 1 } },
        { entity: "issue_labels", row: { issue_id: "i1", label_id: "l1" } },
      ],
      42,
    );

    const reader = await CatalystReplica.openReadOnly({ dbPath, engine: nodeSqliteReadonlyEngine });
    try {
      const view = reader.issues();
      expect(view.map((v) => v.id)).toEqual(["i2", "i1"]); // newest-first, same as the writer view
      expect(view.find((v) => v.id === "i1")!.labels).toEqual([{ id: "l1", name: "bug", color: null }]);
      expect(reader.issue("CTC-1")?.comments.map((c) => c.body)).toEqual(["hello"]);
      expect(reader.cursor).toBe(42); // reads the writer's durable cursor row
      expect(reader.handle).toBeDefined(); // raw read-only driver Database, for drizzle reads
    } finally {
      await reader.close();
    }
  });

  it("auto-detects a READ-ONLY engine when none is injected (node:sqlite in the test env)", async () => {
    const dbPath = tmpDbPath();
    await seedWriterFile(
      dbPath,
      [{ entity: "issues", row: { id: "i1", identifier: "CTC-9", title: "Auto", updated_at: 1 } }],
      3,
    );
    const reader = await CatalystReplica.openReadOnly({ dbPath }); // engine omitted → readonly auto-detect
    try {
      expect(reader.issues()[0]!.title).toBe("Auto");
      expect(reader.cursor).toBe(3);
    } finally {
      await reader.close();
    }
  });

  it("start() on a reader is an inert no-op (already open, no socket, data unchanged)", async () => {
    const dbPath = tmpDbPath();
    await seedWriterFile(
      dbPath,
      [{ entity: "issues", row: { id: "i1", identifier: "CTC-1", title: "X", updated_at: 1 } }],
      7,
    );
    const reader = await CatalystReplica.openReadOnly({ dbPath, engine: nodeSqliteReadonlyEngine });
    try {
      const before = reader.issues();
      await expect(reader.start()).resolves.toBeUndefined(); // no-op, does not throw
      await reader.start(); // still inert on a second call
      expect(reader.issues()).toEqual(before);
      expect(reader.cursor).toBe(7);
    } finally {
      await reader.close();
    }
  });

  it("cannot write — the underlying sqlite handle rejects mutations", async () => {
    const dbPath = tmpDbPath();
    await seedWriterFile(
      dbPath,
      [{ entity: "issues", row: { id: "i1", identifier: "CTC-1", title: "ro", updated_at: 1 } }],
      1,
    );
    const reader = await CatalystReplica.openReadOnly({ dbPath, engine: nodeSqliteReadonlyEngine });
    try {
      const handle = reader.handle as { prepare(sql: string): { run(...a: unknown[]): unknown } };
      // A direct write through the raw handle must throw: the connection is opened read-only.
      expect(() =>
        handle.prepare("INSERT INTO issues (id, updated_at) VALUES (?, ?)").run("i2", 2),
      ).toThrow(/readonly/i);
      // The write never landed; reads still work afterward.
      expect(reader.issues().map((v) => v.id)).toEqual(["i1"]);
    } finally {
      await reader.close();
    }
  });

  it("rejects a ':memory:' path (nothing for a reader to read)", async () => {
    await expect(CatalystReplica.openReadOnly({ dbPath: ":memory:" })).rejects.toThrow(/FILE path/);
  });
});
