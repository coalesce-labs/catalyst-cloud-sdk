import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  applyMigrations,
  MIRROR_MIGRATIONS,
  type MigrationDb,
} from "@catalyst-cloud/schema";
import {
  applyDelta,
  truncateReplica,
  getCursor,
  setCursor,
  type ReplicaWriteDb,
  type ReplicaChange,
} from "@catalyst-cloud/replicate";
import { nodeSqliteEngine, type ReplicaEngine } from "../../src/node";

// Ports apps/host-sync/test/apply.test.ts onto the SDK's ReplicaEngine. The REAL @catalyst-cloud/
// replicate applyDelta write path (the SAME code host-sync's apply.ts now delegates to) runs over the
// SDK's node:sqlite ReplicaEngine, so these assertions exercise genuine SQL semantics: ON CONFLICT
// upserts, the updated_at last-write-wins guard, removed_at soft-delete idempotency, hard-delete, and
// composite-PK delete via entityId.

const SYNC_META_DDL = "CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value TEXT)";

/** Open a fully-migrated in-memory replica engine (mirrors CatalystReplica.start's open+migrate). */
async function openReplicaEngine(): Promise<ReplicaEngine> {
  const engine = await nodeSqliteEngine(":memory:");
  const migrationDb: MigrationDb = {
    exec: (sql) => engine.exec(sql),
    query: (sql) => engine.all(sql),
  };
  applyMigrations(migrationDb, MIRROR_MIGRATIONS);
  engine.exec(SYNC_META_DDL);
  return engine;
}

function writeDbOf(engine: ReplicaEngine): ReplicaWriteDb<unknown> {
  return {
    run: (sql, ...b) => engine.run(sql, ...b),
    get: (sql, ...b) => engine.get(sql, ...b),
  };
}

function apply(engine: ReplicaEngine, change: ReplicaChange): boolean {
  return applyDelta(writeDbOf(engine), change, engine.toBindable);
}

function readCol(engine: ReplicaEngine, table: string, pkCol: string, pk: string, col: string): unknown {
  const row = engine.get(`SELECT ${col} AS c FROM ${table} WHERE ${pkCol} = ?`, pk);
  return row ? row["c"] : null;
}

function count(engine: ReplicaEngine, table: string, liveOnly = false): number {
  const sql = liveOnly
    ? `SELECT COUNT(*) AS n FROM ${table} WHERE removed_at IS NULL`
    : `SELECT COUNT(*) AS n FROM ${table}`;
  const row = engine.get(sql);
  return row ? Number(row["n"]) : 0;
}

describe("ReplicaEngine contract (node:sqlite)", () => {
  let engine: ReplicaEngine;
  beforeEach(async () => {
    engine = await nodeSqliteEngine(":memory:");
    engine.exec("CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT)");
  });
  afterEach(() => engine.close());

  it("exec/run/get/all round-trip and run() reports sqlite3_changes()", () => {
    expect(engine.run("INSERT INTO t (k, v) VALUES (?, ?)", "a", "1")).toBe(1);
    expect(engine.run("INSERT INTO t (k, v) VALUES (?, ?)", "b", "2")).toBe(1);
    expect(engine.get("SELECT v FROM t WHERE k = ?", "a")).toEqual({ v: "1" });
    expect(engine.all("SELECT k, v FROM t ORDER BY k")).toEqual([
      { k: "a", v: "1" },
      { k: "b", v: "2" },
    ]);
    expect(engine.get("SELECT v FROM t WHERE k = ?", "missing")).toBeUndefined();
  });

  it("toBindable coerces booleans to 0/1 and null-ish to null", () => {
    expect(engine.toBindable(true)).toBe(1);
    expect(engine.toBindable(false)).toBe(0);
    expect(engine.toBindable(null)).toBeNull();
    expect(engine.toBindable(undefined)).toBeNull();
    expect(engine.toBindable("x")).toBe("x");
    expect(engine.toBindable(7)).toBe(7);
  });

  it("transaction() commits on success and rolls back on throw", () => {
    engine.transaction(() => {
      engine.run("INSERT INTO t (k, v) VALUES (?, ?)", "a", "1");
    });
    expect(count(engine, "t")).toBe(1);

    expect(() =>
      engine.transaction(() => {
        engine.run("INSERT INTO t (k, v) VALUES (?, ?)", "b", "2");
        throw new Error("boom");
      }),
    ).toThrow("boom");
    // The rolled-back insert is gone; the committed one remains.
    expect(count(engine, "t")).toBe(1);
  });

  it("the factory attaches the raw driver Database as `handle` (for drizzle)", () => {
    // `handle` is on the factory return (ReplicaEngineWithHandle), surfaced via CatalystReplica.handle;
    // the minimal public ReplicaEngine interface omits it, so read it off the concrete object here.
    const handle = (engine as { handle?: unknown }).handle;
    expect(handle).toBeDefined();
    expect(typeof (handle as { prepare?: unknown }).prepare).toBe("function");
  });
});

describe("applyDelta over the ReplicaEngine (host-sync apply.ts port)", () => {
  let engine: ReplicaEngine;
  beforeEach(async () => {
    engine = await openReplicaEngine();
  });
  afterEach(() => engine.close());

  describe("upsert", () => {
    it("inserts a new row and reports it was written", () => {
      const wrote = apply(engine, {
        entity: "issues",
        op: "upsert",
        row: { id: "iss-1", identifier: "CTC-1", title: "Hello", state: "Todo", updated_at: 100 },
      });
      expect(wrote).toBe(true);
      expect(count(engine, "issues")).toBe(1);
      expect(readCol(engine, "issues", "id", "iss-1", "title")).toBe("Hello");
    });

    it("updates an existing row when newer (last-write-wins by updated_at)", () => {
      apply(engine, { entity: "issues", op: "upsert", row: { id: "iss-1", title: "v1", updated_at: 100 } });
      const wrote = apply(engine, {
        entity: "issues",
        op: "upsert",
        row: { id: "iss-1", title: "v2", updated_at: 200 },
      });
      expect(wrote).toBe(true);
      expect(readCol(engine, "issues", "id", "iss-1", "title")).toBe("v2");
      expect(count(engine, "issues")).toBe(1);
    });

    it("rejects a stale upsert (older updated_at) without regressing the row", () => {
      apply(engine, { entity: "issues", op: "upsert", row: { id: "iss-1", title: "current", updated_at: 200 } });
      const wrote = apply(engine, {
        entity: "issues",
        op: "upsert",
        row: { id: "iss-1", title: "stale", updated_at: 100 },
      });
      expect(wrote).toBe(false);
      expect(readCol(engine, "issues", "id", "iss-1", "title")).toBe("current");
    });

    it("coerces a boolean wire value to 0/1 (pull_requests.draft/merged)", () => {
      const wrote = apply(engine, {
        entity: "pull_requests",
        op: "upsert",
        row: { repo_id: "r1", number: 7, state: "open", draft: true, merged: false, updated_at: 1 },
      });
      expect(wrote).toBe(true);
      const row = engine.get("SELECT draft, merged FROM pull_requests WHERE repo_id = ? AND number = ?", "r1", 7);
      expect(row?.["draft"]).toBe(1);
      expect(row?.["merged"]).toBe(0);
    });

    it("no-ops a malformed upsert with zero columns", () => {
      expect(apply(engine, { entity: "issues", op: "upsert", row: {} })).toBe(false);
      expect(count(engine, "issues")).toBe(0);
    });

    it("DO NOTHING on conflict for a pure join row (issue_labels)", () => {
      expect(apply(engine, { entity: "issue_labels", op: "upsert", row: { issue_id: "iss-1", label_id: "lab-1" } })).toBe(true);
      expect(apply(engine, { entity: "issue_labels", op: "upsert", row: { issue_id: "iss-1", label_id: "lab-1" } })).toBe(false);
      expect(count(engine, "issue_labels")).toBe(1);
    });
  });

  describe("delete", () => {
    it("soft-deletes a removed_at table (issues), keeping the row but marking it not-live", () => {
      apply(engine, { entity: "issues", op: "upsert", row: { id: "iss-1", title: "live", updated_at: 100 } });
      const wrote = apply(engine, { entity: "issues", op: "delete", row: { id: "iss-1" } });
      expect(wrote).toBe(true);
      expect(count(engine, "issues")).toBe(1);
      expect(count(engine, "issues", true)).toBe(0);
      expect(readCol(engine, "issues", "id", "iss-1", "removed_at")).not.toBeNull();
    });

    it("is idempotent: re-deleting an already soft-deleted row is a no-op", () => {
      apply(engine, { entity: "issues", op: "upsert", row: { id: "iss-1", title: "live", updated_at: 100 } });
      apply(engine, { entity: "issues", op: "delete", row: { id: "iss-1" } });
      expect(apply(engine, { entity: "issues", op: "delete", row: { id: "iss-1" } })).toBe(false);
    });

    it("hard-deletes a no-removed_at table (relations)", () => {
      apply(engine, {
        entity: "relations",
        op: "upsert",
        row: { id: "rel-1", type: "blocks", issue_identifier: "CTC-1", updated_at: 1 },
      });
      expect(apply(engine, { entity: "relations", op: "delete", row: { id: "rel-1" } })).toBe(true);
      expect(count(engine, "relations")).toBe(0);
    });

    it("resolves a composite-PK delete from entityId ('repo_id:number')", () => {
      apply(engine, { entity: "pull_requests", op: "upsert", row: { repo_id: "r1", number: 7, state: "open", updated_at: 1 } });
      const wrote = apply(engine, { entity: "pull_requests", op: "delete", row: {}, entityId: "r1:7" });
      expect(wrote).toBe(true);
      expect(count(engine, "pull_requests")).toBe(0);
    });

    it("no-ops a delete when the PK can't be located", () => {
      expect(apply(engine, { entity: "issues", op: "delete", row: {} })).toBe(false);
    });
  });

  it("throws on an unknown entity", () => {
    expect(() => apply(engine, { entity: "not_a_table", op: "upsert", row: { id: "x" } })).toThrow(/unknown entity/);
  });

  describe("replica helpers", () => {
    it("truncateReplica clears entity tables but preserves the sync_meta cursor", () => {
      const writeDb = writeDbOf(engine);
      apply(engine, { entity: "issues", op: "upsert", row: { id: "iss-1", updated_at: 1 } });
      setCursor(writeDb, 42, engine.toBindable);
      truncateReplica(writeDb);
      expect(count(engine, "issues")).toBe(0);
      expect(getCursor(writeDb)).toBe(42);
    });

    it("getCursor is null before any snapshot, then round-trips through setCursor", () => {
      const writeDb = writeDbOf(engine);
      expect(getCursor(writeDb)).toBeNull();
      setCursor(writeDb, 7, engine.toBindable);
      expect(getCursor(writeDb)).toBe(7);
      setCursor(writeDb, 13, engine.toBindable);
      expect(getCursor(writeDb)).toBe(13);
    });
  });
});
