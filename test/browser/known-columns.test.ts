// test/browser/known-columns.test.ts — CTC-603, browser/OPFS twin of test/node/known-columns.test.ts.
//
// Before this fix, apply.ts's applyChange(db, change, toBindable) passed NO ApplyOptions at all — not
// even onDroppedColumns — so a column the mirror-fed row carried but @catalyst-cloud/replicate's
// bundled schema didn't know about was silently dropped with nothing even logged. This proves the
// worker's real dispatch path (open → seed → live apply) now threads a PRAGMA-derived `knownColumns`
// override through BOTH call sites (SeedSession's batched seed apply, and worker-core's live
// `applyChanges`), landing a column neither replicate's nor this SDK's own bundled schema knows about.

import { describe, it, expect, beforeAll } from "vitest";
import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type { Database } from "@sqlite.org/sqlite-wasm";
import { createWorkerCore, type WorkerCore } from "../../src/replica/browser/worker-core.js";
import { buildOpenedReplica } from "../../src/replica/browser/ports.js";
import { buildKnownColumnsByTable } from "../../src/replica/known-columns.js";

let sqlite3: Awaited<ReturnType<typeof sqlite3InitModule>>;

beforeAll(async () => {
  sqlite3 = await sqlite3InitModule();
});

describe("buildKnownColumnsByTable over sqlite-wasm (CTC-603 unit level)", () => {
  it("reflects the REAL local table shape via PRAGMA, including a column no bundled schema knows about", () => {
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    const opened = buildOpenedReplica(db);
    db.exec('ALTER TABLE "issues" ADD COLUMN ctc603_probe TEXT');

    const map = buildKnownColumnsByTable(opened.write);
    expect(map.get("issues")?.has("ctc603_probe")).toBe(true);
    expect(map.get("issues")?.has("id")).toBe(true);

    opened.close();
  });
});

describe("worker-core threads PRAGMA-derived knownColumns through BOTH apply call sites (CTC-603)", () => {
  it("a column added to the local table (simulating a newer SDK migration) survives seed AND live apply", async () => {
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    // First pass: create the normal migrated schema (identical to a production open).
    buildOpenedReplica(db);
    // Widen "issues" with a column NEITHER @catalyst-cloud/replicate's bundled schema NOR this SDK's
    // own bundled @catalyst-cloud/schema knows about — simulating a newer SDK migration, deterministic
    // regardless of the packages' CURRENT version pins.
    db.exec('ALTER TABLE "issues" ADD COLUMN ctc603_probe TEXT');

    // Second pass (what the worker's `open` handler does): re-run buildOpenedReplica over the SAME,
    // now-widened db — migrations are idempotent (0 new tags), and knownColumnsByEntity is rebuilt via
    // fresh PRAGMA, so it picks up ctc603_probe this time.
    const core: WorkerCore = createWorkerCore(() => Promise.resolve(buildOpenedReplica(db)));
    await core.handle({ type: "open", dbPath: ":memory:", directory: "unused", identity: "test" });

    // SEED path — SeedSession's batched apply (worker-core's seedBegin-injected closure).
    await core.handle({ type: "seedBegin" });
    await core.handle({
      type: "seedBatch",
      rows: [
        {
          entity: "issues",
          op: "upsert" as const,
          row: {
            id: "seed-1",
            identifier: "CTC-1",
            title: "seeded",
            state: "Todo",
            updated_at: 100,
            ctc603_probe: "seed-value",
          },
        },
      ],
    });
    await core.handle({ type: "seedCommit", cursor: 5 });

    // LIVE path — worker-core's `applyChanges` case.
    await core.handle({
      type: "applyChanges",
      changes: [
        {
          seq: 6,
          entity: "issues",
          op: "upsert" as const,
          entityId: "live-1",
          row: {
            id: "live-1",
            identifier: "CTC-2",
            title: "live",
            state: "Todo",
            updated_at: 200,
            ctc603_probe: "live-value",
          },
        },
      ],
    });

    // Read the ACTUAL column back via raw SQL — the read-model views don't SELECT this column, so
    // this is the direct behavioral proof the value was neither dropped nor lost, on EITHER path.
    const seeded = db.exec('SELECT ctc603_probe FROM "issues" WHERE id = ?', {
      bind: ["seed-1"],
      rowMode: "object",
      returnValue: "resultRows",
    }) as Record<string, unknown>[];
    const live = db.exec('SELECT ctc603_probe FROM "issues" WHERE id = ?', {
      bind: ["live-1"],
      rowMode: "object",
      returnValue: "resultRows",
    }) as Record<string, unknown>[];

    expect(seeded[0]?.ctc603_probe).toBe("seed-value");
    expect(live[0]?.ctc603_probe).toBe("live-value");

    db.close();
  });
});
