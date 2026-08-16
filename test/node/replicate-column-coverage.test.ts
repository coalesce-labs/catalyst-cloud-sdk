// test/node/replicate-column-coverage.test.ts — regression guard for the CTC-529 silent-column-drop
// incident (SDK 0.8.3 + @catalyst-cloud/replicate 0.1.3/0.1.4).
//
// @catalyst-cloud/replicate resolves @catalyst-cloud/schema as ITS OWN nested dependency (a caret
// range in replicate's package.json, independent of this SDK's own top-level, exact-pinned schema
// dependency — see package.json). applyUpsert's CTC-127 forward-compat filter
// (@catalyst-cloud/replicate's src/replicate.ts, KNOWN_COLUMNS / applyUpsert) drops any row key that
// isn't in replicate's OWN bundled MIRROR_TABLE_META[entity].columns — built from WHICHEVER schema
// version replicate's nested dependency happens to resolve to, not the one this SDK migrated the local
// database with.
//
// Concretely: this SDK's own schema pin (0.1.10+) creates agent_sessions.dismissed_at / dismissed_by /
// archived_at via applyMigrations (CTC-529 / migration 0019). But replicate's nested schema dependency
// had drifted to an OLDER resolved version (0.1.3, predating those columns) — so every upsert of an
// agent_sessions row silently DROPPED those three columns before the INSERT even ran. No exception, no
// SQLITE_ERROR, no thrown error: `loadedSchemaIdentity()` still reported "current" because the DDL
// really was current — only the delta-apply's column filter was stale. The CTC-127 guard designed to
// protect a lagging CLIENT from a schema-AHEAD server was inverted by nested-dependency version skew
// into silent data loss on a client that was actually current.
//
// A static version-string compare can't catch this — the whole failure mode IS a version mismatch a
// human didn't notice because it lives one level down in the dependency graph. So this test proves the
// invariant BEHAVIORALLY: it runs this SDK's REAL bundled migrations against a REAL node:sqlite replica
// (exactly as CatalystReplica does), then applies a delta carrying every column this SDK's OWN schema
// knows about for every entity, through the REAL (as-installed) @catalyst-cloud/replicate — and asserts
// every single one actually lands in the table. If replicate's nested schema dependency ever drifts
// behind this SDK's own schema pin again, this test fails the SDK's test run (part of `npm run test`,
// which every publish in this repo runs for real before shipping — see AGENTS.md / release discipline)
// instead of shipping another silent data-loss regression.

import { describe, it, expect } from "vitest";
import {
  applyMigrations,
  MIRROR_MIGRATIONS,
  MIRROR_TABLE_META,
  type MigrationDb,
} from "@catalyst-cloud/schema";
import { applyDelta, setCursor, type ReplicaWriteDb } from "@catalyst-cloud/replicate";
import { nodeSqliteEngine, type ReplicaEngine } from "../../src/node";

const SYNC_META_DDL = "CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value TEXT)";

describe("@catalyst-cloud/replicate applies every column this SDK's bundled schema migrates", () => {
  for (const [entity, meta] of Object.entries(MIRROR_TABLE_META)) {
    it(`"${entity}": no column is silently dropped on upsert`, async () => {
      const engine: ReplicaEngine = await nodeSqliteEngine(":memory:");
      const migrationDb: MigrationDb = { exec: (s) => engine.exec(s), query: (s) => engine.all(s) };
      applyMigrations(migrationDb, MIRROR_MIGRATIONS);
      engine.exec(SYNC_META_DDL);
      const writeDb: ReplicaWriteDb<unknown> = {
        run: (s, ...b) => engine.run(s, ...b),
        get: (s, ...b) => engine.get(s, ...b),
      };
      setCursor(writeDb, 1, engine.toBindable);

      // Every column this SDK's OWN migrated table has, each bound to its own column name so a
      // silently-dropped value is trivially distinguishable from one that landed correctly.
      const row: Record<string, unknown> = {};
      for (const col of meta.columns) row[col] = col;

      const dropped: string[] = [];
      const unknownEntities: string[] = [];
      const written = applyDelta(
        writeDb,
        { entity, op: "upsert", row },
        engine.toBindable,
        {
          onDroppedColumns: (_table, cols) => dropped.push(...cols),
          onUnknownEntity: (e) => unknownEntities.push(e),
        },
      );

      expect(unknownEntities).toEqual([]);
      expect(dropped).toEqual([]);
      expect(written).toBe(true);

      // Behavioral proof, not just a SQL-text check: read the row back out of the REAL table and
      // confirm every column actually persisted the value this test bound it to.
      const where = meta.pk.map((c) => `"${c}" = ?`).join(" AND ");
      const pkVals = meta.pk.map((c) => engine.toBindable(c));
      const persisted = writeDb.get(`SELECT * FROM "${entity}" WHERE ${where}`, ...pkVals);
      expect(persisted).toBeDefined();
      for (const col of meta.columns) {
        expect(persisted?.[col], `column "${entity}.${col}" was dropped or not persisted`).toBe(col);
      }
    });
  }
});
