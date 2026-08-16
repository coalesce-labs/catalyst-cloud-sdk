// test/node/known-columns.test.ts — CTC-603: prove the delta-apply column filter trusts the ACTUAL
// local table shape (PRAGMA table_info), not @catalyst-cloud/replicate's (or this SDK's own) bundled
// @catalyst-cloud/schema copy.
//
// Unlike test/node/replicate-column-coverage.test.ts (which proves the CURRENTLY-INSTALLED packages
// agree, and would only catch a FUTURE version-skew instance by regressing), this test reproduces the
// CTC-529/575 failure mode directly and deterministically, independent of any package's version pins:
// it manually widens the LOCAL table beyond what either @catalyst-cloud/replicate's or this SDK's own
// bundled schema knows about, then proves CatalystReplica still lands the extra column — through BOTH
// applyDelta call sites (the cold-seed batch path AND the live-frame path).

import { afterEach, describe, expect, it, vi } from "vitest";
// `node:path`/`node:os` don't resolve under this repo's tsconfig (`types: []`, no @types/node) — only
// `node:fs` has a hand-rolled ambient declaration (src/sqlite-builtins.d.ts). Mirrors
// test/node/writer-guard.test.ts: a bare (non-absolute) mkdtempSync prefix resolves cwd-relative.
import { mkdtempSync, rmSync } from "node:fs";
import {
  applyMigrations,
  MIRROR_MIGRATIONS,
  type MigrationDb,
} from "@catalyst-cloud/schema";
import {
  CatalystReplica,
  nodeSqliteEngine,
  type WebSocketLike,
  type WebSocketFactory,
} from "../../src/node";
import { buildKnownColumnsByTable } from "../../src/replica/known-columns";

const BASE = "https://api.example.test";
const SYNC_META_DDL = "CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value TEXT)";

// ── Minimal scriptable WebSocket — the same shape catalyst-replica.test.ts uses. ──
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
  fireOpen(): void {
    this.onopen?.({});
  }
  deliver(frame: unknown): void {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }
}

function recordingFactory(): { sockets: FakeWebSocket[]; factory: WebSocketFactory } {
  const sockets: FakeWebSocket[] = [];
  const factory: WebSocketFactory = () => {
    const ws = new FakeWebSocket();
    sockets.push(ws);
    return ws;
  };
  return { sockets, factory };
}

interface SeedRow {
  entity: string;
  row: Record<string, unknown>;
}

function snapshotBody(rows: SeedRow[], cursor: number): string {
  const lines = rows.map((r) =>
    JSON.stringify({ accountId: "tenant-0", entity: r.entity, op: "upsert", row: r.row }),
  );
  lines.push(JSON.stringify({ accountId: "tenant-0", cursor }));
  return lines.join("\n") + "\n";
}

function bufferedSnapshotFetch(rows: SeedRow[], cursor: number) {
  const body = snapshotBody(rows, cursor);
  const fetchImpl = (async () => {
    return { ok: true, status: 200, text: async () => body } as unknown as Response;
  }) as unknown as typeof fetch;
  return fetchImpl;
}

async function startToLive(replica: CatalystReplica, sockets: FakeWebSocket[]): Promise<void> {
  const started = replica.start();
  await vi.waitFor(() => expect(sockets.length).toBeGreaterThan(0));
  sockets[0]!.fireOpen();
  await started;
}

const replicas: CatalystReplica[] = [];
function track(r: CatalystReplica): CatalystReplica {
  replicas.push(r);
  return r;
}
const tmpDirs: string[] = [];
afterEach(async () => {
  while (replicas.length) await replicas.pop()!.close();
  for (const d of tmpDirs.splice(0)) rmSync(d, { recursive: true, force: true });
});

describe("buildKnownColumnsByTable (CTC-603 unit level)", () => {
  it("reflects the REAL local table shape via PRAGMA, including a column no bundled schema knows about", async () => {
    const engine = await nodeSqliteEngine(":memory:");
    const migrationDb: MigrationDb = { exec: (s) => engine.exec(s), query: (s) => engine.all(s) };
    applyMigrations(migrationDb, MIRROR_MIGRATIONS);
    engine.exec(SYNC_META_DDL);
    engine.exec('ALTER TABLE "issues" ADD COLUMN ctc603_probe TEXT');

    const map = buildKnownColumnsByTable(engine);
    expect(map.get("issues")?.has("ctc603_probe")).toBe(true);
    expect(map.get("issues")?.has("id")).toBe(true);
    // sync_meta is a real local table too — harmlessly present, never looked up by applyDelta.
    expect(map.get("sync_meta")?.has("key")).toBe(true);
    // sqlite-internal bookkeeping tables are excluded.
    expect([...map.keys()].some((t) => t.startsWith("sqlite_"))).toBe(false);

    engine.close();
  });

  it("returns an empty set for a table that doesn't exist locally (no throw)", async () => {
    const engine = await nodeSqliteEngine(":memory:");
    const map = buildKnownColumnsByTable(engine);
    expect(map.size).toBe(0);
    engine.close();
  });
});

describe("CatalystReplica applies a column ahead of BOTH bundled schemas (CTC-603)", () => {
  it("a column added to the LOCAL table (simulating a newer SDK migration) survives seed AND live apply, not just the DDL", async () => {
    // A REAL file path, not :memory: — so we can pre-migrate it, widen it by hand (simulating "a
    // newer SDK schema migration"), then point a fresh CatalystReplica at the SAME file. Cwd-relative
    // (no node:path/node:os — see the import comment above).
    const dir = mkdtempSync("ctc603-known-columns-");
    tmpDirs.push(dir);
    const dbPath = `${dir}/replica.sqlite3`;

    // Pre-migrate the file directly (identical to what CatalystReplica.start() does internally),
    // then widen "issues" with a column NEITHER @catalyst-cloud/replicate's bundled schema NOR this
    // SDK's own bundled @catalyst-cloud/schema knows about — the exact CTC-529/575 shape, reproduced
    // deterministically instead of depending on the packages' CURRENT pins happening to be skewed.
    const seedEngine = await nodeSqliteEngine(dbPath);
    const migrationDb: MigrationDb = { exec: (s) => seedEngine.exec(s), query: (s) => seedEngine.all(s) };
    applyMigrations(migrationDb, MIRROR_MIGRATIONS);
    seedEngine.exec(SYNC_META_DDL);
    seedEngine.exec('ALTER TABLE "issues" ADD COLUMN ctc603_probe TEXT');
    seedEngine.close();

    const { sockets, factory } = recordingFactory();
    // No cursor was ever set on this file, so CatalystReplica.start() takes the COLD SEED path —
    // exercising the SEED-BATCH applyDelta call site (catalyst-replica.ts's seedFromSnapshot/flush).
    const fetchImpl = bufferedSnapshotFetch(
      [
        {
          entity: "issues",
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
      5,
    );

    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath,
        engine: nodeSqliteEngine,
        fetchImpl,
        wsFactory: factory,
      }),
    );
    await startToLive(replica, sockets);

    // Now exercise the LIVE-FRAME applyDelta call site (catalyst-replica.ts's applyFrame).
    sockets[0]!.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 6,
      entity: "issues",
      entityId: "live-1",
      op: "upsert",
      row: {
        id: "live-1",
        identifier: "CTC-2",
        title: "live",
        state: "Todo",
        updated_at: 200,
        ctc603_probe: "live-value",
      },
    });

    // Read the ACTUAL column back via raw SQL over the replica's own connection — the read-model
    // views don't SELECT this column, so this is the direct behavioral proof the value was neither
    // dropped nor lost.
    const seededRow = replica.sql
      .exec('SELECT ctc603_probe FROM "issues" WHERE id = ?', "seed-1")
      .toArray()[0];
    const liveRow = replica.sql
      .exec('SELECT ctc603_probe FROM "issues" WHERE id = ?', "live-1")
      .toArray()[0];

    expect(seededRow?.ctc603_probe).toBe("seed-value");
    expect(liveRow?.ctc603_probe).toBe("live-value");
  });
});
