import { afterEach, describe, expect, it, vi } from "vitest";
import {
  applyMigrations,
  MIRROR_MIGRATIONS,
  type MigrationDb,
} from "@catalyst-cloud/schema";
import { applyDelta, setCursor, type ReplicaWriteDb } from "@catalyst-cloud/replicate";
import {
  CatalystReplica,
  nodeSqliteEngine,
  type ReplicaEngine,
  type WebSocketLike,
  type WebSocketFactory,
} from "../../src/node";
import { migrationsChangeRowShape } from "../../src/replica/catalyst-replica";

// End-to-end coverage of CatalystReplica — the managed node/bun replica (CTC-113). The transport is
// driven by a FakeWebSocket and the snapshot feed by an injected fetch stand-in, so every path —
// open+migrate, cold stream-seed, warm resume (no re-seed), live apply, read-model views, soft/hard
// delete, durable cursor, underflow resync, auth URL, close — is deterministic and offline. The write
// path (@catalyst-cloud/replicate) and read path (@catalyst-cloud/read-model) run against a REAL
// node:sqlite engine; no network is touched.

const BASE = "https://api.example.test";
const SYNC_META_DDL = "CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value TEXT)";

// ── A scriptable WebSocket the test drives by hand. ──
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
  lastSent(): unknown {
    return JSON.parse(this.sent[this.sent.length - 1] as string);
  }
}

function recordingFactory(): { sockets: FakeWebSocket[]; urls: string[]; factory: WebSocketFactory } {
  const sockets: FakeWebSocket[] = [];
  const urls: string[] = [];
  const factory: WebSocketFactory = (url: string) => {
    urls.push(url);
    const ws = new FakeWebSocket();
    sockets.push(ws);
    return ws;
  };
  return { sockets, urls, factory };
}

interface SeedRow {
  entity: string;
  row: Record<string, unknown>;
}

/** Build an NDJSON snapshot body (data lines + the final cursor line), matching the server wire. */
function snapshotBody(rows: SeedRow[], cursor: number): string {
  const lines = rows.map((r) => JSON.stringify({ accountId: "tenant-0", entity: r.entity, op: "upsert", row: r.row }));
  lines.push(JSON.stringify({ accountId: "tenant-0", cursor }));
  return lines.join("\n") + "\n";
}

/** A buffered fetch stand-in (a Response with only `.text()`, no body) — exercises the buffered seed
 *  fallback. `set()` swaps the answer for the resync (2nd snapshot) case. */
function bufferedSnapshotFetch(rows: SeedRow[], cursor: number) {
  const state = { body: snapshotBody(rows, cursor) };
  const calls = { count: 0 };
  const headersSeen: (Record<string, string> | undefined)[] = [];
  const fetchImpl = (async (_url: string, init?: { headers?: Record<string, string> }) => {
    calls.count += 1;
    headersSeen.push(init?.headers);
    return { ok: true, status: 200, text: async () => state.body } as unknown as Response;
  }) as unknown as typeof fetch;
  return {
    fetchImpl,
    calls,
    headersSeen,
    set(nextRows: SeedRow[], nextCursor: number) {
      state.body = snapshotBody(nextRows, nextCursor);
    },
  };
}

/** A STREAMING fetch stand-in: a real Response whose ReadableStream body emits the NDJSON in tiny
 *  chunks (chunk size 7) so the seed's partial-line buffering + chunked transactions are exercised. */
function streamingSnapshotFetch(rows: SeedRow[], cursor: number) {
  const calls = { count: 0 };
  const fetchImpl = (async () => {
    calls.count += 1;
    const bytes = new TextEncoder().encode(snapshotBody(rows, cursor));
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        for (let i = 0; i < bytes.length; i += 7) controller.enqueue(bytes.slice(i, i + 7));
        controller.close();
      },
    });
    return new Response(stream, { status: 200 });
  }) as unknown as typeof fetch;
  return { fetchImpl, calls };
}

/** Start the replica and bring its (fake) socket to OPEN so start() resolves on first 'live'. */
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
afterEach(async () => {
  while (replicas.length) await replicas.pop()!.close();
  vi.useRealTimers();
});

describe("CatalystReplica lifecycle + the managed-replica drop-in", () => {
  it("opens, migrates, applies a live change, and reads it back via issues() — the focused proof", async () => {
    const { sockets, factory } = recordingFactory();
    const seed = bufferedSnapshotFetch([], 0);
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "token", token: "tok" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: seed.fetchImpl,
        wsFactory: factory,
      }),
    );

    await startToLive(replica, sockets);
    expect(replica.status).toBe("live");
    expect(replica.issues()).toEqual([]); // empty seed

    sockets[0]!.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 1,
      entity: "issues",
      entityId: "i1",
      op: "upsert",
      row: { id: "i1", identifier: "CTC-1", title: "Hello", state: "Todo", updated_at: 100 },
    });

    const view = replica.issues();
    expect(view).toHaveLength(1);
    expect(view[0]!.id).toBe("i1");
    expect(view[0]!.title).toBe("Hello");
    expect(replica.cursor).toBe(1);
    expect(replica.handle).toBeDefined(); // the raw driver Database for drizzle
  });

  it("fires onChange after an applied live delta (the refetch hook)", async () => {
    const { sockets, factory } = recordingFactory();
    const seed = bufferedSnapshotFetch([], 0);
    let changes = 0;
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: seed.fetchImpl,
        wsFactory: factory,
        onChange: () => {
          changes += 1;
        },
      }),
    );
    await startToLive(replica, sockets);
    const before = changes;
    sockets[0]!.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 2,
      entity: "issues",
      entityId: "i1",
      op: "upsert",
      row: { id: "i1", title: "x", updated_at: 1 },
    });
    expect(changes).toBe(before + 1);
  });

  it("reading before start() throws a clear error", () => {
    const replica = new CatalystReplica({
      baseUrl: BASE,
      account: "tenant-0",
      auth: { kind: "cookie" },
      dbPath: ":memory:",
      engine: nodeSqliteEngine,
    });
    expect(() => replica.issues()).toThrow(/call start\(\)/);
    expect(replica.cursor).toBeNull();
  });
});

describe("CatalystReplica cold stream-seed", () => {
  it("seeds from a buffered /snapshot, advances the cursor, and syncs from it", async () => {
    const { sockets, factory } = recordingFactory();
    const seed = bufferedSnapshotFetch(
      [{ entity: "issues", row: { id: "i1", identifier: "CTC-1", title: "Seed", updated_at: 1 } }],
      5,
    );
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "token", token: "svc" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: seed.fetchImpl,
        wsFactory: factory,
      }),
    );

    await startToLive(replica, sockets);

    expect(seed.calls.count).toBe(1);
    expect(replica.cursor).toBe(5);
    expect(replica.issues().map((v) => v.id)).toEqual(["i1"]);
    expect(sockets[0]!.lastSent()).toEqual({ type: "sync", after: 5 });
    // the host bearer rides the /snapshot fetch as Authorization
    expect(seed.headersSeen[0]?.["authorization"]).toBe("Bearer svc");
  });

  it("seeds from a STREAMING /snapshot body (chunked NDJSON, partial-line buffering)", async () => {
    const { sockets, factory } = recordingFactory();
    const seed = streamingSnapshotFetch(
      [
        { entity: "issues", row: { id: "i1", identifier: "CTC-1", title: "A", updated_at: 1 } },
        { entity: "issues", row: { id: "i2", identifier: "CTC-2", title: "B", updated_at: 2 } },
        { entity: "labels", row: { id: "l1", name: "bug", updated_at: 1 } },
        { entity: "issue_labels", row: { issue_id: "i1", label_id: "l1" } },
      ],
      9,
    );
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: seed.fetchImpl,
        wsFactory: factory,
      }),
    );

    await startToLive(replica, sockets);

    expect(seed.calls.count).toBe(1);
    expect(replica.cursor).toBe(9);
    const view = replica.issues();
    expect(view.map((v) => v.id)).toEqual(["i2", "i1"]); // newest-first
    const i1 = view.find((v) => v.id === "i1")!;
    expect(i1.labels).toEqual([{ id: "l1", name: "bug", color: null }]);
  });

  it("with an existing cursor (injected pre-seeded engine), opens the socket WITHOUT a snapshot", async () => {
    // Pre-seed an engine + cursor, then inject it — exercises the warm-resume / no-reseed path and the
    // "pass a ready ReplicaEngine instance" injection mode.
    const engine: ReplicaEngine = await nodeSqliteEngine(":memory:");
    const migrationDb: MigrationDb = { exec: (s) => engine.exec(s), query: (s) => engine.all(s) };
    applyMigrations(migrationDb, MIRROR_MIGRATIONS);
    engine.exec(SYNC_META_DDL);
    const writeDb: ReplicaWriteDb<unknown> = {
      run: (s, ...b) => engine.run(s, ...b),
      get: (s, ...b) => engine.get(s, ...b),
    };
    setCursor(writeDb, 7, engine.toBindable);
    applyDelta(
      writeDb,
      { entity: "issues", op: "upsert", row: { id: "i1", identifier: "CTC-1", title: "Persisted", updated_at: 1 } },
      engine.toBindable,
    );

    const { sockets, factory } = recordingFactory();
    const seed = bufferedSnapshotFetch([], 0);
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "token", token: "t" },
        dbPath: ":memory:",
        engine,
        fetchImpl: seed.fetchImpl,
        wsFactory: factory,
      }),
    );

    await startToLive(replica, sockets);

    expect(seed.calls.count).toBe(0); // cursor present → no snapshot
    expect(sockets[0]!.lastSent()).toEqual({ type: "sync", after: 7 });
    expect(replica.cursor).toBe(7);
    expect(replica.issues()[0]!.title).toBe("Persisted");
  });
});

describe("CatalystReplica auto-reseed on a column-adding migration (CTC-127)", () => {
  it("migrationsChangeRowShape: true for ADD / CREATE TABLE, false for index-only / none", () => {
    expect(migrationsChangeRowShape(["0008_optimal_rattler"])).toBe(true); // ALTER … ADD state_id/…
    expect(migrationsChangeRowShape(["0000_baseline"])).toBe(true); // CREATE TABLE
    expect(migrationsChangeRowShape([])).toBe(false);
    expect(migrationsChangeRowShape(["does_not_exist"])).toBe(false);
    // A migration whose SQL is CREATE INDEX only (no column/table added) must NOT force a re-seed.
    const indexOnly = Object.entries(MIRROR_MIGRATIONS.migrations).find(
      ([, sql]) =>
        /CREATE\s+INDEX/i.test(sql) && !/\bADD\b/i.test(sql) && !/\bCREATE\s+TABLE\b/i.test(sql),
    )?.[0];
    if (indexOnly) expect(migrationsChangeRowShape([indexOnly])).toBe(false);
  });

  it("a WARM replica behind by a column-adding migration re-seeds to backfill", async () => {
    // Build a replica DB migrated ONLY through 0007 (missing 0008) with a cursor — exactly a client
    // that predates the mirror's column add. On start(), applyMigrations applies 0008
    // (state_id/team_key/team_name) → row shape changed on a WARM DB → cursor deleted → cold re-seed
    // from /snapshot. The warm-resume test above proves the no-drift case fetches NO snapshot; this is
    // its mirror image — the reseed fires.
    const engine: ReplicaEngine = await nodeSqliteEngine(":memory:");
    const migrationDb: MigrationDb = { exec: (s) => engine.exec(s), query: (s) => engine.all(s) };
    const partialBundle = {
      ...MIRROR_MIGRATIONS,
      journal: {
        ...MIRROR_MIGRATIONS.journal,
        entries: MIRROR_MIGRATIONS.journal.entries.filter((e) => e.tag !== "0008_optimal_rattler"),
      },
    };
    applyMigrations(migrationDb, partialBundle); // DB now at 0007 (no state_id/team_key/team_name)
    engine.exec(SYNC_META_DDL);
    const writeDb: ReplicaWriteDb<unknown> = {
      run: (s, ...b) => engine.run(s, ...b),
      get: (s, ...b) => engine.get(s, ...b),
    };
    setCursor(writeDb, 7, engine.toBindable);

    const { sockets, factory } = recordingFactory();
    const seed = bufferedSnapshotFetch([], 0);
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "token", token: "t" },
        dbPath: ":memory:",
        engine,
        fetchImpl: seed.fetchImpl,
        wsFactory: factory,
      }),
    );

    await startToLive(replica, sockets);

    // RE-SEEDED (warm-at-0007 + 0008 applied), unlike the warm-resume test's 0.
    expect(seed.calls.count).toBe(1);
    // 0008's columns now exist on the replica (the ALTER ran) — sanity that the migration applied.
    const cols = new Set(
      (engine.all("PRAGMA table_info(issues)") as Array<{ name: string }>).map((r) => r.name),
    );
    expect(cols.has("state_id")).toBe(true);
  });
});

describe("CatalystReplica read-model over the replica (ADR-0002)", () => {
  async function seededLive(): Promise<{ replica: CatalystReplica; socket: FakeWebSocket }> {
    const { sockets, factory } = recordingFactory();
    const seed = bufferedSnapshotFetch([], 0);
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: seed.fetchImpl,
        wsFactory: factory,
      }),
    );
    await startToLive(replica, sockets);
    return { replica, socket: sockets[0]! };
  }

  function deliver(socket: FakeWebSocket, seq: number, entity: string, op: "upsert" | "delete", row: Record<string, unknown>, entityId?: string): void {
    socket.deliver({ type: "change", accountId: "tenant-0", seq, entity, entityId: entityId ?? "x", op, row });
  }

  it("issues() denormalizes labels + relations, newest-first, excluding soft-removed", async () => {
    const { replica, socket } = await seededLive();
    deliver(socket, 1, "issues", "upsert", { id: "i1", identifier: "CTL-1", title: "older", updated_at: 100 });
    deliver(socket, 2, "issues", "upsert", { id: "i2", identifier: "CTL-2", title: "newer", updated_at: 200 });
    deliver(socket, 3, "issues", "upsert", { id: "i3", identifier: "CTL-3", title: "gone", updated_at: 300 });
    deliver(socket, 4, "labels", "upsert", { id: "l1", name: "bug", updated_at: 1 });
    deliver(socket, 5, "issue_labels", "upsert", { issue_id: "i1", label_id: "l1" });
    deliver(socket, 6, "relations", "upsert", { id: "r1", type: "blocks", issue_identifier: "CTL-1", related_identifier: "CTL-2" });
    deliver(socket, 7, "issues", "delete", {}, "i3");

    const view = replica.issues();
    expect(view.map((v) => v.id)).toEqual(["i2", "i1"]); // i3 soft-removed, newest-first
    const i1 = view.find((v) => v.id === "i1")!;
    expect(i1.labels).toEqual([{ id: "l1", name: "bug", color: null }]);
    expect(i1.relations).toEqual([{ type: "blocks", issue_identifier: "CTL-1", related_identifier: "CTL-2" }]);
  });

  it("issue() resolves one issue's detail (comments) off the replica", async () => {
    const { replica, socket } = await seededLive();
    deliver(socket, 1, "issues", "upsert", { id: "i1", identifier: "CTL-1", title: "An issue", updated_at: 100 });
    deliver(socket, 2, "comments", "upsert", { id: "c1", issue_id: "i1", body: "first", updated_at: 100 });

    const detail = replica.issue("CTL-1");
    expect(detail).not.toBeNull();
    expect(detail!.id).toBe("i1");
    expect(detail!.comments.map((c) => c.body)).toEqual(["first"]);
    expect(replica.issue("CTL-404")).toBeNull();
  });

  it("pulls() joins CI checks by head_sha; projects()/initiatives() read off the replica", async () => {
    const { replica, socket } = await seededLive();
    deliver(socket, 1, "pull_requests", "upsert", { repo_id: "coalesce-labs/catalyst", number: 2, head_sha: "sha-new", state: "open", updated_at: 200 });
    deliver(socket, 2, "check_runs", "upsert", { repo_id: "coalesce-labs/catalyst", check_run_id: "chk-1", head_sha: "sha-new", name: "ci", status: "completed", conclusion: "success" });
    deliver(socket, 3, "projects", "upsert", { id: "p1", name: "Proj", state: "started", updated_at: 1 });
    deliver(socket, 4, "initiatives", "upsert", { id: "n1", name: "Init", updated_at: 1 });

    const pulls = replica.pulls();
    expect(pulls.map((p) => p.number)).toEqual([2]);
    expect(pulls[0]!.checks).toEqual([{ name: "ci", status: "completed", conclusion: "success" }]);
    expect(replica.projects().map((p) => p.id)).toEqual(["p1"]);
    expect(replica.project("p1")?.name).toBe("Proj");
    expect(replica.initiatives().map((n) => n.id)).toEqual(["n1"]);
    expect(replica.initiative("n1")?.name).toBe("Init");
  });

  it("applies a soft-delete frame and advances the durable cursor each time", async () => {
    const { replica, socket } = await seededLive();
    deliver(socket, 1, "issues", "upsert", { id: "i1", identifier: "CTC-1", title: "X", updated_at: 1 });
    expect(replica.issues()).toHaveLength(1);
    deliver(socket, 2, "issues", "delete", {}, "i1");
    expect(replica.issues()).toHaveLength(0); // soft-removed → excluded from the live view
    expect(replica.cursor).toBe(2);
  });
});

describe("CatalystReplica resync + auth", () => {
  it("on a {type:'resync'} frame re-seeds via /snapshot, reconnects, and syncs from the fresh cursor", async () => {
    const { sockets, factory } = recordingFactory();
    const seed = bufferedSnapshotFetch(
      [{ entity: "issues", row: { id: "i1", identifier: "CTC-1", title: "Seed", updated_at: 1 } }],
      5,
    );
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: seed.fetchImpl,
        wsFactory: factory,
      }),
    );

    await startToLive(replica, sockets);
    expect(replica.cursor).toBe(5);

    // The next snapshot the server would serve after the underflow.
    seed.set([{ entity: "issues", row: { id: "i9", identifier: "CTC-9", title: "Re", updated_at: 9 } }], 12);
    sockets[0]!.deliver({ type: "resync", accountId: "tenant-0" });
    await vi.waitFor(() => expect(sockets.length).toBe(2));

    expect(seed.calls.count).toBe(2); // re-seeded once
    expect(sockets[0]!.closed).toBe(true);
    expect(replica.cursor).toBe(12);
    expect(replica.issues().map((v) => v.id)).toEqual(["i9"]); // old row truncated, new seed applied

    sockets[1]!.fireOpen();
    expect(sockets[1]!.lastSent()).toEqual({ type: "sync", after: 12 });
  });

  it("token auth rides the /connect URL as ?token=&account=; cookie auth appends NO token", async () => {
    const tokenCtx = recordingFactory();
    const tokenReplica = track(
      new CatalystReplica({
        baseUrl: "https://h.example/api/v1",
        account: "tenant-7",
        auth: { kind: "token", token: "svc-tok" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: bufferedSnapshotFetch([], 0).fetchImpl,
        wsFactory: tokenCtx.factory,
      }),
    );
    await startToLive(tokenReplica, tokenCtx.sockets);
    const tokenUrl = new URL(tokenCtx.urls[0]!);
    expect(tokenUrl.protocol).toBe("wss:");
    expect(tokenUrl.pathname).toBe("/api/v1/connect");
    expect(tokenUrl.searchParams.get("token")).toBe("svc-tok");
    expect(tokenUrl.searchParams.get("account")).toBe("tenant-7");

    const cookieCtx = recordingFactory();
    const cookieReplica = track(
      new CatalystReplica({
        baseUrl: "https://app.example/api/v1",
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: bufferedSnapshotFetch([], 0).fetchImpl,
        wsFactory: cookieCtx.factory,
      }),
    );
    await startToLive(cookieReplica, cookieCtx.sockets);
    expect(cookieCtx.urls[0]).toBe("wss://app.example/api/v1/connect?account=tenant-0");
    expect(cookieCtx.urls[0]).not.toContain("token");
  });
});

describe("CatalystReplica default engine + teardown", () => {
  it("auto-detects an engine when opts.engine is omitted (node:sqlite in the node test env)", async () => {
    const { sockets, factory } = recordingFactory();
    const seed = bufferedSnapshotFetch(
      [{ entity: "issues", row: { id: "i1", identifier: "CTC-1", title: "Auto", updated_at: 1 } }],
      3,
    );
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        // engine omitted → auto-detect
        fetchImpl: seed.fetchImpl,
        wsFactory: factory,
      }),
    );
    await startToLive(replica, sockets);
    expect(replica.issues()[0]!.title).toBe("Auto");
    expect(replica.cursor).toBe(3);
  });

  it("close() before first 'live' rejects the pending start() and is idempotent", async () => {
    const { sockets, factory } = recordingFactory();
    const seed = bufferedSnapshotFetch([], 0);
    const replica = new CatalystReplica({
      baseUrl: BASE,
      account: "tenant-0",
      auth: { kind: "cookie" },
      dbPath: ":memory:",
      engine: nodeSqliteEngine,
      fetchImpl: seed.fetchImpl,
      wsFactory: factory,
    });
    const started = replica.start();
    await vi.waitFor(() => expect(sockets.length).toBeGreaterThan(0));
    // do NOT fireOpen — close while still 'connecting'
    await replica.close();
    await expect(started).rejects.toThrow(/closed before/);
    expect(replica.status).toBe("stopped");
    await replica.close(); // idempotent — no throw
  });
});
