import { afterEach, describe, expect, it, vi } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { applyMigrations, MIRROR_MIGRATIONS } from "@catalyst-cloud/schema";
import {
  buildPullsView,
  ensureReadModelDdl,
  READ_INDEXES,
  SEARCH_FTS_TABLES,
  type SqlExecutor,
  type SqlValue,
} from "@catalyst-cloud/read-model";
import {
  CatalystReplica,
  nodeSqliteEngine,
  type WebSocketFactory,
  type WebSocketLike,
} from "../../src/node";
import {
  REPLICA_READ_INDEX_NAMES,
  fullScans,
  ftsMarkers,
  planDetails,
  readModelObjects,
  recordStatements,
  schemaText,
} from "../helpers/read-model-plan";

// CTC-4324 — the node replica builds read-model's index set and FTS5 search tables when it opens, by
// calling read-model's own `ensureReadModelDdl`. Before this, a replica held only the Drizzle bundle's
// indexes: the CTC-4276 audit measured the pulls list at 883 ms against 73 ms with the set, the review
// facet reading 71.2M rows against 2,959, and no replica able to run a text search. Every case runs a
// REAL node:sqlite engine; the socket and /snapshot are fakes.

const BASE = "https://api.example.test";

class FakeWebSocket implements WebSocketLike {
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  send(): void {}
  close(): void {}
  deliver(frame: unknown): void {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }
}

const emptySnapshot = (async () =>
  ({ ok: true, status: 200, text: async () => JSON.stringify({ accountId: "tenant-0", cursor: 0 }) + "\n" }) as unknown as Response) as unknown as typeof fetch;

const replicas: CatalystReplica[] = [];
const dirs: string[] = [];
afterEach(async () => {
  while (replicas.length) await replicas.pop()!.close();
  while (dirs.length) fs.rmSync(dirs.pop()!, { recursive: true, force: true });
});

function tmpDbPath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ctc-4324-"));
  dirs.push(dir);
  return path.join(dir, "replica.db");
}

/** Open a writer replica over `dbPath` and bring it to live. `logs` collects every log line. */
async function openReplica(
  dbPath: string,
  logs: string[] = [],
): Promise<{ replica: CatalystReplica; socket: FakeWebSocket }> {
  const sockets: FakeWebSocket[] = [];
  const factory: WebSocketFactory = () => {
    const ws = new FakeWebSocket();
    sockets.push(ws);
    return ws;
  };
  const replica = new CatalystReplica({
    baseUrl: BASE,
    account: "tenant-0",
    auth: { kind: "token", token: "tok" },
    dbPath,
    engine: nodeSqliteEngine,
    fetchImpl: emptySnapshot,
    wsFactory: factory,
    log: (_level, msg) => logs.push(msg),
  });
  replicas.push(replica);
  const started = replica.start();
  await vi.waitFor(() => expect(sockets.length).toBeGreaterThan(0));
  sockets[0]!.onopen?.({});
  await started;
  return { replica, socket: sockets[0]! };
}

async function closeReplica(replica: CatalystReplica): Promise<void> {
  await replica.close();
  replicas.splice(replicas.indexOf(replica), 1);
}

function deliver(socket: FakeWebSocket, seq: number, entity: string, row: Record<string, unknown>): void {
  socket.deliver({ type: "change", accountId: "tenant-0", seq, entity, entityId: `${entity}-${seq}`, op: "upsert", row });
}

/** Enough PRs, reviews and statuses that the pulls builder issues every statement it can. */
function deliverPulls(socket: FakeWebSocket): void {
  let seq = 0;
  for (let n = 1; n <= 3; n++) {
    deliver(socket, ++seq, "pull_requests", { repo_id: "o/r", number: n, head_sha: `sha-${n}`, state: "open", title: `pr ${n}`, updated_at: n });
    deliver(socket, ++seq, "reviews", { repo_id: "o/r", pr_number: n, review_id: `rv-${n}`, user_id: "u1", state: "APPROVED", submitted_at: n });
    deliver(socket, ++seq, "commit_statuses", { repo_id: "o/r", sha: `sha-${n}`, context: "ci", state: "success", updated_at: n });
  }
}

/** A store exactly as SDK 0.12.x left it: the Drizzle bundle and nothing else. */
function bundleOnlyStore(): SqlExecutor {
  const db = new DatabaseSync(":memory:");
  applyMigrations({ exec: (s) => db.exec(s), query: (s) => db.prepare(s).all() as Record<string, unknown>[] }, MIRROR_MIGRATIONS);
  let n = 0;
  for (const i of [1, 2, 3]) {
    db.prepare("INSERT INTO pull_requests (repo_id, number, head_sha, state, title, updated_at) VALUES (?, ?, ?, 'open', ?, ?)").run("o/r", i, `sha-${i}`, `pr ${i}`, i);
    db.prepare("INSERT INTO reviews (repo_id, pr_number, review_id, user_id, state, submitted_at) VALUES ('o/r', ?, ?, 'u1', 'APPROVED', ?)").run(i, `rv-${++n}`, i);
    db.prepare("INSERT INTO commit_statuses (repo_id, sha, context, state, updated_at) VALUES ('o/r', ?, 'ci', 'success', ?)").run(`sha-${i}`, i);
  }
  return { exec: (q: string, ...b: SqlValue[]) => ({ toArray: () => db.prepare(q).all(...(b as never[])) as Record<string, SqlValue>[] }) };
}

const PULL_TABLES = ["pull_requests", "reviews", "r", "r2", "commit_statuses"] as const;
// The review facet tests every PR against its latest reviews, so it walks pull_requests once on the DO
// too. The cost the audit measured is the correlated review lookups: without idx_reviews_pr each one
// is a full scan of reviews (71.2M rows read at tenant-0's size, against 2,959 with it).
const REVIEW_TABLES = ["reviews", "r", "r2", "commit_statuses"] as const;
const loadAllPage = (sql: SqlExecutor) => buildPullsView(sql, 500, 0);
const reviewFacet = (sql: SqlExecutor) => buildPullsView(sql, 100, 0, { review: "approved" });

describe("CTC-4324 — the node replica builds the read-model DDL on open", () => {
  it("a fresh replica store holds every read-model index and FTS table after open", async () => {
    const { replica } = await openReplica(tmpDbPath());

    const { indexes, ftsTables } = readModelObjects(replica.sql);
    expect(replica.sql.exec("SELECT name FROM sqlite_master WHERE name = 'ask_tap_receipts'").toArray()).toEqual([]);
    expect(indexes).toEqual(REPLICA_READ_INDEX_NAMES);
    expect(ftsTables).toEqual([...SEARCH_FTS_TABLES]);
    expect(ftsMarkers(replica.sql).map((m) => m.name)).toEqual([...SEARCH_FTS_TABLES].sort());
  });

  it("a second open of the same file is a no-op: same schema, no FTS rebuild, no build log", async () => {
    const dbPath = tmpDbPath();
    const firstLogs: string[] = [];
    const first = await openReplica(dbPath, firstLogs);
    const schemaBefore = schemaText(first.replica.sql);
    const markersBefore = ftsMarkers(first.replica.sql);
    expect(firstLogs.filter((m) => m.includes("search indexes built"))).toHaveLength(1);
    await closeReplica(first.replica);

    const secondLogs: string[] = [];
    const second = await openReplica(dbPath, secondLogs);
    expect(schemaText(second.replica.sql)).toEqual(schemaBefore);
    expect(ftsMarkers(second.replica.sql)).toEqual(markersBefore);
    expect(secondLogs.filter((m) => m.includes("search indexes built"))).toHaveLength(0);
  });

  it("a warm replica from before the DDL gains it on its next open, indexing the rows it already holds", async () => {
    const dbPath = tmpDbPath();
    const { replica, socket } = await openReplica(dbPath);
    deliver(socket, 1, "issues", { id: "i1", identifier: "CTC-1", title: "Replica search works", updated_at: 1 });
    await closeReplica(replica);

    // Strip the DDL back off, so the file looks like one SDK 0.12.x wrote.
    const raw = new DatabaseSync(dbPath);
    for (const name of READ_INDEXES.map((i) => i.name)) raw.exec(`DROP INDEX IF EXISTS ${name}`);
    for (const name of SEARCH_FTS_TABLES) raw.exec(`DROP TABLE IF EXISTS ${name}`);
    raw.exec("DROP TABLE IF EXISTS search_fts_state");
    raw.close();

    const reopened = await openReplica(dbPath);
    expect(readModelObjects(reopened.replica.sql).indexes).toEqual(REPLICA_READ_INDEX_NAMES);
    const hits = reopened.replica.sql
      .exec("SELECT rowid FROM issues_fts WHERE issues_fts MATCH ?", "search")
      .toArray();
    expect(hits).toHaveLength(1);
  });

  it("a live change reaches the FTS index through read-model's triggers", async () => {
    const { replica, socket } = await openReplica(tmpDbPath());
    deliver(socket, 1, "issues", { id: "i1", identifier: "CTC-1", title: "Indexes on every replica", updated_at: 1 });

    const hits = replica.sql
      .exec(
        "SELECT i.identifier FROM issues_fts JOIN issues i ON i.rowid = issues_fts.rowid WHERE issues_fts MATCH ?",
        "replica",
      )
      .toArray();
    expect(hits).toEqual([{ identifier: "CTC-1" }]);
  });

  it("control: the plan check finds the full scans on a bundle-only store (the instrument works)", () => {
    const sql = bundleOnlyStore();
    expect(fullScans(planDetails(sql, recordStatements(sql, loadAllPage)), PULL_TABLES)).not.toEqual([]);
    expect(fullScans(planDetails(sql, recordStatements(sql, reviewFacet)), REVIEW_TABLES)).toContain("SCAN r2");
    // And the same store with read-model's DDL applied directly has none: the DDL is the difference.
    ensureReadModelDdlEagerly(sql);
    expect(fullScans(planDetails(sql, recordStatements(sql, loadAllPage)), PULL_TABLES)).toEqual([]);
    expect(fullScans(planDetails(sql, recordStatements(sql, reviewFacet)), REVIEW_TABLES)).toEqual([]);
  });

  it("the pulls list (loadAll's page) seeks read-model's indexes, with no full table scan", async () => {
    const { replica, socket } = await openReplica(tmpDbPath());
    deliverPulls(socket);

    const statements = recordStatements(replica.sql, loadAllPage);
    expect(statements.length).toBeGreaterThanOrEqual(3); // the page, reviews, statuses (+ checks)
    const details = planDetails(replica.sql, statements);
    expect(fullScans(details, PULL_TABLES)).toEqual([]);
    expect(details.join("\n")).toMatch(/idx_pulls_keyset/);
    expect(details.join("\n")).toMatch(/idx_reviews_pr/);
    expect(details.join("\n")).toMatch(/idx_commit_statuses_sha/);
    expect(replica.pulls().map((p) => p.number)).toEqual([3, 2, 1]);
  });

  it("the PR review facet seeks idx_reviews_pr for both review lookups, never scanning reviews", async () => {
    const { replica, socket } = await openReplica(tmpDbPath());
    deliverPulls(socket);

    const details = planDetails(replica.sql, recordStatements(replica.sql, reviewFacet));
    expect(fullScans(details, REVIEW_TABLES)).toEqual([]);
    expect(details).toContainEqual(expect.stringMatching(/^SEARCH r EXISTS USING INDEX idx_reviews_pr\b/));
    expect(details).toContainEqual(expect.stringMatching(/^SEARCH r2 USING INDEX idx_reviews_pr\b/));
    expect(reviewFacet(replica.sql).map((p) => p.number)).toEqual([3, 2, 1]);
  });
});

/** read-model's `ensureReadModelDdl` over the control store. That store's executor only runs a
 *  statement on `toArray()`, so each call is forced here, the same trap the SDK adapter closes. */
function ensureReadModelDdlEagerly(sql: SqlExecutor): void {
  ensureReadModelDdl({
    exec: (q, ...b) => {
      const rows = sql.exec(q, ...b).toArray();
      return { toArray: () => rows };
    },
  });
}
