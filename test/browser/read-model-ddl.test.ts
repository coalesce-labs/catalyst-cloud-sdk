// test/browser/read-model-ddl.test.ts — CTC-4324, browser/OPFS twin of test/node/read-model-ddl.test.ts.
//
// The browser worker opens its store through `buildOpenedReplica`, which now applies read-model's index
// set and FTS5 search tables after the migrations, exactly as the node replica and the Mirror DO do.
// Runs the REAL sqlite-wasm build this SDK pins, in plain node; only OPFS itself is not exercised.

import { describe, it, expect, beforeAll } from "vitest";
import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type { Database } from "@sqlite.org/sqlite-wasm";
import {
  buildPullsView,
  SEARCH_FTS_TABLES,
  type SqlExecutor,
} from "@catalyst-cloud/read-model";
import { createWorkerCore } from "../../src/replica/browser/worker-core.js";
import { buildOpenedReplica } from "../../src/replica/browser/ports.js";
import {
  REPLICA_READ_INDEX_NAMES,
  fullScans,
  ftsMarkers,
  planDetails,
  readModelObjects,
  recordStatements,
  schemaText,
} from "../helpers/read-model-plan.js";

let sqlite3: Awaited<ReturnType<typeof sqlite3InitModule>>;

beforeAll(async () => {
  sqlite3 = await sqlite3InitModule();
});

function memoryDb(): Database {
  return new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
}

const loadAllPage = (sql: SqlExecutor) => buildPullsView(sql, 500, 0);
const reviewFacet = (sql: SqlExecutor) => buildPullsView(sql, 100, 0, { review: "approved" });

describe("CTC-4324 — the browser replica builds the read-model DDL on open", () => {
  it("a fresh store holds every read-model index and FTS table after open", () => {
    const opened = buildOpenedReplica(memoryDb());

    const { indexes, ftsTables } = readModelObjects(opened.read);
    expect(opened.read.exec("SELECT name FROM sqlite_master WHERE name = 'ask_tap_receipts'").toArray()).toEqual([]);
    expect(indexes).toEqual(REPLICA_READ_INDEX_NAMES);
    expect(ftsTables).toEqual([...SEARCH_FTS_TABLES]);
    opened.close();
  });

  it("a second open of the same database is a no-op: same schema, no FTS rebuild", () => {
    const db = memoryDb();
    const first = buildOpenedReplica(db);
    const schemaBefore = schemaText(first.read);
    const markersBefore = ftsMarkers(first.read);
    expect(markersBefore).toHaveLength(SEARCH_FTS_TABLES.length);

    const second = buildOpenedReplica(db);
    expect(schemaText(second.read)).toEqual(schemaBefore);
    expect(ftsMarkers(second.read)).toEqual(markersBefore);
    second.close();
  });

  it("rows the worker seeds are searchable through read-model's FTS triggers", async () => {
    const db = memoryDb();
    const core = createWorkerCore(() => Promise.resolve(buildOpenedReplica(db)));
    await core.handle({ type: "open", dbPath: ":memory:", directory: "unused", identity: "test" });
    await core.handle({ type: "seedBegin" });
    await core.handle({
      type: "seedBatch",
      rows: [
        { entity: "issues", op: "upsert" as const, row: { id: "a", identifier: "CTC-1", title: "Search from the browser", updated_at: 1 } },
        { entity: "issues", op: "upsert" as const, row: { id: "b", identifier: "CTC-2", title: "Something else", updated_at: 2 } },
      ],
    });
    await core.handle({ type: "seedCommit", cursor: 10 });

    const hits = db.exec(
      "SELECT i.identifier FROM issues_fts JOIN issues i ON i.rowid = issues_fts.rowid WHERE issues_fts MATCH 'browser'",
      { rowMode: "object", returnValue: "resultRows" },
    );
    expect(hits).toEqual([{ identifier: "CTC-1" }]);
  });

  it("the pulls list and the review facet seek read-model's indexes, never scanning reviews", () => {
    const opened = buildOpenedReplica(memoryDb());
    for (const n of [1, 2, 3]) {
      opened.write.run("INSERT INTO pull_requests (repo_id, number, head_sha, state, updated_at) VALUES ('o/r', ?, ?, 'open', ?)", n, `sha-${n}`, n);
      opened.write.run("INSERT INTO reviews (repo_id, pr_number, review_id, user_id, state, submitted_at) VALUES ('o/r', ?, ?, 'u1', 'APPROVED', ?)", n, `rv-${n}`, n);
      opened.write.run("INSERT INTO commit_statuses (repo_id, sha, context, state, updated_at) VALUES ('o/r', ?, 'ci', 'success', ?)", `sha-${n}`, n);
    }

    const list = planDetails(opened.read, recordStatements(opened.read, loadAllPage));
    expect(fullScans(list, ["pull_requests", "reviews", "commit_statuses"])).toEqual([]);
    expect(list.join("\n")).toMatch(/idx_pulls_keyset/);
    expect(list.join("\n")).toMatch(/idx_reviews_pr/);

    const facet = planDetails(opened.read, recordStatements(opened.read, reviewFacet));
    expect(fullScans(facet, ["reviews", "r", "r2"])).toEqual([]);
    expect(facet).toContainEqual(expect.stringMatching(/^SEARCH r2 USING INDEX idx_reviews_pr\b/));
    expect(reviewFacet(opened.read).map((p) => p.number)).toEqual([3, 2, 1]);
    opened.close();
  });
});
