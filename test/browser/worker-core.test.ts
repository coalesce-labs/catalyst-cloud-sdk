import { describe, it, expect, beforeAll } from "vitest";
import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type { Database } from "@sqlite.org/sqlite-wasm";
import {
  createWorkerCore,
  type WorkerCore,
} from "../../src/replica/browser/worker-core.js";
import { buildOpenedReplica } from "../../src/replica/browser/ports.js";
import type { IssueView } from "@catalyst-cloud/read-model";

// CTC-114 / CTC-131 — the browser replica's ENTIRE dispatch lifecycle, exercised against the REAL
// sqlite-wasm engine and the REAL shared migrations/apply/read-model, in plain node. This is the exact
// code the production Worker runs (db.worker.ts binds the same createWorkerCore to the OPFS opener) —
// the only thing not covered here is the OPFS SAHPool acquisition itself, which has no node analogue.
//
// Before this seam existed the whole path (seed atomicity, delta apply, mid-seed read deferral, view
// output over wasm) had ZERO automated coverage — CTC-131's standing complaint.

let sqlite3: Awaited<ReturnType<typeof sqlite3InitModule>>;

beforeAll(async () => {
  sqlite3 = await sqlite3InitModule();
});

/** A core bound to a fresh :memory: DB — the test twin of db.worker.ts's OPFS binding. */
function memoryCore(): WorkerCore {
  return createWorkerCore(() => {
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    return Promise.resolve(buildOpenedReplica(db));
  });
}

/** A wire issue row with the columns the list view SELECTs; unmentioned columns stay NULL. */
function issueRow(
  id: string,
  over: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    id,
    identifier: `CTC-${id}`,
    title: `issue ${id}`,
    state: "Todo",
    updated_at: 100,
    ...over,
  };
}

async function seed(
  core: WorkerCore,
  rows: { entity: string; row: Record<string, unknown> }[],
  cursor = 10,
) {
  await core.handle({ type: "seedBegin" });
  await core.handle({
    type: "seedBatch",
    rows: rows.map((r) => ({
      entity: r.entity,
      op: "upsert" as const,
      row: r.row,
    })),
  });
  return core.handle({ type: "seedCommit", cursor });
}

describe("worker-core over real sqlite-wasm (CTC-114)", () => {
  it("open → seed → queryIssues serves the read-model view, labels joined", async () => {
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });

    const cursor = await seed(core, [
      { entity: "issues", row: issueRow("a", { updated_at: 300 }) },
      { entity: "issues", row: issueRow("b", { updated_at: 200 }) },
      { entity: "labels", row: { id: "l1", name: "bug", color: "#f00" } },
      { entity: "issue_labels", row: { issue_id: "a", label_id: "l1" } },
    ]);
    expect(cursor).toBe(10);

    const rows = (await core.handle({ type: "queryIssues" })) as IssueView[];
    expect(rows.map((r) => r.id)).toEqual(["a", "b"]); // newest-first by updated_at
    expect(rows[0]?.labels).toEqual([{ id: "l1", name: "bug", color: "#f00" }]);
    expect(rows[1]?.labels).toEqual([]);

    expect(await core.handle({ type: "getCursor" })).toBe(10);
  });

  it("applyChanges upserts + deletes in ONE transaction and advances to the max seq SEEN", async () => {
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    await seed(core, [{ entity: "issues", row: issueRow("a") }]);

    const result = (await core.handle({
      type: "applyChanges",
      changes: [
        {
          seq: 11,
          entity: "issues",
          op: "upsert",
          row: issueRow("b", { updated_at: 400 }),
          entityId: "b",
        },
        // A STALE upsert (updated_at behind the stored row) is rejected by the apply guard — but its
        // seq must still advance the cursor, or the client re-fetches this window forever.
        {
          seq: 12,
          entity: "issues",
          op: "upsert",
          row: issueRow("a", { updated_at: 1 }),
          entityId: "a",
        },
        { seq: 13, entity: "issues", op: "delete", row: {}, entityId: "a" },
      ],
    })) as { applied: number; cursor: number };

    expect(result.cursor).toBe(13);
    expect(result.applied).toBe(2); // b upserted + a deleted; the stale upsert applied nothing

    const rows = (await core.handle({ type: "queryIssues" })) as IssueView[];
    expect(rows.map((r) => r.id)).toEqual(["b"]);
    expect(await core.handle({ type: "getCursor" })).toBe(13);
  });

  it("defers a read that arrives MID-SEED until commit, then serves the fresh snapshot", async () => {
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    await seed(core, [{ entity: "issues", row: issueRow("old") }], 5);

    await core.handle({ type: "seedBegin" }); // truncates inside the open txn
    let settled = false;
    const read = core.handle({ type: "queryIssues" }).then((rows) => {
      settled = true;
      return rows as IssueView[];
    });
    await new Promise((r) => setTimeout(r, 10));
    expect(settled).toBe(false); // the read is GATED — it must not see the truncated mid-seed state

    await core.handle({
      type: "seedBatch",
      rows: [{ entity: "issues", op: "upsert", row: issueRow("new") }],
    });
    await core.handle({ type: "seedCommit", cursor: 6 });

    expect((await read).map((r) => r.id)).toEqual(["new"]);
  });

  it("seedAbort ROLLBACKs to the prior complete snapshot and keeps the prior cursor", async () => {
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    await seed(core, [{ entity: "issues", row: issueRow("keep") }], 7);

    await core.handle({ type: "seedBegin" });
    await core.handle({
      type: "seedBatch",
      rows: [{ entity: "issues", op: "upsert", row: issueRow("discard") }],
    });
    await core.handle({ type: "seedAbort" });

    const rows = (await core.handle({ type: "queryIssues" })) as IssueView[];
    expect(rows.map((r) => r.id)).toEqual(["keep"]);
    expect(await core.handle({ type: "getCursor" })).toBe(7);
  });

  it("rejects a second concurrent seed instead of stranding the open transaction", async () => {
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    await core.handle({ type: "seedBegin" });
    await expect(core.handle({ type: "seedBegin" })).rejects.toThrow(
      /already in progress/,
    );
    // …and the FIRST seed is still intact and committable.
    await core.handle({
      type: "seedBatch",
      rows: [{ entity: "issues", op: "upsert", row: issueRow("a") }],
    });
    expect(await core.handle({ type: "seedCommit", cursor: 1 })).toBe(1);
  });

  it("close aborts an open seed, releases gated reads, and makes further requests fail loudly", async () => {
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    await seed(core, [{ entity: "issues", row: issueRow("a") }]);

    await core.handle({ type: "seedBegin" });
    const gated = core.handle({ type: "queryIssues" });
    await core.handle({ type: "close" });
    // The gated read was released by close's settle() — it must not hang forever. It ran against a
    // closed DB, so either outcome (rows from before close raced it, or a loud error) is acceptable;
    // the contract under test is "settles".
    await gated.catch(() => undefined);

    await expect(core.handle({ type: "queryIssues" })).rejects.toThrow(
      /not open/,
    );
  });

  it("open is idempotent — a second open request reuses the existing replica", async () => {
    let opens = 0;
    const core = createWorkerCore(() => {
      opens++;
      const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
      return Promise.resolve(buildOpenedReplica(db));
    });
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    expect(opens).toBe(1);
  });
});

// CTC-114 review (P1) — the TENANT FENCE. dbPath/directory default to constants, so every tenant on an
// origin opens the SAME persistent OPFS database, and the client's warm start skips /snapshot whenever
// the cursor is non-null. Before this fence, signing in as a different user left the previous tenant's
// rows queryable — and unremovable, because deltas only ever carry changes.
describe("worker-core — tenant fence on open (CTC-114 review)", () => {
  /** Two cores over ONE db handle: the twin of two sessions against the same persistent OPFS file. */
  function sharedDb() {
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    return () =>
      createWorkerCore(() => Promise.resolve(buildOpenedReplica(db)));
  }

  it("wipes the replica and clears the cursor when a DIFFERENT identity opens it", async () => {
    const core = sharedDb();
    const first = core();
    await first.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "user-a",
    });
    await seed(first, [{ entity: "issues", row: issueRow("a") }], 42);
    expect(await first.handle({ type: "getCursor" })).toBe(42);

    // A different signed-in user, same origin, same OPFS file.
    const second = core();
    await second.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "user-b",
    });

    // Cold: the client's `persisted == null` branch is what forces the /snapshot re-seed.
    expect(await second.handle({ type: "getCursor" })).toBeNull();
    // And the previous tenant's rows are GONE, not merely unreachable.
    const rows = (await second.handle({ type: "queryIssues" })) as IssueView[];
    expect(rows).toHaveLength(0);
  });

  it("preserves the warm replica when the SAME identity reopens it", async () => {
    // The negative control: if the fence wiped unconditionally, every reload would re-seed and the
    // warm-start path this replica exists for would be dead code.
    const core = sharedDb();
    const first = core();
    await first.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "user-a",
    });
    await seed(first, [{ entity: "issues", row: issueRow("a") }], 42);

    const second = core();
    await second.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "user-a",
    });

    expect(await second.handle({ type: "getCursor" })).toBe(42);
    const rows = (await second.handle({ type: "queryIssues" })) as IssueView[];
    expect(rows).toHaveLength(1);
  });

  it("adopts a pre-fence database without wiping it", async () => {
    // A replica seeded by an older build has no stored identity. Treat that as "unknown, not foreign":
    // wiping would force a needless full re-seed on every existing install at upgrade time.
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    const make = () =>
      createWorkerCore(() => Promise.resolve(buildOpenedReplica(db)));
    const seeded = make();
    // Seed WITHOUT ever stamping an identity, by seeding before any open stamps one.
    await seeded.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "x",
    });
    await seed(seeded, [{ entity: "issues", row: issueRow("a") }], 7);
    // Simulate the pre-fence state by removing the stamp.
    db.exec("DELETE FROM sync_meta WHERE key = 'identity'");

    const upgraded = make();
    await upgraded.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "x",
    });
    expect(await upgraded.handle({ type: "getCursor" })).toBe(7);
  });
});
