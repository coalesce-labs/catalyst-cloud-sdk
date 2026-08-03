import { describe, it, expect, beforeAll } from "vitest";
import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type { Database } from "@sqlite.org/sqlite-wasm";
import {
  createWorkerCore,
  type WorkerCore,
} from "../../src/replica/browser/worker-core.js";
import { buildOpenedReplica } from "../../src/replica/browser/ports.js";
import type { IssueView } from "@catalyst-cloud/read-model";
import { applyMigrations, MIRROR_MIGRATIONS } from "@catalyst-cloud/schema";

/** The newest migration tag that changes row shape — the one a "behind by a column" DB is missing. */
const LAST_SHAPE_TAG = "0008_optimal_rattler";

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

  it("WIPES a pre-fence database instead of adopting it", async () => {
    // This test previously asserted the OPPOSITE — that a database with no stored identity is adopted
    // as "unknown, not foreign", to spare existing installs a re-seed at upgrade time. That reasoning
    // does not survive contact with the threat model (CTC-114 review, second round):
    //
    //   • An unstamped database is exactly the population that MIGHT belong to another cookie user —
    //     every replica written before the fence existed has no key. A missing identity cannot be
    //     validated, so it cannot be trusted.
    //   • Adopting one stamps the NEW identity over the OLD ROWS while leaving the old cursor intact,
    //     so start() reads warm, skips /snapshot, and renders the previous tenant's issues.
    //   • The cost it was avoiding is zero: `./browser` is new and UNPUBLISHED in 0.8.0, so there is
    //     no install base to spare. It was trading a real cross-tenant read for a saving that does
    //     not exist.
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    const make = () =>
      createWorkerCore(() => Promise.resolve(buildOpenedReplica(db)));
    const seeded = make();
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

    // Cold → the client takes the /snapshot path.
    expect(await upgraded.handle({ type: "getCursor" })).toBeNull();
    // And the rows are GONE, not merely unreachable — so even a FAILED re-seed cannot serve them.
    const rows = (await upgraded.handle({ type: "queryIssues" })) as IssueView[];
    expect(rows).toHaveLength(0);
  });

  it("rejects a re-open under a different identity rather than silently ignoring it", async () => {
    // The fence runs only on the FIRST open. A second open with a different identity used to be a
    // silent no-op, which left the core serving tenant A's rows to a caller that believed it had
    // opened tenant B — the same exposure as the unfenced case, reached by a different route.
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "user-a",
    });
    await expect(
      core.handle({
        type: "open",
        dbPath: ":memory:",
        directory: "unused",
        identity: "user-b",
      }),
    ).rejects.toThrow(/different identity/);
  });

  it("allows an idempotent re-open under the SAME identity (negative control)", async () => {
    const core = memoryCore();
    const open = {
      type: "open" as const,
      dbPath: ":memory:",
      directory: "unused",
      identity: "user-a",
    };
    await core.handle(open);
    await seed(core, [{ entity: "issues", row: issueRow("a") }], 42);
    await expect(core.handle(open)).resolves.toBeUndefined();
    // The re-open must not have re-run the fence and wiped the warm replica.
    expect(await core.handle({ type: "getCursor" })).toBe(42);
  });
});

describe("row-shape migration forces one re-seed — CTC-127's browser twin (CTC-114 review, KtI)", () => {
  /** The bundle minus its last column-adding migration: a replica that predates the mirror's ALTER. */
  const partialBundle = {
    ...MIRROR_MIGRATIONS,
    journal: {
      ...MIRROR_MIGRATIONS.journal,
      entries: MIRROR_MIGRATIONS.journal.entries.filter(
        (e) => e.tag !== LAST_SHAPE_TAG,
      ),
    },
  };

  /** A raw handle migrated only through `bundle`, with `cursor` persisted — a WARM replica.
   *  `bundle` is typed as the runner's own parameter: MIRROR_MIGRATIONS' journal is a readonly TUPLE
   *  of exactly N entries, so a filtered copy is not assignable to `typeof MIRROR_MIGRATIONS`. */
  type Bundle = Parameters<typeof applyMigrations>[1];
  function warmDbAt(bundle: Bundle, cursor: string): Database {
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    applyMigrations(
      {
        exec: (sql: string) => {
          db.exec(sql);
        },
        query: (sql: string) =>
          db.exec(sql, {
            rowMode: "object",
            returnValue: "resultRows",
          }) as Array<Record<string, unknown>>,
      },
      bundle,
    );
    db.exec("CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value TEXT)");
    db.exec(`INSERT INTO sync_meta (key, value) VALUES ('cursor', '${cursor}')`);
    return db;
  }

  function cursorOf(db: Database): unknown {
    const rows = db.exec("SELECT value FROM sync_meta WHERE key = 'cursor'", {
      rowMode: "object",
      returnValue: "resultRows",
    }) as Record<string, unknown>[];
    return rows[0]?.value ?? null;
  }

  it("clears the cursor when opening a warm DB behind a column-adding migration", () => {
    // The row the warm DB already holds was written before the new column existed and holds NULL for
    // it forever — deltas only carry CHANGED rows, so nothing backfills it. Dropping the cursor makes
    // the client's warm-start check read cold and take /snapshot exactly once.
    const db = warmDbAt(partialBundle, "7");
    buildOpenedReplica(db); // applies the missing migration → row shape changed
    expect(cursorOf(db)).toBeNull();
  });

  it("PRESERVES the cursor on a fully-migrated warm DB — the warm-start path stays alive", () => {
    // The mandatory negative control for the test above: without it, "clear the cursor whenever the
    // bundle contains a CREATE TABLE" passes the primary test while making every warm tab re-download
    // the whole (~100 MB) snapshot, and the warm-start path this replica exists for becomes dead code.
    //
    // What actually protects that path is the migration runner's contract — `applyMigrations` reports
    // only the tags it applied THIS call (measured: 9 on a cold DB, 0 on a re-open), so a fully
    // migrated DB evaluates the predicate over an empty list. This assertion is what pins that
    // contract: if the runner ever starts reporting all known tags, `0000_baseline`'s CREATE TABLE
    // makes the predicate true forever and this test is the thing that goes red.
    const db = warmDbAt(MIRROR_MIGRATIONS, "7");
    buildOpenedReplica(db); // nothing new to apply → no shape change
    expect(cursorOf(db)).toBe("7");
  });

  it("applies NOTHING on a re-open — the contract the test above depends on", () => {
    // Asserted directly rather than inferred, because everything above rests on it.
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    const adapter = {
      exec: (sql: string) => {
        db.exec(sql);
      },
      query: (sql: string) =>
        db.exec(sql, { rowMode: "object", returnValue: "resultRows" }) as Array<
          Record<string, unknown>
        >,
    };
    expect(applyMigrations(adapter, MIRROR_MIGRATIONS).appliedTags.length).
      toBeGreaterThan(0);
    expect(applyMigrations(adapter, MIRROR_MIGRATIONS).appliedTags).toEqual([]);
  });

  it("leaves a COLD DB cold without a spurious clear", () => {
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    buildOpenedReplica(db);
    expect(cursorOf(db)).toBeNull();
  });
});
