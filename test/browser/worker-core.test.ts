import { describe, it, expect, beforeAll } from "vitest";
import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type { Database } from "@sqlite.org/sqlite-wasm";
import {
  createWorkerCore,
  type WorkerCore,
} from "../../src/replica/browser/worker-core.js";
import { buildOpenedReplica } from "../../src/replica/browser/ports.js";
import type {
  IssueFacetCount,
  IssueScope,
  IssueView,
  KeysetPage,
} from "@catalyst-cloud/read-model";
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
        { seq: 13 },
        { seq: 14, entity: "issues", op: "delete", row: {}, entityId: "a" },
      ],
    })) as { applied: number; cursor: number };

    expect(result.cursor).toBe(14);
    expect(result.applied).toBe(2); // b upserted + a deleted; the stale upsert applied nothing

    const rows = (await core.handle({ type: "queryIssues" })) as IssueView[];
    expect(rows.map((r) => r.id)).toEqual(["b"]);
    expect(await core.handle({ type: "getCursor" })).toBe(14);
  });

  // CTC-114 review round 4 (P1). The transport DELIBERATELY forwards duplicate / out-of-order frames
  // at `seq <= deliveredSeq` (live-sync-client: "the consumer's stale-guard already dedups it"). The
  // shared apply guard only covers upserts — `ON CONFLICT ... WHERE excluded.updated_at > …` — while a
  // delete keys on the PK alone. So a replayed OLD delete used to remove a row a NEWER change had
  // created, and because the cursor still held the newer seq, a reconnect resumed ABOVE the damage:
  // nothing short of a full re-seed ever repaired it.
  it("a REPLAYED old delete does not remove a row a newer change created", async () => {
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    await seed(core, [], 10);

    // seq 11 deletes "x"; seq 12 re-creates it. After this the DB legitimately HAS "x".
    await core.handle({
      type: "applyChanges",
      changes: [
        { seq: 11, entity: "issues", op: "delete", row: {}, entityId: "x" },
        {
          seq: 12,
          entity: "issues",
          op: "upsert",
          row: issueRow("x", { updated_at: 500 }),
          entityId: "x",
        },
      ],
    });
    expect(
      ((await core.handle({ type: "queryIssues" })) as IssueView[]).map(
        (r) => r.id,
      ),
    ).toEqual(["x"]);

    // The socket redelivers seq 11. It is at/below the cursor, so it is already reflected in the DB.
    const replay = (await core.handle({
      type: "applyChanges",
      changes: [
        { seq: 11, entity: "issues", op: "delete", row: {}, entityId: "x" },
      ],
    })) as { applied: number; cursor: number };

    // THE OUTCOME: the row survives, and the cursor does not regress to the replayed seq.
    expect(
      ((await core.handle({ type: "queryIssues" })) as IssueView[]).map(
        (r) => r.id,
      ),
    ).toEqual(["x"]);
    expect(replay.applied).toBe(0);
    expect(replay.cursor).toBe(12);
    expect(await core.handle({ type: "getCursor" })).toBe(12);
  });

  // Same hazard, one batch: the guard compares against the ROLLING max, not just the entry cursor, so
  // a duplicate that trails its original INSIDE a single drain is caught too.
  it("a duplicate delete repeated WITHIN one batch is skipped", async () => {
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    await seed(core, [], 10);

    const res = (await core.handle({
      type: "applyChanges",
      changes: [
        { seq: 11, entity: "issues", op: "delete", row: {}, entityId: "y" },
        {
          seq: 12,
          entity: "issues",
          op: "upsert",
          row: issueRow("y", { updated_at: 500 }),
          entityId: "y",
        },
        { seq: 11, entity: "issues", op: "delete", row: {}, entityId: "y" },
      ],
    })) as { applied: number; cursor: number };

    expect(
      ((await core.handle({ type: "queryIssues" })) as IssueView[]).map(
        (r) => r.id,
      ),
    ).toEqual(["y"]);
    expect(res.cursor).toBe(12);
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
  /** The bundle truncated BEFORE its column-adding migration: a replica that predates the mirror's
   *  ALTER. Truncated, not filtered — a replica that predates 0008 predates every later migration too,
   *  and a later DATA migration (0040 re-stamps `issues.state` by `issues.state_id`) cannot run on a
   *  DB that skipped the ALTER which added the column it reads. */
  const shapeIdx = MIRROR_MIGRATIONS.journal.entries.findIndex((e) => e.tag === LAST_SHAPE_TAG);
  const partialBundle = {
    ...MIRROR_MIGRATIONS,
    journal: {
      ...MIRROR_MIGRATIONS.journal,
      entries: MIRROR_MIGRATIONS.journal.entries.slice(0, shapeIdx),
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

// CTC-6150 — the two scoped reads the web needs to answer filtered ticket lists and group counts
// locally: queryIssuesScoped (buildIssuesViewKeyset) and queryIssueFacetCounts (buildIssueFacetCounts).
// Same production dispatch over a :memory: sqlite-wasm DB as the tests above.
describe("scoped reads (CTC-6150)", () => {
  type SeedRow = { entity: string; row: Record<string, unknown> };

  async function openCore(): Promise<WorkerCore> {
    const core = memoryCore();
    await core.handle({
      type: "open",
      dbPath: ":memory:",
      directory: "unused",
      identity: "test-identity",
    });
    return core;
  }

  /**
   * Team CTC (`ctc` issues) next to another team (`other` issues). The first `noProject` CTC issues
   * carry no project; the rest are spread over `projects` projects, project N holding the CTC issues
   * whose index lands on it. Counts per project are therefore uneven when `ctc - noProject` does not
   * divide by `projects`, which exercises the facet's largest-first order.
   */
  function teamFixtureRows(o: {
    ctc: number;
    other: number;
    projects: number;
    noProject: number;
  }): SeedRow[] {
    const rows: SeedRow[] = [];
    for (let p = 1; p <= o.projects; p++) {
      rows.push({ entity: "projects", row: { id: `p${p}`, name: `Project ${p}` } });
    }
    for (let i = 1; i <= o.ctc; i++) {
      const projectId =
        i <= o.noProject ? null : `p${((i - o.noProject - 1) % o.projects) + 1}`;
      rows.push({
        entity: "issues",
        row: issueRow(`c${i}`, { team_id: "team-ctc", project_id: projectId, updated_at: 1000 + i }),
      });
    }
    for (let i = 1; i <= o.other; i++) {
      // The other team's issues sit in the same projects, so a facet that ignored the scope would show.
      rows.push({
        entity: "issues",
        row: issueRow(`o${i}`, {
          team_id: "team-other",
          project_id: `p${((i - 1) % o.projects) + 1}`,
          updated_at: 5000 + i,
        }),
      });
    }
    return rows;
  }

  async function seedTeamFixture(
    core: WorkerCore,
    o: { ctc: number; other: number; projects: number; noProject: number },
  ): Promise<void> {
    await seed(core, teamFixtureRows(o));
  }

  const ctcScope = { teamId: "team-ctc", labelId: "l-mirror" } satisfies IssueScope;

  it("queryIssuesScoped returns team CTC's issues carrying the label, newest first", async () => {
    const core = await openCore();
    await seed(core, [
      { entity: "labels", row: { id: "l-mirror", name: "mirror-do" } },
      { entity: "labels", row: { id: "l-other", name: "other" } },
      // Three CTC issues carry the label, at three updated_at values.
      { entity: "issues", row: issueRow("c1", { team_id: "team-ctc", updated_at: 100 }) },
      { entity: "issues", row: issueRow("c2", { team_id: "team-ctc", updated_at: 200 }) },
      { entity: "issues", row: issueRow("c3", { team_id: "team-ctc", updated_at: 300 }) },
      { entity: "issue_labels", row: { issue_id: "c1", label_id: "l-mirror" } },
      { entity: "issue_labels", row: { issue_id: "c2", label_id: "l-mirror" } },
      { entity: "issue_labels", row: { issue_id: "c3", label_id: "l-mirror" } },
      // Newer than all of them, but: another team's issue with the label,
      { entity: "issues", row: issueRow("o1", { team_id: "team-other", updated_at: 400 }) },
      { entity: "issue_labels", row: { issue_id: "o1", label_id: "l-mirror" } },
      // a CTC issue with a different label,
      { entity: "issues", row: issueRow("c4", { team_id: "team-ctc", updated_at: 500 }) },
      { entity: "issue_labels", row: { issue_id: "c4", label_id: "l-other" } },
      // and a removed CTC issue that carries the label.
      { entity: "issues", row: issueRow("c5", { team_id: "team-ctc", updated_at: 600, removed_at: 1 }) },
      { entity: "issue_labels", row: { issue_id: "c5", label_id: "l-mirror" } },
    ]);

    const page = (await core.handle({
      type: "queryIssuesScoped",
      scope: ctcScope,
      limit: 2,
    })) as KeysetPage<IssueView>;
    expect(page.rows.map((r) => r.id)).toEqual(["c3", "c2"]); // updated_at DESC, CTC and label only
    expect(page.rows[0]?.labels).toEqual([{ id: "l-mirror", name: "mirror-do", color: null }]);
    expect(page.nextCursor).not.toBeNull();

    const next = (await core.handle({
      type: "queryIssuesScoped",
      scope: ctcScope,
      limit: 2,
      after: page.nextCursor,
    })) as KeysetPage<IssueView>;
    expect(next.rows.map((r) => r.id)).toEqual(["c1"]);
    expect(next.nextCursor).toBeNull();
  });

  it("queryIssuesScoped with no limit, cursor or sort returns the whole scope in the default order", async () => {
    const core = await openCore();
    await seedTeamFixture(core, { ctc: 5, other: 3, projects: 2, noProject: 1 });

    const page = (await core.handle({
      type: "queryIssuesScoped",
      scope: { teamId: "team-ctc" },
    })) as KeysetPage<IssueView>;
    expect(page.rows.map((r) => r.id)).toEqual(["c5", "c4", "c3", "c2", "c1"]);
    expect(page.nextCursor).toBeNull();
  });

  it("queryIssuesScoped honours the sort option", async () => {
    const core = await openCore();
    await seed(core, [
      { entity: "issues", row: issueRow("low", { team_id: "team-ctc", priority: 4, updated_at: 300 }) },
      { entity: "issues", row: issueRow("urgent", { team_id: "team-ctc", priority: 1, updated_at: 100 }) },
    ]);

    const byUpdated = (await core.handle({
      type: "queryIssuesScoped",
      scope: { teamId: "team-ctc" },
    })) as KeysetPage<IssueView>;
    expect(byUpdated.rows.map((r) => r.id)).toEqual(["low", "urgent"]);

    const byPriority = (await core.handle({
      type: "queryIssuesScoped",
      scope: { teamId: "team-ctc" },
      sort: "priority",
    })) as KeysetPage<IssueView>;
    expect(byPriority.rows.map((r) => r.id)).toEqual(["urgent", "low"]);
  });

  it("queryIssueFacetCounts returns each project's count and the no-project group last", async () => {
    const core = await openCore();
    // 12 projects and 300 CTC issues with no project, the ticket's scenario 2.
    await seedTeamFixture(core, { ctc: 300 + 78, other: 40, projects: 12, noProject: 300 });

    const counts = (await core.handle({
      type: "queryIssueFacetCounts",
      scope: { teamId: "team-ctc" },
      dimension: "project",
    })) as IssueFacetCount[];

    expect(counts).toHaveLength(13); // 12 projects and the "No project" group
    expect(counts.at(-1)).toEqual({ key: null, name: null, count: 300 });
    // 78 project issues over 12 projects: six projects of 7 and six of 6, never the other team's issues.
    const projectCounts = counts.slice(0, 12);
    expect(projectCounts.reduce((sum, c) => sum + c.count, 0)).toBe(78);
    expect(projectCounts.map((c) => c.count)).toEqual([7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6]);
    expect(projectCounts.every((c) => typeof c.key === "string" && c.name === `Project ${c.key?.toString().slice(1)}`)).toBe(true);
  });

  it("the facet's null group count equals queryIssuesScoped's total for { projectId: null }", async () => {
    const core = await openCore();
    await seedTeamFixture(core, { ctc: 130, other: 10, projects: 3, noProject: 120 });

    const counts = (await core.handle({
      type: "queryIssueFacetCounts",
      scope: { teamId: "team-ctc" },
      dimension: "project",
    })) as IssueFacetCount[];
    const none = counts.find((c) => c.key === null);

    // Walk the list by cursor and count what comes back: the group opens as exactly its count.
    let total = 0;
    let after: KeysetPage<IssueView>["nextCursor"] = null;
    do {
      const page = (await core.handle({
        type: "queryIssuesScoped",
        scope: { teamId: "team-ctc", projectId: null },
        limit: 50,
        after,
      })) as KeysetPage<IssueView>;
      total += page.rows.length;
      after = page.nextCursor;
    } while (after !== null);

    expect(none?.count).toBe(120);
    expect(total).toBe(none?.count);
  });

  it("an omitted projectId narrows nothing, while null means no project", async () => {
    const core = await openCore();
    await seedTeamFixture(core, { ctc: 6, other: 0, projects: 2, noProject: 2 });

    const all = (await core.handle({
      type: "queryIssuesScoped",
      scope: { teamId: "team-ctc" },
    })) as KeysetPage<IssueView>;
    const none = (await core.handle({
      type: "queryIssuesScoped",
      scope: { teamId: "team-ctc", projectId: null },
    })) as KeysetPage<IssueView>;
    expect(all.rows).toHaveLength(6);
    expect(none.rows.map((r) => r.id).sort()).toEqual(["c1", "c2"]);
  });

  it("an unknown facet dimension rejects", async () => {
    const core = await openCore();
    await seed(core, []);
    await expect(
      core.handle({
        type: "queryIssueFacetCounts",
        scope: { teamId: "team-ctc" },
        // A caller outside the type system (JS, or a newer web build) can send any string.
        dimension: "nonsense" as unknown as "project",
      }),
    ).rejects.toThrow(/unknown facet dimension/);
  });

  it("a scope carrying waitingOn.excludeSql rejects, for both ops", async () => {
    const core = await openCore();
    await seed(core, [{ entity: "issues", row: issueRow("a", { team_id: "team-ctc" }) }]);
    const scope: IssueScope = {
      teamId: "team-ctc",
      waitingOn: { linearUserId: "u1", mode: "all", excludeSql: "1 = 1" },
    };

    await expect(core.handle({ type: "queryIssuesScoped", scope })).rejects.toThrow(
      /excludeSql is not accepted by the browser replica/,
    );
    await expect(
      core.handle({ type: "queryIssueFacetCounts", scope, dimension: "project" }),
    ).rejects.toThrow(/excludeSql is not accepted by the browser replica/);
  });

  it("a waitingOn scope without excludeSql is accepted", async () => {
    const core = await openCore();
    await seed(core, [{ entity: "issues", row: issueRow("a", { team_id: "team-ctc" }) }]);
    for (const excludeSql of [undefined, null, ""]) {
      const page = (await core.handle({
        type: "queryIssuesScoped",
        scope: { teamId: "team-ctc", waitingOn: { linearUserId: "u1", mode: "all", excludeSql } },
      })) as KeysetPage<IssueView>;
      expect(page.rows).toEqual([]); // nothing is waiting on u1, and nothing threw
    }
  });

  it("scoped reads defer while a seed is open, then answer from the fresh snapshot", async () => {
    const core = await openCore();
    await seed(core, [{ entity: "issues", row: issueRow("old", { team_id: "team-ctc" }) }], 5);

    await core.handle({ type: "seedBegin" });
    let listSettled = false;
    let facetSettled = false;
    const list = core
      .handle({ type: "queryIssuesScoped", scope: { teamId: "team-ctc" } })
      .then((page) => {
        listSettled = true;
        return page as KeysetPage<IssueView>;
      });
    const facets = core
      .handle({ type: "queryIssueFacetCounts", scope: { teamId: "team-ctc" }, dimension: "project" })
      .then((counts) => {
        facetSettled = true;
        return counts as IssueFacetCount[];
      });
    await new Promise((r) => setTimeout(r, 10));
    expect(listSettled).toBe(false); // GATED: neither read may see the truncated mid-seed state
    expect(facetSettled).toBe(false);

    await core.handle({
      type: "seedBatch",
      rows: [{ entity: "issues", op: "upsert", row: issueRow("new", { team_id: "team-ctc" }) }],
    });
    await core.handle({ type: "seedCommit", cursor: 6 });

    expect((await list).rows.map((r) => r.id)).toEqual(["new"]);
    expect(await facets).toEqual([{ key: null, name: null, count: 1 }]);
  });

  it("scoped reads before open reject with 'not open'", async () => {
    const core = memoryCore();
    await expect(
      core.handle({ type: "queryIssuesScoped", scope: { teamId: "team-ctc" } }),
    ).rejects.toThrow(/not open/);
    await expect(
      core.handle({ type: "queryIssueFacetCounts", scope: { teamId: "team-ctc" }, dimension: "project" }),
    ).rejects.toThrow(/not open/);
  });
});
