import { describe, expect, it, vi } from "vitest";
import { SeedSession } from "../../src/replica/browser/seed-session.js";

// A fake ReplicaDb capturing raw SQL (BEGIN/COMMIT/ROLLBACK) and delegating apply/cursor to spies.
function fakeDb() {
  const sql: string[] = [];
  return {
    calls: sql,
    run: (s: string) => {
      sql.push(s);
      return 0;
    },
    get: () => undefined,
    transaction: <T>(fn: () => T) => fn(),
  };
}

describe("SeedSession", () => {
  it("begin truncates and opens a transaction", () => {
    const db = fakeDb();
    const truncate = vi.fn();
    const s = new SeedSession(db as never, { truncate, apply: vi.fn(), setCursor: vi.fn() });
    s.begin();
    expect(db.calls).toContain("BEGIN");
    expect(truncate).toHaveBeenCalledOnce();
  });

  it("batch applies each row via the injected apply fn", () => {
    const db = fakeDb();
    const apply = vi.fn();
    const s = new SeedSession(db as never, { truncate: vi.fn(), apply, setCursor: vi.fn() });
    s.begin();
    s.batch([
      { entity: "issue", op: "upsert", row: { id: "1" } },
      { entity: "issue", op: "upsert", row: { id: "2" } },
    ]);
    expect(apply).toHaveBeenCalledTimes(2);
  });

  it("commit sets the cursor then COMMITs", () => {
    const db = fakeDb();
    const setCursor = vi.fn();
    const s = new SeedSession(db as never, { truncate: vi.fn(), apply: vi.fn(), setCursor });
    s.begin();
    s.commit(99);
    expect(setCursor).toHaveBeenCalledWith(db, 99);
    expect(db.calls[db.calls.length - 1]).toBe("COMMIT");
  });

  it("abort rolls back", () => {
    const db = fakeDb();
    const s = new SeedSession(db as never, {
      truncate: vi.fn(),
      apply: vi.fn(),
      setCursor: vi.fn(),
    });
    s.begin();
    s.abort();
    expect(db.calls).toContain("ROLLBACK");
  });

  it("abort rolls back even when truncate throws mid-begin (open set on BEGIN)", () => {
    const db = fakeDb();
    const truncate = vi.fn(() => {
      throw new Error("truncate boom");
    });
    const s = new SeedSession(db as never, { truncate, apply: vi.fn(), setCursor: vi.fn() });
    expect(() => s.begin()).toThrow(/truncate boom/);
    // BEGIN was issued, so the txn is live — abort must ROLLBACK it (CTC-132 review finding A).
    s.abort();
    expect(db.calls).toContain("BEGIN");
    expect(db.calls).toContain("ROLLBACK");
  });

  it("throws if batch/commit called before begin", () => {
    const db = fakeDb();
    const s = new SeedSession(db as never, {
      truncate: vi.fn(),
      apply: vi.fn(),
      setCursor: vi.fn(),
    });
    expect(() => s.batch([])).toThrow(/begin/i);
    expect(() => s.commit(1)).toThrow(/begin/i);
  });

  it("abort is idempotent when never begun (no ROLLBACK, no throw)", () => {
    // CTC-132 verify: guard the `if (this.open)` branch — abort() on a session that
    // never opened must be a silent no-op, never a stray ROLLBACK on no transaction.
    const db = fakeDb();
    const s = new SeedSession(db as never, {
      truncate: vi.fn(),
      apply: vi.fn(),
      setCursor: vi.fn(),
    });
    expect(() => s.abort()).not.toThrow();
    expect(db.calls).not.toContain("ROLLBACK");
  });

  it("abort swallows a ROLLBACK error and still clears the open flag", () => {
    // CTC-132 verify: the inner `catch {}` in abort() must swallow a throwing ROLLBACK
    // (SQLite auto-rolled-back the txn) so the ORIGINAL error surfaces — and a second
    // abort must not re-issue ROLLBACK (open cleared).
    const calls: string[] = [];
    const db = {
      calls,
      run: (s: string) => {
        calls.push(s);
        if (s === "ROLLBACK") throw new Error("no transaction is active");
        return 0;
      },
      get: () => undefined,
      transaction: <T>(fn: () => T) => fn(),
    };
    const s = new SeedSession(db as never, {
      truncate: vi.fn(),
      apply: vi.fn(),
      setCursor: vi.fn(),
    });
    s.begin();
    expect(() => s.abort()).not.toThrow();
    expect(calls).toContain("ROLLBACK");
    s.abort(); // second abort is a no-op — open was cleared despite the throw
    expect(calls.filter((c) => c === "ROLLBACK")).toHaveLength(1);
  });
});
