// CTC-337 — a corrupt OPFS database must heal itself, and must NOT heal the wrong thing.
//
// Reported from a live browser: the replica pill read "Replica error", and the cause was
// `SQLITE_CORRUPT: database disk image is malformed`. Corruption survives reloads, so `start()` threw
// on EVERY subsequent boot — the replica was bricked permanently, and the only cure was knowing to
// clear origin storage by hand. A cache that is 100% derived from the server should never be able to
// do that: re-seeding is its normal cold-start path.
//
// The recovery is easy. The PREDICATE is the dangerous part, and that is where these tests are
// weighted: a false positive authorizes wiping a database that a second live tab is using.
import { describe, expect, it, vi } from "vitest";
import {
  isCorruptDatabaseError,
  openWithCorruptionRecovery,
  type RecoverablePool,
} from "../../src/replica/browser/sqlite-db.js";

describe("isCorruptDatabaseError — the allowlist that authorizes a wipe", () => {
  it("matches real corruption, including the exact string from the field report", () => {
    // Verbatim from the browser that prompted this.
    expect(
      isCorruptDatabaseError(
        new Error("SQLITE_CORRUPT: sqlite3 result code 11: database disk image is malformed"),
      ),
    ).toBe(true);
    expect(isCorruptDatabaseError(new Error("SQLITE_NOTADB: file is not a database"))).toBe(true);
    expect(isCorruptDatabaseError(new Error("malformed database schema"))).toBe(true);
    expect(isCorruptDatabaseError("database disk image is malformed")).toBe(true);
  });

  it("⛔ does NOT match tab contention — wiping there would destroy another tab's live database", () => {
    // The single most important case in this file. SAHPool throws this when a second tab (or a
    // not-yet-released worker from a just-prior reload) still holds the SyncAccessHandles. It is
    // transient and the caller retries; treating it as corruption would be silent data loss for
    // whoever holds the lock.
    const contention = new Error("NoModificationAllowedError: handle is already open");
    contention.name = "NoModificationAllowedError";
    expect(isCorruptDatabaseError(contention)).toBe(false);

    const invalid = new Error("InvalidStateError: the pool is paused");
    invalid.name = "InvalidStateError";
    expect(isCorruptDatabaseError(invalid)).toBe(false);
  });

  it("does not match ordinary failures that a wipe would not fix", () => {
    for (const e of [
      new Error("QuotaExceededError: storage full"),
      new Error("SQLITE_BUSY: database is locked"),
      new Error("NetworkError: failed to fetch"),
      new Error(""),
      null,
      undefined,
      42,
      {},
    ]) {
      expect(isCorruptDatabaseError(e), `should not wipe for ${String(e)}`).toBe(false);
    }
  });
});

/** A pool whose db constructor throws `throwsWith` until the files are wiped. */
function fakePool(throwsWith: unknown) {
  const state = { wiped: 0, constructed: 0 };
  const pool: RecoverablePool = {
    OpfsSAHPoolDb: class {
      constructor(_p: string) {
        state.constructed += 1;
        if (state.wiped === 0) throw throwsWith;
      }
    },
    wipeFiles: async () => {
      state.wiped += 1;
    },
  };
  return { pool, state };
}

const build = (db: unknown) => ({ opened: db }) as never;

describe("openWithCorruptionRecovery", () => {
  it("wipes and re-opens ONCE when the database is corrupt", async () => {
    const { pool, state } = fakePool(
      new Error("SQLITE_CORRUPT: sqlite3 result code 11: database disk image is malformed"),
    );
    const log = vi.fn();

    await openWithCorruptionRecovery(pool, "/catalyst-replica.sqlite3", build, log);

    expect(state.wiped).toBe(1);
    expect(state.constructed).toBe(2); // failed open, then the fresh one
    // Loud: the RATE of corruption is a signal, and healing silently would hide it entirely.
    expect(log).toHaveBeenCalledTimes(1);
    expect(String(log.mock.calls[0]?.[0])).toMatch(/corrupt/i);
  });

  it("⛔ RETHROWS tab contention without wiping — the safety property, end to end", async () => {
    const contention = new Error("NoModificationAllowedError: handle is already open");
    contention.name = "NoModificationAllowedError";
    const { pool, state } = fakePool(contention);

    await expect(
      openWithCorruptionRecovery(pool, "/catalyst-replica.sqlite3", build, vi.fn()),
    ).rejects.toThrow(/NoModificationAllowedError/);

    // The assertion that matters: another tab's data is still there.
    expect(state.wiped).toBe(0);
  });

  it("does not loop — a second failure after the wipe propagates", async () => {
    // A broken environment (no quota, VFS refusing to register) is not corruption we can clear, and
    // retrying forever would convert a hard failure into a hang.
    const corrupt = new Error("SQLITE_CORRUPT: database disk image is malformed");
    let constructed = 0;
    const pool: RecoverablePool = {
      OpfsSAHPoolDb: class {
        constructor(_p: string) {
          constructed += 1;
          throw corrupt; // still broken even after the wipe
        }
      },
      wipeFiles: async () => {},
    };

    await expect(
      openWithCorruptionRecovery(pool, "/x.sqlite3", build, vi.fn()),
    ).rejects.toThrow(/SQLITE_CORRUPT/);
    expect(constructed).toBe(2); // exactly one retry, not a loop
  });

  it("a healthy open neither wipes nor logs", async () => {
    const state = { wiped: 0 };
    const pool: RecoverablePool = {
      OpfsSAHPoolDb: class {
        constructor(_p: string) {}
      },
      wipeFiles: async () => {
        state.wiped += 1;
      },
    };
    const log = vi.fn();

    await openWithCorruptionRecovery(pool, "/x.sqlite3", build, log);

    expect(state.wiped).toBe(0);
    expect(log).not.toHaveBeenCalled();
  });
});
