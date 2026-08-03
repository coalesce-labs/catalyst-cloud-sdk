// replica/seed-session.ts — the pure transaction state machine behind the batched OPFS seed (CTC-132).
//
// The streamed seed spans MULTIPLE Worker messages (seedBegin → seedBatch* → seedCommit / seedAbort),
// so — unlike the old one-shot `seed` that ran inside a single callback-scoped `transaction()` — the
// SQLite transaction must stay open ACROSS those messages. This class owns that: it opens the txn +
// truncates on `begin`, applies rows per `batch`, and sets the cursor + COMMITs on `commit` (or
// ROLLBACKs on `abort`). The Worker (db.worker.ts) is single-threaded and messages are serialized, so
// the open transaction is safe between batches.
//
// The truncate/apply/setCursor ops are INJECTED so this seam is unit-testable against a fake ReplicaDb
// (test/seed-session.test.ts); the Worker passes the real apply.ts bindings.

import type { ReplicaDb } from "./ports.js";
import type { WireChange } from "./protocol.js";

/** The three replica write primitives the seed drives, injected so tests can fake them. */
export interface SeedOps {
  truncate: (db: ReplicaDb) => void;
  apply: (db: ReplicaDb, rec: WireChange) => boolean;
  setCursor: (db: ReplicaDb, cursor: number) => void;
}

export class SeedSession {
  private open = false;

  constructor(
    private readonly db: ReplicaDb,
    private readonly ops: SeedOps,
  ) {}

  /** Open the transaction and wipe the replica (but not sync_meta) so the fresh snapshot replaces it. */
  begin(): void {
    this.db.run("BEGIN");
    // Mark open BEFORE truncate: once BEGIN succeeds the transaction is live, so abort() must be able
    // to ROLLBACK it even if truncate throws mid-begin (else the txn leaks). CTC-132 review finding A.
    this.open = true;
    this.ops.truncate(this.db);
  }

  /** Apply one bounded batch of rows into the open transaction. */
  batch(rows: WireChange[]): void {
    if (!this.open) throw new Error("seedBatch before seedBegin");
    for (const rec of rows) this.ops.apply(this.db, rec);
  }

  /** Persist the terminal cursor, then COMMIT the whole seed atomically. Returns the cursor. */
  commit(cursor: number): number {
    if (!this.open) throw new Error("seedCommit before seedBegin");
    this.ops.setCursor(this.db, cursor);
    this.db.run("COMMIT");
    this.open = false;
    return cursor;
  }

  /** Roll back a partial seed on error — leaves the prior cursor (sync_meta) intact. Idempotent. */
  abort(): void {
    if (this.open) {
      try {
        this.db.run("ROLLBACK");
      } catch {
        // ROLLBACK can throw if the txn already aborted — swallow so the original error surfaces.
      }
      this.open = false;
    }
  }
}
