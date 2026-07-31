// replica/seed-read-gate.ts — the Worker's mid-seed READ gate (CTC-132 review finding).
//
// The batched streaming seed (seedBegin → seedBatch* → seedCommit/seedAbort) holds ONE SQLite
// transaction open across many Worker messages, and the replica's read + write adapters share the same
// sqlite-wasm handle. So a query RPC (queryIssues/queryIssueDetail/queryPulls) that interleaves BETWEEN
// seedBatch messages would run against the truncated, partially-repopulated transaction and briefly
// hand the UI incomplete results — a regression from the old synchronous one-shot seed, which no read
// could interleave. We can't simply REJECT such reads: both consumers (use-replica-issues.ts refetch +
// onChanged) treat any query rejection as a hard "Replica error". So we DEFER them instead — a read that
// arrives mid-seed waits until the seed settles, then runs against a COMPLETE database. Both terminal
// paths leave a complete DB: commit publishes the fresh snapshot; abort ROLLBACKs to the prior complete
// snapshot. Either way a released read sees whole data, never a partial one.
//
// Pure + injectable so it is unit-testable (test/seed-read-gate.test.ts) without the wasm/OPFS Worker.

export class SeedReadGate {
  /** Resolvers for reads deferred during the current seed; drained on settle(). */
  private waiters: Array<() => void> = [];

  /**
   * Return a promise a read should await before touching the DB. When no seed is open the caller passes
   * `seeding=false` and gets an already-resolved promise (zero overhead on the hot read path); while a
   * seed is open it resolves only once settle() runs.
   */
  whenReadable(seeding: boolean): Promise<void> {
    if (!seeding) return Promise.resolve();
    return new Promise<void>((resolve) => this.waiters.push(resolve));
  }

  /** Release every read deferred during the just-finished seed — call on BOTH commit and abort. */
  settle(): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const resolve of waiters) resolve();
  }
}
