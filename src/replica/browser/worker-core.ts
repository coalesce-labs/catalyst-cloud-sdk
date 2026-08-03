// replica/browser/worker-core.ts — the Worker's request dispatcher, extracted from the worker entry
// (CTC-114) so the ENTIRE open→seed→apply→query lifecycle is testable in plain vitest: the opener is
// INJECTED, so tests drive the exact production dispatch over a `:memory:` sqlite-wasm DB (sqlite-wasm
// runs under node) while db.worker.ts binds it to the OPFS SAHPool opener. This is the browser twin of
// the node CatalystReplica's engine seam, and it pays down CTC-131's logic half — the only part left
// untestable is the OPFS/SAHPool acquisition itself.
//
// Behavior is byte-for-byte the CTC-51/CTC-132 worker: one open replica per core, a single batched
// seed session at a time, reads deferred mid-seed (SeedReadGate), and the ok/err envelope discipline
// (the worker never throws across the boundary).

import {
  buildIssuesView,
  buildIssueDetail,
  buildPullsView,
} from "@catalyst-cloud/read-model";
import type { OpenedReplica } from "./ports.js";
import {
  applyChange,
  setCursor,
  getCursor,
  truncateReplica,
  getIdentity,
  setIdentity,
  clearCursor,
} from "./apply.js";
import { SeedSession } from "./seed-session.js";
import { SeedReadGate } from "./seed-read-gate.js";
import type {
  OpenRequest,
  ReplicaRequest,
  ApplyChangesResult,
} from "./protocol.js";

/** The one OPFS-specific dependency, injected: how to open the replica for an `open` request. */
export type ReplicaOpener = (request: OpenRequest) => Promise<OpenedReplica>;

/** The dispatcher: `handle` resolves one request to its (untyped) result the client casts via ResultMap. */
export interface WorkerCore {
  handle(request: ReplicaRequest): Promise<unknown>;
}

export function createWorkerCore(open: ReplicaOpener): WorkerCore {
  /** The opened replica — set once by the `open` request, then reused for every subsequent command. */
  let replica: OpenedReplica | null = null;

  /** The tenant fence the open replica was admitted under, so a re-open can be checked against it. */
  let openedIdentity: string | null = null;

  /** The in-flight batched seed (CTC-132), if one is open — spans seedBegin → seedBatch* → seedCommit. */
  let session: SeedSession | null = null;

  /** Defers reads that arrive mid-seed until the seed settles, so a query never runs against the
   *  truncated, partially-repopulated transaction (CTC-132 — see seed-read-gate.ts). */
  const readGate = new SeedReadGate();

  /** Narrow `replica` to non-null or throw a message the client surfaces (never a raw undefined deref). */
  function requireReplica(): OpenedReplica {
    if (!replica)
      throw new Error("replica not open — send an 'open' request first");
    return replica;
  }

  async function handle(request: ReplicaRequest): Promise<unknown> {
    switch (request.type) {
      case "open": {
        // A re-open with a DIFFERENT identity is a programming error, not a tenant switch: the fence
        // below runs only on the first open, so silently ignoring the second would leave the core
        // serving tenant A's rows to a caller that believes it opened tenant B. Loud beats silent.
        if (replica) {
          if (openedIdentity !== request.identity) {
            throw new Error(
              "replica already open under a different identity — close() before reopening",
            );
          }
          return undefined;
        }
        replica = await open(request);
        // TENANT FENCE (CTC-114 review). dbPath/directory default to constants, so every tenant on
        // an origin shares one OPFS database. Enforce the fence HERE, before any read can be served:
        // on a mismatch wipe the entity tables and delete the cursor, which makes the client's
        // warm-start check (`getCursor() != null`) read cold and take the /snapshot path. Without
        // this, a cookie-user change left the previous tenant's rows both readable and unremovable —
        // deltas carry changes, never "forget everything".
        const stored = getIdentity(replica.write);
        if (stored !== request.identity) {
          // UNCONDITIONAL on mismatch — including `stored === null` (CTC-114 review, second round).
          // An earlier version adopted a null-identity database on the theory that it was fresh. It is
          // not: every OPFS replica written before this fence existed has no identity key, and those
          // are exactly the databases that may belong to a different cookie user. Adopting one stamps
          // the new identity over the OLD ROWS and leaves the old cursor intact, so start() reads warm,
          // skips /snapshot, and serves the previous tenant. A missing identity cannot be validated,
          // so it must force a cold re-seed.
          //
          // The cost on a genuinely fresh database is a truncate over empty tables and a DELETE that
          // matches no row — which is why this is safe to do unconditionally, and why the alternative
          // (clear the cursor only) is not enough: if the forced re-seed then FAILS, the client stays
          // alive and queryIssues() would still read the previous tenant's rows. Truncating makes
          // "wrong-identity rows are unreadable" unconditional rather than contingent on a later step.
          //
          // ONE transaction, so a crash mid-wipe can never leave rows from A under a stamp saying B.
          const r = replica;
          r.write.transaction(() => {
            truncateReplica(r.write);
            clearCursor(r.write);
            setIdentity(r.write, request.identity);
          });
        }
        openedIdentity = request.identity;
        return undefined;
      }

      // Batched streaming seed (CTC-132) — one open transaction spanning many messages. All transaction
      // logic lives in SeedSession; these handlers just drive it and surface state errors over the
      // ok/err envelope (the worker never throws across the boundary).
      case "seedBegin": {
        // Reject a second concurrent seed instead of OVERWRITING the open one (CTC-132 review finding):
        // reassigning `session` while a transaction is live would strand it — the next BEGIN fails as
        // nested, and the replica is unusable until the Worker terminates. Seeds are serialized by
        // design (start() awaits its reseed; the SDK awaits its reseed callback), so this only ever
        // fires on a genuine double-drive, and rejecting keeps the first seed intact.
        if (session)
          throw new Error("seedBegin while a seed is already in progress");
        const r = requireReplica();
        session = new SeedSession(r.write, {
          truncate: truncateReplica,
          apply: applyChange,
          setCursor,
        });
        session.begin();
        return undefined;
      }

      case "seedBatch": {
        if (!session) throw new Error("seedBatch before seedBegin");
        session.batch(request.rows);
        return undefined;
      }

      case "seedCommit": {
        if (!session) throw new Error("seedCommit before seedBegin");
        const cursor = session.commit(request.cursor);
        session = null;
        readGate.settle(); // release reads deferred during the seed — they now see the fresh snapshot.
        return cursor;
      }

      case "seedAbort": {
        session?.abort();
        session = null;
        readGate.settle(); // release deferred reads — the ROLLBACK restored the prior complete snapshot.
        return undefined;
      }

      case "applyChanges": {
        const r = requireReplica();
        const since = getCursor(r.write) ?? 0;
        let applied = 0;
        let maxSeq = since;
        r.write.transaction(() => {
          for (const rec of request.changes) {
            // SEQ STALE-GUARD (CTC-114 review). The transport deliberately forwards duplicate /
            // out-of-order frames at `seq <= deliveredSeq` (live-sync-client.ts: "passed through
            // unchanged: the consumer's stale-guard already dedups it") — WE are that stale-guard, and
            // `applyDelta`'s only covers upserts (`ON CONFLICT ... WHERE excluded.updated_at > …`).
            // The DELETE path keys on the PK alone and has no guard, so a replayed old delete removed a
            // row that a NEWER change had created. `setCursor` still holds the newer cursor, so a
            // reconnect resumes above the damage and nothing ever repairs it — only a re-seed does.
            //
            // The cursor's meaning is exactly the test we need: everything at or below it is already
            // reflected in the DB — applied as a delta, dropped by the updated_at guard as stale (the
            // DB then holds NEWER data), or carried in the snapshot that set it. So skipping is right
            // in every case. Comparing against the ROLLING `maxSeq` rather than the entry `since` also
            // catches a duplicate that repeats WITHIN one batch: the transport delivers in arrival
            // order, so an oldie always trails the original it duplicates.
            if (rec.seq <= maxSeq) continue;
            if (applyChange(r.write, rec)) applied++;
            maxSeq = rec.seq;
          }
          // Advance to the max seq SEEN (not just applied) — a window of all-stale deltas still moves
          // the cursor forward so we don't re-fetch forever. Twin of the node sync path.
          setCursor(r.write, maxSeq);
        });
        const result: ApplyChangesResult = { applied, cursor: maxSeq };
        return result;
      }

      // Reads DEFER while a seed is open (CTC-132 review finding) so they never observe the truncated,
      // partially-repopulated transaction; readGate.settle() (on commit/abort) releases them against a
      // complete DB. When no seed is open this awaits an already-resolved promise — no added latency.
      case "queryIssues":
        await readGate.whenReadable(session !== null);
        return buildIssuesView(
          requireReplica().read,
          request.limit,
          request.offset,
        );

      case "queryIssueDetail":
        await readGate.whenReadable(session !== null);
        return buildIssueDetail(requireReplica().read, request.identifier);

      case "queryPulls":
        await readGate.whenReadable(session !== null);
        return buildPullsView(
          requireReplica().read,
          request.limit,
          request.offset,
        );

      case "getCursor":
        return getCursor(requireReplica().write);

      // Cooperative teardown (CTC-114): roll back any open seed, release deferred reads, and close the
      // DB so OPFS SyncAccessHandles are freed BEFORE the client terminates the Worker. Idempotent —
      // a close with nothing open is a no-op.
      case "close": {
        session?.abort();
        session = null;
        readGate.settle();
        replica?.close();
        replica = null;
        openedIdentity = null;
        return undefined;
      }
    }
  }

  return { handle };
}
