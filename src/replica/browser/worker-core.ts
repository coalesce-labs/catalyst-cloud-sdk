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

import { buildIssuesView, buildIssueDetail, buildPullsView } from "@catalyst-cloud/read-model";
import type { OpenedReplica } from "./ports.js";
import { applyChange, setCursor, getCursor, truncateReplica } from "./apply.js";
import { SeedSession } from "./seed-session.js";
import { SeedReadGate } from "./seed-read-gate.js";
import type { OpenRequest, ReplicaRequest, ApplyChangesResult } from "./protocol.js";

/** The one OPFS-specific dependency, injected: how to open the replica for an `open` request. */
export type ReplicaOpener = (request: OpenRequest) => Promise<OpenedReplica>;

/** The dispatcher: `handle` resolves one request to its (untyped) result the client casts via ResultMap. */
export interface WorkerCore {
  handle(request: ReplicaRequest): Promise<unknown>;
}

export function createWorkerCore(open: ReplicaOpener): WorkerCore {
  /** The opened replica — set once by the `open` request, then reused for every subsequent command. */
  let replica: OpenedReplica | null = null;

  /** The in-flight batched seed (CTC-132), if one is open — spans seedBegin → seedBatch* → seedCommit. */
  let session: SeedSession | null = null;

  /** Defers reads that arrive mid-seed until the seed settles, so a query never runs against the
   *  truncated, partially-repopulated transaction (CTC-132 — see seed-read-gate.ts). */
  const readGate = new SeedReadGate();

  /** Narrow `replica` to non-null or throw a message the client surfaces (never a raw undefined deref). */
  function requireReplica(): OpenedReplica {
    if (!replica) throw new Error("replica not open — send an 'open' request first");
    return replica;
  }

  async function handle(request: ReplicaRequest): Promise<unknown> {
    switch (request.type) {
      case "open": {
        if (!replica) {
          replica = await open(request);
        }
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
        if (session) throw new Error("seedBegin while a seed is already in progress");
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
            if (applyChange(r.write, rec)) applied++;
            if (rec.seq > maxSeq) maxSeq = rec.seq;
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
        return buildIssuesView(requireReplica().read, request.limit, request.offset);

      case "queryIssueDetail":
        await readGate.whenReadable(session !== null);
        return buildIssueDetail(requireReplica().read, request.identifier);

      case "queryPulls":
        await readGate.whenReadable(session !== null);
        return buildPullsView(requireReplica().read, request.limit, request.offset);

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
        return undefined;
      }
    }
  }

  return { handle };
}
