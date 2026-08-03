// replica/browser/db.worker.ts — the OPFS replica's Worker entry point (CTC-51 → CTC-114, ADR-0002
// browser twin of the node CatalystReplica). SAHPool's OPFS SyncAccessHandles are Worker-only, so the
// WHOLE replica DB lives here and the main thread (browser-replica.ts) drives it over postMessage
// using the protocol.ts contract. buildIssuesView / buildIssueDetail / buildPullsView run UNCHANGED
// over the wasm SqlExecutor — the load-bearing ADR-0002 "same query, four runtimes" proof, in the
// browser.
//
// All dispatch logic lives in worker-core.ts (injected opener, so it is unit-tested over a `:memory:`
// DB); this entry only binds it to the real OPFS opener and the postMessage envelope.
//
// PACKAGING (CTC-114): consumers reach this via `new Worker(new URL("./db.worker.js",
// import.meta.url), { type: "module" })` in browser-replica.ts — the portable idiom Vite, webpack 5
// and Rollup all statically detect and emit as a separate worker chunk. The `@sqlite.org/sqlite-wasm`
// import below (a peer dependency) is resolved by the CONSUMER's bundler inside that chunk, and its
// .wasm asset rides the consumer's asset pipeline.

/// <reference lib="webworker" />

import { openReplica } from "./sqlite-db.js";
import { createWorkerCore } from "./worker-core.js";
import type { Envelope, ReplicaResponse } from "./protocol.js";

const core = createWorkerCore((request) => openReplica(request.dbPath, request.directory));

/** The single message handler — settle every request into an ok/err envelope keyed by its id. */
self.addEventListener("message", (event: MessageEvent<Envelope>) => {
  const { id, request } = event.data;
  core
    .handle(request)
    .then((result) => {
      const ok: ReplicaResponse = { id, ok: true, result };
      self.postMessage(ok);
    })
    .catch((err: unknown) => {
      // Serialize the REAL cause — the client rethrows this message, and a bare "error" with no
      // diagnosable reason was a CTC-51 pain point.
      const fail: ReplicaResponse = {
        id,
        ok: false,
        error: err instanceof Error ? err.message : String(err),
      };
      self.postMessage(fail);
    });
});
