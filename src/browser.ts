// @catalyst-cloud/sdk/browser — the managed replica for the browser (CTC-114, milestone 2 of the
// managed-replica roadmap; ADR-0002).
//
// `BrowserReplica` is the browser twin of `/node`'s CatalystReplica behind ONE import: claim
// single-ownership of the origin's replica (Web Locks — a losing tab degrades to "secondary", never a
// red error), open + migrate an OPFS SAHPool SQLite DB inside a dedicated Worker, stream-seed
// /snapshot in bounded batches, subscribe to the live change feed (the SDK's LiveSyncClient) through a
// coalescing, depth-bounded delta queue, and serve the @catalyst-cloud/read-model `build*View`
// queries over it — the "same query, four runtimes" proof, in the browser.
//
// BUNDLER CONTRACT: the worker is reached via `new Worker(new URL("./db.worker.js",
// import.meta.url), { type: "module" })`, which Vite, webpack 5 and Rollup detect statically and emit
// as a separate chunk; `@sqlite.org/sqlite-wasm` is a PEER dependency resolved by the consumer inside
// that chunk. Import this subpath ONLY from browser code — the root ("@catalyst-cloud/sdk") stays the
// transport-only isomorphic surface, and '/node' never pulls any of this into a node bundle.

export {
  BrowserReplica,
  streamSeedIntoWorker,
  type BrowserReplicaOptions,
  type ReplicaHandlers,
  type ReplicaStatus,
} from "./replica/browser/browser-replica.js";

export { isBrowserReplicaSupported } from "./replica/browser/support.js";

export {
  acquireReplicaLock,
  type ReplicaLockHandle,
  type ReplicaLockOptions,
} from "./replica/browser/browser-lock.js";

export {
  DeltaQueue,
  MAX_APPLY_BATCH_ROWS,
  MAX_INBOX_DEPTH,
  type SeqChange,
  type DeltaQueueOptions,
} from "./replica/browser/delta-queue.js";

export { streamSnapshotBatches, SEED_BATCH_ROWS } from "./replica/browser/snapshot-stream.js";

export type {
  WireChange,
  ReplicaRequest,
  ReplicaResponse,
  Envelope,
  ResultMap,
  ApplyChangesResult,
} from "./replica/browser/protocol.js";

// Re-export the transport surface so a consumer can stay on one import.
export {
  LiveSyncClient,
  type AuthStrategy,
  type LiveSyncStatus,
  type LogLevel,
  type WebSocketLike,
  type WebSocketFactory,
} from "./live-sync-client.js";

export {
  ENTITY_NAMES,
  CHANGE_OPS,
  type AccountId,
  type EntityName,
  type ChangeOp,
  type ChangeFrame,
} from "./types.js";
