// @catalyst-cloud/sdk/node — the managed replica for node/bun (CTC-113).
//
// `CatalystReplica` is host-sync's writer + read seam behind ONE import: open + migrate a local
// SQLite replica, stream-seed /snapshot, subscribe to the live change feed (the SDK's LiveSyncClient),
// and serve the @catalyst-cloud/read-model `build*View` queries SYNCHRONOUSLY over it. Composed from
// the published @catalyst-cloud/{schema,read-model,replicate} packages; the sqlite driver is INJECTED
// (default auto-detect: bun:sqlite, else node:sqlite) so the SDK never hard-deps one.
//
// The root entrypoint ("@catalyst-cloud/sdk") stays the transport-only, isomorphic surface; this
// subpath adds the replica. Import this only from a node/bun backend.

export {
  CatalystReplica,
  type CatalystReplicaOptions,
  type CatalystReplicaReadOnlyOptions,
} from "./replica/catalyst-replica.js";

export {
  bunSqliteEngine,
  bunSqliteReadonlyEngine,
  nodeSqliteEngine,
  nodeSqliteReadonlyEngine,
  betterSqlite3Engine,
  betterSqlite3ReadonlyEngine,
  autoDetectEngine,
  autoDetectReadonlyEngine,
  type ReplicaEngine,
  type EngineFactory,
  type EngineBindable,
  type BetterSqlite3Driver,
} from "./replica/engine.js";

export {
  claimWriterLock,
  type WriterGuardOptions,
  type WriterLockHandle,
} from "./replica/writer-lock.js";

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

// Re-export the read-model view types so a consumer typing the read results needs only this import.
export type {
  SqlExecutor,
  SqlValue,
  IssueView,
  IssueDetailView,
  PullView,
  ProjectView,
  ProjectDetailView,
  InitiativeView,
  InitiativeDetailView,
} from "@catalyst-cloud/read-model";

// Re-export the Drizzle table schema so the `.handle` ad-hoc escape hatch is a one-liner — the
// read-model `build*View` results stay the primary read surface; this is for custom typed queries:
//   import { drizzle } from "drizzle-orm/bun-sqlite";
//   const db = drizzle(replica.handle as Database, { schema: mirrorSchema });
export { mirrorSchema, type MirrorEntityName } from "@catalyst-cloud/schema";
