// @catalyst-cloud/sdk — public entrypoint.
//
// The live-sync client for the catalyst-cloud change feed (browser + node/bun). Open a WebSocket to a
// tenant's mirror, send a cursor-replay request on every (re)connect, and apply each pushed change
// into your own store.
//
// The class is storage-agnostic and auth-injected: see {@link LiveSyncClient}.

export {
  LiveSyncClient,
  buildConnectUrl,
  parseFrame,
  toWsOrigin,
  type AuthStrategy,
  type LiveSyncClientOptions,
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
  type ResyncFrame,
  type SyncFrame,
  type ServerFrame,
} from "./types.js";
