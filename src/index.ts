// @catalyst-cloud/sdk — public entrypoint.
//
// The isomorphic (browser + node/bun) read-layer-over-WebSocket client for the catalyst-cloud
// change-feed (ADR-0008/0009). Open a WebSocket to a tenant's Mirror Durable Object, send a
// cursor-replay request on every (re)connect, and apply each pushed change into your own store.
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
