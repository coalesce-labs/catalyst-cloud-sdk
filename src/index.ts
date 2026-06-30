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

// The opt-in OpenTelemetry contract (CTC-138). Just the `telemetry` option type + the instrumentation
// scope / metric / span / attribute NAMES (plain consts + a structural type, NO `@opentelemetry/api`
// type leak), so a consumer can type the option and reference the exact stream names for dashboards.
export {
  DEFAULT_SCOPE_NAME,
  CATALYST_ATTR,
  REPLICA_METRIC,
  REPLICA_SPAN,
  REPLICA_STATUS_CODE,
  type TelemetryConfig,
} from "./otel.js";
