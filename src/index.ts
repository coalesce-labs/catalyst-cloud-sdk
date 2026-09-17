// @catalyst-cloud/sdk — public entrypoint.
//
// The live-sync client for the catalyst-cloud change feed (browser + node/bun). Open a WebSocket to a
// tenant's mirror, send a cursor-replay request on every (re)connect, and apply each pushed change
// into your own store.
//
// The class is storage-agnostic and auth-injected: see {@link LiveSyncClient}.

export {
  LiveSyncClient,
  AuthError,
  CLOSE_REAUTHENTICATE,
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
  PING_FRAME,
  PONG_FRAME,
  type AccountId,
  type EntityName,
  type ChangeOp,
  type ChangeFrame,
  type ResyncFrame,
  type SyncFrame,
  type PongFrame,
  type HeadFrame,
  type SkipFrame,
  type ServerFrame,
} from "./types.js";

// The opt-in OpenTelemetry contract (CTC-138). Just the `telemetry` option type + the instrumentation
// scope / metric / span / attribute NAMES (plain consts + a structural type, NO `@opentelemetry/api`
// type leak), so a consumer can type the option and reference the exact stream names for dashboards.
export {
  DEFAULT_SCOPE_NAME,
  CATALYST_ATTR,
  REPLICA_METRIC,
  REPLICA_LOG,
  REPLICA_SPAN,
  REPLICA_STATUS_CODE,
  type ReplicaApplyResult,
  type ReplicaGapEvent,
  type TelemetryConfig,
} from "./otel.js";

// CTC-2004 — the typed tenant client: one implementation of every tenant read and write (the
// contract with its ETag cache, the keyset-paged lists, every /api/v1/agent/* write), so the CLI, the
// skill scripts and MCP tools stop carrying their own. Results are discriminated unions on `outcome`;
// nothing throws for a server answer. The `TenantContract` declaration is the SDK's own copy of the
// cloud's (the cloud's package is private), pinned against its committed fixture in test/.
export {
  createTenantClient,
  memoryContractCache,
  normalizeBaseUrl,
  DEFAULT_TIMEOUT_MS,
  NEXT_CURSOR_HEADER,
  HEAD_SEQ_HEADER,
  TOTAL_HEADER,
  BLOCKED_ON_ASK_TOTAL_HEADER,
  CONTRACT_VERSION_HEADER,
  type TenantClient,
  type TenantClientOptions,
  type TenantClientFailure,
  type ContractCacheEntry,
  type ContractCacheStore,
  type ContractOptions,
  type ContractResult,
  type ContractFailure,
  pageCursor,
  type PageCursor,
  type KeysetPageMeta,
  type IssueListParams,
  type IssueListResult,
  type IssueGetResult,
  type PullListParams,
  type PullListResult,
  type PullGetResult,
  type ProjectListParams,
  type ProjectListResult,
  type MeResult,
  AGENT_ROUTE_NAMES,
  type AgentRouteName,
  type AgentCallFailure,
  type ProxiedWriteOutcome,
  type ProxiedWriteBody,
  type IssueStateInput,
  type IssueStateResult,
  type IssueLabelInput,
  type IssueLabelItemResult,
  type IssueLabelResult,
  type IssueCommentInput,
  type IssueCommentResult,
  type IssueCreateInput,
  type IssueCreateResult,
  type ReactionInput,
  type ReactionResult,
  type ProxiedAttachment,
  type AttachmentInput,
  type AttachmentResult,
  type AttachmentsResult,
  type SessionPlanEntry,
  type SessionActivity,
  type SessionInput,
  type EnsureSessionOutcome,
  type SessionResult,
  type AskInput,
  type AskResult,
  type AskAcceptInput,
  type AskAcceptRefusal,
  type AskAcceptResult,
  type ProjectRepositoryInput,
  type RegisteredProjectRepository,
  type ProjectRepositoryRegisterResult,
  type ProjectRepositoryRemoveResult,
} from "./tenant-client.js";

export {
  CONTRACT_ROUTE,
  TENANT_CONTRACT_KEYS,
  isTenantContract,
  readTenantContract,
  routeByName,
  teamByKey,
  teamForTicket,
  stageIdForSlot,
  labelIdFor,
  type TenantContract,
  type WorkflowSlot,
  type LabelScopeKind,
  type ContractLabel,
  type ContractStage,
  type ContractReadinessCheck,
  type ContractTeam,
  type ContractRoute,
  type ContractMergeRepository,
  type ContractMergeCleanPassShape,
  type ContractAdvanceRule,
  type TeamLookup,
  type StageLookup,
  type LabelLookup,
} from "./tenant-contract.js";
