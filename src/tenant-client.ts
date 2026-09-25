import { parseLinearIdentityView, type LinearIdentityResult } from "./linear-identity.js";
// tenant-client.ts — CTC-2004. ONE typed client for every tenant read and write, so the CLI, the
// skill scripts, MCP tools and the cloud repo's own scripts share one implementation instead of six.
//
// What it wraps, and where each shape was read from (the research doc for CTC-2004 cites file:line):
//   • GET /api/v1/agent/contract — the tenant's own fact document, ETag-cached per the document's
//     OWN `cache` policy (docs/agent-contract.md): inside `maxAgeSeconds` the cached copy is used;
//     past it a conditional GET revalidates; past `staleRefusalSeconds` with no successful
//     revalidation the client REFUSES by name. There is no literal fallback for any served field.
//   • GET /api/v1/issues, /pulls (keyset-paged: `?after=`, and the next page rides the
//     `X-Mirror-Next-Cursor` header, absent on the last page), /issues/:identifier, /pulls/:id,
//     /projects, /me.
//   • Every tenant write on the /api/v1/agent/* proxy — issue-state, issue-label, issue-comment,
//     issue-create, reaction, attachment, session, ask, ask-accept — plus the attachments read-back.
//     ⭐ THE PATH IS NEVER A LITERAL: each is resolved from the contract's `routes[]` by its last
//     segment, and a route the tenant's contract does not list is a typed `route-unknown` result.
//
// ⛔ NOTHING THROWS FOR A SERVER ANSWER. Every call resolves to a discriminated union on `outcome`.
// The mirror already speaks that grammar on the proxy (`{outcome, reason}` on every status); the auth
// plane speaks `{error, reason}` (401/403) and the open reads speak `{error}` (400/404) — `classify`
// folds all three into the same union, with a `status` on every arm the server produced, so a consumer
// never branches on an HTTP status or regexes a prose reason (the bundle today classifies the write
// budget's 429 with `/budget/i`; here it is the `rate-limited` arm, with `Retry-After` parsed when the
// server sends one — the write-budget refusal does not, the read floor does).
//
// Runtime-agnostic: the global `fetch`/`Headers`/`URL`/`AbortSignal` only (no node import), fetch is
// injectable for tests exactly as `LiveSyncClient.wsFactory` is, and the contract cache STORE is
// injectable because the bundle keeps it on disk while this module must stay free of `node:fs`.
// `IssueView` and friends are `import type`d from `@catalyst-cloud/read-model` (an existing
// dependency) — erased at build, so the root entry gains no runtime import.

import type {
  IssueDetailView,
  IssueView,
  ProjectView,
  PullDetailView,
  PullView,
} from "@catalyst-cloud/read-model";
import {
  CONTRACT_ROUTE,
  readTenantContract,
  routeByName,
  type TenantContract,
} from "./tenant-contract.js";

// ── Options ─────────────────────────────────────────────────────────────────────────────────────

/** Default per-request deadline — the bundle's `REQUEST_TIMEOUT_MS`. */
export const DEFAULT_TIMEOUT_MS = 15_000;

/** One cached contract: the document, the validator it came with, and when it was fetched. */
export interface ContractCacheEntry {
  etag: string | null;
  /** Epoch ms of the fetch (or the last successful 304 revalidation). */
  fetchedAt: number;
  contractVersion: string;
  doc: TenantContract;
}

/** Where the contract cache lives. In-memory by default; the bundle supplies a disk-backed one. */
export interface ContractCacheStore {
  get(): ContractCacheEntry | null | Promise<ContractCacheEntry | null>;
  set(entry: ContractCacheEntry): void | Promise<void>;
}

/** The default store: one entry, held for the life of the client. */
export function memoryContractCache(): ContractCacheStore {
  let entry: ContractCacheEntry | null = null;
  return {
    get: () => entry,
    set: (e) => {
      entry = e;
    },
  };
}

export interface TenantClientOptions {
  /** A tenant or personal key. Most agent proxy writes require an organization key; `teamWorkflow`
   *  and personal connection methods require the caller's own personal key or device login. */
  key: string;
  /** The service origin (e.g. "https://staging.catalystcloud.dev"). A trailing slash is trimmed;
   *  every route path is absolute under it. */
  baseUrl: string;
  /** Injectable for tests; defaults to the platform global. */
  fetch?: typeof fetch;
  /** Per-request deadline in ms. Default {@link DEFAULT_TIMEOUT_MS}. */
  timeoutMs?: number;
  /** Default {@link memoryContractCache}. */
  contractCache?: ContractCacheStore;
  /** Epoch ms; injectable so a test can cross the cache bounds without sleeping. */
  now?: () => number;
}

// ── The shared failure arms ─────────────────────────────────────────────────────────────────────

/**
 * The arms ANY call can end in. Every arm the server produced carries its `status`; `network` is the
 * one arm with none (fetch threw, or the deadline fired). Nothing throws for a server answer.
 */
export type TenantClientFailure =
  /** 401 — the credential was not accepted. `ref` joins to the cloud's log line. */
  | { outcome: "unauthorized"; status: 401; reason: string; ref: string | null }
  /** 403 — wrong principal class, wrong tenant, or a missing scope (`required` names it). */
  | { outcome: "forbidden"; status: 403; reason: string; required: string | null; account: string | null }
  /** 429 — the per-host daily write budget or the reserved Linear read floor. `retryAfterSeconds` is
   *  the server's own `Retry-After` when it sent one (the read floor does; the write budget does not). */
  | { outcome: "rate-limited"; status: 429; reason: string; retryAfterSeconds: number | null }
  /** A 400-class NAMED refusal — malformed input, a bad cursor, a tenant with no Linear workspace. */
  | { outcome: "rejected"; status: number; reason: string; attempts?: number }
  /** The server named a failure of its own (a 502 partial document, a 503 Linear client). */
  | { outcome: "failed"; status: number; reason: string }
  /** fetch threw, or the deadline fired. */
  | { outcome: "network"; reason: string }
  /** The body was not JSON, or not the shape this route is documented to answer. */
  | { outcome: "shape"; status: number; reason: string }
  /** Any other non-2xx. */
  | { outcome: "http"; status: number; reason: string };

// Team setup is a personal-admin API. Its named refusals matter to an interactive caller, so this
// surface retains `error` as well as the shared transport outcome and never retries a stale plan.
export type TeamWorkflowFailure = TenantClientFailure & { error?: string };
export type TeamWorkflowResult<T> = ({ outcome: "ok"; status: number } & T) | TeamWorkflowFailure;
export interface TeamWorkflowReadiness {
  teamId: string;
  teamKey: string;
  teamName: string;
  status: "ready" | "degraded" | "blocked" | "unchecked";
  checkedAt?: number;
  checks: readonly TeamReadinessCheck[];
  workflowRev: number;
  [field: string]: unknown;
}
export interface TeamReadinessCheck {
  id: string;
  state: "pass" | "fail" | "unknown";
  reason?: string;
  count?: number;
}
export type TeamWorkflowSlot = "dispatch" | "intake" | "research" | "plan" | "implement" | "remediate" | "verify" | "review" | "pr" | "done" | "canceled";
export interface TeamWorkflowStage { id: string; name: string; type: string; position: number }
export interface TeamWorkflowRow {
  slot: TeamWorkflowSlot;
  linearStateId: string | null;
  linearStateName?: string;
  linearStateType?: string;
  source?: "created" | "matched" | "chosen";
  stateStillExists?: boolean;
}
export interface TeamWorkflowSummary extends TeamWorkflowReadiness {
  mode: "mapped-existing" | "adopted-recommended" | "mixed" | null;
  gitAutomation: "off" | "managed";
  mappedSlots: number;
  mappedLoadBearingSlots: number;
  mirrored: boolean | null;
}
export interface TeamList {
  teams: TeamWorkflowSummary[];
  canManage: boolean;
  liveTeamRead: { attempted: boolean; error: string | null; reason?: "no-teams-known" | "unnamed-team" };
  everChecked: boolean;
  mirrorRead: boolean;
}
export interface TeamWorkflowView {
  config: { teamId: string; mode: "mapped-existing" | "adopted-recommended" | "mixed"; gitAutomation: "off" | "managed"; workflowRev: number };
  rows: readonly TeamWorkflowRow[];
  stages: readonly TeamWorkflowStage[];
  stageSource: "linear" | "mirror" | "none";
  readiness: TeamWorkflowReadiness;
  mappingHash: string;
  checklist: string[] | null;
}
/** Save can commit the mapping even when its follow-up readiness recompute is unavailable. */
export type TeamWorkflowSaveResult = Omit<TeamWorkflowView, "mappingHash" | "checklist" | "readiness"> & {
  readiness: TeamWorkflowReadiness | null;
};
export interface TeamMappingRowInput {
  slot: TeamWorkflowSlot;
  linearStateId: string | null;
  source?: "created" | "matched" | "chosen";
}
export interface TeamMappingSaveInput {
  team: string;
  expectedMappingHash: string;
  mode?: "mapped-existing" | "adopted-recommended" | "mixed";
  gitAutomation?: "off" | "managed";
  rows: TeamMappingRowInput[];
}
export interface TeamMigrationChoice { sourceStateId: string; destinationStateId: string }
export interface TeamMigrationSource {
  stateId: string;
  name: string;
  type: string;
  ticketCount: number;
  destinationStateId: string | null;
  destinationSlot: TeamWorkflowSlot | null;
  because: "unique-type" | "terminal-family" | "chosen" | null;
  outcome: "ready" | "needs-a-choice" | "empty" | "moved" | "partially-moved" | "protected" | "retired-in-catalyst";
  reason?: string;
  retiredAt?: number;
}
export interface TeamMigrationPlan {
  teamId: string;
  sources: readonly TeamMigrationSource[];
  migrationHash: string;
  overLimit: boolean;
  issueCount: number;
  retireLogReadable: boolean;
}
export interface TeamAdoptResult {
  teamId: string;
  teamKey: string;
  mode: "adopted-recommended" | "mapped-existing" | "mixed" | null;
  stages: readonly { name: string; type: string; outcome: string; stateId?: string; reason?: string }[];
  planHash: string;
  unfilledLoadBearing: readonly unknown[];
  provenanceGaps: readonly unknown[];
  labels: readonly { name: string; outcome: string; reason?: string }[];
  labelProvenanceGaps: readonly unknown[];
  labelsNotCreated: readonly unknown[];
  checklist?: string[];
  readiness?: TeamWorkflowReadiness | null;
}
export interface TeamUndoPreview { teamId: string; mode: "preview"; candidates: readonly { stateId: string; name: string | null }[]; undoHash: string }
export interface TeamUndoResult {
  archived: readonly { stateId: string; name: string | null }[];
  kept: readonly { stateId: string; name: string | null; reason: string }[];
  failed: readonly { stateId: string; name: string | null; reason: string }[];
  readiness: TeamWorkflowReadiness | null;
}
export interface TeamMigrationChunk { teamId: string; sources: readonly TeamMigrationSource[]; remaining: number; migrationHash: string; moved: number; readiness: TeamWorkflowReadiness | null }
export interface TeamMigrationRetire {
  retired: readonly { stateId: string; name: string }[];
  kept: readonly { stateId: string; name: string; reason: string }[];
  failed: readonly { stateId: string; name: string; reason: string }[];
  logGaps: readonly { stateId: string; reason: string }[];
  readiness: TeamWorkflowReadiness | null;
}

// ── The contract ────────────────────────────────────────────────────────────────────────────────

export type ContractResult =
  | {
      outcome: "ok";
      doc: TenantContract;
      /** `cache` — served inside `maxAgeSeconds` (or on a network failure inside
       *  `staleRefusalSeconds`); `revalidated` — a 304; `network` — a fresh 200. */
      source: "cache" | "network" | "revalidated";
      ageSeconds: number;
      etag: string | null;
      contractVersion: string;
    }
  /** Past `staleRefusalSeconds` and the refresh failed: refused by name, with the failure that
   *  prevented revalidation. No document is served — there is no literal to fall back to. */
  | { outcome: "stale"; ageSeconds: number; staleRefusalSeconds: number; cause: TenantClientFailure }
  /** `offline: true` with nothing cached. */
  | { outcome: "missing"; reason: string }
  | TenantClientFailure;

export type ContractFailure = Exclude<ContractResult, { outcome: "ok" }>;

export interface ContractOptions {
  /** Revalidate even inside `maxAgeSeconds`. */
  refresh?: boolean;
  /** Never touch the network: the cache if any, else `missing`. */
  offline?: boolean;
}

// ── The open reads ──────────────────────────────────────────────────────────────────────────────

declare const PAGE_CURSOR: unique symbol;
/**
 * An opaque keyset token off `X-Mirror-Next-Cursor`. Branded so the only ways to obtain one are a
 * previous page's `nextCursor` or {@link pageCursor} over a token you persisted — an arbitrary string
 * is not a page position.
 */
export type PageCursor = string & { readonly [PAGE_CURSOR]: true };

/** Admit a persisted token as a {@link PageCursor}. */
export function pageCursor(token: string): PageCursor {
  return token as PageCursor;
}

/** `GET /api/v1/issues` — the facets `MirrorDO.handleIssues` reads, by their WIRE names' meaning. */
export interface IssueListParams {
  after?: PageCursor;
  limit?: number;
  teamId?: string;
  teamKey?: string;
  /** An unrecognised value is no narrowing server-side, never a 400 — so the type is closed here. */
  state?: "active" | "backlog" | "done";
  /** Linear's 1..4. */
  priority?: 1 | 2 | 3 | 4;
  assigneeId?: string;
  assignee?: string;
  cycle?: string;
  blocked?: "ask";
  /** `"priority"` orders the FULL scoped set by urgency; absent keeps `updated_at DESC`. */
  sort?: "priority";
  waitingOn?: string;
  waitingMode?: "asks" | "all";
}

export interface KeysetPageMeta {
  /** The next page's `after`, or `null` on the last page (the header is absent there). */
  nextCursor: PageCursor | null;
  /** The scope's FULL row count (`X-Mirror-Total`), `null` when the server did not send it. */
  total: number | null;
  /** The feed head seq this page is consistent with (`X-Mirror-Cursor`). */
  head: number | null;
}

export type IssueListResult = ({ outcome: "ok"; rows: IssueView[] } & KeysetPageMeta) | TenantClientFailure;
export type IssueGetResult =
  | { outcome: "ok"; issue: IssueDetailView; head: number | null }
  | { outcome: "not-found"; status: 404 }
  | TenantClientFailure;

export interface PullListParams {
  after?: PageCursor;
  limit?: number;
  /** `owner/name`. */
  repo?: string;
  state?: "open" | "merged";
  blocked?: "ask";
}

export type PullListResult =
  | ({ outcome: "ok"; rows: PullView[]; blockedOnAskTotal: number | null } & KeysetPageMeta)
  | TenantClientFailure;
export type PullGetResult =
  | { outcome: "ok"; pull: PullDetailView }
  | { outcome: "not-found"; status: 404 }
  | TenantClientFailure;

/** `GET /api/v1/projects` is offset-paged (the legacy `?cursor=` carries the offset). */
export interface ProjectListParams {
  limit?: number;
  offset?: number;
}
export type ProjectListResult = { outcome: "ok"; rows: ProjectView[]; head: number | null } | TenantClientFailure;

/** `GET /api/v1/me` — how a key learns its own tenant. */
export type MeResult =
  | {
      outcome: "ok";
      account: string;
      slug: string;
      name: string;
      permissions: string[] | null;
      principal: "service" | "session";
    }
  | TenantClientFailure;

/** Personal provider consent is initiated by a user credential, never an account key. */
export type PersonalConnectionProvider = "linear" | "github";

export type PersonalConnectionStartResult =
  | { outcome: "ok"; status: 200; authorizationUrl: string; expiresAt: number }
  | { outcome: "workspace-required"; status: 409 }
  | TenantClientFailure;

export type PersonalConnectionStatusResult =
  | { outcome: "absent"; status: 200 }
  | { outcome: "lapsed"; status: 200; lapsedAt: number | null }
  | {
      outcome: "connected";
      status: 200;
      provider: "linear";
      linearUserId: string;
      grantedScope: string | null;
      updatedAt: number;
      expiresAt: number | null;
    }
  | {
      outcome: "connected";
      status: 200;
      provider: "github";
      githubUserId: string;
      githubLogin: string;
      updatedAt: number;
      expiresAt: number | null;
    }
  | { outcome: "unavailable"; status: 503; provider: PersonalConnectionProvider }
  | TenantClientFailure;

// ── The agent proxy — every tenant write, plus the attachments read-back ────────────────────────

/** The routes this client wraps, by the last segment of their contract path. `delegate` (operator
 *  credential only) and `linear/read` (an origin-fresh read the replica rule discourages) are
 *  deliberately not here — see the research doc for CTC-2004 §6. */
export type AgentRouteName =
  | "issue-state"
  | "issue-label"
  | "issue-comment"
  | "issue-create"
  | "reaction"
  | "attachment"
  | "attachments"
  | "session"
  | "ask"
  | "ask-accept"
  | "project-repositories"
  | "project-repositories/remove"
  | "portal-servers"
  | "portal-servers/register"
  | "portal-servers/remove";

/** The runtime twin of {@link AgentRouteName}: the list a test can walk against the contract.
 *  Kept in lockstep with the union by a compile-time equality in test/tenant-client-agent.test.ts. */
export const AGENT_ROUTE_NAMES = [
  "issue-state", "issue-label", "issue-comment", "issue-create", "reaction",
  "attachment", "attachments", "session", "ask", "ask-accept",
  "project-repositories", "project-repositories/remove",
  "portal-servers", "portal-servers/register", "portal-servers/remove",
] as const satisfies readonly AgentRouteName[];

/** The arms every agent call can end in BEFORE the route answers: the contract could not be served,
 *  or the tenant's contract does not list the route. */
export type AgentCallFailure =
  | ContractFailure
  | { outcome: "route-unknown"; route: AgentRouteName; routes: string[] }
  | TenantClientFailure;

/** What `callAgentRoute` hands its verbs — a success, or a failure TAGGED WITH WHERE IT CAME FROM.
 *  `"contract"` is everything that happened before the route was reached (the contract could not be
 *  served, or does not list the route), `"transport"` is the request itself failing, and `"route"`
 *  is the route's own answer. A verb that reinterprets a status — see `notFound` — may only do so
 *  for `"route"`, or it would describe the caller's project using an answer about the contract. */
type AgentCallResult =
  | { ok: true; body: Record<string, unknown>; status: number }
  | { ok: false; from: "contract" | "transport" | "route"; failure: AgentCallFailure };

/** The proxy's shared write outcome (`ProxiedWriteResult`), on the wire with the HTTP status. The
 *  `rejected` arm is the shared {@link TenantClientFailure} one. */
export type ProxiedWriteOutcome =
  | { outcome: "succeeded"; status: number; attempts: number }
  | { outcome: "exhausted"; status: number; attempts: number; lastError: string };

export interface IssueStateInput {
  issueId: string;
  /** The Linear state id — resolve it by SLOT through {@link stageIdForSlot}, never by name. */
  stateId: string;
}
export type IssueStateResult = ProxiedWriteOutcome | AgentCallFailure;

export interface IssueLabelInput {
  issueId: string;
  labelIds: string[];
  mode: "add" | "remove";
}
export interface IssueLabelItemResult {
  labelId: string;
  /** `already-absent` — a remove that found nothing to remove; counts as satisfied, not as a landing. */
  outcome: "succeeded" | "rejected" | "exhausted" | "already-absent";
  attempts: number;
  reason?: string;
  lastError?: string;
}
export type IssueLabelResult =
  | { outcome: "succeeded" | "failed"; status: number; results: IssueLabelItemResult[] }
  | AgentCallFailure;

export interface IssueCommentInput {
  issueId: string;
  body: string;
  parentId?: string;
  /** The agent display name (`botActor.userDisplayName`). Absent means the plain app actor. */
  createAsUser?: string;
}
export type IssueCommentResult = ProxiedWriteOutcome | AgentCallFailure;

export interface IssueCreateInput {
  teamId: string;
  title: string;
  description?: string;
  labelIds?: string[];
  assigneeId?: string;
  parentId?: string;
  /** Linear's 0–4 scale. */
  priority?: 0 | 1 | 2 | 3 | 4;
  stateId?: string;
  projectId?: string;
  createAsUser?: string;
}
export type IssueCreateResult =
  | { outcome: "succeeded"; status: number; attempts: number; identifier: string; id: string; url: string | null }
  | Extract<ProxiedWriteOutcome, { outcome: "exhausted" }>
  | AgentCallFailure;

/** Exactly one of `issueId`/`commentId` — enforced by the SERVER (a 400 `rejected`); the client
 *  sends what it is given rather than carrying a second copy of the rule. */
export interface ReactionInput {
  issueId?: string;
  commentId?: string;
  /** Defaults server-side to the fleet's claim emoji. */
  emoji?: string;
  mode?: "add" | "remove";
  createAsUser?: string;
}
export type ReactionResult =
  | { outcome: "succeeded"; status: number; attempts: number; reactionId: string | null; alreadyPresent?: true; userId?: string | null }
  | { outcome: "succeeded"; status: number; attempts: number; alreadyAbsent: true; removed: 0 }
  | { outcome: "succeeded"; status: number; attempts: number; removed: number }
  | Extract<ProxiedWriteOutcome, { outcome: "exhausted" }>
  | AgentCallFailure;

export interface ProxiedAttachment {
  id: string;
  url: string;
  title: string | null;
  metadata: Record<string, unknown> | null;
}
export interface AttachmentInput {
  issueId: string;
  /** The upsert key — Linear dedups attachments by url. */
  url: string;
  title: string;
  metadata: Record<string, unknown>;
}
export type AttachmentResult = (ProxiedWriteOutcome & { attachment?: ProxiedAttachment }) | AgentCallFailure;
export type AttachmentsResult =
  | { outcome: "succeeded"; status: number; attachments: ProxiedAttachment[] }
  /** The proxy answers an absent issue as a 404 `rejected` — "no attachments" and "no such issue"
   *  are different answers, and this arm keeps them apart. */
  | { outcome: "not-found"; status: 404; reason: string }
  | { outcome: "unauthorized-upstream"; status: number; reason: string }
  | AgentCallFailure;

export interface SessionPlanEntry {
  content: string;
  status: string;
}
export type SessionActivity =
  | { type: "thought" | "response"; body: string }
  | { type: "error"; body: string; reasonCode?: string }
  | { type: "action"; action: string; parameter: string; result?: string };
export interface SessionInput {
  issueId: string;
  plan?: SessionPlanEntry[];
  activity?: SessionActivity;
  externalUrls?: { label: string; url: string }[];
}
/** The proxy's shared write body as it appears INSIDE the session composite (no status of its own). */
export type ProxiedWriteBody =
  | { outcome: "succeeded"; attempts: number }
  | { outcome: "exhausted"; attempts: number; lastError: string }
  | { outcome: "rejected"; attempts: number; reason: string };
export type EnsureSessionOutcome =
  | { outcome: "reused"; sessionId: string; attempts: number }
  | { outcome: "created"; sessionId: string; attempts: number }
  | { outcome: "rejected"; attempts: number; reason: string }
  | { outcome: "exhausted"; attempts: number; lastError: string };
/** ⚠️ The status is the WORST of the sub-outcomes (rejected 400 > exhausted 502 > 200); read
 *  `session`/`plan`/`externalUrls`/`activity` individually — a 502 here can carry a reused session. */
export type SessionResult =
  | { outcome: "ok"; status: number; session: EnsureSessionOutcome; plan?: ProxiedWriteBody; externalUrls?: ProxiedWriteBody; activity?: ProxiedWriteBody }
  | AgentCallFailure;

export interface AskInput {
  teamId: string;
  title: string;
  context: string;
  defaultIfSilent: string;
  /** The idempotency key — a re-raise under the same key answers the existing ask. */
  askKey: string;
  options?: string[];
  blocks?: string[];
  nothingToBlock?: true;
  target?: { membershipId: string } | { email: string };
}
export type AskResult =
  | { outcome: "created"; status: number; askId: string; identifier: string; existing?: true }
  | { outcome: "created-but-relations-partial"; status: number; askId: string; identifier: string; failedBlocks: string[]; existing?: true }
  | { outcome: "exhausted"; status: number; lastError: string }
  | AgentCallFailure;

export interface AskAcceptInput {
  askIssueId: string;
  answerCommentId: string;
  /** A single line of at most 64 characters. */
  acceptedByRole?: string;
}
export type AskAcceptRefusal =
  | "not-an-ask"
  | "issue-not-mirrored"
  | "already-resolved"
  | "answer-comment-not-mirrored"
  | "answer-comment-not-on-ask"
  | "answer-comment-not-wake-target"
  | "answer-comment-is-agent"
  | "not-assignee"
  | "already-in-flight"
  | "readiness-needs-tap"
  | "ask-has-no-team";
export type AskAcceptResult =
  | { outcome: "recorded"; status: number; askIdentifier: string; decisionSummary: string; failedBlockedComments: string[]; resume: unknown[]; unblock?: unknown }
  | { outcome: "refused"; status: number; reason: AskAcceptRefusal }
  | { outcome: "record-failed"; status: number; reason: string; resume?: unknown[] }
  | AgentCallFailure;

/** Both project-repository routes take the same address: a repository plus ONE of project /
 *  teamKey / teamId. This route is NOT accounted against the write budget
 *  (`takesWriteBudgetUnit: false`); it is gated on an admin/owner personal key or an
 *  organization key carrying `mirror:write`. */
export interface ProjectRepositoryInput {
  /** `owner/name`, sent VERBATIM. ⚠️ The cloud validates the trimmed halves but stores what it
   *  was sent (CTC-2503 re-validation) — trim before calling if the value came from a human. */
  repository: string;
  project?: string;
  teamKey?: string;
  teamId?: string;
}
export interface RegisteredProjectRepository {
  repoId: string;
  owner: string;
  name: string;
  /** Present when the `repos` row carries one (e.g. `"paused"`). */
  status?: string;
}
export type ProjectRepositoryRegisterResult =
  | { outcome: "registered"; status: number; registered: RegisteredProjectRepository; created: boolean; linked: boolean }
  /** The project is absent, archived, or another tenant's — one answer for all three. */
  | { outcome: "not-found"; status: 404; reason: string }
  | AgentCallFailure;
export type ProjectRepositoryRemoveResult =
/** `removed: false` is idempotent success, not a failure — there was nothing to unlink. */
  | { outcome: "removed"; status: number; removed: boolean }
  | { outcome: "not-found"; status: 404; reason: string }
  | AgentCallFailure;

/** Upstream auth references only account vault names, never credential values. */
export type PortalServerAuth =
  | { kind: "none" }
  | { kind: "bearer"; secretName: string }
  | { kind: "headers"; headers: { name: string; secretName: string }[] };

export interface PortalServerRegisterInput {
  name: string;
  url: string;
  auth: PortalServerAuth;
}

export interface PortalServer extends PortalServerRegisterInput {
  id: string;
  /** Custom servers stay pending until a server-side account admin approves them. */
  status: "ready" | "pending" | "pending_egress_guard";
}

export type PortalServerRegisterResult =
  | { outcome: "registered"; status: number; server: PortalServer }
  | AgentCallFailure;
export type PortalServersResult =
  | { outcome: "ok"; status: number; servers: PortalServer[] }
  | AgentCallFailure;
export type PortalServerRemoveResult =
  | { outcome: "removed"; status: number; removed: boolean }
  | AgentCallFailure;


// ── Header and param names — the mirror's own strings, each in exactly one place ────────────────

/** The next-page keyset token on `/issues` and `/pulls`; ABSENT on the last page. */
export const NEXT_CURSOR_HEADER = "X-Mirror-Next-Cursor";
/** The feed head seq every list/detail read is consistent with. */
export const HEAD_SEQ_HEADER = "X-Mirror-Cursor";
/** The scope's full row count on `/issues` and `/pulls` — the page can never report what lies past it. */
export const TOTAL_HEADER = "X-Mirror-Total";
/** `/pulls` only: the "N of M blocked on an ask" numerator. */
export const BLOCKED_ON_ASK_TOTAL_HEADER = "X-Mirror-Blocked-On-Ask-Total";
/** The contract route's own version header, equal to the body's `contractVersion`. */
export const CONTRACT_VERSION_HEADER = "x-catalyst-contract-version";

// ── Internals ───────────────────────────────────────────────────────────────────────────────────

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * A detail body is the read-model's own view object; the client cannot re-declare the view to check
 * it field by field without becoming the second copy this ticket deletes, so an object body is
 * accepted as the view. Kept as ONE narrowing site so the assumption is visible and grep-able rather
 * than scattered.
 */
function asView<T extends object>(rec: Record<string, unknown>): T {
  return rec as T;
}

function stringField(rec: Record<string, unknown>, key: string): string | null {
  const v = rec[key];
  return typeof v === "string" ? v : null;
}

function numberField(rec: Record<string, unknown>, key: string): number | null {
  const v = rec[key];
  return typeof v === "number" ? v : null;
}

/** A header carrying an integer (the feed head, a total). `null` when absent or not a number. */
function integerHeader(headers: Headers, name: string): number | null {
  const raw = headers.get(name);
  if (raw === null) return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

interface Answer {
  status: number;
  headers: Headers;
  /** The parsed JSON body, or `undefined` when the body was empty / not JSON. */
  json: unknown;
  /** The first 200 characters of a non-JSON body, for the `http`/`shape` reason. */
  textHead: string;
}

type Sent = { ok: true; answer: Answer } | { ok: false; failure: Extract<TenantClientFailure, { outcome: "network" }> };

/** The reason string a refusal body carries, under whichever of the three grammars it speaks. */
function reasonOf(answer: Answer): string {
  if (isRecord(answer.json)) {
    return (
      stringField(answer.json, "reason") ??
      stringField(answer.json, "error") ??
      stringField(answer.json, "message") ??
      answer.textHead
    );
  }
  return answer.textHead;
}

/**
 * Fold a non-success answer into the shared failure union. Consulted AFTER a route's own success
 * table (which keys on the body's `outcome`), so the server's own discriminator wins whenever it is
 * present and recognised; this handles the auth plane, the budget, and everything unmapped.
 */
function classify(answer: Answer): TenantClientFailure {
  const { status } = answer;
  const rec = isRecord(answer.json) ? answer.json : null;
  const reason = reasonOf(answer);
  if (status === 401) return { outcome: "unauthorized", status, reason, ref: rec ? stringField(rec, "ref") : null };
  if (status === 403) {
    return {
      outcome: "forbidden",
      status,
      reason,
      required: rec ? stringField(rec, "required") : null,
      account: rec ? stringField(rec, "account") : null,
    };
  }
  if (status === 429) {
    const raw = answer.headers.get("retry-after");
    const seconds = raw === null ? Number.NaN : Number(raw);
    return { outcome: "rate-limited", status, reason, retryAfterSeconds: Number.isFinite(seconds) ? seconds : null };
  }
  if (rec !== null && rec["outcome"] === "rejected") {
    const attempts = numberField(rec, "attempts");
    return { outcome: "rejected", status, reason, ...(attempts === null ? {} : { attempts }) };
  }
  if (rec !== null && rec["outcome"] === "failed") return { outcome: "failed", status, reason };
  if (status === 400 && rec !== null && stringField(rec, "error") !== null) {
    return { outcome: "rejected", status, reason };
  }
  if (status >= 200 && status < 300) {
    return { outcome: "shape", status, reason: `unexpected body: ${reason.slice(0, 120)}` };
  }
  return { outcome: "http", status, reason };
}

const SHA256_HEX = /^[a-f0-9]{64}$/;
// Adopt and migrate use FNV-1a staleness tokens (`hash.toString(36)-count.toString(36)`), not SHA-256.
// Mapping and undo deliberately use SHA-256; do not conflate the two wire contracts.
const WORKFLOW_STALENESS_TOKEN = /^[0-9a-z]{1,7}-[0-9a-z]+$/;
const TEAM_SLOTS = new Set(["dispatch", "intake", "research", "plan", "implement", "remediate", "verify", "review", "pr", "done", "canceled"]);
function teamStage(value: unknown): value is TeamWorkflowStage {
  return isRecord(value) && typeof value["id"] === "string" && typeof value["name"] === "string" &&
    typeof value["type"] === "string" && typeof value["position"] === "number";
}
function teamRow(value: unknown): value is TeamWorkflowRow {
  return isRecord(value) && typeof value["slot"] === "string" && TEAM_SLOTS.has(value["slot"]) &&
    (value["linearStateId"] === null || typeof value["linearStateId"] === "string") &&
    (value["linearStateName"] === undefined || typeof value["linearStateName"] === "string") &&
    (value["linearStateType"] === undefined || typeof value["linearStateType"] === "string") &&
    (value["stateStillExists"] === undefined || typeof value["stateStillExists"] === "boolean");
}
function migrationSource(value: unknown): value is TeamMigrationSource {
  return isRecord(value) && typeof value["stateId"] === "string" && typeof value["name"] === "string" &&
    typeof value["type"] === "string" && typeof value["ticketCount"] === "number" &&
    (value["destinationStateId"] === null || typeof value["destinationStateId"] === "string") &&
    (value["destinationSlot"] === null || (typeof value["destinationSlot"] === "string" && TEAM_SLOTS.has(value["destinationSlot"]))) &&
    (value["because"] === null || value["because"] === "unique-type" || value["because"] === "terminal-family" || value["because"] === "chosen") &&
    (value["outcome"] === "ready" || value["outcome"] === "needs-a-choice" || value["outcome"] === "empty" || value["outcome"] === "moved" || value["outcome"] === "partially-moved" || value["outcome"] === "protected" || value["outcome"] === "retired-in-catalyst");
}
function stateResult(value: unknown, reason: boolean, nullableName: boolean): boolean {
  return isRecord(value) && typeof value["stateId"] === "string" &&
    (typeof value["name"] === "string" || (nullableName && value["name"] === null)) &&
    (!reason || typeof value["reason"] === "string");
}
function teamReadiness(value: unknown): value is TeamWorkflowReadiness {
  return isRecord(value) &&
    typeof value["teamId"] === "string" && typeof value["teamKey"] === "string" && typeof value["teamName"] === "string" &&
    (value["status"] === "ready" || value["status"] === "degraded" || value["status"] === "blocked" || value["status"] === "unchecked") &&
    (value["checkedAt"] === undefined || typeof value["checkedAt"] === "number") &&
    typeof value["workflowRev"] === "number" &&
    Array.isArray(value["checks"]) && value["checks"].every((check: unknown) => isRecord(check) &&
      typeof check["id"] === "string" && (check["state"] === "pass" || check["state"] === "fail" || check["state"] === "unknown") &&
      (check["reason"] === undefined || typeof check["reason"] === "string"));
}

function teamList(value: unknown): value is TeamList {
  if (!isRecord(value) || !isRecord(value["liveTeamRead"])) return false;
  return Array.isArray(value["teams"]) && value["teams"].every((team: unknown) =>
    teamReadiness(team) && isRecord(team) &&
    (team["mode"] === null || team["mode"] === "mapped-existing" || team["mode"] === "adopted-recommended" || team["mode"] === "mixed") &&
    (team["gitAutomation"] === "off" || team["gitAutomation"] === "managed") &&
    typeof team["mappedSlots"] === "number" && typeof team["mappedLoadBearingSlots"] === "number" &&
    (team["mirrored"] === null || typeof team["mirrored"] === "boolean")) &&
    typeof value["canManage"] === "boolean" && typeof value["everChecked"] === "boolean" && typeof value["mirrorRead"] === "boolean" &&
    typeof value["liveTeamRead"]["attempted"] === "boolean" &&
    (value["liveTeamRead"]["error"] === null || typeof value["liveTeamRead"]["error"] === "string");
}

function teamView(value: unknown): value is TeamWorkflowView {
  if (!isRecord(value) || !isRecord(value["config"])) return false;
  const config = value["config"];
  return teamConfig(config) &&
    Array.isArray(value["rows"]) && value["rows"].every(teamRow) &&
    Array.isArray(value["stages"]) && value["stages"].every(teamStage) &&
    (value["stageSource"] === "linear" || value["stageSource"] === "mirror" || value["stageSource"] === "none") &&
    teamReadiness(value["readiness"]) &&
    typeof value["mappingHash"] === "string" && SHA256_HEX.test(value["mappingHash"]) &&
    (value["checklist"] === null || (Array.isArray(value["checklist"]) && value["checklist"].every((line: unknown) => typeof line === "string")));
}

function teamConfig(config: Record<string, unknown>): boolean {
  return typeof config["teamId"] === "string" &&
    (config["mode"] === "mapped-existing" || config["mode"] === "adopted-recommended" || config["mode"] === "mixed") &&
    (config["gitAutomation"] === "off" || config["gitAutomation"] === "managed") &&
    typeof config["workflowRev"] === "number";
}

function teamWriteView(value: unknown): boolean {
  if (!isRecord(value) || !isRecord(value["config"])) return false;
  return teamConfig(value["config"]) && Array.isArray(value["rows"]) && value["rows"].every(teamRow) &&
    Array.isArray(value["stages"]) && value["stages"].every(teamStage) &&
    (value["stageSource"] === "linear" || value["stageSource"] === "mirror" || value["stageSource"] === "none") &&
    (value["readiness"] === null || teamReadiness(value["readiness"]));
}

function teamAdopt(value: unknown, apply: boolean): value is TeamAdoptResult {
  if (!isRecord(value)) return false;
  return typeof value["teamId"] === "string" && typeof value["teamKey"] === "string" &&
    (value["mode"] === null || value["mode"] === "adopted-recommended" || value["mode"] === "mapped-existing" || value["mode"] === "mixed") &&
    Array.isArray(value["stages"]) && value["stages"].every((stage: unknown) => isRecord(stage) && typeof stage["name"] === "string" && typeof stage["type"] === "string" && typeof stage["outcome"] === "string") &&
    Array.isArray(value["unfilledLoadBearing"]) &&
    typeof value["planHash"] === "string" && WORKFLOW_STALENESS_TOKEN.test(value["planHash"]) &&
    Array.isArray(value["provenanceGaps"]) && Array.isArray(value["labels"]) && value["labels"].every((label: unknown) => isRecord(label) && typeof label["name"] === "string" && typeof label["outcome"] === "string") &&
    Array.isArray(value["labelProvenanceGaps"]) && Array.isArray(value["labelsNotCreated"]) &&
    (apply ? (value["readiness"] === null || teamReadiness(value["readiness"]))
      : Array.isArray(value["checklist"]) && value["checklist"].every((line: unknown) => typeof line === "string"));
}

/**
 * Trim trailing slashes from the configured origin so route paths (absolute, leading-slash) append
 * cleanly. ⛔ A plain scan, NOT `/\/+$/`: on library input that regex backtracks from every start
 * position when the string ends in a non-slash — measured 4.2 s on 100k slashes (CodeQL
 * security/code-scanning/4 on #65). This is O(n).
 */
export function normalizeBaseUrl(baseUrl: string): string {
  let end = baseUrl.length;
  while (end > 0 && baseUrl.charCodeAt(end - 1) === 47 /* "/" */) end -= 1;
  return baseUrl.slice(0, end);
}

function isPortalServer(value: unknown): value is PortalServer {
  if (!isRecord(value) || typeof value["id"] !== "string" || typeof value["name"] !== "string" ||
      typeof value["url"] !== "string" || (value["status"] !== "ready" && value["status"] !== "pending" && value["status"] !== "pending_egress_guard")) return false;
  const auth = value["auth"];
  if (!isRecord(auth)) return false;
  if (auth["kind"] === "none") return true;
  if (auth["kind"] === "bearer") return typeof auth["secretName"] === "string";
  return auth["kind"] === "headers" && Array.isArray(auth["headers"]) &&
    auth["headers"].every((header: unknown) => isRecord(header) &&
      typeof header["name"] === "string" && typeof header["secretName"] === "string");
}

export function createTenantClient(opts: TenantClientOptions): TenantClient {
  const fetchImpl = opts.fetch ?? fetch;
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const store = opts.contractCache ?? memoryContractCache();
  const now = opts.now ?? Date.now;
  const origin = normalizeBaseUrl(opts.baseUrl);

  function url(path: string, query?: Record<string, string | number | undefined>): string {
    const u = new URL(`${origin}${path}`);
    for (const [k, v] of Object.entries(query ?? {})) {
      if (v !== undefined) u.searchParams.set(k, String(v));
    }
    return u.toString();
  }

  async function send(
    method: "GET" | "POST",
    target: string,
    extraHeaders: Record<string, string>,
    body?: unknown,
  ): Promise<Sent> {
    const headers: Record<string, string> = {
      authorization: `Bearer ${opts.key}`,
      accept: "application/json",
      ...extraHeaders,
    };
    if (body !== undefined) headers["content-type"] = "application/json";
    let res: Response;
    let text: string;
    try {
      res = await fetchImpl(target, {
        method,
        headers,
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.timeout(timeoutMs),
      });
      // ⛔ THE BODY READ IS INSIDE THE TRY (Codex #65 r1, P2). `fetch` resolves once the HEADERS
      // arrive; the stream can still fail, or the deadline fire, while `text()` consumes it — and
      // that rejection is a transport failure exactly like a refused connect, not an exception the
      // public call may leak.
      text = await res.text();
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      return { ok: false, failure: { outcome: "network", reason: `could not reach ${target}: ${reason}` } };
    }
    let json: unknown;
    if (text !== "") {
      try {
        json = JSON.parse(text);
      } catch {
        json = undefined;
      }
    }
    return { ok: true, answer: { status: res.status, headers: res.headers, json, textHead: text.slice(0, 200) } };
  }

  // ── contract() ────────────────────────────────────────────────────────────────────────────────

  async function contract(o: ContractOptions = {}): Promise<ContractResult> {
    const cached = await store.get();
    const nowMs = now();
    const ageSeconds = cached === null ? Number.POSITIVE_INFINITY : Math.max(0, Math.floor((nowMs - cached.fetchedAt) / 1000));
    const fromCache = (entry: ContractCacheEntry): ContractResult => ({
      outcome: "ok",
      doc: entry.doc,
      source: "cache",
      ageSeconds,
      etag: entry.etag,
      contractVersion: entry.contractVersion,
    });

    if (cached !== null && o.refresh !== true && ageSeconds < cached.doc.cache.maxAgeSeconds) {
      return fromCache(cached);
    }
    if (o.offline === true) {
      if (cached !== null) return fromCache(cached);
      return { outcome: "missing", reason: "no cached contract and network reads are disabled" };
    }

    // A revalidation that could not complete — the transport failed, or the server answered a
    // TRANSIENT non-2xx (5xx, 429) — leaves the cached document exactly as usable as the policy says:
    // served until `staleRefusalSeconds`, refused by name as `stale` past it (Codex #65 r1, P1:
    // every agent.* call loads the contract first, so a brief server failure at the max-age
    // boundary must not disable every write). A credential refusal (401/403) or a 400 is NOT
    // transient and is returned as itself: the cache covers the server being away, never the key
    // having been revoked or re-scoped.
    const unavailable = (cause: TenantClientFailure): ContractResult => {
      if (cached === null) return cause;
      if (ageSeconds >= cached.doc.cache.staleRefusalSeconds) {
        return { outcome: "stale", ageSeconds, staleRefusalSeconds: cached.doc.cache.staleRefusalSeconds, cause };
      }
      return fromCache(cached);
    };
    const sent = await send("GET", url(CONTRACT_ROUTE), cached?.etag ? { "if-none-match": cached.etag } : {});
    if (!sent.ok) return unavailable(sent.failure);
    const { answer } = sent;
    if (answer.status === 429 || answer.status >= 500) return unavailable(classify(answer));

    if (answer.status === 304) {
      if (cached === null) {
        return { outcome: "shape", status: 304, reason: "304 with no cached contract to revalidate" };
      }
      const refreshed: ContractCacheEntry = { ...cached, fetchedAt: nowMs };
      await store.set(refreshed);
      return { outcome: "ok", doc: cached.doc, source: "revalidated", ageSeconds: 0, etag: cached.etag, contractVersion: cached.contractVersion };
    }
    if (answer.status === 200) {
      const doc = readTenantContract(answer.json);
      if (doc === null) {
        return { outcome: "shape", status: 200, reason: `${CONTRACT_ROUTE} returned a body that does not read as a TenantContract` };
      }
      const contractVersion = answer.headers.get(CONTRACT_VERSION_HEADER) ?? doc.contractVersion;
      const entry: ContractCacheEntry = { etag: answer.headers.get("etag"), fetchedAt: nowMs, contractVersion, doc };
      await store.set(entry);
      return { outcome: "ok", doc, source: "network", ageSeconds: 0, etag: entry.etag, contractVersion };
    }
    return classify(answer);
  }

  // ── The open reads ────────────────────────────────────────────────────────────────────────────

  /** A keyset list: the bare array body plus the page's headers. */
  function keysetPage(answer: Answer): (KeysetPageMeta & { rows: unknown[] }) | null {
    if (!Array.isArray(answer.json)) return null;
    const next = answer.headers.get(NEXT_CURSOR_HEADER);
    return {
      rows: answer.json,
      nextCursor: next === null ? null : pageCursor(next),
      total: integerHeader(answer.headers, TOTAL_HEADER),
      head: integerHeader(answer.headers, HEAD_SEQ_HEADER),
    };
  }

  /**
   * The mirror's list bodies are the read-model's own view rows; the only shape check a client can
   * make without re-declaring the view is "every row is an object". ⛔ ONE bad row fails the WHOLE
   * page (Codex #65 r1, P2): the cursor and the totals describe the server's page, so a silently
   * dropped row could never be recovered by paging — `ok` with fewer rows would be a lie.
   */
  function rowsOf<T extends object>(rows: unknown[]): T[] | null {
    const out: T[] = [];
    for (const r of rows) {
      if (!isRecord(r)) return null;
      out.push(asView<T>(r));
    }
    return out;
  }

  async function issuesList(params: IssueListParams = {}): Promise<IssueListResult> {
    const sent = await send(
      "GET",
      url("/api/v1/issues", {
        after: params.after,
        limit: params.limit,
        team_id: params.teamId,
        team_key: params.teamKey,
        state: params.state,
        priority: params.priority,
        assignee_id: params.assigneeId,
        assignee: params.assignee,
        cycle: params.cycle,
        blocked: params.blocked,
        sort: params.sort,
        waiting_on: params.waitingOn,
        waiting_mode: params.waitingMode,
      }),
      {},
    );
    if (!sent.ok) return sent.failure;
    if (sent.answer.status !== 200) return classify(sent.answer);
    const page = keysetPage(sent.answer);
    const rows = page === null ? null : rowsOf<IssueView>(page.rows);
    if (page === null || rows === null) {
      return { outcome: "shape", status: 200, reason: "GET /api/v1/issues did not answer an array of rows" };
    }
    return { outcome: "ok", rows, nextCursor: page.nextCursor, total: page.total, head: page.head };
  }

  async function issuesGet(identifier: string): Promise<IssueGetResult> {
    const sent = await send("GET", url(`/api/v1/issues/${encodeURIComponent(identifier)}`), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status === 404) return { outcome: "not-found", status: 404 };
    if (answer.status !== 200) return classify(answer);
    if (!isRecord(answer.json)) return { outcome: "shape", status: 200, reason: "GET /api/v1/issues/:identifier did not answer an object" };
    return { outcome: "ok", issue: asView<IssueDetailView>(answer.json), head: integerHeader(answer.headers, HEAD_SEQ_HEADER) };
  }

  async function pullsList(params: PullListParams = {}): Promise<PullListResult> {
    const sent = await send(
      "GET",
      url("/api/v1/pulls", { repo: params.repo, state: params.state, blocked: params.blocked, after: params.after, limit: params.limit }),
      {},
    );
    if (!sent.ok) return sent.failure;
    if (sent.answer.status !== 200) return classify(sent.answer);
    const page = keysetPage(sent.answer);
    const rows = page === null ? null : rowsOf<PullView>(page.rows);
    if (page === null || rows === null) {
      return { outcome: "shape", status: 200, reason: "GET /api/v1/pulls did not answer an array of rows" };
    }
    return {
      outcome: "ok",
      rows,
      nextCursor: page.nextCursor,
      total: page.total,
      blockedOnAskTotal: integerHeader(sent.answer.headers, BLOCKED_ON_ASK_TOTAL_HEADER),
      head: page.head,
    };
  }

  async function pullsGet(nodeId: string): Promise<PullGetResult> {
    const sent = await send("GET", url(`/api/v1/pulls/${encodeURIComponent(nodeId)}`), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status === 404) return { outcome: "not-found", status: 404 };
    if (answer.status !== 200) return classify(answer);
    if (!isRecord(answer.json)) return { outcome: "shape", status: 200, reason: "GET /api/v1/pulls/:id did not answer an object" };
    return { outcome: "ok", pull: asView<PullDetailView>(answer.json) };
  }

  async function projectsList(params: ProjectListParams = {}): Promise<ProjectListResult> {
    const sent = await send("GET", url("/api/v1/projects", { limit: params.limit, cursor: params.offset }), {});
    if (!sent.ok) return sent.failure;
    if (sent.answer.status !== 200) return classify(sent.answer);
    const rows = Array.isArray(sent.answer.json) ? rowsOf<ProjectView>(sent.answer.json) : null;
    if (rows === null) return { outcome: "shape", status: 200, reason: "GET /api/v1/projects did not answer an array of rows" };
    return { outcome: "ok", rows, head: integerHeader(sent.answer.headers, HEAD_SEQ_HEADER) };
  }

  async function me(): Promise<MeResult> {
    const sent = await send("GET", url("/api/v1/me"), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status !== 200) return classify(answer);
    const body = answer.json;
    if (!isRecord(body)) return { outcome: "shape", status: 200, reason: "GET /api/v1/me did not answer an object" };
    const account = stringField(body, "account");
    const slug = stringField(body, "slug");
    const name = stringField(body, "name");
    const permissions = body["permissions"];
    const principal = body["principal"];
    if (
      account === null ||
      slug === null ||
      name === null ||
      !(permissions === null || (Array.isArray(permissions) && permissions.every((p) => typeof p === "string"))) ||
      !(principal === "service" || principal === "session")
    ) {
      return { outcome: "shape", status: 200, reason: "GET /api/v1/me returned an unexpected shape" };
    }
    return { outcome: "ok", account, slug, name, permissions, principal };
  }

  async function linearIdentityCall(linearUserId?: string): Promise<LinearIdentityResult> {
    const sent = await send(linearUserId === undefined ? "GET" : "POST", url("/me/linear-identity"), {},
      linearUserId === undefined ? undefined : { linearUserId });
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    const reason = isRecord(answer.json) ? answer.json["error"] : undefined;
    if (answer.status === 409 && (reason === "already_resolved" || reason === "already_claimed" || reason === "identity_changed")) {
      return { outcome: "conflict", status: 409, reason };
    }
    if (answer.status !== 200) return classify(answer);
    const view = parseLinearIdentityView(answer.json);
    return view ? { outcome: "ok", ...view } : { outcome: "shape", status: 200, reason: "personal Linear identity returned an unexpected shape" };
  }

  // ── Personal provider consent ────────────────────────────────────────────────────────────────

  async function personalConnectionStart(provider: PersonalConnectionProvider): Promise<PersonalConnectionStartResult> {
    const sent = await send("GET", url(`/connect/${provider}/personal/start`), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status === 409 && isRecord(answer.json) && answer.json["error"] === "linear_workspace_required") {
      return { outcome: "workspace-required", status: 409 };
    }
    if (answer.status !== 200) return classify(answer);
    const body = answer.json;
    if (!isRecord(body) || typeof body["authorizationUrl"] !== "string" ||
        typeof body["expiresAt"] !== "number" || !Number.isFinite(body["expiresAt"])) {
      return { outcome: "shape", status: 200, reason: "personal consent start returned an unexpected shape" };
    }
    // The CLI may open this URL. Refuse a foreign or cross-provider URL even if a bad server answer
    // produced one, and never copy the signed handoff query into an error message.
    let consentUrl: URL;
    try {
      consentUrl = new URL(body["authorizationUrl"]);
    } catch {
      return { outcome: "shape", status: 200, reason: "personal consent start returned an invalid URL" };
    }
    if (consentUrl.origin !== new URL(origin).origin ||
        consentUrl.pathname !== `/connect/${provider}/personal/start` ||
        !consentUrl.searchParams.get("handoff") ||
        !["https:", "http:"].includes(consentUrl.protocol)) {
      return { outcome: "shape", status: 200, reason: "personal consent start returned a URL outside this provider" };
    }
    return { outcome: "ok", status: 200, authorizationUrl: body["authorizationUrl"], expiresAt: body["expiresAt"] };
  }

  async function personalConnectionStatus(provider: PersonalConnectionProvider): Promise<PersonalConnectionStatusResult> {
    const sent = await send("GET", url(`/me/connections/${provider}/personal`), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status === 503 && isRecord(answer.json) &&
        answer.json["error"] === `${provider}_grant_check_unavailable`) {
      return { outcome: "unavailable", status: 503, provider };
    }
    if (answer.status !== 200) return classify(answer);
    const body = answer.json;
    const malformed = (): PersonalConnectionStatusResult => ({
      outcome: "shape", status: 200, reason: "personal connection status returned an unexpected shape",
    });
    if (!isRecord(body)) return malformed();
    if (body["connected"] === false) {
      if (!("reason" in body)) return { outcome: "absent", status: 200 };
      if (body["reason"] === "lapsed" &&
          (body["lapsedAt"] === null ||
            (typeof body["lapsedAt"] === "number" && Number.isFinite(body["lapsedAt"])))) {
        return { outcome: "lapsed", status: 200, lapsedAt: body["lapsedAt"] };
      }
      return malformed();
    }
    if (body["connected"] !== true ||
        typeof body["updatedAt"] !== "number" || !Number.isFinite(body["updatedAt"]) ||
        !(body["expiresAt"] === null ||
          (typeof body["expiresAt"] === "number" && Number.isFinite(body["expiresAt"])))) {
      return malformed();
    }
    if (provider === "linear") {
      if (typeof body["linearUserId"] !== "string" ||
          !(body["grantedScope"] === null || typeof body["grantedScope"] === "string")) return malformed();
      return {
        outcome: "connected", status: 200, provider,
        linearUserId: body["linearUserId"], grantedScope: body["grantedScope"],
        updatedAt: body["updatedAt"], expiresAt: body["expiresAt"],
      };
    }
    if (typeof body["githubUserId"] !== "string" || typeof body["githubLogin"] !== "string") return malformed();
    return {
      outcome: "connected", status: 200, provider,
      githubUserId: body["githubUserId"], githubLogin: body["githubLogin"],
      updatedAt: body["updatedAt"], expiresAt: body["expiresAt"],
    };
  }

  // ── The agent proxy ───────────────────────────────────────────────────────────────────────────

  /**
   * One implementation under every typed verb: resolve the route from the (cached) contract, send
   * the input as the body (POST) or the query (GET), and hand the body back under the route's own
   * success table — the set of `outcome` literals this route is documented to answer with a 2xx OR a
   * mapped error status. Any other answer goes through `classify`.
   */
  async function callAgentRoute(
    name: AgentRouteName,
    input: Record<string, unknown>,
    accept: (body: Record<string, unknown>) => boolean,
  ): Promise<AgentCallResult> {
    const doc = await contract();
    if (doc.outcome !== "ok") return { ok: false, from: "contract", failure: doc };
    const route = routeByName(doc.doc, name);
    if (route === null) {
      return { ok: false, from: "contract", failure: { outcome: "route-unknown", route: name, routes: doc.doc.routes.map((r) => r.path) } };
    }
    const defined = Object.fromEntries(Object.entries(input).filter(([, v]) => v !== undefined));
    const sent =
      route.method === "GET"
        ? await send("GET", url(route.path, stringify(defined)), {})
        : await send("POST", url(route.path), {}, defined);
    if (!sent.ok) return { ok: false, from: "transport", failure: sent.failure };
    const { answer } = sent;
    if (isRecord(answer.json) && accept(answer.json)) return { ok: true, body: answer.json, status: answer.status };
    return { ok: false, from: "route", failure: classify(answer) };
  }

  /** A route's success table: the `outcome` literals it answers with a 2xx OR a mapped error status. */
  function outcomes(...names: readonly string[]): (body: Record<string, unknown>) => boolean {
    return (body) => typeof body["outcome"] === "string" && names.includes(body["outcome"]);
  }

  /** These routes answer a missing/foreign project as a 404 `{error}` body; `classify` folds that
   *  into the `http` catch-all, so lift it to its own arm. Every other status keeps `classify`'s
   *  mapping — 403 → `forbidden`, 400 `{error}` → `rejected`, 409/503 → `http` — each carrying the
   *  route's own error literal as `reason` (reasonOf reads `reason ?? error ?? message`).
   *
   *  ⛔ ONLY the ROUTE's own answer is lifted. `callAgentRoute` fails for three different reasons,
   *  and a 404 from the contract fetch (a wrong `baseUrl`, a tenant whose contract endpoint is not
   *  deployed) or a transport failure says nothing about the caller's project — reporting either as
   *  `not-found` would state "this project is absent, archived, or another tenant's" about a call
   *  that never reached the route. Those keep the arm every other agent verb returns for them. */
  function notFound(r: Extract<AgentCallResult, { ok: false }>): { outcome: "not-found"; status: 404; reason: string } | null {
    return r.from === "route" && r.failure.outcome === "http" && r.failure.status === 404
      ? { outcome: "not-found", status: 404, reason: r.failure.reason }
      : null;
  }

  function stringify(input: Record<string, unknown>): Record<string, string> {
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(input)) out[k] = String(v);
    return out;
  }

  /** The body, verbatim, with the status stamped on — the pass-through the mirror's own
   *  response-shape discipline asks for ("the primitive's own discriminated outcome goes back to the
   *  caller near-verbatim"). One narrowing site per verb, typed by the route's declared union. */
  function stamped<T extends object>(body: Record<string, unknown>, status: number): T {
    return { ...body, status } as T;
  }

  const WRITE_OUTCOMES = outcomes("succeeded", "exhausted");

  const agent: TenantClient["agent"] = {
    async issueState(input) {
      const r = await callAgentRoute("issue-state", { ...input }, WRITE_OUTCOMES);
      return r.ok ? stamped<Extract<IssueStateResult, ProxiedWriteOutcome>>(r.body, r.status) : r.failure;
    },
    async issueLabel(input) {
      const r = await callAgentRoute("issue-label", { ...input }, outcomes("succeeded", "failed"));
      return r.ok ? stamped<Extract<IssueLabelResult, { results: unknown }>>(r.body, r.status) : r.failure;
    },
    async issueComment(input) {
      const r = await callAgentRoute("issue-comment", { ...input }, WRITE_OUTCOMES);
      return r.ok ? stamped<Extract<IssueCommentResult, ProxiedWriteOutcome>>(r.body, r.status) : r.failure;
    },
    async issueCreate(input) {
      const r = await callAgentRoute("issue-create", { ...input }, WRITE_OUTCOMES);
      return r.ok ? stamped<Extract<IssueCreateResult, { outcome: "succeeded" | "exhausted" }>>(r.body, r.status) : r.failure;
    },
    async reaction(input) {
      const r = await callAgentRoute("reaction", { ...input }, WRITE_OUTCOMES);
      return r.ok ? stamped<Extract<ReactionResult, { outcome: "succeeded" | "exhausted" }>>(r.body, r.status) : r.failure;
    },
    async attachment(input) {
      const r = await callAgentRoute("attachment", { ...input }, WRITE_OUTCOMES);
      return r.ok ? stamped<Extract<AttachmentResult, ProxiedWriteOutcome>>(r.body, r.status) : r.failure;
    },
    async attachments(input) {
      const r = await callAgentRoute("attachments", { ...input }, outcomes("succeeded", "rejected", "unauthorized"));
      if (!r.ok) return r.failure;
      const reason = stringField(r.body, "reason") ?? "";
      if (r.body["outcome"] === "rejected") return { outcome: "not-found", status: 404, reason };
      if (r.body["outcome"] === "unauthorized") return { outcome: "unauthorized-upstream", status: r.status, reason };
      return stamped<Extract<AttachmentsResult, { attachments: unknown }>>(r.body, r.status);
    },
    async session(input) {
      // The composite has no top-level `outcome`; its sub-results carry their own. Every status the
      // route produces (200/400/502) carries the same body shape, so the table is "has a session".
      const r = await callAgentRoute("session", { ...input }, (body) => isRecord(body["session"]));
      if (!r.ok) return r.failure;
      return { outcome: "ok", ...stamped<Omit<Extract<SessionResult, { outcome: "ok" }>, "outcome">>(r.body, r.status) };
    },
    async ask(input) {
      const r = await callAgentRoute("ask", { ...input }, outcomes("created", "created-but-relations-partial", "exhausted"));
      return r.ok ? stamped<Extract<AskResult, { askId: unknown } | { outcome: "exhausted" }>>(r.body, r.status) : r.failure;
    },
    async askAccept(input) {
      const r = await callAgentRoute("ask-accept", { ...input }, outcomes("recorded", "refused", "record-failed"));
      return r.ok ? stamped<Extract<AskAcceptResult, { outcome: "recorded" | "refused" | "record-failed" }>>(r.body, r.status) : r.failure;
    },
    async portalServerRegister(input) {
      const r = await callAgentRoute("portal-servers/register", { ...input },
        (body) => body["outcome"] === "registered" && isPortalServer(body["server"]));
      if (r.ok && (r.status < 200 || r.status >= 300)) {
        return { outcome: "http", status: r.status, reason: "portal server request failed" };
      }
      return r.ok ? stamped<Extract<PortalServerRegisterResult, { outcome: "registered" }>>(r.body, r.status) : r.failure;
    },
    async portalServers() {
      const r = await callAgentRoute("portal-servers", {},
        (body) => body["outcome"] === "ok" && Array.isArray(body["servers"]) && body["servers"].every(isPortalServer));
      if (r.ok && (r.status < 200 || r.status >= 300)) {
        return { outcome: "http", status: r.status, reason: "portal server request failed" };
      }
      return r.ok ? stamped<Extract<PortalServersResult, { outcome: "ok" }>>(r.body, r.status) : r.failure;
    },
    async portalServerRemove(input) {
      const r = await callAgentRoute("portal-servers/remove", { ...input },
        (body) => body["outcome"] === "removed" && typeof body["removed"] === "boolean");
      if (r.ok && (r.status < 200 || r.status >= 300)) {
        return { outcome: "http", status: r.status, reason: "portal server request failed" };
      }
      return r.ok ? stamped<Extract<PortalServerRemoveResult, { outcome: "removed" }>>(r.body, r.status) : r.failure;
    },
    async projectRepositoryRegister(input) {
      const r = await callAgentRoute("project-repositories", { ...input }, (body) => isRecord(body["registered"]));
      if (!r.ok) return notFound(r) ?? r.failure;
      return { outcome: "registered", ...stamped<Omit<Extract<ProjectRepositoryRegisterResult, { outcome: "registered" }>, "outcome">>(r.body, r.status) };
    },
    async projectRepositoryRemove(input) {
      const r = await callAgentRoute("project-repositories/remove", { ...input }, (body) => typeof body["removed"] === "boolean");
      if (!r.ok) return notFound(r) ?? r.failure;
      return { outcome: "removed", ...stamped<Omit<Extract<ProjectRepositoryRemoveResult, { outcome: "removed" }>, "outcome">>(r.body, r.status) };
    },
  };

  async function teamCall<T extends object>(method: "GET" | "POST", path: string, body: unknown, guard: (value: unknown) => value is T): Promise<TeamWorkflowResult<T>> {
    const sent = await send(method, url(path), {}, body);
    if (!sent.ok) return sent.failure;
    const answer = sent.answer;
    const error = isRecord(answer.json) ? stringField(answer.json, "error") : null;
    if (answer.status < 200 || answer.status >= 300) {
      const failure = classify(answer);
      const reason = isRecord(answer.json)
        ? stringField(answer.json, "reason") ?? stringField(answer.json, "message") ?? error ?? answer.textHead
        : answer.textHead;
      // Named 404/409 refusals are actionable in the CLI, not generic HTTP failures.
      if (error !== null && (answer.status === 404 || answer.status === 409)) {
        return { outcome: "rejected", status: answer.status, error, reason };
      }
      return { ...failure, reason, ...(error === null ? {} : { error }) };
    }
    if (!guard(answer.json)) return { outcome: "shape", status: answer.status, reason: "unexpected team workflow response" };
    return { ...answer.json, outcome: "ok", status: answer.status };
  }

  function invalidHash(): TeamWorkflowFailure {
    return { outcome: "rejected", status: 0, error: "invalid-hash", reason: "use the token from the preview" };
  }
  const teamPath = "/api/v1/agent/team-workflow";
  const teamPost = <T extends object>(path: string, body: unknown, guard: (value: unknown) => value is T) => teamCall("POST", path, body, guard);
  const teamWorkflow: TenantClient["teamWorkflow"] = {
    teams: () => teamCall("GET", "/api/v1/agent/teams", undefined, teamList),
    get: (team) => teamCall("GET", `${teamPath}?team=${encodeURIComponent(team)}`, undefined, teamView),
    check: (team) => teamPost(`${teamPath}/check`, { team },
      (value): value is { readiness: TeamWorkflowReadiness; ask: Record<string, unknown> } => isRecord(value) && teamReadiness(value["readiness"]) && isRecord(value["ask"]) && typeof value["ask"]["outcome"] === "string"),
    save: (input) => SHA256_HEX.test(input.expectedMappingHash)
      ? teamPost(`${teamPath}/save`, input, (value): value is TeamWorkflowSaveResult => teamWriteView(value))
      : Promise.resolve(invalidHash()),
    adoptPreview: (team) => teamPost(`${teamPath}/adopt`, { team, mode: "preview" }, (value): value is TeamAdoptResult => teamAdopt(value, false)),
    adoptApply: (team, planHash) => WORKFLOW_STALENESS_TOKEN.test(planHash)
      ? teamPost(`${teamPath}/adopt`, { team, mode: "apply", planHash }, (value): value is TeamAdoptResult => teamAdopt(value, true))
      : Promise.resolve(invalidHash()),
    undoPreview: (team) => teamPost(`${teamPath}/adopt-undo`, { team, mode: "preview" },
      (value): value is TeamUndoPreview => isRecord(value) && typeof value["teamId"] === "string" && value["mode"] === "preview" &&
        Array.isArray(value["candidates"]) && value["candidates"].every((row: unknown) => isRecord(row) && typeof row["stateId"] === "string" && (row["name"] === null || typeof row["name"] === "string")) &&
        typeof value["undoHash"] === "string" && SHA256_HEX.test(value["undoHash"])),
    undoApply: (team, undoHash) => SHA256_HEX.test(undoHash)
      ? teamPost(`${teamPath}/adopt-undo`, { team, mode: "apply", undoHash },
        (value): value is TeamUndoResult => isRecord(value) && Array.isArray(value["archived"]) && value["archived"].every((row: unknown) => stateResult(row, false, true)) && Array.isArray(value["kept"]) && value["kept"].every((row: unknown) => stateResult(row, true, true)) && Array.isArray(value["failed"]) && value["failed"].every((row: unknown) => stateResult(row, true, true)) && (value["readiness"] === null || teamReadiness(value["readiness"])))
      : Promise.resolve(invalidHash()),
    migratePreview: (team, choices = []) => teamPost(`${teamPath}/migrate`, { team, step: "preview", choices },
      (value): value is { preview: TeamMigrationPlan } => isRecord(value) && isRecord(value["preview"]) && typeof value["preview"]["teamId"] === "string" && Array.isArray(value["preview"]["sources"]) && value["preview"]["sources"].every(migrationSource) && typeof value["preview"]["migrationHash"] === "string" && WORKFLOW_STALENESS_TOKEN.test(value["preview"]["migrationHash"] as string) && typeof value["preview"]["overLimit"] === "boolean" && typeof value["preview"]["issueCount"] === "number" && typeof value["preview"]["retireLogReadable"] === "boolean"),
    migrateChunk: (team, migrationHash, choices = []) => WORKFLOW_STALENESS_TOKEN.test(migrationHash)
      ? teamPost(`${teamPath}/migrate`, { team, step: "migrate", migrationHash, choices },
        (value): value is TeamMigrationChunk => isRecord(value) && typeof value["teamId"] === "string" && Array.isArray(value["sources"]) && value["sources"].every(migrationSource) && typeof value["remaining"] === "number" && typeof value["migrationHash"] === "string" && WORKFLOW_STALENESS_TOKEN.test(value["migrationHash"]) && typeof value["moved"] === "number" && (value["readiness"] === null || teamReadiness(value["readiness"])))
      : Promise.resolve(invalidHash()),
    migrateRetire: (team, migrationHash, choices = []) => WORKFLOW_STALENESS_TOKEN.test(migrationHash)
      ? teamPost(`${teamPath}/migrate`, { team, step: "retire", migrationHash, choices },
        (value): value is TeamMigrationRetire => isRecord(value) && Array.isArray(value["retired"]) && value["retired"].every((row: unknown) => stateResult(row, false, false)) && Array.isArray(value["kept"]) && value["kept"].every((row: unknown) => stateResult(row, true, false)) && Array.isArray(value["failed"]) && value["failed"].every((row: unknown) => stateResult(row, true, false)) && Array.isArray(value["logGaps"]) && value["logGaps"].every((row: unknown) => isRecord(row) && typeof row["stateId"] === "string" && typeof row["reason"] === "string") && (value["readiness"] === null || teamReadiness(value["readiness"])))
      : Promise.resolve(invalidHash()),
  };

  return {
    contract,
    me,
    linearIdentity: { get: () => linearIdentityCall(), set: (linearUserId) => linearIdentityCall(linearUserId) },
    personalConnections: { start: personalConnectionStart, status: personalConnectionStatus },
    teamWorkflow,
    issues: { list: issuesList, get: issuesGet },
    pulls: { list: pullsList, get: pullsGet },
    projects: { list: projectsList },
    agent,
  };
}

/** The client. See {@link createTenantClient}. */
export interface TenantClient {
  /** Personal bearer team setup. The key's tenant and admin role are enforced by the cloud. */
  teamWorkflow: {
    teams(): Promise<TeamWorkflowResult<TeamList>>;
    get(team: string): Promise<TeamWorkflowResult<TeamWorkflowView>>;
    check(team: string): Promise<TeamWorkflowResult<{ readiness: TeamWorkflowReadiness; ask: Record<string, unknown> }>>;
    save(input: TeamMappingSaveInput): Promise<TeamWorkflowResult<TeamWorkflowSaveResult>>;
    adoptPreview(team: string): Promise<TeamWorkflowResult<TeamAdoptResult>>;
    adoptApply(team: string, planHash: string): Promise<TeamWorkflowResult<TeamAdoptResult>>;
    undoPreview(team: string): Promise<TeamWorkflowResult<TeamUndoPreview>>;
    undoApply(team: string, undoHash: string): Promise<TeamWorkflowResult<TeamUndoResult>>;
    migratePreview(team: string, choices?: TeamMigrationChoice[]): Promise<TeamWorkflowResult<{ preview: TeamMigrationPlan }>>;
    migrateChunk(team: string, migrationHash: string, choices?: TeamMigrationChoice[]): Promise<TeamWorkflowResult<TeamMigrationChunk>>;
    migrateRetire(team: string, migrationHash: string, choices?: TeamMigrationChoice[]): Promise<TeamWorkflowResult<TeamMigrationRetire>>;
  };
  /** Self-service unmatched identity recovery. Personal credentials only. */
  linearIdentity: {
    get(): Promise<LinearIdentityResult>;
    /** Records only the authenticated member's choice. Automatically resolved identities refuse. */
    set(linearUserId: string): Promise<LinearIdentityResult>;
  };
  /** The tenant's fact document, cached per its own `cache` policy. */
  contract(opts?: ContractOptions): Promise<ContractResult>;
  /** `GET /api/v1/me` — the account this key belongs to. */
  me(): Promise<MeResult>;
  /** User-scoped GitHub and Linear OAuth grants. Use a personal key or device-login credential. */
  personalConnections: {
    /** Returns a short lived URL to open in the user's browser. Does not perform consent. */
    start(provider: PersonalConnectionProvider): Promise<PersonalConnectionStartResult>;
    /** A provider outage is `unavailable`, never `absent`. */
    status(provider: PersonalConnectionProvider): Promise<PersonalConnectionStatusResult>;
  };
  issues: {
    /** `GET /api/v1/issues` — keyset-paged; follow `nextCursor` until it is `null`. */
    list(params?: IssueListParams): Promise<IssueListResult>;
    /** `GET /api/v1/issues/:identifier`. */
    get(identifier: string): Promise<IssueGetResult>;
  };
  pulls: {
    list(params?: PullListParams): Promise<PullListResult>;
    /** `GET /api/v1/pulls/:id` — `id` is the PR's GitHub node id. */
    get(nodeId: string): Promise<PullGetResult>;
  };
  projects: {
    list(params?: ProjectListParams): Promise<ProjectListResult>;
  };
  /** The `/api/v1/agent/*` proxy — every tenant write as the app actor, path from `routes[]`. */
  agent: {
    issueState(input: IssueStateInput): Promise<IssueStateResult>;
    issueLabel(input: IssueLabelInput): Promise<IssueLabelResult>;
    issueComment(input: IssueCommentInput): Promise<IssueCommentResult>;
    issueCreate(input: IssueCreateInput): Promise<IssueCreateResult>;
    reaction(input: ReactionInput): Promise<ReactionResult>;
    attachment(input: AttachmentInput): Promise<AttachmentResult>;
    /** `GET …/attachments?issueId=` — the soft-CAS read-back; no write-budget unit. */
    attachments(input: { issueId: string }): Promise<AttachmentsResult>;
    session(input: SessionInput): Promise<SessionResult>;
    ask(input: AskInput): Promise<AskResult>;
    askAccept(input: AskAcceptInput): Promise<AskAcceptResult>;
    portalServerRegister(input: PortalServerRegisterInput): Promise<PortalServerRegisterResult>;
    portalServers(): Promise<PortalServersResult>;
    portalServerRemove(input: { name: string }): Promise<PortalServerRemoveResult>;
    projectRepositoryRegister(input: ProjectRepositoryInput): Promise<ProjectRepositoryRegisterResult>;
    projectRepositoryRemove(input: ProjectRepositoryInput): Promise<ProjectRepositoryRemoveResult>;
  };
}
