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
//   • CTC-2132 — the routes the bundle's own `src/http.ts` transport used to hand-roll: GET
//     /issues/:id/execution, /work-eligibility, /dispatch-queue/current, /fleet-activity/current,
//     /agent-roster/current, /lease/attributions, /coding-accounts (the `diagnostics.*` namespace +
//     `issues.execution`), /cycles, /search, /workflow-stages, /changes (NDJSON — `changes.stream`/
//     `changes.list`), /snapshot?head=1 (`snapshot.head`), plus a generic `request()` escape hatch for
//     any route not otherwise enumerated. This package does not depend on `catalyst-cloud`, so every
//     one of these response types is declared open (an index signature) and validated only by its
//     CONTAINER shape at runtime — never a field — so a cloud-side field addition is never a `shape`
//     refusal (see the "diagnosis / telemetry reads" and "query reads" sections below).
//   • CTC-2111/CTC-2132 — `createTenantClient` accepts either `key: string` (unchanged) or the
//     `AuthStrategy` from `./live-sync-client.js` (`token` | `cookie` | `bearer`), resolved fresh on
//     EVERY request — never captured once, since a `bearer` token rotates (~15 min).
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
import { AuthError, type AuthStrategy } from "./live-sync-client.js";
import { iterateNdjson } from "./ndjson.js";
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

export interface TenantClientBaseOptions {
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

/**
 * Exactly one credential. `key` is the CTC-2004 shape, kept working with identical wire behaviour;
 * `auth` is the CTC-2111 {@link AuthStrategy} the bundle's single credential provider drives
 * (CTC-2132) — `token`/`cookie`/`bearer`, resolved per request exactly as
 * `CatalystReplica.feedHeaders()` resolves it (src/replica/catalyst-replica.ts). Internally `key` is
 * normalized to `{kind:"token", token:key}` at construction, so `send()` never sees two shapes.
 */
export type TenantClientOptions = TenantClientBaseOptions &
  (
    | {
        /** The tenant key. Reads accept any key with `mirror:read`; every `/api/v1/agent/*` route
         *  needs an organization-tier key (`ctc_acct_*`) — a workstation key is refused `403
         *  not-machine-principal`. */
        key: string;
        auth?: never;
      }
    | { auth: AuthStrategy; key?: never }
  );

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

/** `GET /api/v1/issues/:id/execution` — the ticket's own execution/telemetry report. ⛔ The fields
 *  below are DOCUMENTED, not compiled: this repo does not depend on `catalyst-cloud`. The index
 *  signature is deliberate (Decision 4, CTC-2132) — a field the cloud adds must reach the caller, not
 *  be refused as a shape error. The tenant decides which principals may read this route (Decision 5);
 *  the client reports whatever it answers. */
export interface TicketExecutionReport {
  readonly identifier?: string;
  readonly [key: string]: unknown;
}
export type TicketExecutionResult =
  | { outcome: "ok"; status: 200; report: TicketExecutionReport }
  | { outcome: "not-found"; status: 404 }
  | TenantClientFailure;

// ── The diagnosis / telemetry reads — CTC-2132 (Ryan/M2: customer delegate agents diagnosing and
//    unsticking their own work). Seven literal-path open reads, same pattern as `me()`/`issuesList`:
//    no contract fetch, `classify()` every non-2xx. Every interface below carries an index signature
//    and validates only its CONTAINER shape at runtime (Decision 4) — never an individual field —
//    because this repo does not depend on `catalyst-cloud` and cannot verify the field list compiles
//    against the cloud's own view. Decision 5: no doc comment here states which principal class may
//    call a route; a 401/403 already folds through `classify()` with the server's own reason. ─────

/** `GET /api/v1/work-eligibility?team=` — the dispatcher's own "why is nothing moving" answer.
 *  ⛔ DOCUMENTED, not compiled — see the section note above. */
export interface WorkEligibilityReport {
  readonly team?: string;
  readonly [key: string]: unknown;
}
export type WorkEligibilityResult = { outcome: "ok"; status: 200; report: WorkEligibilityReport } | TenantClientFailure;

/** `GET /api/v1/dispatch-queue/current?team=` — an ENVELOPE (`entries[]`), not a bare array. */
export interface DispatchQueueEnvelope {
  readonly entries: readonly Record<string, unknown>[];
  readonly [key: string]: unknown;
}
export type DispatchQueueResult = { outcome: "ok"; status: 200; queue: DispatchQueueEnvelope } | TenantClientFailure;

/** `GET /api/v1/fleet-activity/current` — a bare array of activity rows. */
export type FleetActivityResult =
  | { outcome: "ok"; status: 200; rows: readonly Record<string, unknown>[] }
  | TenantClientFailure;

/** `GET /api/v1/agent-roster/current` — a bare array of roster rows. */
export type AgentRosterResult =
  | { outcome: "ok"; status: 200; rows: readonly Record<string, unknown>[] }
  | TenantClientFailure;

/** `GET /api/v1/lease/attributions?ticket=&phase=` — both params are required (the route 400s when
 *  either is absent); the body shape is tolerant (object or array). */
export type LeaseAttributionsResult = { outcome: "ok"; status: 200; body: unknown } | TenantClientFailure;

/** `GET /api/v1/coding-accounts` — an OBJECT carrying an `accounts` array. */
export interface CodingAccountsReport {
  readonly accounts: readonly Record<string, unknown>[];
  readonly [key: string]: unknown;
}
export type CodingAccountsResult = { outcome: "ok"; status: 200; report: CodingAccountsReport } | TenantClientFailure;

// ── The query reads — CTC-2132 Phase 4: cycles, search, workflow-stages, changes (NDJSON), snapshot
//    head. `@catalyst-cloud/read-model` publishes IssueView/PullView/ProjectView/InitiativeView and
//    NOT CycleView or SearchView (verified against the installed package on 2026-09-18: no
//    src/cycles.ts, no src/search.ts). Same precedent as `TenantContract`: the SDK carries its own
//    structural copy, open by Decision 4 (an index signature, no per-field validation). If
//    `@catalyst-cloud/read-model` ever publishes these views, replace with an `import type`. ───────

/** `GET /api/v1/cycles` — the SDK's own structural copy; see the section note above. */
export interface CycleRow {
  readonly id?: string;
  readonly [key: string]: unknown;
}
export type CyclesListResult = { outcome: "ok"; status: 200; rows: readonly CycleRow[] } | TenantClientFailure;

/** `GET /api/v1/search?q=&limit=` — four result buckets, the SDK's own structural copy. */
export interface SearchParams {
  q: string;
  limit?: number;
}
export interface SearchResults {
  readonly issues?: readonly Record<string, unknown>[];
  readonly pulls?: readonly Record<string, unknown>[];
  readonly projects?: readonly Record<string, unknown>[];
  readonly initiatives?: readonly Record<string, unknown>[];
  readonly [key: string]: unknown;
}
export type SearchResult = { outcome: "ok"; status: 200; results: SearchResults } | TenantClientFailure;

/** `GET /api/v1/workflow-stages` — the key-authenticated twin; its shape is never quoted anywhere in
 *  the pool (Decision 7). Tolerant: accepts `{stages, source?}` OR a bare array, normalizing both. */
export type WorkflowStagesResult =
  | { outcome: "ok"; status: 200; stages: readonly Record<string, unknown>[]; source: string | null }
  | TenantClientFailure;

/** `GET /api/v1/changes?since=` — the NDJSON change feed. */
export type ChangesStreamResult =
  | { outcome: "ok"; status: 200; head: number | null; rows: AsyncGenerator<Record<string, unknown>> }
  /** The tenant can no longer replay from `since` — reseed from `snapshot.head()` (or a full
   *  snapshot) rather than replaying the gap. `head` is the tenant's current head. */
  | { outcome: "resync"; status: 409; head: number | null; reason: string }
  | TenantClientFailure;
export type ChangesListResult =
  | { outcome: "ok"; status: 200; head: number | null; rows: Record<string, unknown>[] }
  | Extract<ChangesStreamResult, { outcome: "resync" }>
  | TenantClientFailure;

/** `GET /api/v1/snapshot?head=1` — the cheap head probe (Decision 7): the shape was inferred from
 *  prose, not a quoted response body, so the parse is deliberately tolerant of three answer shapes. */
export type SnapshotHeadResult =
  | { outcome: "ok"; status: 200; head: number; source: "header" | "body" }
  | { outcome: "shape"; status: number; reason: string }
  | TenantClientFailure;

// ── The generic escape hatch — CTC-2132 Phase 5 ─────────────────────────────────────────────────

export interface RawRequest {
  method?: "GET" | "POST";
  /** ⛔ An ABSOLUTE PATH under the client's own origin ("/api/v1/…"), never a URL. A value that is
   *  not one is refused BEFORE the request is built: `new URL(origin + path)` does not throw on
   *  "@host/x" — it reads the origin as USERINFO and `host` becomes the attacker's, which would send
   *  this client's bearer credential to that host (reproduced: `new URL("https://cloud.example" +
   *  "@evil.example/steal")` → host `evil.example`, no throw). CTC-2132. */
  path: string;
  query?: Record<string, string | number | undefined>;
  headers?: Record<string, string>;
  body?: unknown;
}
export type RawRequestResult =
  | { outcome: "ok"; status: number; json: unknown; headers: Headers }
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
  | "project-repositories/remove";

/** The runtime twin of {@link AgentRouteName}: the list a test can walk against the contract.
 *  Kept in lockstep with the union by a compile-time equality in test/tenant-client-agent.test.ts. */
export const AGENT_ROUTE_NAMES = [
  "issue-state", "issue-label", "issue-comment", "issue-create", "reaction",
  "attachment", "attachments", "session", "ask", "ask-accept",
  "project-repositories", "project-repositories/remove",
] as const satisfies readonly AgentRouteName[];

//   ⏳ DEFERRED TO CTC-2156 — the customer release routes. CTC-2132's M2 addendum asks for typed
//      `agent.ticketRelease` / `agent.ticketReleaseClass` with the refusal shape. The routes DO NOT
//      EXIST in the cloud yet (verified 2026-09-18: CTC-2156 has a research doc and a plan and no
//      implement/validation document anywhere in the thoughts pool), and CTC-2156's own plan states
//      the dependency runs one way — "CTC-2132: not a dependency". When they ship, both are plain
//      `callAgentRoute` extensions and need NO new machinery: their bodies already carry a top-level
//      `outcome`, so `outcomes(...)` works directly and no discriminator synthesis (the
//      `agent.session` / `projectRepositoryRegister` pattern) is needed.
//        POST …/ticket-release        {ticket, because, retryUnchanged?, dryRun?}
//          → {ticket, outcome: "released"|"refused"|"nothing-held", released[], refused[],
//             evidence, notHeld?, auditId}
//        POST …/ticket-release-class  {team, class, because, retryUnchanged?, limit?, dryRun?}
//          → {released[], refused[], truncated}   (capped at 25 tickets per call)
//      Add both to `AgentRouteName` AND `AGENT_ROUTE_NAMES` (the compile-time lockstep in
//      test/tenant-client-agent.test.ts will fail if only one is updated), add a fixture row to
//      test/fixtures/tenant-contract.fixture.json WITHOUT bumping the fixture's own
//      `contractVersion` (the fixture is route-partial by design, CTC-2562 decision 3).

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

type Sent =
  | { ok: true; answer: Answer }
  | { ok: false; failure: Extract<TenantClientFailure, { outcome: "network" | "unauthorized" }> };

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

export function createTenantClient(opts: TenantClientOptions): TenantClient {
  const fetchImpl = opts.fetch ?? fetch;
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const store = opts.contractCache ?? memoryContractCache();
  const now = opts.now ?? Date.now;
  const origin = normalizeBaseUrl(opts.baseUrl);
  const auth: AuthStrategy = opts.auth ?? { kind: "token", token: opts.key as string };

  function url(path: string, query?: Record<string, string | number | undefined>): string {
    const u = new URL(`${origin}${path}`);
    for (const [k, v] of Object.entries(query ?? {})) {
      if (v !== undefined) u.searchParams.set(k, String(v));
    }
    return u.toString();
  }

  /** Async because a `bearer` token rotates (~15 min) and must be resolved FRESH per request — the
   *  same rule `CatalystReplica.feedHeaders()` follows (src/replica/catalyst-replica.ts:1416). */
  async function authHeaders(): Promise<Record<string, string>> {
    if (auth.kind === "token") return { authorization: `Bearer ${auth.token}` };
    if (auth.kind === "cookie") return {};
    let token: string;
    try {
      token = await auth.getToken();
    } catch (err) {
      throw new AuthError(401, `bearer getToken() rejected: ${err instanceof Error ? err.message : String(err)}`);
    }
    return { authorization: `Bearer ${token}` };
  }

  async function send(
    method: string,
    target: string,
    extraHeaders: Record<string, string>,
    body?: unknown,
  ): Promise<Sent> {
    let resolved: Record<string, string>;
    try {
      resolved = await authHeaders();
    } catch (err) {
      const reason = err instanceof AuthError ? err.message : err instanceof Error ? err.message : String(err);
      return { ok: false, failure: { outcome: "unauthorized", status: 401, reason, ref: null } };
    }
    const headers: Record<string, string> = { ...resolved, accept: "application/json", ...extraHeaders };
    if (body !== undefined) headers["content-type"] = "application/json";
    let res: Response;
    let text: string;
    try {
      res = await fetchImpl(target, {
        method,
        headers,
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.timeout(timeoutMs),
        ...(auth.kind === "cookie" ? { credentials: "include" as const } : {}),
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

  /**
   * An IDLE deadline for the streaming path, NOT the wall-clock one `AbortSignal.timeout()` gives.
   * ⛔ `AbortSignal.timeout(timeoutMs)` keeps governing the response BODY once the headers have
   * arrived, so it truncated any `/changes` replay that took longer than `timeoutMs` to drain and
   * lost every row already read (CTC-2132 validate attempt 1, code-review Finding 2) — which defeats
   * the whole point of `changes.stream`. The timer is rearmed on every chunk (`iterateNdjson`'s
   * `onProgress` refund, the same one the replica's snapshot seed uses), so a feed that keeps
   * delivering never expires while a feed that STALLS for `timeoutMs` still does. `release()` clears
   * it when the stream ends, normally or not.
   */
  function idleDeadline(): { signal: AbortSignal; rearm: () => void; release: () => void } {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const release = (): void => {
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
    };
    const rearm = (): void => {
      if (controller.signal.aborted) return;
      release();
      timer = setTimeout(() => {
        timer = undefined;
        controller.abort(new Error(`The operation timed out after ${timeoutMs}ms without progress.`));
      }, timeoutMs);
    };
    rearm();
    return { signal: controller.signal, rearm, release };
  }

  interface RawDeadline {
    /** Refund the idle deadline — call per chunk read off the body. */
    rearm: () => void;
    /** Stop the deadline; MUST be called on every exit path once the body is done with. */
    release: () => void;
  }

  /** A raw-response sibling of `send()`: resolves auth exactly as `send()` does but hands back the
   *  `Response` UNREAD, so an NDJSON body can be streamed instead of buffered through `text()`. The
   *  returned `deadline` is the caller's to drive — see {@link idleDeadline}. */
  async function sendRaw(
    method: string,
    target: string,
    extraHeaders: Record<string, string>,
    body?: unknown,
  ): Promise<
    | { ok: true; res: Response; deadline: RawDeadline }
    | { ok: false; failure: Extract<TenantClientFailure, { outcome: "network" | "unauthorized" }> }
  > {
    let resolved: Record<string, string>;
    try {
      resolved = await authHeaders();
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      return { ok: false, failure: { outcome: "unauthorized", status: 401, reason, ref: null } };
    }
    const headers: Record<string, string> = { ...resolved, ...extraHeaders };
    if (body !== undefined) headers["content-type"] = "application/json";
    const deadline = idleDeadline();
    try {
      const res = await fetchImpl(target, {
        method,
        headers,
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: deadline.signal,
        ...(auth.kind === "cookie" ? { credentials: "include" as const } : {}),
      });
      deadline.rearm(); // the headers arrived: the deadline now bounds IDLE time on the body, not the whole read
      return { ok: true, res, deadline };
    } catch (err) {
      deadline.release();
      const reason = err instanceof Error ? err.message : String(err);
      return { ok: false, failure: { outcome: "network", reason: `could not reach ${target}: ${reason}` } };
    }
  }

  /** Read a `Response` `sendRaw` handed back into the same `Answer` shape `send()` produces — used
   *  for the refusal path of an NDJSON route, whose refusal body IS JSON. */
  async function answerOf(res: Response): Promise<Answer> {
    const text = await res.text();
    let json: unknown;
    if (text !== "") {
      try {
        json = JSON.parse(text);
      } catch {
        json = undefined;
      }
    }
    return { status: res.status, headers: res.headers, json, textHead: text.slice(0, 200) };
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
    // CTC-2132 widened `Sent`'s failure arm from `network` to `network | unauthorized` so a bearer
    // `getToken()` rejection can be returned as a typed arm — which made this line funnel a CREDENTIAL
    // REFUSAL through the stale-cache tolerance above, so a client whose OAuth refresh had failed was
    // told `{outcome:"ok",source:"cache"}` (validate attempt 1, code-review Finding 3). A refusal is
    // not transient; it is returned as itself, exactly as the comment above `unavailable` states and
    // as the 401/403 answered by the SERVER already is (they fall through to `classify` below).
    if (!sent.ok) return sent.failure.outcome === "unauthorized" ? sent.failure : unavailable(sent.failure);
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

  async function issuesExecution(identifier: string): Promise<TicketExecutionResult> {
    const sent = await send("GET", url(`/api/v1/issues/${encodeURIComponent(identifier)}/execution`), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status === 404) return { outcome: "not-found", status: 404 };
    if (answer.status !== 200) return classify(answer);
    if (!isRecord(answer.json)) {
      return { outcome: "shape", status: 200, reason: "GET /api/v1/issues/:id/execution did not answer an object" };
    }
    return { outcome: "ok", status: 200, report: answer.json as TicketExecutionReport };
  }

  // ── The diagnosis / telemetry reads ───────────────────────────────────────────────────────────

  /** A literal-path open read whose body is one object. The shape check is the CONTAINER only
   *  (Decision 4, CTC-2132) — `check` never inspects a field the cloud might rename. */
  async function objectRead(
    path: string,
    query: Record<string, string | number | undefined>,
    label: string,
    check: (body: Record<string, unknown>) => boolean = () => true,
  ): Promise<{ outcome: "ok"; status: 200; body: Record<string, unknown> } | TenantClientFailure> {
    const sent = await send("GET", url(path, query), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status !== 200) return classify(answer);
    if (!isRecord(answer.json) || !check(answer.json)) {
      return { outcome: "shape", status: 200, reason: `${label} did not answer the documented shape` };
    }
    return { outcome: "ok", status: 200, body: answer.json };
  }

  /** A literal-path open read whose body is a bare array. */
  async function arrayRead(
    path: string,
    query: Record<string, string | number | undefined>,
    label: string,
  ): Promise<{ outcome: "ok"; status: 200; rows: Record<string, unknown>[] } | TenantClientFailure> {
    const sent = await send("GET", url(path, query), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status !== 200) return classify(answer);
    const rows = Array.isArray(answer.json) ? rowsOf<Record<string, unknown>>(answer.json) : null;
    if (rows === null) return { outcome: "shape", status: 200, reason: `${label} did not answer an array` };
    return { outcome: "ok", status: 200, rows };
  }

  async function workEligibility(params: { team?: string } = {}): Promise<WorkEligibilityResult> {
    const r = await objectRead("/api/v1/work-eligibility", { team: params.team }, "GET /api/v1/work-eligibility");
    return r.outcome === "ok" ? { outcome: "ok", status: 200, report: r.body as WorkEligibilityReport } : r;
  }

  async function dispatchQueue(params: { team?: string } = {}): Promise<DispatchQueueResult> {
    const r = await objectRead(
      "/api/v1/dispatch-queue/current",
      { team: params.team },
      "GET /api/v1/dispatch-queue/current",
      (body) => Array.isArray(body["entries"]),
    );
    return r.outcome === "ok" ? { outcome: "ok", status: 200, queue: r.body as DispatchQueueEnvelope } : r;
  }

  async function fleetActivity(params: { account?: string } = {}): Promise<FleetActivityResult> {
    const r = await arrayRead("/api/v1/fleet-activity/current", { account: params.account }, "GET /api/v1/fleet-activity/current");
    return r.outcome === "ok" ? { outcome: "ok", status: 200, rows: r.rows } : r;
  }

  async function agentRoster(): Promise<AgentRosterResult> {
    const r = await arrayRead("/api/v1/agent-roster/current", {}, "GET /api/v1/agent-roster/current");
    return r.outcome === "ok" ? { outcome: "ok", status: 200, rows: r.rows } : r;
  }

  async function leaseAttributions(params: { ticket: string; phase: string }): Promise<LeaseAttributionsResult> {
    const sent = await send("GET", url("/api/v1/lease/attributions", { ticket: params.ticket, phase: params.phase }), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status !== 200) return classify(answer);
    if (answer.json === undefined) {
      return { outcome: "shape", status: 200, reason: "GET /api/v1/lease/attributions did not answer JSON" };
    }
    return { outcome: "ok", status: 200, body: answer.json };
  }

  async function codingAccounts(): Promise<CodingAccountsResult> {
    const r = await objectRead("/api/v1/coding-accounts", {}, "GET /api/v1/coding-accounts", (body) => Array.isArray(body["accounts"]));
    return r.outcome === "ok" ? { outcome: "ok", status: 200, report: r.body as CodingAccountsReport } : r;
  }

  // ── The query reads ────────────────────────────────────────────────────────────────────────────

  async function cyclesList(): Promise<CyclesListResult> {
    const r = await arrayRead("/api/v1/cycles", {}, "GET /api/v1/cycles");
    return r.outcome === "ok" ? { outcome: "ok", status: 200, rows: r.rows as CycleRow[] } : r;
  }

  async function search(params: SearchParams): Promise<SearchResult> {
    const r = await objectRead("/api/v1/search", { q: params.q, limit: params.limit }, "GET /api/v1/search");
    return r.outcome === "ok" ? { outcome: "ok", status: 200, results: r.body as SearchResults } : r;
  }

  /** Tolerant (Decision 7): accepts `{stages, source?}` OR a bare array, normalizing both — mirroring
   *  the bundle's own `fetchWorkflowStates`, which already defends against three shapes. */
  async function workflowStages(): Promise<WorkflowStagesResult> {
    const sent = await send("GET", url("/api/v1/workflow-stages"), {});
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status !== 200) return classify(answer);
    if (Array.isArray(answer.json)) {
      const rows = rowsOf<Record<string, unknown>>(answer.json);
      if (rows !== null) return { outcome: "ok", status: 200, stages: rows, source: null };
    }
    if (isRecord(answer.json) && Array.isArray(answer.json["stages"])) {
      const rows = rowsOf<Record<string, unknown>>(answer.json["stages"]);
      if (rows !== null) {
        return { outcome: "ok", status: 200, stages: rows, source: stringField(answer.json, "source") };
      }
    }
    return { outcome: "shape", status: 200, reason: "GET /api/v1/workflow-stages did not answer a recognised shape" };
  }

  /** ⛔ The returned generator MAY THROW mid-iteration — a transport fault after the promise already
   *  resolved cannot be folded into an already-resolved union. Use {@link changesList} for the
   *  nothing-throws, buffered alternative. */
  async function changesStream(params: { since: number | "head"; signal?: AbortSignal }): Promise<ChangesStreamResult> {
    const sent = await sendRaw("GET", url("/api/v1/changes", { since: params.since }), { accept: "application/x-ndjson" });
    if (!sent.ok) return sent.failure;
    const { res, deadline } = sent;
    if (res.status === 409) {
      deadline.release();
      const answer = await answerOf(res);
      return { outcome: "resync", status: 409, head: integerHeader(res.headers, HEAD_SEQ_HEADER), reason: reasonOf(answer) };
    }
    if (res.status !== 200) {
      deadline.release();
      return classify(await answerOf(res));
    }
    const head = integerHeader(res.headers, HEAD_SEQ_HEADER);
    async function* rows(): AsyncGenerator<Record<string, unknown>> {
      try {
        // `onProgress` refunds the idle deadline per chunk, so a long replay is never truncated at
        // `timeoutMs` (code-review Finding 2); `finally` releases it on every exit — EOF, caller
        // abort, a `break` out of the for-await, or a throw from the parse below.
        for await (const line of iterateNdjson(res, {
          ...(params.signal === undefined ? {} : { signal: params.signal }),
          onProgress: deadline.rearm,
        })) {
          let parsed: unknown;
          try {
            parsed = JSON.parse(line);
          } catch {
            throw new Error(`/api/v1/changes emitted a line that is not JSON: ${line.slice(0, 120)}`);
          }
          if (!isRecord(parsed)) throw new Error("/api/v1/changes emitted a line that is not an object");
          yield parsed;
        }
      } finally {
        deadline.release();
      }
    }
    return { outcome: "ok", status: 200, head, rows: rows() };
  }

  async function changesListFn(params: { since: number | "head"; signal?: AbortSignal }): Promise<ChangesListResult> {
    const sent = await changesStream(params);
    if (sent.outcome !== "ok") return sent;
    try {
      const rows: Record<string, unknown>[] = [];
      for await (const row of sent.rows) rows.push(row);
      return { outcome: "ok", status: 200, head: sent.head, rows };
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      return { outcome: "network", reason: `/api/v1/changes stream faulted mid-body: ${reason}` };
    }
  }

  /** The cheap probe (Decision 7) — NEVER falls back to a full snapshot fetch. Reads the head from
   *  `X-Mirror-Cursor` if present, else a JSON body's `head`/`seq`/`cursor`, else the first NDJSON
   *  line parsed as JSON. */
  async function snapshotHead(): Promise<SnapshotHeadResult> {
    const sent = await sendRaw("GET", url("/api/v1/snapshot", { head: 1 }), { accept: "application/x-ndjson, application/json" });
    if (!sent.ok) return sent.failure;
    const { res, deadline } = sent;
    try {
      // ⛔ STATUS FIRST, THEN THE HEADER. `X-Mirror-Cursor` rides REFUSALS too — `changesStream`
      // above reads the head straight off a 409 — so reading it before the status gate reported a
      // 403 as `{outcome:"ok",status:200,head:…}` and told a liveness probe the mirror was healthy
      // while the client was de-authorized (CTC-2132 validate attempt 1, code-review Finding 1).
      // Past this gate `res.status` IS 200, so the literal below is the status, not an assumption.
      if (res.status !== 200) return classify(await answerOf(res));
      const headerHead = integerHeader(res.headers, HEAD_SEQ_HEADER);
      if (headerHead !== null) {
        await res.body?.cancel().catch(() => {});
        return { outcome: "ok", status: 200, head: headerHead, source: "header" };
      }
      const contentType = res.headers.get("content-type") ?? "";
      if (contentType.includes("application/json")) {
        const answer = await answerOf(res);
        const fromBody = isRecord(answer.json)
          ? (numberField(answer.json, "head") ?? numberField(answer.json, "seq") ?? numberField(answer.json, "cursor"))
          : null;
        if (fromBody !== null) return { outcome: "ok", status: 200, head: fromBody, source: "body" };
        return { outcome: "shape", status: 200, reason: "GET /api/v1/snapshot?head=1 did not answer a finite head" };
      }
      for await (const line of iterateNdjson(res, { onProgress: deadline.rearm })) {
        let parsed: unknown;
        try {
          parsed = JSON.parse(line);
        } catch {
          continue;
        }
        if (isRecord(parsed)) {
          const fromLine = numberField(parsed, "head") ?? numberField(parsed, "seq") ?? numberField(parsed, "cursor");
          if (fromLine !== null) return { outcome: "ok", status: 200, head: fromLine, source: "body" };
        }
        break;
      }
      return { outcome: "shape", status: 200, reason: "GET /api/v1/snapshot?head=1 did not answer a finite head" };
    } finally {
      deadline.release();
    }
  }

  // ── The generic escape hatch ───────────────────────────────────────────────────────────────────

  async function request(req: RawRequest): Promise<RawRequestResult> {
    if (!req.path.startsWith("/") || req.path.startsWith("//")) {
      return {
        outcome: "rejected",
        status: 0,
        reason: `request(): path must be an absolute path under the tenant origin, got ${req.path}`,
      };
    }
    const sent = await send(req.method ?? "GET", url(req.path, req.query), req.headers ?? {}, req.body);
    if (!sent.ok) return sent.failure;
    const { answer } = sent;
    if (answer.status < 200 || answer.status >= 300) return classify(answer);
    return { outcome: "ok", status: answer.status, json: answer.json, headers: answer.headers };
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

  return {
    contract,
    me,
    request,
    issues: { list: issuesList, get: issuesGet, execution: issuesExecution },
    pulls: { list: pullsList, get: pullsGet },
    projects: { list: projectsList },
    cycles: { list: cyclesList },
    search,
    workflowStages,
    changes: { stream: changesStream, list: changesListFn },
    snapshot: { head: snapshotHead },
    diagnostics: {
      workEligibility,
      dispatchQueue,
      fleetActivity,
      agentRoster,
      leaseAttributions,
      codingAccounts,
    },
    agent,
  };
}

/** The client. See {@link createTenantClient}. */
export interface TenantClient {
  /** The tenant's fact document, cached per its own `cache` policy. */
  contract(opts?: ContractOptions): Promise<ContractResult>;
  /** `GET /api/v1/me` — the account this key belongs to. */
  me(): Promise<MeResult>;
  /** The generic authed-request escape hatch (CTC-2132) — for a route nobody enumerated. `path` MUST
   *  be an absolute path under this client's own origin; see {@link RawRequest}. */
  request(req: RawRequest): Promise<RawRequestResult>;
  issues: {
    /** `GET /api/v1/issues` — keyset-paged; follow `nextCursor` until it is `null`. */
    list(params?: IssueListParams): Promise<IssueListResult>;
    /** `GET /api/v1/issues/:identifier`. */
    get(identifier: string): Promise<IssueGetResult>;
    /** `GET /api/v1/issues/:id/execution` — the ticket's own execution/telemetry report. */
    execution(identifier: string): Promise<TicketExecutionResult>;
  };
  pulls: {
    list(params?: PullListParams): Promise<PullListResult>;
    /** `GET /api/v1/pulls/:id` — `id` is the PR's GitHub node id. */
    get(nodeId: string): Promise<PullGetResult>;
  };
  projects: {
    list(params?: ProjectListParams): Promise<ProjectListResult>;
  };
  cycles: {
    /** `GET /api/v1/cycles` — the SDK's own structural row type; see {@link CycleRow}. */
    list(): Promise<CyclesListResult>;
  };
  /** `GET /api/v1/search?q=&limit=`. */
  search(params: SearchParams): Promise<SearchResult>;
  /** `GET /api/v1/workflow-stages` — tolerant of a `{stages,source?}` envelope or a bare array. */
  workflowStages(): Promise<WorkflowStagesResult>;
  changes: {
    /** `GET /api/v1/changes?since=` — NDJSON. ⛔ The returned generator MAY THROW mid-iteration; use
     *  `list` for the nothing-throws, buffered alternative. */
    stream(params: { since: number | "head"; signal?: AbortSignal }): Promise<ChangesStreamResult>;
    /** The same feed, buffered — never throws; a mid-stream fault comes back as the `network` arm. */
    list(params: { since: number | "head"; signal?: AbortSignal }): Promise<ChangesListResult>;
  };
  snapshot: {
    /** `GET /api/v1/snapshot?head=1` — the cheap head probe; never falls back to a full snapshot. */
    head(): Promise<SnapshotHeadResult>;
  };
  /** The diagnosis / telemetry reads (CTC-2132, M2 addendum) — a customer delegate agent's own
   *  "why is nothing moving" / "what is my fleet doing" answers. */
  diagnostics: {
    workEligibility(params?: { team?: string }): Promise<WorkEligibilityResult>;
    dispatchQueue(params?: { team?: string }): Promise<DispatchQueueResult>;
    fleetActivity(params?: { account?: string }): Promise<FleetActivityResult>;
    agentRoster(): Promise<AgentRosterResult>;
    leaseAttributions(params: { ticket: string; phase: string }): Promise<LeaseAttributionsResult>;
    codingAccounts(): Promise<CodingAccountsResult>;
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
    projectRepositoryRegister(input: ProjectRepositoryInput): Promise<ProjectRepositoryRegisterResult>;
    projectRepositoryRemove(input: ProjectRepositoryInput): Promise<ProjectRepositoryRemoveResult>;
  };
}
