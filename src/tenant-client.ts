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
  /** The tenant key. Reads accept any key with `mirror:read`; every `/api/v1/agent/*` route needs an
   *  organization-tier key (`ctc_acct_*`) — a workstation key is refused `403 not-machine-principal`. */
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
  | "ask-accept";

/** The arms every agent call can end in BEFORE the route answers: the contract could not be served,
 *  or the tenant's contract does not list the route. */
export type AgentCallFailure =
  | ContractFailure
  | { outcome: "route-unknown"; route: AgentRouteName; routes: string[] }
  | TenantClientFailure;

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
    try {
      res = await fetchImpl(target, {
        method,
        headers,
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      return { ok: false, failure: { outcome: "network", reason: `could not reach ${target}: ${reason}` } };
    }
    const text = await res.text();
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

    const sent = await send("GET", url(CONTRACT_ROUTE), cached?.etag ? { "if-none-match": cached.etag } : {});
    if (!sent.ok) {
      if (cached === null) return sent.failure;
      if (ageSeconds >= cached.doc.cache.staleRefusalSeconds) {
        return { outcome: "stale", ageSeconds, staleRefusalSeconds: cached.doc.cache.staleRefusalSeconds, cause: sent.failure };
      }
      return fromCache(cached);
    }
    const { answer } = sent;

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

  function rowsOf<T>(rows: unknown[]): T[] {
    // The mirror's list bodies are the read-model's own view rows; the only shape check a client can
    // make without re-declaring the view is "an array of objects".
    return rows.filter((r): r is T => isRecord(r));
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
    if (page === null) return { outcome: "shape", status: 200, reason: "GET /api/v1/issues did not answer an array" };
    return { outcome: "ok", rows: rowsOf<IssueView>(page.rows), nextCursor: page.nextCursor, total: page.total, head: page.head };
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
    if (page === null) return { outcome: "shape", status: 200, reason: "GET /api/v1/pulls did not answer an array" };
    return {
      outcome: "ok",
      rows: rowsOf<PullView>(page.rows),
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
    if (!Array.isArray(sent.answer.json)) return { outcome: "shape", status: 200, reason: "GET /api/v1/projects did not answer an array" };
    return { outcome: "ok", rows: rowsOf<ProjectView>(sent.answer.json), head: integerHeader(sent.answer.headers, HEAD_SEQ_HEADER) };
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
  ): Promise<{ ok: true; body: Record<string, unknown>; status: number } | { ok: false; failure: AgentCallFailure }> {
    const doc = await contract();
    if (doc.outcome !== "ok") return { ok: false, failure: doc };
    const route = routeByName(doc.doc, name);
    if (route === null) {
      return { ok: false, failure: { outcome: "route-unknown", route: name, routes: doc.doc.routes.map((r) => r.path) } };
    }
    const defined = Object.fromEntries(Object.entries(input).filter(([, v]) => v !== undefined));
    const sent =
      route.method === "GET"
        ? await send("GET", url(route.path, stringify(defined)), {})
        : await send("POST", url(route.path), {}, defined);
    if (!sent.ok) return { ok: false, failure: sent.failure };
    const { answer } = sent;
    if (isRecord(answer.json) && accept(answer.json)) return { ok: true, body: answer.json, status: answer.status };
    return { ok: false, failure: classify(answer) };
  }

  /** A route's success table: the `outcome` literals it answers with a 2xx OR a mapped error status. */
  function outcomes(...names: readonly string[]): (body: Record<string, unknown>) => boolean {
    return (body) => typeof body["outcome"] === "string" && names.includes(body["outcome"]);
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
  };

  return {
    contract,
    me,
    issues: { list: issuesList, get: issuesGet },
    pulls: { list: pullsList, get: pullsGet },
    projects: { list: projectsList },
    agent,
  };
}

/** The client. See {@link createTenantClient}. */
export interface TenantClient {
  /** The tenant's fact document, cached per its own `cache` policy. */
  contract(opts?: ContractOptions): Promise<ContractResult>;
  /** `GET /api/v1/me` — the account this key belongs to. */
  me(): Promise<MeResult>;
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
  };
}
