// ticket-lease-client.ts — CTC-8872. A seat's ticket leases over the cloud's literal
// `/api/v1/agent/ticket-lease(s)` routes: take, renew, release, read one ticket, list one team.
//
// Every type below restates `packages/protocol/src/lease.ts` in the cloud repository (CTC-6398's
// seat section plus CTC-8871's holder-details slice) under its exact names. The protocol package is
// private and unpublished (`npm view @catalyst-cloud/protocol` is a 404), the same situation the
// `TenantContract` declaration records, so the SDK carries its own restatement pinned against the
// committed fixture in `test/fixtures/ticket-lease-answers.json`. When protocol becomes importable,
// these declarations swap for `export type { … } from "@catalyst-cloud/protocol"` one-for-one.
//
// Answers follow the ticket's two paths: a store refusal is an HTTP 200 that RESOLVES with
// `outcome: "refused"` (the boundary decorates it with a top-level `code`/`reason`, passed through
// untouched); an HTTP failure throws `TicketLeaseHttpError` carrying the server's status and code.

import { DEFAULT_TIMEOUT_MS, normalizeBaseUrl } from "./tenant-client.js";

// ── Wire types, restated from packages/protocol/src/lease.ts ───────────────────────────────────

/** CTC-8871 — the declared kinds of holder. */
export type TicketLeaseHolderKind =
  | "catalyst-seat"
  | "catalyst-runner"
  | "claude-code"
  | "codex"
  | "factory"
  | "devin"
  | "cursor"
  | "other"
  | "unspecified";

/** CTC-8871 — what a caller may declare about its holder on take. The server validates and derives the rest. */
export interface TicketLeaseHolderDetailsInput {
  kind?: TicketLeaseHolderKind;
  displayName?: string;
  sessionUrl?: string;
  externalId?: string;
  contact?: string;
}

/** CTC-8871 — who the credential speaks for. Derived by the server; a client-sent owner is ignored. */
export type TicketLeaseHolderOwner =
  | { type: "person"; userId: string; name: string | null }
  | { type: "organization-key"; keyId: string; keyName: string | null };

/** CTC-8871 — the stored holder details. `declared` is literal `true` on the wire. */
export interface TicketLeaseHolderDetails {
  kind: TicketLeaseHolderKind;
  displayName: string;
  sessionUrl: string | null;
  externalId: string | null;
  contact: string | null;
  owner: TicketLeaseHolderOwner;
  declared: true;
  generation: number;
  recordedAtMs: number;
}

/** The stored ticket lease. Live through `deadlineMs` inclusive. */
export interface TicketLeaseRowWire {
  ticket: string;
  holder: string | null;
  lastHolder: string | null;
  generation: number;
  deadlineMs: number;
  hardDeadlineMs: number;
  acquiredAtMs: number;
  lastProgress: string | null;
  lastProgressAtMs: number | null;
}

/** CTC-8871 — a seat's lease row carries holder details, null when none were recorded. */
export interface SeatTicketLeaseRowWire extends TicketLeaseRowWire {
  holderDetails: TicketLeaseHolderDetails | null;
}

export interface TicketLeaseDelegationWire {
  ticket: string;
  delegationGeneration: number;
  ticketGeneration: number;
  executor: string;
  phase: string;
  createdAtMs: number;
  revokedAtMs: number | null;
}

/** CTC-4537 — what occupies a ticket a claim was refused on. */
export type TicketLeaseOccupiedWire =
  | { scope: "ticket" }
  | { scope: "phase"; phase: string; kind: "lease" | "reservation" };

export type TicketLeaseRefusalReasonWire =
  | "ticket_lease_held"
  | "phase_lease_held"
  | "not_holder"
  | "stale_generation"
  | "expired"
  | "hard_cap"
  | "no_new_progress"
  | "active_descendants"
  | "project_deleting";

/** A store refusal: HTTP 200, `outcome: "refused"`. */
export interface TicketLeaseRefusedWire {
  outcome: "refused";
  refusal: {
    reason: TicketLeaseRefusalReasonWire;
    current: TicketLeaseRowWire | null;
    occupied: TicketLeaseOccupiedWire | null;
    /** The first instant a retry can succeed, when the mirror can know it. */
    retryAtMs: number | null;
  };
}

/** CTC-8871 — a seat route's refusal carries the incumbent's holder details on `current`. */
export interface SeatTicketLeaseRefusedWire {
  outcome: "refused";
  refusal: Omit<TicketLeaseRefusedWire["refusal"], "current"> & {
    current: SeatTicketLeaseRowWire | null;
  };
}

/** `POST /api/v1/lease/ticket/take` — the host verb's answer, which the seat route shares. */
export type TakeTicketLeaseResponse =
  | {
      outcome: "taken";
      lease: TicketLeaseRowWire;
      /** False on a same-holder retry of a live lease: the existing grant, unchanged. */
      minted: boolean;
      /** The caller's own live phase leases bound to this generation, with their clamped deadlines. */
      adopted: ReadonlyArray<{ phase: string; deadlineMs: number }>;
    }
  | TicketLeaseRefusedWire;

export type RenewTicketLeaseResponse =
  | { outcome: "renewed"; lease: TicketLeaseRowWire }
  | TicketLeaseRefusedWire;

export type ReleaseTicketLeaseResponse =
  | { outcome: "released"; lease: TicketLeaseRowWire }
  | TicketLeaseRefusedWire;

/** One ticket lease as a status reader sees it. `retryAtMs` is set only while it is live. */
export interface TicketLeaseStatusWire {
  holder: string | null;
  lastHolder: string | null;
  generation: number;
  live: boolean;
  deadlineMs: number;
  hardDeadlineMs: number;
  acquiredAtMs: number;
  lastProgress: string | null;
  lastProgressAtMs: number | null;
  retryAtMs: number | null;
  /** CTC-8871 — null when the lease predates holder details or the holder declared none. */
  holderDetails: TicketLeaseHolderDetails | null;
  lastHolderDetails: TicketLeaseHolderDetails | null;
}

/** CTC-7145 — the stored handoff of a ticket's last accepted handoff renew. */
export interface TicketLeaseHandoff {
  generation: number;
  completedStage: string;
  nextStage: string | null;
  headSha: string;
  digest: string;
  byteLength: number;
  recordedAtMs: number;
  body: string;
}

/** `GET /api/v1/agent/ticket-lease?ticket=`. `lease` is null for a ticket never leased. */
export interface ReadTicketLeaseResponse {
  ticket: string;
  lease: TicketLeaseStatusWire | null;
  delegations: ReadonlyArray<TicketLeaseDelegationWire>;
  lastHandoff: TicketLeaseHandoff | null;
}

/** `GET /api/v1/agent/ticket-leases` — the list query. `state` defaults to `live`, `limit` to 50, on the server. */
export interface ListTicketLeasesParams {
  team: string;
  state?: "live" | "recent";
  limit?: number;
  /** Opaque: pass `nextCursor` back unchanged. */
  cursor?: string;
}

/** `GET /api/v1/agent/ticket-leases?team=` — CTC-8871's team list, sorted by `deadlineMs`. */
export interface ListTicketLeasesResponse {
  team: string;
  state: "live" | "recent";
  leases: ReadonlyArray<TicketLeaseStatusWire & { ticket: string }>;
  nextCursor: string | null;
}

/** A seat's declared name: 1 to 40 of `a-z`, `0-9` and `-`. */
export interface SeatTicketLeaseIdentity {
  /** A ticket identifier such as `CTC-123`. */
  ticket: string;
  agent: string;
}

export interface TakeSeatTicketLeaseParams extends SeatTicketLeaseIdentity {
  /** Clamped to 5 to 60 minutes; absent means 30. */
  ttlMs?: number;
  holderDetails?: TicketLeaseHolderDetailsInput;
}

/** What a seat did since its last renewal. The mirror keys repeats on `<kind>:<ref>`. */
export interface SeatTicketLeaseProgress {
  kind: "commit" | "artifact" | "finding" | "pr";
  ref: string;
  note?: string;
}

export interface RenewSeatTicketLeaseParams extends SeatTicketLeaseIdentity {
  generation: number;
  progress: SeatTicketLeaseProgress;
  ttlMs?: number;
  /** A field sent replaces the stored one, explicit `null` clears it, an omitted key leaves it alone. */
  holderDetails?: {
    [K in keyof TicketLeaseHolderDetailsInput]?: TicketLeaseHolderDetailsInput[K] | null;
  };
}

export interface ReleaseSeatTicketLeaseParams extends SeatTicketLeaseIdentity {
  generation: number;
}

/**
 * The seat routes' answers: the canonical mutation unions with their lease rows narrowed to
 * `SeatTicketLeaseRowWire` (holder details attached) and the refusal's incumbent row likewise.
 * Derived, not redeclared, so a protocol change to a wire field flows through.
 */
type SeatAnswerOf<T> =
  T extends { outcome: "refused" }
    ? SeatTicketLeaseRefusedWire
    : T extends { lease: TicketLeaseRowWire }
      ? Omit<T, "lease"> & { lease: SeatTicketLeaseRowWire }
      : never;

export type TakeSeatTicketLeaseResponse = SeatAnswerOf<TakeTicketLeaseResponse>;
export type RenewSeatTicketLeaseResponse = SeatAnswerOf<RenewTicketLeaseResponse>;
export type ReleaseSeatTicketLeaseResponse = SeatAnswerOf<ReleaseTicketLeaseResponse>;

// ── The thrown error ──────────────────────────────────────────────────────────────────────────

/** An HTTP failure from a lease route: the server's status, and its `code`/`field` when the body spoke them. */
export class TicketLeaseHttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string | null,
    readonly reason: string,
    readonly field: string | null,
  ) {
    const codePart = code === null ? "" : ` ${code}`;
    const reasonPart = reason === "" ? "" : `: ${reason}`;
    super(`ticket lease: server returned ${status}${codePart}${reasonPart}`);
    this.name = "TicketLeaseHttpError";
  }
}

// ── The client ────────────────────────────────────────────────────────────────────────────────

export interface TicketLeaseClientOptions {
  baseUrl: string;
  token: string;
  fetch?: typeof fetch;
  timeoutMs?: number;
}

export interface TicketLeaseClient {
  take(params: TakeSeatTicketLeaseParams): Promise<TakeSeatTicketLeaseResponse>;
  renew(params: RenewSeatTicketLeaseParams): Promise<RenewSeatTicketLeaseResponse>;
  release(params: ReleaseSeatTicketLeaseParams): Promise<ReleaseSeatTicketLeaseResponse>;
  read(ticket: string): Promise<ReadTicketLeaseResponse>;
  list(params: ListTicketLeasesParams): Promise<ListTicketLeasesResponse>;
}

const TAKE_PATH = "/api/v1/agent/ticket-lease/take";
const RENEW_PATH = "/api/v1/agent/ticket-lease/renew";
const RELEASE_PATH = "/api/v1/agent/ticket-lease/release";
const READ_PATH = "/api/v1/agent/ticket-lease";
const LIST_PATH = "/api/v1/agent/ticket-leases";

interface LeaseAnswer {
  status: number;
  json: unknown;
  text: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function isInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value);
}

function isNullableInteger(value: unknown): value is number | null {
  return value === null || isInteger(value);
}

function isTicketLeaseHolderDetails(value: unknown): value is TicketLeaseHolderDetails {
  if (!isRecord(value) || !isString(value.kind) || !isString(value.displayName)) return false;
  if (!isNullableString(value.sessionUrl) || !isNullableString(value.externalId) || !isNullableString(value.contact))
    return false;
  if (value.declared !== true || !isInteger(value.generation) || !isInteger(value.recordedAtMs)) return false;
  const owner = value.owner;
  if (!isRecord(owner)) return false;
  if (owner.type === "person") return isString(owner.userId) && isNullableString(owner.name);
  if (owner.type === "organization-key") return isString(owner.keyId) && isNullableString(owner.keyName);
  return false;
}

function isTicketLeaseRowWire(value: unknown): value is TicketLeaseRowWire {
  if (!isRecord(value)) return false;
  return (
    isString(value.ticket) &&
    isNullableString(value.holder) &&
    isNullableString(value.lastHolder) &&
    isInteger(value.generation) &&
    isInteger(value.deadlineMs) &&
    isInteger(value.hardDeadlineMs) &&
    isInteger(value.acquiredAtMs) &&
    isNullableString(value.lastProgress) &&
    isNullableInteger(value.lastProgressAtMs)
  );
}

function isSeatTicketLeaseRowWire(value: unknown): value is SeatTicketLeaseRowWire {
  return (
    isTicketLeaseRowWire(value) &&
    isRecord(value) &&
    (value.holderDetails === null || isTicketLeaseHolderDetails(value.holderDetails))
  );
}

function isTicketLeaseStatusWire(value: unknown): value is TicketLeaseStatusWire {
  if (!isRecord(value)) return false;
  return (
    isNullableString(value.holder) &&
    isNullableString(value.lastHolder) &&
    isInteger(value.generation) &&
    typeof value.live === "boolean" &&
    isInteger(value.deadlineMs) &&
    isInteger(value.hardDeadlineMs) &&
    isInteger(value.acquiredAtMs) &&
    isNullableString(value.lastProgress) &&
    isNullableInteger(value.lastProgressAtMs) &&
    isNullableInteger(value.retryAtMs) &&
    (value.holderDetails === null || isTicketLeaseHolderDetails(value.holderDetails)) &&
    (value.lastHolderDetails === null || isTicketLeaseHolderDetails(value.lastHolderDetails))
  );
}

function isTicketLeaseOccupiedWire(value: unknown): value is TicketLeaseOccupiedWire {
  if (!isRecord(value)) return false;
  if (value.scope === "ticket") return true;
  return value.scope === "phase" && isString(value.phase) && (value.kind === "lease" || value.kind === "reservation");
}

function isSeatTicketLeaseRefusedWire(value: unknown): value is SeatTicketLeaseRefusedWire {
  if (!isRecord(value) || value.outcome !== "refused" || !isRecord(value.refusal)) return false;
  const refusal = value.refusal;
  return (
    isString(refusal.reason) &&
    (refusal.current === null || isSeatTicketLeaseRowWire(refusal.current)) &&
    (refusal.occupied === null || isTicketLeaseOccupiedWire(refusal.occupied)) &&
    isNullableInteger(refusal.retryAtMs)
  );
}

function isAdopted(value: unknown): value is ReadonlyArray<{ phase: string; deadlineMs: number }> {
  return (
    Array.isArray(value) &&
    value.every(
      (entry) =>
        isRecord(entry) && isString(entry.phase) && isInteger(entry.deadlineMs),
    )
  );
}

// One predicate per method: request-specific outcome checks stay explicit, extra server fields are
// allowed, and the checked object itself is returned so the wire body passes through unchanged.
function isTakeResponse(value: unknown): value is TakeSeatTicketLeaseResponse {
  if (isSeatTicketLeaseRefusedWire(value)) return true;
  return (
    isRecord(value) &&
    value.outcome === "taken" &&
    isSeatTicketLeaseRowWire(value.lease) &&
    typeof value.minted === "boolean" &&
    isAdopted(value.adopted)
  );
}

function isRenewResponse(value: unknown): value is RenewSeatTicketLeaseResponse {
  if (isSeatTicketLeaseRefusedWire(value)) return true;
  return isRecord(value) && value.outcome === "renewed" && isSeatTicketLeaseRowWire(value.lease);
}

function isReleaseResponse(value: unknown): value is ReleaseSeatTicketLeaseResponse {
  if (isSeatTicketLeaseRefusedWire(value)) return true;
  return isRecord(value) && value.outcome === "released" && isSeatTicketLeaseRowWire(value.lease);
}

function isTicketLeaseDelegationWire(value: unknown): value is TicketLeaseDelegationWire {
  if (!isRecord(value)) return false;
  return (
    isString(value.ticket) &&
    isInteger(value.delegationGeneration) &&
    isInteger(value.ticketGeneration) &&
    isString(value.executor) &&
    isString(value.phase) &&
    isInteger(value.createdAtMs) &&
    isNullableInteger(value.revokedAtMs)
  );
}

function isTicketLeaseHandoff(value: unknown): value is TicketLeaseHandoff {
  if (!isRecord(value)) return false;
  return (
    isInteger(value.generation) &&
    isString(value.completedStage) &&
    isNullableString(value.nextStage) &&
    isString(value.headSha) &&
    isString(value.digest) &&
    isInteger(value.byteLength) &&
    isInteger(value.recordedAtMs) &&
    isString(value.body)
  );
}

function isReadResponse(value: unknown): value is ReadTicketLeaseResponse {
  if (!isRecord(value) || !isString(value.ticket)) return false;
  if (!(value.lease === null || isTicketLeaseStatusWire(value.lease))) return false;
  if (!Array.isArray(value.delegations) || !value.delegations.every(isTicketLeaseDelegationWire)) return false;
  return value.lastHandoff === null || isTicketLeaseHandoff(value.lastHandoff);
}

function isListResponse(value: unknown): value is ListTicketLeasesResponse {
  if (!isRecord(value) || !isString(value.team)) return false;
  if (value.state !== "live" && value.state !== "recent") return false;
  if (
    !Array.isArray(value.leases) ||
    !value.leases.every(
      (row) => isTicketLeaseStatusWire(row) && isRecord(row) && isString(row.ticket),
    )
  )
    return false;
  return isNullableString(value.nextCursor);
}

/** The reason string an error body carries, under whichever of the three grammars it speaks. */
function reasonOf(json: unknown, text: string): string {
  if (isRecord(json)) {
    if (isString(json.reason)) return json.reason;
    if (isString(json.error)) return json.error;
    if (isString(json.message)) return json.message;
  }
  return text.slice(0, 200);
}

function httpErrorOf(answer: LeaseAnswer): TicketLeaseHttpError {
  const json = isRecord(answer.json) ? answer.json : null;
  const code = json !== null && isString(json.code) ? json.code : null;
  const field = json !== null && isString(json.field) ? json.field : null;
  return new TicketLeaseHttpError(answer.status, code, reasonOf(answer.json, answer.text), field);
}

function invalidResponse(status: number): TicketLeaseHttpError {
  return new TicketLeaseHttpError(status, null, "invalid ticket lease response", null);
}

export function createTicketLeaseClient(options: TicketLeaseClientOptions): TicketLeaseClient {
  const origin = normalizeBaseUrl(options.baseUrl);
  const token = options.token;
  const fetchImpl = options.fetch ?? fetch;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;

  async function call(
    method: "GET" | "POST",
    path: string,
    query?: Array<[string, string]>,
    body?: unknown,
  ): Promise<LeaseAnswer> {
    const url = new URL(`${origin}${path}`);
    for (const [name, value] of query ?? []) url.searchParams.set(name, value);
    const headers: Record<string, string> = { authorization: `Bearer ${token}` };
    if (body !== undefined) headers["content-type"] = "application/json";
    // The deadline stays armed through body reading: a slow body aborts, not hangs.
    const response = await fetchImpl(url, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await response.text();
    let json: unknown;
    try {
      json = text === "" ? undefined : JSON.parse(text);
    } catch {
      json = undefined;
    }
    return { status: response.status, json, text };
  }

  async function answerOf<T>(
    method: "GET" | "POST",
    path: string,
    guard: (value: unknown) => value is T,
    query?: Array<[string, string]>,
    body?: unknown,
  ): Promise<T> {
    const answer = await call(method, path, query, body);
    if (answer.status < 200 || answer.status > 299) throw httpErrorOf(answer);
    if (guard(answer.json)) return answer.json;
    throw invalidResponse(answer.status);
  }

  return {
    take: (params) => answerOf("POST", TAKE_PATH, isTakeResponse, undefined, params),
    renew: (params) => answerOf("POST", RENEW_PATH, isRenewResponse, undefined, params),
    release: (params) => answerOf("POST", RELEASE_PATH, isReleaseResponse, undefined, params),
    read: (ticket) => answerOf("GET", READ_PATH, isReadResponse, [["ticket", ticket]]),
    list: (params) => {
      const query: Array<[string, string]> = [["team", params.team]];
      if (params.state !== undefined) query.push(["state", params.state]);
      if (params.limit !== undefined) query.push(["limit", String(params.limit)]);
      if (params.cursor !== undefined) query.push(["cursor", params.cursor]);
      return answerOf("GET", LIST_PATH, isListResponse, query);
    },
  };
}
