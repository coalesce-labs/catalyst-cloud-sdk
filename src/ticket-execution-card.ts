// ticket-execution-card.ts — CTC-10373. The SDK's copy of the cloud's ticket
// execution card: one record per ticket with its execution condition, claim, current stage, queue
// place, blocker, completion, history and recorded cost so far.
//
// ⛔ A COPY, NOT A DEPENDENCY. This repository does not depend on catalyst-cloud. The source of truth is
//   catalyst-cloud `packages/types/src/ticket-execution-card.ts`. This copy carries its
//   types and closed code lists only, never its precedence function: a client reads the condition the
//   cloud decided and never re-derives it. Keep the lists in step with the cloud's; the fixture test
//   in test/ticket-execution-card.test.ts pins the wire forms a client must handle.
//
// ⛔ MISSING IS NEVER ZERO. An amount nobody reported has `usd: null`; a measured zero is its own part
//   state; a reader without cost access gets `cost.access === "restricted"` with no amount at all.
//
// The cloud may add fields. The read methods check the container shape only, so a new field reaches
// the caller instead of becoming a `shape` refusal.

/** The ticket's execution condition. The cloud picks exactly one. */
export const EXECUTION_CONDITIONS = [
  "unknown",
  "complete",
  "paused",
  "running",
  "claimed",
  "blocked",
  "queued",
  "interrupted",
  "failed",
  "cancelled",
  "waiting",
  "idle",
] as const;
export type ExecutionCondition = (typeof EXECUTION_CONDITIONS)[number];

/** The records a card is composed from. A stale or unreadable one is named in `freshness`. */
export const EXECUTION_SOURCES = [
  "ticket_leases",
  "phase_leases",
  "fleet_activity",
  "dispatch_queue",
  "phase_frontier",
  "phase_usage",
  "linear",
] as const;
export type ExecutionSource = (typeof EXECUTION_SOURCES)[number];

/** Platform-observed execution evidence. A seat's own lease progress note is not one of them. */
export const EXECUTION_EVIDENCE_KINDS = [
  "runner_heartbeat",
  "fleet_activity_row",
  "phase_usage_open",
  "helper_report",
  "branch_push",
] as const;
export type ExecutionEvidenceKind = (typeof EXECUTION_EVIDENCE_KINDS)[number];

/** Every code `conditionReason` may carry. A client that prints a reason needs copy for each. */
export const CONDITION_REASON_CODES = [
  "source_stale",
  "no_source_read",
  "dispatcher_held",
  ...EXECUTION_EVIDENCE_KINDS,
  "operator_pause",
  "awaiting_next_grant",
  "awaiting_human_decision",
  "awaiting_review",
  "awaiting_capacity",
] as const;
export type ConditionReasonCode = (typeof CONDITION_REASON_CODES)[number];

/** Who holds a claim. A `controller` claim is the workflow waiting, never execution. */
export type ClaimHolderKind = "seat" | "runner" | "controller";

// ── Cost ──────────────────────────────────────────────────────────────────────────────────────────
// Field-level inside a 200: a whole-request 401 or 403 is the transport's failure arm. Modelled cost
// is tokens at list price, not a charge; label `total` "recorded cost". Compute the cloud cannot tie to
// a ticket reads `not_attributable`, never $0.

export type CostBasis = "measured" | "modelled" | "estimated";

export interface Amount {
  /** Null exactly when coverage is `missing`. */
  usd: number | null;
  coverage: "complete" | "partial" | "missing";
}

export const COST_PART_STATES = [
  "measured",
  "measured_zero",
  "partial",
  "running",
  "not_recorded",
  "not_attributable",
] as const;
export type CostPartState = (typeof COST_PART_STATES)[number];

export interface CostPart extends Amount {
  state: CostPartState;
  /** Sums of the known part by basis. */
  byBasis: Record<CostBasis, number>;
  /** What is absent, by kind (for example `no_report` or `compute_not_measured`). */
  missing: { count: number; kinds: string[] };
}

/** One usage row, on single-ticket reads only. Identity is `(phase, nonce)`. */
export interface CostLine {
  phase: string;
  nonce: string;
  kind: "stage" | "helper" | "orchestration" | "second_opinion";
  provider: string | null;
  harness: string | null;
  model: string | null;
  resolvedModel: string | null;
  tokens: { input: number; output: number; cacheRead: number; cacheWrite: number } | null;
  durationMs: number | null;
  outcome: string | null;
  headlineUsd: number | null;
  costSource: string | null;
  priceSource: string | null;
  computeUsd: number | null;
  computeSource: string | null;
  /** True on the one row that carries its session's compute reading. */
  sessionFirst: boolean;
}

export interface TicketCost {
  access: "granted";
  state: "ok";
  /** The cloud's clock at the read. */
  asOfMs: number;
  lastUsageAtMs: number | null;
  stale: boolean;
  currency: "USD";
  /** Model plus compute over known amounts; reconciles with `detail`. */
  total: Amount;
  model: CostPart;
  compute: CostPart;
  runs: { stage: number; sessions: number; helpers: number; open: number; notLaunched: number };
  /** Summed agent runtime, never elapsed ticket time. */
  runtime: { firstStartedAtMs: number | null; summedRunMs: number | null };
  detail?: CostLine[];
}

/** The card's cost field. None of the three forms is a zero amount or an empty breakdown. Cost needs a
 *  workspace admin or owner; an account API key always reads `restricted`. */
export type TicketCostRead =
  | { access: "restricted"; reason: "account_admin_required" }
  | { access: "granted"; state: "unavailable"; reason: string; lastGoodAtMs: number | null }
  | TicketCost;

// ── The card ──────────────────────────────────────────────────────────────────────────────────────

export interface TicketExecutionHistoryRow {
  /** De-duplication identity across reads: the stage plus its run nonce. */
  attemptId: string;
  stage: string;
  attempt: number | null;
  startedAt: string | null;
  endedAt: string | null;
  outcome: string;
  /** This attempt's own cost, so the card total can be reconciled against its rows. */
  cost: TicketCostRead | null;
}

export interface TicketExecutionCard {
  ticket: { identifier: string; title: string; teamKey: string };
  /** Linear's own state, never overwritten by the condition. */
  linear: { state: string; stateType: string; asOf: string | null };
  condition: ExecutionCondition;
  conditionReason: ConditionReasonCode | null;
  claim: {
    holderLabel: string;
    holderKind: ClaimHolderKind;
    expiresAt: string;
    lastProgressAt: string | null;
  } | null;
  current: {
    stage: string;
    attempt: number | null;
    startedAt: string;
    lastActivityAt: string | null;
    helpers: Array<{ id: string; lastActivityAt: string | null }>;
  } | null;
  queue: { position: number; capacityNote: string | null } | null;
  blocker: { blockingIdentifier: string | null; unblockEvent: string } | null;
  completion: { completedAt: string; outcome: string } | null;
  history: TicketExecutionHistoryRow[];
  cost: TicketCostRead;
  freshness: Array<{ source: ExecutionSource; asOf: string | null; stale: boolean }>;
}

/** The list read. The detail read returns one card exactly as it appears as a list row. */
export interface TicketExecutionCardPage {
  /** The cloud's clock: expiry and freshness are judged against this, never the caller's. */
  serverTime: string;
  cards: TicketExecutionCard[];
  nextCursor: string | null;
}

export interface TicketExecutionCardRead {
  serverTime: string;
  card: TicketExecutionCard;
}

/** The largest page the list read serves. */
export const TICKET_EXECUTION_PAGE_LIMIT_MAX = 100;

const CONDITION_SET: ReadonlySet<string> = new Set(EXECUTION_CONDITIONS);

/** The container check the read methods apply: enough to hand the caller a card, never a field-by-field
 *  validation (a field the cloud adds must reach the caller). */
export function isTicketExecutionCard(value: unknown): value is TicketExecutionCard {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const v = value as Record<string, unknown>;
  const ticket = v["ticket"] as Record<string, unknown> | null | undefined;
  const cost = v["cost"] as Record<string, unknown> | null | undefined;
  return (
    typeof ticket === "object" &&
    ticket !== null &&
    typeof ticket["identifier"] === "string" &&
    typeof v["condition"] === "string" &&
    CONDITION_SET.has(v["condition"]) &&
    Array.isArray(v["history"]) &&
    Array.isArray(v["freshness"]) &&
    typeof cost === "object" &&
    cost !== null &&
    (cost["access"] === "restricted" || cost["access"] === "granted")
  );
}

/** True when the card's recorded cost is known and complete: no part missing, partial or running. */
export function costIsComplete(cost: TicketCostRead): boolean {
  return cost.access === "granted" && "total" in cost && cost.total.coverage === "complete";
}
