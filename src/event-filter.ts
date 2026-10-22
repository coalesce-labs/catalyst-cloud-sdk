/** The flat event filter shared by socket subscriptions, queries and local waits. */
export interface EventScopeFilter {
  type?: readonly string[];
  ticket?: readonly string[];
  repo?: readonly string[];
  ref?: readonly string[];
  pr?: readonly string[];
  session?: readonly string[];
  to?: "me";
}
export type EventScopeFilterInput = {
  [K in Exclude<keyof EventScopeFilter, "to">]?: string | readonly string[];
} & { to?: string };
export type EventFilterKey = keyof EventScopeFilter;
export type EventFilterCheck =
  | { ok: true; filter: EventScopeFilter }
  | {
      ok: false;
      key: EventFilterKey | null;
      value: string | null;
      reason: string;
    };
export const EVENT_FILTER_MAX_VALUES = 50;
const LIST_KEYS = ["ticket", "type", "repo", "ref", "pr", "session"] as const;
const REPO_RE = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const TICKET_RE = /^[A-Z][A-Z0-9]*-\d+$/;
const TYPE_SEGMENT = "[a-z][a-z0-9]*(?:-[a-z0-9]+)*";
const TYPE_RE = new RegExp(`^${TYPE_SEGMENT}(?:\\.${TYPE_SEGMENT})+$`);
const FAMILY_RE = new RegExp(`^${TYPE_SEGMENT}(?:\\.${TYPE_SEGMENT})*\\.\\*$`);

function normalized(key: Exclude<EventFilterKey, "to">, value: string): string {
  if (key === "ticket") return value.toUpperCase();
  if (key === "repo") return value.toLowerCase();
  if (key === "session") {
    const split = value.indexOf("/");
    return split < 0
      ? value
      : value.slice(0, split).toUpperCase() + value.slice(split);
  }
  if (key === "ref" || key === "pr") {
    const split = value.indexOf(key === "ref" ? ":" : "#");
    return split < 0
      ? value
      : value.slice(0, split).toLowerCase() + value.slice(split);
  }
  return value;
}

function invalidReason(
  key: Exclude<EventFilterKey, "to">,
  value: string,
): string | null {
  if (value.includes(",")) return "filter values must not contain a comma";
  switch (key) {
    case "type":
      return TYPE_RE.test(value) || FAMILY_RE.test(value)
        ? null
        : "event type must be a dotted name or family.*";
    case "ticket":
      return TICKET_RE.test(value) ? null : "ticket must be TEAM-number";
    case "repo":
      return REPO_RE.test(value) ? null : "repo must be owner/name";
    case "ref": {
      const split = value.indexOf(":");
      const branch = value.slice(split + 1);
      return split > 0 &&
        REPO_RE.test(value.slice(0, split)) &&
        branch.length > 0 &&
        !/[\s\x00-\x1f\x7f]/.test(branch) &&
        !branch.includes("..")
        ? null
        : "ref must be owner/name:branch";
    }
    case "pr": {
      const split = value.indexOf("#");
      return split > 0 &&
        REPO_RE.test(value.slice(0, split)) &&
        /^[1-9]\d*$/.test(value.slice(split + 1))
        ? null
        : "pr must be owner/name#n with a positive integer";
    }
    case "session":
      return /^[A-Z][A-Z0-9]*-\d+\/[a-z][a-z-]*#[1-9]\d*$/.test(value)
        ? null
        : "session must be TICKET/phase#n with a positive integer";
  }
}

/** Validate once before opening a subscription. Refusals retain the offending value. */
export function checkEventFilter(
  input: EventScopeFilterInput,
): EventFilterCheck {
  const filter: EventScopeFilter = {};
  for (const key of LIST_KEYS) {
    const raw = input[key];
    if (raw === undefined) continue;
    const values: string[] = [];
    for (const item of typeof raw === "string" ? [raw] : raw) {
      const value = item.trim();
      const canonical = normalized(key, value);
      const reason = invalidReason(key, canonical);
      if (reason) return { ok: false, key, value, reason };
      if (!values.includes(canonical)) values.push(canonical);
    }
    if (values.length > EVENT_FILTER_MAX_VALUES) {
      return {
        ok: false,
        key,
        value: null,
        reason: `at most ${EVENT_FILTER_MAX_VALUES} distinct values are allowed per key`,
      };
    }
    if (values.length) filter[key] = values;
  }
  if (input.to !== undefined) {
    const value = input.to.trim();
    if (value !== "me")
      return { ok: false, key: "to", value, reason: "to must be me" };
    filter.to = "me";
  }
  return Object.keys(filter).length
    ? { ok: true, filter }
    : {
        ok: false,
        key: null,
        value: null,
        reason: "at least one event filter key is required",
      };
}

/** Query form: one pair per value, preserving the existing ticket/type ordering. */
export function eventFilterQueryParams(
  filter: EventScopeFilter,
): Array<[string, string]> {
  const pairs: Array<[string, string]> = [];
  for (const key of LIST_KEYS)
    for (const value of filter[key] ?? []) pairs.push([key, value]);
  if (filter.to !== undefined) pairs.push(["to", filter.to]);
  return pairs;
}

/** Socket form: one param per key, with values comma-joined. */
export function eventFilterSocketParams(
  filter: EventScopeFilter,
): Record<string, string> {
  const params: Record<string, string> = {};
  for (const key of LIST_KEYS) {
    const values = filter[key];
    if (values?.length) params[key] = values.join(",");
  }
  if (filter.to !== undefined) params.to = filter.to;
  return params;
}

export interface EventScopeKeys {
  tickets: string[];
  repos: string[];
  refs: string[];
  prs: string[];
  sessions: string[];
}
type ScopedEvent = {
  type: string;
  entity?: { type: string; id: string };
  payload?: unknown;
};
function object(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}
function firstString(...values: unknown[]): string | undefined {
  return values.find(
    (value): value is string => typeof value === "string" && value.length > 0,
  );
}
function firstNumber(...values: unknown[]): number | undefined {
  return values.find(
    (value): value is number =>
      typeof value === "number" && Number.isSafeInteger(value) && value > 0,
  );
}

/**
 * Local twin of the server scope keys, first present value wins:
 * ticket: ticket entity id, then payload.ticket.
 * repo: payload.repo, then payload.repository.full_name.
 * ref: repo + branch from payload.branch, pull_request.head.ref, or ref without refs/heads/.
 * pr: pull_request entity id, then repo + numeric pr, pull_request.number, or GitHub number.
 * session: payload.ticket / phase # nonce, when all three are present.
 * Tickets are upper-cased and repositories lower-cased; branch and phase case is preserved.
 */
export function eventScopeKeys(event: ScopedEvent): EventScopeKeys {
  const payload = object(event.payload);
  const pullRequest = object(payload.pull_request);
  const ticket = firstString(
    event.entity?.type === "ticket" ? event.entity.id : undefined,
    payload.ticket,
  );
  const repo = firstString(payload.repo, object(payload.repository).full_name);
  const branch = firstString(
    payload.branch,
    object(pullRequest.head).ref,
    typeof payload.ref === "string"
      ? payload.ref.replace(/^refs\/heads\//, "")
      : undefined,
  );
  const prNumber = firstNumber(payload.pr, pullRequest.number, payload.number);
  const pr = firstString(
    event.entity?.type === "pull_request" ? event.entity.id : undefined,
    repo && prNumber ? `${repo}#${prNumber}` : undefined,
  );
  const sessionTicket = firstString(payload.ticket);
  const phase = firstString(payload.phase);
  const nonce = firstNumber(payload.nonce);
  return {
    tickets: ticket ? [normalized("ticket", ticket)] : [],
    repos: repo ? [normalized("repo", repo)] : [],
    refs: repo && branch ? [normalized("ref", `${repo}:${branch}`)] : [],
    prs: pr ? [normalized("pr", pr)] : [],
    sessions:
      sessionTicket && phase && nonce
        ? [normalized("session", `${sessionTicket}/${phase}#${nonce}`)]
        : [],
  };
}

export function matchesEventType(
  type: string,
  patterns: readonly string[],
): boolean {
  return patterns.some((pattern) =>
    pattern.endsWith(".*")
      ? type.startsWith(pattern.slice(0, -1))
      : type === pattern,
  );
}

/** Local evaluation of every key but `to`, which only the cloud can resolve. */
export function matchesEventFilter(
  event: ScopedEvent,
  filter: Omit<EventScopeFilter, "to">,
): boolean {
  if (filter.type && !matchesEventType(event.type, filter.type)) return false;
  const keys = eventScopeKeys(event);
  const mapping = {
    ticket: "tickets",
    repo: "repos",
    ref: "refs",
    pr: "prs",
    session: "sessions",
  } as const;
  for (const key of ["ticket", "repo", "ref", "pr", "session"] as const) {
    const values = filter[key];
    if (
      values &&
      !values.some((value) =>
        keys[mapping[key]].includes(normalized(key, value)),
      )
    )
      return false;
  }
  return true;
}
