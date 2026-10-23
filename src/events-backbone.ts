import type { CatalystEvent } from "@catalyst-cloud/schema";
import type { AuthStrategy } from "./live-sync-client.js";

/** HTTP backbone access shared by the cache writer and direct event waits. */
export interface BackboneOptions {
  baseUrl: string;
  auth: Exclude<AuthStrategy, { kind: "cookie" }>;
  tenantId: string;
  fetch?: typeof fetch;
  requestTimeoutMs?: number;
}

const HEAD_HEADER = "x-catalyst-event-backbone-head-seq";
const MAX_EVENT_LINE_BYTES = 1024 * 1024;
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;

export class EventHistoryGapError extends Error {
  constructor(
    message: string,
    readonly head: number,
    readonly resumeFrom: number,
    readonly reason: string,
  ) {
    super(message);
    this.name = "EventHistoryGapError";
  }
}

export class EventSyncHttpError extends Error {
  constructor(readonly status: number) {
    super(`event sync: backbone returned ${status}`);
    this.name = "EventSyncHttpError";
  }
}

export function positiveInteger(value: unknown, name: string): number {
  if (
    (typeof value !== "string" && typeof value !== "number") ||
    String(value).trim() === ""
  )
    throw new Error(`event sync: invalid ${name}`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0)
    throw new Error(`event sync: invalid ${name}`);
  return parsed;
}

export function parseEvent(value: unknown): CatalystEvent {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("event sync: invalid event");
  const event = value as Partial<CatalystEvent>;
  if (
    typeof event.tenantId !== "string" ||
    !Number.isSafeInteger(event.sequence) ||
    (event.sequence ?? -1) < 0 ||
    typeof event.eventId !== "string" ||
    typeof event.type !== "string" ||
    !Number.isSafeInteger(event.schemaVersion) ||
    typeof event.recordedAt !== "string" ||
    !("payload" in event)
  )
    throw new Error("event sync: invalid event");
  return event as CatalystEvent;
}

export async function readNdjson(
  response: Response,
): Promise<Array<{ event: CatalystEvent; line: string }>> {
  if (!response.body) return [];
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = "";
  const rows: Array<{ event: CatalystEvent; line: string }> = [];
  const consume = (line: string) => {
    if (!line) return;
    if (new TextEncoder().encode(line).byteLength > MAX_EVENT_LINE_BYTES)
      throw new Error("event sync: event line exceeds 1 MiB");
    rows.push({ event: parseEvent(JSON.parse(line)), line });
  };
  for (;;) {
    const { done, value } = await reader.read();
    pending += decoder.decode(value, { stream: !done });
    let newline = pending.indexOf("\n");
    while (newline >= 0) {
      consume(pending.slice(0, newline));
      pending = pending.slice(newline + 1);
      newline = pending.indexOf("\n");
    }
    if (pending.length > MAX_EVENT_LINE_BYTES)
      throw new Error("event sync: event line exceeds 1 MiB");
    if (done) break;
  }
  if (pending) consume(pending);
  return rows;
}

export async function request(
  options: BackboneOptions,
  since: number,
  signal: AbortSignal,
): Promise<Response> {
  const timeout = AbortSignal.timeout(
    options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
  );
  const token =
    options.auth.kind === "token"
      ? options.auth.token
      : await options.auth.getToken();
  return (options.fetch ?? fetch)(
    `${options.baseUrl.replace(/\/$/, "")}/api/v1/events/backbone?since=${since}`,
    {
      headers: { authorization: `Bearer ${token}` },
      signal: AbortSignal.any([signal, timeout]),
    },
  );
}

export async function readCurrentHead(
  options: BackboneOptions,
  signal: AbortSignal,
): Promise<number> {
  const response = await request(options, Number.MAX_SAFE_INTEGER, signal);
  if (response.status !== 409) throw new EventSyncHttpError(response.status);
  const body = (await response.json()) as Record<string, unknown>;
  if (body.error !== "cursor_ahead_of_head")
    throw new Error("event sync: head bootstrap contract mismatch");
  return positiveInteger(body.resumeFrom, "bootstrap cursor");
}

export async function readPage(
  options: BackboneOptions,
  cursor: number,
  signal: AbortSignal,
): Promise<{
  head: number;
  rows: Array<{ event: CatalystEvent; line: string }>;
}> {
  const response = await request(options, cursor, signal);
  if (!response.ok && response.status !== 409)
    throw new EventSyncHttpError(response.status);
  const head = positiveInteger(
    response.headers.get(HEAD_HEADER),
    "head header",
  );
  if (response.status === 409) {
    const body = (await response.json()) as Record<string, unknown>;
    throw new EventHistoryGapError(
      "event sync: durable history has a gap",
      head,
      positiveInteger(body.resumeFrom, "resume cursor"),
      typeof body.reason === "string"
        ? body.reason
        : String(body.error ?? "cursor_refused"),
    );
  }
  const rows = await readNdjson(response);
  let previous = cursor;
  for (const row of rows) {
    if (row.event.tenantId !== options.tenantId)
      throw new Error("event sync: tenant mismatch");
    if (row.event.sequence <= previous)
      throw new Error("event sync: non-monotonic sequence");
    previous = row.event.sequence;
  }
  return { head, rows };
}
export function eventIdentity(
  event: Pick<CatalystEvent, "tenantId" | "sequence" | "eventId">,
): string {
  return JSON.stringify([event.tenantId, event.sequence, event.eventId]);
}
