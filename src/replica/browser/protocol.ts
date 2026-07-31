// replica/protocol.ts — the typed RPC contract between the main thread and the OPFS replica Worker
// (CTC-51, ADR-0002 browser twin of apps/host-sync). The SAHPool VFS only works inside a Web Worker
// (OPFS SyncAccessHandles are Worker-only — see EXTERNAL recipe / sqlite.org persistence docs), so the
// whole replica DB lives in db.worker.ts and the main-thread client (replica-client.ts) talks to it
// over postMessage. This module is the SINGLE source of truth for those message shapes — imported by
// both sides so a request and its response can never drift.
//
// The wire records (snapshot lines / change lines) are the SAME change-feed contract host-sync's
// sync-client.ts consumes; the read results are the SAME read-model view types the DO + daemon return.
// Nothing here imports sqlite-wasm or bun:sqlite, so it stays runtime-agnostic and tree-shakes cleanly.

import type { IssueView, IssueDetailView, PullView } from "@catalyst-cloud/read-model";

/** A change-feed record as it rides the wire — the shape both /snapshot data lines and /changes rows
 *  decode to (accountId/seq are bookkeeping the worker tracks separately). Mirrors host-sync's
 *  ChangeRecord (apps/host-sync/src/apply.ts) field-for-field so the apply path is a true twin. */
export interface WireChange {
  entity: string;
  op: "upsert" | "delete";
  row: Record<string, unknown>;
  /** change_log.entity_id — the PK (composite PKs joined with ':'); required to apply a delete. */
  entityId?: string;
}

// ── Requests (main thread → worker) ────────────────────────────────────────────────────────────────

/** Open the OPFS SAHPool DB and bring its schema up to date (applyMigrations + sync_meta). Idempotent. */
export interface OpenRequest {
  type: "open";
  /** Absolute OPFS db path (SAHPool requires a leading slash). */
  dbPath: string;
  /** OPFS directory the SAHPool VFS manages (one per app to avoid cross-engine collisions). */
  directory: string;
}

/**
 * Batched streaming seed (CTC-132) — replaces the one-shot `SeedRequest`. The snapshot rides in across
 * MANY messages so no layer buffers the whole ~74 MB body: `seedBegin` opens one transaction and
 * truncates, each `seedBatch` applies a bounded page, `seedCommit` sets the cursor and COMMITs, and
 * `seedAbort` rolls back on error. The worker holds one open `SeedSession` for the duration.
 */
export interface SeedBeginRequest {
  type: "seedBegin";
}
export interface SeedBatchRequest {
  type: "seedBatch";
  rows: WireChange[];
}
export interface SeedCommitRequest {
  type: "seedCommit";
  cursor: number;
}
export interface SeedAbortRequest {
  type: "seedAbort";
}

/** Apply a batch of live deltas (each a /changes row) and advance the cursor to the max seq seen. */
export interface ApplyChangesRequest {
  type: "applyChanges";
  changes: Array<WireChange & { seq: number }>;
}

/** Read the issues list view locally (buildIssuesView over the OPFS replica). */
export interface QueryIssuesRequest {
  type: "queryIssues";
  limit?: number;
  offset?: number;
}

/** Read one issue's detail view locally (buildIssueDetail). */
export interface QueryIssueDetailRequest {
  type: "queryIssueDetail";
  identifier: string;
}

/** Read the pull-requests view locally (buildPullsView). */
export interface QueryPullsRequest {
  type: "queryPulls";
  limit?: number;
  offset?: number;
}

/** Read the persisted change-feed cursor (or null if no snapshot has ever completed). */
export interface GetCursorRequest {
  type: "getCursor";
}

/**
 * Close the DB cleanly BEFORE the Worker is terminated (CTC-114). `worker.terminate()` alone drops the
 * OPFS SyncAccessHandles by process kill; an explicit close releases them cooperatively so the next
 * boot (same tab reload, or another tab winning the Web Lock) doesn't race a not-yet-released pool.
 */
export interface CloseRequest {
  type: "close";
}

export type ReplicaRequest =
  | OpenRequest
  | SeedBeginRequest
  | SeedBatchRequest
  | SeedCommitRequest
  | SeedAbortRequest
  | ApplyChangesRequest
  | QueryIssuesRequest
  | QueryIssueDetailRequest
  | QueryPullsRequest
  | GetCursorRequest
  | CloseRequest;

// ── Responses (worker → main thread) ─────────────────────────────────────────────────────────────

/** Every request carries an `id`; the worker echoes it so the client can settle the matching promise. */
export interface Envelope<T extends ReplicaRequest = ReplicaRequest> {
  id: number;
  request: T;
}

/** A successful reply, discriminated on the originating request `type`. `result` shape follows from it. */
export interface ReplicaOk {
  id: number;
  ok: true;
  /** The reply payload — view rows, a cursor, an applied count, or null/void for open/seed. */
  result: unknown;
}

/** A failed reply — the worker never throws across the boundary; it serializes the message. */
export interface ReplicaErr {
  id: number;
  ok: false;
  error: string;
}

export type ReplicaResponse = ReplicaOk | ReplicaErr;

/** Result of an applyChanges call — how many rows actually landed and the cursor afterwards. */
export interface ApplyChangesResult {
  applied: number;
  cursor: number;
}

/** Strongly-typed result lookup so the client can cast a reply by the request type it sent. */
export interface ResultMap {
  open: void;
  seedBegin: void;
  seedBatch: void;
  seedCommit: number;
  seedAbort: void;
  applyChanges: ApplyChangesResult;
  queryIssues: IssueView[];
  queryIssueDetail: IssueDetailView | null;
  queryPulls: PullView[];
  getCursor: number | null;
  close: void;
}
