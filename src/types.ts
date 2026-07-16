// catalyst-cloud-sdk wire types — the change-feed contract, SELF-CONTAINED.
//
// The `EntityName` table-name union, the `ChangeOp` op union, and the change/sync/resync frame shapes
// the catalyst-cloud service broadcasts over `/connect`. The SDK is published standalone and owns its
// own copy of this contract; a contract test (test/contract.test.ts) pins the literal members so any
// drift is caught.
//
// This module is free of any runtime-specific or DOM imports — pure type declarations + plain JS
// runtime constants — so it imports cleanly into a browser bundle or a node/bun app alike.

/** The tenant id (also the mirror's name). `"tenant-0"` is an example tenant. */
export type AccountId = string;

/**
 * Canonical mirror table names — the `entity` field on the change-feed wire.
 *
 * The runtime list `ENTITY_NAMES` below is the same set as an array so the contract test can assert
 * membership at runtime (a bare `type` alias erases to nothing and can't be checked).
 */
export type EntityName =
  | "issues"
  | "labels"
  | "users"
  | "issue_labels"
  | "relations"
  | "issue_history"
  | "projects"
  | "cycles"
  | "initiatives"
  | "project_initiatives"
  | "comments"
  | "pull_requests"
  | "check_runs"
  | "commit_statuses"
  | "reviews";

/**
 * The `EntityName` union as a runtime array, in the SAME ORDER as the type above. Frozen so consumers
 * (and the contract test) can iterate the canonical table set. Kept in lockstep with `EntityName` —
 * the contract test asserts the two agree.
 */
export const ENTITY_NAMES = [
  "issues",
  "labels",
  "users",
  "issue_labels",
  "relations",
  "issue_history",
  "projects",
  "cycles",
  "initiatives",
  "project_initiatives",
  "comments",
  "pull_requests",
  "check_runs",
  "commit_statuses",
  "reviews",
] as const satisfies readonly EntityName[];

/** The change op — the change-feed wire contract. */
export type ChangeOp = "upsert" | "delete";

/** The `ChangeOp` union as a runtime array (same lockstep contract as `ENTITY_NAMES`). */
export const CHANGE_OPS = ["upsert", "delete"] as const satisfies readonly ChangeOp[];

/**
 * A live change frame off the `/connect` WebSocket — the exact shape the service broadcasts and
 * replays. One change from the feed, serialized.
 */
export interface ChangeFrame {
  type: "change";
  accountId: AccountId;
  /** The feed sequence — the monotonic cursor the replica advances to. */
  seq: number;
  entity: EntityName;
  /** The entity's primary key (composite PKs joined with ':'). */
  entityId: string;
  op: ChangeOp;
  /** The full normalized row for "upsert"; absent / partial for "delete". */
  row?: Record<string, unknown>;
}

/**
 * The underflow control frame: the consumer's cursor predates the service's retained change buffer →
 * the consumer must re-seed from a full /snapshot.
 */
export interface ResyncFrame {
  type: "resync";
  accountId?: AccountId;
}

/**
 * The catch-up request the consumer sends on every (re)connect: "replay everything after this
 * cursor, in seq order". The service answers with the missed `ChangeFrame`s, or a `ResyncFrame` if
 * `after` predates the retained buffer (cursor underflow).
 */
export interface SyncFrame {
  type: "sync";
  /** The durable cursor: the last feed seq the consumer has applied (-1 if none). */
  after: number;
}

/**
 * The end-of-pass head nudge (CTL-1402). The mirror's reconcile pass appends `change_log` rows but
 * never broadcasts them individually, so a quiet-webhook period leaves the client with no later frame
 * to detect a gap FROM. After a pass the mirror broadcasts ONE `{type:"head", seq:<max change_log
 * seq>}` so the client can notice its baseline trails the feed head and re-request the hole. It is a
 * pure control nudge — never applied and never a cursor advance. Transport-internal: the
 * {@link LiveSyncClient} consumes it and NEVER surfaces it to `onFrame`/`onChange`.
 */
export interface HeadFrame {
  type: "head";
  accountId?: AccountId;
  /** The mirror's current feed head — the max `change_log` seq at the end of the reconcile pass. */
  seq: number;
}

/**
 * The liveness ping/pong wire literals (CTC-135). The client sends `PING_FRAME` after an idle
 * interval; the mirror answers `PONG_FRAME` via `setWebSocketAutoResponse`, which matches the request
 * string BYTE-FOR-BYTE and replies WITHOUT waking a hibernated Durable Object. These must therefore
 * be pinned bytes — never `JSON.stringify(...)` at runtime (a different key order or spacing would
 * silently stop matching). The mirror pins the identical literals (`apps/mirror/src/do/ws.ts`); a
 * contract test in each repo asserts the two agree.
 */
export const PING_FRAME = '{"type":"ping"}';
export const PONG_FRAME = '{"type":"pong"}';

/**
 * The liveness pong the mirror's auto-response returns for a client `PING_FRAME`. Transport-internal:
 * the {@link LiveSyncClient} watchdog consumes it to prove the socket is alive and NEVER surfaces it
 * to `onFrame`/`onChange`.
 */
export interface PongFrame {
  type: "pong";
}

/** Any frame the service can push to a consumer over `/connect`. `pong` and `head` are
 *  transport-internal (consumed by the client, never surfaced to `onFrame`/`onChange`). */
export type ServerFrame = ChangeFrame | ResyncFrame | PongFrame | HeadFrame;
