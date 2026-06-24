// catalyst-cloud-sdk wire types — the change-feed contract, SELF-CONTAINED.
//
// This file is a deliberate, byte-for-byte copy of the portable wire contract from the
// catalyst-cloud monorepo's @catalyst-cloud/types package (packages/types/src/index.ts): the
// `EntityName` table-name union, the `ChangeOp` op union, and the change/sync/resync frame shapes
// the Mirror Durable Object broadcasts over `/connect`. The SDK is published standalone, so it must
// NOT depend on the internal @catalyst-cloud/types workspace package — instead it owns its own copy
// of the contract, and a contract test (test/contract.test.ts) pins the literal members so any drift
// from the monorepo is caught.
//
// Like @catalyst-cloud/types, this module is Cloudflare-Workers-free and DOM-free: it is pure type
// declarations + plain JS runtime constants, so it imports cleanly into a browser bundle, a node/bun
// app, and a Worker alike.

/** The tenant id. Also the Mirror DO name. `"tenant-0"` is Ryan's own workspace (Phase 0). */
export type AccountId = string;

/**
 * Canonical mirror table names — the `entity` field in change_log + the change-feed wire.
 *
 * Mirrors @catalyst-cloud/types `EntityName` exactly. The runtime list `ENTITY_NAMES` below is the
 * same set as an array so the contract test can assert membership at runtime (a bare `type` alias
 * erases to nothing and can't be checked).
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

/** change_log.op — the change-feed wire contract. */
export type ChangeOp = "upsert" | "delete";

/** The `ChangeOp` union as a runtime array (same lockstep contract as `ENTITY_NAMES`). */
export const CHANGE_OPS = ["upsert", "delete"] as const satisfies readonly ChangeOp[];

/**
 * A live change frame off the `/connect` WebSocket — the exact shape `apps/mirror/src/do/ws.ts`
 * broadcasts and replays. One row of the change_log, serialized.
 */
export interface ChangeFrame {
  type: "change";
  accountId: AccountId;
  /** The change_log seq — the monotonic cursor the replica advances to. */
  seq: number;
  entity: EntityName;
  /** The change_log.entity_id — the PK (composite PKs joined with ':'). */
  entityId: string;
  op: ChangeOp;
  /** The full normalized row for "upsert"; absent / partial for "delete". */
  row?: Record<string, unknown>;
}

/**
 * The underflow control frame: the consumer's cursor predates the DO's retained change_log ring →
 * the consumer must re-seed from a full /snapshot.
 */
export interface ResyncFrame {
  type: "resync";
  accountId?: AccountId;
}

/**
 * The catch-up request the consumer sends on every (re)connect: "replay everything after this
 * cursor, in seq order". The DO answers with the missed `ChangeFrame`s, or a `ResyncFrame` if `after`
 * predates the retained ring (cursor underflow).
 */
export interface SyncFrame {
  type: "sync";
  /** The durable cursor: the last change_log.seq the consumer has applied (-1 if none). */
  after: number;
}

/** Any frame the DO can push to a consumer over `/connect`. */
export type ServerFrame = ChangeFrame | ResyncFrame;
