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

/** Any frame the service can push to a consumer over `/connect`. */
export type ServerFrame = ChangeFrame | ResyncFrame;
