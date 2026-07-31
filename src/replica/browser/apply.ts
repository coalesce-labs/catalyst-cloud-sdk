// replica/apply.ts — the browser-side write path for the OPFS replica (CTC-51).
//
// The pure, schema-driven apply logic (applyDelta / truncate / cursor) now lives in the shared,
// runtime-agnostic @catalyst-cloud/replicate package (ADR-0002) — the SAME code apps/host-sync routes
// through, so the two replicas can't drift. This module is the thin BROWSER ADAPTER: it binds the shared
// apply path to the wasm `ReplicaDb` write port + its value coercion (sqlite-db.ts). The old hand-
// maintained "twin" copy of the SQL building is GONE — there is now one source of truth.
//
// Vite-safe: @catalyst-cloud/replicate has NO bun:sqlite import (it adapts to the injected handle), and
// the browser already imports the schema SSOT — so nothing new is dragged into the web bundle.
//
// WIRE CONTRACT (shared — see src/do/changefeed.ts): unchanged; documented in @catalyst-cloud/replicate.

import {
  applyDelta,
  getCursor as replicaGetCursor,
  setCursor as replicaSetCursor,
  truncateReplica as replicaTruncate,
} from "@catalyst-cloud/replicate";
import type { ReplicaDb } from "./ports.js";
import { toBindable } from "./ports.js";
import type { WireChange } from "./protocol.js";

/**
 * Wipe every replica entity table (but NOT sync_meta). Called before replaying a fresh /snapshot so a
 * resync can't leave orphaned rows.
 */
export function truncateReplica(db: ReplicaDb): void {
  replicaTruncate(db);
}

/** Read the persisted change-feed cursor (or null if a snapshot has never completed). */
export function getCursor(db: ReplicaDb): number | null {
  return replicaGetCursor(db);
}

/** Persist the change-feed cursor (the last applied change_log.seq). */
export function setCursor(db: ReplicaDb, cursor: number): void {
  replicaSetCursor(db, cursor, toBindable);
}

/**
 * Apply ONE change-feed record to the replica via the shared, schema-driven write path. Returns true
 * iff a row was actually written (a stale upsert rejected by the updated_at guard, or a delete of an
 * already-absent row, returns false).
 */
export function applyChange(db: ReplicaDb, change: WireChange): boolean {
  return applyDelta(db, change, toBindable);
}
