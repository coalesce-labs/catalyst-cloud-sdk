// replica/migration-shape.ts — CTC-127's row-shape predicate, hoisted so BOTH replicas can use it.
//
// It lived in catalyst-replica.ts, which is unreachable from the browser: that module top-level-imports
// ./engine.js, which imports `node:module`. Cross-importing it into the worker would drag a node builtin
// into a bundle destined for a Worker. This file imports ONLY @catalyst-cloud/schema, so both the node
// replica (catalyst-replica.ts, which re-exports it) and the browser worker (ports.ts) share one
// definition — and neither can drift from the other's idea of what a shape change is.

import { MIRROR_MIGRATIONS } from "@catalyst-cloud/schema";

/**
 * CTC-127: does any of the migrations applied THIS boot change a table's ROW SHAPE — i.e. add a column
 * (`ALTER TABLE … ADD …`) or a new entity table (`CREATE TABLE`)? If so, a WARM replica's existing rows
 * were seeded/applied before that column existed and hold NULL for it, so the caller forces one re-seed
 * to backfill. A pure `CREATE INDEX` migration changes no row shape → no re-seed. Detected by
 * string-matching the applied tags' SQL — drizzle-kit emits bare `ADD` and `CREATE TABLE` (the same
 * vocabulary @catalyst-cloud/schema's migration runner trusts). Conservative-safe: a false positive is
 * a harmless extra re-seed; there are no false negatives for real `ALTER TABLE ADD` / `CREATE TABLE`.
 */
export function migrationsChangeRowShape(
  appliedTags: readonly string[],
): boolean {
  const byTag = MIRROR_MIGRATIONS.migrations as Record<
    string,
    string | undefined
  >;
  return appliedTags.some((tag) => {
    const sql = byTag[tag] ?? "";
    return /\bADD\b/i.test(sql) || /\bCREATE\s+TABLE\b/i.test(sql);
  });
}
