// replica/browser/ports.ts — the three typed ports the browser replica drives over a sqlite-wasm OO1
// `Database`, split from the OPFS opener (sqlite-db.ts) so the SAME port-building code serves BOTH the
// production OPFS SAHPool DB and a plain `:memory:` DB in node-side tests (sqlite-wasm runs fine under
// node — no OPFS required to exercise seed/apply/query logic).
//
// The whole point of ADR-0002 is "one query, four runtimes": the read-model's `SqlExecutor` and the
// schema package's `MigrationDb` are structural interfaces the DO (Workers SqlStorage), the host-sync
// daemon (bun:sqlite), and the browser all satisfy. This file is the browser adapter — the exact
// counterpart to the SDK node engine (src/replica/engine.ts), plus a small write port for the
// delta-apply path.
//
//   • SqlExecutor (read)   — read-model: exec(q, ...binds).toArray() → object rows.
//   • MigrationDb (migrate)— schema: exec(sql) + query(sql) (no bindings).
//   • ReplicaDb (write)    — run/get with positional bindings, for the apply path (apply.ts).

import type { Database } from "@sqlite.org/sqlite-wasm";
import type { SqlExecutor, SqlValue } from "@catalyst-cloud/read-model";
import { applyMigrations, MIRROR_MIGRATIONS, type MigrationDb } from "@catalyst-cloud/schema";

/**
 * The write port the delta-apply path (apply.ts) drives. `run` executes a mutation with positional `?`
 * bindings and reports whether it changed a row (mirrors bun:sqlite's `db.query(sql).run(...).changes`);
 * `get` runs a single-row read; `transaction` wraps a unit of work in BEGIN/COMMIT (ROLLBACK on throw)
 * so a snapshot seed lands atomically — exactly the guarantee host-sync gets from bun's `db.transaction`.
 */
export interface ReplicaDb {
  /** Execute a mutation; returns the number of rows changed (sqlite3_changes()). */
  run(sql: string, ...bindings: SqlValue[]): number;
  /** Run a single-row query; returns the first row as an object, or undefined. */
  get(sql: string, ...bindings: SqlValue[]): Record<string, SqlValue> | undefined;
  /** Wrap `fn` in a transaction (BEGIN/COMMIT; ROLLBACK + rethrow on error). */
  transaction<T>(fn: () => T): T;
}

/** Coerce a wire JSON value to a sqlite-wasm-bindable scalar. Booleans → 0/1, nested objects → JSON
 *  text, undefined/null → null. Mirrors the node engine's `toBindable` so both replicas store the
 *  same bytes for the same wire row. */
export function toBindable(v: unknown): SqlValue {
  if (v === null || v === undefined) return null;
  if (typeof v === "boolean") return v ? 1 : 0;
  if (typeof v === "string" || typeof v === "number") return v;
  // bigint is outside the read-model SqlValue union (no column needs it) — coerce to number.
  if (typeof v === "bigint") return Number(v);
  if (v instanceof ArrayBuffer) return v;
  if (v instanceof Uint8Array) {
    // Copy to a standalone ArrayBuffer (the view may be a slice of a larger buffer).
    return v.slice().buffer;
  }
  // Nested object/array (shouldn't occur — DO rows are flat scalars) → JSON text rather than throw.
  return JSON.stringify(v);
}

/** The opened replica: the raw OO1 handle plus the three typed ports the worker drives. */
export interface OpenedReplica {
  /** read-model read seam — buildIssuesView/buildIssueDetail/buildPullsView run over this UNCHANGED. */
  readonly read: SqlExecutor;
  /** delta-apply write seam — apply.ts (the host-sync twin) routes through this. */
  readonly write: ReplicaDb;
  /** the underlying OO1 db handle (for close / capacity introspection). */
  readonly db: Database;
  /** release the handle (under OPFS SAHPool this frees the SyncAccessHandles for another tab). */
  close(): void;
}

/** A sqlite-wasm bindable array, narrowed to what the read-model/apply paths actually bind. */
function bindArgs(bindings: SqlValue[]): SqlValue[] | undefined {
  return bindings.length > 0 ? bindings : undefined;
}

/**
 * Build the three ports over an ALREADY-OPEN OO1 database, bring its schema up to date by applying the
 * SHARED migration bundle (the same `MIRROR_MIGRATIONS` the DO + daemon use), and create the host-only
 * `sync_meta` cursor table. Pure over the handle — the production opener (sqlite-db.ts, OPFS SAHPool)
 * and the test opener (`:memory:`) both route through here, so the ports can never drift between them.
 */
export function buildOpenedReplica(db: Database): OpenedReplica {
  // read-model SqlExecutor — the load-bearing ADR-0002 proof in the browser. exec(query, ...binds)
  // returns object rows keyed by SELECTed column alias, which buildIssuesView/etc consume directly.
  const read: SqlExecutor = {
    exec: (query: string, ...bindings: SqlValue[]) => ({
      toArray: () =>
        db.exec(query, {
          bind: bindArgs(bindings),
          rowMode: "object",
          returnValue: "resultRows",
        }) as Record<string, SqlValue>[],
    }),
  };

  // Write port — positional-binding mutations + single-row reads + a transaction wrapper.
  const write: ReplicaDb = {
    run: (sql: string, ...bindings: SqlValue[]) => {
      db.exec(sql, { bind: bindArgs(bindings) });
      // sqlite3_changes() via the OO1 helper — how many rows the last statement touched.
      return db.changes();
    },
    get: (sql: string, ...bindings: SqlValue[]) => {
      const rows = db.exec(sql, {
        bind: bindArgs(bindings),
        rowMode: "object",
        returnValue: "resultRows",
      }) as Record<string, SqlValue>[];
      return rows[0];
    },
    transaction: <T>(fn: () => T): T => {
      db.exec("BEGIN");
      try {
        const out = fn();
        db.exec("COMMIT");
        return out;
      } catch (err) {
        try {
          db.exec("ROLLBACK");
        } catch {
          // ROLLBACK can throw if the txn already aborted — swallow so the original error surfaces.
        }
        throw err;
      }
    },
  };

  // Apply the cross-surface mirror migrations via the ~3-line MigrationDb adapter (node-engine twin).
  // exec runs ONE statement; query returns object rows. No bindings — the runner inlines its only literal.
  const migrationAdapter: MigrationDb = {
    exec: (sql) => {
      db.exec(sql);
    },
    query: (sql) =>
      db.exec(sql, { rowMode: "object", returnValue: "resultRows" }) as Array<
        Record<string, unknown>
      >,
  };
  applyMigrations(migrationAdapter, MIRROR_MIGRATIONS);

  // Host-only bookkeeping table (NOT in the DO mirror schema, so not in the bundle) — the cursor.
  // Identical to the node engine's SYNC_META_DDL.
  db.exec("CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value TEXT)");

  return {
    read,
    write,
    db,
    close: () => db.close(),
  };
}
