// replica/read-model-ddl.ts — CTC-4324: every replica builds read-model's index set and FTS5 search
// tables when it opens, shared by the node engine (catalyst-replica.ts) and the browser/OPFS port
// (ports.ts). The DDL itself is read-model's `ensureReadModelDdl`, the same call the Mirror DO makes
// after its migrations; nothing here copies it.
//
// ⛔ THE EXECUTOR HANDED TO IT MUST BE EAGER. `ensureReadModelDdl` issues its DDL and writes as bare
// `sql.exec(...)` calls and never reads the result, which is right on a Durable Object (`SqlStorage`
// runs on `exec`). Both replica read executors run a statement only inside `toArray()`, so passing one
// in would create nothing and report success. This adapter runs every statement at `exec` time.
//
// Kept free of node imports (unlike engine.ts), so the browser bundle can share it.
import { ensureReadModelDdl, type SqlExecutor, type SqlValue } from "@catalyst-cloud/read-model";

/** What the DDL needs from a store: a row read, a write, and a transaction. The node engine and the
 *  browser `ReplicaDb` both satisfy it through a thin wrapper at their call sites. */
export interface ReadModelDdlPort {
  all(sql: string, ...bindings: SqlValue[]): Record<string, SqlValue>[];
  run(sql: string, ...bindings: SqlValue[]): unknown;
  transaction<T>(fn: () => T): T;
}

/** A statement that returns rows. `run` would drop them, and better-sqlite3's `all` throws on a
 *  statement that returns none, so each statement goes to the one that fits. */
function returnsRows(sql: string): boolean {
  return /^\s*(SELECT|WITH|PRAGMA|EXPLAIN)\b/i.test(sql);
}

/**
 * Apply read-model's indexes and FTS5 search tables. Must run after `MIRROR_MIGRATIONS`, which create
 * the tables. Idempotent: every statement is `IF NOT EXISTS`, and an FTS index is filled from its table
 * only until `search_fts_state` records the fill, so every later open writes nothing.
 *
 * One transaction: the first open on a warm replica fills each index from the rows it already holds,
 * and a crash mid-way rolls back to a store the next open simply builds again.
 *
 * Returns the FTS tables filled by this call (empty on every open after the first), so the caller can
 * log the one-time build.
 */
export function applyReadModelDdl(port: ReadModelDdlPort): string[] {
  const eager: SqlExecutor = {
    exec: (query: string, ...bindings: SqlValue[]) => {
      let rows: Record<string, SqlValue>[] = [];
      if (returnsRows(query)) rows = port.all(query, ...bindings);
      else port.run(query, ...bindings);
      return { toArray: () => rows };
    },
  };
  return port.transaction(() => ensureReadModelDdl(eager));
}
