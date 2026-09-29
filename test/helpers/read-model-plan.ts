// CTC-4324 — the instrument the read-model DDL tests share: record the statements a read-model builder
// issues, then ask SQLite how it would run each one (EXPLAIN QUERY PLAN) over the same store.
import {
  READ_INDEXES,
  SEARCH_FTS_TABLES,
  type SqlExecutor,
  type SqlValue,
} from "@catalyst-cloud/read-model";

export interface RecordedStatement {
  query: string;
  bindings: SqlValue[];
}

/** Run `build` over `sql` and return every statement it issued, in order. */
export function recordStatements(
  sql: SqlExecutor,
  build: (sql: SqlExecutor) => unknown,
): RecordedStatement[] {
  const seen: RecordedStatement[] = [];
  build({
    exec: (query, ...bindings) => {
      seen.push({ query, bindings });
      return sql.exec(query, ...bindings);
    },
  });
  return seen;
}

/** The EXPLAIN QUERY PLAN detail lines of every recorded statement, flattened. */
export function planDetails(sql: SqlExecutor, statements: RecordedStatement[]): string[] {
  return statements.flatMap(({ query, bindings }) =>
    sql
      .exec(`EXPLAIN QUERY PLAN ${query}`, ...bindings)
      .toArray()
      .map((row) => String(row.detail)),
  );
}

/** Plan lines that walk a whole table with no index: `SCAN <table>` or `SCAN <table> AS <alias>`, or
 *  a bare `SCAN <alias>` for one of the aliases given. `SCAN … USING [COVERING] INDEX` is an ordered
 *  index walk, not a full scan, and does not match. */
export function fullScans(details: string[], names: readonly string[]): string[] {
  const alt = names.join("|");
  const re = new RegExp(`^SCAN (${alt})( AS \\w+)?$`);
  return details.filter((d) => re.test(d));
}

/** The read-model index and FTS object names a store holds, from its own sqlite_master. */
export function readModelObjects(sql: SqlExecutor): { indexes: string[]; ftsTables: string[] } {
  const names = new Set(
    sql
      .exec("SELECT name FROM sqlite_master")
      .toArray()
      .map((r) => String(r.name)),
  );
  return {
    indexes: READ_INDEXES.map((i) => i.name).filter((n) => names.has(n)),
    ftsTables: SEARCH_FTS_TABLES.filter((n) => names.has(n)),
  };
}

/** The whole schema, as text, so a test can prove a second open changed nothing. */
export function schemaText(sql: SqlExecutor): string[] {
  return sql
    .exec("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name")
    .toArray()
    .map((r) => `${r.type} ${r.name} ${r.tbl_name} ${r.sql ?? ""}`);
}

/** The FTS fill markers, so a test can prove a second open rebuilt nothing. */
export function ftsMarkers(sql: SqlExecutor): Record<string, SqlValue>[] {
  return sql.exec("SELECT name, rebuilt_at FROM search_fts_state ORDER BY name").toArray();
}
