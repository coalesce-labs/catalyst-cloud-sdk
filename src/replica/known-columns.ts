// replica/known-columns.ts — CTC-603: the REAL local table shape via PRAGMA introspection, shared by
// the node engine (catalyst-replica.ts) and the browser/OPFS port (ports.ts).
//
// @catalyst-cloud/replicate's `applyUpsert` defaults its forward-compat column filter
// (`ApplyOptions.knownColumns`) to ITS OWN bundled `@catalyst-cloud/schema` dependency — a separate,
// independently-versioned nested copy from whatever schema version THIS SDK's own `applyMigrations`
// actually used to create the local replica table. When replicate's nested schema lags behind, that
// default silently drops columns the local table genuinely has (CTC-529/575) — and when the local
// table is for an entity replicate's bundled schema doesn't recognize AT ALL, the whole delta failed
// (CTC-393, separately mitigated by a no-throw fix in replicate itself).
//
// `ApplyOptions.knownColumns` exists exactly to override that default (see the doc comment on it in
// @catalyst-cloud/replicate's src/replicate.ts: "a caller that wants PRAGMA-accurate columns"). This
// module builds that override from the ACTUAL local database — `PRAGMA table_info(<table>)` — rather
// than from ANY package's bundled, compile-time column list (replicate's OR this SDK's own
// @catalyst-cloud/schema). That is the whole point: the property this buys is "the delta-apply column
// filter always agrees with what SQLite itself reports the table having," independent of which
// package's schema version created it and independent of any two packages' pins staying in sync by
// hand — durable against a FUTURE version-skew instance, not just the one already fixed by exact pins.

/** The minimal multi-row query capability this needs. Satisfied structurally by the node engine's
 *  `ReplicaEngine.all` and the browser's `ReplicaDb.all` — no import from either lives here, so this
 *  module stays shareable with the browser bundle (unlike engine.ts, which pulls in `node:module`). */
export interface QueryAllPort {
  all(sql: string): Array<Record<string, unknown>>;
}

/**
 * Quote a SQL identifier for interpolation into `PRAGMA table_info(...)`.
 *
 * PRAGMA statements don't accept a bound `?` parameter for the table-name argument on any engine this
 * SDK supports (bun:sqlite, node:sqlite, sqlite-wasm) — the identifier must be inlined. SAFE here only
 * because every caller passes a name that came from {@link listLocalTables} (an enumeration of the
 * database's OWN `sqlite_master` rows), never external input.
 */
function quoteIdent(ident: string): string {
  return `"${ident.replace(/"/g, '""')}"`;
}

/** Every real user table currently in the local sqlite file — excludes sqlite-internal `sqlite_*`
 *  bookkeeping tables (e.g. `sqlite_sequence`). Includes host-only tables like `sync_meta` harmlessly:
 *  `change.entity` never names one, so an unused map entry costs nothing. */
function listLocalTables(db: QueryAllPort): string[] {
  const rows = db.all("SELECT name FROM sqlite_master WHERE type = 'table'");
  return rows.map((r) => String(r.name)).filter((name) => !name.startsWith("sqlite_"));
}

/**
 * Build a per-table known-columns map from the LOCAL database's ACTUAL shape (`PRAGMA table_info`),
 * for use as `ApplyOptions.knownColumns` at every `applyDelta` call site.
 *
 * Call ONCE, right after `applyMigrations` runs — the table shape it reflects is fixed until the next
 * migration run (a fresh `start()` / worker `open`).
 */
export function buildKnownColumnsByTable(db: QueryAllPort): Map<string, ReadonlySet<string>> {
  const map = new Map<string, ReadonlySet<string>>();
  for (const table of listLocalTables(db)) {
    const rows = db.all(`PRAGMA table_info(${quoteIdent(table)})`);
    if (rows.length === 0) continue; // defensive — a real table always reports at least one column
    map.set(table, new Set(rows.map((r) => String(r.name))));
  }
  return map;
}
