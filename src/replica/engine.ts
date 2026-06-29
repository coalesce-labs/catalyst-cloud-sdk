// @catalyst-cloud/sdk/node — the portable sqlite ENGINE seam + the injected driver factories.
//
// CatalystReplica never statically imports a sqlite driver: that is how it avoids hard-depping one.
// It defines one tiny portable `ReplicaEngine` — the union of replicate's `ReplicaWriteDb` (run/get),
// read-model's `SqlExecutor` (all), and schema's `MigrationDb` (exec/query) primitives, plus a
// per-engine `toBindable` (wire JSON → engine-bindable) and `transaction`/`close`. CatalystReplica
// adapts THIS one object into the three ports it needs.
//
// Drivers are INJECTED, not imported. The SDK ships three factory adapters, each dynamic-import()ing
// its driver only when called:
//   • bunSqliteEngine(dbPath)        — `await import("bun:sqlite")` (runtime builtin → never in package.json)
//   • nodeSqliteEngine(dbPath)       — `await import("node:sqlite")` { DatabaseSync } (Node >=22.5, builtin)
//   • betterSqlite3Engine(driver, …) — the consumer passes `import Database from "better-sqlite3"`
//     (an OPTIONAL peer, never statically imported here).
// Any other driver (D1, sql.js, …) is supported by passing a custom `ReplicaEngine`.

/** A bindable SQLite scalar on the node/bun side (booleans stored as 0/1 INTEGER; blobs as Uint8Array). */
export type EngineBindable = string | number | bigint | null | Uint8Array;

// Load RUNTIME-BUILTIN drivers (`bun:sqlite` / `node:sqlite`) via `createRequire`, NOT a literal
// `import("…")`. Vite/vitest and esbuild rewrite (and break) a literal dynamic import of a builtin —
// they strip the `node:`/`bun:` prefix and fail to resolve a bare `sqlite`; and a `new Function`
// import throws "dynamic import callback was not specified" under node ESM. `require` of a builtin is
// untouched by the bundler and resolves natively on both node and Bun. This keeps the drivers OUT of
// package.json (they are platform builtins) and loaded only when a factory is called.
import { createRequire } from "node:module";

const requireBuiltin = createRequire(import.meta.url);

/** Require a RUNTIME-BUILTIN driver module; the type is supplied by the caller via `typeof import(...)`. */
function importBuiltin<T>(specifier: string): Promise<T> {
  return Promise.resolve(requireBuiltin(specifier) as T);
}

/**
 * The portable sqlite engine the SDK owns and adapts. Generic over the bindable type `B` so a driver
 * that binds binary/bigint differently can declare its own; the node/bun builtins use `EngineBindable`.
 */
export interface ReplicaEngine<B = unknown> {
  /** Execute DDL / migration statement(s) for side effect (no rows). */
  exec(sql: string): void;
  /** Run a parameterized SELECT → rows as plain objects (reads + the migration ledger). */
  all(sql: string, ...bindings: B[]): Record<string, B>[];
  /** Run a parameterized mutation → sqlite3_changes() (rows written). */
  run(sql: string, ...bindings: B[]): number;
  /** Run a single-row query → the first row, or undefined. */
  get(sql: string, ...bindings: B[]): Record<string, B> | undefined;
  /** Run `fn` inside one atomic transaction (snapshot-seed batch / delta apply). */
  transaction<T>(fn: () => T): T;
  /** Coerce ONE wire JSON value to an engine-bindable scalar (bool → 0/1, blob/bigint per engine). */
  toBindable: (value: unknown) => B;
  /** Close the underlying database handle. */
  close(): void;
}

/** A factory that opens a `ReplicaEngine` over `dbPath`. May be async (drivers are dynamic-imported). */
export type EngineFactory = (dbPath: string) => ReplicaEngine | Promise<ReplicaEngine>;

/** Coerce a wire JSON value to a node/bun-bindable scalar. Booleans → 0/1, blobs → Uint8Array, nested
 *  object/array → JSON text (DO rows are flat scalars; this just keeps one odd field from wedging sync). */
function toBindable(value: unknown): EngineBindable {
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "string" || typeof value === "number" || typeof value === "bigint") return value;
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  return JSON.stringify(value);
}

/** A prepared-statement shape every driver adapter reduces to (re-prepared per call, like the DO). */
interface StatementPort {
  all(...bindings: EngineBindable[]): Record<string, EngineBindable>[];
  get(...bindings: EngineBindable[]): Record<string, EngineBindable> | undefined;
  run(...bindings: EngineBindable[]): number;
}

/** The per-driver pieces `makeEngine` needs; everything else (the three ports) is shared. */
interface EngineDriver {
  handle: unknown;
  exec(sql: string): void;
  prepare(sql: string): StatementPort;
  transaction<T>(fn: () => T): T;
  close(): void;
}

/** A `ReplicaEngine` plus the raw driver `handle` (exposed via CatalystReplica.handle for drizzle). */
export type ReplicaEngineWithHandle = ReplicaEngine<EngineBindable> & { readonly handle: unknown };

/** Fold a driver into the portable engine. `all`/`run`/`get` re-prepare per call (bounded read path,
 *  and applyDelta's SQL varies per row anyway, so statement caching would not help). */
function makeEngine(driver: EngineDriver): ReplicaEngineWithHandle {
  return {
    handle: driver.handle,
    exec: (sql) => driver.exec(sql),
    all: (sql, ...bindings) => driver.prepare(sql).all(...bindings),
    run: (sql, ...bindings) => driver.prepare(sql).run(...bindings),
    get: (sql, ...bindings) => driver.prepare(sql).get(...bindings),
    transaction: (fn) => driver.transaction(fn),
    toBindable,
    close: () => driver.close(),
  };
}

/**
 * `bun:sqlite` engine. `db.query(sql)` prepares; `.run().changes` is the write count; WAL keeps
 * concurrent readers non-blocking. `bun:sqlite` is a Bun runtime builtin → dynamic-imported, never a
 * package.json dependency.
 *
 * The WRITER opens read-write + WAL. The READER (`readonly`) opens `{ readonly: true }` and sets
 * `busy_timeout = 250` instead of WAL — exactly how CTL's replica-read.mjs opens the same file: under
 * the single-writer/many-reader topology (ADR-0008) a checkpoint can briefly hold the lock, so a
 * reader waits a beat rather than failing. A readonly handle runs NO migrations and rejects writes at
 * the sqlite layer.
 */
async function openBun(dbPath: string, readonly: boolean): Promise<ReplicaEngineWithHandle> {
  const { Database } = await importBuiltin<typeof import("bun:sqlite")>("bun:sqlite");
  const db = readonly ? new Database(dbPath, { readonly: true }) : new Database(dbPath);
  try {
    db.run(readonly ? "PRAGMA busy_timeout = 250" : "PRAGMA journal_mode = WAL");
  } catch {
    // :memory: rejects WAL on some builds; busy_timeout is harmless either way.
  }
  return makeEngine({
    handle: db,
    exec: (sql) => db.exec(sql),
    prepare: (sql) => {
      const st = db.query(sql);
      return {
        all: (...b) => st.all(...b) as Record<string, EngineBindable>[],
        get: (...b) => (st.get(...b) ?? undefined) as Record<string, EngineBindable> | undefined,
        run: (...b) => st.run(...b).changes,
      };
    },
    transaction: (fn) => db.transaction(fn)(),
    close: () => db.close(),
  });
}

export function bunSqliteEngine(dbPath: string): Promise<ReplicaEngineWithHandle> {
  return openBun(dbPath, false);
}

/** Read-only `bun:sqlite` engine (`{ readonly: true }` + `busy_timeout`). See {@link openBun}. */
export function bunSqliteReadonlyEngine(dbPath: string): Promise<ReplicaEngineWithHandle> {
  return openBun(dbPath, true);
}

/**
 * `node:sqlite` engine (Node >=22.5; may need `--experimental-sqlite` on some 22.x). `DatabaseSync`
 * has no `.transaction()` helper, so we wrap BEGIN/COMMIT/ROLLBACK. Also a runtime builtin → never a
 * package.json dependency. The `readonly` variant opens `{ readOnly: true }` (node:sqlite's spelling)
 * + `busy_timeout`; see {@link openBun} for the read-only rationale.
 */
async function openNode(dbPath: string, readonly: boolean): Promise<ReplicaEngineWithHandle> {
  const { DatabaseSync } = await importBuiltin<typeof import("node:sqlite")>("node:sqlite");
  const db = readonly ? new DatabaseSync(dbPath, { readOnly: true }) : new DatabaseSync(dbPath);
  try {
    db.exec(readonly ? "PRAGMA busy_timeout = 250" : "PRAGMA journal_mode = WAL");
  } catch {
    // :memory: cannot WAL — harmless; busy_timeout is allowed on a readonly handle.
  }
  return makeEngine({
    handle: db,
    exec: (sql) => db.exec(sql),
    prepare: (sql) => {
      const st = db.prepare(sql);
      return {
        all: (...b) => st.all(...b) as Record<string, EngineBindable>[],
        get: (...b) => (st.get(...b) ?? undefined) as Record<string, EngineBindable> | undefined,
        run: (...b) => Number(st.run(...b).changes),
      };
    },
    transaction: (fn) => {
      db.exec("BEGIN");
      try {
        const out = fn();
        db.exec("COMMIT");
        return out;
      } catch (err) {
        db.exec("ROLLBACK");
        throw err;
      }
    },
    close: () => db.close(),
  });
}

export function nodeSqliteEngine(dbPath: string): Promise<ReplicaEngineWithHandle> {
  return openNode(dbPath, false);
}

/** Read-only `node:sqlite` engine (`{ readOnly: true }` + `busy_timeout`). See {@link openNode}. */
export function nodeSqliteReadonlyEngine(dbPath: string): Promise<ReplicaEngineWithHandle> {
  return openNode(dbPath, true);
}

/** A statement handle the `better-sqlite3` default export hands back (typed structurally — no import). */
interface BetterSqlite3Statement {
  all(...bindings: unknown[]): unknown[];
  get(...bindings: unknown[]): unknown;
  run(...bindings: unknown[]): { changes: number | bigint; lastInsertRowid: number | bigint };
}
/** The `better-sqlite3` Database surface this engine uses (typed structurally — no import / no peer pin). */
interface BetterSqlite3Database {
  prepare(sql: string): BetterSqlite3Statement;
  exec(sql: string): void;
  pragma(source: string): unknown;
  transaction<Args extends unknown[], R>(fn: (...args: Args) => R): (...args: Args) => R;
  close(): void;
}
/** `import Database from "better-sqlite3"` is structurally assignable to this constructor type. */
export type BetterSqlite3Driver = new (path?: string, options?: unknown) => BetterSqlite3Database;

/**
 * `better-sqlite3` engine. The consumer passes the driver constructor (`import Database from
 * "better-sqlite3"`) so the SDK never statically imports it — it stays an OPTIONAL peer the consumer's
 * bundler resolves. Synchronous (the driver is already loaded), so no dynamic import here. The
 * `readonly` variant opens `{ readonly: true }` + `busy_timeout`; see {@link openBun}.
 */
function openBetter(
  driver: BetterSqlite3Driver,
  dbPath: string,
  readonly: boolean,
): ReplicaEngineWithHandle {
  const db = readonly ? new driver(dbPath, { readonly: true }) : new driver(dbPath);
  try {
    db.pragma(readonly ? "busy_timeout = 250" : "journal_mode = WAL");
  } catch {
    // :memory: cannot WAL — harmless; busy_timeout is allowed on a readonly handle.
  }
  return makeEngine({
    handle: db,
    exec: (sql) => db.exec(sql),
    prepare: (sql) => {
      const st = db.prepare(sql);
      return {
        all: (...b) => st.all(...b) as Record<string, EngineBindable>[],
        get: (...b) => (st.get(...b) ?? undefined) as Record<string, EngineBindable> | undefined,
        run: (...b) => Number(st.run(...b).changes),
      };
    },
    transaction: (fn) => db.transaction(fn)(),
    close: () => db.close(),
  });
}

export function betterSqlite3Engine(
  driver: BetterSqlite3Driver,
  dbPath: string,
): ReplicaEngineWithHandle {
  return openBetter(driver, dbPath, false);
}

/** Read-only `better-sqlite3` engine (`{ readonly: true }` + `busy_timeout`). See {@link openBetter}. */
export function betterSqlite3ReadonlyEngine(
  driver: BetterSqlite3Driver,
  dbPath: string,
): ReplicaEngineWithHandle {
  return openBetter(driver, dbPath, true);
}

/**
 * Auto-detect the default engine when `opts.engine` is omitted: Bun → `bunSqliteEngine`; else
 * `node:sqlite` if available → `nodeSqliteEngine`; else throw a clear "pass opts.engine" error rather
 * than pick an engine that resolves but breaks on first write.
 */
export async function autoDetectEngine(dbPath: string): Promise<ReplicaEngineWithHandle> {
  if (typeof (globalThis as { Bun?: unknown }).Bun !== "undefined") {
    return bunSqliteEngine(dbPath);
  }
  try {
    return await nodeSqliteEngine(dbPath);
  } catch (err) {
    throw new Error(
      "CatalystReplica: no sqlite engine available — pass opts.engine. " +
        "Auto-detect needs Bun (bun:sqlite) or Node >=22.5 (node:sqlite). " +
        `Underlying error: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/** READ-ONLY counterpart of {@link autoDetectEngine}: Bun → `bunSqliteReadonlyEngine`; else
 *  `nodeSqliteReadonlyEngine`. Used by `CatalystReplica.openReadOnly` when no engine is injected. */
export async function autoDetectReadonlyEngine(dbPath: string): Promise<ReplicaEngineWithHandle> {
  if (typeof (globalThis as { Bun?: unknown }).Bun !== "undefined") {
    return bunSqliteReadonlyEngine(dbPath);
  }
  try {
    return await nodeSqliteReadonlyEngine(dbPath);
  } catch (err) {
    throw new Error(
      "CatalystReplica: no read-only sqlite engine available — pass opts.engine. " +
        "Auto-detect needs Bun (bun:sqlite) or Node >=22.5 (node:sqlite). " +
        `Underlying error: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}
