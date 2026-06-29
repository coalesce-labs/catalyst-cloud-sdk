// Minimal ambient declarations for the two RUNTIME-BUILTIN sqlite drivers the `./node` engine
// factories dynamic-import — `bun:sqlite` (Bun) and `node:sqlite` (Node >=22.5). They are platform
// builtins, never npm packages, so they must NOT appear in package.json; declaring just the surface
// the factories touch lets `tsc` type-check `await import("bun:sqlite")` / `await import("node:sqlite")`
// without pulling in `@types/bun` or `@types/node`.
//
// This is a build-time-only input: `tsc` never emits `.d.ts` inputs, and the package ships `dist/`
// only (package.json "files"), so these declarations cannot leak into a consumer's type graph or
// collide with their own `@types/node`. `better-sqlite3` is deliberately NOT declared here — it is an
// optional npm peer the consumer passes in (typed structurally by `BetterSqlite3Driver`), never
// imported by the SDK.

declare module "bun:sqlite" {
  /** A bindable / storable SQLite scalar (matches Bun's `SQLQueryBindings` leaf). */
  export type SQLValue = string | number | bigint | boolean | null | Uint8Array;

  /** A prepared-statement handle (returned by Database.query / Database.prepare). */
  export class Statement<Row = unknown> {
    all(...params: SQLValue[]): Row[];
    get(...params: SQLValue[]): Row | null;
    run(...params: SQLValue[]): { changes: number; lastInsertRowid: number | bigint };
  }

  export class Database {
    constructor(path?: string, options?: { readonly?: boolean; create?: boolean; strict?: boolean });
    query<Row = unknown>(sql: string): Statement<Row>;
    prepare<Row = unknown>(sql: string): Statement<Row>;
    run(sql: string, ...params: SQLValue[]): void;
    exec(sql: string): void;
    transaction<Args extends unknown[], R>(fn: (...args: Args) => R): (...args: Args) => R;
    close(): void;
  }

  export default Database;
}

declare module "node:module" {
  /** Build a CommonJS `require` bound to `path` — used to load runtime-builtin sqlite drivers. */
  export function createRequire(path: string | URL): (id: string) => unknown;
}

// Minimal `node:fs` surface the single-writer guard (src/replica/writer-lock.ts) touches: an atomic
// exclusive create (`openSync(path,"wx")`) for the lock claim, plus the heartbeat read/write/unlink.
// Declared here (not via `@types/node`) for the same reason as the sqlite builtins — a build-time-only
// input that ships nothing into a consumer's type graph. `mkdtempSync`/`rmSync` are used only by tests.
declare module "node:fs" {
  export function openSync(path: string, flags: string): number;
  export function writeSync(fd: number, data: string): number;
  export function closeSync(fd: number): void;
  export function readFileSync(path: string, encoding: "utf8" | "utf-8"): string;
  export function writeFileSync(path: string, data: string): void;
  export function existsSync(path: string): boolean;
  export function unlinkSync(path: string): void;
  export function mkdtempSync(prefix: string): string;
  export function rmSync(path: string, options?: { recursive?: boolean; force?: boolean }): void;
}

declare module "node:sqlite" {
  /** A prepared-statement handle (returned by DatabaseSync.prepare). */
  export interface StatementSync {
    all(...params: unknown[]): unknown[];
    get(...params: unknown[]): unknown;
    run(...params: unknown[]): { changes: number | bigint; lastInsertRowid: number | bigint };
  }

  export class DatabaseSync {
    constructor(path?: string, options?: unknown);
    exec(sql: string): void;
    prepare(sql: string): StatementSync;
    close(): void;
  }
}
