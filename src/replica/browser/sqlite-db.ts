// replica/browser/sqlite-db.ts — open the OPFS SAHPool replica (CTC-51 → CTC-114). Runs INSIDE the
// Worker (db.worker.ts); SAHPool's OPFS SyncAccessHandles are Worker-only. The port construction +
// migrations live in ports.ts (shared with the node-side `:memory:` test opener); this file owns only
// the OPFS-specific open.
//
// ⚠️ SINGLE-CONNECTION-PER-ORIGIN: SAHPool takes EXCLUSIVE SyncAccessHandles on the pool files, so a
// second tab (or a not-yet-released worker from a just-prior reload) booting the same `directory`
// throws `NoModificationAllowedError`. The main-thread Web Locks gate (browser-lock.ts) exists to
// keep a second tab from ever reaching this call — treat an escape here as the last line of defense,
// not the primary one.

import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type { SAHPoolUtil } from "@sqlite.org/sqlite-wasm";
import { buildOpenedReplica, type OpenedReplica } from "./ports.js";

export type { OpenedReplica } from "./ports.js";

/**
 * Is this throw "the bytes on disk are not a usable database"? (CTC-337)
 *
 * ⚠️ THE ALLOWLIST IS THE SAFETY PROPERTY, not the recovery it guards. A `true` here authorizes
 * DESTROYING the pool, so it must match ONLY genuine corruption. In particular it must never match
 * `NoModificationAllowedError` — that is a second tab (or a not-yet-released worker) holding the
 * SyncAccessHandles, and wiping on it would delete a database another live tab is actively using,
 * turning a self-healing recovery into data loss for someone else. Contention is transient and the
 * caller retries; corruption is permanent and only a wipe clears it. Anything unrecognised RETHROWS.
 */
export function isCorruptDatabaseError(err: unknown): boolean {
  const text =
    err instanceof Error ? `${err.name}: ${err.message}` : typeof err === "string" ? err : "";
  if (text === "") return false;
  // Explicit rather than relying on the absence of a corruption marker: this is the case that must
  // not wipe, so it should be impossible to reach the allowlist below by accident.
  if (/NoModificationAllowedError|InvalidStateError/i.test(text)) return false;
  return (
    /SQLITE_CORRUPT/i.test(text) ||
    /SQLITE_NOTADB/i.test(text) ||
    /database disk image is malformed/i.test(text) ||
    /file is not a database/i.test(text) ||
    /malformed database schema/i.test(text)
  );
}

/** The slice of SAHPoolUtil the recovery needs — narrowed so a test can supply a fake pool. */
export interface RecoverablePool {
  OpfsSAHPoolDb: new (dbPath: string) => unknown;
  wipeFiles: () => Promise<void>;
}

/**
 * Open the database, and if the FILE ITSELF is corrupt, destroy it and open a fresh one — ONCE.
 *
 * Safe in a way it would not be for most databases: this replica is 100% DERIVED. Every row came from
 * `/snapshot` plus the change feed, so the local copy holds nothing that does not exist upstream, and
 * re-seeding is the ordinary cold-start path rather than a recovery hack.
 *
 * Without this, one corrupt page bricked the replica PERMANENTLY. The corruption survives reloads, so
 * `start()` threw on every subsequent boot and the reader sat behind a red pill forever unless they
 * knew to clear origin storage by hand — a disposable cache that could not dispose of itself. That is
 * exactly how CTC-337 presented: `SQLITE_CORRUPT: database disk image is malformed`, every load.
 *
 * Retries exactly once. A second failure after a wipe is not corruption we can clear — it is a broken
 * environment (no quota, VFS refusing to register) — and looping would turn it into a hang.
 */
export async function openWithCorruptionRecovery(
  pool: RecoverablePool,
  dbPath: string,
  build: (db: unknown) => OpenedReplica,
  log: (message: string) => void = (m) => console.warn(m),
): Promise<OpenedReplica> {
  try {
    return build(new pool.OpfsSAHPoolDb(dbPath));
  } catch (err) {
    if (!isCorruptDatabaseError(err)) throw err;
    // Loud on purpose. Healing silently would hide that corruption HAPPENED, and the RATE of it is
    // the signal worth having — a replica that quietly re-seeds every session is a different bug.
    log(
      `[replica] OPFS database ${dbPath} is corrupt (${String(err)}); wiping and re-seeding — the replica is derived, so nothing is lost`,
    );
    await pool.wipeFiles();
    return build(new pool.OpfsSAHPoolDb(dbPath));
  }
}

/**
 * Open the OPFS SAHPool replica at `dbPath` (absolute, leading slash REQUIRED) and bring its schema up
 * to date via the shared migration bundle. Async — wasm init + SAHPool acquire are async, unlike the
 * node engine's synchronous open. `clearOnInit:false` is load-bearing for persistence across reloads.
 */
export async function openReplica(dbPath: string, directory: string): Promise<OpenedReplica> {
  const sqlite3 = await sqlite3InitModule();
  const poolUtil: SAHPoolUtil = await sqlite3.installOpfsSAHPoolVfs({
    clearOnInit: false, // PERSIST across reloads — true wipes the pool on every boot.
    initialCapacity: 6, // ≥ 2× the db count (one db + journals).
    directory, // one OPFS dir per app, so engines don't collide.
  });
  return openWithCorruptionRecovery(poolUtil as unknown as RecoverablePool, dbPath, (db) =>
    buildOpenedReplica(db as Parameters<typeof buildOpenedReplica>[0]),
  );
}
