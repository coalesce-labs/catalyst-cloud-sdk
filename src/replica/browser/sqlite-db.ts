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
  const db = new poolUtil.OpfsSAHPoolDb(dbPath);
  return buildOpenedReplica(db);
}
