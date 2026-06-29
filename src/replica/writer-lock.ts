// @catalyst-cloud/sdk/node — the single-writer guard for a CatalystReplica writer (CTC-113).
//
// The managed replica is single-writer/many-reader (ADR-0008): ONE process owns the SQLite file and
// applies the change feed; everyone else opens it READ-ONLY (`CatalystReplica.openReadOnly`). Two
// concurrent WRITERS on the same file would race the cursor + truncate-and-reseed and silently
// diverge. This guard makes that mistake LOUD: when a writer `start()`s it best-effort claims sole
// ownership of `dbPath` via a sidecar lock file `dbPath + '.writer.lock'` carrying `{pid, owner,
// heartbeat}`. A second live writer on the same path throws a clear error instead of corrupting the
// replica.
//
// LIMITS — this is deliberately ADVISORY, not a hard OS lock:
//   • It is cooperative: it only stops OTHER CatalystReplica writers. A raw `new Database(path)` from
//     unrelated code is not blocked (use OS file locking / WAL for that).
//   • Liveness has two signals. The HEARTBEAT (a timestamp rewritten on an interval) is the portable,
//     cross-host signal — a lock older than `staleMs` with no heartbeat is treated as abandoned and
//     reclaimed. The PID probe (`process.kill(pid,0)`) only means anything SAME-HOST; cross-host or
//     when it can't probe, it conservatively assumes the holder is alive and relies on the heartbeat.
//   • A hard `kill -9` can leave a stale lock until `staleMs` elapses; the next writer reclaims it.
//   • `:memory:` (and `file::memory:`) replicas are per-connection, never shared, so the guard is a
//     no-op for them.

import * as fs from "node:fs";
import type { LogLevel } from "../live-sync-client.js";

/** Config for the single-writer guard. Passed as `CatalystReplicaOptions.writerGuard`. */
export interface WriterGuardOptions {
  /** Disable the guard entirely (claim nothing). Default false — the guard is ON for file replicas. */
  disabled?: boolean;
  /** Steal an existing LIVE lock instead of throwing (the configurable override). Default false. */
  override?: boolean;
  /** A lock with no heartbeat newer than this many ms is treated as abandoned + reclaimed. Default 15000. */
  staleMs?: number;
  /** How often the owning writer rewrites its heartbeat. Default max(1000, staleMs/3). */
  heartbeatMs?: number;
}

/** A claimed writer lock. `release()` stops the heartbeat and removes the file if still ours. Idempotent. */
export interface WriterLockHandle {
  /** The sidecar lock file path (`dbPath + '.writer.lock'`). */
  readonly path: string;
  release(): void;
}

/** The on-disk lock record. */
interface LockRecord {
  pid: number;
  /** A per-claim token so two instances in the SAME process don't unlink each other's lock on release. */
  owner: string;
  /** epoch-ms of the last heartbeat (the freshness signal). */
  heartbeat: number;
}

type Logger = (level: LogLevel, msg: string, extra?: unknown) => void;

const DEFAULT_STALE_MS = 15_000;

function readLock(lockPath: string): LockRecord | null {
  try {
    const rec = JSON.parse(fs.readFileSync(lockPath, "utf8")) as LockRecord;
    if (rec && typeof rec.pid === "number" && typeof rec.heartbeat === "number") return rec;
    return null;
  } catch {
    return null; // absent, partial write, or garbage → treat as no lock
  }
}

/** SAME-HOST liveness probe. Returns true when it cannot tell (assume the holder is alive — the
 *  heartbeat staleness check is the portable signal that ultimately reclaims a dead lock). */
function pidAlive(pid: number): boolean {
  const proc = (globalThis as { process?: { kill?: (pid: number, sig: number) => boolean } }).process;
  if (!proc?.kill || !pid) return true;
  try {
    proc.kill(pid, 0); // signal 0 = existence check, never delivered
    return true;
  } catch (err) {
    if ((err as { code?: string }).code === "ESRCH") return false; // no such process
    return true; // EPERM: exists but owned by another user → alive
  }
}

/**
 * Best-effort claim sole ownership of `dbPath` for a writer. Returns a {@link WriterLockHandle} (or
 * `null` when the guard is disabled or `dbPath` is in-memory). Throws a clear error when another LIVE
 * writer already holds the lock and `override` is not set.
 */
export function claimWriterLock(
  dbPath: string,
  opts: WriterGuardOptions,
  log?: Logger,
): WriterLockHandle | null {
  if (opts.disabled) return null;
  if (!dbPath || dbPath === ":memory:" || dbPath.startsWith("file::memory:")) return null;

  const lockPath = `${dbPath}.writer.lock`;
  const staleMs = opts.staleMs ?? DEFAULT_STALE_MS;
  const heartbeatMs = opts.heartbeatMs ?? Math.max(1000, Math.floor(staleMs / 3));
  const pid = (globalThis as { process?: { pid?: number } }).process?.pid ?? 0;
  const owner = `${pid}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  const serialize = (): string =>
    JSON.stringify({ pid, owner, heartbeat: Date.now() } satisfies LockRecord);
  const writeFresh = (): void => fs.writeFileSync(lockPath, serialize());

  // Atomic exclusive create: wins the claim outright if no file exists.
  let created = false;
  try {
    const fd = fs.openSync(lockPath, "wx");
    fs.writeSync(fd, serialize());
    fs.closeSync(fd);
    created = true;
  } catch (err) {
    if ((err as { code?: string }).code !== "EEXIST") throw err;
  }

  if (!created) {
    const existing = readLock(lockPath);
    const held =
      existing != null && Date.now() - existing.heartbeat < staleMs && pidAlive(existing.pid);
    if (held && !opts.override) {
      throw new Error(
        `CatalystReplica: another writer owns this replica at ${dbPath} ` +
          `(pid=${existing!.pid}, last heartbeat ${Date.now() - existing!.heartbeat}ms ago). ` +
          `Only ONE writer may hold a replica file (ADR-0008 single-writer/many-reader); open it ` +
          `read-only with CatalystReplica.openReadOnly() instead. Pass writerGuard:{override:true} ` +
          `to take it over, or writerGuard:{disabled:true} to skip the check. NOTE: this guard is ` +
          `advisory (a sidecar ${lockPath}), not a hard OS lock.`,
      );
    }
    log?.(
      "warn",
      held
        ? `writer-lock: overriding a live writer at ${lockPath} (writerGuard.override)`
        : `writer-lock: reclaiming a stale/abandoned lock at ${lockPath}`,
    );
    writeFresh();
  }

  // Keep the lock fresh so peers see it's alive; unref so it never holds the event loop open.
  const timer = setInterval(() => {
    try {
      writeFresh();
    } catch (err) {
      log?.("warn", "writer-lock heartbeat failed", err);
    }
  }, heartbeatMs);
  (timer as unknown as { unref?: () => void }).unref?.();

  let released = false;
  return {
    path: lockPath,
    release(): void {
      if (released) return;
      released = true;
      clearInterval(timer);
      const cur = readLock(lockPath);
      if (cur && cur.owner === owner) {
        try {
          fs.unlinkSync(lockPath);
        } catch {
          // already gone — fine
        }
      }
    },
  };
}
