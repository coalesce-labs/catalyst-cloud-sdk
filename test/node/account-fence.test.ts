import { afterEach, describe, expect, it, vi } from "vitest";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import {
  CatalystReplica,
  ReplicaAccountMismatchError,
  nodeSqliteEngine,
  type WebSocketLike,
  type WebSocketFactory,
} from "../../src/node";

// THE ACCOUNT FENCE (CTC-582). A replica stamps the account it holds into `sync_meta`, and refuses to
// open under a different one.
//
// ⛔ WHY THE HAZARD IS CREATED BY A FIX. Today `~/catalyst/catalyst-replica.db` is a fixed path, so a
// wrong `CATALYST_REPLICA_DB` is HARMLESS — one path means everyone points at the same correct file.
// CTL-1893 derives the path from the account, and from then on a stale launchd plist or a typo'd
// override points account B's writer at account A's file. The single-writer lock cannot object: it is
// claimed as `dbPath + '.writer.lock'`, so it is the SAME lock, and it reads as correct exclusion
// while tenants mix. The output is plausible data rather than an error — "a wrong answer that reads
// like a right one".
//
// ⚠️ These tests use REAL temp files and a REAL node:sqlite engine, not `:memory:`. The whole subject
// is what one process finds in a file ANOTHER process left behind, which an in-memory database cannot
// express — the second open would see a blank slate and every test would pass vacuously.

const BASE = "https://api.example.test";

// ⚠️ `createRequire`, NOT `import { DatabaseSync } from "node:sqlite"` — the same rule src/replica/
// engine.ts follows and says why: a LITERAL import specifier is resolved by the bundler, which cannot
// resolve a runtime builtin and fails the whole suite at collection ("Failed to load url sqlite").
const { DatabaseSync } = createRequire(import.meta.url)("node:sqlite") as typeof import("node:sqlite");

const dirs: string[] = [];
function tmpDbPath(): string {
  const dir = fs.mkdtempSync("catalyst-account-fence-");
  dirs.push(dir);
  return `${dir}/replica.db`;
}

class FakeWebSocket implements WebSocketLike {
  closed = false;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  send(): void {}
  close(): void {
    this.closed = true;
  }
  fireOpen(): void {
    this.onopen?.({});
  }
}
function recordingFactory(): { sockets: FakeWebSocket[]; factory: WebSocketFactory } {
  const sockets: FakeWebSocket[] = [];
  const factory: WebSocketFactory = () => {
    const ws = new FakeWebSocket();
    sockets.push(ws);
    return ws;
  };
  return { sockets, factory };
}
/** A snapshot carrying ONE issue row, so a re-seed's WIPE is observable as a row count. */
function seededSnapshotFetch(id = "i1"): typeof fetch {
  const body =
    JSON.stringify({
      accountId: "tenant-0",
      entity: "issues",
      op: "upsert",
      row: { id, identifier: `CTC-${id}`, title: "Hello", state: "Todo", updated_at: 100 },
    }) +
    "\n" +
    JSON.stringify({ accountId: "tenant-0", cursor: 7 }) +
    "\n";
  return (async () =>
    ({ ok: true, status: 200, text: async () => body }) as unknown as Response) as unknown as typeof fetch;
}

function emptySnapshotFetch(): typeof fetch {
  return (async () =>
    ({
      ok: true,
      status: 200,
      text: async () => `${JSON.stringify({ cursor: 0 })}\n`,
    }) as unknown as Response) as unknown as typeof fetch;
}

const replicas: CatalystReplica[] = [];
function newWriter(
  dbPath: string,
  account: string,
  accountSource?: "declared" | "default",
  fetchImpl: typeof fetch = seededSnapshotFetch(),
): { replica: CatalystReplica; sockets: FakeWebSocket[] } {
  const { sockets, factory } = recordingFactory();
  const replica = new CatalystReplica({
    baseUrl: BASE,
    account,
    accountSource,
    auth: { kind: "cookie" },
    dbPath,
    engine: nodeSqliteEngine,
    fetchImpl,
    wsFactory: factory,
  });
  replicas.push(replica);
  return { replica, sockets };
}
async function startToLive(replica: CatalystReplica, sockets: FakeWebSocket[]): Promise<void> {
  const started = replica.start();
  await vi.waitFor(() => expect(sockets.length).toBeGreaterThan(0));
  sockets[0]!.fireOpen();
  await started;
}

/** Read the file directly — the assertion must not go through the code under test. */
function readSource(dbPath: string): string | null {
  const db = new DatabaseSync(dbPath);
  try {
    const row = db
      .prepare("SELECT value FROM sync_meta WHERE key = 'account_source'")
      .get() as { value?: string } | undefined;
    return typeof row?.value === "string" ? row.value : null;
  } finally {
    db.close();
  }
}

/** Every issue id at rest — the wipe-vs-rename question answered by CONTENT. */
function issueIds(dbPath: string): string[] {
  const db = new DatabaseSync(dbPath);
  try {
    return (db.prepare("SELECT id FROM issues ORDER BY id").all() as { id: string }[]).map(
      (r) => r.id,
    );
  } finally {
    db.close();
  }
}

/** Row count for one entity table, to prove a re-seed actually wiped. */
function issueCount(dbPath: string): number {
  const db = new DatabaseSync(dbPath);
  try {
    return (db.prepare("SELECT count(*) AS n FROM issues").get() as { n: number }).n;
  } finally {
    db.close();
  }
}

function readStamp(dbPath: string): string | null {
  const db = new DatabaseSync(dbPath);
  try {
    const row = db
      .prepare("SELECT value FROM sync_meta WHERE key = 'account'")
      .get() as { value?: string } | undefined;
    return typeof row?.value === "string" ? row.value : null;
  } finally {
    db.close();
  }
}

/** Every table's name + row count, so "wrote nothing" can be asserted against the whole file. */
function fingerprint(dbPath: string): string {
  const db = new DatabaseSync(dbPath);
  try {
    const tables = db
      .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
      .all() as { name: string }[];
    return tables
      .map((t) => {
        const c = db.prepare(`SELECT count(*) AS n FROM "${t.name}"`).get() as { n: number };
        return `${t.name}:${c.n}`;
      })
      .join(",");
  } finally {
    db.close();
  }
}

afterEach(async () => {
  while (replicas.length) {
    try {
      await replicas.pop()!.close();
    } catch {
      /* ignore */
    }
  }
  while (dirs.length) {
    try {
      fs.rmSync(dirs.pop()!, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  }
  vi.useRealTimers();
});

describe("CTC-582 — a replica knows whose data it holds", () => {
  it("stamps the account on open", async () => {
    const dbPath = tmpDbPath();
    const { replica, sockets } = newWriter(dbPath, "tenant-3");
    await startToLive(replica, sockets);
    await replica.close();
    expect(readStamp(dbPath)).toBe("tenant-3");
  });

  it("reopens happily under the SAME account", async () => {
    const dbPath = tmpDbPath();
    const first = newWriter(dbPath, "tenant-3");
    await startToLive(first.replica, first.sockets);
    await first.replica.close();

    const second = newWriter(dbPath, "tenant-3");
    await expect(startToLive(second.replica, second.sockets)).resolves.toBeUndefined();
    expect(readStamp(dbPath)).toBe("tenant-3");
  });

  it("⛔ REFUSES to open a replica stamped for another account", async () => {
    const dbPath = tmpDbPath();
    const first = newWriter(dbPath, "tenant-3");
    await startToLive(first.replica, first.sockets);
    await first.replica.close();

    const intruder = newWriter(dbPath, "tenant-0");
    await expect(intruder.replica.start()).rejects.toThrow(ReplicaAccountMismatchError);
  });

  it("⛔ names BOTH accounts in the error — a fence that won't say what it caught can't be fixed", async () => {
    const dbPath = tmpDbPath();
    const first = newWriter(dbPath, "tenant-3");
    await startToLive(first.replica, first.sockets);
    await first.replica.close();

    const intruder = newWriter(dbPath, "tenant-0");
    // The operator reading this line has to be able to tell which of the two is the typo. A message
    // that says only "wrong account" sends them to the wrong config file.
    await expect(intruder.replica.start()).rejects.toThrow(/tenant-3/);
    await expect(newWriter(dbPath, "tenant-0").replica.start()).rejects.toThrow(/tenant-0/);
  });

  it("⛔ WRITES NOTHING when it refuses — not even the migrations", async () => {
    // THE CLAIM THAT MATTERS MOST, and the reason the fence sits BEFORE `applyMigrations`. A fence
    // placed after them would already have altered another tenant's file before deciding it had no
    // right to. Fingerprinting every table's row count is the assertion that can actually catch that:
    // checking only that rows were not DELETED would miss a migration having run.
    const dbPath = tmpDbPath();
    const first = newWriter(dbPath, "tenant-3");
    await startToLive(first.replica, first.sockets);
    await first.replica.close();

    const before = fingerprint(dbPath);
    const bytesBefore = fs.statSync(dbPath).size;

    const intruder = newWriter(dbPath, "tenant-0");
    await expect(intruder.replica.start()).rejects.toThrow(ReplicaAccountMismatchError);

    expect(fingerprint(dbPath)).toBe(before);
    expect(fs.statSync(dbPath).size).toBe(bytesBefore);
    // ...and above all it did NOT re-seed itself over the top. Repairing a misconfiguration by
    // destroying the other tenant's replica would be a worse failure than the one being prevented.
    expect(readStamp(dbPath)).toBe("tenant-3");
  });

  it("⛔ releases the writer lock when it refuses", async () => {
    // A refusal that keeps the lock leaves a sidecar sitting on ANOTHER tenant's replica, so their
    // legitimate writer is locked out by our misconfiguration — turning our config error into their
    // outage. The failure must cost the innocent file nothing at all.
    const dbPath = tmpDbPath();
    const first = newWriter(dbPath, "tenant-3");
    await startToLive(first.replica, first.sockets);
    await first.replica.close();

    const intruder = newWriter(dbPath, "tenant-0");
    await expect(intruder.replica.start()).rejects.toThrow(ReplicaAccountMismatchError);
    expect(fs.existsSync(`${dbPath}.writer.lock`)).toBe(false);

    // The rightful owner can still open it afterwards — the real proof, since an orphaned lock is
    // only a problem because of what it blocks.
    const owner = newWriter(dbPath, "tenant-3");
    await expect(startToLive(owner.replica, owner.sockets)).resolves.toBeUndefined();
  });

  it("⛔ ADOPTS an unstamped replica instead of rejecting it", async () => {
    // Every replica seeded before this shipped has no stamp. Treating absent as mismatched would
    // refuse to open on every existing host the moment they upgrade — bricking the fleet to guard
    // against a hazard that does not exist yet. Absent is not mismatched.
    const dbPath = tmpDbPath();
    const first = newWriter(dbPath, "tenant-3");
    await startToLive(first.replica, first.sockets);
    await first.replica.close();

    // Reproduce a pre-CTC-582 file exactly: same data, stamp removed.
    const db = new DatabaseSync(dbPath);
    db.prepare("DELETE FROM sync_meta WHERE key = 'account'").run();
    db.close();
    expect(readStamp(dbPath)).toBeNull();
    const before = fingerprint(dbPath);

    const upgraded = newWriter(dbPath, "tenant-3");
    await expect(startToLive(upgraded.replica, upgraded.sockets)).resolves.toBeUndefined();
    expect(readStamp(dbPath)).toBe("tenant-3");
    // Adoption stamps; it does not wipe. (`sync_meta` gains the account row, so compare the rest.)
    const strip = (f: string) => f.replace(/sync_meta:\d+/, "sync_meta:*");
    expect(strip(fingerprint(dbPath))).toBe(strip(before));
  });

  it("⛔ an unstamped replica is adopted by whoever opens it — the documented residual, pinned", async () => {
    // Deliberately asserting a LIMITATION, so it is a known property rather than a latent surprise.
    // The fence cannot audit the past: a pre-existing file at a wrong path carries no stamp, so the
    // wrong account adopts it silently. CTL's host-side path guard (CTL-1893) is what covers this.
    // If a future change makes this case refuse, this test SHOULD fail and be deleted on purpose.
    const dbPath = tmpDbPath();
    const first = newWriter(dbPath, "tenant-3");
    await startToLive(first.replica, first.sockets);
    await first.replica.close();
    const db = new DatabaseSync(dbPath);
    db.prepare("DELETE FROM sync_meta WHERE key = 'account'").run();
    db.close();

    const stranger = newWriter(dbPath, "tenant-0");
    await expect(startToLive(stranger.replica, stranger.sockets)).resolves.toBeUndefined();
    expect(readStamp(dbPath)).toBe("tenant-0");
  });
});

describe("CTC-582 review (CTL) — a stamp records HOW SURE it is, and only a declared one may refuse", () => {
  // ⛔ THE TRAP THIS CLOSES, MEASURED ON THE LIVE FLEET. The host's `cloud-sync.mjs` resolves
  // `process.env.CATALYST_CLOUD_ACCOUNT || "tenant-0"` — env-only, never consulting config — and that
  // variable is unset on every host. An unconditional stamp would therefore have written `tenant-0`
  // across 100% of the fleet as though a human had asked for it, and the fence would later refuse a
  // host from its OWN replica on the strength of an assertion nobody ever made.
  //
  // ⭐ Same defect class as the ticket itself, turned on itself: something meaning "nobody said"
  // recorded as if it meant "somebody said this".

  it("records provenance alongside the account", async () => {
    const dbPath = tmpDbPath();
    const w = newWriter(dbPath, "tenant-3", "declared");
    await startToLive(w.replica, w.sockets);
    await w.replica.close();
    expect(readStamp(dbPath)).toBe("tenant-3");
    expect(readSource(dbPath)).toBe("declared");
  });

  it("⛔ OMITTING accountSource means `default` — the safe direction, never `declared`", async () => {
    // If omission meant "declared", every existing caller's silent fallback would become a permanent
    // assertion the moment it upgraded. That IS the fleet-wide trap. Asserting the default explicitly
    // because it is a safety property, not a convenience.
    const dbPath = tmpDbPath();
    const w = newWriter(dbPath, "tenant-0"); // no accountSource — exactly what cloud-sync.mjs does
    await startToLive(w.replica, w.sockets);
    await w.replica.close();
    expect(readSource(dbPath)).toBe("default");
  });

  it("⭐ THE FLEET SCENARIO: a defaulted stamp does NOT brick a host that later declares its account", async () => {
    // The whole point. Host boots with CATALYST_CLOUD_ACCOUNT unset → stamps `tenant-0` as `default`.
    // It is later legitimately configured as `tenant-3`. It must NOT be refused from its own file.
    const dbPath = tmpDbPath();
    const defaulted = newWriter(dbPath, "tenant-0"); // defaulted
    await startToLive(defaulted.replica, defaulted.sockets);
    await defaulted.replica.close();
    expect(readSource(dbPath)).toBe("default");
    expect(issueCount(dbPath)).toBe(1); // tenant-0's row is really there

    const declared = newWriter(dbPath, "tenant-3", "declared");
    await expect(startToLive(declared.replica, declared.sockets)).resolves.toBeUndefined();
    expect(readStamp(dbPath)).toBe("tenant-3");
    expect(readSource(dbPath)).toBe("declared");
  });

  it("⛔ and that transition WIPES rather than renames — a stamp must never mislabel real rows", async () => {
    // CTL's review proposed "a declared value may overwrite a defaulted one exactly once", which is
    // right about the STAMP. This is the part that makes it safe about the DATA: the rows in the file
    // were fetched for tenant-0, so stamping tenant-3 over them would leave account A's data under a
    // stamp saying B — a mislabelled replica every later check believes.
    //
    // ⚠️ PROVED BY CONTENT, NOT BY A TIMING WINDOW. An earlier version of this test asserted zero rows
    // between open and the transport's re-seed; that window does not exist, because clearing the
    // cursor makes `start()` cold-seed immediately and the fixture put an identical row straight back
    // — the wipe was real and the assertion could not see it. Giving the second account a DIFFERENT
    // row id makes the question answerable at rest: a wipe leaves only the new row, a rename leaves
    // both.
    const dbPath = tmpDbPath();
    const defaulted = newWriter(dbPath, "tenant-0", undefined, seededSnapshotFetch("old"));
    await startToLive(defaulted.replica, defaulted.sockets);
    await defaulted.replica.close();
    expect(issueIds(dbPath)).toEqual(["old"]);

    const declared = newWriter(dbPath, "tenant-3", "declared", seededSnapshotFetch("new"));
    await startToLive(declared.replica, declared.sockets);
    await declared.replica.close();

    expect(issueIds(dbPath)).toEqual(["new"]); // tenant-0's row is GONE, not relabelled
    expect(readStamp(dbPath)).toBe("tenant-3");
    expect(readSource(dbPath)).toBe("declared");
  });

  it("⛔ the old tenant's rows are gone even when the RE-SEED FAILS", async () => {
    // ⚠️ THIS TEST EXISTS BECAUSE A NEGATIVE CONTROL DIDN'T FIRE. Commenting out `truncateReplica` in
    // the reseed path left all 16 tests green: clearing the cursor forces a cold seed, and the seed
    // truncates on its own, so the earlier guard absorbed the mutation and the wipe looked redundant.
    //
    // ⭐ It is not redundant — it is defence in depth, and the layer it defends is only reachable when
    // the re-seed FAILS. Without it, a failed seed leaves the previous account's rows READABLE under a
    // stamp naming the new one: `queryIssues()` would serve tenant-0's data to tenant-3. The wipe makes
    // "wrong-account rows are unreadable" unconditional instead of contingent on a later step that may
    // not happen. (The browser fence wraps its wipe for exactly this reason — worker-core.ts.)
    const dbPath = tmpDbPath();
    const defaulted = newWriter(dbPath, "tenant-0", undefined, seededSnapshotFetch("old"));
    await startToLive(defaulted.replica, defaulted.sockets);
    await defaulted.replica.close();
    expect(issueIds(dbPath)).toEqual(["old"]);

    // A snapshot that cannot land. The open still succeeds; the seed is what breaks.
    const failing = (async () =>
      ({ ok: false, status: 503, text: async () => "upstream down" }) as unknown as Response) as unknown as typeof fetch;
    const declared = newWriter(dbPath, "tenant-3", "declared", failing);
    try {
      await startToLive(declared.replica, declared.sockets);
    } catch {
      /* the seed failing is the POINT — the fence's work must already be durable */
    }
    await declared.replica.close();

    expect(issueIds(dbPath)).toEqual([]); // ⛔ not ["old"] — tenant-0's rows must not survive
  });

  it("⛔ a DECLARED stamp still refuses — provenance weakens nothing that was actually asserted", async () => {
    const dbPath = tmpDbPath();
    const owner = newWriter(dbPath, "tenant-3", "declared");
    await startToLive(owner.replica, owner.sockets);
    await owner.replica.close();

    const intruder = newWriter(dbPath, "tenant-0", "declared");
    await expect(intruder.replica.start()).rejects.toThrow(ReplicaAccountMismatchError);
    expect(issueCount(dbPath)).toBe(1); // and it did NOT wipe — those rows may be a real tenant's
  });

  it("⛔ TWO DISAGREEING DEFAULTS refuse — nothing licenses deleting either", async () => {
    // Reachable when two builds ship different built-in fallbacks. Neither side asserted anything, so
    // there is no basis to prefer one — and therefore certainly none to wipe one.
    const dbPath = tmpDbPath();
    const a = newWriter(dbPath, "tenant-0");
    await startToLive(a.replica, a.sockets);
    await a.replica.close();

    const b = newWriter(dbPath, "tenant-9"); // also defaulted, different fallback
    await expect(b.replica.start()).rejects.toThrow(ReplicaAccountMismatchError);
    expect(issueCount(dbPath)).toBe(1);
    expect(readStamp(dbPath)).toBe("tenant-0");
  });

  it("upgrades default → declared for the SAME account, and never downgrades", async () => {
    const dbPath = tmpDbPath();
    const first = newWriter(dbPath, "tenant-3"); // defaulted
    await startToLive(first.replica, first.sockets);
    await first.replica.close();
    expect(readSource(dbPath)).toBe("default");

    const second = newWriter(dbPath, "tenant-3", "declared"); // now declared — confirms it
    await startToLive(second.replica, second.sockets);
    await second.replica.close();
    expect(readSource(dbPath)).toBe("declared");

    // ⚠️ A later caller that FORGETS to declare must not un-assert it: a declaration already made is
    // not unmade by someone else's omission, or the fence would silently weaken over time.
    const third = newWriter(dbPath, "tenant-3");
    await startToLive(third.replica, third.sockets);
    await third.replica.close();
    expect(readSource(dbPath)).toBe("declared");
  });

  it("names the provenance in the error, so a caller can tell the two refusals apart", async () => {
    const dbPath = tmpDbPath();
    const a = newWriter(dbPath, "tenant-0");
    await startToLive(a.replica, a.sockets);
    await a.replica.close();
    const b = newWriter(dbPath, "tenant-9");
    // "neither was declared" and "someone declared this is another tenant's" need different remedies:
    // the first is "go say which", the second is "you are pointed at the wrong file".
    await expect(b.replica.start()).rejects.toThrow(/Neither account was declared/);
  });
});
