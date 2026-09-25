# @catalyst-cloud/sdk

**Keep a live local copy of your Linear and GitHub project data — pushed to you in real time, without polling rate limits or webhook tunnels.**

When you run coding agents at scale — dozens or more working in parallel — they all need the same live view of your Linear and GitHub state to coordinate, and you quickly hit the rate limits of the very systems your team depends on. catalyst-cloud is a service that mirrors your Linear + GitHub state **once** and pushes every change out to your fleet; this SDK is how your code subscribes to that stream and keeps a local copy current.

## Why

- **Real-time updates, no tunnels.** catalyst-cloud is the single webhook subscriber for your Linear and GitHub. Changes are **pushed** to you over an outbound connection — so you never stand up a public endpoint, smee/ngrok tunnel, or webhook gateway just to hear about an update locally.
- **One subscriber, not N.** Your whole fleet reads from the shared mirror instead of each agent polling Linear and GitHub directly — so a large fleet doesn't multiply load against those rate limits.
- **A local copy, not just a feed.** You don't get a firehose of events to babysit — you get your data, kept current, that you can query locally.

## Coverage

- **Linear** — issues, projects, cycles, initiatives, comments, labels, history, and relations. **Mature.**
- **GitHub** — pull requests, checks, commit statuses, and reviews. **Maturing.**
- **Knowledge base** — thoughts / memories. **On the roadmap.**

## Requirements

A catalyst-cloud account and an auth token. (In the browser, a same-origin session cookie is used instead — no token in the page.)

```sh
npm install @catalyst-cloud/sdk
```

## Today, and where this is going

- **Today** — the SDK is the live-sync client. It keeps your local store current: it manages the connection, replays anything you missed while disconnected, and recovers automatically. You provide the storage (any SQLite, OPFS in the browser, or in-memory) and apply each change.
- **Coming** — a fully managed local replica: a strongly-typed SQLite database you read **directly via [Drizzle ORM](https://orm.drizzle.team)**, with the syncing handled for you. The typed read layer already exists; we're folding it into the SDK so the database is something you read, not something you assemble.

## Usage

### Backend / agent host

Authenticate with a token and land every change into a local SQLite replica. On the first run (or after a long disconnect) the SDK asks you to re-seed from a snapshot; after that you just apply each pushed change.

```ts
import { Database } from "bun:sqlite"; // Node: better-sqlite3 with the same calls
import { LiveSyncClient } from "@catalyst-cloud/sdk";

const db = new Database("replica.sqlite");
db.run("CREATE TABLE IF NOT EXISTS issues (id TEXT PRIMARY KEY, row TEXT, seq INTEGER)");
db.run("CREATE TABLE IF NOT EXISTS sync_state (k TEXT PRIMARY KEY, cursor INTEGER)");

const client = new LiveSyncClient({
  baseUrl: process.env.CATALYST_CLOUD_BASE_URL!,
  accountId: "tenant-0",
  auth: { kind: "token", token: process.env.CATALYST_CLOUD_TOKEN! },

  // Full re-seed: fetch the current snapshot and rewrite the replica; resolve to the fresh cursor.
  reseed: async () => {
    const snap = (await (
      await fetch(`${process.env.CATALYST_CLOUD_BASE_URL}/snapshot?account=tenant-0`, {
        headers: { authorization: `Bearer ${process.env.CATALYST_CLOUD_TOKEN}` },
      })
    ).json()) as { cursor: number; issues: { id: string }[] };
    const tx = db.transaction(() => {
      db.run("DELETE FROM issues");
      const ins = db.prepare("INSERT INTO issues (id, row, seq) VALUES (?, ?, ?)");
      for (const row of snap.issues) ins.run(row.id, JSON.stringify(row), snap.cursor);
      db.run(
        "INSERT INTO sync_state (k, cursor) VALUES ('replica', ?) ON CONFLICT(k) DO UPDATE SET cursor = excluded.cursor",
        [snap.cursor],
      );
    });
    tx();
    return snap.cursor;
  },

  getCursor: () =>
    (db.query("SELECT cursor FROM sync_state WHERE k = 'replica'").get() as
      | { cursor: number }
      | undefined)?.cursor ?? null,

  // Apply each pushed change to your local copy.
  onChange: (frame) => {
    if (frame.op === "delete") db.run("DELETE FROM issues WHERE id = ?", [frame.entityId]);
    else
      db.run(
        "INSERT INTO issues (id, row, seq) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET row = excluded.row, seq = excluded.seq",
        [frame.entityId, JSON.stringify(frame.row), frame.seq],
      );
    db.run("UPDATE sync_state SET cursor = ? WHERE k = 'replica'", [frame.seq]);
  },
});

await client.start(); // resolves only when stop() is called — keeps the process alive between changes
```

### Browser

In the browser there's no token: the same-origin session cookie authenticates the connection, and the SDK never puts anything secret in the URL. Storage is yours (here, a `Map`; in a real app, OPFS / IndexedDB).

```ts
import { LiveSyncClient } from "@catalyst-cloud/sdk";

const issues = new Map<string, unknown>();
let cursor: number | null = null;

const client = new LiveSyncClient({
  baseUrl: "https://app.example.com/api/v1", // same origin as the page
  accountId: "tenant-0",
  auth: { kind: "cookie" },
  reseed: async () => {
    const snap = (await (await fetch("/api/v1/snapshot?account=tenant-0")).json()) as {
      cursor: number;
      issues: { id: string }[];
    };
    issues.clear();
    for (const row of snap.issues) issues.set(row.id, row);
    cursor = snap.cursor;
    return snap.cursor;
  },
  getCursor: () => cursor,
  onChange: (frame) => {
    if (frame.op === "delete") issues.delete(frame.entityId);
    else issues.set(frame.entityId, frame.row);
    cursor = frame.seq;
  },
  onStatus: (s) => setConnectionBadge(s), // "live" / "reconnecting" / …
});

void client.start(); // never await in the browser
// client.stop() on teardown
```

Requires a platform `WebSocket` (browser, Bun, or Node ≥22). On older Node, inject a `wsFactory` that wraps the [`ws`](https://www.npmjs.com/package/ws) package.

### Browser — managed OPFS replica (`/browser`)

`BrowserReplica` is the browser twin of `/node`'s `CatalystReplica`: an OPFS-persisted SQLite replica in a dedicated Web Worker, seeded from `/snapshot` in bounded streamed batches, kept live off the same change feed, and read through the same `@catalyst-cloud/read-model` views — no hand-written worker, apply, or seeding code in your app.

```ts
import {
  BrowserReplica,
  isBrowserReplicaSupported,
  type ReplicaStatus,
} from "@catalyst-cloud/sdk/browser";

let status: ReplicaStatus = "loading";

if (isBrowserReplicaSupported()) {
  const replica = new BrowserReplica(
    {
      onChanged: () => void refreshList(), // after the seed and after every applied delta drain
      onStatus: (s) => {
        status = s; // "loading" | "live" | "reconnecting" | "error" | "secondary" | …
        setBadge(s);
      },
    },
    {
      baseUrl: "/api/v1", // same-origin, cookie-authed; accountId optional
      // REQUIRED. The OPFS database is shared by every replica on the origin and a warm cursor skips
      // the snapshot, so without a fence a change of signed-in user serves the PREVIOUS user's rows.
      // Pass a stable per-user id from your session — e.g. the WorkOS `user.id`. It never leaves the
      // browser; it is only compared against the value stamped in the local DB, and a mismatch wipes
      // the replica and forces a re-seed.
      identity: session.user.id,
    },
  );
  // AWAIT it. start() suspends on the Web Lock before the worker exists, so a query issued against a
  // not-yet-started replica rejects with "replica client closed".
  await replica.start(); // resolves once seeded — or immediately, with status "secondary"

  // Another tab already owns the origin's replica. That is a normal outcome, not an error: this tab
  // should read via its usual fetch path and leave the local DB alone.
  if (status !== "secondary") {
    const rows = await replica.queryIssues(); // read-model IssueView[], served locally
  }
  // replica.close() on teardown — cooperatively releases the OPFS handles, then terminates the worker
}
```

`status` above is whatever your `onStatus` handler last recorded. `start()` REJECTS if the boot fails (bad origin, missing worker chunk, OPFS unavailable) — it does not resolve into a broken state, so a `try`/`catch` around it is where you surface the error to the user.

Built in, because browsers need them:

- **Single-owner election** (Web Locks): OPFS SAHPool is single-connection-per-origin, so exactly one tab boots the replica; every other tab gets status `"secondary"` (a clean state, not an error) and should read via its normal fetch path.
- **Backpressure**: live deltas are coalesced into batched, single-flight applies with a bounded buffer; a backlog too deep to replay escalates to a fresh snapshot instead of growing.

**Peer dependency**: install [`@sqlite.org/sqlite-wasm`](https://www.npmjs.com/package/@sqlite.org/sqlite-wasm) yourself — the SDK never bundles the wasm.

**Bundlers**: the worker is created with `new Worker(new URL("./db.worker.js", import.meta.url), { type: "module" })`, which Vite, webpack 5, and Rollup detect statically and split into its own chunk (nothing wasm-related touches your main bundle; `/node` consumers never see it at all). Notes:

- **Vite**: works out of the box in `build`. In dev, if pre-bundling interferes with the worker URL, add `optimizeDeps: { exclude: ["@catalyst-cloud/sdk"] }`.
- **webpack 5 / Rollup**: the `new Worker(new URL(...))` syntax is supported natively (Rollup needs worker support in your config, e.g. `@surma/rollup-plugin-off-main-thread` or equivalent).
- **Exotic bundlers**: pass `createWorker` in `BrowserReplicaOptions` and construct the worker however your toolchain requires. The worker module is published at the dedicated subpath **`@catalyst-cloud/sdk/browser/db-worker`** — that specifier is the supported entry point:

  ```ts
  // Vite
  import DbWorker from "@catalyst-cloud/sdk/browser/db-worker?worker";
  createWorker: () => new DbWorker();

  // webpack 5 / anything that resolves a bare specifier to a URL
  createWorker: () =>
    new Worker(
      new URL("@catalyst-cloud/sdk/browser/db-worker", import.meta.url),
      { type: "module" },
    );
  ```

  The worker is a **side-effect module** (it registers a message handler and exports nothing), so it is listed in the package's `sideEffects` array — do not configure your bundler to tree-shake it, or it will load and register nothing and every replica call will hang.

### Tenant client — typed reads and writes over HTTP

`createTenantClient` is one implementation of every tenant read and write, so a CLI, a skill script, an MCP tool and your own code share it instead of each carrying a bearer, a base URL and a response parser. Every call resolves to a discriminated union on `outcome` — nothing throws for a server answer — and every `agent.*` write's path is resolved from the tenant's own contract (`GET /api/v1/agent/contract`, read once and cached under the document's own ETag policy), never a literal.

```ts
import { createTenantClient, stageIdForSlot, teamForTicket } from "@catalyst-cloud/sdk";

const client = createTenantClient({
  key: process.env.CATALYST_CLOUD_TOKEN!,        // an organization-tier key for agent.* writes
  baseUrl: "https://staging.catalystcloud.dev",
});

// A keyset-paged read: follow `nextCursor` (typed, read off X-Mirror-Next-Cursor) until it is null.
let after = undefined;
do {
  const page = await client.issues.list({ teamKey: "ENG", state: "active", limit: 100, after });
  if (page.outcome !== "ok") throw new Error(`${page.outcome}: ${"reason" in page ? page.reason : ""}`);
  for (const row of page.rows) console.log(row.identifier, row.title);
  after = page.nextCursor ?? undefined;
} while (after);

// A write as the app actor. The state id comes from the contract by SLOT, never by a display name.
const contract = await client.contract();
if (contract.outcome !== "ok") throw new Error(`contract: ${contract.outcome}`);
const team = teamForTicket(contract.doc, "ENG-12");
const stage = team.outcome === "ok" ? stageIdForSlot(team.team, "research") : team;
if (stage.outcome !== "ok") throw new Error(stage.outcome);   // "slot-unmapped" / "slot-stale" / "team-unknown"

const issue = await client.issues.get("ENG-12");
if (issue.outcome !== "ok") throw new Error(issue.outcome);
const moved = await client.agent.issueState({ issueId: issue.issue.id, stateId: stage.stateId });
switch (moved.outcome) {
  case "succeeded":     break;                                  // moved.attempts
  case "rate-limited":  /* moved.retryAfterSeconds may be null */ break;
  case "forbidden":     /* moved.required names a missing scope */ break;
  case "route-unknown": /* this tenant's contract lists no issue-state route */ break;
  default:              /* rejected | exhausted | failed | unauthorized | network | shape | http | stale | missing */ break;
}
```

The contract cache is in memory per client by default; pass `contractCache: { get, set }` to keep it on disk (the bundle does). `fetch`, `now` and `timeoutMs` are injectable so a test never opens a socket.

Personal GitHub and Linear grants use a member's own key or device-login credential. A tenant account key cannot identify the member. `start` returns a short lived URL for the member to open in a browser; the SDK does not complete provider consent or return provider tokens. Check `status` afterward, and treat `unavailable` as an unknown provider check rather than an absent grant.

```ts
const member = createTenantClient({
  key: process.env.CATALYST_CLOUD_TOKEN!, // personal key or device-login JWT
  baseUrl: "https://staging.catalystcloud.dev",
});
const started = await member.personalConnections.start("github");
if (started.outcome === "ok") console.log(started.authorizationUrl);
const grant = await member.personalConnections.status("github");
if (grant.outcome === "connected") console.log(grant.githubLogin);
```

Personal Linear consent normally binds the member to the provider viewer. For an unmatched member, `linearIdentity.get()` returns the current identity and any available roster choices. Choose an explicit ID with `linearIdentity.set(id)` and read it back afterward. These calls require a personal credential, accept no membership selector, and cannot replace an automatically resolved identity. Conflicts distinguish `already_resolved`, `already_claimed`, and `identity_changed`; reload before retrying.

```ts
const identity = await member.linearIdentity.get();
if (identity.outcome === "ok") console.log(identity.identity, identity.options);
// After the member chooses an ID from the offered roster:
const saved = await member.linearIdentity.set(selectedLinearUserId);
if (saved.outcome === "ok") console.log(await member.linearIdentity.get());
```

### Durable event cache for node and Bun

`@catalyst-cloud/sdk/events` mirrors the tenant's exact durable Catalyst event backbone into an append-only local cache. It uses bounded HTTP replay with an idle backoff; local consumers tail files and never poll GitHub, Linear, or cloud tables. Start and stop it with the process that currently needs the tenant instead of installing another permanent daemon.

```ts
import { CatalystEventSync, tailCachedEvents } from "@catalyst-cloud/sdk/events";

const sync = new CatalystEventSync({
  baseUrl: "https://staging.catalystcloud.dev",
  auth: { kind: "token", token: process.env.CATALYST_CLOUD_TOKEN! },
  tenantId: "tenant-1",
});
const running = sync.start();
for await (const event of tailCachedEvents({ tenantId: "tenant-1", signal })) {
  console.log(event.type, event.sequence);
}
await sync.stop();
await running;
```

The default path is `$XDG_STATE_HOME/catalyst/events/<tenant>/backbone/`, falling back to `~/.local/state/catalyst/events/<tenant>/backbone/`. Daily `YYYY-MM-DD[-NNN].jsonl` segments contain the cloud's `CatalystEvent` envelope unchanged. `cursor.json` is owned by the one sync writer; readers keep their own offsets elsewhere and do not write into this directory. The stable replay identity is `(tenantId, sequence, eventId)`.

The writer fsyncs appended records before atomically advancing its cursor. On restart it truncates an incomplete final line and derives the resume cursor from the last complete record, making a crash between append and checkpoint safe. Server archive gaps and reads behind the local retention floor raise `EventHistoryGapError` with the head, suggested resume point, and reason; the SDK does not silently skip history. New caches bootstrap at the current backbone head. Closed daily segments are retained for seven days or until the cache reaches 256 MiB, whichever happens first. The active segment rotates before the same byte bound, so it cannot grow without limit; `retainDays` and `maxCacheBytes` configure these bounds.

This API only reads the durable backbone. It does not publish events or treat raw provider events, the legacy coordination feed, or analytical samples as execution or billing authority. Use the existing authorized cloud operations to report outcomes and changes.

## API

| Export | What it is |
| --- | --- |
| `LiveSyncClient` | The client — `new LiveSyncClient(opts)`, then `start()` / `stop()`. |
| `LiveSyncClientOptions` | The options shape (auth, reseed, getCursor, onChange, onStatus, backoff, wsFactory, log). |
| `AuthStrategy` | `{ kind: "token"; token }` (backend) or `{ kind: "cookie" }` (browser). |
| `LiveSyncStatus` | `"connecting"` · `"live"` · `"reconnecting"` · `"resyncing"` · `"error"` · `"stopped"`. |
| `ChangeFrame` · `EntityName` · `ChangeOp` | The change shape + the entity/op contract. |
| `createTenantClient` | The typed HTTP client — `contract()`, `me()`, `issues.list/get`, `pulls.list/get`, `projects.list`, `personalConnections.start/status`, `linearIdentity.get/set`, `agent.*` (issueState, issueLabel, issueComment, issueCreate, reaction, attachment, attachments, session, ask, askAccept, projectRepositoryRegister, projectRepositoryRemove). |
| `TenantContract` · `routeByName` · `teamByKey` · `teamForTicket` · `stageIdForSlot` · `labelIdFor` | The contract document's shape and the pure accessors over it, each returning a typed miss rather than throwing. |
| `TenantClientFailure` · `PageCursor` · `pageCursor` · `memoryContractCache` | The shared failure arms, the opaque page token, and the default contract cache store. |
| `CatalystEventSync` · `readCachedEvents` · `tailCachedEvents` · `EventHistoryGapError` | Node/Bun durable-backbone replay, local cache readers, and explicit history-gap handling from `@catalyst-cloud/sdk/events`. |

`start()` resolves only when `stop()` is called. On a backend, `await` it to keep the process alive; in a browser, never await it — just call `stop()` on teardown.

## License

MIT © Coalesce Labs — see [LICENSE](LICENSE).
