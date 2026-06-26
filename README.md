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

## API

| Export | What it is |
| --- | --- |
| `LiveSyncClient` | The client — `new LiveSyncClient(opts)`, then `start()` / `stop()`. |
| `LiveSyncClientOptions` | The options shape (auth, reseed, getCursor, onChange, onStatus, backoff, wsFactory, log). |
| `AuthStrategy` | `{ kind: "token"; token }` (backend) or `{ kind: "cookie" }` (browser). |
| `LiveSyncStatus` | `"connecting"` · `"live"` · `"reconnecting"` · `"resyncing"` · `"error"` · `"stopped"`. |
| `ChangeFrame` · `EntityName` · `ChangeOp` | The change shape + the entity/op contract. |

`start()` resolves only when `stop()` is called. On a backend, `await` it to keep the process alive; in a browser, never await it — just call `stop()` on teardown.

## License

MIT © Coalesce Labs — see [LICENSE](LICENSE).
