# @catalyst-cloud/sdk

The **isomorphic read-layer-over-WebSocket client** for the catalyst-cloud change-feed.

`catalyst-cloud` mirrors each tenant's Linear + GitHub state into a per-tenant Cloudflare Durable
Object and exposes it as a `change_log` feed. This SDK is the **push** consumer of that feed: it opens
an outbound WebSocket to a tenant's Mirror DO (`{baseUrl}/connect`), sends a cursor-replay request on
every (re)connect, and hands you each pushed change so you can land it into your own store.

It is the standalone extraction of the host daemon's live client, generalized to run **unchanged in a
browser, in Node (>=22), or in Bun**. It is the single push transport behind both the host replicas
and the web UI per [ADR-0008](https://github.com/coalesce-labs/catalyst-cloud/blob/main/docs/adr/0008-catalyst-host-local-replica-consumption.md)
(host reads from an SDK-managed per-host replica) and [ADR-0009](https://github.com/coalesce-labs/catalyst-cloud/blob/main/docs/adr/0009-browser-live-updates-over-websocket.md)
(browser live updates over a **hibernating** WebSocket, not SSE — so an idle tab can't pin the DO).

## Install

```sh
npm install @catalyst-cloud/sdk
```

Requires a platform `WebSocket` global (browser, Bun, or Node >=22). On older Node, inject a
`wsFactory` (e.g. wrapping the [`ws`](https://www.npmjs.com/package/ws) package) — the shared core has
no node-only import.

## Design

The client is **transport-only and storage-agnostic**. It knows nothing about where your data lives
(`bun:sqlite`, OPFS, in-memory) or how you authenticate — both are **injected**:

- **`auth`** — `{ kind: "token", token }` (a trusted backend appends `?token=`; the Worker strips it
  before forwarding to the DO) **or** `{ kind: "cookie" }` (the browser appends nothing; the
  same-origin session cookie rides the upgrade). The cookie path can never leak a token.
- **`reseed`** — an async callback returning the fresh cursor after a full re-seed. Called on a
  `{type:"resync"}` underflow frame (and before the first connect if you have no cursor). A host pulls
  `/snapshot` into SQLite here; a browser re-runs its own seed.
- **`getCursor` / `onChange`** — you own the cursor and the writes; the client drives the wire.
- **`onStatus`** — connection lifecycle (`connecting` / `live` / `reconnecting` / `resyncing` /
  `error` / `stopped`) so a UI can show "live" vs "reconnecting".

It reconnects with **capped exponential backoff** (1 s → ×2 → 30 s ceiling) and never throws out of
the event loop. There is **no client-side keepalive** — re-implementing the WebSocket heartbeat would
re-pin the hibernating DO, defeating ADR-0009.

## Usage

### Browser (cookie auth, no token)

In the browser there is **no token**: pass `auth: { kind: "cookie" }` and the same-origin session
cookie rides the WebSocket upgrade automatically. The SDK appends nothing secret to the URL, so a
token can never leak from a browser tab. Storage is yours — here it is a trivial in-memory map; in a
real app you would seed from your own `/snapshot` fetch into OPFS / IndexedDB.

```ts
import { LiveSyncClient } from "@catalyst-cloud/sdk";

const issues = new Map<string, unknown>();
let cursor: number | null = null;

const client = new LiveSyncClient({
  baseUrl: "https://app.example.com/api/v1", // same origin as the page
  accountId: "tenant-0",
  auth: { kind: "cookie" }, // same-origin cookie rides the upgrade; no token ever in the URL

  // Full (re)seed from your own snapshot endpoint; resolve to the fresh cursor.
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

void client.start(); // never await in the browser — it resolves only on stop()
// on teardown (component unmount, page hide):
client.stop();
```

### Backend / host daemon (token auth, `bun:sqlite` reseed)

A trusted backend authenticates with a service token (`?token=`, stripped by the Worker before it
reaches the DO) and lands every frame into a local SQLite replica. On a `{type:"resync"}` underflow
(or the very first run with no cursor) it pulls a full `/snapshot` and rewrites the table:

```ts
import { Database } from "bun:sqlite"; // node: use better-sqlite3 with the same calls
import { LiveSyncClient } from "@catalyst-cloud/sdk";

const db = new Database("replica.sqlite");
db.run("CREATE TABLE IF NOT EXISTS issues (id TEXT PRIMARY KEY, row TEXT, seq INTEGER)");
db.run("CREATE TABLE IF NOT EXISTS sync_state (k TEXT PRIMARY KEY, cursor INTEGER)");

const baseUrl = process.env.CATALYST_CLOUD_BASE_URL!; // incl. /api/v1
const token = process.env.ADMIN_TOKEN!;

const client = new LiveSyncClient({
  baseUrl,
  accountId: "tenant-0",
  auth: { kind: "token", token },

  // Full re-seed: pull /snapshot over HTTPS and rewrite the replica in one transaction.
  // Must resolve to the fresh cursor (the snapshot's max change_log.seq).
  reseed: async () => {
    const res = await fetch(`${baseUrl}/snapshot?account=tenant-0`, {
      headers: { authorization: `Bearer ${token}` },
    });
    const snap = (await res.json()) as { cursor: number; issues: { id: string }[] };
    const tx = db.transaction(() => {
      db.run("DELETE FROM issues");
      const ins = db.prepare("INSERT INTO issues (id, row, seq) VALUES (?, ?, ?)");
      for (const row of snap.issues) ins.run(row.id, JSON.stringify(row), snap.cursor);
      db.run(
        "INSERT INTO sync_state (k, cursor) VALUES ('replica', ?) " +
          "ON CONFLICT(k) DO UPDATE SET cursor = excluded.cursor",
        [snap.cursor],
      );
    });
    tx();
    return snap.cursor;
  },

  // Own the cursor + the writes; the client just drives the wire.
  getCursor: () =>
    (db.query("SELECT cursor FROM sync_state WHERE k = 'replica'").get() as
      | { cursor: number }
      | undefined)?.cursor ?? null,

  onChange: (frame) => {
    const tx = db.transaction(() => {
      if (frame.op === "delete") {
        db.run("DELETE FROM issues WHERE id = ?", [frame.entityId]);
      } else {
        db.run(
          "INSERT INTO issues (id, row, seq) VALUES (?, ?, ?) " +
            "ON CONFLICT(id) DO UPDATE SET row = excluded.row, seq = excluded.seq",
          [frame.entityId, JSON.stringify(frame.row), frame.seq],
        );
      }
      db.run("UPDATE sync_state SET cursor = ? WHERE k = 'replica'", [frame.seq]);
    });
    tx();
  },
});

await client.start(); // resolves only when stop() is called — keeps the process alive between deltas
```

> On Node older than 22, inject a `wsFactory` that wraps the [`ws`](https://www.npmjs.com/package/ws)
> package: `wsFactory: (url) => new WebSocket(url) as unknown as WebSocketLike`. Node 22+ and Bun
> expose a global `WebSocket`, so no factory is needed.

## API

| Export | What it is |
| --- | --- |
| `LiveSyncClient` | The client. `new LiveSyncClient(opts)`, then `start()` / `stop()` / `connectUrl()`. |
| `LiveSyncClientOptions` | The full options shape (auth, reseed, getCursor, onChange, onStatus, backoff, wsFactory, log). |
| `AuthStrategy` | `{ kind: "token"; token }` (backend) or `{ kind: "cookie" }` (browser). |
| `LiveSyncStatus` | `"connecting"` · `"live"` · `"reconnecting"` · `"resyncing"` · `"error"` · `"stopped"`. |
| `WebSocketLike` / `WebSocketFactory` | The minimal structural WS surface + the injectable factory (for old Node / tests). |
| `ChangeFrame` · `ResyncFrame` · `SyncFrame` · `ServerFrame` | The wire frames (see below). |
| `EntityName` · `ChangeOp` · `ENTITY_NAMES` · `CHANGE_OPS` | The canonical table / op contract (types + frozen runtime arrays). |
| `buildConnectUrl` · `parseFrame` · `toWsOrigin` | The pure helpers the client is built from (exported for diagnostics/tests). |

`start()` returns a Promise that resolves **only** when `stop()` is called — on a backend `await` it
to keep the process alive; in a browser never await it and just call `stop()` on teardown.

## Wire contract

The frame shapes (`ChangeFrame`, `ResyncFrame`, `SyncFrame`, `EntityName`, `ChangeOp`) are vendored as
a self-contained copy of the catalyst-cloud `@catalyst-cloud/types` contract
([`src/types.ts`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/main/src/types.ts)) — the
SDK has **no dependency** on that internal package. A contract test pins the literal members so drift
from the monorepo is caught.

## Contributing

The source of truth is `src/`; the published `dist/` is generated by `tsc` and must stay byte-stable
(the catalyst-cloud monorepo consumes it via a `file:` dependency). Workflow:

```sh
bun install
bun run build        # tsc -p tsconfig.build.json → dist/
bun run typecheck    # tsc --noEmit over src + test
bunx vitest run      # the test suite (contract + client)
```

Before opening a PR, run all three and confirm `dist/` has no unexpected diff. CI (GitHub Actions)
runs install + build + test on every push and PR. Releases are cut by bumping `version` and running
`npm publish` (the `prepublishOnly` hook rebuilds `dist/` first; the package is published with public
access via `publishConfig`).

## License

MIT © Coalesce Labs — see [LICENSE](LICENSE).
