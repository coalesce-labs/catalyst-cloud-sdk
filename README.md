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

### Browser (cookie auth, OPFS store)

```ts
import { LiveSyncClient } from "@catalyst-cloud/sdk";

const client = new LiveSyncClient({
  baseUrl: "https://app.example.com/api/v1",
  accountId: "tenant-0",
  auth: { kind: "cookie" }, // same-origin cookie rides the upgrade; no token ever in the URL
  reseed: async () => seedFromSnapshot(), // your seed; returns the fresh cursor
  getCursor: () => store.cursor,
  onChange: (frame) => store.apply(frame),
  onStatus: (s) => ui.setConnection(s),
});

void client.start();
// on teardown:
client.stop();
```

### Backend (token auth)

```ts
import { LiveSyncClient } from "@catalyst-cloud/sdk";

const client = new LiveSyncClient({
  baseUrl: process.env.CATALYST_CLOUD_BASE_URL!, // incl. /api/v1
  accountId: "tenant-0",
  auth: { kind: "token", token: process.env.ADMIN_TOKEN! },
  reseed: () => pullSnapshotIntoSqlite(),
  getCursor: () => getCursor(db),
  onChange: (frame) => applyChange(db, frame),
});

await client.start(); // resolves only when stop() is called
```

## Wire contract

The frame shapes (`ChangeFrame`, `ResyncFrame`, `SyncFrame`, `EntityName`, `ChangeOp`) are vendored in
[`src/types.ts`](src/types.ts) as a self-contained copy of the catalyst-cloud `@catalyst-cloud/types`
contract — the SDK has **no dependency** on that internal package. A contract test pins the literal
members so drift from the monorepo is caught.

## License

MIT
