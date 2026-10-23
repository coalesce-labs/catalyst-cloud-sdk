# @catalyst-cloud/sdk

Build apps that read and update your Catalyst Cloud data.

Get live updates for your Linear tickets and GitHub pull requests.

Install: `npm install @catalyst-cloud/sdk`

[SDK documentation](https://github.com/coalesce-labs/catalyst-cloud-sdk#readme)

```ts
import { createTenantClient } from "@catalyst-cloud/sdk";

const client = createTenantClient({
  baseUrl: "https://staging.catalystcloud.dev",
  auth: { kind: "bearer", getToken: async () => credentialProvider.accessToken() },
});
const result = await client.issues.get("CTC-4504");
if (result.outcome === "ok") console.log(result.issue);
```

Use your existing credential provider for token refresh. A native desktop process owns its bearer and returns typed data to the webview. The internal web BFF is cookie-only and is not a supported SDK contract. Read methods retain the deployed tenant contract; this release does not invent the projected or batched routes proposed by the API research.

## Imports

| Import | Purpose | Local database |
| --- | --- | --- |
| `@catalyst-cloud/sdk` | Typed HTTP and live transport | None |
| `@catalyst-cloud/sdk/http` | HTTP client only | None |
| `@catalyst-cloud/sdk/live` | Storage-independent WebSocket transport | None |
| `@catalyst-cloud/sdk-replica-node` | Optional Node/Bun replica | Native SQLite |
| `@catalyst-cloud/sdk-replica-browser` | Optional browser replica | SQLite WASM and OPFS |
| `@catalyst-cloud/sdk/events` | Direct event waits and optional Node/Bun event cache | None for direct waits; local JSONL files for cache waits |

`LiveSyncClient` is a change-feed transport. The caller supplies cursor and resync handlers. Importing or constructing it does not open a database. A consumer may use changes to invalidate cloud views; replay underflow requires refreshing those views and its cursor, not seeding SQL. An authoritative domain-event stream is separate from this row feed.

## Optional replicas and migration from 0.13

Local sync is optional for every client. There is no offline-browsing promise. A disconnected cache is stale and cannot authorize a write. Execution hosts may keep their shared replicas while cloud capacity is measured. Native desktop caching uses the same supervised per-tenant daemon as the CLI and hosts. A desktop window opens a read-only reader instead of creating a second writer.

```sh
# Only when local SQL is required
npm install @catalyst-cloud/sdk-replica-node
# Browser SQL also needs its explicit driver peer
npm install @catalyst-cloud/sdk-replica-browser @sqlite.org/sqlite-wasm
```

The replica modules own the schema, replication and shared-query dependencies. The Node module uses built-in SQLite on Bun or Node 22; `better-sqlite3` is an optional injected driver peer. The browser module declares its SQLite WASM peer separately. npm has no per-export peer dependencies, so these are separate optional package manifests rather than pretending a subpath can own peers.

Existing `/node`, `/browser`, `/browser/db-worker` and `/events` paths remain compatibility exports. From 0.14, users of those replica paths must install the corresponding optional module or explicitly install their SQL peers. New explicit `/replica/node`, `/replica/browser` and `/replica/browser/db-worker` aliases identify the opt-in. Default HTTP/live installs do not need those peers. The root and HTTP declaration bundles contain the pinned wire view types so a TypeScript consumer does not need a SQL package merely to read a response.

Before updating catalyst-cloud consumers, adapt its schema-publish gate to validate the optional replica modules and install the appropriate module alongside any compatibility replica import. The current gate deliberately reads schema pins only from SDK hard dependencies, so it will reject a 0.14 core-only pin as unrecognized. Keep existing cloud consumers on 0.13 until that migration passes.

The normal `publish.yml` release workflow builds, tests and packs all three packages, then publishes the core followed by the Node and browser modules. A rerun skips an already published version only when its registry integrity matches the packed artifact. Different bytes require a version bump. The module manifests are release artifacts in `modules/`; their versions and core SDK peers must match.

For the first publication of the two new names, dispatch that workflow on main with `bootstrap_modules` enabled and a temporary `SDK_BOOTSTRAP_TOKEN` repository secret. Core publication still uses its existing npm trusted publisher. After the initial module publication, configure each module's trusted publisher for `coalesce-labs/catalyst-cloud-sdk`, workflow `publish.yml`, then remove that temporary secret. Later releases use GitHub OIDC for all three packages. The optional modules' first publication must not be mistaken for completion of the catalyst-cloud dependency-gate migration above.

The remaining examples describe the optional transport and replica lifecycle.

## Coverage

- **Linear** — issues, projects, cycles, initiatives, comments, labels, history, and relations. **Mature.**
- **GitHub** — pull requests, checks, commit statuses, and reviews. **Maturing.**
- **Knowledge base** — thoughts / memories. **On the roadmap.**

## Requirements

A catalyst-cloud account and an auth token. (In the browser, a same-origin session cookie is used instead — no token in the page.)

```sh
npm install @catalyst-cloud/sdk
```

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

// Or drive it with the CTC-2111 AuthStrategy — the same shape LiveSyncClient/CatalystReplica take —
// so one credential provider serves every SDK surface. `bearer` is resolved FRESH on every request.
const clientWithAuth = createTenantClient({
  auth: { kind: "bearer", getToken: () => credentialProvider.accessToken() },
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

`client.messages.read({ ticket: "CTC-1234", since: 932140 })` reads message bodies, senders (`from`) and timestamps (`sentAt`), oldest first, at most 50 a page. `since` is the inclusive backbone sequence of the `comms.message.posted` event; omit it for the first page, then pass the returned `next` as `since` on the following read. Stop paging when `messages` is empty or `next` is `null`. An empty page can retain a numeric `next`; save it to resume when another message arrives. Use your own key: a phase session credential is refused by this read route. A ticket outside your teams returns `forbidden` with the server's reason; a server without the route returns `unsupported`.

The contract cache is in memory per client by default; pass `contractCache: { get, set }` to keep it on disk (the bundle does). `fetch`, `now` and `timeoutMs` are injectable so a test never opens a socket.

**CTC-2132 — the remaining routes the bundle's own transport used to reach:**

| Namespace | Methods |
| --- | --- |
| `client.issues.execution(identifier)` | `GET /api/v1/issues/:id/execution` — the ticket's own execution/telemetry report. |
| `client.diagnostics.*` | `workEligibility`, `dispatchQueue`, `fleetActivity`, `agentRoster`, `leaseAttributions`, `codingAccounts` — a customer delegate agent's own "why is nothing moving" / "what is my fleet doing" answers. |
| `client.cycles.list()` | `GET /api/v1/cycles`. |
| `client.search({ q, limit })` | `GET /api/v1/search` — issues/pulls/projects/initiatives buckets. |
| `client.workflowStages()` | `GET /api/v1/workflow-stages` (key-authenticated twin). |
| `client.changes.stream({ since, signal })` / `client.changes.list(...)` | `GET /api/v1/changes` — the NDJSON change feed; `stream()`'s generator may throw mid-iteration on a transport fault, `list()` never throws. `stream()`'s ok arm also carries `close()`: a caller that reads `head` and does not drain `rows` must call it to release the body. |
| `client.snapshot.head()` | `GET /api/v1/snapshot?head=1` — the cheap head probe; never falls back to a full snapshot. |
| `client.request({ method, path, query, headers, body })` | The generic authed-request escape hatch for a route nobody enumerated. **`path` must be an absolute path under the client's own origin** (e.g. `"/api/v1/…"`), never a URL — a value that escapes it is refused before anything is sent. |

Every one of the above is documented from cloud-side prose this package cannot compile against (`catalyst-cloud` is not a dependency here), so each response type carries an index signature and the runtime check validates only the response's *container* shape (an array, or an object with the documented key) — never an individual field — so a cloud-side field addition is never refused as a shape error.

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

An agent can record an answer the ask's owner gave in chat with `agent.recordProxyAskAnswer({ askIssueId, ownerQuote, saidIn, saidAtMs, optionIndex?, acceptedByRole? })`. Quote the owner's words exactly. Leave out `optionIndex` for a free-text answer. `acceptedByRole` optionally names the recording agent's role (one line, at most 64 characters). The answer is recorded as given on the owner's behalf, never as the owner's own. It is refused with `proxy-answering-off` until the owner turns it on with `askProxyAnswering.set(true)`; it is off by default. The owner can take a recorded answer back with `askProxyAnswering.disown(askId)`, which reopens the ask. The `askProxyAnswering` calls require the owner's personal credential.

```ts
const opted = await member.askProxyAnswering.set(true);
const recorded = await agentClient.agent.recordProxyAskAnswer({
  askIssueId: "ENG-50",
  ownerQuote: "Go with B.",
  optionIndex: 1,
  saidIn: chatSessionId,
  saidAtMs: Date.now(),
});
if (recorded.outcome === "refused") console.log(recorded.reason);
```

A repository's Flow metrics come from `flow.series({ repo, period?, granularity? })`. `repo` is `owner/name`. `period` is one of `FLOW_PERIODS`, from `24h` to `26w`; left out, the cloud answers two weeks. `granularity` is `hour` or `day`; left out, the period picks the bucket width. Each entry of `series.columns` is one bucket with `startMs` and `endMs` (epoch milliseconds, end exclusive) and `pr.open`, the pull requests open just before the bucket ends, drafts included. `pr` is `null` when the workspace cannot attribute pull requests to the repository. The last bucket ends at `generatedAt` and is `partial`. Any other granularity is `rejected` with `error: "invalid-granularity"` and `allowed: ["hour", "day"]` before a request is sent. Hourly buckets over a long period can exceed the cloud's response budget; that answer is `rejected` with status 400 and a reason that suggests daily buckets or a shorter period.

```ts
import { createTenantClient } from "@catalyst-cloud/sdk";

const client = createTenantClient({ key: process.env.CATALYST_API_KEY!, baseUrl: "https://staging.catalystcloud.dev" });

const flow = await client.flow.series({ repo: "coalesce-labs/catalyst-cloud", period: "1w", granularity: "hour" });
if (flow.outcome === "ok") {
  for (const bucket of flow.series.columns) console.log(new Date(bucket.endMs).toISOString(), bucket.pr?.open ?? "unavailable");
} else {
  console.log(flow.outcome, flow.reason);
}
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

#### Waiting for events

`waitForEvents` waits directly on the cloud, with its own subscription and no local cache. Its default timeout, `DEFAULT_EVENT_WAIT_TIMEOUT_MS`, is four hours. Socket keepalive and occasional replay checks keep the wait healthy; it produces no output and does not return during a silent wait. Supply a token or refreshing bearer credential. Phase session credentials can subscribe when the server admits them.

```ts
import { waitForEvents, EVENT_WAIT_EXIT_CODES } from "@catalyst-cloud/sdk/events";

const result = await waitForEvents({
  baseUrl: "https://staging.catalystcloud.dev",
  auth: { kind: "token", token: process.env.CATALYST_API_KEY! },
  tenantId: "workspace-1", // your workspace ID
  filter: {
    type: "pr.conflict.detected",
    ref: "acme/web:feature/checkout",
  },
  timeoutMs: 4 * 60 * 60 * 1000,
});
if (result.outcome === "matched") {
  for (const event of result.events) console.log(JSON.stringify(event));
}
process.exitCode = EVENT_WAIT_EXIT_CODES[result.outcome];
```

A filter needs at least one key. Values of one key are OR'ed; different keys are AND'ed. Each key accepts a string or a list of up to 50 distinct values, except `to`, which accepts only `"me"`. `events.query` accepts the same keys and sends each value separately.

| Filter key | Example |
| --- | --- |
| `type` | `"pr.conflict.detected"` or a family prefix, `"pr.*"` |
| `ticket` | `"CTC-1234"` |
| `repo` | `"coalesce-labs/catalyst-cloud"` |
| `ref` | `"acme/web:feature/checkout"` |
| `pr` | `"coalesce-labs/catalyst-cloud#9001"` |
| `session` | `"CTC-1234/implement#7"` |
| `to` | `"me"`, resolved by the cloud against your credential |

A direct wait checks access before opening its socket and returns the server's refusal when your key cannot reach the requested scope. After a dropped connection it replays from its cursor, waits for index coverage, and falls back to backbone replay where permitted. It reports a history gap explicitly if retained history cannot cover the cursor.

A matched result contains events oldest first, up to `maxEvents` (default 100). Save its `cursor` and pass it as `after` on the next wait: later matches are delivered once. The cursor covers only events already delivered or proven absent; a capped batch leaves later events for the next call. Timeout and abort results also include the last proven cursor, which may be `null` if none was established.

| Outcome | Meaning | `EVENT_WAIT_EXIT_CODES` |
| --- | --- | --- |
| `matched` | One or more matching events and a resume cursor | `0` |
| `failed` | A history gap, unsupported server, invalid response, or unavailable cache; inspect `code` and `reason` | `1` |
| `refused` | Invalid filter or refused cloud access; inspect `source`, `filter` and `reason` | `3` |
| `timeout` | No match before the deadline; includes `waitedMs` and the resume cursor | `124` |
| `aborted` | The caller aborted its signal; includes the resume cursor | `130` |

For continuous delivery, `watchEvents({ ...options, signal })` yields matched batches from one subscription without a timeout. It finishes after yielding a refusal, failure or abort. Breaking out of the loop closes the subscription.

```ts
import { watchEvents } from "@catalyst-cloud/sdk/events";

const controller = new AbortController();
for await (const batch of watchEvents({
  baseUrl: "https://staging.catalystcloud.dev",
  auth: { kind: "token", token: process.env.CATALYST_API_KEY! },
  tenantId: "workspace-1",
  filter: { type: "comms.message.posted", ticket: "CTC-1234" },
  signal: controller.signal,
})) {
  if (batch.outcome === "matched") console.log(batch.events, batch.cursor);
  else console.error(batch);
}
```

A key can hold at most 20 event sockets at once. To share one upstream connection among many waits, run one `CatalystEventSync` writer and use `waitForCachedEvents` for each reader:

```ts
import { waitForCachedEvents } from "@catalyst-cloud/sdk/events";

const result = await waitForCachedEvents({
  tenantId: "workspace-1",
  filter: { type: "comms.message.posted", ticket: "CTC-1234" },
  // directory: "/path/to/existing/event-cache", // optional override
});
```

A cache wait reads incrementally, opens no upstream connection and never creates or writes a cache. It fails immediately with `cache-missing` or `cache-writer-stopped` when the cache or its live writer is absent. A cache holds only its retained history, so a cursor older than that history returns `history-gap`. It supports the same cursor, timeout and batch options. `to: "me"` is refused locally because only the cloud can resolve your identity; use a direct wait for that filter.

## API

| Export | What it is |
| --- | --- |
| `LiveSyncClient` | The client — `new LiveSyncClient(opts)`, then `start()` / `stop()`. |
| `LiveSyncClientOptions` | The options shape (auth, reseed, getCursor, onChange, onStatus, backoff, wsFactory, log). |
| `AuthStrategy` | `{ kind: "token"; token }` (backend) or `{ kind: "cookie" }` (browser). |
| `LiveSyncStatus` | `"connecting"` · `"live"` · `"reconnecting"` · `"resyncing"` · `"error"` · `"stopped"`. |
| `ChangeFrame` · `EntityName` · `ChangeOp` | The change shape + the entity/op contract. |
| `createTenantClient` | The typed HTTP client — `contract()`, `me()`, `request()`, `issues.list/get/execution`, `pulls.list/get`, `projects.list`, `personalConnections.start/status`, `linearIdentity.get/set`, `askProxyAnswering.get/set/disown`, `cycles.list`, `flow.series`, `search`, `workflowStages`, `changes.stream/list`, `events.query/pages`, `messages.read`, `snapshot.head`, `diagnostics.*` (workEligibility, dispatchQueue, fleetActivity, agentRoster, leaseAttributions, codingAccounts), `agent.*` (issueState, issueLabel, issueComment, issueCreate, reaction, attachment, attachments, session, ask, askAccept, recordProxyAskAnswer, projectRepositoryRegister, projectRepositoryRemove, message). Accepts either `key: string` or the CTC-2111 `auth: AuthStrategy`. |
| `TenantContract` · `routeByName` · `teamByKey` · `teamForTicket` · `stageIdForSlot` · `labelIdFor` | The contract document's shape and the pure accessors over it, each returning a typed miss rather than throwing. |
| `TenantClientFailure` · `PageCursor` · `pageCursor` · `memoryContractCache` | The shared failure arms, the opaque page token, and the default contract cache store. |
| `CatalystEventSync` · `readCachedEvents` · `tailCachedEvents` · `EventHistoryGapError` | Node/Bun durable-backbone replay, local cache readers, and explicit history-gap handling from `@catalyst-cloud/sdk/events`. |
| `waitForEvents` · `watchEvents` · `waitForCachedEvents` | One-shot and continuous filtered waits from `@catalyst-cloud/sdk/events`. |
| `EventWaitOptions` · `EventWatchOptions` · `EventWaitResult` · `WaitedEvent` · `CachedEventWaitOptions` | Wait inputs, event batches and typed terminal outcomes from `@catalyst-cloud/sdk/events`. |
| `DEFAULT_EVENT_WAIT_TIMEOUT_MS` · `EVENT_WAIT_EXIT_CODES` | Four-hour default and outcome-to-exit-code mapping from `@catalyst-cloud/sdk/events`. |
| `EventScopeFilter` · `EventScopeFilterInput` · `checkEventFilter` · `eventScopeKeys` · `matchesEventFilter` · `matchesEventType` | Shared filter grammar, scope derivation and matching from `@catalyst-cloud/sdk/events`. |
| `EventFilterKey` · `EventFilterCheck` · `EventScopeKeys` · `EVENT_FILTER_MAX_VALUES` · `eventFilterSocketParams` · `eventFilterQueryParams` | Filter validation types, limits and serialization from `@catalyst-cloud/sdk/events`. |
| `MessagesReadParams` · `MessagesReadResult` · `AgentMessageRecord` | Message read inputs, results and records from the package root. |

`start()` resolves only when `stop()` is called. On a backend, `await` it to keep the process alive; in a browser, never await it — just call `stop()` on teardown.

## License

MIT © Coalesce Labs — see [LICENSE](LICENSE).
