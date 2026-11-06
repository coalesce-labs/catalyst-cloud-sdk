---
date: 2026-11-06T13:36:00+00:00
researcher: Claude (research-codebase skill)
git_commit: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
branch: CTC-6150
git_main_sha: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
git_merge_base: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
repository: catalyst-cloud-sdk
topic: "CTC-6150: what exists in the SDK for the browser replica to answer scoped ticket filters and facet counts locally (queryIssuesScoped, queryIssueFacetCounts)"
tags: [research, codebase, browser-replica, worker-protocol, read-model, issue-scope, facet-counts, release-train]
status: complete
last_updated: 2026-11-06
last_updated_by: Claude (research-codebase skill)
type: research
source_ticket: CTC-6150
---

# Research: CTC-6150, scoped ticket queries and facet counts on the browser replica

**Date**: 2026-11-06T13:36:00+00:00
**Researcher**: Claude (research-codebase skill)
**Git Commit**: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2 (HEAD, equal to fetched `origin/main` and to the merge base)
**Branch**: CTC-6150
**Repository**: catalyst-cloud-sdk

## Research Question

The ticket (CTC-6150, "The browser replica should answer scoped ticket filters and facet counts without a server round trip") asks for two SDK ops so a browser replica can answer scoped and facet queries locally. Its Gherkin scenarios are the requirements:

1. A browser replica holding 20,000 issues, 6,000 of them in team CTC. The web asks for team CTC's tickets carrying label "mirror-do", newest first. The replica returns them through the SDK's `queryIssuesScoped`, and the first page returns in under 10 ms p50.
2. Team CTC has tickets in 12 projects and 300 tickets with no project. The web asks the replica for project facet counts within team CTC. `queryIssueFacetCounts` returns each project's count and a "No project" count of 300, in under 25 ms p50.
3. Given a catalyst-cloud-sdk release that contains both ops, when catalyst-cloud's SDK range is bumped in its five package.json files and bun.lock, the web's replica path uses the two ops and drops the no-replica-op tag for these reads.

The ticket also says to check CTC-5078 first: if the browser replica has retired, this ticket closes as superseded.

This document describes what exists in the repository today. It does not propose a design.

## Summary

Neither op exists. `queryIssuesScoped` and `queryIssueFacetCounts` have zero hits in the repository at HEAD, and `@catalyst-cloud/sdk` 0.16.23, the version in `package.json` and the npm `latest` tag, is the newest release. The browser replica's worker protocol still carries `queryIssues`, `queryIssueDetail` and `queryPulls` as its only read requests (`src/replica/browser/protocol.ts:122-124`). `queryIssues` carries only `limit` and `offset`, and the worker calls `buildIssuesView(read, limit, offset)` without the builder's fourth `scope` argument (`src/replica/browser/worker-core.ts:199-205`).

The builders both ops would wrap are already installed. `@catalyst-cloud/read-model` 0.16.0, the only 0.16.x release on npm, exports `buildIssuesView(sql, limit, offset, scope?)` and `buildIssueFacetCounts(sql, scope, dimension)` with the `IssueScope` type (CTC-3109 code). The browser replica already runs read-model's index set and FTS tables on every open (`src/replica/browser/ports.ts:178`, `src/replica/read-model-ddl.ts:39-49`). The SDK's `read-model` range is `^0.16.0`, which already admits 0.16.0. Adding an op to the browser replica touches the same places the three existing ops touch: the request interface, the `ReplicaRequest` union and `ResultMap` in `protocol.ts`, a `switch` case in `worker-core.ts`, a method on `BrowserReplica`, and the `SEED_GATED_WORKER_RPCS` set.

Several points in the scenarios do not line up one to one with what the builders take or return. `IssueScope.labelId` is a label id, and no read-model code resolves a label name such as "mirror-do". The "No project" facet entry comes back as `{ key: null, name: null, count }`, so the words "No project" are not in the data. The first-page and facet timings have no existing test harness: the repo has no latency tests. I measured both builders over the SDK's own browser worker core at the scenarios' sizes, in Bun with in-memory sqlite-wasm: p50 was 1.6 to 4.1 ms for scoped pages and 3.8 to 4.0 ms for project facet counts, excluding the Worker message round trip and OPFS (details below). I could not read CTC-5078, CTC-3935 or the catalyst-cloud repository from this container, so the supersede check and the third scenario are unverified.

## Detailed Findings

### 1. The browser replica's worker protocol today

**What exists**: a typed RPC contract between the main thread and the OPFS Worker.

- Request interfaces: `QueryIssuesRequest { type: "queryIssues"; limit?; offset? }` (`src/replica/browser/protocol.ts:81-86`), `QueryIssueDetailRequest` (`:88-92`), `QueryPullsRequest` (`:94-99`).
- `ReplicaRequest` is an 11-member union (`:115-126`). `ResultMap` maps each `type` to its result (`:160-171`): `queryIssues: IssueView[]`, `queryIssueDetail: IssueDetailView | null`, `queryPulls: PullView[]`, `getCursor: number | null`.
- `WorkerCore.handle` returns `Promise<unknown>` (`worker-core.ts:39-41`). The client casts through `ResultMap[K]` (`browser-replica.ts:517-543`). Nothing enforces the match at runtime. Tests cast results themselves, for example `as IssueView[]`.
- The three read cases are at `worker-core.ts:199-217`. Each awaits `readGate.whenReadable(session !== null)`, then runs a read-model builder over `requireReplica().read`. `queryIssues` calls `buildIssuesView(read, request.limit, request.offset)` and omits `scope`. `buildPullsView` is likewise called without its scope, and `buildIssueDetail` without `opts`.
- `db.worker.ts` has no per-op code. It forwards each envelope to `core.handle` and posts `{ id, ok: true, result }` or `{ id, ok: false, error }` (`db.worker.ts:26-44`).
- `src/browser.ts:44-51` re-exports the protocol types `WireChange, ReplicaRequest, ReplicaResponse, Envelope, ResultMap, ApplyChangesResult`. It does not name individual request interfaces and re-exports none of read-model's view types. `src/node.ts:80-91` does re-export `IssueView`, `IssueDetailView`, `PullView` and others from read-model, but not `IssueScope`, `IssueFacetCount` or `IssueFacetDimension`.

**Connections**: `BrowserReplica.queryIssues(limit?, offset?)` posts `{ type: "queryIssues", limit, offset }` (`browser-replica.ts:956-958`). `queryIssueDetail` and `queryPulls` follow at `:961-968`. Each is a bare `return this.call({...})`.

### 2. How a request travels and is bounded (client side)

**What exists**: `call<K>` (`browser-replica.ts:517-543`) rejects with a latched `workerError` first, then with `"replica client closed"` if disposed or not started. Otherwise it allocates `this.nextId++`, registers `{resolve, reject}` in `pending`, and `postMessage`s the envelope with no transfer list, so request and response are structured-cloned. It returns `withWorkerDeadline(request.type, sent)`.

- Every request type gets a deadline (`DEFAULT_WORKER_RPC_TIMEOUT_MS = 120_000`, `:79`), "bounded by construction" (`:507-516`). `workerRpcTimeoutMs <= 0` disables it.
- `SEED_GATED_WORKER_RPCS` (`:97-101`) is a `ReadonlySet<string>` holding exactly `queryIssues`, `queryIssueDetail`, `queryPulls`. On expiry, a member of that set re-arms a fresh timer while a seed is in flight (`this.seedAbort` is set). Any other type takes the wedge path: `failWorker(err)` latches the error, releases the worker and the Web Lock, and rejects (`:1007-1039`). The set is keyed by string, not by the request-type union, so the compiler does not tie a new read op to it.
- `SeedReadGate` (`seed-read-gate.ts:25-35`) exists because the read and write adapters share one sqlite-wasm handle. A read arriving between `seedBegin` and `seedCommit` would see a truncated, half-filled transaction. Each read case in `worker-core.ts` opts into the gate itself by awaiting it.
- No request-shape validation exists on either side. `validate.ts` only checks constructor options. Limit and offset handling is read-model's clamps.

### 3. The open path guarantees read-model's indexes and FTS tables

**What exists**: `buildOpenedReplica(db)` (`src/replica/browser/ports.ts:107-209`) builds the `read` executor (`:108-118`), the `write` port, applies `MIRROR_MIGRATIONS`, creates `sync_meta`, and calls `applyReadModelDdl(write)` (`:178`) before any query dispatch. `applyReadModelDdl` (`src/replica/read-model-ddl.ts:39-49`) runs read-model's `ensureReadModelDdl` in one transaction through an eager executor, because the browser `read` executor only runs a statement inside `toArray()` (`read-model-ddl.ts:6-9`). It is idempotent. The Node replica does the same (`catalyst-replica.ts:592`).

The `read` executor is `exec(query, ...bindings) => { toArray: () => db.exec(query, { bind, rowMode: "object", returnValue: "resultRows" }) }` (`ports.ts:108-118`): synchronous sqlite-wasm calls on the Worker thread.

### 4. What read-model 0.16.0 provides (installed in `node_modules/@catalyst-cloud/read-model`, source-only package)

Paths below are relative to that package's `src/`.

**`IssueScope`** (`issues.ts:983-1092`) is, per its own doc comment, "THE ONLY FILTER THE READ MODEL APPLIES". Fields relevant to the scenarios:

- `teamId` (case-insensitive, `lower(team_id) = ?`) and `teamKey` fallback; `teamId` wins if both are set (`issues.ts:1297-1317`).
- `labelId`: "Issues carrying this label (`issue_labels.label_id`)", applied as `issues.id IN (SELECT issue_id FROM issue_labels WHERE label_id = ?)` (`:1269-1273`). It is an exact, case-sensitive id match. Nothing in the package resolves a label name to an id. The only label-name comparisons are against fixed ask-label constants (`waiting-on-me.ts:37,41`), and `read-indexes.ts:276-278` records that `labels(name)` "served no query".
- `projectId`: `null` means "no project" (`project_id IS NULL`); `undefined` or a blank string narrows nothing (`:1056-1061, 1279-1284`). The doc warns against passing a `searchParams.get()` result straight through. Structured clone through `postMessage` keeps `null` and `undefined` distinct, unlike JSON.
- Other fields: `initiativeId`, `cycleId`, `stateBucket`, `priority`, `assignee`, `waitingOn`, `blockedOnAsk`, `stateName`, and the `no*` flags (`noLabel`, `noCycle`, `noInitiative`, `noAssignee`, `noPriority`, `noTeam`, `noState`, `unresolvedAssignee`). All are plain data (strings, numbers, booleans, null, and the nested `waitingOn` and `assignee` objects), with no functions or Maps. `waitingOn.excludeSql` is a caller-supplied SQL string spliced into the statement.
- `drivingFacet` (`:1117-1123`): the first of `labelId`, `initiativeId`, `cycleId`, `stateName` that is set drives the plan, and the other seekable terms get a unary `+` so SQLite seeks the driver's index. This changes the plan, not the result set.

**`buildIssuesView(sql, limit = 100, offset = 0, scope?)`** (`issues.ts:1533-1560`): `SELECT ${ISSUE_PAGE_COLUMNS} FROM issues WHERE ${live}${clause} ORDER BY updated_at DESC LIMIT ? OFFSET ?`, then `hydrateIssueRows`. "Newest first" here is `updated_at DESC` with no tie-break and no `COALESCE`. `clampLimit` returns 100 for null, non-finite or below 1, and caps at `MAX_VIEW_LIMIT` 500 (`pagination.ts:13-30`).

**`buildIssuesViewKeyset(sql, { limit, after, scope, sort })`** (`issues.ts:1602-1724`) returns `{ rows, nextCursor }`, ordered `COALESCE(updated_at, 0) DESC, id DESC`, with an opaque cursor, and `sort` of `"updated" | "priority" | "blocking"`. `countIssues(sql, scope)` (`:1327-1334`) counts under the same scope clause. Neither replica references the keyset builder or `countIssues` today.

**`hydrateIssueRows`** (`issues.ts:1352-1531`) adds page-scoped statements: users for assignees, live labels, relations, ask edges, and ask-assignee users. A page of up to 91 rows costs 3 to 5 extra statements, chunked at 100 bound parameters.

**`buildIssueFacetCounts(sql, scope, dimension): IssueFacetCount[]`** (`issue-facets.ts:272-365`):

- `IssueFacetCount = { key: string | number | null; name: string | null; count: number }` (`:66-70`). Dimensions: `label, project, initiative, cycle, state, assignee, team, priority` (`:48-57`).
- Project dimension SQL (`:355-364`): a `GROUP BY project_id` over `issues` under the scope clause, `LEFT JOIN projects t ON t.id = g.key`. Issues with no project come back as `{ key: null, name: null, count }`. `ordered` (`:186-195`) drops zero-count groups, sorts largest first then by name, and puts the null group last. A project id with no mirrored `projects` row comes back with a string key and `name: null`. A project with no issues in scope is never listed.
- `issueFacetGroupScope("project", key)` returns `{ projectId: key }`, which for the null group is `{ projectId: null }` (`:223-224`). The doc says `{ ...scope, ...issueFacetGroupScope(d, g.key) }` passed to `countIssues` returns exactly the group's count.
- "The counts are over the scope as given: to count a menu's choices while that menu's own filter is set, pass the scope without it" (`:250-252`).
- Unscoped, "EVERY DIMENSION IS A WALK OF EVERY LIVE ISSUE: an accepted whole-tenant aggregate" (`:268-270`).

**Adjacent, not named in this ticket's scenarios**: `buildIssueTextInScope` and `countIssueTextInScope` (`issue-facets.ts:479-571`) answer "search inside a filtered list" through `issues_fts`. No SDK op wraps them. The ticket's text says CTC-3109's web half sends those reads to the server.

**Indexes** (`read-indexes.ts`, all `IF NOT EXISTS`): `idx_issue_labels_label ON issue_labels(label_id, issue_id)` (`:166-168`), `idx_issues_team_order ON issues(lower(team_id), removed_at, COALESCE(updated_at,0) DESC, id DESC)` (`:144-147`), `idx_issues_project_order` (`:159-162`), `idx_issues_live_keyset` (`:79-82`). The bundled schema adds `idx_issues_updated(removed_at, updated_at)`, `idx_issues_team(team_id)`, `idx_issues_identifier`. The in-package comments cite 20,000-issue plan tests in `apps/mirror/test/list-facets-query-plan.test.ts` (not in this repo), for example a label on 120 of 20,000 issues at "0.13 ms and a few hundred rows seeking the label" against 1.9 ms walking the live keyset index (`issues.ts:1110-1116`). The comments state no latency target for the offset page builder.

**Published versions**: `npm view` lists `@catalyst-cloud/read-model` versions ending at 0.16.0, and `@catalyst-cloud/sdk` ending at 0.16.23 with `latest` = 0.16.23. The SDK's `package.json` declares `@catalyst-cloud/read-model` as `^0.16.0` in `devDependencies`, `peerDependencies` and the replica modules' `dependencies`, and `bun.lock` resolves it to 0.16.0.

### 5. Node replica read surface (for parity context)

`CatalystReplica` exposes `get sql(): SqlExecutor`, `issues(opts?)`, `issue(identifier)`, `pulls(opts?)`, `projects`, `project`, `initiatives`, `initiative` and a `cursor` getter (`src/replica/catalyst-replica.ts:894-920`). `issues` calls `buildIssuesView(this.sql, opts?.limit, opts?.offset)` with no scope. The browser exposes the first three as async RPCs. Because the Node replica hands out the raw `sql` executor, a Node consumer can already call any read-model builder, including the scoped and facet ones. The browser keeps `read` private inside the Worker (`OpenedReplica.read`, `ports.ts:63`).

### 6. Tests and measurement tooling that exist

- `test/browser/worker-core.test.ts`: `memoryCore()` builds `createWorkerCore` over a `:memory:` sqlite-wasm DB through `buildOpenedReplica` (`:23-35`); `issueRow(id, over)` (`:38-50`); `seed(core, rows, cursor)` runs `seedBegin`, one `seedBatch`, `seedCommit` (`:52-67`). It exercises only `queryIssues`, with no `limit` or `offset`, and seeds at most about 3 rows per test. Neither `queryIssueDetail` nor `queryPulls` has a worker-core test, and no browser test checks a query result through the client.
- `test/browser/browser-replica.test.ts`: `FakeWorker(answers, delays, silentFor)` records envelopes in `received` and replies per request type (`:32-90`). Its query tests cover only error paths: closed, load failure, wedged read with `workerRpcTimeoutMs: 40` (`:351-357, 514, 1100-1124`). The seed-gated re-arm branch (`browser-replica.ts:1017`) has no test.
- `test/helpers/read-model-plan.ts`: `recordStatements`, `planDetails` (runs `EXPLAIN QUERY PLAN`), `fullScans`. `test/browser/read-model-ddl.test.ts:72-102` uses them to assert the pulls list and review facet seek read-model's indexes with `fullScans(...) === []`, over three seeded rows.
- No test measures wall-clock latency or p50. There is no `performance.now`, `hrtime`, `bench` or `p50` anywhere in `test/`, `scripts/` or `src/`. The only elapsed-time assertions are generous bounds in unrelated tests (`test/tenant-client-reads.test.ts:249-253`, `test/ticket-lease-client.test.ts:201-204`). The only mention of the CTC-4276 timings (pulls list 883 ms against 73 ms) is a comment at `test/node/read-model-ddl.test.ts:33-35`; the tests assert plans, not times.
- No existing helper seeds thousands of rows. `MAX_INBOX_DEPTH` at 20,000 in `delta-queue.test.ts:293` is a constant, not a seed.

### 7. Measurement I ran for the two latency figures

I wrote a scratch script outside the repo (`/tmp/ctc6150-measure/measure.ts`, not committed). It seeds 20,000 issues through the SDK's real `createWorkerCore` seed path (`seedBegin`, batches, `seedCommit`) over `:memory:` sqlite-wasm with read-model's index set applied by `buildOpenedReplica`. 6,000 issues are in team CTC: 300 with no project and 5,700 spread over 12 projects. The other 14,000 are in another team. Each timing is a direct call to the read-model builder over `opened.read`, 200 iterations after 20 warmups, in Bun 1.4.2 on the research host.

| Call (team CTC = `teamId`) | label on 120 CTC issues | label on 1,500 CTC issues |
|---|---|---|
| `buildIssuesView(read, 100, 0, {teamId, labelId})` | p50 2.96 ms, p95 4.17 ms | p50 4.12 ms, p95 4.78 ms |
| same with limit 50 | p50 1.57 ms | p50 2.65 ms |
| `buildIssuesView(read, 100, 0)` (today's `queryIssues`) | p50 2.28 ms | p50 2.30 ms |
| `buildIssuesView(read, 100, 0, {teamId})` | p50 2.45 ms | p50 2.60 ms |
| `buildIssueFacetCounts(read, {teamId}, "project")` | p50 4.03 ms, p95 4.44 ms | p50 3.79 ms, p95 4.12 ms |
| `buildIssueFacetCounts(read, {teamId}, "label")` | p50 5.53 ms | p50 6.05 ms |
| `buildIssueFacetCounts(read, {teamId}, "state")` | p50 7.20 ms | p50 7.25 ms |
| `buildIssueFacetCounts(read, {}, "project")` (unscoped) | p50 12.29 ms | p50 12.63 ms |

The project facet result had 13 entries: 12 projects at 475 each and `{"key":null,"name":null,"count":300}` last, so the null group matches the scenario's 300.

Query-plan lines for hand-written equivalents of the two statements (the team plus label page, and the project group-by) showed `SEARCH issue_labels USING COVERING INDEX idx_issue_labels_label (label_id=?)` with a temp b-tree for the order, and `SEARCH issues USING INDEX idx_issues_team_order (<expr>=? AND removed_at=?)` with a temp b-tree for the group-by.

What these numbers do not include: the Worker `postMessage` round trip and structured clone of up to 100 hydrated `IssueView`s, OPFS SAHPool page-cache effects, a browser's CPU, and Worker-thread contention with an in-progress `applyChanges` (sqlite-wasm calls are synchronous on the one Worker thread). The label density in the scenario is unspecified; the table shows two densities. The scenario's 10 ms and 25 ms thresholds are therefore not yet measured end to end through the Worker.

### 8. Where the new ops would land and what releasing them involves

**Per-op touchpoints in the browser replica** (derived from how the three existing ops are wired; nothing was changed):

1. `src/replica/browser/protocol.ts`: request interface, `ReplicaRequest` union member, `ResultMap` entry, and type imports from read-model.
2. `src/replica/browser/worker-core.ts`: a `switch` case that awaits the read gate and calls the builder over `requireReplica().read`.
3. `src/replica/browser/browser-replica.ts`: a public method through `this.call`, and (the set is untyped) an entry in `SEED_GATED_WORKER_RPCS`.
4. Tests in `test/browser/worker-core.test.ts` (and `browser-replica.test.ts` for the client path).
5. `README.md` BrowserReplica section (`:174-240` mentions only `queryIssues`) and `CHANGELOG.md`.
6. Type surface: the `/browser` entry exports none of read-model's view or scope types today (see section 1).

**History**: all three existing query ops arrived together in `47d27ad` ("CTC-114 — './browser' entry ... (0.8.0) (#11)", 31 files), per the GitHub commits API. The local clone is shallow with one commit, so no per-op history exists locally. Later protocol changes (`1af3f0b` CTC-1097, `003aed7` 0.8.6) touched `protocol.ts`, `worker-core.ts` and `browser-replica.ts` plus tests.

**Release mechanics** (`.agents/rules/release-train.md`, `.agents/skills/release-train/SKILL.md`, `scripts/release.mjs`, `scripts/release-train-check.mjs`, `release-train.json`):

- `@catalyst-cloud/sdk`, `@catalyst-cloud/sdk-replica-node` and `@catalyst-cloud/sdk-replica-browser` share one version, now 0.16.23 (`package.json:3`, `modules/replica-node/package.json`, `modules/replica-browser/package.json`). Each module's SDK peer equals `^<version>`. `scripts/release.mjs` throws unless the three match.
- The rule says additive changes, such as a new method, are patches for that member alone. A MINOR moves for every member at once. `AGENTS.md` requires loading the `release-train` skill and running `train-status.mjs` before any bump.
- Prior feature releases changed `package.json`, both module `package.json` files, and `CHANGELOG.md`. For example `a293617` (0.16.23, ticket leases) also changed `README.md`, `src/index.ts` and `scripts/verify-pack.mjs`. Publishing happens by creating the GitHub Release `vX.Y.Z`, which triggers `.github/workflows/publish.yml`.
- `scripts/verify-pack.mjs` is a consumer smoke test. It bundles `BrowserReplica` and the db-worker subpath with esbuild but does not name any op. `release:check`, build, `typecheck`, `typecheck:scripts`, `vitest run` and `test:pack` run in CI.
- CHANGELOG entries are plain customer prose, newest first, with a "Requires a Catalyst Cloud server..." line when a feature needs the server and "Update the SDK and both replicas together." when relevant. The skill's wording rules say to write "workspace" not "tenant" and to leave out internal service names and people.
- The ticket says the SDK release was approved under CTC-3935 on 2026-09-28. That is not recorded in any file in this repository (no hits for CTC-3935), and the release train's recorded approval is CTC-5137 (`release-train.json`).

### 9. The catalyst-cloud side (third scenario)

The catalyst-cloud repository is not visible from this container (GitHub API returns 404 for `coalesce-labs/catalyst-cloud`). Nothing here shows the web's replica path, the `no-replica-op` tag, or the five package.json files. Within this repo, `no-replica-op` has no hits.

## Code References

Local references use commit `c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2`, which is `origin/main`.

- [`src/replica/browser/protocol.ts:81-99`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/protocol.ts#L81-L99) - the three read request interfaces
- [`src/replica/browser/protocol.ts:115-126`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/protocol.ts#L115-L126) - `ReplicaRequest` union
- [`src/replica/browser/protocol.ts:160-171`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/protocol.ts#L160-L171) - `ResultMap`
- [`src/replica/browser/worker-core.ts:196-217`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/worker-core.ts#L196-L217) - the read cases (no `scope` passed)
- [`src/replica/browser/seed-read-gate.ts:25-35`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/seed-read-gate.ts#L25-L35) - mid-seed read deferral
- [`src/replica/browser/browser-replica.ts:97-101`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/browser-replica.ts#L97-L101) - `SEED_GATED_WORKER_RPCS`
- [`src/replica/browser/browser-replica.ts:517-543`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/browser-replica.ts#L517-L543) - `call<K>`
- [`src/replica/browser/browser-replica.ts:956-968`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/browser-replica.ts#L956-L968) - `queryIssues`, `queryIssueDetail`, `queryPulls`
- [`src/replica/browser/browser-replica.ts:1007-1039`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/browser-replica.ts#L1007-L1039) - `withWorkerDeadline`
- [`src/replica/browser/ports.ts:108-118,178`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/browser/ports.ts#L108-L118) - `read` executor and `applyReadModelDdl(write)`
- [`src/replica/read-model-ddl.ts:39-49`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/read-model-ddl.ts#L39-L49) - `applyReadModelDdl`
- [`src/replica/catalyst-replica.ts:894-907`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/replica/catalyst-replica.ts#L894-L907) - Node `sql`, `issues`, `issue`, `pulls`
- [`src/browser.ts:44-51`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/browser.ts#L44-L51) - protocol type re-exports
- [`src/node.ts:80-91`](https://github.com/coalesce-labs/catalyst-cloud-sdk/blob/c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2/src/node.ts#L80-L91) - read-model type re-exports
- `test/browser/worker-core.test.ts:23-67` - `memoryCore`, `issueRow`, `seed`
- `test/browser/browser-replica.test.ts:32-90` - `FakeWorker`
- `test/helpers/read-model-plan.ts` - `recordStatements`, `planDetails`, `fullScans`
- `node_modules/@catalyst-cloud/read-model/src/issues.ts:983-1092` - `IssueScope`
- `node_modules/@catalyst-cloud/read-model/src/issues.ts:1132-1318` - `issueScopeClause`
- `node_modules/@catalyst-cloud/read-model/src/issues.ts:1533-1560` - `buildIssuesView`
- `node_modules/@catalyst-cloud/read-model/src/issues.ts:1602-1724` - `buildIssuesViewKeyset`
- `node_modules/@catalyst-cloud/read-model/src/issue-facets.ts:215-246` - `issueFacetGroupScope`
- `node_modules/@catalyst-cloud/read-model/src/issue-facets.ts:272-365` - `buildIssueFacetCounts`
- `node_modules/@catalyst-cloud/read-model/src/read-indexes.ts:79-82,144-168` - the issue indexes
- `node_modules/@catalyst-cloud/schema/src/mirror.ts:97-114` - `labels` table, `team_id` "nullable and ADVISORY"
- `release-train.json`, `scripts/release.mjs`, `scripts/release-train-check.mjs`, `.agents/rules/release-train.md` - release train

## Architecture Documentation

**Data flow for a read today**: web code calls `BrowserReplica.queryX(...)` on the main thread. `call` assigns an id, stores the promise in `pending`, and posts an `Envelope` (structured clone) to the dedicated Worker. `db.worker.ts` hands it to `createWorkerCore().handle`. A read case awaits `SeedReadGate.whenReadable(session !== null)`, then runs a read-model builder synchronously over the sqlite-wasm handle through the `read` `SqlExecutor`. The builder's plain-object result is posted back as `{ id, ok: true, result }`, and the client resolves the pending promise. Errors cross as `error: string`, and the client rebuilds an `Error`.

**One query, four runtimes**: the same read-model builders run in the Mirror Durable Object (over `SqlStorage`), the host-sync daemon (bun:sqlite), the Node replica (`CatalystReplica.sql`) and the browser Worker (sqlite-wasm). The browser's two SQL seams are `read` (a `SqlExecutor`) and `write` (a `ReplicaDb`), both over one OO1 `Database`. FTS5 is recorded in read-model as proven in all three SQL runtimes, including `@sqlite.org/sqlite-wasm` 3.53.0-build1 (`search-fts.ts:10-14`).

**Scope as the single filter**: `issueScopeClause` is the one place a scope becomes SQL. The page, `countIssues` and the facet counts all use it, so a facet group's count, its opened list and the total describe one set (`issues.ts:1125-1131`, `issue-facets.ts:1-3`).

**Packaging**: `@catalyst-cloud/read-model`, `replicate` and `schema` are optional peers of the core SDK. The SDK is a train member with its two replica modules, versioned together, released by GitHub Release.

## Historical Context (from thoughts/)

This checkout has no `thoughts/` directory and `humanlayer thoughts status` reports "Thoughts not configured", so no earlier research or plan documents could be read. The directory `thoughts/shared/research/` was created only to hold this document. History in this document comes from the live code, the installed packages, `CHANGELOG.md`, and the GitHub commits API for the shallow clone's missing history:

- `47d27ad` (CTC-114, 0.8.0, #11) introduced the browser replica and its three read ops together.
- CTC-3109 is the source of read-model's facet and text-in-scope builders (`// CTC-3109` comments in `issue-facets.ts`, `issues.ts`, `read-indexes.ts`, `index.ts:82`). The ticket describes its web half as catalyst-cloud #12761, with this ticket split out by ask CTC-6145.
- CTC-4324 added read-model's index set and FTS tables to both replicas (`read-model-ddl.ts` header).

## Open Questions

1. CTC-5078 and supersession. The ticket says to check whether the browser replica has retired (forecast 2026-10-29 to 30) before starting. I cannot read that ticket here. What I can see: as of 2026-11-06 the browser replica source is in this repo at HEAD, `modules/replica-browser` exists, and 0.16.23 is the npm `latest`.
2. Label by name or by id. Scenario 1 says "label 'mirror-do'", while `IssueScope.labelId` takes an id and no read-model builder resolves a name. `labels` has `name`, `team_id` (documented as nullable and advisory) and `removed_at`, so a name can in principle map to more than one label id. The label facet's `key` is the label id, so the web may already hold ids from facet counts. What the op's input should be is not settled by the ticket.
3. Which list builder `queryIssuesScoped` wraps and what it returns. `buildIssuesView` (offset page, `updated_at DESC`) is what `queryIssues` uses. `buildIssuesViewKeyset` (cursor page, `COALESCE(updated_at,0) DESC, id DESC`, optional `sort`) is the one the indexes are named for. `countIssues` gives the scope's total. The ticket names only the op, a "first page", and "newest first".
4. Facet result shape against the scenario wording. The project facet's null group is `{ key: null, name: null, count: 300 }`. Whether the op returns `IssueFacetCount[]` as-is or adds a display name for the null group is not stated.
5. How the latency figures are to be verified. The repo has no p50 or benchmark harness. My in-process numbers (section 7) are well under both thresholds, but they exclude the Worker round trip, OPFS, and a real browser. Which environment the ticket's "under 10 ms p50" and "under 25 ms p50" apply to is not stated, nor is the label's density.
6. Gate and deadline membership for new ops. `SEED_GATED_WORKER_RPCS` is a string set, so a new read op not added to it takes the flat wedge path. Each read case in `worker-core.ts` also has to await the read gate itself.
7. Type export surface. `/browser` re-exports no read-model view or scope types, and `/node` re-exports `IssueView` but not `IssueScope` or `IssueFacetCount`. How the web types the new ops' inputs and outputs (importing read-model directly, or through the SDK) is not recorded here.
8. Approval and version. The ticket cites CTC-3935 for the release approval; I found it only in the ticket text. Under `.agents/rules/release-train.md` an additive op is a patch (the next patch on the 0.16 line is 0.16.24), but the release skill must be loaded and `train-status.mjs` run before any bump, and I did not run it.
9. Scenario 3 depends on catalyst-cloud (five package.json files, bun.lock, the `no-replica-op` tag), which this container cannot see.
10. Text search inside a filtered list (`buildIssueTextInScope`) is a separate read-model capability with no SDK op and is outside the ticket's three scenarios.

## Related Documents

- No earlier thoughts documents were available in this checkout.
- Source ticket text: CTC-6150 (supplied to the run as `ticket.md`).
- Related tickets named in the ticket: CTC-3109 (web half, catalyst-cloud #12761), CTC-6145 (ask), CTC-5078 (browser replica retirement), CTC-3935 (SDK release approval).
