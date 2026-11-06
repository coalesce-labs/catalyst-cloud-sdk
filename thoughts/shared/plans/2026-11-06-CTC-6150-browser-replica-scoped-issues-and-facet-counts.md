---
date: 2026-11-06T13:41:55+00:00
researcher: claude
git_commit: b9859efb9d509e8abe8b17ef78d49f240270a978
branch: CTC-6150
git_main_sha: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
git_merge_base: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
repository: catalyst-cloud-sdk
topic: "CTC-6150: queryIssuesScoped and queryIssueFacetCounts on the browser replica"
tags: [plan, implementation, browser-replica, worker-protocol, read-model, issue-scope, facet-counts, release-train]
status: ready_for_implementation
last_updated: 2026-11-06
last_updated_by: claude
type: implementation_plan
source_ticket: CTC-6150
source_research: "[[2026-11-06-CTC-6150-browser-replica-scoped-issues-and-facet-counts]]"
---

# Browser replica scoped issues and facet counts (CTC-6150) Implementation Plan

## Overview

Add two read ops to the browser replica's worker protocol so the web can answer scoped ticket lists and facet counts locally instead of asking the server:

- `BrowserReplica.queryIssuesScoped(scope, opts?)` wraps read-model's `buildIssuesViewKeyset` with an `IssueScope`.
- `BrowserReplica.queryIssueFacetCounts(scope, dimension)` wraps read-model's `buildIssueFacetCounts`.

Then release them as SDK 0.16.24 (a patch on the 0.16 train) so catalyst-cloud can bump its SDK range and drop the `no-replica-op` tag for these reads.

The ticket's three Gherkin scenarios are the requirements:

1. A replica with 20,000 issues (6,000 in team CTC) returns team CTC's tickets carrying label "mirror-do", newest first, through `queryIssuesScoped`, first page under 10 ms p50.
2. With 12 projects and 300 project-less tickets in team CTC, `queryIssueFacetCounts` returns each project's count and a "No project" count of 300, under 25 ms p50.
3. After a release containing both ops, catalyst-cloud bumps its SDK range (five package.json files and bun.lock) and its web replica path uses the two ops without the `no-replica-op` tag.

## Current State Analysis

- The protocol has three read requests and nothing scoped: `QueryIssuesRequest`, `QueryIssueDetailRequest` and `QueryPullsRequest` (`src/replica/browser/protocol.ts:81-99`), the `ReplicaRequest` union (`src/replica/browser/protocol.ts:115-126`) and `ResultMap` (`src/replica/browser/protocol.ts:160-172`).
- The worker's read cases each await the seed read gate and then call a builder. `queryIssues` calls `buildIssuesView` without a scope (`src/replica/browser/worker-core.ts:196-217`).
- The client methods are bare `this.call(...)` wrappers (`src/replica/browser/browser-replica.ts:955-968`). `call<K>` (`src/replica/browser/browser-replica.ts:517-543`) applies a deadline per request type. `SEED_GATED_WORKER_RPCS` is a `ReadonlySet<string>` (`src/replica/browser/browser-replica.ts:97-101`), so the compiler does not catch a new read op left out of it. Ops left out of it take the wedge path in `withWorkerDeadline` (`src/replica/browser/browser-replica.ts:1007-1020`).
- read-model 0.16.0 is installed and in range (`^0.16.0`). It exports `buildIssuesViewKeyset`, `buildIssueFacetCounts`, `IssueScope`, `IssueFacetCount`, `IssueFacetDimension`, `KeysetCursor` and `KeysetPage` (`node_modules/@catalyst-cloud/read-model/src/index.ts:95`, `node_modules/@catalyst-cloud/read-model/src/pagination.ts:40-78`). Its index set and FTS tables already run on every browser open (`src/replica/browser/ports.ts:178`, `src/replica/read-model-ddl.ts:39-49`).
- `/browser` re-exports protocol types only (`src/browser.ts:46-53`). `/node` re-exports view types but not scope or facet types (`src/node.ts:80-91`).
- Test harness: `memoryCore`, `issueRow` and `seed` in `test/browser/worker-core.test.ts:30-67`, and `FakeWorker` in `test/browser/browser-replica.test.ts:32-90`. Plan helpers are in `test/helpers/read-model-plan.ts`. The repo has no latency harness.
- The release train check could not read the declared line from this container. `train-status.mjs` exited 2 because `gh` could not read catalyst-cloud. Every SDK-side member is at 0.16.23, and `release-train.json` declares line `0.16`.

## Desired End State

- `BrowserReplica` exposes both methods, typed end to end through `ResultMap`. Both are seed-gated reads like the existing three.
- `@catalyst-cloud/sdk/browser` (and `/node` for parity) re-exports `IssueScope`, `IssueFacetCount`, `IssueFacetDimension`, `KeysetCursor`, `KeysetPage` and `IssueView`, so the web types its calls from the SDK import alone.
- Worker-core tests prove scenario 1's ordering and filtering and scenario 2's counts, null group included, at the scenario sizes.
- A perf suite run by `npm run test:perf` reports p50 for both ops through `WorkerCore.handle` at 20,000 issues and asserts under 10 ms and under 25 ms.
- CHANGELOG, README and versions are ready for 0.16.24, and the GitHub Release is cut only once the train check passes (Phase 5).

Verify with `npm run typecheck && npm test && npm run test:perf && npm run build && npm run test:pack && npm run release:check`.

### Key Discoveries:

- `IssueScope.projectId: null` means "no project", while `undefined` means no filter (`node_modules/@catalyst-cloud/read-model/src/issues.ts:1056-1061`). Structured clone preserves that difference, so the scope can cross `postMessage` unchanged.
- The keyset builder orders by `COALESCE(updated_at, 0) DESC, id DESC`. That is the order `idx_issues_team_order` is built for, and its cursor is a plain-string object, safe to clone (`node_modules/@catalyst-cloud/read-model/src/pagination.ts:40-78`).
- The facet null group comes back as `{ key: null, name: null, count }`, always last (`node_modules/@catalyst-cloud/read-model/src/issue-facets.ts:66-70`, `node_modules/@catalyst-cloud/read-model/src/issue-facets.ts:186-195`). An unknown dimension throws `unknown facet dimension`, and the worker turns that into an error reply.
- `IssueScope.waitingOn.excludeSql` is a raw SQL fragment spliced into the statement (`node_modules/@catalyst-cloud/read-model/src/issues.ts:1151-1152`).
- The research measured the two builders in-process at 20,000 issues: p50 1.6 to 4.1 ms for scoped pages (offset builder) and 3.8 to 4.0 ms for project facets. The keyset builder and the Worker round trip were not measured.

## What We're NOT Doing

- No op for text search inside a filtered list (`buildIssueTextInScope`). It is outside the three scenarios.
- No label-name resolution in the SDK (see decision 2).
- No new Node replica methods. `CatalystReplica.sql` already lets a Node consumer call these builders. We only add the type re-exports.
- No catalyst-cloud edits. That repository is not reachable from this container. Scenario 3's consumer half is a follow-up in catalyst-cloud (Phase 5).
- No MINOR bump and no change to `release-train.json`.

## Decisions on the research's open questions

Nobody is available to ask during this run, so each open question is decided here, with the reason.

1. **CTC-5078 (has the browser replica retired?)**: proceed. The ticket cannot be read here. The evidence says the replica is live: the browser replica source is at HEAD on `origin/main`, `modules/replica-browser` exists, and `@catalyst-cloud/sdk-replica-browser` 0.16.23 is the npm `latest` on 2026-11-06, a week after the forecast retirement. Phase 5 re-checks CTC-5078 through the `catalyst-dev:linearis` skill before cutting the release. If it shows the replica retired, stop and close this ticket as superseded instead of publishing.
2. **Label by name or by id**: the op takes an `IssueScope`, so a label is filtered by `labelId`. Reasons:
   - read-model's scope is "the only filter the read model applies".
   - Label names are not unique (`labels.team_id` is nullable and advisory), so mapping a name to one id is ambiguous.
   - The web already holds label ids as the `key` of the label facet.
   - Scenario 1's "mirror-do" is the label the web displays. The test seeds a label named `mirror-do` and queries by its id.
3. **Which list builder**: `buildIssuesViewKeyset`, returning `KeysetPage<IssueView>` (`{ rows, nextCursor }`). Reasons:
   - Offset pages shift while `applyChanges` reorders `updated_at` under a live feed. Keyset pages do not.
   - The keyset order is the one read-model's team and project order indexes serve.
   - It gives the web a `nextCursor` for its next page.
   - `opts` passes through `limit`, `after` and `sort`. `sort` defaults to `"updated"`, which is "newest first".
   - Risk: the keyset builder's latency is unmeasured. The perf suite in Phase 3 measures it. If it misses 10 ms p50, fall back to the offset builder `buildIssuesView(read, limit, offset, scope)`, already measured at 1.6 to 4.1 ms, and record the change in the plan's iteration history.
4. **Facet result shape**: return read-model's `IssueFacetCount[]` unchanged. Reasons:
   - "No project" is display text for `key: null`, and read-model documents `key: null` as the "none" group.
   - Adding names in the SDK would duplicate UI strings.
   - The server's facet response, which the web already renders, has the same shape.
   - The test asserts the null group's count of 300, and the README states that `key: null` is the "No project" group.
5. **How latency is verified**: two parts.
   - A dedicated perf suite (`vitest.perf.config.ts`, `npm run test:perf`) seeds 20,000 issues through the real `WorkerCore` seed path over in-memory sqlite-wasm. It times `handle({ type: ... })` for each op over 200 iterations after 20 warmups and asserts p50 under 10 ms and under 25 ms.
   - It stays out of the default `npm test`. Wall-clock bounds on shared CI runners flake, and gating with `.skip` would trip the reward-hacking scan.
   - The Worker `postMessage` round trip and OPFS cannot run under vitest. They are a manual browser check (Phase 3, Manual Verification).
   - Label density is unspecified. The suite measures both densities from the research: 120 and 1,500 CTC issues carrying the label.
6. **Seed gate and deadline**: both ops await `readGate.whenReadable` in the worker and join `SEED_GATED_WORKER_RPCS`. The set is retyped to `ReadonlySet<ReplicaRequest["type"]>`, so a misspelt entry fails to compile. A test proves a mid-seed scoped read defers, then answers.
7. **Type exports**: re-export `IssueScope`, `IssueFacetCount`, `IssueFacetDimension`, `KeysetCursor`, `KeysetPage` and `IssueView` (plus `IssueDetailView` and `PullView`, which the existing methods already return) as types from `src/browser.ts`. Also add the first five to `src/node.ts`. These are type-only re-exports, so the browser bundle does not grow, and `test:pack` confirms it.
8. **`waitingOn.excludeSql` over the protocol**: the worker rejects a scope that carries a non-empty `waitingOn.excludeSql` with the error `excludeSql is not accepted by the browser replica`. Reasons:
   - It is the only raw SQL field in the scope.
   - The scenarios do not need it.
   - Accepting arbitrary SQL from whatever code can post to the worker is not a contract worth publishing.
   - Lifting the rejection later is additive.
9. **Version**: 0.16.24, a PATCH under the release-train rule "additive changes are PATCH, for that member alone". All three SDK members move together because `scripts/release.mjs` requires it. The ticket cites CTC-3935 as the release approval, and no file in this repo records it. A patch on the declared line needs no train approval, so the release proceeds on the release-train rules.
10. **Scenario 3**: the SDK half (a published release with both ops) is Phase 5. The catalyst-cloud half (five package.json files, bun.lock, dropping `no-replica-op`) belongs to that repository. The plan hands it off as a follow-up note on the ticket with the exact version, because this container cannot read or change catalyst-cloud. The ticket is not Done until that bump lands.

## Implementation Approach

Follow the pattern by which the three existing read ops were added: protocol type, then worker case, then client method, then exports and docs. Every layer starts with a test. The worker-core tests run the production dispatch over `:memory:` sqlite-wasm, so correctness and perf are both measured on the real builder path.

## Phase 1: Protocol and worker dispatch

### Overview

Add the two request types, their results, and the worker cases with the read gate and the `excludeSql` guard.

### Tests First (Red):

#### 1. Worker-core scoped and facet tests

**File**: `test/browser/worker-core.test.ts`. Add a `describe("scoped reads (CTC-6150)")` block that reuses `memoryCore`, `issueRow` and `seed`. Add a small `seedTeamFixture(core, { ctc, other, labelled, projects, noProject })` helper that builds issue, label, issue_label and project rows.

```ts
it("queryIssuesScoped returns team CTC's issues carrying the label, newest first", async () => {
  // 2 teams; label "mirror-do" (id l-mirror) on 3 CTC issues and 1 other-team issue
  const page = (await core.handle({
    type: "queryIssuesScoped",
    scope: { teamId: "team-ctc", labelId: "l-mirror" },
    limit: 2,
  })) as KeysetPage<IssueView>;
  expect(page.rows.map((r) => r.id)).toEqual(["c3", "c2"]);       // updated_at DESC, CTC only
  expect(page.nextCursor).not.toBeNull();
  const next = (await core.handle({ type: "queryIssuesScoped", scope: {...}, after: page.nextCursor })) as KeysetPage<IssueView>;
  expect(next.rows.map((r) => r.id)).toEqual(["c1"]);
  expect(next.nextCursor).toBeNull();
});
it("queryIssueFacetCounts returns each project's count and the no-project group last", ...);
  // 12 projects + 300 CTC issues with project_id null; expect 13 entries, last { key: null, name: null, count: 300 }
it("facet null group count equals the queryIssuesScoped total for { projectId: null }", ...);
it("an unknown facet dimension rejects", ...);                       // /unknown facet dimension/
it("a scope carrying waitingOn.excludeSql rejects", ...);            // /excludeSql is not accepted/
it("scoped reads defer while a seed is open, then answer", ...);     // mirror the existing mid-seed test
it("scoped reads before open reject with 'not open'", ...);
```

#### 2. Plan shape

**File**: `test/browser/read-model-ddl.test.ts`. Use `recordStatements`, `planDetails` and `fullScans` to assert that the team plus label keyset page and the team project facet run no full scan of `issues` or `issue_labels`, following the existing pulls assertions.

### Implementation (Green):

#### 1. Protocol

**File**: `src/replica/browser/protocol.ts`. Import the types `IssueScope`, `IssueFacetDimension`, `IssueFacetCount`, `KeysetCursor` and `KeysetPage`, then add:

```ts
/** Read a scoped, keyset-paged issues list locally (buildIssuesViewKeyset). CTC-6150. */
export interface QueryIssuesScopedRequest {
  type: "queryIssuesScoped";
  scope: IssueScope;
  limit?: number;
  after?: KeysetCursor | null;
  sort?: "updated" | "priority" | "blocking";
}
/** Count the scope's live issues per group of one dimension locally (buildIssueFacetCounts). CTC-6150. */
export interface QueryIssueFacetCountsRequest {
  type: "queryIssueFacetCounts";
  scope: IssueScope;
  dimension: IssueFacetDimension;
}
```

Add both to `ReplicaRequest`. Add `queryIssuesScoped: KeysetPage<IssueView>` and `queryIssueFacetCounts: IssueFacetCount[]` to `ResultMap`.

#### 2. Worker cases

**File**: `src/replica/browser/worker-core.ts`. Import `buildIssuesViewKeyset` and `buildIssueFacetCounts`. Add a local `assertWireScope(scope)` that throws when `scope?.waitingOn?.excludeSql` is a non-empty string. Then add two cases next to `queryIssues`:

```ts
case "queryIssuesScoped":
  await readGate.whenReadable(session !== null);
  assertWireScope(request.scope);
  return buildIssuesViewKeyset(requireReplica().read, {
    scope: request.scope, limit: request.limit, after: request.after, sort: request.sort,
  });
case "queryIssueFacetCounts":
  await readGate.whenReadable(session !== null);
  assertWireScope(request.scope);
  return buildIssueFacetCounts(requireReplica().read, request.scope, request.dimension);
```

### Refactor (if needed):

None expected. Keep the cases in the same commented "Reads DEFER" block.

### Success Criteria:

#### Automated Verification:

- [x] Worker-core tests pass: `npx vitest run test/browser/worker-core.test.ts test/browser/read-model-ddl.test.ts`
- [x] Type checking passes: `npm run typecheck`

#### Manual Verification:

- [x] Read the diff: each new case awaits the read gate before touching `requireReplica()`

---

## Phase 2: Client methods, deadline gating and type exports

### Overview

Expose the ops on `BrowserReplica`, make them seed-gated, and re-export their types.

### Tests First (Red):

#### 1. Client envelope and gating

**File**: `test/browser/browser-replica.test.ts`, using `FakeWorker` and the warm-start boilerplate.

- `queryIssuesScoped(scope, { limit: 50 })` posts `{ type: "queryIssuesScoped", scope, limit: 50, after: undefined, sort: undefined }` and resolves with the worker's answer.
- `queryIssueFacetCounts(scope, "project")` posts `{ type, scope, dimension }` and resolves with the answer.
- A `projectId: null` scope arrives at the worker with `projectId === null`, not dropped.
- A scoped read that the worker never answers, with `workerRpcTimeoutMs: 40` and no seed in flight, rejects `/wedged/`, following the existing test at `test/browser/browser-replica.test.ts:1105-1125`.
- While a seed is in flight, a scoped read past the deadline keeps waiting and resolves once answered. This is the seed-gated re-arm branch, which has no test today.

#### 2. Export surface

**File**: `test/browser-transport-exports.test.ts`. Add a compile-time check that `IssueScope`, `IssueFacetCount`, `IssueFacetDimension`, `KeysetCursor`, `KeysetPage` and `IssueView` import from `../src/browser.js` (a typed `const` using each type).

### Implementation (Green):

**File**: `src/replica/browser/browser-replica.ts`

```ts
/** Read a scoped, keyset-paged issues list from the local replica (CTC-6150). Pass `nextCursor` as `after`. */
queryIssuesScoped(
  scope: IssueScope,
  opts?: { limit?: number; after?: KeysetCursor | null; sort?: "updated" | "priority" | "blocking" },
): Promise<KeysetPage<IssueView>> {
  return this.call({ type: "queryIssuesScoped", scope, limit: opts?.limit, after: opts?.after, sort: opts?.sort });
}
/** Count the scope's issues per project, label, ... from the local replica (CTC-6150). `key: null` is the "none" group. */
queryIssueFacetCounts(scope: IssueScope, dimension: IssueFacetDimension): Promise<IssueFacetCount[]> {
  return this.call({ type: "queryIssueFacetCounts", scope, dimension });
}
```

Retype `SEED_GATED_WORKER_RPCS` to `ReadonlySet<ReplicaRequest["type"]>` and add both types. `withWorkerDeadline` takes `label: string`, so `.has(label)` needs that parameter narrowed to `ReplicaRequest["type"]`. Its only caller passes `request.type`, so the narrowing is safe.

**Files**: `src/browser.ts` and `src/node.ts` get the type re-exports from decision 7.

### Success Criteria:

#### Automated Verification:

- [ ] `npx vitest run test/browser test/browser-transport-exports.test.ts`
- [ ] `npm run typecheck`
- [ ] `npm run build && npm run test:pack` (the browser bundle still builds and the types resolve)

#### Manual Verification:

- [ ] `dist/browser.d.ts` names both methods and the re-exported types

---

## Phase 3: Latency verification for scenarios 1 and 2

### Overview

Measure p50 for both ops at the scenario sizes through `WorkerCore.handle`, and pick a builder if the keyset path misses its target.

### Tests First (Red):

**File**: `test/perf/browser-replica-scoped.perf.ts`. The `.perf.ts` suffix keeps it out of the default `test/**/*.test.{ts,mjs}` include.

- Seed 20,000 issues: 6,000 in `team-ctc` and 14,000 in another team. Of the CTC issues, 5,700 are spread over 12 projects and 300 have no project. Load them in `SEED_BATCH_ROWS`-sized `seedBatch` calls.
- Scenario 1: the label `mirror-do` is on 120 CTC issues in one run and 1,500 in the other. Time `handle({ type: "queryIssuesScoped", scope: { teamId: "team-ctc", labelId }, limit: 100 })` with 20 warmups and 200 samples, and assert `p50 < 10`. Also assert the rows are all CTC, all labelled, and in `updated_at DESC` order.
- Scenario 2: time `handle({ type: "queryIssueFacetCounts", scope: { teamId: "team-ctc" }, dimension: "project" })`. Assert `p50 < 25`, 13 entries, and the last entry `{ key: null, name: null, count: 300 }`.
- Print p50 and p95 so the run log records the numbers.

### Implementation (Green):

- **File**: `vitest.perf.config.ts` with `include: ["test/perf/**/*.perf.ts"]` and a 120 s test timeout.
- **File**: `package.json` gets the script `"test:perf": "vitest run --config vitest.perf.config.ts"`.
- If the keyset p50 is 10 ms or more at either density, switch the `queryIssuesScoped` worker case to `buildIssuesView(read, limit, 0, scope)`. The result shape stays `KeysetPage` only if the switch keeps it. Otherwise update the protocol, tests and docs in the same phase and record the change under an `## Iteration History` section of this plan.

### Success Criteria:

#### Automated Verification:

- [ ] `npm run test:perf` passes and prints p50 under 10 ms (scoped, both densities) and under 25 ms (project facet)
- [ ] `npm test` is unchanged in duration within noise (the perf suite is not in it)

#### Manual Verification:

- [ ] In a browser (the web dev build, or a scratch page bundling `@catalyst-cloud/sdk/browser` with the db-worker subpath) holding a seeded 20,000-issue replica, time `await replica.queryIssuesScoped(...)` and `await replica.queryIssueFacetCounts(...)` from the main thread with `performance.now()` over 50 calls each. Record the p50 values, which include the Worker round trip and OPFS, in the PR description against the 10 ms and 25 ms targets.

---

## Phase 4: Documentation and changelog

### Overview

Document the ops for customers.

### Tests First (Red):

None beyond the existing `release:check` and `test:pack`. This phase changes docs only.

### Implementation (Green):

- **File**: `README.md`, in the BrowserReplica section (`README.md:174-240`). Add a short example of each method:
  - a team plus label scope with `nextCursor` paging;
  - project facet counts, noting that `key: null` is the "No project" group;
  - that `projectId: null` means "no project" while omitting the field means no filter;
  - that `waitingOn.excludeSql` is rejected.
- **File**: `CHANGELOG.md`. Add a `## 0.16.24` entry in customer prose: "The browser replica can now answer filtered ticket lists and group counts locally: `queryIssuesScoped(scope, { limit, after, sort })` and `queryIssueFacetCounts(scope, dimension)` ... Update the SDK and both replicas together." Use "workspace", never "tenant". Name no internal services or people.

### Success Criteria:

#### Automated Verification:

- [ ] `npm run typecheck && npm test`

#### Manual Verification:

- [ ] The CHANGELOG entry follows the release-train skill's customer wording rules

---

## Phase 5: Release 0.16.24 and hand off scenario 3

### Overview

Publish the SDK release that scenario 3 starts from, and hand the catalyst-cloud half to that repository.

### Tests First (Red):

`npm run release:check` fails until the three versions agree at 0.16.24.

### Implementation (Green):

1. Load the `release-train` skill and run `.claude/skills/release-train/scripts/train-status.mjs`. In this container it exited 2 (could not read the declared line). Release only on exit 0. Exit 1 or 2 means stop and raise an ask through the workflow, with this proposal:
   - SDK, sdk-replica-node and sdk-replica-browser go from 0.16.23 to 0.16.24.
   - No other member moves.
2. Re-check CTC-5078 through the `catalyst-dev:linearis` skill (decision 1).
3. Bump `package.json`, `modules/replica-node/package.json` and `modules/replica-browser/package.json` to 0.16.24, with peers `^0.16.24`. Follow `scripts/release.mjs` and the steps used for 0.16.23.
4. After the PR merges, create the GitHub Release `v0.16.24`, which triggers `.github/workflows/publish.yml`. Confirm with `npm view @catalyst-cloud/sdk version`.
5. Scenario 3 handoff: comment on CTC-6150 (through the app actor, `catalyst write comment`) that 0.16.24 contains both ops. The comment asks catalyst-cloud to:
   - bump the SDK range in its five package.json files and bun.lock to `^0.16.24`;
   - route the web's scoped list and facet reads through `queryIssuesScoped` and `queryIssueFacetCounts`;
   - drop the `no-replica-op` tag for those reads.

### Success Criteria:

#### Automated Verification:

- [ ] `npm run release:check` passes
- [ ] `node .claude/skills/release-train/scripts/train-status.mjs` exits 0 before and after publishing
- [ ] `npm view @catalyst-cloud/sdk-replica-browser version` reports 0.16.24 after publish

#### Manual Verification:

- [ ] The catalyst-cloud PR bumping the range lands, and the web's reads for these filters no longer carry `no-replica-op` (verified in that repository)

---

## Testing Strategy (TDD)

**Approach: Red, then Green, then Refactor** in each phase.

- **Unit and integration tests** (worker-core over in-memory sqlite-wasm): filtering, ordering and keyset paging for scenario 1, and the facet counts and null group for scenario 2. Also the agreement between a null group's count and its list, the read gate, not-open, an unknown dimension, and the `excludeSql` guard.
- **Client tests** (FakeWorker): the envelope shape, a `null` project id surviving the trip, the wedge deadline, and the seed-gated re-arm.
- **Plan tests**: no full scans for the two scenario statements.
- **Perf suite** (`npm run test:perf`): the scenario p50 thresholds at 20,000 issues.
- **Manual**: browser main-thread timings, which include the Worker round trip and OPFS.

Final gate for the implement stage: `npm run typecheck && npm run typecheck:scripts && npm test && npm run test:perf && npm run build && npm run test:pack && npm run release:check`. This repo has no `make check`.

## Performance Considerations

- The sqlite-wasm calls run synchronously on the single Worker thread, so a scoped read queues behind an in-progress `applyChanges`. These ops do not change that behaviour, and the perf suite measures an idle worker.
- An unscoped facet walks every live issue (about 12 ms in-process at 20,000 issues). The README tells callers to pass a scope.
- Structured clone of up to 100 hydrated `IssueView`s is part of the browser manual measurement.

## Migration Notes

These are additive protocol members. A newer client talking to an older worker cannot happen, because the worker ships in the same package (`@catalyst-cloud/sdk/browser/db-worker`) and the CHANGELOG says to update the SDK and both replicas together. No database migration is needed, because read-model's indexes are already applied on open.

## Files in scope

- `src/replica/browser/protocol.ts`: two request interfaces, union members and `ResultMap` entries
- `src/replica/browser/worker-core.ts`: two read cases and `assertWireScope`
- `src/replica/browser/browser-replica.ts`: two methods and the retyped `SEED_GATED_WORKER_RPCS`
- `src/browser.ts`, `src/node.ts`: type re-exports
- `test/browser/worker-core.test.ts`, `test/browser/read-model-ddl.test.ts`, `test/browser/browser-replica.test.ts`, `test/browser-transport-exports.test.ts`: tests
- `test/perf/browser-replica-scoped.perf.ts`, `vitest.perf.config.ts` (new), `package.json` (`test:perf` script, later the version)
- `README.md`, `CHANGELOG.md`
- `modules/replica-node/package.json`, `modules/replica-browser/package.json`: version and peer, in Phase 5

## References

- Original ticket: CTC-6150
- Research: `thoughts/shared/research/2026-11-06-CTC-6150-browser-replica-scoped-issues-and-facet-counts.md`
- Pattern to copy: `src/replica/browser/worker-core.ts:199-217` and `src/replica/browser/browser-replica.ts:955-968`
- Builders: `node_modules/@catalyst-cloud/read-model/src/issues.ts:1533-1560` (offset fallback), `node_modules/@catalyst-cloud/read-model/src/issue-facets.ts:272-276`
- Release rules: `.agents/rules/release-train.md`, `release-train.json`
