---
date: 2026-11-06
source_ticket: CTC-6150
repository: catalyst-cloud-sdk
plan: thoughts/shared/plans/2026-11-06-CTC-6150-browser-replica-scoped-issues-and-facet-counts.md
pr: coalesce-labs/catalyst-cloud-sdk#238
status: FAIL
git_commit: c61bc5ed4f1807d188ab30792e078dd03dc7090a
branch: CTC-6150
git_main_sha: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
git_merge_base: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
---

# Validation Report: Browser replica scoped issues and facet counts

**Plan**: `thoughts/shared/plans/2026-11-06-CTC-6150-browser-replica-scoped-issues-and-facet-counts.md`
**Validated**: 2026-11-06 (fresh run on head c61bc5e; every command below was re-run, the earlier report was not trusted)
**Validation Status**: FAIL

## Validation Source

- `git_commit`: c61bc5ed4f1807d188ab30792e078dd03dc7090a
- `branch`: CTC-6150
- `git_main_sha`: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
- `git_merge_base`: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2

Main has not moved since the plan (plan merge base equals current main), so there is no main movement to report. Commits after the code head (`55dd76b`) only touch thoughts documents.

## Scenarios

- met: The browser replica answers a scoped ticket query
- met: The browser replica returns facet counts for a filtered list
- missed: catalyst-cloud picks up the release: no release containing both ops exists (`package.json` version reads 0.16.23, newest tag is v0.16.23), and the catalyst-cloud bump and `no-replica-op` removal cannot be read here; no decision by the ticket or its owner removed it from this ticket

## Scenario evidence

1. Scoped query. `src/replica/browser/worker-core.ts:224` adds the `queryIssuesScoped` case over `buildIssuesViewKeyset` (default sort "updated", newest first); `BrowserReplica.queryIssuesScoped` exposes it. Worker-core tests cover team plus label filtering, order and keyset paging. The perf suite (20,000 issues, 6,000 in team CTC) passed with p50 under 10 ms at both label densities (this run's facet figure was 4.96 ms p50; scoped figures are in the test output, all under the bound). The label is filtered by id, not name (plan decision 2); the test seeds a label named `mirror-do` and queries by its id. Browser-with-OPFS timings in the plan were not reproduced here (no browser).
2. Facet counts. `queryIssueFacetCounts` wraps `buildIssueFacetCounts`. The perf suite asserts 13 entries, last `{ key: null, name: null, count: 300 }`, and p50 under 25 ms: 4.96 ms p50, 8.21 ms p95 this run. "No project" is `key: null` (plan decision 4).
3. Pickup. `missed`, not `narrowed`: only plan decision 10 moves the catalyst-cloud half out of this ticket, and that is the plan author's choice, not a decision by the ticket or its owner. Not `unread` alone: the Given (a release containing both ops) is this repository's job and does not hold. `CHANGELOG.md` has a `## 0.16.24` heading that no release backs. The When and Then happen in catalyst-cloud, which this container cannot read.

## Implementation Status

- Phase 1 protocol and worker dispatch: implemented; the `excludeSql` guard has a bypass (finding 1).
- Phase 2 client methods, retyped `SEED_GATED_WORKER_RPCS`, type re-exports (`src/browser.ts`, `src/node.ts`): implemented as planned.
- Phase 3 perf suite, `vitest.perf.config.ts`, `test:perf` script: implemented, thresholds met.
- Phase 4 README and CHANGELOG: done.
- Phase 5 release 0.16.24: not done. Its three Automated Verification boxes are unchecked in the plan; version files unchanged; CTC-5078 not re-checked.

### Deviations (`thoughts/shared/plans/CTC-6150/deviations.json`)

- Entry 1 (Phase 5 step 3, version bump held after train-status exit 2): the tree matches the declared actual (three package.json files at 0.16.23). Declared. I could not re-run train-status against catalyst-cloud here.
- Entry 2 (CTC-5078 not re-checked): matches. Declared.
- No undeclared deviation found in the diff.

A matching declaration does not make the phase conformant. The plan promised the 0.16.24 versions, a release and a passing release check at that state; scenario 3 depends on it. The deviation changes what the plan promised, so it is material.

## Automated Verification Results

Run from the repository root on this head.

- `npm run typecheck`: exit 0
- `npm run typecheck:scripts`: exit 0
- `npm test`: exit 0, 59 files, 904 tests passed
- `npm run test:perf`: exit 0, 6 tests passed; facet p50 4.96 ms (bound 25 ms); scoped tests passed their 10 ms p50 bound
- `npm run build`: exit 0
- `npm run test:pack`: exit 0 (`consumerTypes`, `optionalModules`, `browserAndWorkerBundles` all passed)
- `npm run release:check`: exit 0, 8 of 8 pass (at 0.16.23, so it does not prove the 0.16.24 state)
- Lint: none configured. Dependency audit: none configured; the diff adds no dependency.

### validate-type-safety

- Step 1 type check: PASS. Step 3 test inclusion: PASS (`tsconfig.json` includes `src` and `test`). Step 4 tests: PASS. Step 5 lint: SKIPPED.
- Step 2 reward-hacking scan against base c20b763: FAIL. Three added `as unknown as` casts with no explanatory comment (pattern 1, HIGH):
  - `test/browser/browser-replica.test.ts:1293` `worker as unknown as Worker`
  - `test/browser/browser-replica.test.ts:1426` `worker as unknown as Worker`
  - `test/perf/browser-replica-scoped.perf.ts:98` `new sqlite3.oo1.DB(":memory:", "c") as unknown as Database`
  - Mitigation, not an exemption: each repeats a pattern that already appears dozens of times in those test files at the base, and wraps a test double, not production code.
  - No `as any`, ts-ignore, non-null assertion, skip or only on added lines; no tsconfig diff. The `.skip` match at `test/perf/browser-replica-scoped.perf.ts:14` is a comment.

## Code Review Findings

- Matches plan: protocol types, worker cases behind the seed read gate, client methods, retyped gate set, type re-exports, docs, perf suite.
- Deviations: Phase 5 held (declared, material).
- Potential issues:

1. `src/replica/browser/worker-core.ts:54` (MEDIUM, correctness and security, confidence 90). `assertWireScope` refuses `excludeSql` only when `typeof excludeSql === "string"`. read-model splices it on a truthiness test into a template literal (`node_modules/@catalyst-cloud/read-model/src/issues.ts:1151`), so an array survives structured clone, passes the guard and reaches the SQL. Reproduced this run with a temporary test (removed): `excludeSql: ["no_such_col_x = 1"]` through `queryIssuesScoped` failed with `SQLITE_ERROR ... no such column: no_such_col_x`. Plan decision 8, the README and the CHANGELOG promise a non-empty `excludeSql` is rejected; the tests (`test/browser/worker-core.test.ts:803`) only try the string `"1 = 1"`. Impact is bounded (local replica, one statement, caller already runs in the page). Fix: reject any value other than `undefined`, `null` or `""` and add an array and number test.

### review-code

Verdict FAIL: finding 1 only. AGENTS.md rules are workflow rules and none scopes to these files. The PR has no review threads.

### review-security

Verdict FAIL: finding 1 as a MEDIUM SQL injection (guard bypass), confidence 85. Input is any message posted to the db-worker with a non-string truthy `excludeSql`; sink is the splice in read-model's scope clause used by both new ops. No other issue found.

## Manual Testing Required

- [ ] Run `node .agents/skills/release-train/scripts/train-status.mjs` where catalyst-cloud is readable; on exit 0 bump the three package.json files to 0.16.24 (peers `^0.16.24`).
- [ ] Re-check CTC-5078 (replica retired means close as superseded).
- [ ] After merge, create GitHub Release v0.16.24 and confirm `npm view @catalyst-cloud/sdk version`.
- [ ] In catalyst-cloud, bump the SDK range in the five package.json files and bun.lock, route the reads through the two ops, drop `no-replica-op`.

## Recommendations

- Fix the `excludeSql` guard at `src/replica/browser/worker-core.ts:54` and add the array test.
- Comment or remove the three `as unknown as` casts.
- Finish Phase 5 and the catalyst-cloud pickup before marking CTC-6150 Done.

```catalyst-validation-ladder
{"version":1,"steps":[
  {"step":"plan-conformance","verdict":"FAIL","materiality":"material","declared":true,"detail":"Phases 1-4 built; Phase 5 (0.16.24 bump, release, CTC-5078 re-check) not delivered. deviations.json entries match the tree (declared) but change what the plan promised. Scenario 3 missed."},
  {"step":"type-safety","verdict":"FAIL","detail":"validate-type-safety: typecheck exit 0, 904 tests pass, lint SKIPPED. scan-reward-hacking FAIL: 3 undocumented 'as unknown as' on added test lines (HIGH).","findings":[{"path":"test/browser/browser-replica.test.ts","line":1293},{"path":"test/browser/browser-replica.test.ts","line":1426},{"path":"test/perf/browser-replica-scoped.perf.ts","line":98}]},
  {"step":"code-review","verdict":"FAIL","detail":"review-code: 1 finding, MEDIUM, assertWireScope accepts a non-string excludeSql and it reaches the SQL; reproduced","findings":[{"path":"src/replica/browser/worker-core.ts","line":54}]},
  {"step":"security-review","verdict":"FAIL","detail":"review-security: 1 confirmed MEDIUM sql_injection (guard bypass) via array excludeSql, confidence 85","findings":[{"path":"src/replica/browser/worker-core.ts","line":54}]},
  {"step":"security-audit","verdict":"SKIPPED","detail":"no dependency audit configured in this repo; the diff adds no dependencies"}
]}
```
