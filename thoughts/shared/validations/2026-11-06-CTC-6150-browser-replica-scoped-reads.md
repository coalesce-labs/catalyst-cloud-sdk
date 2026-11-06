---
date: 2026-11-06
source_ticket: CTC-6150
repository: catalyst-cloud-sdk
plan: thoughts/shared/plans/2026-11-06-CTC-6150-browser-replica-scoped-issues-and-facet-counts.md
pr: coalesce-labs/catalyst-cloud-sdk#238
status: FAIL
git_commit: 55dd76b331b92c917a70cb186a36a39ef846b372
branch: CTC-6150
git_main_sha: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
git_merge_base: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
---

# Validation Report: Browser replica scoped issues and facet counts

**Plan**: `thoughts/shared/plans/2026-11-06-CTC-6150-browser-replica-scoped-issues-and-facet-counts.md`
**Validated**: 2026-11-06
**Validation Status**: FAIL

## Validation Source

- `git_commit`: 55dd76b331b92c917a70cb186a36a39ef846b372
- `branch`: CTC-6150
- `git_main_sha`: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
- `git_merge_base`: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2

Main has not moved since the plan (plan merge base equals current main), so there is no main movement to report.

## Scenarios

- met: The browser replica answers a scoped ticket query
- met: The browser replica returns facet counts for a filtered list
- missed: catalyst-cloud picks up the release: the release its Given needs does not exist (package.json and npm `latest` read 0.16.23, no `v0.16.24` tag), and its When and Then are carried out in catalyst-cloud, which this run could not read; no recorded decision removed it from this ticket

## Scenario evidence

1. Scoped query. `src/replica/browser/worker-core.ts` adds `queryIssuesScoped` (wraps `buildIssuesViewKeyset`, default sort "updated", newest first) and `BrowserReplica.queryIssuesScoped` exposes it. Worker-core tests cover team plus label filtering, order and paging. Perf suite at 20,000 issues (6,000 in team CTC), p50 against 10 ms: 3.38 ms (label on 120) and 5.29 ms (label on 1,500) in the final run; 4.31 and 6.18 ms in an earlier run. The label is filtered by id, not by name (plan decision 2); the test seeds a label named `mirror-do` and queries by its id. The plan's headless-Chrome numbers (OPFS plus Worker round trip) I could not reproduce.
2. Facet counts. `queryIssueFacetCounts` wraps `buildIssueFacetCounts`. The perf suite asserts 13 entries, the last `{ key: null, name: null, count: 300 }`, and p50 under 25 ms; measured 6.21 ms (7.13 ms earlier). "No project" is `key: null` (plan decision 4), so the web renders the text.
3. Pickup. Classified `missed`, not `narrowed`: the only thing that moves it out of this ticket is plan decision 10, which is the plan author's own choice, not a recorded decision by the ticket or its owner. Not `unread` alone either: the Given (a release containing both ops) is this repository's job and the evidence shows it fails: `package.json` 0.16.23, `git tag` ends at `v0.16.23`, `npm view @catalyst-cloud/sdk version` prints 0.16.23, and `CHANGELOG.md` already has a `## 0.16.24` heading that no release backs. The When (bump the SDK range in five package.json files and bun.lock) and Then (web path uses the two ops, `no-replica-op` dropped) are done in catalyst-cloud, which this container cannot read, so those halves are unchecked here.

## Validation Source

- `git_commit`: 9437f70d7da182842682fbb0215a5b1e1047147c (the code under review is unchanged since 55dd76b331b92c917a70cb186a36a39ef846b372; 9437f70 only adds the first version of this report)
- `branch`: CTC-6150
- `git_main_sha`: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
- `git_merge_base`: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2

Main has not moved since the plan (plan merge base equals current main), so there is no main movement to report.

## Implementation Status

- Phase 1 protocol and worker dispatch: implemented, but the `excludeSql` guard has a bypass (see Code Review Findings).
- Phase 2 client methods, retyped `SEED_GATED_WORKER_RPCS`, type re-exports in `src/browser.ts` and `src/node.ts`: implemented as planned.
- Phase 3 perf suite, `vitest.perf.config.ts`, `test:perf` script: implemented; thresholds met.
- Phase 4 README and CHANGELOG: done.
- Phase 5 release 0.16.24: not done. Its three Automated Verification boxes are unchecked in the plan; versions are unchanged; CTC-5078 was not re-checked.

### Plan conformance and the deviation rule

The validate-plan skill's rule for declared deviations, quoted: "Match each declared entry against the diff and its evidence, including the full main commit SHA when main forced the departure. List unmatched code changes as undeclared deviations and cite their file and line; list a declaration as declared only when its actual change and evidence match." The ladder field rules add: "`material` when a deviation changes what the plan promised" and "`declared` ... `true` when the deviation is one the implementation declared up front." Section 3 adds: confirm that every phase "has matching code and every 'Automated Verification' command ran".

Applying it to `thoughts/shared/plans/CTC-6150/deviations.json`:

- Entry 1 (Phase 5 step 3, version bump held): actual (three package.json files at 0.16.23) matches the tree; evidence (train-status exit 2, `gh api` could not read catalyst-cloud) is plausible and consistent with the release-train rule to stop on exit 2. It counts as declared.
- Entry 2 (CTC-5078 not re-checked): the actual matches (no re-check recorded). Declared.
- No undeclared deviations in the diff.

The rule makes a matching declaration "declared"; it does not make it conformant. The plan promised 0.16.24 versions, a release, and a passing `release:check` and `train-status` at that state; none of that was delivered, and scenario 3 depends on it. The deviation changes what the plan promised, so it is `material`. Verdict: plan-conformance FAIL, `materiality: material`, `declared: true`. The hold was the correct reaction to train-status exit 2; it is still an unfinished phase.

## Automated Verification Results

Each command was run from the repository root on this head, with its real exit status.

- `npm run typecheck`: exit 0 (tsc --noEmit, no output)
- `npm run typecheck:scripts`: exit 0
- `npm test`: exit 0. `Test Files  59 passed (59)`, `Tests  904 passed (904)`, `Duration  9.61s`
- `npm run test:perf`: exit 0. `Test Files  1 passed (1)`, `Tests  6 passed (6)`
  - `[perf] queryIssuesScoped, label on 120, limit 100: p50 3.38 ms, p95 4.77 ms (200 samples after 20 warmups)`
  - `[perf] queryIssuesScoped, label on 1500, limit 100: p50 5.29 ms, p95 8.23 ms (200 samples after 20 warmups)`
  - `[perf] queryIssueFacetCounts, project, team CTC: p50 6.21 ms, p95 8.87 ms (200 samples after 20 warmups)`
- `npm run build`: exit 0. Tail: `Writing src/tenant-client.ts -> dist/tenant-client.d.ts`, `Done in 1.28s`
- `npm run test:pack`: exit 0. Tail: `{"corePackedBytes":445082,...,"consumerTypes":"passed","optionalModules":"passed","browserAndWorkerBundles":"passed"}`
- `npm run release:check`: exit 0. `# tests 8`, `# pass 8`, `# fail 0`. This runs at 0.16.23, so it does not prove the 0.16.24 state.
- PR #238 checks (`gh pr checks 238`): `install · build · typecheck · test` pass (59s), CodeQL and both Analyze jobs pass; Mergify entries skipping.

### validate-type-safety (invoked with the Skill tool; CATALYST_PHASE=validate, so record, never fix)

- Tooling: package manager npm (`package-lock`-style scripts; `bun.lock` is present but the repo's scripts are npm), linter none (no `.trunk/`, biome or eslint config), type check `npm run typecheck`.
- Step 0 tsconfig (`tsconfig.json`): `strict: true` YES. INFO: `noUncheckedIndexedAccess`, `noImplicitReturns`, `exactOptionalPropertyTypes` missing. No tsconfig change in the diff.
- Step 1 type check: PASS, exit 0.
- Step 2 reward-hacking scan (scan-reward-hacking, base c20b763, app severity): FAIL. Three added lines match pattern 1 (`as unknown as`) with no documenting comment, which is HIGH and not acceptable by its row:
  - `test/browser/browser-replica.test.ts:1293` `worker as unknown as Worker`
  - `test/browser/browser-replica.test.ts:1426` `worker as unknown as Worker`
  - `test/perf/browser-replica-scoped.perf.ts:98` `new sqlite3.oo1.DB(":memory:", "c") as unknown as Database`
  - Mitigation, not an exemption: each repeats an existing pattern (29 `as unknown as Worker` casts already in that test file at the base; the same `Database` cast in `test/browser/known-columns.test.ts` and `test/browser/read-model-ddl.test.ts`) and wraps a test double, not production code. The skill's "acceptable when" requires a comment and a tracking ticket, and only `as any` has a test-mock allowance, so the scan verdict is FAIL. No other pattern matches on added lines (patterns 2 to 7, 9 and 10 clean); patterns 11 and 12: no tsconfig diff.
  - Required fix: add the documenting comment (library limitation, runtime check, ticket) above each cast, or type the doubles without the double cast.
- Step 3 test inclusion: PASS. `tsconfig.json` includes `src` and `test`. `tsconfig.build.json:13` excludes `test`; that is the build-only config and predates this branch.
- Step 4 tests: PASS (904 passed; PR CI green at the same head).
- Step 5 lint: SKIPPED, no linter detected.
- type-coverage: not installed, not run.
- Overall: FAIL, `substep: reward-hacking`.

## Code Review Findings

### review-code (invoked; base origin/main = c20b763, ancestry proven, 13 code files)

Lenses: guidelines (AGENTS.md rules are workflow rules for tickets and PRs; none scopes to these files), diff bugs, history, prior review comments (gh authenticated; PR has no review threads), in-code guidance. One finding survives. Verdict FAIL.

- `src/replica/browser/worker-core.ts:54`, MEDIUM, correctness, confidence 90: `assertWireScope` only refuses `excludeSql` when `typeof excludeSql === "string"`, but read-model splices it with a truthiness check and a template literal (`waiting?.excludeSql ? { sql: \`${waitingBase.sql} AND ${waiting.excludeSql}\` }`, `node_modules/@catalyst-cloud/read-model/src/issues.ts:1151-1152`). An array survives structured clone, passes the guard, and stringifies into the SQL. I confirmed it with a scratch test outside the repo (since deleted): a scope `{ teamId, waitingOn: { linearUserId: "u1", mode: "all", excludeSql: ["no_such_column_injected = 1"] } }` through `WorkerCore.handle({type:"queryIssuesScoped"})` fails with `SQLITE_ERROR: no such column: no_such_column_injected`, so the array's text reached the statement. The plan's decision 8 and the CHANGELOG and README promise that such a scope is rejected; the tests only try the string `"1 = 1"`.
- Pre-existing items and the three test casts above are not code-review findings.
- Dropped candidates: CHANGELOG ahead of package.json (plan-declared, covered under plan conformance); `projectId: null` clone semantics (verified by tests).

### review-security (invoked; same base and files)

Phases run: repository context (read-model builds statements with bindings except the one `excludeSql` fragment), comparative analysis, vulnerability assessment. One confirmed finding. Verdict FAIL.

- `src/replica/browser/worker-core.ts:54`, MEDIUM, `sql_injection` (guard bypass), confidence 85. Input: any `postMessage` to the db-worker with a `scope.waitingOn.excludeSql` that is a non-string truthy value (an array; structured clone keeps arrays). Sink: the `AND ${waiting.excludeSql}` splice in `issueScopeClause` used by both new ops. Exploit scenario: script that can post to the worker (same-origin page code, or a compromised dependency in the web bundle) sends `excludeSql: ["1=1 OR id IN (SELECT ...)"]` and runs a chosen SELECT fragment over the local replica, the very access the guard was added to withhold. Impact is bounded: the data is the user's own local replica, a single prepared statement, and the caller already holds the page. Fix: reject any `excludeSql` that is not `undefined`, `null` or `""` (`if (excludeSql != null && excludeSql !== "") throw`), and add a test with an array and a number.
- Informational: none.

### Other notes

- The perf suite sits outside `npm test`; whether CI should run `npm run test:perf` is undecided (I did not read `ci.yml` for it).
- CHANGELOG `0.16.24` precedes the version bump.

## Manual Testing Required

- [ ] Run `node .agents/skills/release-train/scripts/train-status.mjs` where catalyst-cloud is readable; on exit 0 bump the three package.json files to 0.16.24 (peers `^0.16.24`).
- [ ] Re-check CTC-5078 (replica retired means close as superseded).
- [ ] After merge, create GitHub Release v0.16.24 and confirm `npm view @catalyst-cloud/sdk version`.
- [ ] In catalyst-cloud, bump the SDK range in the five package.json files and bun.lock, route the reads through the two ops, drop the `no-replica-op` tag.

## Recommendations

- Fix the `excludeSql` guard (`worker-core.ts:54`) and add the array test.
- Document or remove the three `as unknown as` casts.
- Finish Phase 5 and the catalyst-cloud pickup before marking CTC-6150 Done.

```catalyst-validation-ladder
{"version":1,"steps":[
  {"step":"plan-conformance","verdict":"FAIL","materiality":"material","declared":true,"detail":"Phases 1-4 built; Phase 5 (0.16.24 bump, release, CTC-5078 re-check) not delivered. deviations.json entries match the tree so they are declared, but the rule only makes them declared; the deviation changes what the plan promised, so material. Scenario 3 missed."},
  {"step":"type-safety","verdict":"FAIL","substep":"reward-hacking","detail":"validate-type-safety: typecheck exit 0, tests 904 passed, lint SKIPPED (none). scan-reward-hacking FAIL: 3 undocumented 'as unknown as' on added test lines (HIGH), pattern 1","findings":[{"path":"test/browser/browser-replica.test.ts","line":1293},{"path":"test/browser/browser-replica.test.ts","line":1426},{"path":"test/perf/browser-replica-scoped.perf.ts","line":98}]},
  {"step":"code-review","verdict":"FAIL","detail":"review-code vs origin/main: 1 finding, MEDIUM correctness, assertWireScope accepts a non-string excludeSql (array) and it reaches the SQL; reproduced","findings":[{"path":"src/replica/browser/worker-core.ts","line":54}]},
  {"step":"security-review","verdict":"FAIL","detail":"review-security vs origin/main: 1 confirmed MEDIUM sql_injection (guard bypass) via array excludeSql, confidence 85","findings":[{"path":"src/replica/browser/worker-core.ts","line":54}]},
  {"step":"security-audit","verdict":"SKIPPED","detail":"no dependency audit configured in this repo's scripts; the diff adds no dependencies"}
]}
```
