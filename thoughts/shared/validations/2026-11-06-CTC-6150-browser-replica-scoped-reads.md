---
date: 2026-11-06
source_ticket: CTC-6150
repository: catalyst-cloud-sdk
plan: thoughts/shared/plans/2026-11-06-CTC-6150-browser-replica-scoped-issues-and-facet-counts.md
pr: coalesce-labs/catalyst-cloud-sdk#238
status: PARTIAL
git_commit: 55dd76b331b92c917a70cb186a36a39ef846b372
branch: CTC-6150
git_main_sha: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
git_merge_base: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
---

# Validation Report: Browser replica scoped issues and facet counts

**Plan**: `thoughts/shared/plans/2026-11-06-CTC-6150-browser-replica-scoped-issues-and-facet-counts.md`
**Validated**: 2026-11-06
**Validation Status**: PARTIAL (code and tests complete; release and the catalyst-cloud pickup not done)

## Validation Source

- `git_commit`: 55dd76b331b92c917a70cb186a36a39ef846b372
- `branch`: CTC-6150
- `git_main_sha`: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2
- `git_merge_base`: c20b7637b3fefeea3d32d9e261f4f5aa7e25caf2

Main has not moved since the plan (plan merge base equals current main), so there is no main movement to report.

## Scenarios

- met: The browser replica answers a scoped ticket query
- met: The browser replica returns facet counts for a filtered list
- missed: catalyst-cloud picks up the release: no release contains the two ops (package.json files still read 0.16.23, no v0.16.24 release), and the catalyst-cloud bump and `no-replica-op` removal are in another repository this run could not read

## Scenario evidence

1. Scoped query. `src/replica/browser/worker-core.ts` adds `queryIssuesScoped` (wraps `buildIssuesViewKeyset`, default sort "updated" = newest first) and `BrowserReplica.queryIssuesScoped` exposes it (`src/replica/browser/browser-replica.ts`). Worker-core tests cover team plus label filtering, order and paging. I re-ran `npm run test:perf` at 20,000 issues (6,000 in team CTC): p50 4.31 ms (label on 120) and 6.18 ms (label on 1,500) against the 10 ms target. The label is filtered by `labelId`, not by name (plan decision 2); the test seeds a label named `mirror-do` and queries by its id. Caveat: p95 at density 1,500 was 10.00 ms; only p50 is the requirement. The plan's headless-Chrome figures (OPFS and Worker round trip, p50 4.3 and 5.8 ms) are recorded in the plan; I could not reproduce them here.
2. Facet counts. `queryIssueFacetCounts` wraps `buildIssueFacetCounts`. The perf suite asserts 13 entries with the last `{ key: null, name: null, count: 300 }` and p50 under 25 ms; measured p50 7.13 ms. The "No project" label is `key: null` (plan decision 4), so the web must render the text.
3. Pickup. Plan Phase 5 and `thoughts/shared/plans/CTC-6150/deviations.json` declare that the version bump was held because `train-status.mjs` exited 2 (catalyst-cloud unreadable) and CTC-5078 was not re-checked. The declaration matches the tree: versions unchanged, CHANGELOG already carries a `## 0.16.24` entry. Without the bump, merging this PR does not produce a release; the CHANGELOG heading is ahead of package.json.

## Implementation Status

- Phase 1 protocol and worker dispatch: implemented as planned, including the `excludeSql` guard and read gate.
- Phase 2 client methods, retyped `SEED_GATED_WORKER_RPCS`, type re-exports in `src/browser.ts` and `src/node.ts`: implemented as planned.
- Phase 3 perf suite, `vitest.perf.config.ts`, `test:perf` script: implemented; thresholds met.
- Phase 4 README and CHANGELOG: done.
- Phase 5 release: not done (declared deviation, matched by evidence). Its three automated criteria are unchecked in the plan.

## Automated Verification Results

- `npm run typecheck` exit 0
- `npm run typecheck:scripts` exit 0
- `npm test` exit 0 (59 files, 904 tests)
- `npm run test:perf` exit 0 (6 tests)
- `npm run build` exit 0
- `npm run test:pack` exit 0
- `npm run release:check` exit 0 (at 0.16.23, so it does not prove the 0.16.24 state)

## Code Review Findings

- Matches plan: all source changes in the diff map to plan files; the two declared deviations are the only departures. No undeclared deviations found.
- Reward-hacking scan of added lines: no `as any`, `@ts-*`, lint suppressions, `.skip`/`.only` or non-null assertions. The `as unknown as Worker` and `as unknown as Database` casts in new tests repeat patterns already present at main.
- Potential issue: `assertWireScope` guards only `waitingOn.excludeSql`; it is a client-side-trusted check inside the worker, which is the plan's stated scope.
- Potential issue: CHANGELOG `0.16.24` precedes the version bump, so `package.json` (0.16.23) and the changelog disagree until the release step runs.
- Process: the perf suite is outside `npm test`, so CI must run `npm run test:perf` if the thresholds are to be gated; I did not check `ci.yml`.

## Manual Testing Required

- [ ] Run `node .agents/skills/release-train/scripts/train-status.mjs` where catalyst-cloud is readable; on exit 0 bump the three package.json files to 0.16.24 (peers `^0.16.24`).
- [ ] Re-check CTC-5078 (browser replica retired means close as superseded).
- [ ] After merge, create GitHub Release v0.16.24 and confirm `npm view @catalyst-cloud/sdk version`.
- [ ] In catalyst-cloud, bump the SDK range in the five package.json files and bun.lock, route the reads through the two ops and drop the `no-replica-op` tag.

## Recommendations

- Do not mark CTC-6150 Done until scenario 3's release and catalyst-cloud pickup land.
- Decide whether `test:perf` belongs in CI.

```catalyst-validation-ladder
{"version":1,"steps":[
  {"step":"plan-conformance","verdict":"FAIL","materiality":"material","declared":true,"detail":"Phases 1-4 match the plan; Phase 5 (0.16.24 version bump, release, CTC-5078 re-check) not done, declared in deviations.json and matches the tree. Scenario 3 missed."},
  {"step":"type-safety","verdict":"PASS","detail":"typecheck, typecheck:scripts, npm test (904), test:perf, build, test:pack, release:check all exit 0; reward-hacking grep of added lines clean"},
  {"step":"code-review","verdict":"PASS","detail":"manual diff review: no defects found in the source changes"},
  {"step":"security-review","verdict":"PASS","detail":"manual review: raw SQL field excludeSql is rejected at the worker; no other input reaches SQL unparameterized by read-model"},
  {"step":"security-audit","verdict":"SKIPPED","detail":"no dependency audit run; diff adds no dependencies"}
]}
```
