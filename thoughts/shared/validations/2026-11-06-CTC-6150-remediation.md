# CTC-6150 remediation report

Validation report: `thoughts/shared/validations/2026-11-06-CTC-6150-browser-replica-scoped-reads.md` (FAIL, code head c61bc5e). Remediated at HEAD 4c67625.

| Finding | Class | Action | Evidence |
| ------- | ----- | ------ | -------- |
| F1 `assertWireScope` passes non-string truthy `excludeSql` | valid | fixed in `00b194a` | `src/replica/browser/worker-core.ts:51-58`. New `it.each` in `test/browser/worker-core.test.ts:819` (array, number, object, true) failed before the fix: the object case reached SQLite ("no such column: object Object"). Now green. Absent, null and "" still pass. |
| F2 cast at `test/browser/browser-replica.test.ts:1293` | valid | documented in `23c401e` | `FakeWorker` is a partial stand-in for `Worker`; no typed alternative short of a full Worker mock. Comment added. |
| F3 cast at `test/browser/browser-replica.test.ts:1426` | valid | documented in `23c401e` | Same reason, comment added. |
| F4 cast at `test/perf/browser-replica-scoped.perf.ts:98` | valid | documented in `23c401e` | `oo1.DB` and `Database` types are incompatible; same cast and reason as `test/browser/worker-core.test.ts:38`. Comment added. |
| Phase 5 version bump | needs-human (out of scope) | none | package.json stays 0.16.23, waits on a release-train decision. |

Not touched: CHANGELOG version heading, scenario 3 (catalyst-cloud), CTC-5078, whether CI runs test:perf. All need a human.

No tsconfig change, no skipped tests. The new tests avoid a cast by building the scope via `JSON.parse(JSON.stringify(...))`.

## Gates (real exit codes)

typecheck 0, typecheck:scripts 0, test 0 (908 passed), test:perf 0, build 0, test:pack 0.
