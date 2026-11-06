// test/perf/browser-replica-scoped.perf.ts — CTC-6150 scenarios 1 and 2, measured.
//
//   Scenario 1: a replica holding 20,000 issues, 6,000 of them in team CTC, returns team CTC's tickets
//               carrying one label, newest first, through queryIssuesScoped — first page under 10 ms p50.
//   Scenario 2: team CTC has tickets in 12 projects and 300 with no project; queryIssueFacetCounts
//               returns each project's count and a "No project" count of 300 — under 25 ms p50.
//
// WHAT IS MEASURED: `WorkerCore.handle` — the production dispatch, the real read-model statements and the
// real sqlite-wasm engine, over an in-memory database, with the replica seeded through the real
// seedBegin/seedBatch/seedCommit path. WHAT IS NOT: the Worker `postMessage` round trip and OPFS, neither
// of which exists under node. Those are the plan's manual browser check.
//
// Run with `npm run test:perf`. It is NOT part of `npm test`: a wall-clock bound on a shared runner
// flakes, and gating it with `.skip` would hide it from the reward-hacking scan.

import { describe, it, expect, beforeAll } from "vitest";
import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type { Database } from "@sqlite.org/sqlite-wasm";
import type {
  IssueFacetCount,
  IssueView,
  KeysetPage,
} from "@catalyst-cloud/read-model";
import {
  createWorkerCore,
  type WorkerCore,
} from "../../src/replica/browser/worker-core.js";
import { buildOpenedReplica } from "../../src/replica/browser/ports.js";
import { SEED_BATCH_ROWS } from "../../src/replica/browser/snapshot-stream.js";
import type { WireChange } from "../../src/replica/browser/protocol.js";

const TOTAL_ISSUES = 20_000;
const CTC_ISSUES = 6_000;
const PROJECTS = 12;
const NO_PROJECT = 300;
const WARMUPS = 20;
const SAMPLES = 200;
/** The label's density is unspecified in the ticket, so both research densities are measured. */
const LABEL_DENSITIES = [120, 1_500] as const;

const SCOPED_P50_BUDGET_MS = 10;
const FACET_P50_BUDGET_MS = 25;

interface Fixture {
  core: WorkerCore;
  /** CTC issue ids carrying the label, with their updated_at, for checking the page. */
  labelled: Map<string, number>;
  updatedAt: Map<string, number>;
}

/** Deterministic, so a failing run can be reproduced: a small LCG, no Math.random. */
function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 0x1_0000_0000;
  };
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx] ?? Number.NaN;
}

/** Time `run` WARMUPS times unmeasured, then SAMPLES times measured; return the p50 and p95 in ms. */
async function measure(
  label: string,
  run: () => Promise<unknown>,
): Promise<{ p50: number; p95: number }> {
  for (let i = 0; i < WARMUPS; i++) await run();
  const samples: number[] = [];
  for (let i = 0; i < SAMPLES; i++) {
    const start = performance.now();
    await run();
    samples.push(performance.now() - start);
  }
  samples.sort((a, b) => a - b);
  const p50 = percentile(samples, 0.5);
  const p95 = percentile(samples, 0.95);
  console.log(
    `[perf] ${label}: p50 ${p50.toFixed(2)} ms, p95 ${p95.toFixed(2)} ms (${SAMPLES} samples after ${WARMUPS} warmups)`,
  );
  return { p50, p95 };
}

let sqlite3: Awaited<ReturnType<typeof sqlite3InitModule>>;

beforeAll(async () => {
  sqlite3 = await sqlite3InitModule();
});

/**
 * 20,000 issues: 6,000 in team CTC and 14,000 in another team. Of the CTC issues, 5,700 are spread over
 * 12 projects and 300 have no project. `labelledCount` of the CTC issues carry the label, spread evenly.
 */
async function seedFixture(labelledCount: number): Promise<Fixture> {
  const core = createWorkerCore(() => {
    const db = new sqlite3.oo1.DB(":memory:", "c") as unknown as Database;
    return Promise.resolve(buildOpenedReplica(db));
  });
  await core.handle({
    type: "open",
    dbPath: ":memory:",
    directory: "unused",
    identity: "perf",
  });

  const rand = lcg(6150);
  const rows: WireChange[] = [];
  const labelled = new Map<string, number>();
  const updatedAt = new Map<string, number>();
  const upsert = (entity: string, row: Record<string, unknown>): void => {
    rows.push({ entity, op: "upsert", row });
  };

  upsert("labels", { id: "l-mirror", name: "mirror-do" });
  upsert("labels", { id: "l-noise", name: "noise" });
  for (let p = 1; p <= PROJECTS; p++) {
    upsert("projects", { id: `p${p}`, name: `Project ${p}` });
  }

  const every = Math.floor(CTC_ISSUES / labelledCount);
  for (let i = 0; i < TOTAL_ISSUES; i++) {
    const ctc = i < CTC_ISSUES;
    const id = `${ctc ? "c" : "o"}${i}`;
    // Spread over about six months of milliseconds so the keyset order is not insertion order.
    const updated = 1_700_000_000_000 + Math.floor(rand() * 15_000_000_000);
    updatedAt.set(id, updated);
    upsert("issues", {
      id,
      identifier: `${ctc ? "CTC" : "OTH"}-${i}`,
      title: `Issue ${i} about the mirror`,
      state: i % 3 === 0 ? "Done" : "Todo",
      priority: i % 5,
      team_id: ctc ? "team-ctc" : "team-other",
      project_id: ctc
        ? i < NO_PROJECT
          ? null
          : `p${((i - NO_PROJECT) % PROJECTS) + 1}`
        : `p${(i % PROJECTS) + 1}`,
      updated_at: updated,
      created_at: updated - 86_400_000,
    });
    if (ctc && i % every === 0 && labelled.size < labelledCount) {
      labelled.set(id, updated);
      upsert("issue_labels", { issue_id: id, label_id: "l-mirror" });
    } else if (i % 7 === 0) {
      upsert("issue_labels", { issue_id: id, label_id: "l-noise" });
    }
  }

  await core.handle({ type: "seedBegin" });
  for (let at = 0; at < rows.length; at += SEED_BATCH_ROWS) {
    await core.handle({ type: "seedBatch", rows: rows.slice(at, at + SEED_BATCH_ROWS) });
  }
  await core.handle({ type: "seedCommit", cursor: 1 });
  return { core, labelled, updatedAt };
}

describe.each(LABEL_DENSITIES)(
  "queryIssuesScoped at 20,000 issues, the label on %i of team CTC's 6,000 (CTC-6150 scenario 1)",
  (labelledCount) => {
    let fixture: Fixture;

    beforeAll(async () => {
      fixture = await seedFixture(labelledCount);
    });

    const scope = { teamId: "team-ctc", labelId: "l-mirror" };

    it("returns team CTC's labelled issues newest first", async () => {
      const page = (await fixture.core.handle({
        type: "queryIssuesScoped",
        scope,
        limit: 100,
      })) as KeysetPage<IssueView>;

      const expected = [...fixture.labelled.entries()]
        .sort(([idA, a], [idB, b]) => b - a || (idA < idB ? 1 : -1))
        .slice(0, 100)
        .map(([id]) => id);
      expect(page.rows.map((r) => r.id)).toEqual(expected);
      expect(page.rows.every((r) => fixture.labelled.has(r.id))).toBe(true);
      expect(page.rows.every((r) => r.labels.some((l) => l.id === "l-mirror"))).toBe(true);
    });

    it(`answers the first page in under ${SCOPED_P50_BUDGET_MS} ms p50`, async () => {
      const { p50 } = await measure(
        `queryIssuesScoped, label on ${labelledCount}, limit 100`,
        () => fixture.core.handle({ type: "queryIssuesScoped", scope, limit: 100 }),
      );
      expect(p50).toBeLessThan(SCOPED_P50_BUDGET_MS);
    });
  },
);

describe("queryIssueFacetCounts at 20,000 issues (CTC-6150 scenario 2)", () => {
  let fixture: Fixture;

  beforeAll(async () => {
    fixture = await seedFixture(LABEL_DENSITIES[0]);
  });

  const request = {
    type: "queryIssueFacetCounts" as const,
    scope: { teamId: "team-ctc" },
    dimension: "project" as const,
  };

  it("returns each project's count and a 'No project' count of 300", async () => {
    const counts = (await fixture.core.handle(request)) as IssueFacetCount[];

    expect(counts).toHaveLength(PROJECTS + 1);
    expect(counts.at(-1)).toEqual({ key: null, name: null, count: NO_PROJECT });
    expect(counts.slice(0, PROJECTS).reduce((sum, c) => sum + c.count, 0)).toBe(
      CTC_ISSUES - NO_PROJECT,
    );
  });

  it(`answers in under ${FACET_P50_BUDGET_MS} ms p50`, async () => {
    const { p50 } = await measure("queryIssueFacetCounts, project, team CTC", () =>
      fixture.core.handle(request),
    );
    expect(p50).toBeLessThan(FACET_P50_BUDGET_MS);
  });
});
