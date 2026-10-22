// tenant-client-flow.test.ts — CTC-6367. `flow.series()` over GET /api/v1/flow-series. Every body
// here carries exactly the keys the cloud's `FlowSeries` schema requires
// (test/fixtures/flow-series-openapi.json), so a fixture cannot drift from the published contract.

import { describe, expect, it } from "vitest";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
import { createTenantClient, FLOW_GRANULARITIES, FLOW_PERIODS } from "../src/index";
import schemaDoc from "./fixtures/flow-series-openapi.json";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";
const REPO = "coalesce-labs/catalyst-cloud";
const HOUR = 3_600_000;
const T0 = Date.UTC(2026, 9, 15, 13, 0, 0);

function client(script: Parameters<typeof scriptedFetch>[0]) {
  const net = scriptedFetch(script);
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

function column(startMs: number, endMs: number, open: number | null, partial = false) {
  return {
    startMs, endMs, label: new Date(startMs).toISOString(), partial, ticket: null,
    pr: open === null ? null : { opened: 1, merged: 1, closedUnmerged: 0, net: 0, open, cycleSeconds: [] },
  };
}

function body(period: string, columns: readonly Record<string, unknown>[]) {
  return {
    project: { id: "proj-1", name: "Catalyst Cloud", repo: REPO },
    period, boundary: { kind: "iana", timeZone: "America/Chicago" }, generatedAt: T0, columns,
    previousColumns: [], ticketEvents: [], ticketCycleEpisodes: [], openAgeByColumn: null, prEvents: [],
    prCycleEpisodes: [], currentTodo: null, currentOpen: null,
    scopeAvailability: { tickets: { available: false }, pullRequests: { available: true } },
    coverage: {}, source: "tenant-do",
  };
}

const hourly = Array.from({ length: 168 }, (_, i) => column(T0 + i * HOUR, T0 + (i + 1) * HOUR, 30 + (i % 9), i === 167));

describe("flow.series", () => {
  it("reads 168 hourly buckets for one week, each with its bounds and open pull request count", async () => {
    const { net, c } = client([() => json(200, body("1w", hourly))]);
    const r = await c.flow.series({ repo: REPO, period: "1w", granularity: "hour" });
    const sent = new URL(net.calls[0]!.url);
    expect(sent.pathname).toBe("/api/v1/flow-series");
    expect(Object.fromEntries(sent.searchParams)).toEqual({ repo: REPO, period: "1w", granularity: "hour" });
    expect(r.outcome).toBe("ok");
    if (r.outcome !== "ok") return;
    expect(r.series.columns).toHaveLength(168);
    expect(r.series.columns.map((col) => [col.startMs, col.endMs, col.pr?.open])).toEqual(
      hourly.map((col) => [col.startMs, col.endMs, col.pr?.open]),
    );
  });

  it("returns the daily buckets the route answers, with the same counts", async () => {
    const midnight = Date.UTC(2026, 9, 22, 5, 0, 0);
    const daily = [column(midnight - 24 * HOUR, midnight, 37), column(midnight, midnight + 7 * HOUR, 35, true)];
    const { net, c } = client([() => json(200, body("24h", daily))]);
    const r = await c.flow.series({ repo: REPO, period: "24h", granularity: "day" });
    expect(new URL(net.calls[0]!.url).searchParams.get("granularity")).toBe("day");
    expect(r).toMatchObject({ outcome: "ok", status: 200, series: { columns: daily } });
  });

  it("omits period and granularity when the caller does, so the cloud's defaults apply", async () => {
    const { net, c } = client([() => json(200, body("2w", hourly.slice(0, 14)))]);
    await c.flow.series({ repo: REPO });
    expect([...new URL(net.calls[0]!.url).searchParams.keys()]).toEqual(["repo"]);
  });

  it("⭐ refuses a granularity other than hour or day WITHOUT SENDING ANYTHING, naming both", async () => {
    const { net, c } = client([]);
    // @ts-expect-error CTC-6367: the type already refuses "minute"; a plain JavaScript caller can still pass it.
    const r = await c.flow.series({ repo: REPO, period: "1w", granularity: "minute" });
    expect(r).toEqual({
      outcome: "rejected", status: 0, error: "invalid-granularity", value: "minute", allowed: ["hour", "day"],
      reason: 'flow.series(): granularity must be "hour" or "day", got "minute"',
    });
    expect(net.calls).toHaveLength(0);
  });

  it("sends a period it does not know and reports the cloud's refusal (the cloud owns the list)", async () => {
    const { net, c } = client([() => json(400, { error: "invalid-period" })]);
    // @ts-expect-error CTC-6367: a plain JavaScript caller can pass a period this SDK version does not list.
    const r = await c.flow.series({ repo: REPO, period: "52w" });
    expect(new URL(net.calls[0]!.url).searchParams.get("period")).toBe("52w");
    expect(r).toMatchObject({ outcome: "rejected", status: 400 });
  });

  it("accepts a bucket whose pull requests are unavailable", async () => {
    const { c } = client([() => json(200, body("24h", [column(T0, T0 + HOUR, null)]))]);
    const r = await c.flow.series({ repo: REPO, period: "24h" });
    expect(r.outcome === "ok" && r.series.columns[0]!.pr).toBeNull();
  });

  it("accepts fields the SDK does not declare", async () => {
    const extra = { ...body("1w", hourly), brandNewCloudField: 1, columns: hourly.map((col) => ({ ...col, newer: true })) };
    const { c } = client([() => json(200, extra)]);
    expect((await c.flow.series({ repo: REPO, period: "1w", granularity: "hour" })).outcome).toBe("ok");
  });

  it("is a shape failure when a bucket lacks its bounds or open count", async () => {
    for (const bad of [
      { columns: "x" },
      body("1w", [{ ...column(T0, T0 + HOUR, 3), endMs: "later" }]),
      body("1w", [{ ...column(T0, T0 + HOUR, 3), pr: { opened: 1 } }]),
    ]) {
      const { c } = client([() => json(200, bad)]);
      expect((await c.flow.series({ repo: REPO })).outcome).toBe("shape");
    }
  });

  it("is a shape failure when any other field the SDK declares is missing", async () => {
    const noProject = Object.fromEntries(Object.entries(body("1w", hourly)).filter(([key]) => key !== "project"));
    for (const bad of [
      { columns: [] },
      noProject,
      { ...body("1w", hourly), generatedAt: "now" },
      body("1w", [{ ...column(T0, T0 + HOUR, 3), partial: "no" }]),
      body("1w", [{ ...column(T0, T0 + HOUR, 3), pr: { open: 2 } }]),
    ]) {
      const { c } = client([() => json(200, bad)]);
      expect((await c.flow.series({ repo: REPO })).outcome).toBe("shape");
    }
  });

  it("folds the budget refusal and the auth plane into the shared arms", async () => {
    const tooLarge = { error: "flow-series-too-large", reason: "Use daily buckets or a shorter period." };
    expect(await client([() => json(400, tooLarge)]).c.flow.series({ repo: REPO, period: "26w", granularity: "hour" })).toEqual({
      outcome: "rejected", status: 400, reason: "Use daily buckets or a shorter period.",
    });
    expect((await client([() => json(401, { error: "unauthorized" })]).c.flow.series({ repo: REPO })).outcome).toBe("unauthorized");
  });
});

describe("⭐ the SDK's Flow types match the cloud's published schema", () => {
  const op = schemaDoc.paths["/api/v1/flow-series"].get;
  const schema = schemaDoc.components.schemas.FlowSeries;
  const col = schema.properties.columns.items;
  const enumOf = (name: string) => op.parameters.find((p) => p.name === name)?.schema.enum;

  it("accepts exactly the periods and granularities the route accepts", () => {
    expect(op.parameters.map((p) => p.name)).toEqual(["repo", "period", "granularity"]);
    expect(enumOf("period")).toEqual([...FLOW_PERIODS]);
    expect(enumOf("granularity")).toEqual([...FLOW_GRANULARITIES]);
  });

  it("names only fields the schema requires", () => {
    expect(schema.required).toEqual(expect.arrayContaining(["project", "period", "generatedAt", "columns"]));
    expect(col.required).toEqual(expect.arrayContaining(["startMs", "endMs", "label", "partial", "pr"]));
    const pr = col.properties.pr.anyOf.find((s) => s.type === "object");
    expect(pr?.required).toEqual(expect.arrayContaining(["open", "opened", "merged", "closedUnmerged", "net"]));
    expect(col.properties.pr.anyOf.some((s) => s.type === "null")).toBe(true);
  });

  it("builds test bodies with exactly the schema's required keys", () => {
    const b = body("1w", hourly);
    expect(Object.keys(b).sort()).toEqual([...schema.required].sort());
    expect(Object.keys(b.columns[0]!).sort()).toEqual([...col.required].sort());
  });
});
