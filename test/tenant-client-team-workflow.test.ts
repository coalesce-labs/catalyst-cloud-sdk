import { describe, expect, it } from "vitest";
import { createTenantClient } from "../src/index";
import { json, scriptedFetch } from "./helpers/scripted-fetch";

const BASE = "https://cloud.example";
const KEY = "ctc_user_personal";
const HASH = "a".repeat(64);
// The mirror's planHash/migrationHash use base-36 FNV/count tokens, unlike mappingHash/undoHash.
const STALENESS_TOKEN = "1z141z4-3";
const readiness = { teamId: "team-1", teamKey: "ENG", teamName: "Engineering", status: "ready", checkedAt: 123, workflowRev: 1, checks: [] };
const workflow = { config: { teamId: "team-1", mode: "mapped-existing", gitAutomation: "off", workflowRev: 1 }, rows: [{ slot: "dispatch", linearStateId: "state-1", linearStateName: "Todo", source: "chosen", stateStillExists: true }], stages: [{ id: "state-1", name: "Todo", type: "unstarted", position: 1 }], stageSource: "linear", readiness, mappingHash: HASH, checklist: [] };

function client(responses: ReturnType<typeof json>[]) {
  const net = scriptedFetch(responses.map((response) => () => response));
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }) };
}

describe("personal team workflow", () => {
  it("lists teams and reads a workflow including its mapping hash and nullable checklist", async () => {
    const { c, net } = client([json(200, { teams: [], canManage: true, liveTeamRead: { attempted: false, error: null }, everChecked: false, mirrorRead: true }), json(200, workflow), json(200, { ...workflow, checklist: null })]);
    expect(await c.teamWorkflow.teams()).toMatchObject({ outcome: "ok", teams: [] });
    expect(await c.teamWorkflow.get("ENG")).toMatchObject({ outcome: "ok", mappingHash: HASH, checklist: [] });
    expect(await c.teamWorkflow.get("ENG")).toMatchObject({ outcome: "ok", checklist: null });
    expect(net.calls.map((call) => [call.method, call.url])).toEqual([
      ["GET", `${BASE}/api/v1/agent/teams`],
      ["GET", `${BASE}/api/v1/agent/team-workflow?team=ENG`],
      ["GET", `${BASE}/api/v1/agent/team-workflow?team=ENG`],
    ]);
    expect(net.calls.every((call) => call.headers.authorization === `Bearer ${KEY}`)).toBe(true);
  });

  it("checks and saves with the preview hash", async () => {
    const { c, net } = client([json(200, { readiness, ask: { outcome: "not-needed", reason: "ready" } }), json(200, { config: workflow.config, rows: [], stages: [], stageSource: "linear", readiness })]);
    expect(await c.teamWorkflow.check("ENG")).toMatchObject({ outcome: "ok", readiness });
    expect(await c.teamWorkflow.save({ team: "ENG", expectedMappingHash: HASH, mode: "mapped-existing", gitAutomation: "off", rows: [{ slot: "dispatch", linearStateId: "state-1", source: "chosen" }] })).toMatchObject({ outcome: "ok", readiness });
    expect(net.calls.map((call) => call.body)).toEqual([
      { team: "ENG" },
      { team: "ENG", expectedMappingHash: HASH, mode: "mapped-existing", gitAutomation: "off", rows: [{ slot: "dispatch", linearStateId: "state-1", source: "chosen" }] },
    ]);
  });

  it("reports a committed save when the cloud cannot recompute readiness", async () => {
    const { c, net } = client([json(200, {
      config: workflow.config,
      rows: workflow.rows,
      stages: workflow.stages,
      stageSource: "linear",
      readiness: null,
    })]);
    expect(await c.teamWorkflow.save({ team: "ENG", expectedMappingHash: HASH, rows: [{ slot: "dispatch", linearStateId: "state-1", source: "chosen" }] })).toMatchObject({
      outcome: "ok", status: 200, readiness: null,
      rows: workflow.rows,
    });
    expect(net.calls).toHaveLength(1);
    expect(net.calls[0]?.url).toBe(`${BASE}/api/v1/agent/team-workflow/save`);
  });

  it("keeps adoption and undo previews separate from applies", async () => {
    const adopt = { teamId: "team-1", teamKey: "ENG", mode: "adopted-recommended", stages: [], unfilledLoadBearing: [], planHash: STALENESS_TOKEN, provenanceGaps: [], labels: [], labelProvenanceGaps: [], labelsNotCreated: [] };
    const undo = { teamId: "team-1", mode: "preview", candidates: [], undoHash: HASH };
    const { c, net } = client([json(200, { ...adopt, checklist: [] }), json(200, { ...adopt, readiness }), json(200, undo), json(200, { archived: [], kept: [], failed: [], readiness })]);
    expect(await c.teamWorkflow.adoptPreview("ENG")).toMatchObject({ outcome: "ok", planHash: STALENESS_TOKEN });
    expect(await c.teamWorkflow.adoptApply("ENG", STALENESS_TOKEN)).toMatchObject({ outcome: "ok", readiness });
    expect(await c.teamWorkflow.undoPreview("ENG")).toMatchObject({ outcome: "ok", undoHash: HASH });
    expect(await c.teamWorkflow.undoApply("ENG", HASH)).toMatchObject({ outcome: "ok", readiness });
    expect(net.calls.map((call) => call.body)).toEqual([
      { team: "ENG", mode: "preview" }, { team: "ENG", mode: "apply", planHash: STALENESS_TOKEN },
      { team: "ENG", mode: "preview" }, { team: "ENG", mode: "apply", undoHash: HASH },
    ]);
  });

  it("previews, migrates, and retires with the same chosen destinations and hash", async () => {
    const choices = [{ sourceStateId: "old", destinationStateId: "new" }];
    const { c, net } = client([
      json(200, { preview: { teamId: "team-1", sources: [], migrationHash: STALENESS_TOKEN, overLimit: false, issueCount: 0, retireLogReadable: true } }),
      json(200, { teamId: "team-1", sources: [], remaining: 0, migrationHash: STALENESS_TOKEN, moved: 0, readiness }),
      json(200, { retired: [], kept: [], failed: [], logGaps: [], readiness }),
    ]);
    expect(await c.teamWorkflow.migratePreview("ENG", choices)).toMatchObject({ outcome: "ok", preview: { migrationHash: STALENESS_TOKEN } });
    expect(await c.teamWorkflow.migrateChunk("ENG", STALENESS_TOKEN, choices)).toMatchObject({ outcome: "ok", remaining: 0 });
    expect(await c.teamWorkflow.migrateRetire("ENG", STALENESS_TOKEN, choices)).toMatchObject({ outcome: "ok", readiness });
    expect(net.calls.map((call) => call.body)).toEqual([
      { team: "ENG", step: "preview", choices }, { team: "ENG", step: "migrate", migrationHash: STALENESS_TOKEN, choices },
      { team: "ENG", step: "retire", migrationHash: STALENESS_TOKEN, choices },
    ]);
  });

  it("preserves named refusals and never retries a stale plan", async () => {
    const { c, net } = client([
      json(401, { error: "expired", reason: "Reconnect your Linear grant" }),
      json(403, { error: "not-an-admin", message: "Admin seat needed" }),
      json(404, { error: "team-unknown", message: "No team" }),
      json(409, { error: "plan-stale", reason: "Changed" }),
      json(400, { error: "no-grant", reason: "Connect Linear" }),
    ]);
    expect(await c.teamWorkflow.adoptPreview("ENG")).toMatchObject({ outcome: "unauthorized", error: "expired", reason: "Reconnect your Linear grant" });
    expect(await c.teamWorkflow.get("ENG")).toMatchObject({ outcome: "forbidden", error: "not-an-admin" });
    expect(await c.teamWorkflow.get("ENG")).toMatchObject({ outcome: "rejected", error: "team-unknown", status: 404, reason: "No team" });
    expect(await c.teamWorkflow.save({ team: "ENG", expectedMappingHash: HASH, rows: [] })).toMatchObject({ outcome: "rejected", error: "plan-stale", status: 409, reason: "Changed" });
    expect(await c.teamWorkflow.adoptApply("ENG", STALENESS_TOKEN)).toMatchObject({ outcome: "rejected", error: "no-grant", reason: "Connect Linear" });
    expect(net.calls).toHaveLength(5);
  });

  it("rejects a migration availability value that is not Boolean", async () => {
    const { c } = client([json(200, { preview: { teamId: "team-1", sources: [], migrationHash: STALENESS_TOKEN, overLimit: false, issueCount: 0, retireLogReadable: true, actionsAvailable: "false" } })]);
    expect(await c.teamWorkflow.migratePreview("ENG")).toMatchObject({ outcome: "shape", status: 200 });
  });

  it("passes a paused migration preview to callers without issuing a write", async () => {
    const { c, net } = client([json(200, { preview: { teamId: "team-1", sources: [], migrationHash: STALENESS_TOKEN, overLimit: false, issueCount: 0, retireLogReadable: true, actionsAvailable: false } })]);
    expect(await c.teamWorkflow.migratePreview("ENG")).toMatchObject({ outcome: "ok", preview: { actionsAvailable: false } });
    expect(net.calls.map((call) => call.body)).toEqual([{ team: "ENG", step: "preview", choices: [] }]);
  });

  it("refuses malformed successes and invalid local hashes before a write", async () => {
    const { c, net } = client([json(200, { checklist: [] }), json(200, { preview: { migrationHash: STALENESS_TOKEN } }), json(200, { ...workflow, stages: [{}] })]);
    expect(await c.teamWorkflow.get("ENG")).toMatchObject({ outcome: "shape", status: 200 });
    expect(await c.teamWorkflow.migratePreview("ENG")).toMatchObject({ outcome: "shape", status: 200 });
    expect(await c.teamWorkflow.get("ENG")).toMatchObject({ outcome: "shape", status: 200 });
    expect(await c.teamWorkflow.adoptApply("ENG", "bad")).toMatchObject({ outcome: "rejected", error: "invalid-hash" });
    expect(net.calls).toHaveLength(3);
  });

  it("does not apply after an unavailable preview", async () => {
    const net = scriptedFetch([() => new Error("offline")]);
    const c = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await c.teamWorkflow.adoptPreview("ENG")).toMatchObject({ outcome: "network" });
    expect(net.calls).toHaveLength(1);
  });
});
