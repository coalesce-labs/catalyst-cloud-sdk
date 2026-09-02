import { describe, expect, it } from "vitest";
import {
  ENTITY_NAMES,
  CHANGE_OPS,
  PING_FRAME,
  PONG_FRAME,
  parseFrame,
  type ChangeFrame,
  type ResyncFrame,
  type SyncFrame,
  type PongFrame,
  type HeadFrame,
  type EntityName,
  type ChangeOp,
} from "../src/index";

// The SDK vendors the change-feed wire contract (src/types.ts) instead of depending on the internal
// @catalyst-cloud/types workspace package. This test pins the LITERAL members + frame shapes so the
// vendored copy can be diffed against the documented contract and any drift from the monorepo is
// caught at SDK CI time (we can't import the internal package to compare against it directly).

describe("wire contract", () => {
  it("EntityName covers exactly the canonical mirror feed tables, in order", () => {
    // Byte-for-byte the @catalyst-cloud/types EntityName union (packages/types/src/index.ts).
    //
    // ⛔ CTC-643 — THIS ASSERTION WAS GREEN WHILE THE UNION WAS STALE BY NINE, and that is the
    // lesson worth keeping. It claims to check the SDK against the service's union, but what it
    // actually compares is one hard-coded copy against another hard-coded copy — both of them here,
    // in this repo, neither of them the thing they are about. The service added nine entities over
    // several months; nothing in this file could notice, because nothing in this file reads it.
    // It kept passing every single day it was wrong.
    //
    // ⚠️ IT IS STILL A LITERAL, and deliberately so — the SDK is published standalone and genuinely
    // cannot import `@catalyst-cloud/types` to diff against (that is the header's own note). So the
    // honest statement of what this test does is: it pins the SDK's union against DELIBERATE
    // change — an edit to `types.ts` must be matched here, so no one widens the wire contract by
    // accident.
    //
    // ⭐ THE "IT CANNOT DETECT THE SERVICE MOVING FIRST" GAP IS NOW CLOSED, and this comment used to
    // say it needed "a generated artifact or a published contract fixture". It did not: the SDK
    // already depends on `@catalyst-cloud/schema`, whose `MIRROR_TABLE_META` is derived from the
    // mirror's own Drizzle definitions via `getTableConfig`. `entity-names-drift.test.ts` reads that
    // and fails naming the delta. This file keeps the literal (deliberate-change detection); that
    // one owns tracking the service. Correcting the note rather than leaving a stale "unsolvable"
    // beside a solved problem.
    const expected = [
      "issues",
      "labels",
      "users",
      "issue_labels",
      "relations",
      "issue_history",
      "projects",
      "cycles",
      "initiatives",
      "project_initiatives",
      "comments",
      "pull_requests",
      "check_runs",
      "commit_statuses",
      "reviews",
      "fleet_activity",
      "agent_sessions",
      "agent_activities",
      "pr_review_comments",
      "pr_commits",
      "pr_conversation_comments",
      "pr_events",
      "workflow_states",
      "team_workflow_mapping",
      // CTC-667: pushes (rebase detection), deployments + deployment_statuses (the deploy state
      // machine), pr_review_threads (the merge gate's resolution state). Deliberate change, matched
      // here on purpose — that is this file's whole job.
      "pushes",
      // CTC-704: the per-DELIVERY push row — deliberate change, matched here on purpose.
      "push_events",
      "deployments",
      "deployment_statuses",
      "pr_review_threads",
      // CTC-667 item 4: GitHub's own check-suite rollup — same, deliberate.
      "check_suites",
      // CTC-355 — the two fleet-liveness feed entities, added here in step with types.ts because
      // this literal exists to make a widening of the wire contract DELIBERATE.
      "fleet_host_liveness",
      "fleet_anomalies",
    ];
    expect([...ENTITY_NAMES]).toEqual(expected);
    // ⛔ NO HAND-TYPED COUNT HERE. CTC-643's own ticket: "`toHaveLength(15)` must not simply become
    // `toHaveLength(24)` — a hand-typed count is what let this drift for four ticket generations."
    // The count that tracks the mirror lives in entity-names-drift.test.ts, DERIVED from the schema.
    // This file's job is the literal wire contract; asserting a length here as well would put a
    // second hand-maintained number beside the list it is meant to guard.
    expect(ENTITY_NAMES).toHaveLength(expected.length);
    // No duplicates (the union has none).
    expect(new Set(ENTITY_NAMES).size).toBe(ENTITY_NAMES.length);
  });

  it("ChangeOp is exactly { upsert, delete }", () => {
    expect([...CHANGE_OPS]).toEqual(["upsert", "delete"]);
  });

  it("ENTITY_NAMES values are assignable to the EntityName type (lockstep)", () => {
    // A pure type-level assertion: if the runtime array and the type diverge, this stops compiling.
    const sample: EntityName = ENTITY_NAMES[0];
    expect(sample).toBe("issues");
    const op: ChangeOp = CHANGE_OPS[0];
    expect(op).toBe("upsert");
  });

  it("ChangeFrame has the documented shape (type/accountId/seq/entity/entityId/op/row?)", () => {
    const frame: ChangeFrame = {
      type: "change",
      accountId: "tenant-0",
      seq: 42,
      entity: "issues",
      entityId: "i1",
      op: "upsert",
      row: { id: "i1", title: "X", updated_at: 1 },
    };
    expect(frame.type).toBe("change");
    expect(Object.keys(frame).sort()).toEqual(
      ["accountId", "entity", "entityId", "op", "row", "seq", "type"].sort(),
    );
    // row is optional (delete frames omit it).
    const del: ChangeFrame = {
      type: "change",
      accountId: "tenant-0",
      seq: 43,
      entity: "issues",
      entityId: "i1",
      op: "delete",
    };
    expect(del.row).toBeUndefined();
  });

  it("ResyncFrame is {type:'resync', accountId?}", () => {
    const frame: ResyncFrame = { type: "resync", accountId: "tenant-0" };
    expect(frame.type).toBe("resync");
    const bare: ResyncFrame = { type: "resync" };
    expect(bare.accountId).toBeUndefined();
  });

  it("SyncFrame is {type:'sync', after:number}", () => {
    const frame: SyncFrame = { type: "sync", after: 7 };
    expect(frame).toEqual({ type: "sync", after: 7 });
  });

  // CTC-135 liveness ping/pong. The mirror registers setWebSocketAutoResponse(PING_FRAME → PONG_FRAME),
  // which matches the request STRING byte-for-byte, so these literals MUST equal the mirror's copies in
  // apps/mirror/src/do/ws.ts exactly — any drift (key order, spacing) silently breaks the auto-pong.
  it("pins the liveness ping/pong wire bytes (must match apps/mirror/src/do/ws.ts exactly)", () => {
    expect(PING_FRAME).toBe('{"type":"ping"}');
    expect(PONG_FRAME).toBe('{"type":"pong"}');
    // The bytes parse to the intended objects — but the wire contract is the STRING, not the object.
    expect(JSON.parse(PING_FRAME)).toEqual({ type: "ping" });
    expect(JSON.parse(PONG_FRAME)).toEqual({ type: "pong" });
  });

  it("parseFrame recognizes a pong as {type:'pong'} (so the watchdog can consume it)", () => {
    const frame = parseFrame(PONG_FRAME);
    expect(frame).toEqual({ type: "pong" });
  });

  it("PongFrame is {type:'pong'}", () => {
    const frame: PongFrame = { type: "pong" };
    expect(frame.type).toBe("pong");
  });

  // CTL-1402 end-of-pass head nudge: the mirror broadcasts one {type:"head", seq:<feed head>} after a
  // reconcile pass whose change_log rows were never individually broadcast, so the client can detect a
  // trailing gap with no later change frame to trigger from. Transport-internal (never surfaced).
  it("HeadFrame is {type:'head', seq:number, accountId?}", () => {
    const frame: HeadFrame = { type: "head", accountId: "tenant-0", seq: 42 };
    expect(frame.type).toBe("head");
    expect(frame.seq).toBe(42);
    const bare: HeadFrame = { type: "head", seq: 7 };
    expect(bare.accountId).toBeUndefined();
  });

  it("parseFrame recognizes a head as {type:'head', seq}", () => {
    const frame = parseFrame(JSON.stringify({ type: "head", accountId: "tenant-0", seq: 42 }));
    expect(frame?.type).toBe("head");
    expect((frame as HeadFrame).seq).toBe(42);
  });
});
