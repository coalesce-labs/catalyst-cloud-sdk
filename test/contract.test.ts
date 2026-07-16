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
  it("EntityName covers exactly the 15 canonical mirror tables, in order", () => {
    // Byte-for-byte the @catalyst-cloud/types EntityName union (packages/types/src/index.ts 69-84).
    expect([...ENTITY_NAMES]).toEqual([
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
    ]);
    expect(ENTITY_NAMES).toHaveLength(15);
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
