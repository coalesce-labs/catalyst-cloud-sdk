import { describe, expect, expectTypeOf, it } from "vitest";
import {
  createTenantClient,
  type AgentMessageRecord,
  type MessagesReadParams,
  type MessagesReadResult,
} from "../src/index";
import { json, scriptedFetch, text } from "./helpers/scripted-fetch";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";
const MESSAGE_932140 = {
  id: "msg_3f2a9c0d4b1e4f7a8c6d2e1b0a9f8e7d",
  seq: 932140,
  kind: "note",
  body: "resync your stack",
  from: {
    kind: "person",
    id: "user_01J9ZK2Q",
    label: "user:user_01J9ZK2Q",
    futureField: true,
  },
  sentAt: "2026-10-22T20:14:03.512Z",
  futureField: "retained",
};
const PAGE = { ticket: "CTC-1234", next: 932141, messages: [MESSAGE_932140] };

function client(script: Parameters<typeof scriptedFetch>[0]) {
  const net = scriptedFetch(script);
  return {
    net,
    c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch }),
  };
}

describe("messages.read", () => {
  it("reads one page with the seat's own key and returns body, sender and time verbatim", async () => {
    const { net, c } = client([() => json(200, PAGE)]);
    const r = await c.messages.read({ ticket: "CTC-1234", since: 932140 });
    expect(net.calls).toHaveLength(1);
    expect(net.calls.at(0)).toMatchObject({
      method: "GET",
      headers: { authorization: `Bearer ${KEY}` },
      url: `${BASE}/api/v1/agent/messages?ticket=CTC-1234&since=932140`,
    });
    expect(r).toEqual({ outcome: "ok", status: 200, ...PAGE });
    if (r.outcome !== "ok") throw new Error(`expected ok, got ${r.outcome}`);
    expectTypeOf(r.messages).toEqualTypeOf<AgentMessageRecord[]>();
    expectTypeOf(r).toExtend<MessagesReadResult>();
    expect(r.messages.at(0)).toMatchObject({
      body: "resync your stack",
      from: { label: "user:user_01J9ZK2Q" },
      sentAt: "2026-10-22T20:14:03.512Z",
    });
  });

  it("omits since when not given", async () => {
    const { net, c } = client([
      () => json(200, { ...PAGE, messages: [], next: null }),
    ]);
    const params: MessagesReadParams = { ticket: "CTC-1234" };
    expect(await c.messages.read(params)).toEqual({
      outcome: "ok",
      status: 200,
      ticket: "CTC-1234",
      messages: [],
      next: null,
    });
    expect(net.calls.at(0)?.url).toBe(
      `${BASE}/api/v1/agent/messages?ticket=CTC-1234`,
    );
  });

  it("answers a ticket outside the seat's teams with the cloud's 403 reason", async () => {
    const reason =
      "your seat is scoped to particular projects and does not include team OPS; an owner or admin can widen it";
    const { c } = client([
      () =>
        json(403, { outcome: "rejected", code: "team-not-accessible", reason }),
    ]);
    expect(await c.messages.read({ ticket: "OPS-77" })).toEqual({
      outcome: "forbidden",
      status: 403,
      reason,
      required: null,
      account: null,
    });
  });

  it("answers a server without the route as unsupported, not as no messages", async () => {
    const { c } = client([() => text(404, "Not Found")]);
    expect(await c.messages.read({ ticket: "CTC-1234" })).toEqual({
      outcome: "unsupported",
      status: 404,
      reason: "this server has no GET /api/v1/agent/messages",
    });
  });

  it("passes a 400 through as rejected with the cloud's reason", async () => {
    const { c } = client([
      () => json(400, { outcome: "rejected", reason: "ticket not recognized" }),
    ]);
    expect(await c.messages.read({ ticket: "CTC-1234" })).toEqual({
      outcome: "rejected",
      status: 400,
      reason: "ticket not recognized",
    });
  });

  it("refuses an empty ticket or a negative since before sending anything", async () => {
    const { net, c } = client([() => json(200, PAGE)]);
    for (const params of [
      { ticket: "" },
      { ticket: "  " },
      { ticket: "CTC-1234", since: -1 },
      { ticket: "CTC-1234", since: 1.5 },
      { ticket: "CTC-1234", since: NaN },
      { ticket: "CTC-1234", since: Infinity },
      { ticket: "CTC-1234", since: Number.MAX_SAFE_INTEGER + 1 },
    ]) {
      expect(await c.messages.read(params)).toMatchObject({
        outcome: "rejected",
        status: 0,
      });
    }
    expect(net.calls).toHaveLength(0);
  });

  it("answers an undocumented body as shape", async () => {
    const malformed = [
      null,
      { ...PAGE, ticket: null },
      { ...PAGE, next: "932141" },
      { ...PAGE, messages: {} },
      ...["id", "seq", "kind", "body", "from", "sentAt"].map((key) => ({
        ...PAGE,
        messages: [{ ...MESSAGE_932140, [key]: null }],
      })),
      ...["kind", "id", "label"].map((key) => ({
        ...PAGE,
        messages: [
          { ...MESSAGE_932140, from: { ...MESSAGE_932140.from, [key]: null } },
        ],
      })),
    ];
    const { c } = client(malformed.map((body) => () => json(200, body)));
    for (const _ of malformed)
      expect(await c.messages.read({ ticket: "CTC-1234" })).toMatchObject({
        outcome: "shape",
        status: 200,
      });
  });

  it("retains a zero inclusive cursor and folds transport errors", async () => {
    const { net, c } = client([() => new Error("offline")]);
    expect(
      await c.messages.read({ ticket: "CTC-1234", since: 0 }),
    ).toMatchObject({ outcome: "network" });
    expect(net.calls.at(0)?.url).toBe(
      `${BASE}/api/v1/agent/messages?ticket=CTC-1234&since=0`,
    );
  });
});
