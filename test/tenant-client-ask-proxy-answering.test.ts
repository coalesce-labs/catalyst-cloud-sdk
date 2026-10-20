// tenant-client-ask-proxy-answering.test.ts — CTC-5663 (SDK half, CTC-6279). The ask owner's own
// controls: `GET`/`PUT /me/ask-proxy-answering` (the opt-in, default off) and `POST /me/ask-disown`.
// Every server body below is the shape the mirror's own handlers produce
// (connect/ask-proxy-answering-routes.ts, connect/ask-disown-routes.ts, and the tenant DO behind them).

import { describe, expect, it } from "vitest";
import { createTenantClient } from "../src/index";
import { json, scriptedFetch } from "./helpers/scripted-fetch";

const BASE = "https://cloud.example";
function client(responses: ReturnType<typeof json>[]) {
  const net = scriptedFetch(responses.map((response) => () => response));
  return { net, c: createTenantClient({ key: "ctc_user_self", baseUrl: BASE, fetch: net.fetch }) };
}

describe("askProxyAnswering.get / set — the owner's opt-in", () => {
  it("reads the setting and writes only `enabled`, with the member's own credential", async () => {
    const { c, net } = client([json(200, { enabled: false }), json(200, { enabled: true })]);
    expect(await c.askProxyAnswering.get()).toEqual({ outcome: "ok", status: 200, enabled: false });
    expect(await c.askProxyAnswering.set(true)).toEqual({ outcome: "ok", status: 200, enabled: true });
    expect(net.calls).toMatchObject([
      { method: "GET", url: `${BASE}/me/ask-proxy-answering`, body: null, headers: { authorization: "Bearer ctc_user_self" } },
      { method: "PUT", url: `${BASE}/me/ask-proxy-answering`, body: { enabled: true } },
    ]);
  });

  it("⛔ an unresolved Linear identity is its own arm, never a silent `false`", async () => {
    const { c } = client([json(409, { error: "no_linear_identity" })]);
    expect(await c.askProxyAnswering.get()).toEqual({ outcome: "no-linear-identity", status: 409 });
  });

  it.each([[{}], [{ enabled: "true" }], [[]], [null]])("refuses a malformed success body %j", async (body: unknown) => {
    const { c } = client([json(200, body)]);
    expect(await c.askProxyAnswering.get()).toMatchObject({ outcome: "shape", status: 200 });
  });

  it("keeps the auth plane's answers distinct (an account key is `forbidden`)", async () => {
    const { c } = client([json(401, { error: "unauthorized" }), json(403, { error: "personal_credential_required" }), json(400, { error: "invalid_body" })]);
    expect(await c.askProxyAnswering.get()).toMatchObject({ outcome: "unauthorized", status: 401 });
    expect(await c.askProxyAnswering.set(true)).toMatchObject({ outcome: "forbidden", status: 403, reason: "personal_credential_required" });
    expect(await c.askProxyAnswering.set(false)).toMatchObject({ outcome: "rejected", status: 400 });
  });
});

describe("askProxyAnswering.disown — the owner takes a proxy answer back", () => {
  it("⭐ POSTs only the issue id, and passes disowned/refused/disown-failed through with their statuses", async () => {
    const tickets = [{ ticket: "ENG-49", phase: "validate", status: "held-again" }];
    const { c, net } = client([
      json(200, { outcome: "disowned", askIdentifier: "ENG-50", tickets }),
      json(409, { outcome: "refused", reason: "already-disowned" }),
      json(502, { outcome: "disown-failed", askIdentifier: "", reason: "linear unavailable" }),
    ]);
    expect(await c.askProxyAnswering.disown("ENG-50")).toEqual({ outcome: "disowned", status: 200, askIdentifier: "ENG-50", tickets });
    expect(net.calls[0]).toMatchObject({ method: "POST", url: `${BASE}/me/ask-disown`, body: { issueId: "ENG-50" } });
    expect(await c.askProxyAnswering.disown("ENG-50")).toEqual({ outcome: "refused", status: 409, reason: "already-disowned" });
    expect(await c.askProxyAnswering.disown("ENG-50")).toEqual({
      outcome: "disown-failed", status: 502, askIdentifier: "", reason: "linear unavailable",
    });
  });

  it("⛔ the identity precondition and the auth plane do not read as a disown outcome", async () => {
    const { c } = client([json(409, { error: "no_linear_identity" }), json(403, { error: "personal_credential_required" }), json(400, { error: "invalid_body" })]);
    expect(await c.askProxyAnswering.disown("ENG-50")).toEqual({ outcome: "no-linear-identity", status: 409 });
    expect(await c.askProxyAnswering.disown("ENG-50")).toMatchObject({ outcome: "forbidden", status: 403 });
    expect(await c.askProxyAnswering.disown("")).toMatchObject({ outcome: "rejected", status: 400 });
  });
});
