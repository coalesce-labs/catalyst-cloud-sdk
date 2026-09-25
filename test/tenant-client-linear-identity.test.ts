import { describe, expect, it } from "vitest";
import { createTenantClient } from "../src/index";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
const BASE = "https://cloud.example";
function client(responses: ReturnType<typeof json>[]) {
  const net = scriptedFetch(responses.map((response) => () => response));
  return { net, c: createTenantClient({ key: "ctc_user_self", baseUrl: BASE, fetch: net.fetch }) };
}
describe("personal Linear identity", () => {
  it("reads offered choices and sends only the member's selected Linear id", async () => {
    const identity = { resolution: "manual", linearUserId: "lin_self", resolvedAt: 123, resolvedBy: "self" };
    const options = [{ id: "lin_self", name: null, displayName: "Self", avatarUrl: null }];
    const { c, net } = client([json(200, { identity: { resolution: "no_match" }, options }), json(200, { identity })]);
    expect(await c.linearIdentity.get()).toEqual({ outcome: "ok", identity: { resolution: "no_match" }, options });
    expect(await c.linearIdentity.set("lin_self")).toEqual({ outcome: "ok", identity });
    expect(net.calls).toMatchObject([
      { method: "GET", url: `${BASE}/me/linear-identity`, body: null, headers: { authorization: "Bearer ctc_user_self" } },
      { method: "POST", url: `${BASE}/me/linear-identity`, body: { linearUserId: "lin_self" } },
    ]);
  });
  it("preserves absent options rather than inventing an empty roster", async () => {
    const { c } = client([json(200, { identity: { resolution: "resolved", linearUserId: "lin_self", resolvedAt: 123 } })]);
    expect(await c.linearIdentity.get()).toEqual({ outcome: "ok", identity: { resolution: "resolved", linearUserId: "lin_self", resolvedAt: 123 } });
  });
  it.each(["already_resolved", "already_claimed", "identity_changed"])("returns explicit conflict %s", async (reason) => {
    const { c } = client([json(409, { error: reason })]);
    expect(await c.linearIdentity.set("lin_self")).toEqual({ outcome: "conflict", status: 409, reason });
  });
  it.each([
    { identity: { resolution: "resolved", linearUserId: "lin_self" } },
    { identity: { resolution: "manual", linearUserId: "lin_self", resolvedAt: 123 } },
    { identity: { resolution: "unknown" } },
    { identity: { resolution: "no_match" }, options: [{ id: 1 }] },
    { identity: { resolution: "no_match" }, options: null },
  ])("refuses malformed success bodies", async (body) => {
    const { c } = client([json(200, body)]);
    expect(await c.linearIdentity.get()).toMatchObject({ outcome: "shape", status: 200 });
  });
  it("retains authorization and provider failure distinctions", async () => {
    const { c } = client([json(401, { error: "unauthorized" }), json(403, { error: "personal_credential_required" }), json(503, { error: "roster_unavailable" })]);
    expect(await c.linearIdentity.get()).toMatchObject({ outcome: "unauthorized" });
    expect(await c.linearIdentity.get()).toMatchObject({ outcome: "forbidden" });
    expect(await c.linearIdentity.set("lin_self")).toMatchObject({ outcome: "http", status: 503 });
  });
});
