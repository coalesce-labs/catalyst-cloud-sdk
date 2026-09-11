// tenant-client-agent.test.ts — CTC-2004 Tier 1, criterion 3: every `agent.*` write's path comes from
// the contract's `routes[]`, and its result is a discriminated union on `outcome` — never a thrown
// prose string.
//
// Every server body below is the shape the mirror's own handler produces (agent-write-routes.ts and
// the primitives it wraps), read in the research doc for CTC-2004 with file:line. The first fetch of
// every scenario serves the committed contract fixture so the route table is the cloud's real one.

import { describe, expect, it } from "vitest";
import fixture from "./fixtures/tenant-contract.fixture.json";
import { json, scriptedFetch, text, type Scripted } from "./helpers/scripted-fetch";
import { createTenantClient, memoryContractCache, type TenantContract } from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_acct_writer";
const ETAG = '"abc"';

const serveContract: Scripted = () => json(200, fixture, { etag: ETAG, "x-catalyst-contract-version": "1.0.0" });

function client(script: Scripted[]) {
  const net = scriptedFetch([serveContract, ...script]);
  return { net, c: createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch, contractCache: memoryContractCache() }) };
}

function pathOf(doc: typeof fixture, name: string): string {
  const route = doc.routes.find((r) => r.path.endsWith(`/${name}`));
  if (route === undefined) throw new Error(`fixture has no ${name} route`);
  return route.path;
}

describe("agent.issueComment — the path, the body, the pass-through", () => {
  it("⭐ POSTs to the contract's issue-comment path with exactly the given body, and passes the outcome through with its status", async () => {
    const { net, c } = client([() => json(200, { outcome: "succeeded", attempts: 1 })]);
    const res = await c.agent.issueComment({ issueId: "iss-1", body: "hello" });
    expect(res).toEqual({ outcome: "succeeded", status: 200, attempts: 1 });
    expect(net.calls).toHaveLength(2);
    const post = net.calls[1]!;
    expect(post.method).toBe("POST");
    expect(post.url).toBe(`${BASE}${pathOf(fixture, "issue-comment")}`);
    expect(post.headers["authorization"]).toBe(`Bearer ${KEY}`);
    expect(post.headers["content-type"]).toBe("application/json");
    // No hostId, no undefined keys: the credential identifies the host, and absent means absent.
    expect(post.body).toEqual({ issueId: "iss-1", body: "hello" });
  });

  it("optional fields ride only when given", async () => {
    const { net, c } = client([() => json(200, { outcome: "succeeded", attempts: 1 })]);
    await c.agent.issueComment({ issueId: "iss-1", body: "hi", parentId: "c-0", createAsUser: "steward" });
    expect(net.calls[1]!.body).toEqual({ issueId: "iss-1", body: "hi", parentId: "c-0", createAsUser: "steward" });
  });

  it("⭐ the path is NEVER a literal: a contract that moves the route is followed", async () => {
    const moved: TenantContract = {
      ...(JSON.parse(JSON.stringify(fixture)) as TenantContract),
      routes: [{ method: "POST", path: "/api/v2/agent/x/issue-comment", takesWriteBudgetUnit: true, since: "2.0.0" }],
    };
    const net = scriptedFetch([() => json(200, moved, { etag: ETAG }), () => json(200, { outcome: "succeeded", attempts: 1 })]);
    const c = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    await c.agent.issueComment({ issueId: "iss-1", body: "hello" });
    expect(net.calls[1]!.url).toBe(`${BASE}/api/v2/agent/x/issue-comment`);
  });

  it("⛔ a route the contract does not list is `route-unknown`, and no POST is made", async () => {
    const without: TenantContract = {
      ...(JSON.parse(JSON.stringify(fixture)) as TenantContract),
      routes: fixture.routes.filter((r) => !r.path.endsWith("/issue-comment")).map((r) => ({ ...r, method: r.method === "GET" ? "GET" : "POST" })),
    };
    const net = scriptedFetch([() => json(200, without, { etag: ETAG }), () => new Error("must not be called")]);
    const c = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const res = await c.agent.issueComment({ issueId: "iss-1", body: "hello" });
    expect(res).toEqual({ outcome: "route-unknown", route: "issue-comment", routes: without.routes.map((r) => r.path) });
    expect(net.calls).toHaveLength(1);
  });

  it("⛔ when the contract cannot be served, the write returns THAT result and makes no POST", async () => {
    const net = scriptedFetch([() => json(403, { error: "forbidden", reason: "not-machine-principal" }), () => new Error("must not be called")]);
    const c = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await c.agent.issueComment({ issueId: "iss-1", body: "hello" })).toMatchObject({ outcome: "forbidden", reason: "not-machine-principal" });
    expect(net.calls).toHaveLength(1);
  });

  it("the contract is fetched once and then served from the cache across writes", async () => {
    const { net, c } = client([() => json(200, { outcome: "succeeded", attempts: 1 })]);
    await c.agent.issueComment({ issueId: "iss-1", body: "a" });
    await c.agent.issueComment({ issueId: "iss-1", body: "b" });
    expect(net.calls.map((x) => x.method)).toEqual(["GET", "POST", "POST"]);
  });
});

describe("the status-to-arm mapping on a write", () => {
  it("429 write budget (no Retry-After) → rate-limited with retryAfterSeconds null; with the header → parsed", async () => {
    const { c } = client([
      () => json(429, { outcome: "rejected", reason: "host write budget exceeded: 3000/3000 writes today (UTC 2026-09-11)" }),
      () => json(429, { outcome: "rejected", reason: "Linear request budget reserved for writes" }, { "Retry-After": "42" }),
    ]);
    expect(await c.agent.issueState({ issueId: "i", stateId: "s" })).toEqual({
      outcome: "rate-limited",
      status: 429,
      reason: "host write budget exceeded: 3000/3000 writes today (UTC 2026-09-11)",
      retryAfterSeconds: null,
    });
    expect(await c.agent.issueState({ issueId: "i", stateId: "s" })).toMatchObject({ outcome: "rate-limited", retryAfterSeconds: 42 });
  });

  it("403 {outcome:rejected, required:mirror:write} (the scope gate in enforce) → forbidden naming the scope", async () => {
    const { c } = client([() => json(403, { outcome: "rejected", reason: "credential lacks mirror:write", required: "mirror:write" })]);
    expect(await c.agent.issueState({ issueId: "i", stateId: "s" })).toEqual({
      outcome: "forbidden",
      status: 403,
      reason: "credential lacks mirror:write",
      required: "mirror:write",
      account: null,
    });
  });

  it("503 {outcome:failed} (the Linear client threw) → failed", async () => {
    const { c } = client([() => json(503, { outcome: "failed", reason: "could not reach Linear" })]);
    expect(await c.agent.issueState({ issueId: "i", stateId: "s" })).toEqual({ outcome: "failed", status: 503, reason: "could not reach Linear" });
  });

  it("400 {outcome:rejected, attempts:0} → rejected with attempts; 400 without attempts (route-level validation) → rejected without", async () => {
    const { c } = client([
      () => json(400, { outcome: "rejected", attempts: 0, reason: "issueId must be a non-empty string" }),
      () => json(400, { outcome: "rejected", reason: "stateId must be a non-empty string" }),
    ]);
    expect(await c.agent.issueState({ issueId: "", stateId: "s" })).toEqual({ outcome: "rejected", status: 400, reason: "issueId must be a non-empty string", attempts: 0 });
    expect(await c.agent.issueState({ issueId: "i", stateId: "" })).toEqual({ outcome: "rejected", status: 400, reason: "stateId must be a non-empty string" });
  });

  it("502 {outcome:exhausted} → exhausted with attempts and lastError", async () => {
    const { c } = client([() => json(502, { outcome: "exhausted", attempts: 3, lastError: "upstream 500" })]);
    expect(await c.agent.issueState({ issueId: "i", stateId: "s" })).toEqual({ outcome: "exhausted", status: 502, attempts: 3, lastError: "upstream 500" });
  });

  it("fetch throwing → network; a 200 with a non-JSON body → shape; an unknown outcome on a 200 → shape", async () => {
    const { c } = client([
      () => new Error("socket hang up"),
      () => text(200, "<html>"),
      () => json(200, { outcome: "mystery" }),
    ]);
    const a = await c.agent.issueState({ issueId: "i", stateId: "s" });
    expect(a.outcome).toBe("network");
    expect(await c.agent.issueState({ issueId: "i", stateId: "s" })).toMatchObject({ outcome: "shape", status: 200 });
    expect(await c.agent.issueState({ issueId: "i", stateId: "s" })).toMatchObject({ outcome: "shape", status: 200 });
  });
});

describe("the per-route bodies", () => {
  it("issueLabel passes results[] through — already-absent intact — with the top-level outcome and status", async () => {
    const body = {
      outcome: "succeeded",
      results: [
        { labelId: "l1", outcome: "succeeded", attempts: 1 },
        { labelId: "l2", outcome: "already-absent", attempts: 1 },
      ],
    };
    const { net, c } = client([
      () => json(200, body),
      () => json(400, { outcome: "failed", results: [{ labelId: "l3", outcome: "rejected", attempts: 1, reason: "no such label" }] }),
    ]);
    expect(await c.agent.issueLabel({ issueId: "i", labelIds: ["l1", "l2"], mode: "remove" })).toEqual({ ...body, status: 200 });
    expect(net.calls[1]!.body).toEqual({ issueId: "i", labelIds: ["l1", "l2"], mode: "remove" });
    expect(net.calls[1]!.url).toBe(`${BASE}${pathOf(fixture, "issue-label")}`);
    expect(await c.agent.issueLabel({ issueId: "i", labelIds: ["l3"], mode: "add" })).toMatchObject({ outcome: "failed", status: 400, results: [{ labelId: "l3", outcome: "rejected" }] });
  });

  it("issueCreate's success carries the identifier, id and url the route hands back", async () => {
    const { net, c } = client([() => json(200, { outcome: "succeeded", attempts: 1, identifier: "ENG-42", id: "iss-42", url: "https://linear.app/acme/issue/ENG-42" })]);
    const res = await c.agent.issueCreate({ teamId: "team-eng", title: "T", labelIds: ["l1"], priority: 2, createAsUser: "intake" });
    expect(res).toEqual({ outcome: "succeeded", status: 200, attempts: 1, identifier: "ENG-42", id: "iss-42", url: "https://linear.app/acme/issue/ENG-42" });
    expect(net.calls[1]!.body).toEqual({ teamId: "team-eng", title: "T", labelIds: ["l1"], priority: 2, createAsUser: "intake" });
    expect(net.calls[1]!.url).toBe(`${BASE}${pathOf(fixture, "issue-create")}`);
  });

  it("reaction sends exactly what it was given (no client-side copy of the server's validation) and passes the three success shapes", async () => {
    const { net, c } = client([
      () => json(200, { outcome: "succeeded", attempts: 1, reactionId: "r1", alreadyPresent: true, userId: "u1" }),
      () => json(200, { outcome: "succeeded", attempts: 1, alreadyAbsent: true, removed: 0 }),
      () => json(400, { outcome: "rejected", reason: "exactly one of issueId / commentId, not both" }),
    ]);
    expect(await c.agent.reaction({ issueId: "i", emoji: "👀" })).toEqual({ outcome: "succeeded", status: 200, attempts: 1, reactionId: "r1", alreadyPresent: true, userId: "u1" });
    expect(net.calls[1]!.body).toEqual({ issueId: "i", emoji: "👀" });
    expect(await c.agent.reaction({ commentId: "c", mode: "remove" })).toEqual({ outcome: "succeeded", status: 200, attempts: 1, alreadyAbsent: true, removed: 0 });
    expect(await c.agent.reaction({ issueId: "i", commentId: "c" })).toMatchObject({ outcome: "rejected", status: 400 });
    expect(net.calls[3]!.body).toEqual({ issueId: "i", commentId: "c" });
  });

  it("attachment upserts and returns the attachment; attachments() is a GET with ?issueId= (the method from the route entry)", async () => {
    const att = { id: "a1", url: "https://x/claim", title: "claim", metadata: { owner: "h1" } };
    const { net, c } = client([
      () => json(200, { outcome: "succeeded", attempts: 1, attachment: att }),
      () => json(200, { outcome: "succeeded", attachments: [att] }),
      () => json(404, { outcome: "rejected", reason: "issue not found" }),
    ]);
    expect(await c.agent.attachment({ issueId: "i", url: att.url, title: "claim", metadata: { owner: "h1" } })).toEqual({ outcome: "succeeded", status: 200, attempts: 1, attachment: att });
    expect(await c.agent.attachments({ issueId: "i" })).toEqual({ outcome: "succeeded", status: 200, attachments: [att] });
    const get = net.calls[2]!;
    expect(get.method).toBe("GET");
    expect(get.url).toBe(`${BASE}${pathOf(fixture, "attachments")}?issueId=i`);
    expect(get.body).toBeNull();
    expect(await c.agent.attachments({ issueId: "nope" })).toEqual({ outcome: "not-found", status: 404, reason: "issue not found" });
  });

  it("session returns the composite body with its worst-of status", async () => {
    const body = {
      session: { outcome: "reused", sessionId: "s1", attempts: 1 },
      activity: { outcome: "exhausted", attempts: 3, lastError: "boom" },
    };
    const { net, c } = client([() => json(502, body)]);
    const res = await c.agent.session({ issueId: "i", activity: { type: "thought", body: "…" }, plan: [{ content: "step 1", status: "inProgress" }] });
    expect(res).toEqual({ outcome: "ok", status: 502, ...body });
    expect(net.calls[1]!.body).toEqual({ issueId: "i", activity: { type: "thought", body: "…" }, plan: [{ content: "step 1", status: "inProgress" }] });
  });

  it("ask passes created / created-but-relations-partial through", async () => {
    const { net, c } = client([
      () => json(200, { outcome: "created", askId: "a1", identifier: "ENG-50" }),
      () => json(200, { outcome: "created-but-relations-partial", askId: "a1", identifier: "ENG-50", failedBlocks: ["ENG-49"], existing: true }),
    ]);
    const input = { teamId: "team-eng", title: "Which?", context: "ctx", defaultIfSilent: "A", askKey: "k1", options: ["A", "B"], blocks: ["ENG-49"] };
    expect(await c.agent.ask(input)).toEqual({ outcome: "created", status: 200, askId: "a1", identifier: "ENG-50" });
    expect(net.calls[1]!.body).toEqual(input);
    expect(await c.agent.ask({ ...input, target: { email: "a@b.c" } })).toMatchObject({ outcome: "created-but-relations-partial", failedBlocks: ["ENG-49"], existing: true });
  });

  it("askAccept maps recorded/refused/record-failed with their statuses", async () => {
    const { c } = client([
      () => json(200, { outcome: "recorded", askIdentifier: "ENG-50", decisionSummary: "A", failedBlockedComments: [], resume: [] }),
      () => json(409, { outcome: "refused", reason: "not-assignee" }),
      () => json(502, { outcome: "record-failed", reason: "write failed" }),
    ]);
    const input = { askIssueId: "a1", answerCommentId: "c1", acceptedByRole: "steward" };
    expect(await c.agent.askAccept(input)).toMatchObject({ outcome: "recorded", status: 200, askIdentifier: "ENG-50" });
    expect(await c.agent.askAccept(input)).toEqual({ outcome: "refused", status: 409, reason: "not-assignee" });
    expect(await c.agent.askAccept(input)).toEqual({ outcome: "record-failed", status: 502, reason: "write failed" });
  });
});
