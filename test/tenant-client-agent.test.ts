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
import {
  AGENT_ROUTE_NAMES,
  createTenantClient,
  memoryContractCache,
  readTenantContract,
  routeByName,
  type AgentRouteName,
  type TenantContract,
} from "../src/index";

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

  it("projectRepositoryRegister ⭐ POSTs to the contract's project-repositories path with exactly the given body, and passes {registered,created,linked} through with outcome stamped on", async () => {
    const registered = { repoId: "tenant-0:coalesce-labs__dev-skills", owner: "coalesce-labs", name: "dev-skills" };
    const { net, c } = client([() => json(200, { registered, created: true, linked: true })]);
    const res = await c.agent.projectRepositoryRegister({ repository: "coalesce-labs/dev-skills", project: "proj-1" });
    expect(res).toEqual({ outcome: "registered", status: 200, registered, created: true, linked: true });
    expect(net.calls[1]!.method).toBe("POST");
    expect(net.calls[1]!.url).toBe(`${BASE}${pathOf(fixture, "project-repositories")}`);
    expect(net.calls[1]!.body).toEqual({ repository: "coalesce-labs/dev-skills", project: "proj-1" });
  });

  it("projectRepositoryRegister sends the address field it was given (project / teamKey / teamId) and no others", async () => {
    const { net, c } = client([() => json(200, { registered: { repoId: "r", owner: "o", name: "n" }, created: false, linked: true })]);
    await c.agent.projectRepositoryRegister({ repository: "o/n", teamKey: "ENG" });
    expect(net.calls[1]!.body).toEqual({ repository: "o/n", teamKey: "ENG" });
  });

  it("projectRepositoryRemove ⭐ reaches the NESTED path, not the register route, and {removed:false} is a removed success, not a failure", async () => {
    const { net, c } = client([() => json(200, { removed: false })]);
    const res = await c.agent.projectRepositoryRemove({ repository: "coalesce-labs/dev-skills", teamId: "team-eng" });
    expect(res).toEqual({ outcome: "removed", status: 200, removed: false });
    expect(net.calls[1]!.url).toBe(`${BASE}${pathOf(fixture, "project-repositories/remove")}`);
    expect(net.calls[1]!.url).not.toBe(`${BASE}${pathOf(fixture, "project-repositories")}`);
  });

  it("project-repositories error arms: 403 forbidden, 404 not-found, 400 rejected, and 409/503 preserved as http with the route's literal as reason", async () => {
    const { c } = client([
      () => json(403, { error: "forbidden", message: "managing repositories requires an admin or owner role" }),
      () => json(404, { error: "project_not_found" }),
      () => json(400, { error: "github_not_connected" }),
      () => json(409, { error: "registry_not_migrated" }),
      () => json(503, { error: "github_unverified" }),
    ]);
    // reasonOf reads `reason ?? error ?? message`; this shape has no `reason` field, so the route's
    // own error LITERAL ("forbidden") wins over the human-readable `message` — the plan's documented
    // behavior for classify()'s shared auth-plane handling, not something this route special-cases.
    expect(await c.agent.projectRepositoryRegister({ repository: "o/n", project: "p" })).toEqual({
      outcome: "forbidden",
      status: 403,
      reason: "forbidden",
      required: null,
      account: null,
    });
    expect(await c.agent.projectRepositoryRegister({ repository: "o/n", project: "p" })).toEqual({
      outcome: "not-found",
      status: 404,
      reason: "project_not_found",
    });
    expect(await c.agent.projectRepositoryRegister({ repository: "o/n", project: "p" })).toEqual({
      outcome: "rejected",
      status: 400,
      reason: "github_not_connected",
    });
    expect(await c.agent.projectRepositoryRegister({ repository: "o/n", project: "p" })).toEqual({
      outcome: "http",
      status: 409,
      reason: "registry_not_migrated",
    });
    expect(await c.agent.projectRepositoryRemove({ repository: "o/n", project: "p" })).toEqual({
      outcome: "http",
      status: 503,
      reason: "github_unverified",
    });
  });

  it("project-repositories routes are route-unknown, with no POST, when the contract omits them", async () => {
    const without: TenantContract = {
      ...(JSON.parse(JSON.stringify(fixture)) as TenantContract),
      routes: fixture.routes
        .filter((r) => !r.path.includes("project-repositories"))
        .map((r) => ({ ...r, method: r.method === "GET" ? ("GET" as const) : ("POST" as const) })),
    };
    const net = scriptedFetch([() => json(200, without, { etag: ETAG }), () => new Error("must not be called")]);
    const c = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await c.agent.projectRepositoryRegister({ repository: "o/n", project: "p" })).toEqual({
      outcome: "route-unknown",
      route: "project-repositories",
      routes: without.routes.map((r) => r.path),
    });
    expect(net.calls).toHaveLength(1);
  });

  it("⛔ a 404 from the CONTRACT fetch is returned unchanged, NOT lifted to the route's project not-found arm", async () => {
    // The route answers 404 `{error:"project_not_found"}` for a project that is absent, archived or
    // another tenant's, and only THAT is `not-found`. A 404 on GET /api/v1/agent/contract — a wrong
    // baseUrl, a tenant whose contract endpoint is not deployed — says nothing about the caller's
    // project, so it stays the shared `http` arm every other agent verb returns for it.
    const register = scriptedFetch([() => json(404, { error: "not_found" }), () => new Error("must not be called")]);
    const rc = createTenantClient({ key: KEY, baseUrl: BASE, fetch: register.fetch, contractCache: memoryContractCache() });
    expect(await rc.agent.projectRepositoryRegister({ repository: "o/n", project: "p" })).toEqual({
      outcome: "http",
      status: 404,
      reason: "not_found",
    });
    expect(register.calls).toHaveLength(1);

    const remove = scriptedFetch([() => json(404, { error: "not_found" }), () => new Error("must not be called")]);
    const mc = createTenantClient({ key: KEY, baseUrl: BASE, fetch: remove.fetch, contractCache: memoryContractCache() });
    expect(await mc.agent.projectRepositoryRemove({ repository: "o/n", project: "p" })).toEqual({
      outcome: "http",
      status: 404,
      reason: "not_found",
    });
    expect(remove.calls).toHaveLength(1);
  });
});

describe("⭐ the drift guard (CTC-2562): every route this SDK wraps must resolve against the committed fixture", () => {
  /** Compile-time exact type equality — the device test/tenant-contract.test.ts already uses. */
  type TypesAreEqual<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
  const namesInLockstep: TypesAreEqual<(typeof AGENT_ROUTE_NAMES)[number], AgentRouteName> = true;

  it("the runtime AGENT_ROUTE_NAMES list is kept in lockstep with the AgentRouteName union at compile time", () => {
    expect(namesInLockstep).toBe(true);
  });

  it("every route this client wraps exists in the committed contract fixture", () => {
    const doc = readTenantContract(fixture);
    if (doc === null) throw new Error("the committed fixture does not read as a TenantContract");
    for (const name of AGENT_ROUTE_NAMES) expect([name, routeByName(doc, name) !== null]).toEqual([name, true]);
  });
});
