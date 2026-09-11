// tenant-client-reads.test.ts — CTC-2004 Tier 1, criterion 2: `issues.list({ after })` returns rows
// plus a typed `nextCursor` read from `X-Mirror-Next-Cursor`; and the sibling open reads.
//
// The param NAMES are asserted exactly, because the bundle sends `team=`/`project=` today and the DO
// reads `team_key=`/`team_id=` and no `project` at all — the mismatch is why it re-filters client-side.
// The header names are the mirror's own (`MirrorDO.handleIssues`/`handlePulls`), spelled once here as
// literals AND referenced through the exported constants, so a rename on either side is caught.

import { describe, expect, it } from "vitest";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
import {
  BLOCKED_ON_ASK_TOTAL_HEADER,
  HEAD_SEQ_HEADER,
  NEXT_CURSOR_HEADER,
  TOTAL_HEADER,
  createTenantClient,
  pageCursor,
} from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_user_reader";

const ROW = {
  id: "issue-1",
  identifier: "ENG-1",
  title: "One",
  state: "Todo",
  assignee: null,
  assignee_id: null,
  assignee_name: null,
  assignee_avatar_url: null,
  priority: 2,
  estimate: null,
  project_id: null,
  cycle_id: null,
  team_id: "team-eng",
  sort_order: null,
  updated_at: 100,
  labels: [],
  relations: [],
};

describe("issues.list", () => {
  it("⭐ sends the DO's own param names and reads the next cursor, total and head off the headers", async () => {
    expect(NEXT_CURSOR_HEADER).toBe("X-Mirror-Next-Cursor");
    expect(TOTAL_HEADER).toBe("X-Mirror-Total");
    expect(HEAD_SEQ_HEADER).toBe("X-Mirror-Cursor");
    const net = scriptedFetch([
      () => json(200, [ROW], { "X-Mirror-Next-Cursor": "tok2", "X-Mirror-Total": "298", "X-Mirror-Cursor": "1201" }),
    ]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const res = await client.issues.list({
      after: pageCursor("tok1"),
      limit: 50,
      teamKey: "ENG",
      state: "active",
      priority: 2,
      sort: "priority",
      blocked: "ask",
      waitingOn: "user-1",
      waitingMode: "all",
    });
    expect(net.calls).toHaveLength(1);
    const sent = new URL(net.calls[0]!.url);
    expect(sent.origin + sent.pathname).toBe(`${BASE}/api/v1/issues`);
    expect(Object.fromEntries(sent.searchParams)).toEqual({
      after: "tok1",
      limit: "50",
      team_key: "ENG",
      state: "active",
      priority: "2",
      sort: "priority",
      blocked: "ask",
      waiting_on: "user-1",
      waiting_mode: "all",
    });
    expect(net.calls[0]!.headers["authorization"]).toBe(`Bearer ${KEY}`);
    expect(res).toEqual({ outcome: "ok", rows: [ROW], nextCursor: "tok2", total: 298, head: 1201 });
  });

  it("⭐ the last page has NO next-cursor header → nextCursor: null; absent totals → null", async () => {
    const net = scriptedFetch([() => json(200, [ROW])]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const res = await client.issues.list();
    expect(res).toEqual({ outcome: "ok", rows: [ROW], nextCursor: null, total: null, head: null });
    expect(new URL(net.calls[0]!.url).search).toBe("");
  });

  it("the next cursor is typed: it feeds straight back into `after` and pageCursor() admits a persisted token", async () => {
    const net = scriptedFetch([
      () => json(200, [ROW], { "X-Mirror-Next-Cursor": "tok2" }),
      () => json(200, []),
    ]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const first = await client.issues.list({ limit: 1 });
    if (first.outcome !== "ok" || first.nextCursor === null) throw new Error("expected a next page");
    const second = await client.issues.list({ after: first.nextCursor, limit: 1 });
    expect(second).toMatchObject({ outcome: "ok", rows: [], nextCursor: null });
    expect(new URL(net.calls[1]!.url).searchParams.get("after")).toBe("tok2");
    expect(pageCursor("saved")).toBe("saved");
  });

  it("a malformed cursor is the server's 400 {error} → rejected with that reason", async () => {
    const net = scriptedFetch([() => json(400, { error: "invalid ?after= cursor" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.issues.list({ after: pageCursor("garbage") })).toEqual({
      outcome: "rejected",
      status: 400,
      reason: "invalid ?after= cursor",
    });
  });

  it("a 200 whose body is not an array is a `shape` outcome — no nine-key unwrapping", async () => {
    const net = scriptedFetch([() => json(200, { rows: [ROW] })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.issues.list()).toMatchObject({ outcome: "shape", status: 200 });
  });

  it("a 401 on a read is the same unauthorized arm the contract read answers", async () => {
    const net = scriptedFetch([() => json(401, { error: "unauthorized", reason: "credential-not-accepted", ref: "r1" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.issues.list()).toEqual({ outcome: "unauthorized", status: 401, reason: "credential-not-accepted", ref: "r1" });
  });
});

describe("issues.get", () => {
  it("encodes the identifier into the path and answers the detail with the head", async () => {
    const detail = { ...ROW, comments: [], activity: [] };
    const net = scriptedFetch([() => json(200, detail, { "X-Mirror-Cursor": "77" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const res = await client.issues.get("ENG 1/x");
    expect(net.calls[0]!.url).toBe(`${BASE}/api/v1/issues/ENG%201%2Fx`);
    expect(res).toEqual({ outcome: "ok", issue: detail, head: 77 });
  });

  it("404 {error:'not found'} → not-found, never a thrown string", async () => {
    const net = scriptedFetch([() => json(404, { error: "not found" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.issues.get("ENG-999")).toEqual({ outcome: "not-found", status: 404 });
  });

  it("400 invalid_field (a malformed segment) → rejected naming the field", async () => {
    const net = scriptedFetch([() => json(400, { error: "invalid_field", field: "identifier" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.issues.get("%ZZ")).toEqual({ outcome: "rejected", status: 400, reason: "invalid_field" });
  });
});

describe("pulls", () => {
  const PULL = { repo_id: "acme/api", number: 7, node_id: "PR_7", title: "t", state: "open", checks: [] };

  it("list sends repo/state/blocked/after/limit and reads the blocked-on-ask total too", async () => {
    expect(BLOCKED_ON_ASK_TOTAL_HEADER).toBe("X-Mirror-Blocked-On-Ask-Total");
    const net = scriptedFetch([
      () =>
        json(200, [PULL], {
          "X-Mirror-Next-Cursor": "p2",
          "X-Mirror-Total": "12",
          "X-Mirror-Blocked-On-Ask-Total": "3",
          "X-Mirror-Cursor": "900",
        }),
    ]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const res = await client.pulls.list({ repo: "acme/api", state: "open", blocked: "ask", after: pageCursor("p1"), limit: 5 });
    expect(Object.fromEntries(new URL(net.calls[0]!.url).searchParams)).toEqual({
      repo: "acme/api",
      state: "open",
      blocked: "ask",
      after: "p1",
      limit: "5",
    });
    expect(res).toEqual({ outcome: "ok", rows: [PULL], nextCursor: "p2", total: 12, blockedOnAskTotal: 3, head: 900 });
  });

  it("get encodes the node id and maps 404 to not-found", async () => {
    const net = scriptedFetch([() => json(200, { ...PULL, reviews: [] }), () => json(404, { error: "not found" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.pulls.get("PR_7")).toEqual({ outcome: "ok", pull: { ...PULL, reviews: [] } });
    expect(await client.pulls.get("PR_x")).toEqual({ outcome: "not-found", status: 404 });
    expect(net.calls[0]!.url).toBe(`${BASE}/api/v1/pulls/PR_7`);
  });
});

describe("projects.list and me", () => {
  it("projects.list is offset-paged: limit + the legacy numeric `cursor` param", async () => {
    const project = { id: "p1", name: "Alpha", state: "started", initiatives: [] };
    const net = scriptedFetch([() => json(200, [project], { "X-Mirror-Cursor": "5" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    const res = await client.projects.list({ limit: 10, offset: 20 });
    expect(Object.fromEntries(new URL(net.calls[0]!.url).searchParams)).toEqual({ limit: "10", cursor: "20" });
    expect(res).toEqual({ outcome: "ok", rows: [project], head: 5 });
  });

  it("me() returns the five identity fields and refuses an unknown principal as `shape`", async () => {
    const me = { account: "acct-1", slug: "acme", name: "Acme", permissions: ["mirror:read"], principal: "service" };
    const net = scriptedFetch([() => json(200, me), () => json(200, { ...me, principal: "robot" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.me()).toEqual({ outcome: "ok", ...me });
    expect(net.calls[0]!.url).toBe(`${BASE}/api/v1/me`);
    expect(await client.me()).toMatchObject({ outcome: "shape", status: 200 });
  });

  it("me() on a non-operational account is the server's 403 → forbidden with the account named", async () => {
    const net = scriptedFetch([() => json(403, { error: "forbidden", reason: "account-not-operational", account: "acct-1" })]);
    const client = createTenantClient({ key: KEY, baseUrl: BASE, fetch: net.fetch });
    expect(await client.me()).toEqual({ outcome: "forbidden", status: 403, reason: "account-not-operational", required: null, account: "acct-1" });
  });
});
