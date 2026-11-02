// ticket-lease-client.test.ts — CTC-8872: the seat ticket lease client over the cloud's literal
// /api/v1/agent/ticket-lease(s) routes. The four ticket scenarios run first (take with holder
// details, a store refusal that resolves, a validation refusal that throws, a team list); the rest
// pin the wire shape: exact URLs, bearer, JSON bodies with null-in/undefined-out serialization,
// error decoding, timeout aborts and malformed-body rejection. Every server body comes from the
// committed fixture (test/fixtures/ticket-lease-answers.json, provenance beside it) or is an inline
// equivalent, so the tests never depend on a live cloud.

import { describe, expect, it } from "vitest";
import fixtureJson from "./fixtures/ticket-lease-answers.json";
import { empty, json, scriptedFetch, text, type Scripted } from "./helpers/scripted-fetch";
import {
  createTicketLeaseClient,
  TicketLeaseHttpError,
  type ListTicketLeasesResponse,
  type ReadTicketLeaseResponse,
  type ReleaseSeatTicketLeaseResponse,
  type RenewSeatTicketLeaseParams,
  type RenewSeatTicketLeaseResponse,
  type SeatTicketLeaseProgress,
  type SeatTicketLeaseRowWire,
  type TakeSeatTicketLeaseResponse,
  type TicketLeaseClientOptions,
  type TicketLeaseHolderDetails,
  type TicketLeaseHolderDetailsInput,
} from "../src/index";

const BASE = "https://cloud.example";
const KEY = "ctc_acct_writer";
const TAKE_URL = `${BASE}/api/v1/agent/ticket-lease/take`;

// A JSON import widens every string literal, so the fixture cannot assign to the response unions
// directly (TS2322 on `outcome`). The cast only names the declared shapes for compile-time use;
// every body is also driven through the client's own runtime guards by the transport tests below,
// which is the real pin. Tracked by CTC-8872; swap for `@catalyst-cloud/protocol` types when
// published.
const fixture = fixtureJson as unknown as {
  takeTaken: TakeSeatTicketLeaseResponse;
  takeRefusedHeld: TakeSeatTicketLeaseResponse;
  takeRejectedContact: { outcome: "rejected"; code: string; field: string; reason: string };
  renewRenewed: RenewSeatTicketLeaseResponse;
  renewRefusedStale: RenewSeatTicketLeaseResponse;
  renewRejectedKindChanged: { outcome: "rejected"; code: string; field: string; reason: string };
  releaseReleased: ReleaseSeatTicketLeaseResponse;
  readNeverLeased: ReadTicketLeaseResponse;
  readLive: ReadTicketLeaseResponse;
  listLive: ListTicketLeasesResponse;
  listRecentNext: ListTicketLeasesResponse;
};

function leaseClient(script: Scripted[], options: Partial<TicketLeaseClientOptions> = {}) {
  const net = scriptedFetch(script);
  const c = createTicketLeaseClient({ baseUrl: BASE, token: KEY, fetch: net.fetch, ...options });
  return { net, c };
}

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error("expected the call to throw");
}

describe("take — the ticket's first three scenarios", () => {
  it("⭐ scenario 1: take with holder details resolves taken with the lease's holder details", async () => {
    const { net, c } = leaseClient([() => json(200, fixture.takeTaken)]);
    const res = await c.take({ ticket: "CTC-6480", agent: "factory-droid", holderDetails: { kind: "factory" } });
    expect(res.outcome).toBe("taken");
    if (res.outcome !== "taken") throw new Error("unreachable");
    expect(res.lease.holderDetails?.kind).toBe("factory");
    expect(res.lease.holderDetails?.displayName).toBe("factory-droid");
    expect(res.lease.holderDetails?.owner).toEqual({
      type: "organization-key",
      keyId: "key_factory",
      keyName: "factory-trial",
    });
    expect(res.minted).toBe(true);
    expect(res.adopted).toEqual([]);
    expect(net.calls).toHaveLength(1);
    const post = net.calls[0];
    expect(post.method).toBe("POST");
    expect(post.url).toBe(TAKE_URL);
    expect(post.headers["authorization"]).toBe(`Bearer ${KEY}`);
    expect(post.headers["content-type"]).toBe("application/json");
    expect(post.body).toEqual({ ticket: "CTC-6480", agent: "factory-droid", holderDetails: { kind: "factory" } });
  });

  it("⭐ scenario 2: a held ticket resolves refused with ticket_lease_held, not a throw", async () => {
    const { c } = leaseClient([() => json(200, fixture.takeRefusedHeld)]);
    const res = await c.take({ ticket: "CTC-6480", agent: "factory-droid", holderDetails: { kind: "factory" } });
    expect(res.outcome).toBe("refused");
    if (res.outcome !== "refused") throw new Error("unreachable");
    expect(res.refusal.reason).toBe("ticket_lease_held");
    expect(res.refusal.current?.holderDetails?.displayName).toBe("review-2");
    expect(res.refusal.current?.holderDetails?.owner).toEqual({ type: "person", userId: "user_2f9c", name: "Ryan" });
    // The boundary's decoration passes through unchanged: both reasons survive, neither is rewritten.
    expect(res).toEqual(fixture.takeRefusedHeld);
  });

  it("⭐ scenario 3: an email contact is sent as given, and the 400 throws with the server's code", async () => {
    const { net, c } = leaseClient([() => json(400, fixture.takeRejectedContact)]);
    const error = (await rejection(
      c.take({ ticket: "CTC-6480", agent: "factory-droid", holderDetails: { kind: "factory", contact: "ops@example.com" } }),
    )) as TicketLeaseHttpError;
    // No preflight: the contact reached the server verbatim.
    expect(net.calls[0].body).toEqual({
      ticket: "CTC-6480",
      agent: "factory-droid",
      holderDetails: { kind: "factory", contact: "ops@example.com" },
    });
    expect(error).toBeInstanceOf(TicketLeaseHttpError);
    expect(error.name).toBe("TicketLeaseHttpError");
    expect(error.status).toBe(400);
    expect(error.code).toBe("holder_contact_invalid");
    expect(error.field).toBe("contact");
    expect(error.reason).toBe(fixture.takeRejectedContact.reason);
    expect(error.message).toBe(
      `ticket lease: server returned 400 holder_contact_invalid: ${fixture.takeRejectedContact.reason}`,
    );
  });

  it("trims a trailing slash from the base URL; ttlMs rides only when given", async () => {
    const net = scriptedFetch([() => json(200, fixture.takeTaken), () => json(200, fixture.takeTaken)]);
    const c = createTicketLeaseClient({ baseUrl: `${BASE}/`, token: KEY, fetch: net.fetch });
    await c.take({ ticket: "CTC-6480", agent: "factory-droid" });
    await c.take({ ticket: "CTC-6480", agent: "factory-droid", ttlMs: 600_000 });
    expect(net.calls[0].url).toBe(TAKE_URL);
    expect(net.calls[0].body).toEqual({ ticket: "CTC-6480", agent: "factory-droid" });
    expect(net.calls[1].body).toEqual({ ticket: "CTC-6480", agent: "factory-droid", ttlMs: 600_000 });
  });
});

describe("HTTP failures throw TicketLeaseHttpError", () => {
  it("carries the server's status, code, field and reason for 401, 403, 429 and 500", async () => {
    for (const status of [401, 403, 429, 500]) {
      const { c } = leaseClient([() => json(status, { code: "invalid_request", field: "token", reason: "nope" })]);
      const error = (await rejection(c.take({ ticket: "CTC-6480", agent: "factory-droid" }))) as TicketLeaseHttpError;
      expect(error, `status ${status}`).toBeInstanceOf(TicketLeaseHttpError);
      expect(error.name, `status ${status}`).toBe("TicketLeaseHttpError");
      expect(error.status, `status ${status}`).toBe(status);
      expect(error.code, `status ${status}`).toBe("invalid_request");
      expect(error.field, `status ${status}`).toBe("token");
      expect(error.reason, `status ${status}`).toBe("nope");
      expect(error.message, `status ${status}`).toBe(`ticket lease: server returned ${status} invalid_request: nope`);
    }
  });

  it("extracts the reason from error, then message, when reason is absent", async () => {
    const first = leaseClient([() => json(500, { error: "worker stuck" })]);
    const fromError = (await rejection(first.c.take({ ticket: "CTC-6480", agent: "a" }))) as TicketLeaseHttpError;
    expect(fromError.reason).toBe("worker stuck");
    expect(fromError.code).toBeNull();
    const second = leaseClient([() => json(500, { message: "worker stuck badly" })]);
    const fromMessage = (await rejection(second.c.take({ ticket: "CTC-6480", agent: "a" }))) as TicketLeaseHttpError;
    expect(fromMessage.reason).toBe("worker stuck badly");
  });

  it("reads a non-JSON error body as text: null code and field, reason capped at 200 characters", async () => {
    const body = "<html>502 Bad Gateway</html>".repeat(30);
    const { c } = leaseClient([() => text(502, body)]);
    const error = (await rejection(c.take({ ticket: "CTC-6480", agent: "a" }))) as TicketLeaseHttpError;
    expect(error).toBeInstanceOf(TicketLeaseHttpError);
    expect(error.code).toBeNull();
    expect(error.field).toBeNull();
    expect(error.reason).toBe(body.slice(0, 200));
    expect(error.message).toBe(`ticket lease: server returned 502: ${body.slice(0, 200)}`);
  });

  it("an empty error body yields the bare status message", async () => {
    const { c } = leaseClient([() => empty(500)]);
    const error = (await rejection(c.take({ ticket: "CTC-6480", agent: "a" }))) as TicketLeaseHttpError;
    expect(error.code).toBeNull();
    expect(error.reason).toBe("");
    expect(error.message).toBe("ticket lease: server returned 500");
  });

  it("a malformed JSON error body falls back to its text", async () => {
    const { c } = leaseClient([() => text(500, "{oops")]);
    const error = (await rejection(c.take({ ticket: "CTC-6480", agent: "a" }))) as TicketLeaseHttpError;
    expect(error.code).toBeNull();
    expect(error.reason).toBe("{oops");
  });

  it("propagates a network failure as the original error, unwrapped", async () => {
    const networkError = new Error("dns lookup failed");
    const net = scriptedFetch([() => networkError]);
    const c = createTicketLeaseClient({ baseUrl: BASE, token: KEY, fetch: net.fetch });
    const error = await rejection(c.take({ ticket: "CTC-6480", agent: "a" }));
    expect(error).toBe(networkError);
  });

  it("aborts a hanging request at the configured deadline and propagates the timeout error", async () => {
    let signal: AbortSignal | undefined;
    const hanging: typeof fetch = (_input, init) =>
      new Promise<Response>((_resolve, reject) => {
        signal = init?.signal ?? undefined;
        init?.signal?.addEventListener("abort", () => reject(init.signal?.reason));
      });
    const c = createTicketLeaseClient({ baseUrl: BASE, token: KEY, fetch: hanging, timeoutMs: 25 });
    const startedAt = Date.now();
    const error = (await rejection(c.take({ ticket: "CTC-6480", agent: "a" }))) as DOMException;
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    expect(signal?.aborted).toBe(true);
    expect(error).toBeInstanceOf(DOMException);
    expect(error.name).toBe("TimeoutError");
  });

  it("keeps the deadline armed through body reading", async () => {
    // A real Response cannot hang mid-body, so this double is the only way to prove the abort
    // signal stays honored while the body is read; it rejects with the signal's own reason, which
    // the client must propagate unwrapped. Tracked by CTC-8872.
    const slowBody: typeof fetch = (_input, init) =>
      Promise.resolve({
        ok: true,
        status: 200,
        text: () =>
          new Promise<string>((_resolve, reject) => {
            init?.signal?.addEventListener("abort", () => reject(init.signal?.reason));
          }),
      } as unknown as Response);
    const c = createTicketLeaseClient({ baseUrl: BASE, token: KEY, fetch: slowBody, timeoutMs: 25 });
    const error = (await rejection(c.take({ ticket: "CTC-6480", agent: "a" }))) as DOMException;
    expect(error).toBeInstanceOf(DOMException);
    expect(error.name).toBe("TimeoutError");
  });
});

describe("malformed 2xx bodies are rejected, additive fields are not", () => {
  const junk: Array<[string, Response]> = [
    ["an array body", json(200, [1, 2])],
    ["a null body", json(200, null)],
    ["invalid JSON", text(200, "{oops")],
    ["an unknown outcome", json(200, { outcome: "deployed" })],
    ["another method's outcome", json(200, fixtureJson.renewRenewed)],
    ["a missing successful lease", json(200, { outcome: "taken", minted: true, adopted: [] })],
    ["a refusal without a refusal block", json(200, { outcome: "refused" })],
    [
      "an incomplete refusal",
      json(200, { outcome: "refused", refusal: { reason: "ticket_lease_held" } }),
    ],
  ];

  it("throws the invalid-response error for each, with the actual status", async () => {
    for (const [name, response] of junk) {
      const { c } = leaseClient([() => response]);
      const error = (await rejection(c.take({ ticket: "CTC-6480", agent: "a" }))) as TicketLeaseHttpError;
      expect(error, name).toBeInstanceOf(TicketLeaseHttpError);
      expect(error.status, name).toBe(200);
      expect(error.code, name).toBeNull();
      expect(error.field, name).toBeNull();
      expect(error.reason, name).toBe("invalid ticket lease response");
    }
  });

  it("accepts additive server fields on a successful body", async () => {
    const decorated = { ...(fixture.takeTaken as object), futureField: { note: "added later" } };
    const { c } = leaseClient([() => json(200, decorated)]);
    const res = await c.take({ ticket: "CTC-6480", agent: "factory-droid" });
    expect(res.outcome).toBe("taken");
  });
});

describe("renew", () => {
  it("sends exactly the given params: explicit nulls ride, undefined keys disappear", async () => {
    const { net, c } = leaseClient([() => json(200, fixture.renewRenewed)]);
    const res = await c.renew({
      ticket: "CTC-6480",
      agent: "factory-droid",
      generation: 1,
      progress: { kind: "pr", ref: "15901" },
      holderDetails: { externalId: "s_91aa", contact: null },
    });
    expect(res.outcome).toBe("renewed");
    if (res.outcome !== "renewed") throw new Error("unreachable");
    expect(res.lease.holderDetails?.externalId).toBe("s_91aa");
    expect(res.lease.holderDetails?.contact).toBeNull();
    expect(net.calls[0].url).toBe(`${BASE}/api/v1/agent/ticket-lease/renew`);
    expect(net.calls[0].method).toBe("POST");
    expect(net.calls[0].headers["authorization"]).toBe(`Bearer ${KEY}`);
    expect(net.calls[0].body).toEqual({
      ticket: "CTC-6480",
      agent: "factory-droid",
      generation: 1,
      progress: { kind: "pr", ref: "15901" },
      holderDetails: { externalId: "s_91aa", contact: null },
    });
  });

  it("ttlMs rides only when given", async () => {
    const { net, c } = leaseClient([() => json(200, fixture.renewRenewed), () => json(200, fixture.renewRenewed)]);
    const base: RenewSeatTicketLeaseParams = {
      ticket: "CTC-6480",
      agent: "factory-droid",
      generation: 1,
      progress: { kind: "commit", ref: "405a0b47" },
    };
    const first = await c.renew(base);
    const second = await c.renew({ ...base, ttlMs: 600_000 });
    expect(first.outcome).toBe("renewed");
    expect(second.outcome).toBe("renewed");
    expect(net.calls[0].body).toEqual(base);
    expect(net.calls[1].body).toEqual({ ...base, ttlMs: 600_000 });
  });

  it("a stale_generation renew resolves refused with the current holder's details", async () => {
    const { c } = leaseClient([() => json(200, fixture.renewRefusedStale)]);
    const res = await c.renew({
      ticket: "CTC-6480",
      agent: "factory-droid",
      generation: 1,
      progress: { kind: "pr", ref: "15902" },
    });
    expect(res.outcome).toBe("refused");
    if (res.outcome !== "refused") throw new Error("unreachable");
    expect(res.refusal.reason).toBe("stale_generation");
    expect(res.refusal.current?.holderDetails?.displayName).toBe("factory-droid");
    expect(res).toEqual(fixture.renewRefusedStale);
  });

  it("a no_new_progress renew resolves refused", async () => {
    const { c } = leaseClient([
      () =>
        json(200, {
          outcome: "refused",
          refusal: { reason: "no_new_progress", current: null, occupied: null, retryAtMs: null },
          code: "operation_refused",
          reason: "request refused",
        }),
    ]);
    const res = await c.renew({
      ticket: "CTC-6480",
      agent: "factory-droid",
      generation: 2,
      progress: { kind: "pr", ref: "15901" },
    });
    expect(res.outcome).toBe("refused");
    if (res.outcome !== "refused") throw new Error("unreachable");
    expect(res.refusal.reason).toBe("no_new_progress");
    expect(res.refusal.current).toBeNull();
  });

  it("a 400 holder_kind_changed throws with the server's code and field", async () => {
    const { c } = leaseClient([() => json(400, fixture.renewRejectedKindChanged)]);
    const error = (await rejection(
      c.renew({
        ticket: "CTC-6480",
        agent: "factory-droid",
        generation: 2,
        progress: { kind: "pr", ref: "15902" },
        holderDetails: { kind: "claude-code" },
      }),
    )) as TicketLeaseHttpError;
    expect(error).toBeInstanceOf(TicketLeaseHttpError);
    expect(error.status).toBe(400);
    expect(error.code).toBe("holder_kind_changed");
    expect(error.field).toBe("kind");
  });
});

describe("release", () => {
  it("sends ticket, agent and generation, and preserves nullable holder details", async () => {
    const { net, c } = leaseClient([() => json(200, fixture.releaseReleased)]);
    const res = await c.release({ ticket: "CTC-6480", agent: "factory-droid", generation: 3 });
    expect(res.outcome).toBe("released");
    if (res.outcome !== "released") throw new Error("unreachable");
    expect(res.lease.holderDetails?.externalId).toBe("s_91aa");
    expect(res.lease.holderDetails?.contact).toBeNull();
    expect(net.calls[0].url).toBe(`${BASE}/api/v1/agent/ticket-lease/release`);
    expect(net.calls[0].method).toBe("POST");
    expect(net.calls[0].body).toEqual({ ticket: "CTC-6480", agent: "factory-droid", generation: 3 });
  });

  it("a not_holder release resolves refused", async () => {
    const { c } = leaseClient([
      () =>
        json(200, {
          outcome: "refused",
          refusal: { reason: "not_holder", current: null, occupied: null, retryAtMs: null },
          code: "operation_refused",
          reason: "request refused",
        }),
    ]);
    const res = await c.release({ ticket: "CTC-6480", agent: "factory-droid", generation: 1 });
    expect(res.outcome).toBe("refused");
    if (res.outcome !== "refused") throw new Error("unreachable");
    expect(res.refusal.reason).toBe("not_holder");
  });
});

describe("read", () => {
  it("sends a URL-encoded ticket and no body, and preserves a never-leased null", async () => {
    const { net, c } = leaseClient([() => json(200, fixture.readNeverLeased)]);
    const res = await c.read("CTC-6480");
    expect(res).toEqual(fixture.readNeverLeased);
    expect(res.lease).toBeNull();
    expect(net.calls[0].method).toBe("GET");
    expect(net.calls[0].url).toBe(`${BASE}/api/v1/agent/ticket-lease?ticket=CTC-6480`);
    expect(net.calls[0].body).toBeNull();
    expect(net.calls[0].headers["authorization"]).toBe(`Bearer ${KEY}`);
    expect("content-type" in net.calls[0].headers).toBe(false);
  });

  it("URL-encodes an awkward ticket id", async () => {
    const { net, c } = leaseClient([() => json(200, fixture.readNeverLeased)]);
    await c.read("CTC 6480/x");
    expect(net.calls[0].url).toBe(`${BASE}/api/v1/agent/ticket-lease?ticket=CTC+6480%2Fx`);
  });

  it("preserves holderDetails, lastHolderDetails, delegations and lastHandoff", async () => {
    const { c } = leaseClient([() => json(200, fixture.readLive)]);
    const res = await c.read("CTC-6480");
    expect(res).toEqual(fixture.readLive);
    if (res.lease === null) throw new Error("unreachable");
    expect(res.lease.holderDetails?.displayName).toBe("factory-droid");
    expect(res.lease.lastHolderDetails?.displayName).toBe("review-2");
    expect(res.delegations[0]?.executor).toBe("review-2");
    expect(res.lastHandoff?.completedStage).toBe("implement");
  });

  it("a body missing its required keys is invalid; a GET server error follows the shared path", async () => {
    const first = leaseClient([() => json(200, { ticket: "CTC-6480" })]);
    const invalid = (await rejection(first.c.read("CTC-6480"))) as TicketLeaseHttpError;
    expect(invalid).toBeInstanceOf(TicketLeaseHttpError);
    expect(invalid.reason).toBe("invalid ticket lease response");
    const second = leaseClient([() => json(500, { code: "internal", reason: "boom" })]);
    const serverError = (await rejection(second.c.read("CTC-6480"))) as TicketLeaseHttpError;
    expect(serverError).toBeInstanceOf(TicketLeaseHttpError);
    expect(serverError.status).toBe(500);
    expect(serverError.code).toBe("internal");
  });
});

describe("list", () => {
  it("⭐ scenario 4: resolves a team's three live leases and a null next cursor", async () => {
    const { net, c } = leaseClient([() => json(200, fixture.listLive)]);
    const page = await c.list({ team: "CTC" });
    expect(page.leases.map((row) => row.ticket)).toEqual(["CTC-6480", "CTC-6477", "CTC-6476"]);
    expect(page.leases[0]?.holderDetails?.displayName).toBe("factory-droid");
    expect(page.leases[2]?.holderDetails).toBeNull();
    expect(page.leases[2]?.lastHolderDetails?.displayName).toBe("review-2");
    expect(page.nextCursor).toBeNull();
    expect(net.calls).toHaveLength(1);
    expect(net.calls[0].method).toBe("GET");
    expect(net.calls[0].url).toBe(`${BASE}/api/v1/agent/ticket-leases?team=CTC`);
    expect(net.calls[0].body).toBeNull();
  });

  it("rides state, limit and an opaque cursor; a non-null nextCursor makes no second request", async () => {
    const { net, c } = leaseClient([() => json(200, fixture.listRecentNext)]);
    const page = await c.list({ team: "CTC/A", state: "recent", limit: 2, cursor: "opaque+/=" });
    expect(page.nextCursor).toBe("opaque+/=");
    expect(page.leases).toEqual([]);
    expect(net.calls).toHaveLength(1);
    expect(net.calls[0].url).toBe(
      `${BASE}/api/v1/agent/ticket-leases?team=CTC%2FA&state=recent&limit=2&cursor=opaque%2B%2F%3D`,
    );
  });

  it("rejects malformed list bodies", async () => {
    const junk: Array<[string, unknown]> = [
      ["missing leases and nextCursor", { team: "CTC", state: "live" }],
      ["a wrong state", { team: "CTC", state: "all", leases: [], nextCursor: null }],
      ["missing nextCursor", { team: "CTC", state: "live", leases: [] }],
      [
        "a malformed holder details row",
        { team: "CTC", state: "live", leases: [{ ticket: "CTC-6480", holderDetails: 7 }], nextCursor: null },
      ],
    ];
    for (const [name, body] of junk) {
      const { c } = leaseClient([() => json(200, body)]);
      const error = (await rejection(c.list({ team: "CTC" }))) as TicketLeaseHttpError;
      expect(error, name).toBeInstanceOf(TicketLeaseHttpError);
      expect(error.reason, name).toBe("invalid ticket lease response");
    }
  });
});

describe("root exports and the protocol-derived types", () => {
  it("exports createTicketLeaseClient and TicketLeaseHttpError from the root", () => {
    expect(typeof createTicketLeaseClient).toBe("function");
    const error = new TicketLeaseHttpError(400, "holder_contact_invalid", "not a link", "contact");
    expect(error).toBeInstanceOf(Error);
    expect(error.status).toBe(400);
    expect(error.code).toBe("holder_contact_invalid");
    expect(error.field).toBe("contact");
    expect(error.reason).toBe("not a link");
    expect(error.name).toBe("TicketLeaseHttpError");
    expect(error.message).toBe("ticket lease: server returned 400 holder_contact_invalid: not a link");
  });

  it("compiles the fixture bodies under the canonical names, holder details included", () => {
    const taken: TakeSeatTicketLeaseResponse = fixture.takeTaken;
    const refused: TakeSeatTicketLeaseResponse = fixture.takeRefusedHeld;
    if (taken.outcome !== "taken" || refused.outcome !== "refused") throw new Error("fixture arms changed");
    const holder: TicketLeaseHolderDetails | null = taken.lease.holderDetails;
    const incumbent: SeatTicketLeaseRowWire | null = refused.refusal.current;
    expect(holder?.owner).toEqual({ type: "organization-key", keyId: "key_factory", keyName: "factory-trial" });
    expect(incumbent?.holderDetails?.displayName).toBe("review-2");
  });

  it("takes holder details on take and explicit null clears on renew", () => {
    const details: TicketLeaseHolderDetailsInput = { kind: "factory", displayName: "factory-droid" };
    const patch: RenewSeatTicketLeaseParams["holderDetails"] = { contact: null, externalId: "s_91aa" };
    const progress: SeatTicketLeaseProgress = { kind: "pr", ref: "15901", note: "CI green" };
    expect(details.kind).toBe("factory");
    expect(patch?.contact).toBeNull();
    expect(progress.note).toBe("CI green");
  });

  it("rejects an invalid progress kind and a mistyped detail at compile time", () => {
    // Deliberate compile-time negative cases (CTC-8872 type fixture): each @ts-expect-error is
    // asserted unused by tsc — the error is the test.
    // @ts-expect-error CTC-8872: "deploy" is not a progress kind
    const badProgress: SeatTicketLeaseProgress = { kind: "deploy", ref: "15901" };
    // @ts-expect-error CTC-8872: displayName must be a string
    const badDetail: TicketLeaseHolderDetailsInput = { kind: "factory", displayName: 7 };
    expect(badProgress.ref).toBe("15901");
    expect(badDetail.kind).toBe("factory");
  });
});
