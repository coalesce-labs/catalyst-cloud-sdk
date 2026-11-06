// Complete cloud fixtures captured at 2a05ad4a and 684ef744. Do not remove route rows
// to make the parser pass: these are the documents actual consumers receive.
import { describe, expect, it } from "vitest";
import main from "./fixtures/main-2a05ad4a.json";
import shipping from "./fixtures/shipping-684ef744.json";
import { isTenantContract, readTenantContract, type ContractRoute } from "../src/tenant-contract";
import { createTenantClient } from "../src/tenant-client";

const fixtures = [
  { name: "main 2a05ad4a", doc: main },
  { name: "shipping 684ef744", doc: shipping },
];
const reviewWrite = {
  method: "PUT",
  path: "/api/v1/agent/tenant/review-agents/write",
  takesWriteBudgetUnit: false,
  since: "2.8.0",
  idempotencyKeyField: null,
} as const;

const typedReviewWrite: ContractRoute = reviewWrite;
// @ts-expect-error An arbitrary PUT must not be expressible as a ContractRoute.
const arbitraryTypedPut: ContractRoute = { ...reviewWrite, path: "/api/v1/agent/custom" };
// @ts-expect-error The known PUT must preserve its write-budget metadata.
const alteredTypedPut: ContractRoute = { ...reviewWrite, takesWriteBudgetUnit: true };
// @ts-expect-error DELETE remains unsupported in the legacy route catalog.
const typedDelete: ContractRoute = { ...reviewWrite, method: "DELETE" };
// @ts-expect-error The known PUT has a fixed introduction version.
const alteredTypedSince: ContractRoute = { ...reviewWrite, since: "2.9.0" };
// @ts-expect-error The known PUT requires null, not a new idempotency key field.
const alteredTypedKey: ContractRoute = { ...reviewWrite, idempotencyKeyField: "requestId" };
void [typedReviewWrite, arbitraryTypedPut, alteredTypedPut, typedDelete, alteredTypedSince, alteredTypedKey];

describe("CTC-4633 complete tenant contract compatibility", () => {
  it.each(fixtures)("reads the complete $name document without dropping the legal PUT", ({ doc }) => {
    expect(doc.routes.filter((route) => route.method === "PUT")).toEqual([reviewWrite]);
    expect(isTenantContract(doc)).toBe(true);
    expect(readTenantContract(doc)).toEqual(doc);
  });

  it.each(fixtures)("TenantClient.contract admits the complete $name wire document", async ({ doc }) => {
    const client = createTenantClient({
      baseUrl: "https://contract-fixture.invalid",
      key: "synthetic-only",
      fetch: async () => Response.json(doc),
    });
    const result = await client.contract();
    expect(result.outcome).toBe("ok");
    if (result.outcome === "ok") expect(result.doc).toEqual(doc);
  });
});

const refusedRows = [
  { name: "arbitrary PUT", row: { ...reviewWrite, path: "/api/v1/agent/custom" } },
  { name: "changed since", row: { ...reviewWrite, since: "2.9.0" } },
  { name: "changed write budget", row: { ...reviewWrite, takesWriteBudgetUnit: true } },
  { name: "changed key field", row: { ...reviewWrite, idempotencyKeyField: "requestId" } },
  { name: "missing key field", row: { method: "PUT", path: reviewWrite.path, takesWriteBudgetUnit: false, since: "2.8.0" } },
];

describe("CTC-4633 PUT admission remains restricted", () => {
  it.each(refusedRows)("refuses $name through guard and client", async ({ row }) => {
    for (const { doc } of fixtures) {
      const malformed = { ...doc, routes: [...doc.routes, row] };
      expect(isTenantContract(malformed)).toBe(false);
      expect(readTenantContract(malformed)).toBeNull();
      const client = createTenantClient({
        baseUrl: "https://contract-fixture.invalid",
        key: "synthetic-only",
        fetch: async () => Response.json(malformed),
      });
      expect(await client.contract()).toMatchObject({ outcome: "shape", status: 200 });
    }
  });
});

// The row the cloud has published since contract 2.28.0, which every read refused until CTC-9961.
const projectDelete = {
  method: "DELETE",
  path: "/api/v1/agent/projects/:id",
  takesWriteBudgetUnit: false,
  idempotencyKeyField: null,
  since: "2.28.0",
};
const unseenRows = [
  { name: "the published project DELETE", row: projectDelete },
  { name: "PATCH", row: { ...projectDelete, method: "PATCH" } },
  { name: "custom method", row: { ...projectDelete, method: "CUSTOM" } },
];

function clientFor(doc: unknown, calls: string[] = []) {
  return createTenantClient({
    baseUrl: "https://contract-fixture.invalid",
    key: "synthetic-only",
    fetch: async (input, init) => {
      const url = new URL(String(input));
      calls.push(`${init?.method ?? "GET"} ${url.pathname}`);
      if (url.pathname === "/api/v1/agent/contract") return Response.json(doc);
      return Response.json({ outcome: "ok", servers: [] });
    },
  });
}

describe("CTC-9961 a route with a method this SDK has not seen", () => {
  it.each(unseenRows)("drops only $name and reads the rest of the document", async ({ row }) => {
    for (const { doc } of fixtures) {
      const newer = { ...doc, routes: [...doc.routes, row] };
      expect(isTenantContract(newer)).toBe(false);
      expect(readTenantContract(newer)).toEqual(doc);
      const result = await clientFor(newer).contract();
      expect(result.outcome).toBe("ok");
      if (result.outcome === "ok") expect(result.doc).toEqual(doc);
    }
  });

  it("does not change the document it was given", () => {
    const newer = { ...main, routes: [...main.routes, projectDelete] };
    readTenantContract(newer);
    expect(newer.routes).toContain(projectDelete);
  });

  it("still lists MCP servers from a contract holding the project DELETE", async () => {
    const calls: string[] = [];
    const client = clientFor({ ...main, routes: [...main.routes, projectDelete] }, calls);
    const result = await client.agent.portalServers();
    expect(result.outcome).toBe("ok");
    expect(calls).toContain("GET /api/v1/agent/portal-servers");
  });

  it("answers an unseen-method route as unknown and never sends it", async () => {
    const calls: string[] = [];
    const remove = "/api/v1/agent/portal-servers/remove";
    const routes = main.routes.filter((route) => route.path !== remove);
    const client = clientFor({ ...main, routes: [...routes, { ...projectDelete, path: remove }] }, calls);
    const result = await client.agent.portalServerRemove({ name: "docs" });
    expect(result).toMatchObject({ outcome: "route-unknown" });
    expect(calls).toEqual(["GET /api/v1/agent/contract"]);
  });

  const malformed = [
    { name: "no path", row: { method: "DELETE", takesWriteBudgetUnit: false, since: "2.28.0" } },
    { name: "no write budget", row: { method: "DELETE", path: projectDelete.path, since: "2.28.0" } },
    { name: "no since", row: { method: "DELETE", path: projectDelete.path, takesWriteBudgetUnit: false } },
    { name: "a method that is not text", row: { ...projectDelete, method: 7 } },
    { name: "no method", row: { path: projectDelete.path, takesWriteBudgetUnit: false, since: "2.28.0" } },
  ];
  it.each(malformed)("still refuses a row with $name", async ({ row }) => {
    const broken = { ...main, routes: [...main.routes, row] };
    expect(readTenantContract(broken)).toBeNull();
    expect(await clientFor(broken).contract()).toMatchObject({ outcome: "shape", status: 200 });
  });
});
