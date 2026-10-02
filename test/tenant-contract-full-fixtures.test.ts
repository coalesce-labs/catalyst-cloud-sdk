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
  { name: "DELETE", row: { ...reviewWrite, method: "DELETE" } },
  { name: "PATCH", row: { ...reviewWrite, method: "PATCH" } },
  { name: "custom method", row: { ...reviewWrite, method: "CUSTOM" } },
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
