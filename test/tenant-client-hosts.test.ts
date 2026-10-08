import { describe, expect, it } from "vitest";
import { createTenantClient } from "../src/index";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
const inventory = {
  account: "acme",
  readAtMs: 1800000000000,
  machines: [
    {
      id: "host_1",
      name: "studio-mac",
      ownership: "self_hosted",
      teams: ["ENG"],
      slots: 4,
      inUse: 1,
      free: 3,
      lastCheckInAtMs: 1800000000000,
      status: "live",
      removable: true,
      renameable: true,
    },
  ],
  capacity: {
    slots: 4,
    inUse: 1,
    free: 3,
    selfHostedSlots: 4,
    catalystSlots: 0,
  },
};
describe("typed machine reads and admin mutations", () => {
  it("uses the same member credential for list and admin verbs, encoding the account", async () => {
    const net = scriptedFetch([
      () => json(200, inventory),
      () => json(200, { ok: true, hostId: "host_1", name: "studio-mac-2" }),
      () => json(200, { ok: true, hostId: "host_1" }),
    ]);
    const c = createTenantClient({
      key: "ctc_user_member",
      baseUrl: "https://cloud.example",
      fetch: net.fetch,
    });
    expect(await c.hosts.list({ account: "acme" })).toMatchObject({
      outcome: "ok",
      ...inventory,
    });
    expect(
      await c.hosts.rename("host_1", "studio-mac-2", { account: "acme" }),
    ).toMatchObject({ outcome: "ok", name: "studio-mac-2" });
    expect(await c.hosts.remove("host_1", { account: "acme" })).toMatchObject({
      outcome: "ok",
      hostId: "host_1",
    });
    expect(net.calls.map((c) => [c.method, c.url])).toEqual([
      ["GET", "https://cloud.example/api/v1/hosts/machines?account=acme"],
      ["POST", "https://cloud.example/api/v1/hosts/host_1/name?account=acme"],
      ["DELETE", "https://cloud.example/api/v1/hosts/host_1?account=acme"],
    ]);
    expect(net.calls[1]?.body).toEqual({ name: "studio-mac-2" });
    for (const call of net.calls)
      expect(call.headers.authorization).toBe("Bearer ctc_user_member");
  });
  it("keeps permission failures and malformed inventories distinct from empty capacity", async () => {
    const net = scriptedFetch([
      () =>
        json(403, {
          error: "admin_required",
          message: "Admin access is required.",
        }),
      () =>
        json(200, {
          ...inventory,
          machines: [{ ...inventory.machines[0], slots: "4" }],
        }),
      () => json(503, { error: "machine_inventory_unavailable" }),
    ]);
    const c = createTenantClient({
      key: "member",
      baseUrl: "https://cloud.example",
      fetch: net.fetch,
    });
    expect(await c.hosts.remove("host_1")).toMatchObject({
      outcome: "forbidden",
      status: 403,
      error: "admin_required",
    });
    expect(await c.hosts.list()).toMatchObject({ outcome: "shape" });
    expect(await c.hosts.list()).toMatchObject({
      outcome: "http",
      status: 503,
    });
  });
});
