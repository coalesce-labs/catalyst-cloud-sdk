// tenant-client-exports.test.ts — CTC-2132 Phase 6. Every new tenant-client symbol reaches a
// consumer through the package root, and every method on the `TenantClient` interface is actually
// wired into the returned object. This is the drift guard for this ticket: the `AGENT_ROUTE_NAMES`/
// `AgentRouteName` compile-time lockstep CTC-2562 built protects contract-resolved routes only; none
// of CTC-2132's routes are contract-resolved, so a literal method-path walk is the equivalent
// mechanism — a build or test failure rather than a runtime surprise.

import { describe, expect, it } from "vitest";
import { json, scriptedFetch } from "./helpers/scripted-fetch";
import * as root from "../src/index";
import { createTenantClient } from "../src/index";

const BASE = "https://cloud.example";

function resolve(obj: unknown, path: string): unknown {
  return path.split(".").reduce<unknown>((acc, key) => (acc as Record<string, unknown> | undefined)?.[key], obj);
}

describe("package root exports", () => {
  it("every new tenant-client symbol is exported from the package root", () => {
    for (const name of ["createTenantClient", "pageCursor", "memoryContractCache", "normalizeBaseUrl"]) {
      expect(root).toHaveProperty(name);
    }
  });
});

describe("⭐ every method on the TenantClient interface is present on a constructed client", () => {
  it("walks every documented method path", () => {
    const net = scriptedFetch([() => json(200, {})]);
    const client = createTenantClient({ key: "k", baseUrl: BASE, fetch: net.fetch });
    for (const path of [
      "contract",
      "me",
      "request",
      "issues.list",
      "issues.get",
      "issues.execution",
      "pulls.list",
      "pulls.get",
      "projects.list",
      "cycles.list",
      "search",
      "workflowStages",
      "changes.stream",
      "changes.list",
      "snapshot.head",
      "diagnostics.workEligibility",
      "diagnostics.dispatchQueue",
      "diagnostics.fleetActivity",
      "diagnostics.agentRoster",
      "diagnostics.leaseAttributions",
      "diagnostics.codingAccounts",
      "agent.issueState",
      "agent.issueLabel",
      "agent.issueComment",
      "agent.issueCreate",
      "agent.reaction",
      "agent.attachment",
      "agent.attachments",
      "agent.session",
      "agent.ask",
      "agent.askAccept",
      "agent.projectRepositoryRegister",
      "agent.projectRepositoryRemove",
    ]) {
      expect([path, typeof resolve(client, path)]).toEqual([path, "function"]);
    }
  });
});
