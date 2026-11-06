// test/browser-transport-exports.test.ts — the /browser subpath re-exports the transport surface so a
// browser consumer stays on one import; CTC-2111 adds a bearer AuthStrategy, so AuthError and
// CLOSE_REAUTHENTICATE must ride that subpath too (a browser consumer needs `instanceof AuthError` for
// onAuthError from the same import it built the client from). Mirrors /node and the root.
import { describe, it, expect } from "vitest";
import * as browser from "../src/browser";
import * as root from "../src/index";
import type {
  IssueDetailView,
  IssueFacetCount,
  IssueFacetDimension,
  IssueScope,
  IssueView,
  KeysetCursor,
  KeysetPage,
  PullView,
} from "../src/browser";
import type * as nodeSurface from "../src/node";

describe("the /browser subpath re-exports the bearer-auth transport symbols (CTC-2111)", () => {
  it("exports AuthError (the same class as the root) and CLOSE_REAUTHENTICATE", () => {
    expect(typeof browser.AuthError).toBe("function");
    // Same class object as the root export — instanceof works across both import sites.
    expect(browser.AuthError).toBe(root.AuthError);
    const err = new browser.AuthError(4401, "reauthenticate");
    expect(err).toBeInstanceOf(browser.AuthError);
    expect(err.code).toBe(4401);
    expect(browser.CLOSE_REAUTHENTICATE).toBe(4401);
  });
});

// CTC-6150 — the web types its calls to queryIssuesScoped / queryIssueFacetCounts from the SDK import
// alone. These are compile-time checks (npm run typecheck): a missing re-export is a type error here.
describe("the /browser and /node subpaths re-export the scoped-read types (CTC-6150)", () => {
  it("types a scoped call end to end from the /browser import", () => {
    const scope: IssueScope = { teamId: "team-ctc", labelId: "l-mirror", projectId: null };
    const dimension: IssueFacetDimension = "project";
    const cursor: KeysetCursor = { updatedAt: "300", id: "c3" };
    const rows: IssueView[] = [];
    const page: KeysetPage<IssueView> = { rows, nextCursor: cursor };
    const counts: IssueFacetCount[] = [{ key: null, name: null, count: 300 }];
    const detail: IssueDetailView | null = null;
    const pulls: PullView[] = [];

    expect(scope.projectId).toBeNull();
    expect(dimension).toBe("project");
    expect(page.nextCursor).toBe(cursor);
    expect(counts[0]?.key).toBeNull();
    expect([detail, pulls]).toEqual([null, []]);
  });

  it("re-exports the same scope types from /node", () => {
    const scope: nodeSurface.IssueScope = { teamId: "team-ctc" };
    const dimension: nodeSurface.IssueFacetDimension = "label";
    const counts: nodeSurface.IssueFacetCount[] = [];
    const page: nodeSurface.KeysetPage<nodeSurface.IssueView> = { rows: [], nextCursor: null };
    const cursor: nodeSurface.KeysetCursor | null = page.nextCursor;

    expect([scope.teamId, dimension, counts.length, cursor]).toEqual(["team-ctc", "label", 0, null]);
  });
});
