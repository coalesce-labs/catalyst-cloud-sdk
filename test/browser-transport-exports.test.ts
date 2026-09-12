// test/browser-transport-exports.test.ts — the /browser subpath re-exports the transport surface so a
// browser consumer stays on one import; CTC-2111 adds a bearer AuthStrategy, so AuthError and
// CLOSE_REAUTHENTICATE must ride that subpath too (a browser consumer needs `instanceof AuthError` for
// onAuthError from the same import it built the client from). Mirrors /node and the root.
import { describe, it, expect } from "vitest";
import * as browser from "../src/browser";
import * as root from "../src/index";

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
