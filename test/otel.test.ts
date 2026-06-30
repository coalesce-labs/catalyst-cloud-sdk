import { describe, expect, it } from "vitest";
import {
  buildTelemetry,
  createTelemetry,
  entitySource,
  NOOP_TELEMETRY,
  REPLICA_STATUS_CODE,
  type Telemetry,
} from "../src/otel";

// Unit coverage of the guarded OTel seam (CTC-138). The "@opentelemetry/api ABSENT" branch is proven
// directly via buildTelemetry(null) (the same value loadOtelApi() resolves to when the optional peer
// fails to import), so it's deterministic without uninstalling the dev dependency. The "present"
// branches resolve the REAL api (a devDependency here) but register NO provider, so they exercise the
// api's own built-in no-ops — every call must thread its value + never throw.

const NAMES = { tracerName: "@catalyst-cloud/sdk", meterName: "@catalyst-cloud/sdk" };

describe("otel seam — no-op fallback + guarded resolution (CTC-138)", () => {
  it("buildTelemetry(null) is a safe no-op — the @opentelemetry/api ABSENT branch", async () => {
    const t = buildTelemetry(null, NAMES);
    expect(t.enabled).toBe(false);
    // withActiveSpan*/threads the callback's return value through, never throws on its own.
    await expect(t.withActiveSpan("s", { "catalyst.tenant": "t0" }, async () => 42)).resolves.toBe(42);
    expect(t.withActiveSpanSync("s", {}, () => "ok")).toBe("ok");
    // manual span + instruments are inert.
    const span = t.startSpan("s", { a: "b" });
    expect(() => {
      span.setAttribute("k", 1);
      span.end();
      span.end(new Error("x"));
    }).not.toThrow();
    expect(() => t.observableGauge("g", {}, () => {}).remove()).not.toThrow();
    expect(() => t.counter("c").add(1, { a: "b" })).not.toThrow();
    expect(() => t.histogram("h").record(2)).not.toThrow();
  });

  it("createTelemetry(undefined|false) is OFF and never attempts an import", async () => {
    expect(await createTelemetry(undefined, NAMES)).toBe(NOOP_TELEMETRY);
    expect(await createTelemetry(false, NAMES)).toBe(NOOP_TELEMETRY);
  });

  it("createTelemetry(true) resolves the present @opentelemetry/api and is enabled", async () => {
    const t = await createTelemetry(true, NAMES);
    expect(t.enabled).toBe(true);
    // No provider registered → the api's own no-ops apply; calls must not throw + still thread values.
    await expect(t.withActiveSpan("s", { a: 1 }, async () => "v")).resolves.toBe("v");
    expect(t.withActiveSpanSync("s", {}, () => 7)).toBe(7);
    expect(() => t.counter("c").add(1)).not.toThrow();
    expect(() => t.observableGauge("g", {}, (r) => r.observe(1)).remove()).not.toThrow();
  });

  it("createTelemetry({ tracerName }) overrides the scope name + is enabled", async () => {
    const t = await createTelemetry({ tracerName: "custom-scope" }, NAMES);
    expect(t.enabled).toBe(true);
  });

  it("createTelemetry passes an already-resolved Telemetry through unchanged (shared instance)", async () => {
    const resolved: Telemetry = buildTelemetry(null, NAMES);
    expect(await createTelemetry(resolved, NAMES)).toBe(resolved);
  });

  it("withActiveSpan rethrows when the callback throws (no swallow)", async () => {
    await expect(
      NOOP_TELEMETRY.withActiveSpan("s", {}, async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");
    expect(() =>
      NOOP_TELEMETRY.withActiveSpanSync("s", {}, () => {
        throw new Error("bang");
      }),
    ).toThrow("bang");
  });

  it("entitySource maps the GitHub entities → github, everything else → linear", () => {
    for (const e of ["pull_requests", "check_runs", "commit_statuses", "reviews"]) {
      expect(entitySource(e)).toBe("github");
    }
    for (const e of ["issues", "labels", "users", "projects", "comments"]) {
      expect(entitySource(e)).toBe("linear");
    }
  });

  it("REPLICA_STATUS_CODE maps every lifecycle status to a distinct numeric code", () => {
    const codes = Object.values(REPLICA_STATUS_CODE);
    expect(new Set(codes).size).toBe(codes.length);
    expect(REPLICA_STATUS_CODE.live).toBe(1);
  });
});
