// @catalyst-cloud/sdk — guarded, zero-hard-dep OpenTelemetry seam (CTC-138).
//
// `@opentelemetry/api` is an OPTIONAL peer dependency. This module is the ONE place the SDK touches
// it, and it does so through a guarded **dynamic** import resolved once during the async `start()`
// path — never a static top-level `import`. That distinction is load-bearing:
//
//   • A static `import { trace, metrics } from "@opentelemetry/api"` fails MODULE RESOLUTION when a
//     consumer hasn't installed the package → a hard dependency in practice. The api package's
//     built-in no-op tracer/meter only save you AFTER it has been resolved (i.e. when it's installed
//     but no provider is registered); they do nothing for "not installed at all".
//   • The guarded `await import("@opentelemetry/api")` here catches the resolution failure and falls
//     back to hand-rolled no-op stubs, so the SDK imports + runs with the package entirely ABSENT.
//
// When the package IS present and the consumer has registered a global TracerProvider/MeterProvider,
// spans + metrics route through `trace.getTracer()` / `metrics.getMeter()` — so the consumer's
// Resource (service.name, etc.) flows through automatically. When present but with NO provider, the
// api's own no-ops apply and every call is harmless.
//
// Kept free of any node-only import so the browser bundle stays clean; the public surfaces below are
// declared STRUCTURALLY (no `@opentelemetry/api` type import) so no OTel type leaks into the package's
// generated `.d.ts`, and the whole seam tree-shakes away (package.json `sideEffects:false`) when
// telemetry is off.

import type { LiveSyncStatus } from "./live-sync-client.js";

/** Opt-in switch for the OTel seam. `false`/absent = OFF (NO dynamic import, truly zero overhead);
 *  `true` = ON with default instrumentation-scope names; an object overrides the tracer/meter name. */
export type TelemetryConfig = boolean | { tracerName?: string; meterName?: string };

/** A metric/span attribute value (OTel rejects `undefined`; {@link cleanAttrs} strips it). */
export type AttrValue = string | number | boolean;
/** Attribute bag accepted at call sites — `undefined` values are dropped before reaching OTel. */
export type Attributes = Record<string, AttrValue | undefined>;

/** The shared `catalyst.*` semconv attribute keys emitted across spans + metrics. */
export const CATALYST_ATTR = {
  /** The tenant id (= mirror/account name). */
  tenant: "catalyst.tenant",
  /** The upstream system a row originates from: `"linear"` | `"github"`. */
  source: "catalyst.source",
  /** The durable change-feed cursor (last applied seq). */
  cursor: "catalyst.replica.cursor",
  /** head_seq − cursor (mirror lag in seqs), when head_seq is known. */
  lagSeq: "catalyst.replica.lag_seq",
  /** The connection lifecycle status string. */
  status: "catalyst.replica.status",
  /** Rows applied during a seed. */
  rowCount: "catalyst.replica.row_count",
  /** Rows in one seed apply-batch. */
  batchRows: "catalyst.replica.batch_rows",
  /** The per-frame apply outcome (CTL-1402): `"applied"` | `"skipped"` | `"failed"`. Low-cardinality
   *  — safe as a metric label; seq/entity/err_message are VALUES on the log line, never labels. */
  result: "catalyst.replica.result",
} as const;

/** The per-frame apply outcome: a row written, dropped by the stale-guard, or a failed transaction. */
export type ReplicaApplyResult = "applied" | "skipped" | "failed";

/** Metric instrument names (OTel metric stream names). */
export const REPLICA_METRIC = {
  cursor: "catalyst.replica.cursor",
  lagSeq: "catalyst.replica.lag_seq",
  freshnessMs: "catalyst.replica.freshness_ms",
  status: "catalyst.replica.status",
  applied: "catalyst.replica.applied",
  applyBatchRows: "catalyst.replica.apply_batch_rows",
} as const;

/** Structured-log MESSAGE names the daemon's `log` callback routes to Loki, where the fleet's
 *  logs→metrics connector materializes them (the fleet has no in-process MeterProvider, so the
 *  per-frame apply RESULT rides a structured log line, not an OTLP metric — CTL-1402). */
export const REPLICA_LOG = {
  /** Per applied frame: `{result, seq, entity, source, err_message?}`. */
  apply: "catalyst.replica.apply",
} as const;

/** Span names. */
export const REPLICA_SPAN = {
  seed: "catalyst.replica.seed",
  applyBatch: "catalyst.replica.apply_batch",
  resync: "catalyst.replica.resync",
  reconnect: "catalyst.replica.reconnect",
} as const;

/** `LiveSyncStatus` → a numeric gauge code (the `catalyst.replica.status` gauge value). */
export const REPLICA_STATUS_CODE: Record<LiveSyncStatus, number> = {
  connecting: 0,
  live: 1,
  reconnecting: 2,
  resyncing: 3,
  error: 4,
  stopped: 5,
};

/** The default instrumentation-scope name for both the tracer and the meter. */
export const DEFAULT_SCOPE_NAME = "@catalyst-cloud/sdk";

/** The mirror entities sourced from GitHub (the rest are Linear) — drives the `catalyst.source` attr. */
const GITHUB_ENTITIES: ReadonlySet<string> = new Set([
  "pull_requests",
  "check_runs",
  "commit_statuses",
  "reviews",
]);

/** Map a change-feed `entity` (table name) to its upstream source system, for `catalyst.source`. */
export function entitySource(entity: string): "github" | "linear" {
  return GITHUB_ENTITIES.has(entity) ? "github" : "linear";
}

// ── The facade the SDK call sites use (no OTel types — works on the no-op path identically). ───────

/** A span handle passed into the `withActiveSpan*` callback for setting attributes mid-span. */
export interface SpanHandle {
  setAttribute(key: string, value: AttrValue): void;
}

/** A manually-ended span (for callback-driven lifecycles like a WS connect attempt). */
export interface ManualSpan extends SpanHandle {
  /** End the span. Pass an error to mark it ERROR + record the exception; otherwise it ends OK. */
  end(error?: unknown): void;
}

/** Read side of an observable-gauge callback: observe a value with optional attributes. */
export interface ObservableResult {
  observe(value: number, attributes?: Attributes): void;
}
export type ObservableCallback = (result: ObservableResult) => void;

/** A handle to unregister an observable-gauge callback (called on replica close). */
export interface GaugeRegistration {
  remove(): void;
}

export interface Counter {
  add(value: number, attributes?: Attributes): void;
}
export interface Histogram {
  record(value: number, attributes?: Attributes): void;
}

export interface MetricOptions {
  description?: string;
  unit?: string;
}

/** The opt-in telemetry facade. The no-op instance ({@link NOOP_TELEMETRY}) implements the SAME shape
 *  so call sites are branch-free. */
export interface Telemetry {
  /** True only when routed through a real (installed) `@opentelemetry/api`. */
  readonly enabled: boolean;
  /** Run an async `fn` inside an active span; ends OK on resolve, records the exception + ERROR on
   *  reject (then rethrows). Active so child spans (e.g. a seed under a resync) auto-parent. */
  withActiveSpan<T>(name: string, attributes: Attributes, fn: (span: SpanHandle) => Promise<T>): Promise<T>;
  /** The synchronous variant (e.g. a seed apply-batch). */
  withActiveSpanSync<T>(name: string, attributes: Attributes, fn: (span: SpanHandle) => T): T;
  /** Start a manually-ended span (e.g. a WS connect attempt ended across onopen/onclose). */
  startSpan(name: string, attributes?: Attributes): ManualSpan;
  /** Register an async gauge whose callback reads live instance state; returns an unregister handle. */
  observableGauge(name: string, options: MetricOptions, callback: ObservableCallback): GaugeRegistration;
  counter(name: string, options?: MetricOptions): Counter;
  histogram(name: string, options?: MetricOptions): Histogram;
}

// ── No-op implementation (used when telemetry is OFF or `@opentelemetry/api` is absent). ───────────

const NOOP_SPAN_HANDLE: SpanHandle = { setAttribute() {} };
const NOOP_MANUAL_SPAN: ManualSpan = { setAttribute() {}, end() {} };
const NOOP_REGISTRATION: GaugeRegistration = { remove() {} };
const NOOP_COUNTER: Counter = { add() {} };
const NOOP_HISTOGRAM: Histogram = { record() {} };

/** A telemetry that does nothing but still threads the callback's return value through `withActiveSpan*`. */
export const NOOP_TELEMETRY: Telemetry = {
  enabled: false,
  withActiveSpan: (_name, _attributes, fn) => fn(NOOP_SPAN_HANDLE),
  withActiveSpanSync: (_name, _attributes, fn) => fn(NOOP_SPAN_HANDLE),
  startSpan: () => NOOP_MANUAL_SPAN,
  observableGauge: () => NOOP_REGISTRATION,
  counter: () => NOOP_COUNTER,
  histogram: () => NOOP_HISTOGRAM,
};

// ── The minimal STRUCTURAL surface of `@opentelemetry/api` we drive (no type import → clean .d.ts). ─

interface SpanSurface {
  setAttribute(key: string, value: AttrValue): unknown;
  recordException(exception: unknown): void;
  setStatus(status: { code: number; message?: string }): unknown;
  end(): void;
}
interface TracerSurface {
  startSpan(name: string, options?: { attributes?: Record<string, AttrValue> }): SpanSurface;
  startActiveSpan<T>(
    name: string,
    options: { attributes?: Record<string, AttrValue> },
    fn: (span: SpanSurface) => T,
  ): T;
}
interface ObservableResultSurface {
  observe(value: number, attributes?: Record<string, AttrValue>): void;
}
interface ObservableGaugeSurface {
  addCallback(cb: (result: ObservableResultSurface) => void): void;
  removeCallback(cb: (result: ObservableResultSurface) => void): void;
}
interface CounterSurface {
  add(value: number, attributes?: Record<string, AttrValue>): void;
}
interface HistogramSurface {
  record(value: number, attributes?: Record<string, AttrValue>): void;
}
interface MeterSurface {
  createObservableGauge(name: string, options?: MetricOptions): ObservableGaugeSurface;
  createCounter(name: string, options?: MetricOptions): CounterSurface;
  createHistogram(name: string, options?: MetricOptions): HistogramSurface;
}
/** Just the bits of the `@opentelemetry/api` module namespace we read. */
export interface OtelApiSurface {
  trace: { getTracer(name: string, version?: string): TracerSurface };
  metrics: { getMeter(name: string, version?: string): MeterSurface };
  SpanStatusCode: { OK: number; ERROR: number };
}

/** Drop `undefined`-valued attributes (OTel rejects them) → a clean attribute record. */
function cleanAttrs(attributes: Attributes | undefined): Record<string, AttrValue> {
  const out: Record<string, AttrValue> = {};
  if (!attributes) return out;
  for (const [k, v] of Object.entries(attributes)) {
    if (v !== undefined) out[k] = v;
  }
  return out;
}

/**
 * Build a REAL telemetry facade over a resolved `@opentelemetry/api` surface (or the NO-OP when
 * `api` is null — the "package absent" branch). Exported (but NOT re-exported from the package
 * entrypoints) so it can be unit-tested directly with `null` for the absent case.
 */
export function buildTelemetry(api: OtelApiSurface | null, names: { tracerName: string; meterName: string }): Telemetry {
  if (!api) return NOOP_TELEMETRY;

  const tracer = api.trace.getTracer(names.tracerName);
  const meter = api.metrics.getMeter(names.meterName);
  const OK = api.SpanStatusCode.OK;
  const ERROR = api.SpanStatusCode.ERROR;

  const endSpan = (span: SpanSurface, error?: unknown): void => {
    if (error !== undefined) {
      span.recordException(error);
      span.setStatus({ code: ERROR, message: error instanceof Error ? error.message : String(error) });
    } else {
      span.setStatus({ code: OK });
    }
    span.end();
  };
  const handleFor = (span: SpanSurface): SpanHandle => ({
    setAttribute: (k, v) => {
      span.setAttribute(k, v);
    },
  });

  return {
    enabled: true,
    withActiveSpan(name, attributes, fn) {
      return tracer.startActiveSpan(name, { attributes: cleanAttrs(attributes) }, async (span) => {
        try {
          const result = await fn(handleFor(span));
          endSpan(span);
          return result;
        } catch (err) {
          endSpan(span, err);
          throw err;
        }
      });
    },
    withActiveSpanSync(name, attributes, fn) {
      return tracer.startActiveSpan(name, { attributes: cleanAttrs(attributes) }, (span) => {
        try {
          const result = fn(handleFor(span));
          endSpan(span);
          return result;
        } catch (err) {
          endSpan(span, err);
          throw err;
        }
      });
    },
    startSpan(name, attributes) {
      const span = tracer.startSpan(name, { attributes: cleanAttrs(attributes) });
      return {
        setAttribute: (k, v) => {
          span.setAttribute(k, v);
        },
        end: (error) => endSpan(span, error),
      };
    },
    observableGauge(name, options, callback) {
      const gauge = meter.createObservableGauge(name, options);
      const cb = (result: ObservableResultSurface): void => {
        callback({ observe: (value, attrs) => result.observe(value, cleanAttrs(attrs)) });
      };
      gauge.addCallback(cb);
      return { remove: () => gauge.removeCallback(cb) };
    },
    counter(name, options) {
      const instrument = meter.createCounter(name, options);
      return { add: (value, attrs) => instrument.add(value, cleanAttrs(attrs)) };
    },
    histogram(name, options) {
      const instrument = meter.createHistogram(name, options);
      return { record: (value, attrs) => instrument.record(value, cleanAttrs(attrs)) };
    },
  };
}

// ── The guarded one-time dynamic import. ──────────────────────────────────────────────────────────

let apiPromise: Promise<OtelApiSurface | null> | undefined;
let warnedAbsent = false;

/** Dynamic-import `@opentelemetry/api` ONCE (cached). Resolves to its surface, or `null` if the
 *  package isn't installed — the catch is what makes it a TRUE optional peer dependency. */
async function loadOtelApi(): Promise<OtelApiSurface | null> {
  if (apiPromise === undefined) {
    apiPromise = import("@opentelemetry/api").then(
      (mod) => {
        // The named exports (trace/metrics/SpanStatusCode) may sit on the module namespace OR on
        // `default`, depending on the CJS/ESM interop (node vs a bundler/test runner). Pick whichever
        // object actually carries them rather than guessing `default ?? mod`.
        const hasApi = (o: unknown): o is OtelApiSurface =>
          typeof o === "object" &&
          o !== null &&
          "trace" in o &&
          "metrics" in o &&
          "SpanStatusCode" in o;
        const ns = mod as { default?: unknown };
        if (hasApi(mod)) return mod as unknown as OtelApiSurface;
        if (hasApi(ns.default)) return ns.default;
        return null;
      },
      () => null,
    );
  }
  return apiPromise;
}

/** True iff `value` is an already-resolved {@link Telemetry} (internal pass-through, e.g. a replica
 *  sharing its instance with the transport) rather than a {@link TelemetryConfig}. */
function isTelemetry(value: unknown): value is Telemetry {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as Telemetry).withActiveSpan === "function"
  );
}

/**
 * Resolve the opt-in `telemetry` option into a {@link Telemetry}. Defaults to {@link NOOP_TELEMETRY}
 * (no dynamic import attempted) when undefined/false so the off path is truly zero-overhead. When ON,
 * guard-imports `@opentelemetry/api`; if absent, warns once and falls back to no-op (never throws). An
 * already-resolved `Telemetry` is returned as-is so a replica + its transport share ONE instance.
 */
export async function createTelemetry(
  config: TelemetryConfig | Telemetry | undefined,
  defaults: { tracerName: string; meterName: string },
): Promise<Telemetry> {
  if (isTelemetry(config)) return config;
  if (config === undefined || config === false) return NOOP_TELEMETRY;

  const names =
    config === true
      ? defaults
      : {
          tracerName: config.tracerName ?? defaults.tracerName,
          meterName: config.meterName ?? defaults.meterName,
        };

  const api = await loadOtelApi();
  if (!api) {
    if (!warnedAbsent) {
      warnedAbsent = true;
      // eslint-disable-next-line no-console
      console.warn(
        "[catalyst-sdk:otel] telemetry was enabled but the optional peer '@opentelemetry/api' is not " +
          "installed — running with no-op telemetry. Install '@opentelemetry/api' to emit spans/metrics.",
      );
    }
    return NOOP_TELEMETRY;
  }
  return buildTelemetry(api, names);
}
