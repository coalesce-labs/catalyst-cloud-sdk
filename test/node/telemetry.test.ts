import { afterEach, describe, expect, it, vi } from "vitest";
import { context, metrics, trace } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { MeterProvider, MetricReader } from "@opentelemetry/sdk-metrics";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import {
  CatalystReplica,
  nodeSqliteEngine,
  type WebSocketLike,
  type WebSocketFactory,
} from "../../src/node";

// End-to-end coverage of the OTel seam (CTC-138) wired into CatalystReplica. The transport is driven
// by a FakeWebSocket and /snapshot by an injected fetch, so seed + live-apply are deterministic and
// offline. A REAL OpenTelemetry SDK (BasicTracerProvider + InMemorySpanExporter, MeterProvider + an
// in-memory collecting reader) is registered as the consumer's GLOBAL provider, and we assert the
// `catalyst.replica.*` spans + metrics fire with the right names/attributes and that the consumer's
// Resource (service.name) flows through. Then: api-present-but-no-provider must not throw, and
// telemetry OFF must emit nothing (opt-in + zero overhead) while the replica still works.

const BASE = "https://api.example.test";
const SCOPE = "@catalyst-cloud/sdk";

class FakeWebSocket implements WebSocketLike {
  sent: string[] = [];
  closed = false;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  send(data: string): void {
    this.sent.push(data);
  }
  close(): void {
    this.closed = true;
  }
  fireOpen(): void {
    this.onopen?.({});
  }
  deliver(frame: unknown): void {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }
}

function recordingFactory(): { sockets: FakeWebSocket[]; factory: WebSocketFactory } {
  const sockets: FakeWebSocket[] = [];
  const factory: WebSocketFactory = () => {
    const ws = new FakeWebSocket();
    sockets.push(ws);
    return ws;
  };
  return { sockets, factory };
}

interface SeedRow {
  entity: string;
  row: Record<string, unknown>;
}
function snapshotBody(rows: SeedRow[], cursor: number): string {
  const lines = rows.map((r) =>
    JSON.stringify({ accountId: "tenant-0", entity: r.entity, op: "upsert", row: r.row }),
  );
  lines.push(JSON.stringify({ accountId: "tenant-0", cursor }));
  return lines.join("\n") + "\n";
}

/** A buffered /snapshot fetch stand-in; when `headSeq` is given the Response carries the CTC-137
 *  `x-catalyst-head-seq` header so the lag_seq gauge has a head to compare against. */
function snapshotFetch(rows: SeedRow[], cursor: number, headSeq?: number): typeof fetch {
  const body = snapshotBody(rows, cursor);
  const headers =
    headSeq == null
      ? undefined
      : { get: (name: string) => (name === "x-catalyst-head-seq" ? String(headSeq) : null) };
  return (async () =>
    ({ ok: true, status: 200, text: async () => body, headers }) as unknown as Response) as unknown as typeof fetch;
}

async function startToLive(replica: CatalystReplica, sockets: FakeWebSocket[]): Promise<void> {
  const started = replica.start();
  await vi.waitFor(() => expect(sockets.length).toBeGreaterThan(0));
  sockets[0]!.fireOpen();
  await started;
}

// ── A MetricReader that collects on demand (drives the observable-gauge callbacks). ──
class CollectingReader extends MetricReader {
  protected async onForceFlush(): Promise<void> {}
  protected async onShutdown(): Promise<void> {}
}

interface InstalledProviders {
  spanExporter: InMemorySpanExporter;
  reader: CollectingReader;
}

/** Register a real Tracer + Meter provider as the consumer's GLOBAL providers, with a custom Resource
 *  so we can prove service.name flows through. */
function installProviders(): InstalledProviders {
  const resource = resourceFromAttributes({ "service.name": "test-consumer" });
  const spanExporter = new InMemorySpanExporter();
  const tracerProvider = new BasicTracerProvider({
    resource,
    spanProcessors: [new SimpleSpanProcessor(spanExporter)],
  });
  trace.setGlobalTracerProvider(tracerProvider);
  const reader = new CollectingReader();
  const meterProvider = new MeterProvider({ resource, readers: [reader] });
  metrics.setGlobalMeterProvider(meterProvider);
  return { spanExporter, reader };
}

interface AnyMetric {
  descriptor: { name: string };
  dataPoints: Array<{ value: unknown; attributes: Record<string, unknown> }>;
}
async function collect(reader: CollectingReader): Promise<{
  resourceAttrs: Record<string, unknown>;
  byName: Map<string, AnyMetric>;
}> {
  const { resourceMetrics } = await reader.collect();
  const scope = resourceMetrics.scopeMetrics.find((s) => s.scope.name === SCOPE);
  const byName = new Map<string, AnyMetric>(
    (scope?.metrics ?? []).map((m) => [m.descriptor.name, m as unknown as AnyMetric]),
  );
  return {
    resourceAttrs: resourceMetrics.resource.attributes as Record<string, unknown>,
    byName,
  };
}

const replicas: CatalystReplica[] = [];
function track(r: CatalystReplica): CatalystReplica {
  replicas.push(r);
  return r;
}
afterEach(async () => {
  while (replicas.length) await replicas.pop()!.close();
  // Reset the global providers so each test starts from a known state (re-registration is allowed
  // only after disable()).
  trace.disable();
  metrics.disable();
  context.disable();
  vi.useRealTimers();
});

describe("CatalystReplica OpenTelemetry seam (CTC-138)", () => {
  it("emits seed/apply_batch/reconnect spans + the replica.* metrics through the consumer's global providers", async () => {
    context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
    const { spanExporter, reader } = installProviders();
    const { sockets, factory } = recordingFactory();
    const fetchImpl = snapshotFetch(
      [{ entity: "issues", row: { id: "i1", identifier: "CTC-1", title: "Seed", updated_at: 1 } }],
      5,
      9, // mirror head_seq from x-catalyst-head-seq
    );
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl,
        wsFactory: factory,
        telemetry: true,
      }),
    );

    await startToLive(replica, sockets);
    // One live frame (a GitHub entity → catalyst.source=github), advancing the cursor to 6.
    sockets[0]!.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 6,
      entity: "pull_requests",
      entityId: "r1:1",
      op: "upsert",
      row: { repo_id: "r1", number: 1, state: "open", title: "PR", updated_at: 2 },
    });

    // ── SPANS ──
    const spans = spanExporter.getFinishedSpans();
    const names = spans.map((s) => s.name);
    expect(names).toContain("catalyst.replica.seed");
    expect(names).toContain("catalyst.replica.apply_batch");
    expect(names).toContain("catalyst.replica.reconnect");

    const seed = spans.find((s) => s.name === "catalyst.replica.seed")!;
    expect(seed.attributes["catalyst.tenant"]).toBe("tenant-0");
    expect(seed.attributes["catalyst.replica.row_count"]).toBe(1);
    expect(seed.attributes["catalyst.replica.cursor"]).toBe(5);
    // The consumer's Resource (service.name) flows through the span.
    expect(seed.resource.attributes["service.name"]).toBe("test-consumer");

    // apply_batch nests UNDER the seed span (active-span context propagation).
    const batch = spans.find((s) => s.name === "catalyst.replica.apply_batch")!;
    expect(batch.attributes["catalyst.replica.batch_rows"]).toBe(1);
    expect(batch.parentSpanContext?.spanId).toBe(seed.spanContext().spanId);

    const reconnect = spans.find((s) => s.name === "catalyst.replica.reconnect")!;
    expect(reconnect.attributes["catalyst.tenant"]).toBe("tenant-0");

    // ── METRICS ──
    const { resourceAttrs, byName } = await collect(reader);
    expect(resourceAttrs["service.name"]).toBe("test-consumer");

    const cursor = byName.get("catalyst.replica.cursor")!;
    expect(Number(cursor.dataPoints[0]!.value)).toBe(6); // advanced by the live frame
    expect(cursor.dataPoints[0]!.attributes["catalyst.tenant"]).toBe("tenant-0");

    const status = byName.get("catalyst.replica.status")!;
    expect(Number(status.dataPoints[0]!.value)).toBe(1); // live
    expect(status.dataPoints[0]!.attributes["catalyst.replica.status"]).toBe("live");

    const lag = byName.get("catalyst.replica.lag_seq")!;
    expect(Number(lag.dataPoints[0]!.value)).toBe(9 - 6); // head_seq 9 − cursor 6

    const freshness = byName.get("catalyst.replica.freshness_ms")!;
    expect(Number(freshness.dataPoints[0]!.value)).toBeGreaterThanOrEqual(0);

    const applied = byName.get("catalyst.replica.applied")!;
    expect(Number(applied.dataPoints[0]!.value)).toBeGreaterThanOrEqual(1);
    expect(applied.dataPoints[0]!.attributes["catalyst.source"]).toBe("github");
    // CTL-1402: the counter now carries the apply outcome as a low-cardinality label.
    expect(applied.dataPoints[0]!.attributes["catalyst.replica.result"]).toBe("applied");

    const histogram = byName.get("catalyst.replica.apply_batch_rows")!;
    expect((histogram.dataPoints[0]!.value as { count: number }).count).toBeGreaterThanOrEqual(1);

    expect(replica.headSeq).toBe(9);
    expect(replica.freshnessMs).not.toBeNull();
  });

  it("lag_seq is omitted (no data points) when no head_seq header is observed", async () => {
    const { reader } = installProviders();
    const { sockets, factory } = recordingFactory();
    const fetchImpl = snapshotFetch([], 0); // NO x-catalyst-head-seq header
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl,
        wsFactory: factory,
        telemetry: true,
      }),
    );
    await startToLive(replica, sockets);

    expect(replica.headSeq).toBeNull();
    const { byName } = await collect(reader);
    // The gauge is registered, but its callback observes nothing while head_seq is unknown → 0 points.
    const lag = byName.get("catalyst.replica.lag_seq");
    expect(lag?.dataPoints ?? []).toHaveLength(0);
    // cursor is still observed unconditionally.
    expect(byName.get("catalyst.replica.cursor")!.dataPoints).toHaveLength(1);
  });

  it("with @opentelemetry/api present but NO provider registered, telemetry=true never throws", async () => {
    // Deliberately do NOT installProviders(): the api is present (devDep) but the globals are noop.
    const { sockets, factory } = recordingFactory();
    const fetchImpl = snapshotFetch(
      [{ entity: "issues", row: { id: "i1", identifier: "CTC-1", title: "x", updated_at: 1 } }],
      3,
    );
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl,
        wsFactory: factory,
        telemetry: true,
      }),
    );
    await expect(startToLive(replica, sockets)).resolves.toBeUndefined();
    sockets[0]!.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 4,
      entity: "issues",
      entityId: "i2",
      op: "upsert",
      row: { id: "i2", identifier: "CTC-2", title: "y", updated_at: 2 },
    });
    expect(replica.issues()).toHaveLength(2);
    expect(replica.cursor).toBe(4);
  });

  it("telemetry OFF (default): emits NO spans/metrics even with providers registered, replica still works", async () => {
    const { spanExporter, reader } = installProviders();
    const { sockets, factory } = recordingFactory();
    const fetchImpl = snapshotFetch([], 0);
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl,
        wsFactory: factory,
        // telemetry omitted → OFF
      }),
    );
    await startToLive(replica, sockets);
    sockets[0]!.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 1,
      entity: "issues",
      entityId: "i1",
      op: "upsert",
      row: { id: "i1", identifier: "CTC-1", title: "z", updated_at: 1 },
    });

    expect(spanExporter.getFinishedSpans()).toHaveLength(0);
    const { byName } = await collect(reader);
    expect(byName.size).toBe(0); // no catalyst.replica.* instruments registered
    // freshness stamping is telemetry-independent + cheap, so the getter still works.
    expect(replica.freshnessMs).not.toBeNull();
    expect(replica.cursor).toBe(1);
  });
});

// ── CTL-1402: applyFrame emits catalyst.replica.apply{result} on every live frame ─────────────────
// The fleet runs NO in-process MeterProvider, so the per-frame apply RESULT rides a STRUCTURED LOG
// line (via the consumer's `log` callback → daemon → Loki → logs-to-metrics connector), independent
// of the OTel seam. These tests capture `log` and prove applied/skipped/failed emit with the right
// fields (result, seq, entity, source, err_message) — the join key `seq` ties to the mirror's
// ingest.committed.headSeq.
describe("CatalystReplica apply-result telemetry (CTL-1402)", () => {
  interface LogLine {
    level: string;
    msg: string;
    extra: Record<string, unknown>;
  }
  const applyLines = (logs: LogLine[]): Record<string, unknown>[] =>
    logs.filter((l) => l.msg === "catalyst.replica.apply").map((l) => ({ level: l.level, ...l.extra }));

  async function liveReplica(logs: LogLine[]): Promise<{ socket: FakeWebSocket }> {
    const { sockets, factory } = recordingFactory();
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: snapshotFetch([], 0),
        wsFactory: factory,
        // telemetry OFF on purpose — the apply-result LOG line is the fleet's primary signal and must
        // fire regardless of the OTLP seam.
        log: (level, msg, extra) =>
          logs.push({ level, msg, extra: (extra ?? {}) as Record<string, unknown> }),
      }),
    );
    await startToLive(replica, sockets);
    return { socket: sockets[0]! };
  }

  it("emits result:applied with seq+entity+source when a frame writes a row", async () => {
    const logs: LogLine[] = [];
    const { socket } = await liveReplica(logs);
    socket.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 1,
      entity: "issues",
      entityId: "i1",
      op: "upsert",
      row: { id: "i1", identifier: "CTL-1", title: "T", updated_at: 10 },
    });
    expect(applyLines(logs)).toContainEqual(
      expect.objectContaining({
        level: "info",
        result: "applied",
        seq: 1,
        entity: "issues",
        source: "linear",
      }),
    );
  });

  it("emits result:skipped when the last-write-wins guard drops an out-of-order delta", async () => {
    const logs: LogLine[] = [];
    const { socket } = await liveReplica(logs);
    // First delta writes (updated_at 10); a later frame carrying an OLDER updated_at is dropped by the
    // replica's stale-guard → applyDelta returns false → result:skipped (nothing was written).
    socket.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 1,
      entity: "issues",
      entityId: "i1",
      op: "upsert",
      row: { id: "i1", identifier: "CTL-1", title: "new", updated_at: 10 },
    });
    socket.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 2,
      entity: "issues",
      entityId: "i1",
      op: "upsert",
      row: { id: "i1", identifier: "CTL-1", title: "stale", updated_at: 5 },
    });
    const lines = applyLines(logs);
    expect(lines).toContainEqual(expect.objectContaining({ result: "applied", seq: 1 }));
    expect(lines).toContainEqual(
      expect.objectContaining({ result: "skipped", seq: 2, entity: "issues", source: "linear" }),
    );
  });

  it("emits result:failed with the untruncated err_message when applyDelta throws", async () => {
    const logs: LogLine[] = [];
    const { socket } = await liveReplica(logs);
    // An unknown entity makes applyDelta throw → the catch records result:failed + the error message
    // (the untruncated string the #127 column-drift investigation needs).
    socket.deliver({
      type: "change",
      accountId: "tenant-0",
      seq: 1,
      entity: "not_a_table",
      entityId: "x",
      op: "upsert",
      row: { id: "x" },
    });
    const failed = applyLines(logs).find((l) => l["result"] === "failed");
    expect(failed).toMatchObject({ level: "error", result: "failed", seq: 1, entity: "not_a_table" });
    expect(String(failed!["err_message"])).toContain("unknown entity");
  });
});

// ── CTL-1402: the catalyst.replica.gaps counter (gap detected/healed as low-cardinality OTLP) ─────
describe("CatalystReplica gap telemetry counter (CTL-1402)", () => {
  it("bumps catalyst.replica.gaps with gap_event=detected then healed across a gap episode", async () => {
    const { reader } = installProviders();
    const { sockets, factory } = recordingFactory();
    const replica = track(
      new CatalystReplica({
        baseUrl: BASE,
        account: "tenant-0",
        auth: { kind: "cookie" },
        dbPath: ":memory:",
        engine: nodeSqliteEngine,
        fetchImpl: snapshotFetch([], 5),
        wsFactory: factory,
        telemetry: true,
      }),
    );
    await startToLive(replica, sockets);

    const frame = (seq: number) => ({
      type: "change",
      accountId: "tenant-0",
      seq,
      entity: "issues",
      entityId: `i${seq}`,
      op: "upsert",
      row: { id: `i${seq}`, identifier: `CTC-${seq}`, title: `t${seq}`, updated_at: seq },
    });
    sockets[0]!.deliver(frame(6)); // contiguous
    sockets[0]!.deliver(frame(9)); // gap (7,8) → detected
    sockets[0]!.deliver(frame(7)); // replay…
    sockets[0]!.deliver(frame(8));
    sockets[0]!.deliver(frame(9)); // …heals

    const { byName } = await collect(reader);
    const gaps = byName.get("catalyst.replica.gaps")!;
    const byEvent = new Map(
      gaps.dataPoints.map((p) => [p.attributes["catalyst.replica.gap_event"], p]),
    );
    expect(Number(byEvent.get("detected")!.value)).toBe(1);
    expect(Number(byEvent.get("healed")!.value)).toBe(1);
    expect(byEvent.get("detected")!.attributes["catalyst.tenant"]).toBe("tenant-0");
    expect(byEvent.has("escalated")).toBe(false); // the replay healed it — no re-seed
    expect(replica.cursor).toBe(9);
  });
});
