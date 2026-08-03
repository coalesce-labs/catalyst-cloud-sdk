// replica/browser/validate.ts — option validation shared by every PUBLIC entry point of ./browser.
//
// Why this exists (CTC-114 review rounds 5, 10 and 12). `DeltaQueue`, `DeltaQueueOptions`,
// `streamSnapshotBatches` and `acquireReplicaLock` are all exported from `@catalyst-cloud/sdk/browser`,
// so their numeric knobs are consumer-supplied and can arrive from unchecked runtime configuration —
// and TypeScript helps none of the untyped-JS callers those exports invite.
//
// The review found the SAME defect three times, each time in whichever module had not been guarded
// yet: `maxBatch` (round 5), `maxDepth`/`maxApplyRetries`/`retryDelayMs` (round 10), then `batchSize`
// and the lock's `retries` (round 12). Guarding them module by module is how a class of defect
// survives three rounds. One helper, used at every boundary, is the seam.
//
// `NaN` is the sharp value in every case, and always for the same reason: EVERY comparison against it
// is false, so a bound expressed as `x > limit` silently ceases to exist rather than degrading. That
// is worse than a wrong-but-finite value, because nothing downstream can detect it.

/**
 * Require a positive integer (>= 1). Rejects `NaN`, `Infinity`, fractions, zero and negatives.
 *
 * `label` names the option in the error, so a consumer sees which knob they got wrong rather than a
 * failure somewhere downstream — the whole point of validating at construction.
 */
export function requirePositiveInt(
  owner: string,
  label: string,
  value: unknown,
): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    throw new Error(
      `${owner}: ${label} must be a positive integer (got ${String(value)})`,
    );
  }
  return value;
}

/**
 * Require a non-negative finite number — for values where 0 is meaningful (a delay, a retry count).
 *
 * Split from {@link requirePositiveInt} because collapsing them would force a legitimate `0` to be
 * spelled `1`, and a retry delay of 0 (retry on the next tick) is a real configuration.
 */
export function requireNonNegativeInt(
  owner: string,
  label: string,
  value: unknown,
): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error(
      `${owner}: ${label} must be a non-negative integer (got ${String(value)})`,
    );
  }
  return value;
}

/** Require a finite, non-negative number (fractions allowed — milliseconds, not counts). */
export function requireNonNegativeFinite(
  owner: string,
  label: string,
  value: unknown,
): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new Error(
      `${owner}: ${label} must be a non-negative finite number (got ${String(value)})`,
    );
  }
  return value;
}
