import { describe, expect, it } from "vitest";
import { SeedReadGate } from "../../src/replica/browser/seed-read-gate.js";

describe("SeedReadGate", () => {
  it("resolves immediately when no seed is open (seeding=false)", async () => {
    const gate = new SeedReadGate();
    // An already-resolved promise: awaiting it must not hang and must not need a settle().
    await expect(gate.whenReadable(false)).resolves.toBeUndefined();
  });

  it("defers a read while a seed is open, then releases it on settle()", async () => {
    const gate = new SeedReadGate();
    let released = false;
    const pending = gate.whenReadable(true).then(() => {
      released = true;
    });
    // Still open — the read must NOT have run yet (would otherwise see the mid-seed truncated txn).
    await Promise.resolve();
    expect(released).toBe(false);
    gate.settle();
    await pending;
    expect(released).toBe(true);
  });

  it("releases every read deferred during the same seed", async () => {
    const gate = new SeedReadGate();
    const order: number[] = [];
    const a = gate.whenReadable(true).then(() => order.push(1));
    const b = gate.whenReadable(true).then(() => order.push(2));
    const c = gate.whenReadable(true).then(() => order.push(3));
    gate.settle();
    await Promise.all([a, b, c]);
    expect(order).toEqual([1, 2, 3]);
  });

  it("settle() with no deferred reads is a no-op (safe on abort of a read-free seed)", () => {
    const gate = new SeedReadGate();
    expect(() => gate.settle()).not.toThrow();
  });

  it("a fresh seed after settle() defers independently (waiters are not retained)", async () => {
    const gate = new SeedReadGate();
    gate.whenReadable(true); // deferred during seed #1
    gate.settle(); // seed #1 commits/aborts → drained
    let released2 = false;
    const second = gate.whenReadable(true).then(() => {
      released2 = true;
    });
    await Promise.resolve();
    expect(released2).toBe(false); // seed #2's read is still deferred, not resolved by seed #1's settle
    gate.settle();
    await second;
    expect(released2).toBe(true);
  });
});
