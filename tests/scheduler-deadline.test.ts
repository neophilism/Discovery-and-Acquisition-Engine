import test from "node:test";
import assert from "node:assert/strict";
import { AcquisitionScheduler } from "../src/scheduler.ts";
import { DomainError } from "../src/contracts.ts";
import type { OutcomeVerifier } from "../src/scheduler.ts";
import {
  setup,
  schedulerPolicy,
  worker,
  capture,
  receipt,
} from "./scheduler-helpers.ts";
function block(ms: number) {
  const until = performance.now() + ms;
  while (performance.now() < until) {}
}
async function execution(t: test.TestContext, verifier: OutcomeVerifier) {
  const f = await setup(t, verifier);
  const leased = await f.store.leaseNext(worker, "collection", 1, "lease");
  const work = await f.store.consume(
    worker,
    "collection",
    leased.snapshot.revision,
    leased.lease.token,
  );
  return { ...f, token: leased.lease.token, revision: work.revision };
}
for (const mode of ["captured", "no-capture"] as const) {
  test(`blocked verifier cannot commit late ${mode} receipt before its delayed timer`, async (t) => {
    let signal: AbortSignal | undefined;
    const f = await execution(t, {
      verify: async (_c, s) => {
        signal = s;
        block(80);
        return true;
      },
    });
    const outcome =
      mode === "captured"
        ? { capture: capture(), receipt }
        : { failureCode: "source_unavailable", receipt };
    await assert.rejects(
      f.store.complete(
        worker,
        "collection",
        f.revision,
        f.token,
        outcome,
        "complete",
      ),
      { code: "receipt_timeout" },
    );
    assert.equal(signal?.aborted, true);
    const current = await f.store.load(worker, "collection");
    assert.equal(current.revision, f.revision);
    assert.equal(current.state.leases[0]!.status, "running");
    // Rollback leaves no cached completion. A later, independently timely verification can resolve the original execution.
    const timely = new AcquisitionScheduler(f.db, schedulerPolicy, {
      verify: async () => true,
    });
    assert.equal(
      (
        await timely.complete(
          worker,
          "collection",
          f.revision,
          f.token,
          outcome,
          "complete",
        )
      ).state.leases[0]!.status,
      "completed",
    );
  });
}
test("successful and rejected receipt callbacks close owned signals and hide diagnostics", async (t) => {
  let signal: AbortSignal | undefined;
  const f = await execution(t, {
    verify: async (_c, s) => {
      signal = s;
      return true;
    },
  });
  await f.store.complete(
    worker,
    "collection",
    f.revision,
    f.token,
    { capture: capture(), receipt },
    "complete",
  );
  assert.equal(signal?.aborted, true);
  const g = await execution(t, {
    verify: async (_c, s) => {
      signal = s;
      throw new DomainError("private-provider-code", "private-provider-secret");
    },
  });
  await assert.rejects(
    g.store.complete(
      worker,
      "collection",
      g.revision,
      g.token,
      { capture: capture(), receipt },
      "bad",
    ),
    (e) =>
      e instanceof Error &&
      !e.message.includes("private-provider") &&
      (e as DomainError).code === "receipt_unavailable",
  );
  assert.equal(signal?.aborted, true);
});
test("late asynchronous verification is fenced and strict-true rejection never records a completion", async (t) => {
  let release!: (value: boolean) => void, signal: AbortSignal | undefined;
  const wait = new Promise<boolean>((r) => {
    release = r;
  });
  const f = await execution(t, {
    verify: (_c, s) => {
      signal = s;
      return wait;
    },
  });
  await assert.rejects(
    f.store.complete(
      worker,
      "collection",
      f.revision,
      f.token,
      { capture: capture(), receipt },
      "late",
    ),
    { code: "receipt_timeout" },
  );
  assert.equal(signal?.aborted, true);
  release(true);
  await new Promise((r) => setImmediate(r));
  assert.equal((await f.store.load(worker, "collection")).revision, f.revision);
  for (const value of [false, 1, "true", {}]) {
    const store = new AcquisitionScheduler(f.db, schedulerPolicy, {
      verify: async () => value as boolean,
    });
    await assert.rejects(
      store.complete(
        worker,
        "collection",
        f.revision,
        f.token,
        { capture: capture(), receipt },
        "strict",
      ),
    );
  }
  assert.equal(
    (await f.store.load(worker, "collection")).state.leases[0]!.status,
    "running",
  );
});
