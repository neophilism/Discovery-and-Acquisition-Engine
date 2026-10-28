import test from "node:test";
import assert from "node:assert/strict";
import type { Database } from "@accountability/evidence/database";
import { AcquisitionScheduler } from "../src/scheduler.ts";
import {
  setup,
  worker,
  owner,
  schedulerPolicy,
  verifier,
  receipt,
  capture,
} from "./scheduler-helpers.ts";
const later = "2026-01-01T00:00:02Z";
function afterRead(db: Database, read: () => void) {
  let locked = false,
    observed = false;
  const wrapped: Database = {
    transaction: (fn) =>
      db.transaction((q) =>
        fn({
          query: async <T>(sql: string, values?: unknown[]) => {
            const r = await q.query<T>(sql, values);
            if (sql.includes("FOR UPDATE")) locked = true;
            if (
              sql.includes("SELECT") &&
              sql.includes("public.evidence_record")
            ) {
              assert.equal(locked, true);
              observed = true;
              read();
            }
            return r;
          },
        }),
      ),
    close: async () => {},
  };
  return { db: wrapped, observed: () => observed };
}
test("current recovery expires unconsumed lease using clock AFTER actual locked SQL read", async (t) => {
  const f = await setup(t);
  await f.store.leaseNext(worker, "collection", 1, "lease");
  let now = owner.now;
  const d = afterRead(f.db, () => {
    now = later;
  });
  const c = { ...owner, actor: { ...owner.actor } };
  const pending = new AcquisitionScheduler(
    d.db,
    schedulerPolicy,
    verifier,
  ).recoverCurrent(c, "collection", 2, "recover", () => now);
  c.projectId = "foreign";
  c.actor.id = "foreign";
  const recovered = await pending;
  assert.equal(d.observed(), true);
  assert.equal(recovered.revision, 3);
  assert.equal(recovered.state.leases[0]!.status, "completed");
  assert.equal(
    recovered.state.collection.attempts[0]!.failureCode,
    "lease_expired_unconsumed",
  );
  assert.equal(recovered.state.collection.attempts[0]!.completedAt, later);
});
test("expired consumed recovery preserves unresolved attempt/capacity until exact verified completion", async (t) => {
  const f = await setup(t),
    leased = await f.store.leaseNext(worker, "collection", 1, "lease");
  await f.store.consumeCurrent(
    worker,
    "collection",
    2,
    leased.lease.token,
    () => worker.now,
  );
  const recovered = await f.store.recoverCurrent(
    owner,
    "collection",
    3,
    "recover",
    () => later,
  );
  assert.equal(recovered.state.leases[0]!.status, "reconcile");
  assert.equal(recovered.state.collection.attempts[0]!.status, "started");
  assert.equal(recovered.state.collection.attempts[0]!.failureCode, null);
  await assert.rejects(
    f.store.leaseNextCurrent(worker, "collection", 4, "new", () => later),
    { code: "concurrency_limit" },
  );
  await assert.rejects(
    f.store.consumeCurrent(
      worker,
      "collection",
      4,
      leased.lease.token,
      () => later,
    ),
    { code: "lease_consumed" },
  );
  const done = await f.store.complete(
    { ...worker, now: later },
    "collection",
    4,
    leased.lease.token,
    { capture: capture(later), receipt },
    "complete",
  );
  assert.equal(done.state.leases[0]!.status, "completed");
});
test("recovery rejects invalid clocks; timely no-op retains the existing revision", async (t) => {
  const f = await setup(t);
  await f.store.leaseNext(worker, "collection", 1, "lease");
  await assert.rejects(
    f.store.recoverCurrent(
      owner,
      "collection",
      2,
      "invalid",
      undefined as never,
    ),
    { code: "invalid_clock" },
  );
  for (const clock of [
    () => "bad",
    () => "2025-01-01T00:00:00Z",
    () => {
      throw Error("private clock detail");
    },
  ])
    await assert.rejects(
      f.store.recoverCurrent(owner, "collection", 2, "invalid", clock),
      (e) =>
        (e as { code: string }).code === "clock_unavailable" &&
        !String(e).includes("private clock"),
    );
  assert.equal((await f.store.load(owner, "collection")).revision, 2);
  assert.equal(
    (
      await f.store.recoverCurrent(
        owner,
        "collection",
        2,
        "timely",
        () => owner.now,
      )
    ).revision,
    2,
  );
});
test("lost recovery COMMIT acknowledgement preserves original current/legacy replay without new transition", async (t) => {
  const f = await setup(t);
  await f.store.leaseNext(worker, "collection", 1, "lease");
  let committed = false;
  const db: Database = {
    transaction: async (fn) => {
      await f.db.transaction(fn);
      committed = true;
      throw Error("fixture lost commit acknowledgement");
    },
    close: async () => {},
  };
  await assert.rejects(
    new AcquisitionScheduler(db, schedulerPolicy, verifier).recoverCurrent(
      owner,
      "collection",
      2,
      "same",
      () => later,
    ),
    /lost commit/,
  );
  assert.equal(committed, true);
  const original = await f.store.recover(owner, "collection", 2, "same");
  let called = false;
  assert.deepEqual(
    await f.store.recoverCurrent(owner, "collection", 2, "same", () => {
      called = true;
      throw Error("not fresh authority");
    }),
    original,
  );
  assert.equal(called, false);
  assert.equal(original.revision, 3);
  assert.equal(
    (await f.store.load(owner, "collection")).state.collection.attempts.length,
    1,
  );
});
