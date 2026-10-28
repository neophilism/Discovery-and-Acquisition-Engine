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
} from "./scheduler-helpers.ts";
function delayed(db: Database, onRead: () => void) {
  let locked = false,
    observed = false;
  const wrapped: Database = {
    transaction: (fn) =>
      db.transaction((q) =>
        fn({
          query: async <T>(sql: string, values?: unknown[]) => {
            const result = await q.query<T>(sql, values);
            if (sql.includes("FOR UPDATE")) locked = true;
            if (
              sql.includes("public.evidence_record") &&
              sql.includes("SELECT")
            ) {
              assert.equal(locked, true);
              observed = true;
              onRead();
            }
            return result;
          },
        }),
      ),
    close: async () => {},
  };
  return { db: wrapped, observed: () => observed };
}
test("current consumption samples clock AFTER actual locked SQL read and denies expired lease without consuming", async (t) => {
  const f = await setup(t),
    lease = await f.store.leaseNext(worker, "collection", 1, "lease");
  let now = worker.now;
  const d = delayed(f.db, () => {
    now = "2026-01-01T00:00:02Z";
  });
  const store = new AcquisitionScheduler(d.db, schedulerPolicy, verifier);
  await assert.rejects(
    store.consumeCurrent(worker, "collection", 2, lease.lease.token, () => now),
    { code: "lease_consumed" },
  );
  assert.equal(d.observed(), true);
  assert.equal((await f.store.load(owner, "collection")).revision, 2);
  assert.equal(
    (await f.store.load(owner, "collection")).state.leases[0]!.status,
    "leased",
  );
});
test("current consumption checks stored source expiry using live time, not captured request time", async (t) => {
  const f = await setup(t),
    lease = await f.store.leaseNext(worker, "collection", 1, "lease");
  await assert.rejects(
    f.store.consumeCurrent(
      worker,
      "collection",
      2,
      lease.lease.token,
      () => "2026-02-01T00:00:00Z",
    ),
    { code: "connector_disabled" },
  );
  assert.equal((await f.store.load(owner, "collection")).revision, 2);
});
test("current consumption is one-shot, preserves uncertain COMMIT and captures context before awaits", async (t) => {
  const f = await setup(t),
    lease = await f.store.leaseNext(worker, "collection", 1, "lease");
  let acknowledged = false;
  const db: Database = {
    transaction: async (fn) => {
      await f.db.transaction(fn);
      acknowledged = true;
      throw Error("fixture lost commit acknowledgement");
    },
    close: async () => {},
  };
  const store = new AcquisitionScheduler(db, schedulerPolicy, verifier),
    c = { ...worker, actor: { ...worker.actor } };
  const pending = store.consumeCurrent(
    c,
    "collection",
    2,
    lease.lease.token,
    () => worker.now,
  );
  c.projectId = "foreign";
  c.actor.id = "foreign";
  await assert.rejects(pending, /lost commit/);
  assert.equal(acknowledged, true);
  const current = await f.store.load(owner, "collection");
  assert.equal(current.state.leases[0]!.status, "running");
  await assert.rejects(
    f.store.consumeCurrent(
      worker,
      "collection",
      current.revision,
      lease.lease.token,
      () => worker.now,
    ),
    { code: "lease_consumed" },
  );
});
test("missing, malformed, regressed and throwing clocks fail closed without leaking diagnostics", async (t) => {
  const f = await setup(t),
    lease = await f.store.leaseNext(worker, "collection", 1, "lease");
  await assert.rejects(
    f.store.consumeCurrent(
      worker,
      "collection",
      2,
      lease.lease.token,
      undefined as never,
    ),
    { code: "invalid_clock" },
  );
  for (const clock of [
    () => "not-time",
    () => "2025-01-01T00:00:00Z",
    () => {
      throw Error("private clock diagnostic");
    },
  ]) {
    await assert.rejects(
      f.store.consumeCurrent(worker, "collection", 2, lease.lease.token, clock),
      (e) =>
        (e as { code: string }).code === "clock_unavailable" &&
        !String(e).includes("private clock"),
    );
    assert.equal((await f.store.load(owner, "collection")).revision, 2);
  }
  const work = await f.store.consumeCurrent(
    worker,
    "collection",
    2,
    lease.lease.token,
    () => worker.now,
  );
  assert.equal(work.revision, 3);
});
