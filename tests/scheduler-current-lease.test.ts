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
function afterRead(db: Database, onRead: () => void) {
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
              onRead();
            }
            return r;
          },
        }),
      ),
    close: async () => {},
  };
  return { db: wrapped, observed: () => observed };
}
test("current lease timestamps and attempt start use clock after actual locked SQL read", async (t) => {
  const f = await setup(t);
  let now = worker.now;
  const d = afterRead(f.db, () => {
    now = "2026-01-01T00:00:02Z";
  });
  const store = new AcquisitionScheduler(d.db, schedulerPolicy, verifier);
  const c = { ...worker, actor: { ...worker.actor } };
  const pending = store.leaseNextCurrent(c, "collection", 1, "live", () => now);
  c.projectId = "foreign";
  c.actor.id = "foreign";
  const leased = await pending;
  assert.equal(d.observed(), true);
  assert.equal(leased.lease.actorId, worker.actor.id);
  assert.equal(leased.lease.leasedAt, now);
  assert.equal(leased.lease.expiresAt, "2026-01-01T00:00:03.000Z");
  assert.equal(leased.snapshot.state.collection.attempts[0]!.startedAt, now);
  assert.equal(
    (
      await store.consumeCurrent(
        worker,
        "collection",
        2,
        leased.lease.token,
        () => now,
      )
    ).revision,
    3,
  );
});
test("source policy expiry after locked read cannot create lease/attempt", async (t) => {
  const f = await setup(t);
  let now = worker.now;
  const d = afterRead(f.db, () => {
    now = "2026-02-01T00:00:00Z";
  });
  const store = new AcquisitionScheduler(d.db, schedulerPolicy, verifier);
  await assert.rejects(
    store.leaseNextCurrent(worker, "collection", 1, "expired", () => now),
    { code: "connector_disabled" },
  );
  assert.equal(d.observed(), true);
  const current = await f.store.load(owner, "collection");
  assert.equal(current.revision, 1);
  assert.equal(current.state.leases.length, 0);
  assert.equal(current.state.collection.attempts.length, 0);
});
test("current lease rejects missing, invalid, regressed and throwing clocks without mutations", async (t) => {
  const f = await setup(t);
  await assert.rejects(
    f.store.leaseNextCurrent(
      worker,
      "collection",
      1,
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
      f.store.leaseNextCurrent(worker, "collection", 1, "invalid", clock),
      (e) =>
        (e as { code: string }).code === "clock_unavailable" &&
        !String(e).includes("private clock"),
    );
  assert.equal((await f.store.load(owner, "collection")).revision, 1);
});
test("historical current/legacy lease replay preserves fingerprint and cannot renew expired execution authority", async (t) => {
  const f = await setup(t);
  const leased = await f.store.leaseNextCurrent(
    worker,
    "collection",
    1,
    "same",
    () => worker.now,
  );
  assert.deepEqual(
    await f.store.leaseNext(worker, "collection", 1, "same"),
    leased,
  );
  let called = false;
  assert.deepEqual(
    await f.store.leaseNextCurrent(worker, "collection", 1, "same", () => {
      called = true;
      return "2026-03-01T00:00:00Z";
    }),
    leased,
  );
  assert.equal(called, false);
  await assert.rejects(
    f.store.consumeCurrent(
      worker,
      "collection",
      2,
      leased.lease.token,
      () => "2026-01-01T00:00:02Z",
    ),
    { code: "lease_consumed" },
  );
  assert.equal((await f.store.load(owner, "collection")).revision, 2);
});
test("lost current lease COMMIT acknowledgement replays original metadata without a second attempt", async (t) => {
  const f = await setup(t);
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
    new AcquisitionScheduler(db, schedulerPolicy, verifier).leaseNextCurrent(
      worker,
      "collection",
      1,
      "lost",
      () => worker.now,
    ),
    /lost commit/,
  );
  assert.equal(committed, true);
  const replay = await f.store.leaseNextCurrent(
    worker,
    "collection",
    1,
    "lost",
    () => worker.now,
  );
  assert.equal(replay.snapshot.revision, 2);
  assert.equal(replay.snapshot.state.collection.attempts.length, 1);
  assert.deepEqual(replay.snapshot, await f.store.load(owner, "collection"));
});
