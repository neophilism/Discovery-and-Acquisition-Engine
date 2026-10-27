import test from "node:test";
import assert from "node:assert/strict";
import { scoped, verifyAudit } from "@accountability/evidence/database";
import { AcquisitionScheduler } from "../src/scheduler.ts";
import { collectionCoverage } from "../src/collection.ts";
import {
  owner,
  worker,
  receipt,
  capture,
  setup,
  driver,
  schedulerPolicy,
  verifier,
} from "./scheduler-helpers.ts";
const later = { ...worker, now: "2026-01-01T00:00:02Z" };
test("durable lease, one-shot consumption and verified completion retain exact receipt/provenance and outbox", async (t) => {
  const { store, db } = await setup(t);
  const lease = await store.leaseNext(worker, "collection", 1, "lease");
  assert.deepEqual(
    await store.leaseNext(worker, "collection", 1, "lease"),
    lease,
  );
  assert.equal(
    (await store.load(worker, "collection")).state.collection.attempts.length,
    1,
  );
  const work = await store.consume(
    worker,
    "collection",
    lease.snapshot.revision,
    lease.lease.token,
  );
  assert.equal(work.url, "https://records.example/a");
  assert.equal(work.purpose, "research");
  assert.equal(work.acquisitionKey.length, 64);
  const done = await store.complete(
    worker,
    "collection",
    work.revision,
    lease.lease.token,
    { capture: capture(), receipt },
    "complete",
  );
  assert.equal(done.state.leases[0]!.status, "completed");
  assert.deepEqual(done.state.leases[0]!.receipt, receipt);
  assert.equal(
    collectionCoverage(worker, done.state.collection).capturedItems,
    1,
  );
  assert.deepEqual(
    await store.complete(
      worker,
      "collection",
      work.revision,
      lease.lease.token,
      { capture: capture(), receipt },
      "complete",
    ),
    done,
  );
  await scoped(db, owner, async (tx) => {
    assert(verifyAudit(await tx.audits()));
    assert.equal((await tx.list("discovery-schedule-v1")).length, 1);
    assert.equal(
      (await tx.claim("observer"))?.topic,
      "discovery.schedule.changed",
    );
  });
});
test("atomic revision contenders cannot claim overlapping capacity; consumed work never reissues after lost acknowledgement", async (t) => {
  const { store, db } = await setup(t);
  const results = await Promise.allSettled([
    store.leaseNext(worker, "collection", 1, "one"),
    store.leaseNext(worker, "collection", 1, "two"),
  ]);
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
  const lease = results.find((r) => r.status === "fulfilled")!.value;
  const lossy = new AcquisitionScheduler(
    {
      transaction: async (fn) => {
        const result = await db.transaction(fn);
        throw Error("lost acknowledgement");
      },
      close: async () => {},
    },
    schedulerPolicy,
    verifier,
  );
  await assert.rejects(
    lossy.consume(
      worker,
      "collection",
      lease.snapshot.revision,
      lease.lease.token,
    ),
    /lost acknowledgement/,
  );
  const current = await store.load(worker, "collection");
  assert.equal(current.state.leases[0]!.status, "running");
  await assert.rejects(
    store.consume(worker, "collection", current.revision, lease.lease.token),
    { code: "lease_consumed" },
  );
  await assert.rejects(
    store.leaseNext(worker, "collection", current.revision, "more"),
    { code: "concurrency_limit" },
  );
});
test("recovery expires untouched work but fences ambiguous consumed work until verified resolution", async (t) => {
  const first = await setup(t),
    leased = await first.store.leaseNext(worker, "collection", 1, "lease");
  const recovered = await first.store.recover(
    later,
    "collection",
    leased.snapshot.revision,
    "recover",
  );
  assert.equal(
    recovered.state.collection.attempts[0]!.failureCode,
    "lease_expired_unconsumed",
  );
  await assert.rejects(
    first.store.consume(
      later,
      "collection",
      recovered.revision,
      leased.lease.token,
    ),
    { code: "lease_consumed" },
  );
  const second = await setup(t),
    l = await second.store.leaseNext(worker, "collection", 1, "lease"),
    work = await second.store.consume(
      worker,
      "collection",
      l.snapshot.revision,
      l.lease.token,
    );
  const unknown = await second.store.recover(
    later,
    "collection",
    work.revision,
    "recover",
  );
  assert.equal(unknown.state.leases[0]!.status, "reconcile");
  assert.equal(unknown.state.collection.attempts[0]!.status, "started");
  await assert.rejects(
    second.store.leaseNext(later, "collection", unknown.revision, "new"),
    { code: "concurrency_limit" },
  );
  const done = await second.store.complete(
    { ...owner, now: later.now },
    "collection",
    unknown.revision,
    l.lease.token,
    { capture: capture(), receipt },
    "resolve",
  );
  assert.equal(done.state.collection.attempts[0]!.status, "captured");
});
test("receipts must verify exact attempt outcomes; denied or unavailable verifiers roll back state", async (t) => {
  const { store, db } = await setup(t),
    l = await store.leaseNext(worker, "collection", 1, "lease"),
    work = await store.consume(
      worker,
      "collection",
      l.snapshot.revision,
      l.lease.token,
    );
  for (const receiptValue of [
    { ...receipt, id: "unverified" },
    { ...receipt, projectId: "other" },
  ])
    await assert.rejects(
      store.complete(
        worker,
        "collection",
        work.revision,
        l.lease.token,
        { capture: capture(), receipt: receiptValue },
        "bad",
      ),
    );
  for (const verify of [
    {
      verify: async () => {
        throw Error("private-provider-secret");
      },
    },
    {
      verify: async (_c: unknown, signal: AbortSignal) =>
        new Promise<boolean>(() => {
          signal.addEventListener("abort", () => {});
        }),
    },
  ]) {
    const failing = new AcquisitionScheduler(db, schedulerPolicy, verify);
    await assert.rejects(
      failing.complete(
        worker,
        "collection",
        work.revision,
        l.lease.token,
        { capture: capture(), receipt },
        "unavailable",
      ),
      (e) =>
        e instanceof Error && !e.message.includes("private-provider-secret"),
    );
  }
  assert.equal(
    (await store.load(worker, "collection")).revision,
    work.revision,
  );
  const failed = await store.complete(
    worker,
    "collection",
    work.revision,
    l.lease.token,
    { failureCode: "source_unavailable", receipt },
    "failed",
  );
  assert.equal(failed.state.collection.attempts[0]!.status, "failed");
});
test("scope, purpose, current worker roles, ownership and human pause are enforced", async (t) => {
  const { store } = await setup(t);
  for (const context of [
    { ...worker, tenantId: "other" },
    { ...worker, projectId: "other" },
    { ...worker, purpose: "other" },
    { ...worker, actor: { ...worker.actor, roles: [] } },
    { ...worker, actor: { ...worker.actor, id: "unknown" } },
  ])
    await assert.rejects(store.load(context, "collection"));
  const l = await store.leaseNext(worker, "collection", 1, "lease");
  await assert.rejects(
    store.consume(
      { ...worker, actor: { ...worker.actor, id: "other" } },
      "collection",
      l.snapshot.revision,
      l.lease.token,
    ),
    { code: "lease_owner_denied" },
  );
  await assert.rejects(
    store.setEnabled(worker, "collection", l.snapshot.revision, false, "pause"),
    { code: "scheduler_denied" },
  );
  const paused = await store.setEnabled(
    owner,
    "collection",
    l.snapshot.revision,
    false,
    "pause",
  );
  await assert.rejects(
    store.consume(worker, "collection", paused.revision, l.lease.token),
    { code: "scheduler_paused" },
  );
  const resumed = await store.setEnabled(
    owner,
    "collection",
    paused.revision,
    true,
    "resume",
  );
  const work = await store.consume(
    worker,
    "collection",
    resumed.revision,
    l.lease.token,
  );
  const again = await store.setEnabled(
    owner,
    "collection",
    work.revision,
    false,
    "again",
  );
  assert.equal(
    (
      await store.complete(
        worker,
        "collection",
        again.revision,
        l.lease.token,
        { capture: capture(), receipt },
        "done",
      )
    ).state.enabled,
    false,
  );
});
test("transaction rollback and persistent restart preserve scheduling fences and saved command results", async (t) => {
  const s = await setup(t, verifier, true);
  const fault = { beforeCommit: true },
    broken = new AcquisitionScheduler(
      driver(s.pg, fault),
      schedulerPolicy,
      verifier,
    );
  await assert.rejects(
    broken.leaseNext(worker, "collection", 1, "failed"),
    /transaction failure/,
  );
  assert.equal((await s.store.load(worker, "collection")).revision, 1);
  const l = await s.store.leaseNext(worker, "collection", 1, "lease");
  await s.restart();
  assert.deepEqual(
    await s.store.leaseNext(worker, "collection", 1, "lease"),
    l,
  );
  const work = await s.store.consume(
    worker,
    "collection",
    l.snapshot.revision,
    l.lease.token,
  );
  await s.restart();
  assert.equal(
    (await s.store.load(worker, "collection")).state.leases[0]!.status,
    "running",
  );
  await assert.rejects(
    s.store.consume(worker, "collection", work.revision, l.lease.token),
    { code: "lease_consumed" },
  );
});
