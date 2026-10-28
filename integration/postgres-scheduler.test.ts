import test, { before, after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  postgresDatabase,
  scoped,
  verifyAudit,
} from "@accountability/evidence/database";
import type { Database } from "@accountability/evidence/database";
import { AcquisitionScheduler } from "../src/scheduler.ts";
import {
  owner,
  worker,
  collection,
  schedulerPolicy,
} from "../tests/scheduler-helpers.ts";
const supplied = process.env.DAE_TEST_ADMIN_URL;
assert.ok(
  process.env.DAE_TEST_DISPOSABLE_POSTGRES === "1" && supplied,
  "Explicit disposable PostgreSQL fixture configuration required",
);
const url = new URL(supplied);
assert.ok(
  ["postgres:", "postgresql:"].includes(url.protocol) &&
    ["localhost", "127.0.0.1"].includes(url.hostname) &&
    url.pathname === "/discovery_ci" &&
    url.username === "postgres" &&
    !url.search &&
    !url.hash,
  "Only the designated empty loopback fixture is accepted",
);
const admin = postgresDatabase(url.toString());
before(async () => {
  await admin.transaction(async (q) => {
    assert.equal(
      (
        await q.query<{ fresh: boolean }>(
          "SELECT to_regclass('public.evidence_project') IS NULL AS fresh",
        )
      ).rows[0]!.fresh,
      true,
      "Fixture must be empty; never reset a database",
    );
    const migration = await readFile(
      new URL(
        "../migrations/001_foundation.sql",
        import.meta.resolve("@accountability/evidence/database"),
      ),
      "utf8",
    );
    assert.equal(migration.match(/^BEGIN;$/gm)?.length, 1);
    assert.equal(migration.match(/^COMMIT;$/gm)?.length, 1);
    await q.query(migration.replace(/^BEGIN;$/m, "").replace(/^COMMIT;$/m, ""));
    await q.query(
      "CREATE ROLE discovery_network_login LOGIN PASSWORD 'fixture-runtime-only' NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION; GRANT evidence_runtime TO discovery_network_login",
    );
  });
});
after(() => admin.close());
function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
async function fixture(t: test.TestContext, id: string) {
  const c = { ...owner, tenantId: "fiction", projectId: id },
    w = { ...worker, tenantId: c.tenantId, projectId: id };
  const policy = {
    ...schedulerPolicy,
    tenantId: c.tenantId,
    projectId: id,
    receiptTimeoutMs: 1000,
  };
  const now = { value: c.now };
  function connect(label: string) {
    const u = new URL(url);
    u.username = "discovery_network_login";
    u.password = "fixture-runtime-only";
    u.searchParams.set("application_name", id + "-" + label);
    const raw = postgresDatabase(u.toString());
    let closed = false;
    const db: Database = {
      transaction: raw.transaction.bind(raw),
      close: async () => {
        if (!closed) {
          closed = true;
          await raw.close();
        }
      },
    };
    t.after(() => db.close());
    return db;
  }
  const a = connect("a"),
    b = connect("b");
  const identity = await Promise.all(
    [a, b].map((db) =>
      db.transaction((q) =>
        q.query<{ pid: number; login: string }>(
          "SELECT pg_backend_pid() AS pid, session_user AS login",
        ),
      ),
    ),
  );
  assert.notEqual(identity[0]!.rows[0]!.pid, identity[1]!.rows[0]!.pid);
  assert(identity.every((r) => r.rows[0]!.login === "discovery_network_login"));
  const store = (db: Database) =>
    new AcquisitionScheduler(db, policy, { verify: async () => true });
  const sa = store(a),
    sb = store(b);
  await sa.create(
    c,
    {
      ...collection,
      sourcePolicy: {
        ...collection.sourcePolicy,
        tenantId: c.tenantId,
        projectId: id,
      },
    },
    "create",
  );
  return { a, b, c, w, sa, sb, store, connect, now, id };
}
async function observedWait(id: string) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const r = await admin.transaction((q) =>
      q.query<{ blocked: boolean }>(
        "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND cardinality(pg_blocking_pids(pid))>0) AS blocked",
        [id + "-b"],
      ),
    );
    if (r.rows[0]!.blocked) return;
    await new Promise((r) => setTimeout(r, 10));
  }
  assert.fail("Must directly observe PostgreSQL backend blocking");
}
test(
  "independent PostgreSQL consumers return one acknowledged work description",
  { timeout: 20000 },
  async (t) => {
    const f = await fixture(t, "consume-race"),
      lease = await f.sa.leaseNextCurrent(
        f.w,
        "collection",
        1,
        "lease",
        () => f.now.value,
      );
    const r = await Promise.allSettled([
      f.sa.consumeCurrent(
        f.w,
        "collection",
        2,
        lease.lease.token,
        () => f.now.value,
      ),
      f.sb.consumeCurrent(
        f.w,
        "collection",
        2,
        lease.lease.token,
        () => f.now.value,
      ),
    ]);
    assert.equal(r.filter((v) => v.status === "fulfilled").length, 1);
    const denied = r.find(
      (v) => v.status === "rejected",
    ) as PromiseRejectedResult;
    assert.equal(denied.reason.code, "revision_conflict");
    assert.equal(
      (await f.sb.load(f.c, "collection")).state.leases[0]!.status,
      "running",
    );
    await scoped(f.b, f.c, async (tx) =>
      assert.ok(verifyAudit(await tx.audits())),
    );
  },
);
for (const mode of ["lease", "consume", "recover"] as const) {
  test(
    `current ${mode} samples clock after directly observed PostgreSQL lock wait`,
    { timeout: 20000 },
    async (t) => {
      const f = await fixture(t, "clock-" + mode);
      const lease =
        mode === "lease"
          ? null
          : await f.sa.leaseNextCurrent(
              f.w,
              "collection",
              1,
              "lease",
              () => f.now.value,
            );
      const locked = gate(),
        release = gate();
      const hold = scoped(f.a, f.c, async (tx) => {
        await tx.audit("fixture.scheduler.lock", { fixture: true });
        locked.resolve();
        await release.promise;
      });
      let pending: Promise<unknown> | undefined;
      try {
        await Promise.race([locked.promise, hold]);
        pending =
          mode === "lease"
            ? f.sb.leaseNextCurrent(
                f.w,
                "collection",
                1,
                "lease",
                () => f.now.value,
              )
            : mode === "consume"
              ? f.sb.consumeCurrent(
                  f.w,
                  "collection",
                  2,
                  lease!.lease.token,
                  () => f.now.value,
                )
              : f.sb.recoverCurrent(
                  f.c,
                  "collection",
                  2,
                  "recover",
                  () => f.now.value,
                );
        const result = pending.then(
          (value) => ({ value }),
          (error) => ({ error }),
        );
        await observedWait(f.id);
        f.now.value = "2026-01-01T00:00:02Z";
        release.resolve();
        await hold;
        const r = await result;
        if (mode === "consume") {
          assert.ok("error" in r);
          assert.equal((r.error as { code: string }).code, "lease_consumed");
          assert.equal((await f.sb.load(f.c, "collection")).revision, 2);
        } else {
          assert.ok("value" in r);
          const state = await f.sb.load(f.c, "collection");
          if (mode === "lease") {
            assert.equal(state.state.leases[0]!.leasedAt, f.now.value);
            assert.equal(
              state.state.leases[0]!.expiresAt,
              "2026-01-01T00:00:03.000Z",
            );
          } else {
            assert.equal(state.state.leases[0]!.status, "completed");
            assert.equal(
              state.state.collection.attempts[0]!.completedAt,
              f.now.value,
            );
          }
        }
      } finally {
        release.resolve();
        await hold;
        await pending?.catch(() => {});
      }
    },
  );
}
test(
  "lost consumption COMMIT acknowledgement survives fresh pool and recovery keeps uncertainty blocked",
  { timeout: 20000 },
  async (t) => {
    const f = await fixture(t, "lost-consume"),
      lease = await f.sa.leaseNextCurrent(
        f.w,
        "collection",
        1,
        "lease",
        () => f.now.value,
      );
    let committed = false;
    const lossy: Database = {
      transaction: async (fn) => {
        await f.a.transaction(fn);
        committed = true;
        throw Error("fixture lost COMMIT acknowledgement");
      },
      close: async () => {},
    };
    await assert.rejects(
      f
        .store(lossy)
        .consumeCurrent(
          f.w,
          "collection",
          2,
          lease.lease.token,
          () => f.now.value,
        ),
      /lost COMMIT/,
    );
    assert.equal(committed, true);
    await f.a.close();
    const db = f.connect("fresh"),
      fresh = f.store(db),
      state = await fresh.load(f.c, "collection");
    assert.equal(state.state.leases[0]!.status, "running");
    await assert.rejects(
      fresh.consumeCurrent(
        f.w,
        "collection",
        3,
        lease.lease.token,
        () => f.now.value,
      ),
      { code: "lease_consumed" },
    );
    f.now.value = "2026-01-01T00:00:02Z";
    const recovered = await fresh.recoverCurrent(
      f.c,
      "collection",
      3,
      "recover",
      () => f.now.value,
    );
    assert.equal(recovered.state.leases[0]!.status, "reconcile");
    assert.equal(recovered.state.collection.attempts[0]!.status, "started");
    await assert.rejects(
      fresh.leaseNextCurrent(f.w, "collection", 4, "next", () => f.now.value),
      { code: "concurrency_limit" },
    );
    await scoped(db, f.c, async (tx) =>
      assert.ok(verifyAudit(await tx.audits())),
    );
  },
);
test(
  "network runtime rejects administrator and lowered administrator, schema creation and unscoped reads",
  { timeout: 20000 },
  async (t) => {
    const f = await fixture(t, "isolation");
    await assert.rejects(f.store(admin).load(f.c, "collection"), {
      code: "unsafe_database_role",
    });
    const lowered: Database = {
      transaction: (fn) =>
        admin.transaction(async (q) => {
          await q.query("SET LOCAL ROLE evidence_runtime");
          return fn(q);
        }),
      close: async () => {},
    };
    await assert.rejects(f.store(lowered).load(f.c, "collection"), {
      code: "unsafe_database_role",
    });
    await assert.rejects(
      f.b.transaction((q) =>
        q.query("CREATE TABLE public.discovery_forbidden(id integer)"),
      ),
    );
    await assert.rejects(f.b.transaction((q) => q.query("SET ROLE postgres")));
    const unscoped = await f.b.transaction(async (q) => {
      await q.query("SET LOCAL ROLE evidence_runtime");
      return q.query("SELECT * FROM public.evidence_record");
    });
    assert.equal(unscoped.rows.length, 0);
    await scoped(f.b, { ...f.c, projectId: "other-project" }, async (tx) =>
      assert.deepEqual(await tx.list("discovery-schedule-v1"), []),
    );
    assert.equal((await f.sb.load(f.c, "collection")).revision, 1);
  },
);
