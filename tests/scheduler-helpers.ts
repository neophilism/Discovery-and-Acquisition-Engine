import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import type { Database } from "@accountability/evidence/database";
import { AcquisitionScheduler } from "../src/scheduler.ts";
import type { OutcomeVerifier, SchedulerPolicy } from "../src/scheduler.ts";
import { hashBytes } from "../src/contracts.ts";
import type { Context, Ref } from "../src/contracts.ts";
import type { Capture, SourcePolicy } from "../src/index.ts";
export const owner: Context = {
  tenantId: "t",
  projectId: "p",
  actor: { id: "owner", kind: "human", roles: ["owner"] },
  purpose: "research",
  requestId: "r",
  now: "2026-01-01T00:00:00Z",
};
export const worker: Context = {
  ...owner,
  actor: { id: "worker", kind: "service", roles: ["discovery:acquire"] },
};
export const schedulerPolicy: SchedulerPolicy = {
  tenantId: "t",
  projectId: "p",
  purpose: "research",
  managerIds: ["owner"],
  workers: [
    { id: "worker", kind: "service" },
    { id: "other", kind: "service" },
  ],
  maximumConcurrent: 1,
  leaseMs: 1000,
  receiptTimeoutMs: 20,
};
const sourcePolicy: SourcePolicy = {
  tenantId: "t",
  projectId: "p",
  id: "source",
  allowedHosts: ["records.example"],
  enabled: true,
  expiresAt: "2026-02-01T00:00:00Z",
  maximumBytes: 1024,
  maximumRedirects: 1,
  userAgent: "Research/1",
  accessBasis: "Authorized fictional test",
};
export const collection = {
  id: "collection",
  sourcePolicy,
  items: [
    {
      id: "a",
      url: "https://records.example/a",
      expectedMediaType: "text/plain",
    },
    {
      id: "b",
      url: "https://records.example/b",
      expectedMediaType: "text/plain",
    },
  ],
  refreshAfterMs: 60000,
};
export const receipt: Ref = {
  tenantId: "t",
  projectId: "p",
  kind: "acquisition-receipt",
  id: "verified",
  revision: 2,
};
export function capture(now = owner.now): Capture {
  const bytes = Buffer.from("Fictional archive record");
  return {
    tenantId: "t",
    projectId: "p",
    sourcePolicyId: "source",
    requestedUrl: collection.items[0]!.url,
    finalUrl: collection.items[0]!.url,
    retrievedAt: now,
    status: 200,
    contentType: "text/plain",
    bytes,
    sha256: hashBytes(bytes),
    redirects: [],
    etag: null,
    lastModified: null,
  };
}
export const verifier: OutcomeVerifier = {
  verify: async (c) =>
    c.receipt.id === "verified" &&
    c.itemId === "a" &&
    c.requestedUrl === collection.items[0]!.url &&
    (c.outcome === "no-capture" || c.sourceSha256 === capture().sha256),
};
export function driver(
  pg: PGlite,
  fault: { beforeCommit?: boolean; afterCommit?: boolean } = {},
): Database {
  return {
    transaction: async (fn) => {
      const result = await pg.transaction(async (q) => {
        const value = await fn({
          query: async <T>(sql: string, values?: unknown[]) => ({
            rows: (await q.query(sql, values)).rows as T[],
          }),
        });
        if (fault.beforeCommit) throw Error("transaction failure");
        return value;
      });
      if (fault.afterCommit) throw Error("lost acknowledgement");
      return result;
    },
    close: () => pg.close(),
  };
}
export async function setup(
  t: { after(fn: () => Promise<void>): void },
  verify = verifier,
  persistent = false,
) {
  const path = persistent
    ? await mkdtemp(join(tmpdir(), "discovery-test-"))
    : undefined;
  let pg = new PGlite(path);
  await pg.exec(
    await readFile(
      new URL(
        "../migrations/001_foundation.sql",
        import.meta.resolve("@accountability/evidence/database"),
      ),
      "utf8",
    ),
  );
  await pg.exec(
    "CREATE ROLE discovery_test_login LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION; GRANT evidence_runtime TO discovery_test_login; SET SESSION AUTHORIZATION discovery_test_login",
  );
  let db = driver(pg),
    store = new AcquisitionScheduler(db, schedulerPolicy, verify);
  t.after(async () => {
    await pg.close();
    if (path) await rm(path, { recursive: true, force: true });
  });
  await store.create(owner, collection, "create");
  return {
    get db() {
      return db;
    },
    get store() {
      return store;
    },
    get pg() {
      return pg;
    },
    async restart() {
      if (!path) throw Error("Persistent fixture required");
      await pg.close();
      pg = new PGlite(path);
      await pg.exec("SET SESSION AUTHORIZATION discovery_test_login");
      db = driver(pg);
      store = new AcquisitionScheduler(db, schedulerPolicy, verify);
    },
  };
}
