import test from "node:test";
import assert from "node:assert/strict";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { CatalogCheckpointStore } from "../src/catalog-checkpoint.ts";
import { hashBytes } from "../src/contracts.ts";
import type { Context } from "../src/contracts.ts";
import type { Capture } from "../src/index.ts";
import type { JsonArchiveConfiguration } from "../src/connectors.ts";
import { driver } from "./scheduler-helpers.ts";

const context: Context = {
  tenantId: "t",
  projectId: "p",
  actor: { id: "worker", kind: "service", roles: [] },
  purpose: "research",
  requestId: "r",
  now: "2026-01-01T00:00:00Z",
};
const policy = {
  tenantId: "t",
  projectId: "p",
  id: "catalog",
  allowedHosts: ["archive.example"],
  enabled: true,
  expiresAt: "2026-02-01T00:00:00Z",
  maximumBytes: 1024 * 1024,
  maximumRedirects: 2,
  userAgent: "Research/1",
  accessBasis: "Fictional test",
};
const configuration: JsonArchiveConfiguration = {
  id: "archive",
  catalogPolicy: policy,
  itemPolicy: {
    ...policy,
    id: "items",
    allowedHosts: ["records.example"],
  },
  maximumEntries: 10,
};
const limits = {
  maximumPages: 10,
  maximumTotalBytes: 1024 * 1024,
  maximumItems: 10,
  maximumObservations: 100,
};

function page(path: string, id: string, next: string | null): Capture {
  const bytes = Buffer.from(
    JSON.stringify({
      version: 1,
      entries: [
        {
          id,
          url: `https://records.example/${id}`,
          mediaType: "text/plain",
        },
      ],
      next,
    }),
  );
  return {
    tenantId: "t",
    projectId: "p",
    sourcePolicyId: "catalog",
    requestedUrl: `https://archive.example/${path}`,
    finalUrl: `https://archive.example/${path}`,
    retrievedAt: context.now,
    status: 200,
    contentType: "application/json",
    bytes,
    sha256: hashBytes(bytes),
    redirects: [],
    etag: null,
    lastModified: null,
  };
}

async function fixture(
  t: { after(fn: () => Promise<void>): void },
  persistent = false,
) {
  const path = persistent
    ? await mkdtemp(join(tmpdir(), "catalog-checkpoint-"))
    : undefined;
  let pg = new PGlite(path),
    closed = false;
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
    "CREATE ROLE catalog_checkpoint_test LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION; GRANT evidence_runtime TO catalog_checkpoint_test; SET SESSION AUTHORIZATION catalog_checkpoint_test",
  );
  let database = driver(pg),
    store = new CatalogCheckpointStore(database, configuration, limits);
  t.after(async () => {
    if (!closed) await pg.close();
    if (path) await rm(path, { recursive: true, force: true });
  });
  return {
    get store() {
      return store;
    },
    get database() {
      return database;
    },
    async restart() {
      assert(path);
      await pg.close();
      closed = true;
      pg = new PGlite(path);
      await pg.exec("SET SESSION AUTHORIZATION catalog_checkpoint_test");
      database = driver(pg);
      store = new CatalogCheckpointStore(database, configuration, limits);
      closed = false;
    },
  };
}

test("durable traversal resumes after database reopen with exact provenance and sequence", async (t) => {
  const f = await fixture(t, true);
  const created = await f.store.create(
    context,
    "records",
    "https://archive.example/1",
  );
  const first = await f.store.append(
    context,
    "records",
    created.revision,
    page("1", "a", "https://archive.example/2"),
  );
  assert.equal(first.revision, 2);
  await f.restart();
  const resumed = await f.store.load(context, "records");
  assert.equal(resumed.revision, 2);
  assert.equal(resumed.state.traversal.nextUrl, "https://archive.example/2");
  const finished = await f.store.append(
    context,
    "records",
    resumed.revision,
    page("2", "b", null),
  );
  assert.equal(finished.revision, 3);
  assert.equal(finished.state.traversal.endDeclared, true);
  assert.deepEqual(
    finished.state.traversal.proposals.map((proposal) => proposal.item.url),
    ["https://records.example/a", "https://records.example/b"],
  );
  assert.equal(finished.state.traversal.pages[0]!.sha256, page(
    "1",
    "a",
    "https://archive.example/2",
  ).sha256);
});

test("optimistic contenders commit at most one page", async (t) => {
  const f = await fixture(t);
  await f.store.create(context, "records", "https://archive.example/1");
  const results = await Promise.allSettled([
    f.store.append(
      context,
      "records",
      1,
      page("1", "a", "https://archive.example/2"),
    ),
    f.store.append(
      context,
      "records",
      1,
      page("1", "a", "https://archive.example/2"),
    ),
  ]);
  assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
  assert.equal(results.filter((result) => result.status === "rejected").length, 1);
  const current = await f.store.load(context, "records");
  assert.equal(current.revision, 2);
  assert.equal(current.state.traversal.pages.length, 1);
});

test("scope purpose configuration sequence and stale revision rejection are atomic", async (t) => {
  const f = await fixture(t);
  await f.store.create(context, "records", "https://archive.example/1");
  for (const operation of [
    () =>
      f.store.append(
        { ...context, tenantId: "foreign" },
        "records",
        1,
        page("1", "a", null),
      ),
    () =>
      f.store.append(
        { ...context, purpose: "other" },
        "records",
        1,
        page("1", "a", null),
      ),
    () => f.store.append(context, "records", 0, page("1", "a", null)),
    () => f.store.append(context, "records", 1, page("2", "a", null)),
  ])
    await assert.rejects(operation);
  const changedLimits = new CatalogCheckpointStore(
    f.database,
    configuration,
    { ...limits, maximumPages: 9 },
  );
  await assert.rejects(changedLimits.load(context, "records"));
  const current = await f.store.load(context, "records");
  assert.equal(current.revision, 1);
  assert.equal(current.state.traversal.pages.length, 0);
});
