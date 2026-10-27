import test from "node:test";
import assert from "node:assert/strict";
import { CatalogTraversal } from "../src/catalog-traversal.ts";
import { hashBytes } from "../src/contracts.ts";
import type { Context } from "../src/contracts.ts";
import type { Capture } from "../src/index.ts";
import type { JsonArchiveConfiguration } from "../src/connectors.ts";
const c: Context = {
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
const config: JsonArchiveConfiguration = {
  id: "archive",
  catalogPolicy: policy,
  itemPolicy: { ...policy, id: "items", allowedHosts: ["records.example"] },
  maximumEntries: 10,
};
const limits = {
  maximumPages: 10,
  maximumTotalBytes: 1024 * 1024,
  maximumItems: 10,
  maximumObservations: 100,
};
const entry = (
  id: string,
  url = "https://records.example/a",
  mediaType: string | null = "text/plain",
) => ({ id, url, mediaType });
function page(path: string, entries: unknown[], next: string | null): Capture {
  const bytes = Buffer.from(JSON.stringify({ version: 1, entries, next }));
  return {
    tenantId: "t",
    projectId: "p",
    sourcePolicyId: "catalog",
    requestedUrl: "https://archive.example/" + path,
    finalUrl: "https://archive.example/" + path,
    retrievedAt: c.now,
    status: 200,
    contentType: "application/json",
    bytes,
    sha256: hashBytes(bytes),
    redirects: [],
    etag: null,
    lastModified: null,
  };
}
const traversal = (overrides = {}) =>
  new CatalogTraversal(c, config, "https://archive.example/1", {
    ...limits,
    ...overrides,
  });
test("explicit pages deduplicate targets while preserving all aliases and exact capture provenance", () => {
  const t = traversal();
  assert.equal(t.snapshot().endDeclared, false);
  const p1 = page("1", [entry("a")], "https://archive.example/2"),
    p2 = page(
      "2",
      [entry("alias"), entry("b", "https://records.example/b")],
      null,
    );
  t.append(c, p1);
  const result = t.append(c, p2);
  assert.equal(result.pages.length, 2);
  assert.equal(result.proposals.length, 2);
  assert.equal(result.observationCount, 3);
  assert.equal(result.totalBytes, p1.bytes.length + p2.bytes.length);
  assert.equal(result.endDeclared, true);
  assert.deepEqual(
    result.proposals[0]!.observations.map((o) => o.advertisedId),
    ["a", "alias"],
  );
  assert.equal(
    result.proposals[0]!.observations[1]!.provenance.catalogSha256,
    p2.sha256,
  );
  assert.equal(result.nextUrl, null);
  assert.throws(() => t.append(c, p2), { code: "catalog_finished" });
});
test("loops through earlier requests, redirects or continuations and out-of-sequence pages fail atomically", () => {
  const t = traversal();
  t.append(c, page("1", [], "https://archive.example/2"));
  const before = t.snapshot();
  for (const input of [
    page("3", [], null),
    page("2", [], "https://archive.example/1"),
    { ...page("2", [], null), finalUrl: "https://archive.example/1" },
    { ...page("2", [], null), redirects: ["https://archive.example/1"] },
  ]) {
    assert.throws(() => t.append(c, input));
    assert.deepEqual(t.snapshot(), before);
  }
  t.append(c, page("2", [], null));
  assert.equal(t.snapshot().endDeclared, true);
});
test("page byte item and repeated-observation limits bound the whole traversal without partial commit", () => {
  for (const [overrides, first, second, code] of [
    [
      { maximumPages: 1 },
      page("1", [], "https://archive.example/2"),
      page("2", [], null),
      "catalog_page_limit",
    ],
    [
      { maximumTotalBytes: 150 },
      page("1", [], "https://archive.example/2"),
      page("2", [entry("b")], null),
      "catalog_byte_limit",
    ],
    [
      { maximumItems: 1 },
      page("1", [entry("a")], "https://archive.example/2"),
      page("2", [entry("b", "https://records.example/b")], null),
      "catalog_item_limit",
    ],
    [
      { maximumObservations: 1 },
      page("1", [entry("a")], "https://archive.example/2"),
      page("2", [entry("a")], null),
      "catalog_observation_limit",
    ],
  ] as const) {
    const t = traversal(overrides);
    t.append(c, first);
    const before = t.snapshot();
    assert.throws(() => t.append(c, second), { code });
    assert.deepEqual(t.snapshot(), before);
  }
  assert.throws(() => traversal({ maximumPages: 101 }), {
    code: "invalid_traversal_limit",
  });
});
test("cross-page identity and media conflicts, foreign scope and altered bytes cannot change a checkpoint", () => {
  const t = traversal();
  t.append(c, page("1", [entry("a")], "https://archive.example/2"));
  const before = t.snapshot();
  for (const input of [
    page("2", [entry("a", "https://records.example/other")], null),
    page(
      "2",
      [entry("alias", "https://records.example/a", "application/pdf")],
      null,
    ),
    { ...page("2", [], null), sha256: "0".repeat(64) },
  ]) {
    assert.throws(() => t.append(c, input));
    assert.deepEqual(t.snapshot(), before);
  }
  assert.throws(
    () => t.append({ ...c, tenantId: "foreign" }, page("2", [], null)),
    { code: "scope_denied" },
  );
  assert.throws(
    () => t.append({ ...c, purpose: "other" }, page("2", [], null)),
    { code: "purpose_denied" },
  );
});
test("caller mutation cannot alter admission, retained history or stable root-scoped proposal identities", () => {
  const cfg = structuredClone(config),
    t = new CatalogTraversal(c, cfg, "https://archive.example/1", limits);
  cfg.itemPolicy.allowedHosts = ["evil.example"];
  const source = page("1", [entry("a")], null),
    result = t.append(c, source),
    id = result.proposals[0]!.item.id;
  source.bytes.fill(0);
  result.proposals[0]!.item.url = "https://evil.example/";
  assert.equal(
    t.snapshot().proposals[0]!.item.url,
    "https://records.example/a",
  );
  assert.equal(
    traversal().append(c, page("1", [entry("a")], null)).proposals[0]!.item.id,
    id,
  );
  assert.equal(
    t.snapshot().pages[0]!.sha256,
    hashBytes(page("1", [entry("a")], null).bytes),
  );
});
test("canonical prior redirect aliases cannot evade loop fencing and policy expiry leaves state intact", () => {
  const t = traversal(),
    first = page("1", [], "https://archive.example/2");
  first.finalUrl = "https://archive.example:443/canonical";
  first.redirects = [first.finalUrl];
  t.append(c, first);
  const before = t.snapshot();
  assert.throws(
    () =>
      t.append(c, {
        ...page("2", [], null),
        finalUrl: "https://archive.example/canonical",
      }),
    { code: "catalog_loop" },
  );
  assert.throws(
    () => t.append({ ...c, now: "2026-02-01T00:00:00Z" }, page("2", [], null)),
    { code: "connector_disabled" },
  );
  assert.deepEqual(t.snapshot(), before);
});
