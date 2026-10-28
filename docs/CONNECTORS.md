# Connector SDK and JSON archive catalogs

The opt-in `./connectors` export defines `CatalogConnector`, `CatalogPage` and `CatalogEntry` and implements `JsonArchiveConnector`. It reads a single explicitly approved catalog through the existing HTTPS capture boundary. There is no automatic crawl, item download, continuation following or publication. The host must authenticate and authorize the operation and budget repeated calls.

The supported country-neutral interchange schema is exactly:

```json
{"version":1,"entries":[{"id":"record-1","url":"https://records.example/item","mediaType":"application/pdf"}],"next":null}
```

All fields are required; unknown fields are rejected. `mediaType` is null or one of the four capture MIME types. Entries use absolute URLs, and `next` is null or an absolute approved catalog URL. Catalog and item policies are separate, trusted configuration in the same scope. Untrusted catalog content cannot change either policy. Up to 1000 entries and 1 MiB of valid UTF-8 JSON are supported, subject to tighter configured budgets. Duplicate IDs/normalized URLs and same-page continuation loops fail closed. Across pages the host must enforce a total page/byte budget, loop detection and duplicate reconciliation; the adapter makes no completeness claim.

Each proposal retains the advertised ID/URL, normalized acquisition URL, stable adapter-scoped item ID, index position, exact catalog digest, request/final URLs and retrieval time. The returned capture includes independent exact bytes for evidence quarantine. Digest equality checks content consistency; it does not authenticate an arbitrary supplied Capture or prove a catalog statement. The pure decoder accepts trusted capture-service output, not direct unauthenticated remote requests. A host must commit catalog bytes and verify an evidence receipt before treating them as durably archived. No receipt is invented by this adapter.

The output is a proposal, not an authorization or a factual finding. A human owner still authorizes collection creation with the independently approved item policy. Item acquisition repeats HTTPS/DNS/media checks and must enter evidence quarantine. Stable IDs use connector ID, requested catalog URL and advertised entry ID: adapters deployed with changed semantics need a new connector identity. Byte editions are retained in provenance separately.

Tests use fictional catalogs and injected network functions over the actual capture/collection boundaries. No external archive service, archive-specific API compatibility, full WARC/ZIP parsing, robots-policy evaluation, durable scheduler or deployed credentials are qualified by this work. Third-party connector implementations and injected dependencies are trusted deployment code; the interface does not sandbox arbitrary code. Capture cancellation/deadlines are reused for each call, not a cross-page job deadline.

## Explicit cross-page traversal

`./catalog-traversal` exports `CatalogTraversal`, an in-memory proposal planner over the JSON adapter. The host explicitly supplies each trusted Capture in the declared sequence. Append decodes the original bytes again under fixed catalog/item policies and purpose, then atomically advances its private checkpoint. A failed page leaves the previous checkpoint intact. There is no automatic network following, persistence, resume import, evidence-receipt verification or collection authorization.

Configurable aggregate limits are capped at 100 pages, 25 MiB of accepted catalog bytes, 1000 unique targets and 10000 observations. Repeated observations consume the observation/byte budgets even when their targets deduplicate. Page-budget exhaustion with a remaining continuation is incomplete, never a successful end. `endDeclared` means only that an accepted catalog page advertised `next: null`; it makes no claim that the archive or underlying historical record is complete. Because captures are supplied after retrieval, the host must apply remaining request/byte/cost budgets before performing I/O; acceptance limits cannot undo an already-issued request.

Previously visited requested/final/redirect URL aliases are normalized before loop detection. Within this catalog schema, advertised IDs use a traversal-wide namespace: changing an ID's target is a conflict. Different IDs for the same normalized target reconcile into one proposal only when their advertised media types agree exactly (including null); conflicting metadata is not silently guessed. Every alias and original per-page observation/provenance remains retained. Root-scoped proposal IDs bind connector, canonical root URL and normalized target URL. They deliberately differ from single-page observation IDs. No item/observation counts become incident counts or independent corroboration counts.

Snapshots are detached metadata copies, not authentication or durable archival receipts; exact captured bytes must be quarantined and archived separately by the trusted host. The planner contains no source policies in its returned snapshot and performs no sends, publication or actual item acquisition. Provider-specific API adapters, durable traversal checkpoints and deployed orchestration remain pending.

## Durable traversal checkpoints

`./catalog-checkpoint` exports `CatalogCheckpointStore`, an opt-in operational metadata store over the pinned Evidence database SDK. A trusted host supplies an authenticated `Context`, fixed JSON connector configuration, aggregate traversal limits and an explicit checkpoint identity. Creation persists the empty traversal at revision 1. Append requires the exact observed revision, holds the project transaction lock, revalidates the stored scope, purpose, configuration digest and limits digest, then advances and persists the page atomically. Concurrent stale writers fail rather than skipping, duplicating or overwriting a page.

A stored checkpoint contains normalized traversal metadata, page provenance, proposal observations and aggregate budgets. It does not contain captured bytes, credentials, evidence receipts or a transferable acquisition authorization. The host must quarantine exact bytes and verify evidence receipts separately before relying on durable archival. Reopening the operational database and constructing the store with the identical binding resumes the declared next page; altered scope, purpose, connector policy/configuration or limits fail closed. Loading historical metadata is not permission to perform current network I/O, and each append repeats the connector's current policy-expiry and capture-integrity checks.

Tests apply the canonical migration under the restricted runtime role, reopen a filesystem-backed PGlite database, resume the next page, race exact-revision contenders and verify rejected writes leave the prior revision unchanged. PGlite does not establish independent PostgreSQL network-session contention, deployed authentication, provider compatibility, live capture, evidence-service integration or production acceptance. The store performs no crawling, automatic continuation, item download, sends, publication or completeness claim. Provider APIs and archive-format parsers remain future work.

