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
