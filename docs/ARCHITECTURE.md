# Acquisition boundary

This library captures one explicitly approved HTTPS source at a time. Source policies bind tenant, project, exact hostname allowlist, expiry, access basis, byte/redirect budgets and an identifying user agent. DNS resolution is checked for public IPv4 addresses and the chosen address is pinned into the HTTPS connection. Each redirect repeats URL, host and DNS checks. Credentials, fragments, custom ports, private/reserved addresses, compressed transport and partial responses are rejected. TLS continues to verify the original hostname.

Captured exact bytes carry request/final URLs, retrieval time, redirects, response metadata and SHA256. A capture is not a factual conclusion or permission to publish. Captured files must enter evidence quarantine before processing. Tests inject DNS and transport; no live capture is required.

Current limits: IPv4 HTTPS only, four explicit MIME types, no bulk crawler, provider-specific archive API adapters, robots-policy evaluator or deployed worker daemon yet. The opt-in JSON archive catalog adapter and connector SDK are described in CONNECTORS.md. Policy is trusted deployment configuration and must be enforced by an authenticated action gateway. A bounded total deadline covers DNS resolution, redirects and transport awaits. Cancellation reaches the transport; a timed-out resolver can never later start a transport call. Native DNS may continue its underlying OS query after the awaiting capture has failed; deployment concurrency limits remain required. Future work is retained in registry/prs.json.

## Collection acquisition contract

`createCollection`, `dueItems`, `startAcquisition`, `finishAcquisition` and `collectionCoverage` implement bounded collection/refresh planning with exact normalized allowlisted URLs, immutable attempt receipts, refresh cooldown and explicit capture/failure outcomes. Completion requires the durable evidence receipt supplied by the storage adapter. Coverage distinguishes expected items, successfully captured items, unseen/failed work, acquisition attempts and unique byte editions; repeated refreshes do not become new incidents or independent sources.

State must be persisted with atomic revision comparison before leasing an acquisition and after its evidence receipt is committed. The optional durable scheduler described below retains started attempts until completion or explicit recovery. A capture digest and collection count are provenance/coverage signals, not proof of factual claims.

## Asynchronous admission and output ownership

Capture snapshots authenticated context, source policy and configured resolver/transport functions before awaiting dependencies. Caller mutation cannot change scope, approved hosts, byte limits or provenance mid-flight. DNS answers are copied before validation and the checked address is passed as an immutable pin; transports receive a separate URL object. Returned byte arrays are independent copies, so later adapter buffer reuse cannot invalidate their digest. Provider errors are redacted, including adapter domain errors. The optional trusted `timeoutMs` is 10-30000 milliseconds (default 30000), and `signal` enables cancellation. A pre-aborted signal performs no DNS or transport work. One deadline spans all redirects; individual operations do not reset it.

A custom adapter is trusted deployment code and must honor the signal and constrain its own resources. The wrapper bounds awaiting it but cannot terminate arbitrary noncooperating code or an OS DNS request. Tests use fictional URLs and injected dependencies only; no external live capture or deployment acceptance is claimed by these injected tests. Scope/expiry admission is a captured capability decision for this bounded attempt; a future gateway must coordinate mid-flight revocation if required.

Native transport qualification also uses a local ephemeral TLS server and a child-specific test CA, without disabling hostname verification. It checks the actual pinned lookup, Host authority, wrong-host certificate rejection, oversized response, compressed response and cancellation. This intentionally invokes the transport directly with a loopback pin for the test; the public capture URL/address layer continues to reject loopback. No external endpoint or global trust-store change is used. The test requires the Node 24 runtime and `/usr/bin/openssl`, both available in CI.

## Durable scheduler adapter

The opt-in `./scheduler` adapter persists approved collection scheduling, actor-bound leases, one-shot work consumption, verified outcomes and uncertainty recovery in its own operational database. See SCHEDULER.md for trusted-host contracts, database roles, worker obligations and qualification limits. A deployed worker daemon and external receipt-service integration remain separate.
