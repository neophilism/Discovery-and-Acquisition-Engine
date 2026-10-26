# Acquisition boundary

This library captures one explicitly approved HTTPS source at a time. Source policies bind tenant, project, exact hostname allowlist, expiry, access basis, byte/redirect budgets and an identifying user agent. DNS resolution is checked for public IPv4 addresses and the chosen address is pinned into the HTTPS connection. Each redirect repeats URL, host and DNS checks. Credentials, fragments, custom ports, private/reserved addresses, compressed transport and partial responses are rejected. TLS continues to verify the original hostname.

Captured exact bytes carry request/final URLs, retrieval time, redirects, response metadata and SHA256. A capture is not a factual conclusion or permission to publish. Captured files must enter evidence quarantine before processing. Tests inject DNS and transport; no live capture is required.

Current limits: IPv4 HTTPS only, four explicit MIME types, no bulk crawler, archive API adapters, robots-policy evaluator, durable queue or UI yet. Policy is trusted deployment configuration and must be enforced by an authenticated action gateway. DNS latency is provider-dependent. Future work is retained in registry/prs.json.
