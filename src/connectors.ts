import {
  digest, DomainError, hashBytes, invariant, nonempty, requireScope, validTime,
} from './contracts.ts';
import type { Context, Scope } from './contracts.ts';
import { allowedUrl, capture } from './index.ts';
import type { Capture, CaptureDependencies, SourcePolicy } from './index.ts';
import type { CollectionItem } from './collection.ts';

/** Untrusted catalog metadata becomes proposals, never an acquisition grant. */
export interface CatalogEntry {
  item: CollectionItem;
  advertisedId: string;
  advertisedUrl: string;
  provenance: {
    connectorId: string;
    adapterVersion: 'json-archive-v1';
    catalogRequestedUrl: string;
    catalogFinalUrl: string;
    catalogSha256: string;
    retrievedAt: string;
    entryIndex: number;
  };
}
export interface CatalogPage extends Scope {
  connectorId: string;
  capture: Capture;
  entries: readonly CatalogEntry[];
  /** Explicit continuation proposal. The adapter never automatically follows it. */
  nextUrl: string | null;
}
export interface CatalogConnector {
  readonly id: string;
  read(context: Context, url: string, dependencies?: CaptureDependencies): Promise<CatalogPage>;
}
export interface JsonArchiveConfiguration {
  id: string;
  catalogPolicy: SourcePolicy;
  itemPolicy: SourcePolicy;
  maximumEntries: number;
}
const types = ['text/plain', 'text/html', 'application/json', 'application/pdf', 'application/warc', 'application/zip'];
const maximumCatalogBytes = 1024 * 1024;

function policyAdmission(context: Context, policy: SourcePolicy): void {
  requireScope(context, policy);
  invariant(policy.enabled === true && validTime(context.now) < validTime(policy.expiresAt),
    'connector_disabled', 'Connector policy is disabled or expired');
  nonempty(policy.id, 'policy identity', 128);
  nonempty(policy.accessBasis, 'access basis');
}
function configuration(context: Context, input: JsonArchiveConfiguration): JsonArchiveConfiguration {
  const config = structuredClone(input);
  nonempty(config.id, 'connector identity', 128);
  policyAdmission(context, config.catalogPolicy);
  policyAdmission(context, config.itemPolicy);
  invariant(Number.isSafeInteger(config.maximumEntries) && config.maximumEntries >= 1 &&
    config.maximumEntries <= 1000, 'invalid_connector', 'Catalog entry limit must be 1-1000');
  return config;
}
function object(value: unknown, fields: readonly string[]): Record<string, unknown> {
  invariant(value !== null && typeof value === 'object' && !Array.isArray(value),
    'invalid_catalog', 'Catalog object required');
  const result = value as Record<string, unknown>;
  invariant(Object.keys(result).length === fields.length && fields.every(key => Object.hasOwn(result, key)),
    'invalid_catalog', 'Catalog fields do not match the supported schema');
  return result;
}
function approved(raw: unknown, policy: SourcePolicy): string {
  nonempty(raw, 'catalog URL', 4096);
  try { return allowedUrl(raw, policy).href; }
  catch { throw new DomainError('catalog_url_denied', 'Catalog URL is outside approved source policy'); }
}

/** Decode an already captured catalog. A matching hash is provenance, not authentication. */
export function decodeJsonArchive(
  context: Context, input: JsonArchiveConfiguration, source: Capture,
): CatalogPage {
  const ctx = structuredClone(context), config = configuration(ctx, input);
  const captured = structuredClone(source);
  requireScope(ctx, captured);
  invariant(captured.sourcePolicyId === config.catalogPolicy.id && captured.status === 200 &&
    captured.contentType === 'application/json', 'catalog_capture_mismatch', 'Expected this policy’s complete JSON capture');
  invariant(captured.bytes.length <= Math.min(maximumCatalogBytes, config.catalogPolicy.maximumBytes) &&
    hashBytes(captured.bytes) === captured.sha256,
    'catalog_capture_mismatch', 'Catalog capture exceeds budget or differs from its digest');
  invariant(validTime(captured.retrievedAt) <= validTime(ctx.now) &&
    validTime(captured.retrievedAt) < validTime(config.catalogPolicy.expiresAt),
    'catalog_capture_mismatch', 'Catalog capture time is outside policy');
  approved(captured.requestedUrl, config.catalogPolicy);
  approved(captured.finalUrl, config.catalogPolicy);
  invariant(captured.redirects.length <= config.catalogPolicy.maximumRedirects,
    'catalog_capture_mismatch', 'Catalog redirect budget exceeded');
  for (const url of captured.redirects) approved(url, config.catalogPolicy);
  let decoded: unknown;
  try { decoded = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(captured.bytes)); }
  catch { throw new DomainError('invalid_catalog', 'Catalog must contain valid UTF-8 JSON'); }
  const root = object(decoded, ['version', 'entries', 'next']);
  invariant(root.version === 1 && Array.isArray(root.entries) && root.entries.length <= config.maximumEntries,
    'invalid_catalog', 'Unsupported catalog version or entry limit');
  const ids = new Set<string>(), urls = new Set<string>();
  const entries = root.entries.map((value: unknown, entryIndex: number): CatalogEntry => {
    const entry = object(value, ['id', 'url', 'mediaType']);
    nonempty(entry.id, 'catalog entry identity', 128);
    const url = approved(entry.url, config.itemPolicy);
    invariant(entry.mediaType === null || (typeof entry.mediaType === 'string' && types.includes(entry.mediaType)),
      'invalid_catalog', 'Unsupported advertised media type');
    invariant(!ids.has(entry.id) && !urls.has(url), 'duplicate_catalog_entry', 'Duplicate catalog identity or normalized URL');
    ids.add(entry.id); urls.add(url);
    return {
      item: {
        id: digest({ connectorId: config.id, catalogUrl: captured.requestedUrl, entryId: entry.id }),
        url, expectedMediaType: entry.mediaType,
      },
      advertisedId: entry.id, advertisedUrl: entry.url as string,
      provenance: {
        connectorId: config.id, adapterVersion: 'json-archive-v1',
        catalogRequestedUrl: captured.requestedUrl, catalogFinalUrl: captured.finalUrl,
        catalogSha256: captured.sha256, retrievedAt: captured.retrievedAt, entryIndex,
      },
    };
  });
  let nextUrl: string | null = null;
  if (root.next !== null) {
    nextUrl = approved(root.next, config.catalogPolicy);
    invariant(![captured.requestedUrl, captured.finalUrl, ...captured.redirects].includes(nextUrl),
      'catalog_loop', 'Catalog continuation repeats this capture');
  }
  return { tenantId: ctx.tenantId, projectId: ctx.projectId, connectorId: config.id,
    capture: captured, entries, nextUrl };
}

/** Trusted adapter implementation; transports remain injected deployment code. */
export class JsonArchiveConnector implements CatalogConnector {
  readonly id: string;
  #config: JsonArchiveConfiguration;
  constructor(config: JsonArchiveConfiguration) {
    this.#config = structuredClone(config);
    this.id = config.id;
  }
  async read(context: Context, url: string, dependencies: CaptureDependencies = {}): Promise<CatalogPage> {
    const ctx = structuredClone(context), config = configuration(ctx, this.#config);
    // A catalog has a tighter budget than individual evidence objects.
    config.catalogPolicy.maximumBytes = Math.min(config.catalogPolicy.maximumBytes, maximumCatalogBytes);
    const requested = approved(url, config.catalogPolicy);
    const result = await capture(ctx, requested, config.catalogPolicy, dependencies);
    return decodeJsonArchive(ctx, config, result);
  }
}
