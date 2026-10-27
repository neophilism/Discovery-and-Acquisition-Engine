import { allowedUrl } from './index.ts';
import type { Capture } from './index.ts';
import { decodeJsonArchive } from './connectors.ts';
import type { CatalogEntry, JsonArchiveConfiguration } from './connectors.ts';
import { digest, invariant, nonempty, requireScope } from './contracts.ts';
import type { Context, Scope } from './contracts.ts';
import type { CollectionItem } from './collection.ts';
export type CatalogTraversalLimits = {
  maximumPages: number; maximumTotalBytes: number; maximumItems: number; maximumObservations: number;
};
export type CatalogItemProposal = { item: CollectionItem; observations: readonly CatalogEntry[]; };
export type CatalogPageRecord = {
  requestedUrl: string; finalUrl: string; redirects: readonly string[]; sha256: string;
  retrievedAt: string; byteCount: number; sourcePolicyId: string; nextUrl: string|null;
};
export type CatalogTraversalSnapshot = Scope & {
  schemaVersion: 1; connectorId: string; rootUrl: string; nextUrl: string|null;
  endDeclared: boolean; totalBytes: number; observationCount: number;
  pages: readonly CatalogPageRecord[]; proposals: readonly CatalogItemProposal[];
};
/** In-memory proposal planning, not durable archival, authentication or a network crawler. */
export class CatalogTraversal {
  readonly #config: JsonArchiveConfiguration;
  readonly #limits: CatalogTraversalLimits;
  readonly #purpose: string;
  #state: CatalogTraversalSnapshot;
  constructor(context: Context, configuration: JsonArchiveConfiguration, rootUrl: string, limits: CatalogTraversalLimits) {
    this.#config=structuredClone(configuration);this.#limits=structuredClone(limits);this.#purpose=context.purpose;
    requireScope(context,this.#config.catalogPolicy);requireScope(context,this.#config.itemPolicy);nonempty(context.purpose,'catalog purpose',200);
    for(const [value,maximum] of [[limits.maximumPages,100],[limits.maximumTotalBytes,25*1024*1024],[limits.maximumItems,1000],[limits.maximumObservations,10000]])
      invariant(Number.isSafeInteger(value)&&value!>=1&&value!<=maximum!,'invalid_traversal_limit','Catalog traversal limits are outside supported bounds');
    const root=allowedUrl(rootUrl,this.#config.catalogPolicy).href;
    this.#state={tenantId:context.tenantId,projectId:context.projectId,schemaVersion:1,connectorId:this.#config.id,rootUrl:root,nextUrl:root,endDeclared:false,totalBytes:0,observationCount:0,pages:[],proposals:[]};
  }
  snapshot(): CatalogTraversalSnapshot {return structuredClone(this.#state);}
  /** The trusted host explicitly supplies one captured page. Rejection leaves the prior checkpoint unchanged. */
  append(context: Context, source: Capture): CatalogTraversalSnapshot {
    requireScope(context,this.#state);
    invariant(context.purpose===this.#purpose,'purpose_denied','Traversal purpose differs');
    invariant(!this.#state.endDeclared,'catalog_finished','Catalog has already declared its end');
    invariant(this.#state.pages.length<this.#limits.maximumPages,'catalog_page_limit','Catalog page budget exhausted');
    invariant(source.bytes.length<=1024*1024&&this.#state.totalBytes+source.bytes.length<=this.#limits.maximumTotalBytes,'catalog_byte_limit','Catalog byte budget exhausted');
    const page=decodeJsonArchive(context,this.#config,source),captured=page.capture;
    const requested=allowedUrl(captured.requestedUrl,this.#config.catalogPolicy).href;
    invariant(requested===this.#state.nextUrl,'catalog_sequence','Capture does not match the explicit next page');
    const visited=new Set(this.#state.pages.flatMap(p=>[p.requestedUrl,p.finalUrl,...p.redirects]));
    const addresses=[requested,captured.finalUrl,...captured.redirects].map(url=>allowedUrl(url,this.#config.catalogPolicy).href);
    invariant(addresses.every(url=>!visited.has(url))&&(page.nextUrl===null||(!visited.has(page.nextUrl)&&!addresses.includes(page.nextUrl))),'catalog_loop','Catalog continuation or redirect revisits a prior page');
    const count=this.#state.observationCount+page.entries.length;
    invariant(count<=this.#limits.maximumObservations,'catalog_observation_limit','Catalog observation budget exhausted');
    const proposals=structuredClone(this.#state.proposals) as CatalogItemProposal[];
    const byUrl=new Map(proposals.map(p=>[p.item.url,p]));
    const byAdvertisedId=new Map(proposals.flatMap(p=>p.observations.map(o=>[o.advertisedId,p.item.url] as const)));
    for(const entry of page.entries) {
      invariant(!byAdvertisedId.has(entry.advertisedId)||byAdvertisedId.get(entry.advertisedId)===entry.item.url,'catalog_identity_conflict','An advertised catalog identity changed its target');
      byAdvertisedId.set(entry.advertisedId,entry.item.url);
      const existing=byUrl.get(entry.item.url);
      if(existing) {
        invariant(existing.item.expectedMediaType===entry.item.expectedMediaType,'catalog_media_conflict','Duplicate target has conflicting advertised media types');
        existing.observations=[...existing.observations,entry];
      } else {
        invariant(proposals.length<this.#limits.maximumItems,'catalog_item_limit','Unique catalog item budget exhausted');
        const proposal:CatalogItemProposal={item:{...entry.item,id:digest({connectorId:this.#config.id,rootUrl:this.#state.rootUrl,url:entry.item.url})},observations:[entry]};
        proposals.push(proposal);byUrl.set(entry.item.url,proposal);
      }
    }
    const record:CatalogPageRecord={requestedUrl:requested,finalUrl:captured.finalUrl,redirects:captured.redirects,sha256:captured.sha256,retrievedAt:captured.retrievedAt,byteCount:captured.bytes.length,sourcePolicyId:captured.sourcePolicyId,nextUrl:page.nextUrl};
    this.#state={...this.#state,nextUrl:page.nextUrl,endDeclared:page.nextUrl===null,totalBytes:this.#state.totalBytes+captured.bytes.length,observationCount:count,pages:[...this.#state.pages,record],proposals};
    return this.snapshot();
  }
}
