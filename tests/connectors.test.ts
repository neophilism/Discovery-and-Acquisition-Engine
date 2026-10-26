import test from 'node:test';
import assert from 'node:assert/strict';
import { JsonArchiveConnector, decodeJsonArchive } from '../src/connectors.ts';
import { capture } from '../src/index.ts';
import { createCollection } from '../src/collection.ts';
import { hashBytes } from '../src/contracts.ts';
import type { Context } from '../src/contracts.ts';
import type { Capture, SourcePolicy } from '../src/index.ts';
import type { JsonArchiveConfiguration } from '../src/connectors.ts';
const ctx: Context = { tenantId:'tenant',projectId:'study',actor:{id:'research',kind:'service',roles:['researcher']},purpose:'research',requestId:'r',now:'2026-01-01T00:00:00Z' };
const policy: SourcePolicy = {tenantId:'tenant',projectId:'study',id:'catalog',allowedHosts:['archive.example'],enabled:true,expiresAt:'2026-02-01T00:00:00Z',maximumBytes:2000000,maximumRedirects:1,userAgent:'Research/1',accessBasis:'Approved fictional catalog'};
const config: JsonArchiveConfiguration = {id:'archive',catalogPolicy:policy,itemPolicy:{...policy,id:'items',allowedHosts:['records.example']},maximumEntries:10};
const manifest = {version:1,entries:[{id:'record-1',url:'https://records.example/a',mediaType:'application/pdf'}],next:'https://archive.example/page2'};
function source(value: unknown = manifest): Capture {
 const bytes=Buffer.from(JSON.stringify(value));
 return {...policy,sourcePolicyId:policy.id,requestedUrl:'https://archive.example/page1',finalUrl:'https://archive.example/page1',retrievedAt:ctx.now,status:200,contentType:'application/json',bytes,sha256:hashBytes(bytes),redirects:[],etag:null,lastModified:null};
}
test('catalog proposals preserve exact capture/entry provenance and need separate human authorization', () => {
 const s=source(), page=decodeJsonArchive(ctx,config,s);
 assert.equal(page.entries[0]!.item.url,manifest.entries[0]!.url);
 assert.equal(page.entries[0]!.provenance.catalogSha256,s.sha256);
 assert.equal(page.entries[0]!.provenance.entryIndex,0);
 assert.equal(page.nextUrl,manifest.next);
 assert.throws(()=>createCollection(ctx,{id:'collection',sourcePolicy:config.itemPolicy,items:page.entries.map(e=>e.item),refreshAfterMs:60000}),{code:'owner_required'});
 const owner={...ctx,actor:{id:'owner',kind:'human' as const,roles:['owner']}};
 assert.equal(createCollection(owner,{id:'collection',sourcePolicy:config.itemPolicy,items:page.entries.map(e=>e.item),refreshAfterMs:60000}).items.length,1);
 s.bytes.fill(0); assert.equal(hashBytes(page.capture.bytes),page.capture.sha256);
 assert.equal(page.entries[0]!.item.id,decodeJsonArchive(ctx,config,source()).entries[0]!.item.id);
});
test('adapter uses pinned capture and never follows advertised records or continuation',async()=>{
 let calls=0;
 const adapter=new JsonArchiveConnector(config);
 const page=await adapter.read(ctx,'https://archive.example/page1',{resolve:async()=>['8.8.8.8'],transport:async(url,options)=>{
  calls++;assert.equal(url.hostname,'archive.example');assert.equal(options.maximumBytes,1024*1024);
  return {status:200,headers:{'content-type':'application/json'},bytes:source().bytes};
 }});
 assert.equal(calls,1);assert.equal(page.entries.length,1);
});
test('schema and advertised URLs cannot widen policy, smuggle commands or repeat entries',()=>{
 for(const value of [
  {...manifest,version:2},{...manifest,grant:{enabled:true}},
  {...manifest,entries:[{...manifest.entries[0],send:true}]},
  {...manifest,entries:[{...manifest.entries[0],url:'http://records.example/a'}]},
  {...manifest,entries:[{...manifest.entries[0],url:'https://private.example/a'}]},
  {...manifest,entries:[{...manifest.entries[0],url:'https://records.example/a#secret'}]},
  {...manifest,entries:[{...manifest.entries[0],mediaType:'application/zip'}]},
  {...manifest,entries:[manifest.entries[0],manifest.entries[0]]},
  {...manifest,entries:[manifest.entries[0],{id:'another',url:'https://records.example:443/a',mediaType:null}]},
  {...manifest,next:'https://records.example/next'},
  {...manifest,next:'https://archive.example/page1'},
 ]) assert.throws(()=>decodeJsonArchive(ctx,config,source(value)));
});
test('scope, disabled policies, tampered bytes, size and malformed UTF-8 are rejected',()=>{
 for(const c of [{...config,itemPolicy:{...config.itemPolicy,tenantId:'other'}},{...config,itemPolicy:{...config.itemPolicy,enabled:false}},{...config,maximumEntries:0}]) assert.throws(()=>decodeJsonArchive(ctx,c,source()));
 for(const patch of [{tenantId:'other'},{sha256:'0'.repeat(64)},{contentType:'text/plain'},{sourcePolicyId:'other'},{retrievedAt:'2027-01-01T00:00:00Z'},{bytes:new Uint8Array(1024*1024+1)}]) assert.throws(()=>decodeJsonArchive(ctx,config,{...source(),...patch}));
 const invalid=Buffer.from([0xff]);assert.throws(()=>decodeJsonArchive(ctx,config,{...source(),bytes:invalid,sha256:hashBytes(invalid)}),{code:'invalid_catalog'});
 assert.throws(()=>decodeJsonArchive(ctx,{...config,maximumEntries:1},source({...manifest,entries:[manifest.entries[0],{id:'b',url:'https://records.example/b',mediaType:null}]})),{code:'invalid_catalog'});
});
test('configuration/context are captured before asynchronous dependencies and failures remain redacted',async()=>{
 const input=structuredClone(config), context=structuredClone(ctx), adapter=new JsonArchiveConnector(input);
 input.itemPolicy.allowedHosts=['evil.example'];
 const result=await adapter.read(context,'https://archive.example/page1',{resolve:async()=>{
  (context as {tenantId:string}).tenantId='changed'; return ['8.8.8.8'];
 },transport:async()=>({status:200,headers:{'content-type':'application/json'},bytes:source().bytes})});
 assert.equal(result.tenantId,'tenant');assert.equal(result.entries[0]!.item.url,manifest.entries[0]!.url);
 let calls=0;
 await assert.rejects(adapter.read(ctx,'https://unapproved.example/list',{resolve:async()=>{calls++;return []}}));assert.equal(calls,0);
 await assert.rejects(adapter.read(ctx,'https://archive.example/list',{resolve:async()=>{throw Error('credential-secret')}}),{code:'capture_unavailable',message:'Capture transport or resolution could not complete'});
 const abort=new AbortController();abort.abort();
 await assert.rejects(adapter.read(ctx,'https://archive.example/list',{signal:abort.signal,resolve:async()=>{calls++;return []}}),{code:'capture_aborted'});assert.equal(calls,0);
});
