import{request}from'node:https';import{resolve4}from'node:dns/promises';import{isIP}from'node:net';
import{hashBytes,invariant,requireScope,validTime,nonempty}from'./contracts.ts';import type{Context,Scope}from'./contracts.ts';
export interface SourcePolicy extends Scope{id:string;allowedHosts:readonly string[];enabled:boolean;expiresAt:string;maximumBytes:number;maximumRedirects:number;userAgent:string;accessBasis:string;}
export interface Capture extends Scope{sourcePolicyId:string;requestedUrl:string;finalUrl:string;retrievedAt:string;status:200;contentType:string;sha256:string;bytes:Uint8Array;redirects:readonly string[];etag:string|null;lastModified:string|null;}
export function allowedUrl(raw:string,policy:SourcePolicy):URL{
 let u:URL;try{u=new URL(raw);}catch{throw new Error('Invalid source URL');}
 invariant(u.protocol==='https:'&&!u.username&&!u.password&&!u.hash&&(!u.port||u.port==='443'),'unsafe_url','Only credential-free HTTPS sources are supported');
 invariant(policy.allowedHosts.includes(u.hostname.toLowerCase())&&!isIP(u.hostname),'host_denied','Source hostname is not approved');return u;
}
export function isPublicIpv4(ip:string):boolean{if(isIP(ip)!==4)return false;const p=ip.split('.').map(Number);const a=p[0]!,b=p[1]!,c=p[2]!;return !(a===0||a===10||a===127||a>=224||(a===100&&b>=64&&b<=127)||(a===169&&b===254)||(a===172&&b>=16&&b<=31)||(a===192&&(b===168||b===0||b===2||b===88&&c===99))||(a===198&&(b===18||b===19||b===51&&c===100))||(a===203&&b===0&&c===113));}
export interface NetworkReply{status:number;headers:Record<string,string|undefined>;bytes:Uint8Array;}
export type CaptureTransport=(url:URL,options:{address:string;maximumBytes:number;userAgent:string;signal:AbortSignal})=>Promise<NetworkReply>;
export const pinnedHttpsTransport:CaptureTransport=(url,options)=>new Promise((resolve,reject)=>{
 const req=request(url,{method:'GET',family:4,lookup:(_host,_options,callback)=>callback(null,options.address,4),signal:options.signal,timeout:10000,headers:{'User-Agent':options.userAgent,'Accept-Encoding':'identity','Accept':'text/plain,text/html,application/json,application/pdf'}},res=>{
  const encoding=res.headers['content-encoding'];if(encoding&&encoding!=='identity'){res.destroy();reject(new Error('unsupported_encoding'));return;}
  const chunks:Buffer[]=[];let size=0;res.on('data',(chunk:Buffer)=>{size+=chunk.length;if(size>options.maximumBytes){res.destroy(new Error('capture_too_large'));return;}chunks.push(chunk);});
  res.on('error',reject);res.on('end',()=>resolve({status:res.statusCode??0,headers:Object.fromEntries(['content-type','location','etag','last-modified'].map(k=>[k,typeof res.headers[k]==='string'?res.headers[k]:undefined])),bytes:Buffer.concat(chunks)}));
 });req.on('timeout',()=>req.destroy(new Error('capture_timeout')));req.on('error',reject);req.end();
});
export async function capture(ctx:Context,raw:string,policy:SourcePolicy,deps:{resolve?:(hostname:string)=>Promise<string[]>;transport?:CaptureTransport}={}):Promise<Capture>{
 requireScope(ctx,policy);invariant(policy.enabled&&validTime(ctx.now)<validTime(policy.expiresAt),'connector_disabled','Source policy unavailable or expired');
 invariant(Number.isInteger(policy.maximumBytes)&&policy.maximumBytes>0&&policy.maximumBytes<=25*1024*1024&&Number.isInteger(policy.maximumRedirects)&&policy.maximumRedirects>=0&&policy.maximumRedirects<=5,'invalid_policy','Capture limits are invalid');nonempty(policy.accessBasis,'access basis');nonempty(policy.userAgent,'user agent',200);invariant(!/[\r\n]/.test(policy.userAgent),'invalid_policy','Invalid user agent');
 let current=allowedUrl(raw,policy);const redirects:string[]=[];const seen=new Set<string>();const signal=AbortSignal.timeout(30000);
 for(;;){invariant(!seen.has(current.href),'redirect_loop','Source redirect loop');seen.add(current.href);const addresses=await(deps.resolve??resolve4)(current.hostname);invariant(addresses.length>0&&addresses.every(isPublicIpv4),'unsafe_address','Source DNS must resolve only to public IPv4 addresses');
 const response=await(deps.transport??pinnedHttpsTransport)(current,{address:addresses[0]!,maximumBytes:policy.maximumBytes,userAgent:policy.userAgent,signal});invariant(response.bytes.length<=policy.maximumBytes,'capture_too_large','Capture exceeds byte budget');
 if([301,302,303,307,308].includes(response.status)){invariant(redirects.length<policy.maximumRedirects&&typeof response.headers.location==='string','redirect_denied','Redirect budget or location missing');current=allowedUrl(new URL(response.headers.location,current).href,policy);redirects.push(current.href);continue;}
 invariant(response.status===200,'source_unavailable','Source did not return a complete successful response');const contentType=(response.headers['content-type']??'application/octet-stream').split(';')[0]!.trim().toLowerCase();
 invariant(['text/plain','text/html','application/json','application/pdf'].includes(contentType),'unsupported_media','Source content type is unsupported');
 return {tenantId:ctx.tenantId,projectId:ctx.projectId,sourcePolicyId:policy.id,requestedUrl:raw,finalUrl:current.href,retrievedAt:ctx.now,status:200,contentType,sha256:hashBytes(response.bytes),bytes:response.bytes,redirects,etag:response.headers.etag??null,lastModified:response.headers['last-modified']??null};
 }
}
