import { allowedUrl } from './index.ts';
import type { Capture, SourcePolicy } from './index.ts';
import { hashBytes, invariant, requireScope, validTime } from './contracts.ts';
import type { Context, Scope } from './contracts.ts';

export type ArchiveFormatLimits = {
  maximumEntries: number;
  maximumEntryBytes: number;
  maximumExpandedBytes: number;
  maximumHeaderBytes: number;
};
export type WarcRecord = {
  format: 'warc';
  id: string;
  kind: string;
  contentType: string;
  payloadOffset: number;
  payloadLength: number;
  payloadSha256: string;
};
export type ZipMember = {
  format: 'zip';
  id: string;
  method: 0 | 8;
  crc32: number;
  dataOffset: number;
  compressedLength: number;
  uncompressedLength: number;
};
export type ArchiveInventory = Scope & {
  schemaVersion: 1;
  format: 'warc' | 'zip';
  requestedUrl: string;
  finalUrl: string;
  retrievedAt: string;
  archiveSha256: string;
  byteCount: number;
  entries: readonly (WarcRecord | ZipMember)[];
};

const warcTypes = new Set(['warcinfo','response','resource','request','metadata','revisit','conversion','continuation']);
const zipTypes = new Set(['application/zip','application/x-zip-compressed']);
function limits(input: ArchiveFormatLimits): ArchiveFormatLimits {
  const value = structuredClone(input);
  invariant(Number.isSafeInteger(value.maximumEntries) && value.maximumEntries >= 1 && value.maximumEntries <= 10000,
    'invalid_archive_limits', 'Archive entry limit must be 1-10000');
  invariant(Number.isSafeInteger(value.maximumEntryBytes) && value.maximumEntryBytes >= 1 && value.maximumEntryBytes <= 100*1024*1024,
    'invalid_archive_limits', 'Archive entry byte limit is invalid');
  invariant(Number.isSafeInteger(value.maximumExpandedBytes) && value.maximumExpandedBytes >= value.maximumEntryBytes && value.maximumExpandedBytes <= 500*1024*1024,
    'invalid_archive_limits', 'Archive expansion limit is invalid');
  invariant(Number.isSafeInteger(value.maximumHeaderBytes) && value.maximumHeaderBytes >= 256 && value.maximumHeaderBytes <= 64*1024,
    'invalid_archive_limits', 'Archive header limit is invalid');
  return value;
}
function admit(context: Context, sourcePolicy: SourcePolicy, input: Capture, mime: ReadonlySet<string>, maximumBytes: number): Capture {
  const capture = structuredClone(input), policy = structuredClone(sourcePolicy);
  requireScope(context, policy); requireScope(context, capture);
  invariant(policy.enabled && validTime(context.now) < validTime(policy.expiresAt), 'connector_disabled', 'Archive policy is disabled or expired');
  invariant(capture.sourcePolicyId === policy.id && capture.status === 200 && mime.has(capture.contentType), 'archive_capture_mismatch', 'Expected a complete archive capture');
  invariant(capture.bytes.length <= Math.min(policy.maximumBytes, maximumBytes) && hashBytes(capture.bytes) === capture.sha256,
    'archive_capture_mismatch', 'Archive capture exceeds budget or differs from its digest');
  invariant(validTime(capture.retrievedAt) <= validTime(context.now) && validTime(capture.retrievedAt) < validTime(policy.expiresAt),
    'archive_capture_mismatch', 'Archive capture time is outside policy');
  for (const raw of [capture.requestedUrl, capture.finalUrl, ...capture.redirects]) {
    try { allowedUrl(raw, policy); } catch { invariant(false, 'archive_capture_mismatch', 'Archive URL is outside approved policy'); }
  }
  invariant(capture.redirects.length <= policy.maximumRedirects, 'archive_capture_mismatch', 'Archive redirect budget exceeded');
  return capture;
}
function inventory(context: Context, capture: Capture, format: 'warc'|'zip', entries: readonly (WarcRecord|ZipMember)[]): ArchiveInventory {
  return {tenantId:context.tenantId,projectId:context.projectId,schemaVersion:1,format,requestedUrl:capture.requestedUrl,
    finalUrl:capture.finalUrl,retrievedAt:capture.retrievedAt,archiveSha256:capture.sha256,byteCount:capture.bytes.length,entries};
}
function find(bytes: Uint8Array, needle: readonly number[], start: number, limit: number): number {
  const end = Math.min(bytes.length - needle.length, start + limit);
  outer: for (let i=start;i<=end;i++) { for(let j=0;j<needle.length;j++) if(bytes[i+j]!==needle[j]) continue outer; return i; }
  return -1;
}
function ascii(bytes: Uint8Array): string {
  invariant(bytes.every(value => value === 13 || value === 10 || (value >= 0x20 && value <= 0x7e)), 'invalid_warc', 'WARC headers must be printable ASCII');
  return new TextDecoder().decode(bytes);
}

/** Parse bounded WARC record envelopes from an already approved exact capture. Payloads remain unextracted bytes. */
export function parseWarc(context: Context, sourcePolicy: SourcePolicy, input: Capture, configured: ArchiveFormatLimits): ArchiveInventory {
  const bound=limits(configured), capture=admit(context,sourcePolicy,input,new Set(['application/warc']),25*1024*1024), bytes=capture.bytes;
  const records: WarcRecord[]=[]; const ids=new Set<string>(); let cursor=0, expanded=0;
  while(cursor<bytes.length) {
    invariant(records.length < bound.maximumEntries, 'archive_limit', 'WARC record limit exceeded');
    const headerEnd=find(bytes,[13,10,13,10],cursor,bound.maximumHeaderBytes);
    invariant(headerEnd>=0, 'invalid_warc', 'WARC header delimiter missing or too large');
    const lines=ascii(bytes.subarray(cursor,headerEnd)).split('\r\n'), version=lines.shift();
    invariant(version==='WARC/1.0' || version==='WARC/1.1', 'invalid_warc', 'Unsupported WARC version');
    const headers=new Map<string,string>();
    for(const line of lines){const split=line.indexOf(':'); invariant(split>0,'invalid_warc','Malformed WARC header'); const name=line.slice(0,split).toLowerCase(), value=line.slice(split+1).trim(); invariant(/^[a-z0-9-]+$/.test(name)&&value.length>0&&!headers.has(name),'invalid_warc','Invalid or duplicate WARC header'); headers.set(name,value);}
    const id=headers.get('warc-record-id'), kind=headers.get('warc-type'), contentType=headers.get('content-type'), lengthText=headers.get('content-length');
    invariant(id!==undefined && /^<[^<>\r\n]{1,2048}>$/.test(id) && !ids.has(id),'invalid_warc','WARC record identity is invalid or duplicated');
    invariant(kind!==undefined && warcTypes.has(kind),'invalid_warc','Unsupported WARC record type');
    invariant(contentType!==undefined && /^[^\s/]+\/[^\s;]+(?:\s*;[^\r\n]*)?$/.test(contentType),'invalid_warc','Invalid WARC content type');
    invariant(lengthText!==undefined && /^(0|[1-9][0-9]*)$/.test(lengthText),'invalid_warc','Invalid WARC content length');
    const length=Number(lengthText); invariant(Number.isSafeInteger(length)&&length<=bound.maximumEntryBytes,'archive_limit','WARC payload limit exceeded');
    const payloadOffset=headerEnd+4, payloadEnd=payloadOffset+length;
    invariant(payloadEnd<=bytes.length,'invalid_warc','WARC payload is truncated');
    expanded+=length; invariant(expanded<=bound.maximumExpandedBytes,'archive_limit','WARC aggregate payload limit exceeded');
    records.push({format:'warc',id,kind,contentType,payloadOffset,payloadLength:length,payloadSha256:hashBytes(bytes.subarray(payloadOffset,payloadEnd))}); ids.add(id);
    cursor=payloadEnd;
    if(cursor<bytes.length){invariant(bytes[cursor]===13&&bytes[cursor+1]===10&&bytes[cursor+2]===13&&bytes[cursor+3]===10,'invalid_warc','WARC record delimiter missing'); cursor+=4;}
  }
  invariant(records.length>0,'invalid_warc','WARC must contain at least one record');
  return inventory(context,capture,'warc',records);
}

function view(bytes: Uint8Array): DataView { return new DataView(bytes.buffer,bytes.byteOffset,bytes.byteLength); }
function name(bytes: Uint8Array, utf8: boolean): string {
  if(!utf8) invariant(bytes.every(value=>value>=0x20&&value<=0x7e),'invalid_zip','Legacy ZIP names must be printable ASCII');
  let value:string; try{value=new TextDecoder('utf-8',{fatal:true}).decode(bytes);}catch{invariant(false,'invalid_zip','ZIP member name is not valid UTF-8');}
  invariant(value.length>0&&value.length<=4096&&!value.includes('\\')&&!value.includes('\0')&&!value.startsWith('/')&&!/^[A-Za-z]:/.test(value)&&!value.endsWith('/'), 'unsafe_archive_path','ZIP member path is unsafe');
  invariant(value.split('/').every(part=>part!==''&&part!=='.'&&part!=='..'),'unsafe_archive_path','ZIP member path is unsafe'); return value;
}
/** Read the bounded ZIP central directory without extracting or decompressing members. */
export function parseZip(context: Context, sourcePolicy: SourcePolicy, input: Capture, configured: ArchiveFormatLimits): ArchiveInventory {
  const bound=limits(configured), capture=admit(context,sourcePolicy,input,zipTypes,25*1024*1024), bytes=capture.bytes, data=view(bytes);
  let eocd=-1; for(let i=Math.max(0,bytes.length-65557);i<=bytes.length-22;i++) if(data.getUint32(i,true)===0x06054b50) eocd=i;
  invariant(eocd>=0&&eocd+22+data.getUint16(eocd+20,true)===bytes.length,'invalid_zip','ZIP end record is missing or malformed');
  const disk=data.getUint16(eocd+4,true), directoryDisk=data.getUint16(eocd+6,true), diskEntries=data.getUint16(eocd+8,true), entryCount=data.getUint16(eocd+10,true);
  const directorySize=data.getUint32(eocd+12,true), directoryOffset=data.getUint32(eocd+16,true);
  invariant(disk===0&&directoryDisk===0&&diskEntries===entryCount&&entryCount!==0xffff&&directorySize!==0xffffffff&&directoryOffset!==0xffffffff,'unsupported_zip','Multi-disk and ZIP64 archives are unsupported');
  invariant(entryCount>=1&&entryCount<=bound.maximumEntries&&directoryOffset+directorySize===eocd,'archive_limit','ZIP directory bounds or entry limit invalid');
  const entries:ZipMember[]=[]; const names=new Set<string>(); let cursor=directoryOffset, expanded=0;
  for(let index=0;index<entryCount;index++){
    invariant(cursor+46<=eocd&&data.getUint32(cursor,true)===0x02014b50,'invalid_zip','ZIP central directory is malformed');
    const flags=data.getUint16(cursor+8,true), method=data.getUint16(cursor+10,true), crc32=data.getUint32(cursor+16,true), compressed=data.getUint32(cursor+20,true), uncompressed=data.getUint32(cursor+24,true);
    const nameLength=data.getUint16(cursor+28,true), extraLength=data.getUint16(cursor+30,true), commentLength=data.getUint16(cursor+32,true), localOffset=data.getUint32(cursor+42,true), next=cursor+46+nameLength+extraLength+commentLength;
    invariant(next<=eocd&&(flags&1)===0&&(method===0||method===8)&&compressed!==0xffffffff&&uncompressed!==0xffffffff&&localOffset!==0xffffffff,'unsupported_zip','Encrypted, ZIP64 or unsupported-compression member');
    const memberName=name(bytes.subarray(cursor+46,cursor+46+nameLength),(flags&0x800)!==0); invariant(!names.has(memberName),'invalid_zip','Duplicate ZIP member name');
    invariant(uncompressed<=bound.maximumEntryBytes,'archive_limit','ZIP member expansion limit exceeded'); expanded+=uncompressed; invariant(expanded<=bound.maximumExpandedBytes,'archive_limit','ZIP aggregate expansion limit exceeded');
    invariant(uncompressed===0 || compressed>0, 'invalid_zip','Nonempty ZIP member has no compressed bytes');
    invariant(uncompressed===0 || uncompressed<=compressed*1000, 'archive_limit','ZIP compression ratio limit exceeded');
    invariant(localOffset+30<=directoryOffset&&data.getUint32(localOffset,true)===0x04034b50,'invalid_zip','ZIP local header is missing');
    const localFlags=data.getUint16(localOffset+6,true), localMethod=data.getUint16(localOffset+8,true), localNameLength=data.getUint16(localOffset+26,true), localExtraLength=data.getUint16(localOffset+28,true);
    const dataOffset=localOffset+30+localNameLength+localExtraLength;
    invariant(localFlags===flags&&localMethod===method&&dataOffset+compressed<=directoryOffset,'invalid_zip','ZIP local and central headers disagree');
    const localName=name(bytes.subarray(localOffset+30,localOffset+30+localNameLength),(flags&0x800)!==0); invariant(localName===memberName,'invalid_zip','ZIP local member name differs');
    entries.push({format:'zip',id:memberName,method:method as 0|8,crc32,dataOffset,compressedLength:compressed,uncompressedLength:uncompressed}); names.add(memberName); cursor=next;
  }
  invariant(cursor===eocd,'invalid_zip','ZIP central directory has undeclared data');
  return inventory(context,capture,'zip',entries);
}
