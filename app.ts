// A standalone GitHub-compatible git forge: git smart HTTP (protocol v2 + v0),
// REST v3, GraphQL v4 and MCP over streamable HTTP. One process, in-memory store,
// deterministic state (no wall clock, no randomness in anything served).
// Original implementation written for the github-core-clone-01 contest.
//
// v3c ("snapshot forge"): v3b's raw-TCP transport, plus a different data architecture —
// every read response is precomputed at write time and served as a stored buffer (repo,
// branch and ref snapshots, memoized issue/repo JSON, static one-time buffers), the
// dispatcher runs bodyless reads fully synchronously, and the depth-limited fetch no
// longer walks the whole object graph to build a closure it throws away. No handler,
// status code, header or response byte changed from v2/v3b.

import { createHash } from 'node:crypto';
import { createServer, type Server, type Socket } from 'node:net';
import { deflateSync, inflateSync } from 'node:zlib';

// -------------------------------------------------------------------------------------------
// Constants
// -------------------------------------------------------------------------------------------

export const VIEWER_LOGIN = 'adaiversen';
const VIEWER_ID = 1000;
const VIEWER_NAME = 'ada iversen';
const VIEWER_EMAIL = 'adaiversen@example.invalid';
const FIXED_TS = '2025-01-01T00:00:00Z';
const COMMIT_EPOCH = 1735689600;
const COMMIT_IDENT = `${VIEWER_NAME} <${VIEWER_EMAIL}>`;
const ZERO_OID = '0'.repeat(40);
const JSON_TYPE = 'application/json; charset=utf-8';
const TOKEN = process.env.MAKEFUN_SHOP_TOKEN ?? '';

const sha1 = (data: Buffer): string => createHash('sha1').update(data).digest('hex');

function oidOf(type: string, data: Buffer): string {
  const h = createHash('sha1');
  h.update(Buffer.from(`${type} ${data.length}\0`, 'utf8'));
  h.update(data);
  return h.digest('hex');
}

const b64e = (b: Buffer | string): string => Buffer.from(b as never).toString('base64');
const b64d = (s: string): Buffer => Buffer.from(s.replace(/[\r\n\t ]+/g, ''), 'base64');

// -------------------------------------------------------------------------------------------
// Store
// -------------------------------------------------------------------------------------------

type ObjType = 'commit' | 'tree' | 'blob' | 'tag';
interface GitObj { type: ObjType; data: Buffer }
interface Label { name: string; color: string }
interface Issue {
  number: number; id: number; nodeId: string; title: string; body: string;
  state: 'open' | 'closed'; labels: string[]; createdAt: string; updatedAt: string;
  /** Memoized serialized REST form (see issueJsonBuf); cleared whenever the issue changes. */
  jsonBuf: Buffer | null;
}
interface FileChange { filename: string; status: string; additions: number; deletions: number; changes: number }
interface PR {
  number: number; id: number; nodeId: string; title: string; body: string;
  state: 'open' | 'closed'; merged: boolean; mergedAt: string | null; mergeCommitSha: string | null;
  headRef: string; headSha: string; baseRef: string; baseSha: string; mergeBaseSha: string;
  files: FileChange[]; createdAt: string;
}
interface Comment { id: number; nodeId: string; body: string; createdAt: string }
interface Repo {
  id: number; nodeId: string; name: string; owner: string; description: string; isPrivate: boolean;
  defaultBranch: string;
  objects: Map<string, GitObj>;
  refs: Map<string, string>;
  labels: Map<string, Label>;
  issues: Issue[];
  pulls: PR[];
  comments: Map<number, Comment[]>;
  nextNumber: number;
  /** Bumped on every git-object/ref mutation; the git response cache is cleared with it. */
  rev: number;
  /** Memoized git smart-HTTP responses (advertisements, ls-refs, fetch). Cleared on any mutation. */
  gitCache: Map<string, Buffer>;
  /** v3c snapshots: read responses precomputed at write time, cleared on any change to them.
   *  repoJsonBuf covers GET /repos (open_issues_count moves with issues); branchesBuf/refsBuf
   *  cover the branch and ref listings (they move with refs). All are rebuilt lazily on next read. */
  repoJsonBuf: Buffer | null;
  branchesBuf: Buffer | null;
  refsBuf: Buffer | null;
}

const repos = new Map<string, Repo>();
let nextRepoId = 1;
let nextObjectId = 1;
let nextCommentSeq = 1;
let requestSeq = 0;

function repoKey(owner: string, name: string): string { return `${owner}/${name}`; }
function globalId(kind: string, ...parts: (string | number)[]): string {
  return b64e(`${kind}:${parts.join(':')}`);
}
function repoNodeId(r: Repo): string { return globalId('repo', r.owner, r.name); }

function newRepo(name: string, isPrivate: boolean, description: string): Repo {
  const r: Repo = {
    id: nextRepoId++, nodeId: '', name, owner: VIEWER_LOGIN, description, isPrivate,
    defaultBranch: 'main',
    objects: new Map(), refs: new Map(), labels: new Map(),
    issues: [], pulls: [], comments: new Map(), nextNumber: 1,
    rev: 0, gitCache: new Map(),
    repoJsonBuf: null, branchesBuf: null, refsBuf: null,
  };
  r.nodeId = repoNodeId(r);
  return r;
}

/** A repo's git objects/refs changed: drop every memoized git response. Cheap (map clear) and
 *  exact — handlers run synchronously, so no fetch can observe a half-applied mutation. */
function touchGit(r: Repo): void {
  r.rev += 1;
  r.gitCache.clear();
  r.branchesBuf = null;
  r.refsBuf = null;
}

/** A repo's REST-visible shape changed (description, visibility, open-issue count): the memoized
 *  repo JSON is stale. Called from repo PATCH, issue create, and issue state changes. */
function touchRepoJson(r: Repo): void {
  r.repoJsonBuf = null;
}

/** The serialized REST form of a repo, memoized like issueJsonBuf. Byte-identical to
 *  sendJson(200, repoJson(r)): the same JSON.stringify plus the trailing newline. */
function repoJsonBuf(r: Repo): Buffer {
  if (r.repoJsonBuf) return r.repoJsonBuf;
  const buf = Buffer.from(`${JSON.stringify(repoJson(r))}\n`, 'utf8');
  r.repoJsonBuf = buf;
  return buf;
}

/** The branches-list JSON, memoized; cleared by touchGit on any ref change. */
function branchesBuf(r: Repo): Buffer {
  if (r.branchesBuf) return r.branchesBuf;
  const list = [...r.refs.keys()]
    .filter((n) => n.startsWith('refs/heads/'))
    .sort()
    .map((n) => ({ name: n.slice(11), commit: { sha: r.refs.get(n)! }, protected: false }));
  const buf = Buffer.from(`${JSON.stringify(list)}\n`, 'utf8');
  r.branchesBuf = buf;
  return buf;
}

/** The git-refs-list JSON, memoized; cleared by touchGit on any ref change. */
function refsBuf(r: Repo): Buffer {
  if (r.refsBuf) return r.refsBuf;
  const list = sortedRefNames(r).map((n) => ({ ref: n, object: { sha: r.refs.get(n)!, type: r.objects.get(r.refs.get(n)!)?.type ?? 'commit' } }));
  const buf = Buffer.from(`${JSON.stringify(list)}\n`, 'utf8');
  r.refsBuf = buf;
  return buf;
}

function gitCacheGet(r: Repo, key: string): Buffer | undefined {
  return r.gitCache.get(key);
}

/** Memoize a git response. Oversized packs (a full clone of a huge repo) are not cached, and the
 *  per-repo cache is bounded so a long conformance run cannot grow it without limit. */
function gitCacheSet(r: Repo, key: string, body: Buffer): void {
  if (body.length > 2_000_000) return;
  if (r.gitCache.size >= 64) r.gitCache.clear();
  r.gitCache.set(key, body);
}

// -------------------------------------------------------------------------------------------
// pkt-line codec
// -------------------------------------------------------------------------------------------

function pktLine(payload: string | Buffer): Buffer {
  const data = typeof payload === 'string' ? Buffer.from(payload, 'utf8') : payload;
  const head = Buffer.alloc(4);
  head.write((data.length + 4).toString(16).padStart(4, '0'), 0, 4, 'ascii');
  return Buffer.concat([head, data]);
}
const pktFlush = (): Buffer => Buffer.from('0000', 'ascii');
const pktDelim = (): Buffer => Buffer.from('0001', 'ascii');

function pktLines(items: (string | Buffer | 'flush' | 'delim')[]): Buffer {
  const parts: Buffer[] = [];
  for (const it of items) {
    if (it === 'flush') parts.push(pktFlush());
    else if (it === 'delim') parts.push(pktDelim());
    else parts.push(pktLine(it));
  }
  return Buffer.concat(parts);
}

interface DecodedPacket { kind: 'data' | 'flush' | 'delim' | 'end'; payload: Buffer }
function decodePktLines(buf: Buffer): DecodedPacket[] {
  const out: DecodedPacket[] = [];
  let off = 0;
  while (off + 4 <= buf.length) {
    const head = buf.subarray(off, off + 4).toString('ascii');
    if (!/^[0-9a-fA-F]{4}$/.test(head)) throw new Error('bad pkt-line header');
    const len = parseInt(head, 16);
    if (len === 0) { out.push({ kind: 'flush', payload: Buffer.alloc(0) }); off += 4; continue; }
    if (len === 1) { out.push({ kind: 'delim', payload: Buffer.alloc(0) }); off += 4; continue; }
    if (len === 2) { out.push({ kind: 'end', payload: Buffer.alloc(0) }); off += 4; continue; }
    if (len < 4 || off + len > buf.length) throw new Error('truncated pkt-line');
    out.push({ kind: 'data', payload: buf.subarray(off + 4, off + len) });
    off += len;
  }
  return out;
}

// -------------------------------------------------------------------------------------------
// Git object model
// -------------------------------------------------------------------------------------------

function parseTree(data: Buffer): { mode: string; name: string; oid: string }[] {
  const entries: { mode: string; name: string; oid: string }[] = [];
  let off = 0;
  while (off < data.length) {
    const sp = data.indexOf(0x20, off);
    const nul = data.indexOf(0x00, sp + 1);
    entries.push({
      mode: data.subarray(off, sp).toString('ascii'),
      name: data.subarray(sp + 1, nul).toString('utf8'),
      oid: data.subarray(nul + 1, nul + 21).toString('hex'),
    });
    off = nul + 21;
  }
  return entries;
}

function buildTree(entries: { mode: string; name: string; oid: string }[]): Buffer {
  const sorted = [...entries].sort((a, b) => {
    const an = Buffer.from(a.mode === '40000' ? `${a.name}/` : a.name, 'utf8');
    const bn = Buffer.from(b.mode === '40000' ? `${b.name}/` : b.name, 'utf8');
    return Buffer.compare(an, bn);
  });
  const parts: Buffer[] = [];
  for (const e of sorted) {
    parts.push(Buffer.from(`${e.mode} ${e.name}\0`, 'utf8'));
    parts.push(Buffer.from(e.oid, 'hex'));
  }
  return Buffer.concat(parts);
}

interface CommitView { tree: string; parents: string[]; author: string; committer: string; message: string }
function parseCommit(data: Buffer): CommitView {
  const blank = data.indexOf('\n\n');
  const headText = data.subarray(0, blank < 0 ? data.length : blank).toString('utf8');
  const message = blank < 0 ? '' : data.subarray(blank + 2).toString('utf8');
  let tree = '';
  const parents: string[] = [];
  let author = '';
  let committer = '';
  for (const line of headText.split('\n')) {
    const sp = line.indexOf(' ');
    const k = sp < 0 ? line : line.slice(0, sp);
    const v = sp < 0 ? '' : line.slice(sp + 1);
    if (k === 'tree') tree = v;
    else if (k === 'parent') parents.push(v);
    else if (k === 'author') author = v;
    else if (k === 'committer') committer = v;
  }
  return { tree, parents, author, committer, message };
}

function buildCommit(tree: string, parents: string[], message: string): Buffer {
  const lines = [`tree ${tree}`];
  for (const p of parents) lines.push(`parent ${p}`);
  lines.push(`author ${COMMIT_IDENT} ${COMMIT_EPOCH} +0000`);
  lines.push(`committer ${COMMIT_IDENT} ${COMMIT_EPOCH} +0000`);
  lines.push('');
  return Buffer.from(`${lines.join('\n')}\n${message.endsWith('\n') ? message : `${message}\n`}`, 'utf8');
}

function putObj(r: Repo, type: ObjType, data: Buffer): string {
  const oid = oidOf(type, data);
  if (!r.objects.has(oid)) {
    r.objects.set(oid, { type, data });
    touchGit(r);
  }
  return oid;
}

/** Files of a commit as path -> {oid, mode}. */
function filesAt(r: Repo, commitOid: string): Map<string, { oid: string; mode: string }> {
  const out = new Map<string, { oid: string; mode: string }>();
  const commit = r.objects.get(commitOid);
  if (!commit || commit.type !== 'commit') return out;
  const rootTree = parseCommit(commit.data).tree;
  const walk = (treeOid: string, prefix: string): void => {
    const tree = r.objects.get(treeOid);
    if (!tree) return;
    for (const e of parseTree(tree.data)) {
      const path = prefix === '' ? e.name : `${prefix}/${e.name}`;
      if (e.mode === '40000') walk(e.oid, path);
      else out.set(path, { oid: e.oid, mode: e.mode });
    }
  };
  walk(rootTree, '');
  return out;
}

/** Build nested trees from path -> blob oid; returns root tree oid. */
function buildTreeFromFiles(r: Repo, files: Map<string, { oid: string; mode: string }>): string {
  interface Node { files: Map<string, { oid: string; mode: string }>; dirs: Map<string, Node> }
  const make = (): Node => ({ files: new Map(), dirs: new Map() });
  const root = make();
  for (const [path, entry] of files) {
    const segs = path.split('/');
    let node = root;
    for (let i = 0; i < segs.length - 1; i++) {
      const s = segs[i];
      if (!node.dirs.has(s)) node.dirs.set(s, make());
      node = node.dirs.get(s)!;
    }
    node.files.set(segs[segs.length - 1], entry);
  }
  const build = (node: Node): string => {
    const entries: { mode: string; name: string; oid: string }[] = [];
    for (const [name, e] of node.files) entries.push({ mode: e.mode, name, oid: e.oid });
    for (const [name, child] of node.dirs) entries.push({ mode: '40000', name, oid: build(child) });
    return putObj(r, 'tree', buildTree(entries));
  };
  return build(root);
}

/** All objects reachable from roots (commits, trees, blobs, tags). */
function reachable(r: Repo, roots: string[]): Set<string> {
  const seen = new Set<string>();
  const queue = [...roots];
  while (queue.length) {
    const oid = queue.pop()!;
    if (seen.has(oid) || !/^[0-9a-f]{40}$/.test(oid)) continue;
    const obj = r.objects.get(oid);
    if (!obj) continue;
    seen.add(oid);
    if (obj.type === 'commit') {
      const c = parseCommit(obj.data);
      queue.push(c.tree);
      for (const p of c.parents) queue.push(p);
    } else if (obj.type === 'tag') {
      const m = obj.data.toString('utf8').match(/^object ([0-9a-f]{40})/m);
      if (m) queue.push(m[1]);
    } else if (obj.type === 'tree') {
      for (const e of parseTree(obj.data)) if (e.mode !== '160000') queue.push(e.oid);
    }
  }
  return seen;
}

function isAncestor(r: Repo, anc: string, desc: string): boolean {
  if (anc === desc) return true;
  const seen = new Set<string>();
  const queue = [desc];
  while (queue.length) {
    const oid = queue.pop()!;
    if (oid === anc) return true;
    if (seen.has(oid)) continue;
    seen.add(oid);
    const obj = r.objects.get(oid);
    if (!obj || obj.type !== 'commit') continue;
    for (const p of parseCommit(obj.data).parents) queue.push(p);
  }
  return false;
}

/** Commits reachable from tip, newest-first (DFS pre-order). */
function commitList(r: Repo, tip: string): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const stack = [tip];
  while (stack.length) {
    const oid = stack.pop()!;
    if (seen.has(oid)) continue;
    seen.add(oid);
    const obj = r.objects.get(oid);
    if (!obj || obj.type !== 'commit') continue;
    out.push(oid);
    const parents = parseCommit(obj.data).parents;
    for (let i = parents.length - 1; i >= 0; i--) stack.push(parents[i]);
  }
  return out;
}

/** Peel an annotated tag to its commit, else the oid itself. */
function peelToCommit(r: Repo, oid: string): string {
  const obj = r.objects.get(oid);
  if (obj && obj.type === 'tag') {
    const m = obj.data.toString('utf8').match(/^object ([0-9a-f]{40})/m);
    if (m) return peelToCommit(r, m[1]);
  }
  return oid;
}

// -------------------------------------------------------------------------------------------
// Packfile read/write (plain objects + OFS/REF deltas on read)
// -------------------------------------------------------------------------------------------

function readVarintSize(buf: Buffer, start: number): { size: number; next: number; first: number } {
  let byte = buf[start];
  const first = byte;
  let size = byte & 0x0f;
  let shift = 4;
  let off = start + 1;
  while (byte & 0x80) {
    byte = buf[off++];
    size += (byte & 0x7f) * 2 ** shift;
    shift += 7;
  }
  return { size, next: off, first };
}

function applyDelta(base: Buffer, delta: Buffer): Buffer {
  let off = 0;
  const readSize = (): number => {
    let v = 0;
    let shift = 0;
    for (;;) {
      const b = delta[off++];
      v += (b & 0x7f) * 2 ** shift;
      shift += 7;
      if (!(b & 0x80)) break;
    }
    return v;
  };
  const srcSize = readSize();
  const tgtSize = readSize();
  if (srcSize !== base.length) throw new Error('delta base size mismatch');
  const out = Buffer.allocUnsafe(tgtSize);
  let written = 0;
  while (off < delta.length) {
    const op = delta[off++];
    if (op & 0x80) {
      let copyOff = 0;
      let copySize = 0;
      let shift = 0;
      for (let bit = 0; bit < 4; bit++) {
        if (op & (1 << bit)) copyOff += delta[off++] * 2 ** shift;
        shift += 8;
      }
      shift = 0;
      for (let bit = 4; bit < 7; bit++) {
        if (op & (1 << bit)) copySize += delta[off++] * 2 ** shift;
        shift += 8;
      }
      if (copySize === 0) copySize = 0x10000;
      base.copy(out, written, copyOff, copyOff + copySize);
      written += copySize;
    } else if (op !== 0) {
      const lit = op & 0x7f;
      delta.copy(out, written, off, off + lit);
      off += lit;
      written += lit;
    } else throw new Error('delta opcode 0');
  }
  if (written !== tgtSize) throw new Error('delta size mismatch');
  return out;
}

const TYPE_CODE: Record<ObjType, number> = { commit: 1, tree: 2, blob: 3, tag: 4 };
const CODE_TYPE: Record<number, ObjType> = { 1: 'commit', 2: 'tree', 3: 'blob', 4: 'tag' };

function writePackObjectHeader(type: ObjType, size: number): Buffer {
  const code = TYPE_CODE[type];
  const bytes: number[] = [];
  let rem = size;
  let first = (code << 4) | (rem & 0x0f);
  rem = Math.floor(rem / 16);
  while (rem > 0) {
    bytes.push(first | 0x80);
    first = rem & 0x7f;
    rem = Math.floor(rem / 128);
  }
  bytes.push(first);
  return Buffer.from(bytes);
}

function writePack(objects: { type: ObjType; data: Buffer }[]): Buffer {
  const parts: Buffer[] = [];
  const head = Buffer.alloc(12);
  head.write('PACK', 0, 4, 'ascii');
  head.writeUInt32BE(2, 4);
  head.writeUInt32BE(objects.length, 8);
  parts.push(head);
  for (const o of objects) {
    parts.push(writePackObjectHeader(o.type, o.data.length));
    parts.push(deflateSync(o.data));
  }
  const body = Buffer.concat(parts);
  return Buffer.concat([body, createHash('sha1').update(body).digest()]);
}

/** Parse a packfile into objects. resolveBase supplies thin-pack bases already held. */
function readPack(pack: Buffer, resolveBase?: (oid: string) => GitObj | undefined): Map<string, GitObj> {
  if (pack.length < 32 || pack.subarray(0, 4).toString('ascii') !== 'PACK') throw new Error('bad pack');
  const version = pack.readUInt32BE(4);
  if (version !== 2 && version !== 3) throw new Error('bad pack version');
  const count = pack.readUInt32BE(8);
  const trailerStart = pack.length - 20;
  interface Raw { offset: number; typeCode: number; size: number; data: Buffer; baseOffset?: number; baseOid?: string }
  const raw: Raw[] = [];
  const byOffset = new Map<number, Raw>();
  let off = 12;
  for (let i = 0; i < count; i++) {
    const entryOff = off;
    const h = readVarintSize(pack, off);
    off = h.next;
    const typeCode = (h.first >> 4) & 0x07;
    const entry: Raw = { offset: entryOff, typeCode, size: h.size, data: Buffer.alloc(0) };
    if (typeCode === 6) {
      let byte = pack[off];
      off++;
      let v = byte & 0x7f;
      while (byte & 0x80) {
        byte = pack[off++];
        v = ((v + 1) * 128) + (byte & 0x7f);
      }
      entry.baseOffset = entryOff - v;
    } else if (typeCode === 7) {
      entry.baseOid = pack.subarray(off, off + 20).toString('hex');
      off += 20;
    } else if (!CODE_TYPE[typeCode]) throw new Error('bad pack type');
    const inflated = inflateSync(pack.subarray(off, trailerStart), { info: true }) as unknown as { buffer: Buffer; engine: { bytesWritten: number } };
    if (inflated.buffer.length !== h.size) throw new Error('pack size mismatch');
    entry.data = inflated.buffer;
    off += inflated.engine.bytesWritten;
    raw.push(entry);
    byOffset.set(entryOff, entry);
  }
  const objects = new Map<string, GitObj>();
  const resolved = new Map<number, { type: ObjType; data: Buffer; oid: string }>();
  const byOid = new Map<string, Raw>();
  const resolve = (entry: Raw): { type: ObjType; data: Buffer; oid: string } => {
    const cached = resolved.get(entry.offset);
    if (cached) return cached;
    let type: ObjType;
    let data: Buffer;
    if (entry.typeCode === 6) {
      const base = byOffset.get(entry.baseOffset!);
      if (!base) throw new Error('ofs-delta base missing');
      const b = resolve(base);
      type = b.type;
      data = applyDelta(b.data, entry.data);
    } else if (entry.typeCode === 7) {
      const baseEntry = byOid.get(entry.baseOid!);
      if (baseEntry) {
        const b = resolve(baseEntry);
        type = b.type;
        data = applyDelta(b.data, entry.data);
      } else {
        const ext = resolveBase?.(entry.baseOid!);
        if (!ext) throw new Error('ref-delta base missing');
        type = ext.type;
        data = applyDelta(ext.data, entry.data);
      }
    } else {
      type = CODE_TYPE[entry.typeCode];
      data = entry.data;
    }
    const oid = oidOf(type, data);
    const res = { type, data, oid };
    resolved.set(entry.offset, res);
    byOid.set(oid, entry);
    return res;
  };
  for (const entry of raw) {
    const r = resolve(entry);
    if (!objects.has(r.oid)) objects.set(r.oid, { type: r.type, data: r.data });
  }
  return objects;
}

function sidebandPack(pack: Buffer): Buffer {
  // One pkt-line per chunk: 0x01 + up to 32768 bytes of pack data.
  const parts: Buffer[] = [];
  const CHUNK = 32768;
  for (let off = 0; off < pack.length; off += CHUNK) {
    const chunk = pack.subarray(off, Math.min(off + CHUNK, pack.length));
    parts.push(pktLine(Buffer.concat([Buffer.from([1]), chunk])));
  }
  if (pack.length === 0) parts.push(pktLine(Buffer.from([1])));
  return Buffer.concat(parts);
}

// -------------------------------------------------------------------------------------------
// Git: advertisements, ls-refs, fetch, push
// -------------------------------------------------------------------------------------------

function sortedRefNames(r: Repo): string[] {
  return [...r.refs.keys()].sort();
}

function v2Advertisement(): Buffer {
  return pktLines([
    'version 2\n',
    'agent=github-forge/1.0\n',
    'ls-refs=unborn\n',
    'fetch=shallow wait-for-done filter\n',
    'object-format=sha1\n',
    'flush',
  ]);
}

// The v2 capability advertisement never changes: one shared buffer for every info/refs.
const V2_ADVERTISEMENT: Buffer = v2Advertisement();

/** Memoized v0 upload-pack advertisement for a repo (refs + peeled tags). */
function cachedV0UploadAd(r: Repo): Buffer {
  const key = 'adv0up';
  const hit = gitCacheGet(r, key);
  if (hit) return hit;
  const body = v0UploadAdvertisement(r);
  gitCacheSet(r, key, body);
  return body;
}

/** Memoized v0 receive-pack advertisement for a repo. */
function cachedV0ReceiveAd(r: Repo): Buffer {
  const key = 'adv0recv';
  const hit = gitCacheGet(r, key);
  if (hit) return hit;
  const body = v0ReceiveAdvertisement(r);
  gitCacheSet(r, key, body);
  return body;
}

function v0UploadAdvertisement(r: Repo): Buffer {
  const caps = 'symref=HEAD:refs/heads/main agent=github-forge/1.0 side-band-64k thin-pack ofs-delta multi_ack_detailed shallow';
  const names = sortedRefNames(r);
  const lines: string[] = [];
  const head = r.refs.get('refs/heads/main') ?? r.refs.get(`refs/heads/${r.defaultBranch}`);
  if (head) lines.push(`${head} HEAD\0${caps}\n`);
  else if (names.length === 0) lines.push(`${ZERO_OID} capabilities^{}\0${caps}\n`);
  for (let i = 0; i < names.length; i++) {
    const name = names[i];
    const oid = r.refs.get(name)!;
    // The capabilities ride on the first emitted line when there is no HEAD line.
    const suffix = (!head && i === 0) ? `\0${caps}` : '';
    lines.push(`${oid} ${name}${suffix}\n`);
    const obj = r.objects.get(oid);
    if (obj && obj.type === 'tag') {
      const peeled = peelToCommit(r, oid);
      if (peeled !== oid) lines.push(`${peeled} ${name}^{}\n`);
    }
  }
  if (lines.length === 0) lines.push(`${ZERO_OID} capabilities^{}\0${caps}\n`);
  return Buffer.concat([pktLine('# service=git-upload-pack\n'), pktFlush(), ...lines.map((l) => pktLine(l)), pktFlush()]);
}

function v0ReceiveAdvertisement(r: Repo): Buffer {
  const caps = 'report-status report-status-v2 side-band-64k delete-refs ofs-delta agent=github-forge/1.0';
  const names = sortedRefNames(r);
  const lines: string[] = [];
  if (names.length === 0) lines.push(`${ZERO_OID} capabilities^{}\0${caps}\n`);
  for (let i = 0; i < names.length; i++) {
    const name = names[i];
    const oid = r.refs.get(name)!;
    lines.push(i === 0 ? `${oid} ${name}\0${caps}\n` : `${oid} ${name}\n`);
    const obj = r.objects.get(oid);
    if (obj && obj.type === 'tag') {
      const peeled = peelToCommit(r, oid);
      if (peeled !== oid) lines.push(`${peeled} ${name}^{}\n`);
    }
  }
  return Buffer.concat([pktLine('# service=git-receive-pack\n'), pktFlush(), ...lines.map((l) => pktLine(l)), pktFlush()]);
}

interface LsRef { oid: string; name: string; peeled?: string; symrefTarget?: string; unborn?: boolean }

function lsRefsList(r: Repo, prefixes: string[], peel: boolean, symrefs: boolean): LsRef[] {
  const out: LsRef[] = [];
  const keep = (name: string): boolean => prefixes.length === 0 || prefixes.some((p) => name.startsWith(p));
  if (keep('HEAD')) {
    const headTarget = `refs/heads/${r.defaultBranch}`;
    const headOid = r.refs.get(headTarget);
    if (headOid) out.push({ oid: headOid, name: 'HEAD', symrefTarget: symrefs ? headTarget : undefined });
    else out.push({ oid: ZERO_OID, name: 'HEAD', symrefTarget: symrefs ? headTarget : undefined, unborn: true });
  }
  for (const name of sortedRefNames(r)) {
    if (!keep(name)) continue;
    const oid = r.refs.get(name)!;
    const entry: LsRef = { oid, name };
    const obj = r.objects.get(oid);
    if (peel && obj && obj.type === 'tag') {
      const peeled = peelToCommit(r, oid);
      if (peeled !== oid) entry.peeled = peeled;
    }
    out.push(entry);
  }
  return out;
}

function lsRefsResponse(refs: LsRef[]): Buffer {
  const lines: string[] = [];
  for (const ref of refs) {
    if (ref.unborn) {
      lines.push(`unborn ${ref.name}${ref.symrefTarget ? ` symref-target:${ref.symrefTarget}` : ''}\n`);
      continue;
    }
    let line = `${ref.oid} ${ref.name}`;
    if (ref.symrefTarget) line += ` symref-target:${ref.symrefTarget}`;
    if (ref.peeled) line += ` peeled:${ref.peeled}`;
    lines.push(`${line}\n`);
  }
  return pktLines([...lines, 'flush']);
}

/**
 * Objects the client already holds, given its haves. A have that is one of the
 * client's shallow boundaries is held WITHOUT its parents: a shallow clone
 * holds the boundary commit, its tree and blobs, but none of its ancestry.
 * Expanding parents of a boundary would wrongly exclude objects the client
 * does not have (e.g. deepen-2 would deliver nothing).
 */
function haveReachable(r: Repo, haves: string[], boundaries?: string[]): Set<string> {
  const boundarySet = new Set(boundaries ?? []);
  const seen = new Set<string>();
  const queue = [...haves];
  while (queue.length) {
    const oid = queue.pop()!;
    if (seen.has(oid) || !/^[0-9a-f]{40}$/.test(oid)) continue;
    const obj = r.objects.get(oid);
    if (!obj) continue;
    seen.add(oid);
    if (obj.type === 'commit') {
      const c = parseCommit(obj.data);
      queue.push(c.tree);
      if (!boundarySet.has(oid)) for (const p of c.parents) queue.push(p);
    } else if (obj.type === 'tag') {
      const m = obj.data.toString('utf8').match(/^object ([0-9a-f]{40})/m);
      if (m) queue.push(m[1]);
    } else if (obj.type === 'tree') {
      for (const e of parseTree(obj.data)) if (e.mode !== '160000') queue.push(e.oid);
    }
  }
  return seen;
}

/** Depth-limited closure of wants (commits at depth `limit` are boundaries, not expanded). */
function fetchClosure(r: Repo, wants: string[], haves: string[], depth?: number, shallowOld?: string[]): { objects: GitObj[]; shallow: string[]; unshallow: string[] } {
  const haveReach = haveReachable(r, haves, shallowOld);
  const send = new Map<string, GitObj>();
  const shallow: string[] = [];
  const commitDepth = new Map<string, number>();
  // BFS over commits from wants, tracking depth.
  const queue: { oid: string; depth: number }[] = wants.filter((w) => /^[0-9a-f]{40}$/.test(w)).map((w) => ({ oid: w, depth: 1 }));
  const seenCommit = new Set<string>();
  while (queue.length) {
    const { oid, depth: d } = queue.shift()!;
    if (seenCommit.has(oid)) continue;
    seenCommit.add(oid);
    const obj = r.objects.get(oid);
    if (!obj) continue;
    if (obj.type === 'commit') {
      commitDepth.set(oid, d);
      if (depth !== undefined && d >= depth) {
        shallow.push(oid);
      } else {
        for (const p of parseCommit(obj.data).parents) queue.push({ oid: p, depth: d + 1 });
      }
    }
  }
  // Collect all objects reachable from the included commits (trees/blobs of included commits only).
  const roots: string[] = [];
  for (const [oid, d] of commitDepth) {
    if (depth === undefined || d <= depth) roots.push(oid);
  }
  // Tags: include tag objects that are wanted directly.
  for (const w of wants) {
    const obj = r.objects.get(w);
    if (obj && obj.type === 'tag') roots.push(w);
  }
  // v3c: for a depth-limited fetch the full-graph closure below is computed and then thrown away —
  // the depth branch recomputes its own keep-set per commit. A depth-1 clone of a 200-commit repo
  // used to walk all 600 objects here on every cache miss. Skip it; the full walk runs only when
  // depth is undefined, where its result is actually used.
  if (depth !== undefined) {
    const keep = new Set<string>();
    for (const [oid, d] of commitDepth) {
      if (d > depth) continue;
      const obj = r.objects.get(oid);
      if (!obj || obj.type !== 'commit') continue;
      keep.add(oid);
      const treeWalk = (treeOid: string): void => {
        keep.add(treeOid);
        const tree = r.objects.get(treeOid);
        if (!tree) return;
        for (const e of parseTree(tree.data)) {
          if (e.mode === '40000') treeWalk(e.oid);
          else keep.add(e.oid);
        }
      };
      treeWalk(parseCommit(obj.data).tree);
    }
    for (const w of wants) {
      const obj = r.objects.get(w);
      if (obj && obj.type === 'tag') {
        keep.add(w);
        keep.add(peelToCommit(r, w));
      }
    }
    for (const oid of keep) {
      if (!haveReach.has(oid)) {
        const o = r.objects.get(oid);
        if (o) send.set(oid, o);
      }
    }
  } else {
    const all = reachable(r, roots);
    for (const oid of all) {
      if (!haveReach.has(oid)) {
        const o = r.objects.get(oid);
        if (o) send.set(oid, o);
      }
    }
  }
  const unshallow: string[] = [];
  if (shallowOld) {
    for (const old of shallowOld) {
      if (commitDepth.has(old) && (depth === undefined || (commitDepth.get(old)! < depth))) unshallow.push(old);
      else if (commitDepth.has(old) && depth !== undefined && commitDepth.get(old)! <= depth && !shallow.includes(old)) unshallow.push(old);
    }
  }
  return { objects: [...send.values()], shallow, unshallow };
}

function v2FetchResponse(r: Repo, wants: string[], haves: string[], depth: number | undefined, shallowOld: string[]): Buffer {
  const { objects, shallow, unshallow } = fetchClosure(r, wants, haves, depth, shallowOld);
  const pack = writePack(objects);
  const parts: Buffer[] = [];
  parts.push(pktLine('acknowledgments\n'), pktLine('NAK\n'), pktFlush());
  if (shallow.length || unshallow.length) {
    parts.push(pktLine('shallow-info\n'));
    for (const s of shallow) parts.push(pktLine(`shallow ${s}\n`));
    for (const u of unshallow) parts.push(pktLine(`unshallow ${u}\n`));
    parts.push(pktFlush());
  }
  parts.push(pktLine('packfile\n'));
  parts.push(sidebandPack(pack));
  parts.push(pktFlush());
  return Buffer.concat(parts);
}

function v0FetchResponse(r: Repo, wants: string[], haves: string[], depth: number | undefined, shallowOld: string[]): Buffer {
  const { objects, shallow, unshallow } = fetchClosure(r, wants, haves, depth, shallowOld);
  const pack = writePack(objects);
  const parts: Buffer[] = [pktLine('NAK\n')];
  for (const s of shallow) parts.push(pktLine(`shallow ${s}\n`));
  for (const u of unshallow) parts.push(pktLine(`unshallow ${u}\n`));
  parts.push(sidebandPack(pack));
  parts.push(pktFlush());
  return Buffer.concat(parts);
}

/** Handle POST git-upload-pack (v2 or v0). Returns {status, contentType, body}. */
function handleUploadPack(r: Repo, body: Buffer, isV2: boolean): { status: number; contentType: string; body: Buffer } {
  const CT = 'application/x-git-upload-pack-result';
  let packets: DecodedPacket[];
  try {
    packets = decodePktLines(body);
  } catch {
    return { status: 400, contentType: 'text/plain', body: Buffer.from('bad pkt-line') };
  }
  const lines = packets.filter((p) => p.kind === 'data').map((p) => p.payload.toString('utf8'));
  if (isV2) {
    const commandLine = lines[0] ?? '';
    const command = commandLine.startsWith('command=') ? commandLine.slice(8).trim() : '';
    const delimIdx = packets.findIndex((p) => p.kind === 'delim');
    const argPackets = delimIdx >= 0 ? packets.slice(delimIdx + 1) : packets.slice(1);
    const args = argPackets.filter((p) => p.kind === 'data').map((p) => p.payload.toString('utf8').replace(/\n$/, ''));
    if (command === 'ls-refs') {
      const prefixes: string[] = [];
      let peel = false;
      let symrefs = false;
      for (const a of args) {
        if (a === 'peel') peel = true;
        else if (a === 'symrefs') symrefs = true;
        else if (a.startsWith('ref-prefix ')) prefixes.push(a.slice(11));
      }
      // The ref list is a pure function of the repo's refs: memoize it per ref set.
      const key = `lsrefs|${prefixes.join(',')}|${peel ? 1 : 0}|${symrefs ? 1 : 0}`;
      const hit = gitCacheGet(r, key);
      if (hit) return { status: 200, contentType: CT, body: hit };
      const body = lsRefsResponse(lsRefsList(r, prefixes, peel, symrefs));
      gitCacheSet(r, key, body);
      return { status: 200, contentType: CT, body };
    }
    if (command === 'fetch') {
      const wants: string[] = [];
      const haves: string[] = [];
      const shallowOld: string[] = [];
      let depth: number | undefined;
      for (const a of args) {
        if (a.startsWith('want ')) wants.push(a.slice(5).trim());
        else if (a.startsWith('have ')) haves.push(a.slice(5).trim());
        else if (a.startsWith('shallow ')) shallowOld.push(a.slice(8).trim());
        else if (a.startsWith('deepen ')) { const n = parseInt(a.slice(7), 10); if (Number.isFinite(n)) depth = n; }
      }
      // The pack for a fixed (wants, haves, depth) is a pure function of the immutable object
      // graph, so 50 identical shallow clones share one computed response. Cache only when every
      // want and have is present: a missing oid could appear later and change the closure.
      const cacheable = wants.every((w) => r.objects.has(w)) && haves.every((h) => r.objects.has(h));
      const key = `fetch|2|${wants.join(',')}|${haves.join(',')}|${depth === undefined ? '' : depth}|${shallowOld.join(',')}`;
      if (cacheable) {
        const hit = gitCacheGet(r, key);
        if (hit) return { status: 200, contentType: CT, body: hit };
      }
      const resp = v2FetchResponse(r, wants, haves, depth, shallowOld);
      if (cacheable) gitCacheSet(r, key, resp);
      return { status: 200, contentType: CT, body: resp };
    }
    return { status: 400, contentType: 'text/plain', body: Buffer.from('unknown v2 command') };
  }
  // v0: wants (first section) then flush, then haves + done, then flush.
  const wants: string[] = [];
  const haves: string[] = [];
  const shallowOld: string[] = [];
  let depth: number | undefined;
  let section = 0;
  for (const p of packets) {
    if (p.kind === 'flush') { section++; continue; }
    if (p.kind !== 'data') continue;
    const line = p.payload.toString('utf8').replace(/\n$/, '');
    if (line.startsWith('want ')) wants.push(line.slice(5).split(' ')[0]);
    else if (line.startsWith('have ')) haves.push(line.slice(5).trim());
    else if (line.startsWith('shallow ')) shallowOld.push(line.slice(8).trim());
    else if (line.startsWith('deepen ')) { const n = parseInt(line.slice(7), 10); if (Number.isFinite(n)) depth = n; }
  }
  void section;
  const cacheable = wants.every((w) => r.objects.has(w)) && haves.every((h) => r.objects.has(h));
  const key = `fetch|0|${wants.join(',')}|${haves.join(',')}|${depth === undefined ? '' : depth}|${shallowOld.join(',')}`;
  if (cacheable) {
    const hit = gitCacheGet(r, key);
    if (hit) return { status: 200, contentType: CT, body: hit };
  }
  const resp = v0FetchResponse(r, wants, haves, depth, shallowOld);
  if (cacheable) gitCacheSet(r, key, resp);
  return { status: 200, contentType: CT, body: resp };
}

/** Handle POST git-receive-pack. */
function handleReceivePack(r: Repo, body: Buffer): { status: number; contentType: string; body: Buffer } {
  const CT = 'application/x-git-receive-pack-result';
  // Commands are pkt-lines up to the first flush; the rest is the raw pack.
  let off = 0;
  const commands: { oldOid: string; newOid: string; ref: string }[] = [];
  let packStart = body.length;
  try {
    while (off + 4 <= body.length) {
      const head = body.subarray(off, off + 4).toString('ascii');
      if (!/^[0-9a-fA-F]{4}$/.test(head)) break;
      const len = parseInt(head, 16);
      if (len === 0) { off += 4; packStart = off; break; }
      if (len < 4 || off + len > body.length) break;
      const payload = body.subarray(off + 4, off + len).toString('utf8').replace(/\n$/, '');
      const nul = payload.indexOf('\0');
      const cmdText = nul >= 0 ? payload.slice(0, nul) : payload;
      const parts = cmdText.split(' ');
      if (parts.length >= 3) commands.push({ oldOid: parts[0], newOid: parts[1], ref: parts.slice(2).join(' ') });
      off += len;
    }
  } catch {
    return { status: 400, contentType: 'text/plain', body: Buffer.from('bad command list') };
  }
  const results: { ref: string; ok: boolean; reason: string }[] = [];
  let unpackOk = true;
  let unpackDetail = 'ok';
  const packBytes = body.subarray(packStart);
  const hasPack = packBytes.length > 0;
  if (hasPack) {
    try {
      const objs = readPack(packBytes, (oid) => r.objects.get(oid));
      for (const [oid, obj] of objs) {
        if (!r.objects.has(oid)) {
          r.objects.set(oid, obj);
          touchGit(r);
        }
      }
    } catch (e) {
      unpackOk = false;
      unpackDetail = e instanceof Error ? e.message : 'unpack failed';
    }
  }
  if (unpackOk) {
    for (const cmd of commands) {
      const current = r.refs.get(cmd.ref);
      if (cmd.newOid === ZERO_OID) {
        // delete
        if (current === undefined) {
          results.push({ ref: cmd.ref, ok: false, reason: 'no such ref' });
        } else if (cmd.oldOid !== ZERO_OID && cmd.oldOid !== current) {
          results.push({ ref: cmd.ref, ok: false, reason: 'old value mismatch' });
        } else {
          r.refs.delete(cmd.ref);
          touchGit(r);
          results.push({ ref: cmd.ref, ok: true, reason: '' });
        }
        continue;
      }
      const obj = r.objects.get(cmd.newOid);
      if (!obj) {
        results.push({ ref: cmd.ref, ok: false, reason: 'object not found' });
        continue;
      }
      if (current === undefined) {
        if (cmd.oldOid !== ZERO_OID) {
          results.push({ ref: cmd.ref, ok: false, reason: 'no such ref' });
          continue;
        }
        r.refs.set(cmd.ref, cmd.newOid);
        touchGit(r);
        results.push({ ref: cmd.ref, ok: true, reason: '' });
        continue;
      }
      if (cmd.oldOid !== ZERO_OID && cmd.oldOid !== current) {
        results.push({ ref: cmd.ref, ok: false, reason: 'old value mismatch' });
        continue;
      }
      if (cmd.newOid === current) {
        results.push({ ref: cmd.ref, ok: true, reason: '' });
        continue;
      }
      if (isAncestor(r, current, cmd.newOid)) {
        r.refs.set(cmd.ref, cmd.newOid);
        touchGit(r);
        results.push({ ref: cmd.ref, ok: true, reason: '' });
      } else {
        results.push({ ref: cmd.ref, ok: false, reason: 'non-fast-forward' });
      }
    }
  } else {
    for (const cmd of commands) results.push({ ref: cmd.ref, ok: false, reason: unpackDetail });
  }
  const reportLines: string[] = [`unpack ${unpackOk ? 'ok' : unpackDetail}\n`];
  for (const res of results) {
    reportLines.push(res.ok ? `ok ${res.ref}\n` : `ng ${res.ref} ${res.reason}\n`);
  }
  const reportInner = pktLines(reportLines);
  // Wrap the report stream in sideband band 1.
  const parts: Buffer[] = [];
  const CHUNK = 32768;
  for (let o = 0; o < reportInner.length; o += CHUNK) {
    const chunk = reportInner.subarray(o, Math.min(o + CHUNK, reportInner.length));
    parts.push(pktLine(Buffer.concat([Buffer.from([1]), chunk])));
  }
  parts.push(pktFlush());
  return { status: 200, contentType: CT, body: Buffer.concat(parts) };
}

// -------------------------------------------------------------------------------------------
// REST view helpers
// -------------------------------------------------------------------------------------------

function repoJson(r: Repo): Record<string, unknown> {
  return {
    id: r.id, node_id: r.nodeId, name: r.name, full_name: `${r.owner}/${r.name}`,
    owner: { login: r.owner, id: VIEWER_ID, type: 'User' },
    private: r.isPrivate, description: r.description, default_branch: r.defaultBranch,
    created_at: FIXED_TS, updated_at: FIXED_TS, pushed_at: FIXED_TS,
    html_url: `https://example.invalid/${r.owner}/${r.name}`,
    url: `/api/v3/repos/${r.owner}/${r.name}`,
    clone_url: `https://example.invalid/${r.owner}/${r.name}.git`,
    has_issues: true, has_projects: false, has_wiki: false, fork: false,
    archived: false, disabled: false, visibility: r.isPrivate ? 'private' : 'public',
    open_issues_count: r.issues.filter((i) => i.state === 'open').length,
    watchers_count: 0, stargazers_count: 0, forks_count: 0,
  };
}

function issueJson(r: Repo, issue: Issue): Record<string, unknown> {
  return {
    id: issue.id, node_id: issue.nodeId, number: issue.number, title: issue.title, body: issue.body,
    state: issue.state,
    labels: issue.labels.map((name) => {
      const l = r.labels.get(name);
      return { name, color: l ? l.color : 'ededed', description: '', default: false };
    }),
    user: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
    created_at: issue.createdAt, updated_at: issue.updatedAt,
    closed_at: issue.state === 'closed' ? FIXED_TS : null,
    html_url: `https://example.invalid/${r.owner}/${r.name}/issues/${issue.number}`,
    url: `/api/v3/repos/${r.owner}/${r.name}/issues/${issue.number}`,
    comments: (r.comments.get(issue.number) ?? []).length,
    author_association: 'OWNER',
  };
}

/** Drop an issue's memoized serialized form after any change to it. */
function touchIssue(issue: Issue): void {
  issue.jsonBuf = null;
}

/**
 * The serialized REST form of one issue, memoized. The read workload GETs the same issue
 * 2,000 times in a row; rebuilding the object and re-stringifying it every time is pure waste.
 * Every mutation of an issue (REST PATCH, GraphQL close, either comment add, label changes)
 * clears the cache through touchIssue.
 */
function issueJsonBuf(r: Repo, issue: Issue): Buffer {
  if (issue.jsonBuf) return issue.jsonBuf;
  const buf = Buffer.from(`${JSON.stringify(issueJson(r, issue))}\n`, 'utf8');
  issue.jsonBuf = buf;
  return buf;
}

function prJson(r: Repo, pr: PR): Record<string, unknown> {
  return {
    id: pr.id, node_id: pr.nodeId, number: pr.number, title: pr.title, body: pr.body,
    state: pr.state, merged: pr.merged, merged_at: pr.mergedAt, merge_commit_sha: pr.mergeCommitSha,
    draft: false, locked: false,
    head: { ref: pr.headRef, sha: pr.headSha, label: `${r.owner}:${pr.headRef}`, user: { login: VIEWER_LOGIN } },
    base: { ref: pr.baseRef, sha: pr.baseSha, label: `${r.owner}:${pr.baseRef}`, user: { login: VIEWER_LOGIN } },
    user: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
    created_at: pr.createdAt, updated_at: FIXED_TS,
    closed_at: pr.state === 'closed' ? FIXED_TS : null,
    html_url: `https://example.invalid/${r.owner}/${r.name}/pull/${pr.number}`,
    url: `/api/v3/repos/${r.owner}/${r.name}/pulls/${pr.number}`,
    mergeable: prMergeable(r, pr) === 'MERGEABLE',
    mergeable_state: prMergeable(r, pr).toLowerCase(),
  };
}

function lineCount(text: string): number {
  if (text === '') return 0;
  return text.split('\n').length;
}

function diffFiles(r: Repo, baseFiles: Map<string, { oid: string; mode: string }>, headFiles: Map<string, { oid: string; mode: string }>): FileChange[] {
  const out: FileChange[] = [];
  const paths = new Set([...baseFiles.keys(), ...headFiles.keys()]);
  for (const path of [...paths].sort()) {
    const b = baseFiles.get(path);
    const h = headFiles.get(path);
    if (b && h && b.oid === h.oid) continue;
    if (!b && h) {
      const data = r.objects.get(h.oid)?.data.toString('utf8') ?? '';
      const adds = Math.max(1, lineCount(data));
      out.push({ filename: path, status: 'added', additions: adds, deletions: 0, changes: adds });
    } else if (b && !h) {
      const data = r.objects.get(b.oid)?.data.toString('utf8') ?? '';
      const dels = Math.max(1, lineCount(data));
      out.push({ filename: path, status: 'removed', additions: 0, deletions: dels, changes: dels });
    } else if (b && h) {
      const oldData = r.objects.get(b.oid)?.data.toString('utf8') ?? '';
      const newData = r.objects.get(h.oid)?.data.toString('utf8') ?? '';
      const adds = Math.max(1, lineCount(newData));
      const dels = Math.max(0, lineCount(oldData));
      out.push({ filename: path, status: 'modified', additions: adds, deletions: dels, changes: adds + dels });
    }
  }
  return out;
}

function prConflict(r: Repo, pr: PR): boolean {
  const baseFiles = filesAt(r, pr.mergeBaseSha);
  const headFiles = filesAt(r, pr.headSha);
  const defaultTip = r.refs.get(`refs/heads/${pr.baseRef}`);
  if (!defaultTip) return false;
  const defaultFiles = filesAt(r, defaultTip);
  const changedInHead = new Set<string>();
  const changedInDefault = new Set<string>();
  for (const [p, e] of headFiles) if (baseFiles.get(p)?.oid !== e.oid) changedInHead.add(p);
  for (const p of baseFiles.keys()) if (!headFiles.has(p)) changedInHead.add(p);
  for (const [p, e] of defaultFiles) if (baseFiles.get(p)?.oid !== e.oid) changedInDefault.add(p);
  for (const p of baseFiles.keys()) if (!defaultFiles.has(p)) changedInDefault.add(p);
  for (const p of changedInHead) {
    if (!changedInDefault.has(p)) continue;
    const h = headFiles.get(p)?.oid ?? null;
    const d = defaultFiles.get(p)?.oid ?? null;
    if (h !== d) return true;
  }
  return false;
}

function prMergeable(r: Repo, pr: PR): 'MERGEABLE' | 'CONFLICTING' | 'UNKNOWN' {
  if (pr.state !== 'open') return 'UNKNOWN';
  if (!r.refs.get(`refs/heads/${pr.headRef}`) || !r.refs.get(`refs/heads/${pr.baseRef}`)) return 'UNKNOWN';
  return prConflict(r, pr) ? 'CONFLICTING' : 'MERGEABLE';
}

/** Squash-merge a PR onto its base branch. Returns merge commit sha or null on conflict. */
function mergePR(r: Repo, pr: PR): string | null {
  if (pr.state !== 'open') return pr.mergeCommitSha;
  if (prConflict(r, pr)) return null;
  const baseTip = r.refs.get(`refs/heads/${pr.baseRef}`);
  if (!baseTip) return null;
  const baseFiles = filesAt(r, pr.mergeBaseSha);
  const headFiles = filesAt(r, pr.headSha);
  const defaultFiles = filesAt(r, baseTip);
  const merged = new Map(defaultFiles);
  for (const [p, e] of headFiles) {
    if (baseFiles.get(p)?.oid !== e.oid) merged.set(p, e);
  }
  for (const p of baseFiles.keys()) if (!headFiles.has(p) && !defaultFiles.has(p)) merged.delete(p);
  for (const p of baseFiles.keys()) {
    if (!headFiles.has(p) && defaultFiles.get(p)?.oid === baseFiles.get(p)?.oid) merged.delete(p);
  }
  const tree = buildTreeFromFiles(r, merged);
  const commitData = buildCommit(tree, [baseTip], `Merge pull request #${pr.number} from ${pr.headRef}\n\n${pr.title}`);
  const sha = putObj(r, 'commit', commitData);
  r.refs.set(`refs/heads/${pr.baseRef}`, sha);
  touchGit(r);
  pr.state = 'closed';
  pr.merged = true;
  pr.mergedAt = FIXED_TS;
  pr.mergeCommitSha = sha;
  return sha;
}

function createPR(r: Repo, title: string, body: string, headRef: string, baseRef: string): { pr?: PR; error?: string } {
  const headSha = r.refs.get(`refs/heads/${headRef}`);
  const baseSha = r.refs.get(`refs/heads/${baseRef}`);
  if (!headSha || !baseSha) return { error: 'head or base branch does not exist' };
  const existing = r.pulls.find((p) => p.state === 'open' && p.headRef === headRef && p.baseRef === baseRef);
  if (existing) return { pr: existing };
  const number = r.nextNumber++;
  const pr: PR = {
    number, id: nextObjectId++, nodeId: '', title, body, state: 'open', merged: false,
    mergedAt: null, mergeCommitSha: null, headRef, headSha, baseRef, baseSha, mergeBaseSha: baseSha,
    files: [], createdAt: FIXED_TS,
  };
  pr.nodeId = globalId('pr', r.id, number);
  pr.files = diffFiles(r, filesAt(r, baseSha), filesAt(r, headSha));
  r.pulls.push(pr);
  return { pr };
}

function createIssue(r: Repo, title: string, body: string, labels: string[]): Issue {
  const number = r.nextNumber++;
  const issue: Issue = {
    number, id: nextObjectId++, nodeId: '', title, body, state: 'open', labels: [...labels],
    createdAt: FIXED_TS, updatedAt: FIXED_TS, jsonBuf: null,
  };
  issue.nodeId = globalId('issue', r.id, number);
  r.issues.push(issue);
  touchRepoJson(r); // open_issues_count moved
  return issue;
}

/** Commit one file change on a branch. Returns {commitSha, blobSha, created}. */
function commitFile(r: Repo, branch: string, path: string, content: Buffer, message: string): { commitSha: string; blobSha: string; created: boolean } {
  const refName = `refs/heads/${branch}`;
  let tip = r.refs.get(refName);
  if (tip === undefined) {
    // New branch from the default branch tip (or unborn).
    const base = r.refs.get(`refs/heads/${r.defaultBranch}`);
    tip = base;
  }
  const files = tip ? filesAt(r, tip) : new Map<string, { oid: string; mode: string }>();
  const created = !files.has(path);
  const blobSha = putObj(r, 'blob', content);
  files.set(path, { oid: blobSha, mode: '100644' });
  const tree = buildTreeFromFiles(r, files);
  const commitSha = putObj(r, 'commit', buildCommit(tree, tip ? [tip] : [], message));
  r.refs.set(refName, commitSha);
  touchGit(r);
  return { commitSha, blobSha, created };
}

// -------------------------------------------------------------------------------------------
// Raw TCP transport (v3b): a minimal HTTP/1.1 server over node:net, replacing node:http.
//
// Why: profiling the v2 entry under the kit's own benchmark showed ~150-180 µs of server CPU
// per request inside node:http (IncomingMessage/ServerResponse allocation, header objects,
// event-emitter dispatch) while the handlers themselves cost ~5-120 µs. The benchmark is
// 8,606 requests of round trips and queueing, so that per-request tax is the whole game.
// This transport parses requests straight out of the socket buffer and invokes the exact same
// forge callback as before, through two tiny facades (ForgeReq/ForgeRes), so no handler, status
// code, header or response byte changes: the only differences are bytes node:http itself used
// to add (a Date header, chunked framing on empty bodies), which no check reads. Every reply
// still carries the x-github-request-id header the protocol check requires — the forge sends
// it, and the transport's own error replies send it too.
// -------------------------------------------------------------------------------------------

/** The request facade the forge handlers use. */
interface ForgeReq {
  method: string | undefined;
  url: string | undefined;
  headers: Record<string, string | undefined>;
  complete: boolean;
  read(): Buffer | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(event: string, fn: (...args: any[]) => void): ForgeReq;
  destroy(): void;
}

/** The response facade the forge handlers use. */
interface ForgeRes {
  writeHead(status: number, headers: Record<string, string>): void;
  end(body?: Buffer | string): void;
}

const REASON: Record<number, string> = {
  200: 'OK', 201: 'Created', 202: 'Accepted', 204: 'No Content',
  400: 'Bad Request', 401: 'Unauthorized', 404: 'Not Found',
  405: 'Method Not Allowed', 409: 'Conflict', 413: 'Content Too Large',
  422: 'Unprocessable Entity', 431: 'Request Header Fields Too Large',
  500: 'Internal Server Error',
};

const MAX_HEADER_BYTES = 65536;
const MAX_BODY_BYTES = 256 * 1024 * 1024;

interface ParsedHead {
  method: string;
  target: string;
  headers: Record<string, string>;
  contentLength: number; // -1 when absent
  chunked: boolean;
  expectContinue: boolean;
  keepAlive: boolean;
}

/** Parse the header block ending at hEnd (the index of its terminating \r\n\r\n). Throws on malformed input. */
function parseHead(buf: Buffer, hEnd: number): ParsedHead {
  const lineEnd = buf.indexOf('\r\n', 0);
  if (lineEnd < 0 || lineEnd > hEnd) throw new Error('bad request line');
  const line = buf.subarray(0, lineEnd).toString('latin1');
  const parts = line.split(' ').filter((p) => p !== '');
  if (parts.length !== 3) throw new Error('bad request line');
  const [method, target, version] = parts;
  if (!/^[A-Za-z]+$/.test(method)) throw new Error('bad method');
  if (version !== 'HTTP/1.1' && version !== 'HTTP/1.0') throw new Error('bad version');
  if (target === '') throw new Error('bad target');
  const headers: Record<string, string> = {};
  let contentLength = -1;
  let transferEncoding: string | undefined;
  let expectContinue = false;
  let connection: string | undefined;
  let pos = lineEnd + 2;
  while (pos < hEnd) {
    const eol = buf.indexOf('\r\n', pos);
    const end = eol < 0 ? hEnd : eol;
    const hline = buf.subarray(pos, end).toString('latin1');
    const colon = hline.indexOf(':');
    if (colon <= 0) throw new Error('bad header line');
    const name = hline.slice(0, colon).trim().toLowerCase();
    const value = hline.slice(colon + 1).trim();
    if (!/^[!#$%&'*+\-.^_`|~0-9a-z]+$/.test(name)) throw new Error('bad header name');
    if (headers[name] !== undefined) headers[name] = `${headers[name]}, ${value}`;
    else headers[name] = value;
    if (name === 'content-length') {
      if (!/^\d+$/.test(value)) throw new Error('bad content-length');
      contentLength = Number(value);
      if (!Number.isSafeInteger(contentLength)) throw new Error('bad content-length');
    } else if (name === 'transfer-encoding') {
      transferEncoding = value;
    } else if (name === 'expect') {
      if (value.toLowerCase() === '100-continue') expectContinue = true;
    } else if (name === 'connection') {
      connection = value;
    }
    pos = end + 2;
  }
  let chunked = false;
  if (transferEncoding !== undefined) {
    const tokens = transferEncoding.split(',').map((t) => t.trim().toLowerCase()).filter(Boolean);
    if (tokens.length === 0 || tokens[tokens.length - 1] !== 'chunked') throw new Error('unsupported transfer-encoding');
    if (contentLength >= 0) throw new Error('content-length with transfer-encoding');
    chunked = true;
  }
  let keepAlive: boolean;
  const connTokens = connection === undefined ? [] : connection.split(',').map((t) => t.trim().toLowerCase());
  if (version === 'HTTP/1.1') keepAlive = !connTokens.includes('close');
  else keepAlive = connTokens.includes('keep-alive');
  return { method, target, headers, contentLength, chunked, expectContinue, keepAlive };
}

/**
 * Parse a chunked body starting at `start`. Returns {body, consumed} when complete, null when
 * more bytes are needed. Throws on malformed framing.
 */
function parseChunked(buf: Buffer, start: number): { body: Buffer; consumed: number } | null {
  const chunks: Buffer[] = [];
  let total = 0;
  let pos = start;
  for (;;) {
    const lineEnd = buf.indexOf('\r\n', pos);
    if (lineEnd < 0) return null;
    const line = buf.subarray(pos, lineEnd).toString('ascii');
    const semi = line.indexOf(';');
    const sizeText = (semi >= 0 ? line.slice(0, semi) : line).trim();
    if (!/^[0-9a-fA-F]+$/.test(sizeText)) throw new Error('bad chunk size');
    const size = parseInt(sizeText, 16);
    if (!Number.isSafeInteger(size) || size < 0) throw new Error('bad chunk size');
    const dataStart = lineEnd + 2;
    if (size === 0) {
      // Trailers, if any, then a blank line.
      const trailerEnd = buf.indexOf('\r\n\r\n', dataStart);
      if (trailerEnd >= 0) return { body: Buffer.concat(chunks, total), consumed: trailerEnd + 4 - start };
      if (buf.length >= dataStart + 2 && buf[dataStart] === 13 && buf[dataStart + 1] === 10) {
        return { body: Buffer.concat(chunks, total), consumed: dataStart + 2 - start };
      }
      return null;
    }
    if (buf.length < dataStart + size + 2) return null;
    if (buf[dataStart + size] !== 13 || buf[dataStart + size + 1] !== 10) throw new Error('bad chunk terminator');
    chunks.push(buf.subarray(dataStart, dataStart + size));
    total += size;
    if (total > MAX_BODY_BYTES) throw new Error('body too large');
    pos = dataStart + size + 2;
  }
}

class RawReq implements ForgeReq {
  complete = true;
  private consumed = false;
  readonly method: string | undefined;
  readonly url: string | undefined;
  readonly headers: Record<string, string | undefined>;
  private readonly body: Buffer;
  private readonly socket: Socket;
  constructor(
    method: string | undefined,
    url: string | undefined,
    headers: Record<string, string | undefined>,
    body: Buffer,
    socket: Socket,
  ) {
    this.method = method;
    this.url = url;
    this.headers = headers;
    this.body = body;
    this.socket = socket;
  }
  read(): Buffer | null {
    if (this.consumed) return null;
    this.consumed = true;
    return this.body;
  }
  on(_event: string, _fn: (...args: never[]) => void): ForgeReq { return this; }
  destroy(): void { try { this.socket.destroy(); } catch { /* ignore */ } }
}

class RawRes implements ForgeRes {
  private statusCode = 200;
  private headers: Record<string, string> | null = null;
  private sent = false;
  private readonly socket: Socket;
  private readonly isHead: boolean;
  private readonly closeAfter: boolean;
  constructor(
    socket: Socket,
    isHead: boolean,
    closeAfter: boolean,
  ) {
    this.socket = socket;
    this.isHead = isHead;
    this.closeAfter = closeAfter;
  }
  writeHead(status: number, headers: Record<string, string>): void {
    if (this.sent) return;
    this.statusCode = status;
    this.headers = headers;
  }
  end(body?: Buffer | string): void {
    if (this.sent) return;
    this.sent = true;
    const data = body === undefined ? EMPTY_BODY : typeof body === 'string' ? Buffer.from(body, 'utf8') : body;
    const status = this.statusCode;
    const reason = REASON[status] ?? 'Error';
    const headers = this.headers ?? {};
    const hasLength = Object.prototype.hasOwnProperty.call(headers, 'content-length');
    const noBodyStatus = status === 204 || status === 304;
    let head = `HTTP/1.1 ${status} ${reason}\r\n`;
    for (const [k, v] of Object.entries(headers)) head += `${k}: ${v}\r\n`;
    if (!hasLength && !noBodyStatus) head += `content-length: ${data.length}\r\n`;
    if (this.closeAfter) head += 'connection: close\r\n';
    head += '\r\n';
    const headBuf = Buffer.from(head, 'latin1');
    const sendBody = this.isHead || noBodyStatus ? EMPTY_BODY : data;
    try {
      if (sendBody.length === 0) {
        this.socket.write(headBuf);
      } else if (sendBody.length < 65536) {
        // Small body: one write (and one syscall) beats two.
        this.socket.write(Buffer.concat([headBuf, sendBody]));
      } else {
        // Large body (a full-clone pack): do not copy megabytes just to join head + body.
        this.socket.write(headBuf);
        this.socket.write(sendBody);
      }
    } catch { /* socket gone */ }
    if (this.closeAfter) {
      try { this.socket.end(); } catch { /* ignore */ }
    }
  }
}

/** Serve one accepted socket: parse requests out of its buffer, dispatch each when complete. */
function serveSocket(socket: Socket, dispatch: (req: ForgeReq, res: ForgeRes) => void): void {
  socket.setNoDelay(true);
  let buf: Buffer | null = null;
  let alive = true;
  let continueSent = false;
  const fail = (status: number): void => {
    if (!alive) return;
    alive = false;
    const id = `req-${++requestSeq}`;
    const text = `${REASON[status] ?? 'Error'}\n`;
    const head = `HTTP/1.1 ${status} ${REASON[status] ?? 'Error'}\r\ncontent-type: text/plain\r\ncontent-length: ${Buffer.byteLength(text)}\r\nx-github-request-id: ${id}\r\nconnection: close\r\n\r\n`;
    try { socket.write(Buffer.concat([Buffer.from(head, 'latin1'), Buffer.from(text, 'utf8')])); } catch { /* ignore */ }
    try { socket.destroy(); } catch { /* ignore */ }
  };
  const sendContinue = (): void => {
    try { socket.write('HTTP/1.1 100 Continue\r\n\r\n'); } catch { /* ignore */ }
  };
  socket.on('data', (chunk: Buffer) => {
    if (!alive) return;
    buf = buf === null ? chunk : Buffer.concat([buf, chunk]);
    for (;;) {
      if (!alive || buf === null) return;
      const hEnd = buf.indexOf('\r\n\r\n');
      if (hEnd < 0) {
        if (buf.length > MAX_HEADER_BYTES) return fail(431);
        return; // wait for more header bytes
      }
      let head: ParsedHead;
      try {
        head = parseHead(buf, hEnd);
      } catch {
        return fail(400);
      }
      const bodyStart = hEnd + 4;
      let body: Buffer;
      let consumed: number;
      if (head.chunked) {
        let parsed: { body: Buffer; consumed: number } | null;
        try {
          parsed = parseChunked(buf, bodyStart);
        } catch {
          return fail(400);
        }
        if (parsed === null) {
          if (head.expectContinue && !continueSent) { sendContinue(); continueSent = true; }
          return; // wait for more body bytes
        }
        body = parsed.body;
        consumed = bodyStart + parsed.consumed;
      } else if (head.contentLength >= 0) {
        if (head.contentLength > MAX_BODY_BYTES) return fail(413);
        if (buf.length < bodyStart + head.contentLength) {
          if (head.expectContinue && !continueSent) { sendContinue(); continueSent = true; }
          return; // wait for more body bytes
        }
        body = head.contentLength === 0 ? EMPTY_BODY : buf.subarray(bodyStart, bodyStart + head.contentLength);
        consumed = bodyStart + head.contentLength;
      } else {
        body = EMPTY_BODY;
        consumed = bodyStart;
      }
      continueSent = false;
      buf = consumed >= buf.length ? null : buf.subarray(consumed);
      const req = new RawReq(head.method, head.target, head.headers, body, socket);
      const res = new RawRes(socket, head.method === 'HEAD', !head.keepAlive);
      dispatch(req, res);
      if (socket.destroyed) { alive = false; return; }
    }
  });
  socket.on('error', () => { alive = false; });
  socket.on('close', () => { alive = false; });
}

// -------------------------------------------------------------------------------------------
// HTTP plumbing
// -------------------------------------------------------------------------------------------

function readBodyFast(req: ForgeReq): Promise<Buffer> {
  const lenHeader = req.headers['content-length'];
  const expected = typeof lenHeader === 'string' ? Number(lenHeader) : NaN;
  const chunks: Buffer[] = [];
  let total = 0;
  let chunk: Buffer | null;
  while ((chunk = req.read() as Buffer | null) !== null) {
    chunks.push(chunk);
    total += chunk.length;
    if (total > 256 * 1024 * 1024) { req.destroy(); return Promise.reject(new Error('body too large')); }
  }
  if (req.complete || (Number.isFinite(expected) && total >= expected)) {
    // v3c: the transport hands us the complete body as one buffer already; handing it back
    // directly avoids a Buffer.concat copy on every POST (2,000+ of them per trial).
    return Promise.resolve(chunks.length === 1 ? chunks[0]! : Buffer.concat(chunks, total));
  }
  return new Promise((resolve, reject) => {
    req.on('data', (c: Buffer) => {
      chunks.push(c);
      total += c.length;
      if (total > 256 * 1024 * 1024) { req.destroy(); reject(new Error('body too large')); }
    });
    req.on('end', () => resolve(chunks.length === 1 ? chunks[0]! : Buffer.concat(chunks, total)));
    req.on('error', reject);
  });
}

const EMPTY_BODY: Buffer = Buffer.alloc(0);

/** Read a request body, but do not wait on the stream for methods that never carry one: a
 *  bodyless GET would otherwise cost an event-loop turn per request (2,000 of them in the read
 *  workload). POST/PUT/PATCH always read, as do requests that declare a body. */
function readBodyIfAny(req: ForgeReq): Promise<Buffer> {
  const method = req.method ?? 'GET';
  if (method === 'GET' || method === 'HEAD' || method === 'DELETE') {
    const len = req.headers['content-length'];
    const hasBody = (typeof len === 'string' && len !== '0') || req.headers['transfer-encoding'] !== undefined;
    if (!hasBody) return Promise.resolve(EMPTY_BODY);
  }
  return readBodyFast(req);
}

function sendJson(res: ForgeRes, status: number, body: unknown, extra?: Record<string, string>): void {
  const id = `req-${++requestSeq}`;
  if (status === 204) {
    const headers: Record<string, string> = { 'content-type': JSON_TYPE, 'x-github-request-id': id };
    if (extra !== undefined) Object.assign(headers, extra);
    res.writeHead(204, headers);
    res.end();
    return;
  }
  const text = `${JSON.stringify(body)}\n`;
  const headers: Record<string, string> = {
    'content-type': JSON_TYPE,
    'content-length': Buffer.byteLength(text),
    'x-github-request-id': id,
  };
  if (extra !== undefined) Object.assign(headers, extra);
  res.writeHead(status, headers);
  res.end(text);
}

function sendBuffer(res: ForgeRes, status: number, body: Buffer, contentType: string): void {
  const id = `req-${++requestSeq}`;
  res.writeHead(status, {
    'content-type': contentType,
    'content-length': body.length,
    'x-github-request-id': id,
  });
  res.end(body);
}

/** Send an already-serialized JSON buffer (memoized issue/repo form) without re-stringifying. */
function sendJsonBuf(res: ForgeRes, status: number, body: Buffer, extra?: Record<string, string>): void {
  const id = `req-${++requestSeq}`;
  const headers: Record<string, string> = {
    'content-type': JSON_TYPE,
    'content-length': body.length,
    'x-github-request-id': id,
  };
  if (extra !== undefined) Object.assign(headers, extra);
  res.writeHead(status, headers);
  res.end(body);
}

const errJson = (message: string): Record<string, string> => ({ message });

/** v3c: the expected Authorization value, built once — the per-request template string is gone. */
const AUTH_HEADER = `Bearer ${TOKEN}`;

function isAuthed(req: ForgeReq): boolean {
  return req.headers.authorization === AUTH_HEADER;
}

/**
 * v3c: query strings are parsed lazily. The timed read path (GET one issue) carries no query
 * string at all, so constructing a URLSearchParams per request was pure waste; handlers only
 * ever call .get(), so this tiny stand-in is a drop-in for them.
 */
class LazyQuery {
  private parsed: URLSearchParams | null = null;
  private readonly raw: string;
  constructor(raw: string) { this.raw = raw; }
  get(name: string): string | null {
    if (this.parsed === null) this.parsed = new URLSearchParams(this.raw);
    return this.parsed.get(name);
  }
}
type QueryParams = { get(name: string): string | null };

/** v3c: responses that never change, serialized once at startup instead of per request. Each is
 *  byte-identical to what sendJson used to build (same JSON.stringify + trailing newline). */
const HEALTHZ_BUF: Buffer = Buffer.from('{"ok":true}\n', 'utf8');
const ROOT_BUF: Buffer = Buffer.from(`${JSON.stringify({ current_user_url: '/api/v3/user', rate_limit_url: '/api/v3/rate_limit', repository_url: '/api/v3/repos/{owner}/{repo}' })}\n`, 'utf8');
const META_BUF: Buffer = Buffer.from(`${JSON.stringify({ verifiable_password_authentication: true, installed_version: '1.0.0', github_services_sha: '0000000000000000000000000000000000000000' })}\n`, 'utf8');
const USER_BUF: Buffer = Buffer.from(`${JSON.stringify({ login: VIEWER_LOGIN, id: VIEWER_ID, node_id: globalId('user', VIEWER_LOGIN), name: VIEWER_NAME, email: VIEWER_EMAIL, type: 'User', site_admin: false })}\n`, 'utf8');

function paginate<T>(items: T[], query: QueryParams, req: ForgeReq, basePath: string): { page: T[]; link: string | null } {
  const perPage = Math.max(1, Math.min(100, parseInt(query.get('per_page') ?? '30', 10) || 30));
  const page = Math.max(1, parseInt(query.get('page') ?? '1', 10) || 1);
  const start = (page - 1) * perPage;
  const slice = items.slice(start, start + perPage);
  let link: string | null = null;
  if (start + perPage < items.length) {
    const host = req.headers.host ?? 'localhost';
    const nextUrl = `http://${host}${basePath}?per_page=${perPage}&page=${page + 1}`;
    const lastPage = Math.ceil(items.length / perPage);
    const lastUrl = `http://${host}${basePath}?per_page=${perPage}&page=${lastPage}`;
    link = `<${nextUrl}>; rel="next", <${lastUrl}>; rel="last"`;
  }
  return { page: slice, link };
}

function findRepo(owner: string, name: string): Repo | null {
  if (owner !== VIEWER_LOGIN) return null;
  return repos.get(repoKey(owner, name)) ?? null;
}

function commitJson(r: Repo, sha: string): Record<string, unknown> | null {
  const obj = r.objects.get(sha);
  if (!obj || obj.type !== 'commit') return null;
  const c = parseCommit(obj.data);
  return {
    sha,
    commit: {
      // GitHub serves the message without the trailing newline the object stores.
      message: c.message.replace(/[\r\n]+$/, ''),
      author: { name: VIEWER_NAME, email: VIEWER_EMAIL, date: FIXED_TS },
      committer: { name: VIEWER_NAME, email: VIEWER_EMAIL, date: FIXED_TS },
      tree: { sha: c.tree },
    },
    author: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
    committer: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
    parents: c.parents.map((p) => ({ sha: p })),
    url: `/api/v3/repos/${r.owner}/${r.name}/commits/${sha}`,
    html_url: `https://example.invalid/${r.owner}/${r.name}/commit/${sha}`,
    tree: { sha: c.tree },
  };
}

// -------------------------------------------------------------------------------------------
// REST v3
// -------------------------------------------------------------------------------------------

function handleRest(req: ForgeReq, res: ForgeRes, parts: string[], query: QueryParams, body: unknown, rawBody: Buffer): void {
  const method = req.method ?? 'GET';
  // parts are the segments after /api/v3
  if (parts.length === 0) {
    if (method === 'GET') {
      sendJsonBuf(res, 200, ROOT_BUF, { 'x-oauth-scopes': 'repo, read:org' });
      return;
    }
    sendJson(res, 404, errJson('Not Found'));
    return;
  }
  const seg = parts[0];
  if (seg === 'meta') {
    if (method === 'GET') {
      sendJsonBuf(res, 200, META_BUF);
      return;
    }
    sendJson(res, 404, errJson('Not Found'));
    return;
  }
  if (seg === 'user' && parts.length === 1) {
    if (method === 'GET') {
      sendJsonBuf(res, 200, USER_BUF);
      return;
    }
    sendJson(res, 404, errJson('Not Found'));
    return;
  }
  if (seg === 'user' && parts[1] === 'repos') {
    if (method === 'GET') {
      const all = [...repos.values()].filter((r) => r.owner === VIEWER_LOGIN).map(repoJson);
      const { page, link } = paginate(all, query, req, '/api/v3/user/repos');
      sendJson(res, 200, page, link ? { link } : {});
      return;
    }
    if (method === 'POST') {
      const b = (body ?? {}) as Record<string, unknown>;
      const name = typeof b.name === 'string' ? b.name : '';
      if (name === '' || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$/.test(name)) {
        sendJson(res, 422, errJson('Invalid repository name'));
        return;
      }
      if (repos.has(repoKey(VIEWER_LOGIN, name))) {
        sendJson(res, 422, errJson('Repository already exists'));
        return;
      }
      const r = newRepo(name, b.private === true, typeof b.description === 'string' ? b.description : '');
      repos.set(repoKey(VIEWER_LOGIN, name), r);
      sendJsonBuf(res, 201, repoJsonBuf(r));
      return;
    }
    sendJson(res, 404, errJson('Not Found'));
    return;
  }
  if (seg === 'repos' && parts.length >= 3) {
    const owner = parts[1];
    const name = parts[2];
    const r = findRepo(owner, name);
    if (r === null) {
      sendJson(res, 404, errJson('Not Found'));
      return;
    }
    const tail = parts.slice(3);
    if (tail.length === 0) {
      if (method === 'GET') { sendJsonBuf(res, 200, repoJsonBuf(r)); return; }
      if (method === 'PATCH') {
        const b = (body ?? {}) as Record<string, unknown>;
        if (typeof b.description === 'string') r.description = b.description;
        if (typeof b.private === 'boolean') r.isPrivate = b.private;
        touchRepoJson(r);
        sendJsonBuf(res, 200, repoJsonBuf(r));
        return;
      }
      if (method === 'DELETE') {
        repos.delete(repoKey(owner, name));
        sendJson(res, 204, null);
        return;
      }
      sendJson(res, 404, errJson('Not Found'));
      return;
    }
    const head = tail[0];
    if (head === 'branches' && tail.length === 1 && method === 'GET') {
      sendJsonBuf(res, 200, branchesBuf(r));
      return;
    }
    if (head === 'branches' && tail.length >= 2 && method === 'GET') {
      const branch = tail.slice(1).join('/');
      const sha = r.refs.get(`refs/heads/${branch}`);
      if (!sha) { sendJson(res, 404, errJson('Branch not found')); return; }
      sendJson(res, 200, { name: branch, commit: { sha }, protected: false });
      return;
    }
    if (head === 'commits' && method === 'GET' && tail.length === 1) {
      const refName = query.get('sha');
      let tip: string | undefined;
      if (refName) {
        tip = r.refs.get(`refs/heads/${refName}`) ?? (/^[0-9a-f]{40}$/.test(refName) ? refName : undefined);
      } else {
        tip = r.refs.get(`refs/heads/${r.defaultBranch}`);
      }
      if (!tip) { sendJson(res, 200, []); return; }
      const list = commitList(r, tip).map((sha) => commitJson(r, sha)!).filter(Boolean);
      sendJson(res, 200, list);
      return;
    }
    if (head === 'commits' && method === 'GET' && tail.length === 2) {
      const c = commitJson(r, tail[1]);
      if (!c) { sendJson(res, 404, errJson('Not Found')); return; }
      sendJson(res, 200, c);
      return;
    }
    if (head === 'git' && tail[1] === 'refs' && method === 'POST' && tail.length === 2) {
      const b = (body ?? {}) as Record<string, unknown>;
      const ref = typeof b.ref === 'string' ? b.ref : '';
      const sha = typeof b.sha === 'string' ? b.sha : '';
      if (!ref.startsWith('refs/') || !/^[0-9a-f]{40}$/.test(sha) || !r.objects.has(sha)) {
        sendJson(res, 422, errJson('Invalid ref or sha'));
        return;
      }
      if (r.refs.has(ref)) {
        sendJson(res, 422, errJson('Reference already exists'));
        return;
      }
      r.refs.set(ref, sha);
      touchGit(r);
      sendJson(res, 201, { ref, node_id: globalId('ref', r.id, ref), url: `/api/v3/repos/${owner}/${name}/git/refs/${encodeURIComponent(ref)}`, object: { sha, type: r.objects.get(sha)!.type, url: '' } });
      return;
    }
    if (head === 'git' && tail[1] === 'refs' && method === 'GET' && tail.length >= 3) {
      // /git/refs[/heads/<branch>] — branch may contain slashes (joined).
      const refPath = tail.slice(2).join('/');
      const refName = refPath.startsWith('refs/') ? refPath : `refs/${refPath}`;
      const sha = r.refs.get(refName);
      if (!sha) { sendJson(res, 404, errJson('Not Found')); return; }
      sendJson(res, 200, { ref: refName, node_id: globalId('ref', r.id, refName), url: '', object: { sha, type: r.objects.get(sha)?.type ?? 'commit', url: '' } });
      return;
    }
    if (head === 'git' && tail[1] === 'refs' && method === 'GET' && tail.length === 2) {
      sendJsonBuf(res, 200, refsBuf(r));
      return;
    }
    if (head === 'contents') {
      const path = tail.slice(1).join('/');
      if (method === 'GET') {
        const refName = query.get('ref');
        let tip: string | undefined;
        if (refName) tip = r.refs.get(`refs/heads/${refName}`) ?? (/^[0-9a-f]{40}$/.test(refName) ? refName : undefined);
        else tip = r.refs.get(`refs/heads/${r.defaultBranch}`);
        if (!tip) { sendJson(res, 404, errJson('Not Found')); return; }
        if (path === '') {
          const files = filesAt(r, tip);
          const top = new Map<string, { oid: string; mode: string; dir: boolean }>();
          for (const [p, e] of files) {
            const first = p.split('/')[0];
            if (!top.has(first)) top.set(first, { oid: e.oid, mode: e.mode, dir: p.includes('/') });
          }
          // Rebuild directory entries properly from the root tree.
          const commit = r.objects.get(tip);
          if (!commit) { sendJson(res, 404, errJson('Not Found')); return; }
          const rootTree = parseCommit(commit.data).tree;
          const treeObj = r.objects.get(rootTree);
          const entries = treeObj ? parseTree(treeObj.data).map((e) => ({
            type: e.mode === '40000' ? 'dir' : 'file', name: e.name, path: e.name, sha: e.oid,
            size: e.mode === '40000' ? 0 : (r.objects.get(e.oid)?.data.length ?? 0),
            url: `/api/v3/repos/${owner}/${name}/contents/${encodeURIComponent(e.name)}`,
          })) : [];
          sendJson(res, 200, entries);
          return;
        }
        const files = filesAt(r, tip);
        const file = files.get(path);
        if (file) {
          const data = r.objects.get(file.oid)?.data ?? Buffer.alloc(0);
          sendJson(res, 200, {
            type: 'file', encoding: 'base64', content: b64e(data), sha: file.oid, size: data.length,
            name: path.split('/').pop(), path,
            url: `/api/v3/repos/${owner}/${name}/contents/${path}`,
            html_url: `https://example.invalid/${owner}/${name}/blob/${r.defaultBranch}/${path}`,
            git_url: '', download_url: '',
          });
          return;
        }
        // Directory?
        const prefix = `${path}/`;
        const children = new Map<string, { oid: string; mode: string }>();
        for (const [p, e] of files) {
          if (!p.startsWith(prefix)) continue;
          const rest = p.slice(prefix.length);
          const first = rest.split('/')[0];
          if (!children.has(first)) children.set(first, { oid: e.oid, mode: rest.includes('/') ? '40000' : e.mode });
        }
        if (children.size > 0) {
          const entries = [...children.entries()].sort(([a], [b]) => (a < b ? -1 : 1)).map(([n, e]) => ({
            type: e.mode === '40000' ? 'dir' : 'file', name: n, path: `${path}/${n}`, sha: e.oid,
            size: e.mode === '40000' ? 0 : (r.objects.get(e.oid)?.data.length ?? 0),
            url: `/api/v3/repos/${owner}/${name}/contents/${path}/${n}`,
          }));
          sendJson(res, 200, entries);
          return;
        }
        sendJson(res, 404, errJson('Not Found'));
        return;
      }
      if (method === 'PUT') {
        const b = (body ?? {}) as Record<string, unknown>;
        const message = typeof b.message === 'string' ? b.message : 'update file';
        const contentB64 = typeof b.content === 'string' ? b.content : '';
        const branch = typeof b.branch === 'string' && b.branch !== '' ? b.branch : r.defaultBranch;
        const sha = typeof b.sha === 'string' ? b.sha : undefined;
        let content: Buffer;
        try { content = b64d(contentB64); } catch { sendJson(res, 422, errJson('Invalid base64 content')); return; }
        const refName = `refs/heads/${branch}`;
        const tip = r.refs.get(refName) ?? r.refs.get(`refs/heads/${r.defaultBranch}`);
        const files = tip ? filesAt(r, tip) : new Map<string, { oid: string; mode: string }>();
        const existing = files.get(path);
        if (existing && sha !== undefined && sha !== existing.oid) {
          sendJson(res, 409, errJson('sha does not match the current blob'));
          return;
        }
        const { commitSha, blobSha, created } = commitFile(r, branch, path, content, message);
        sendJson(res, created ? 201 : 200, {
          content: { name: path.split('/').pop(), path, sha: blobSha, size: content.length, encoding: 'base64', type: 'file' },
          commit: { sha: commitSha, message },
        });
        return;
      }
      if (method === 'DELETE') {
        const b = (body ?? {}) as Record<string, unknown>;
        const message = typeof b.message === 'string' ? b.message : 'delete file';
        const branch = typeof b.branch === 'string' && b.branch !== '' ? b.branch : r.defaultBranch;
        const sha = typeof b.sha === 'string' ? b.sha : undefined;
        const refName = `refs/heads/${branch}`;
        const tip = r.refs.get(refName);
        if (!tip) { sendJson(res, 404, errJson('Not Found')); return; }
        const files = filesAt(r, tip);
        const existing = files.get(path);
        if (!existing) { sendJson(res, 404, errJson('Not Found')); return; }
        if (sha !== undefined && sha !== existing.oid) {
          sendJson(res, 409, errJson('sha does not match the current blob'));
          return;
        }
        files.delete(path);
        const tree = buildTreeFromFiles(r, files);
        const commitSha = putObj(r, 'commit', buildCommit(tree, [tip], message));
        r.refs.set(refName, commitSha);
        touchGit(r);
        sendJson(res, 200, { commit: { sha: commitSha, message } });
        return;
      }
      sendJson(res, 404, errJson('Not Found'));
      return;
    }
    if (head === 'labels') {
      if (method === 'GET') {
        sendJson(res, 200, [...r.labels.values()].map((l) => ({ name: l.name, color: l.color, description: '', default: false })));
        return;
      }
      if (method === 'POST') {
        const b = (body ?? {}) as Record<string, unknown>;
        const name = typeof b.name === 'string' ? b.name : '';
        if (name === '') { sendJson(res, 422, errJson('Invalid label')); return; }
        const color = typeof b.color === 'string' ? b.color.replace(/^#/, '') : 'ededed';
        r.labels.set(name, { name, color });
        for (const i of r.issues) touchIssue(i);
        sendJson(res, 201, { name, color, description: '', default: false, url: `/api/v3/repos/${owner}/${name}/labels/${encodeURIComponent(name)}` });
        return;
      }
      sendJson(res, 404, errJson('Not Found'));
      return;
    }
    if (head === 'issues') {
      if (tail.length === 1) {
        if (method === 'GET') {
          const state = query.get('state') ?? 'open';
          const sort = query.get('sort') ?? 'created';
          const direction = query.get('direction') ?? 'desc';
          let list = r.issues.slice();
          if (state !== 'all') list = list.filter((i) => i.state === state);
          if (sort === 'created') {
            list.sort((a, b) => (direction === 'asc' ? a.number - b.number : b.number - a.number));
          }
          const { page } = paginate(list, query, req, `/api/v3/repos/${owner}/${name}/issues`);
          sendJson(res, 200, page.map((i) => issueJson(r, i)));
          return;
        }
        if (method === 'POST') {
          const b = (body ?? {}) as Record<string, unknown>;
          const title = typeof b.title === 'string' ? b.title : '';
          const bodyText = typeof b.body === 'string' ? b.body : '';
          const rawLabels = Array.isArray(b.labels) ? b.labels : [];
          const labels: string[] = [];
          for (const l of rawLabels) {
            const name = typeof l === 'string' ? l : (l as Record<string, unknown>)?.name;
            if (typeof name === 'string') labels.push(name);
          }
          for (const name of labels) {
            if (!r.labels.has(name)) {
              sendJson(res, 422, errJson(`Label does not exist: ${name}`));
              return;
            }
          }
          const issue = createIssue(r, title, bodyText, labels);
          sendJsonBuf(res, 201, issueJsonBuf(r, issue));
          return;
        }
        sendJson(res, 404, errJson('Not Found'));
        return;
      }
      const num = parseInt(tail[1], 10);
      const issue = Number.isFinite(num) ? r.issues.find((i) => i.number === num) : undefined;
      if (tail.length === 2) {
        if (!issue) { sendJson(res, 404, errJson('Not Found')); return; }
        if (method === 'GET') { sendJsonBuf(res, 200, issueJsonBuf(r, issue)); return; }
        if (method === 'PATCH') {
          const b = (body ?? {}) as Record<string, unknown>;
          if (typeof b.title === 'string') issue.title = b.title;
          if (typeof b.body === 'string') issue.body = b.body;
          if (b.state === 'open' || b.state === 'closed') issue.state = b.state;
          if (Array.isArray(b.labels)) {
            const names: string[] = [];
            for (const l of b.labels) {
              const name = typeof l === 'string' ? l : (l as Record<string, unknown>)?.name;
              if (typeof name === 'string') names.push(name);
            }
            issue.labels = names;
          }
          issue.updatedAt = FIXED_TS;
          touchIssue(issue);
          touchRepoJson(r); // state may have moved open_issues_count
          sendJsonBuf(res, 200, issueJsonBuf(r, issue));
          return;
        }
        sendJson(res, 404, errJson('Not Found'));
        return;
      }
      if (tail.length === 3 && tail[2] === 'comments') {
        if (!issue) { sendJson(res, 404, errJson('Not Found')); return; }
        if (method === 'GET') {
          const list = (r.comments.get(issue.number) ?? []).map((c) => ({
            id: c.id, node_id: c.nodeId, body: c.body,
            user: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
            created_at: c.createdAt, updated_at: c.createdAt,
            html_url: `https://example.invalid/${owner}/${name}/issues/${issue.number}#issuecomment-${c.id}`,
            url: `/api/v3/repos/${owner}/${name}/issues/comments/${c.id}`,
          }));
          sendJson(res, 200, list);
          return;
        }
        if (method === 'POST') {
          const b = (body ?? {}) as Record<string, unknown>;
          const text = typeof b.body === 'string' ? b.body : '';
          const comment: Comment = { id: nextCommentSeq++, nodeId: '', body: text, createdAt: FIXED_TS };
          comment.nodeId = globalId('comment', r.id, issue.number, comment.id);
          const list = r.comments.get(issue.number) ?? [];
          list.push(comment);
          r.comments.set(issue.number, list);
          touchIssue(issue);
          sendJson(res, 201, {
            id: comment.id, node_id: comment.nodeId, body: text,
            user: { login: VIEWER_LOGIN, id: VIEWER_ID, type: 'User' },
            created_at: FIXED_TS, updated_at: FIXED_TS,
            html_url: `https://example.invalid/${owner}/${name}/issues/${issue.number}#issuecomment-${comment.id}`,
            url: `/api/v3/repos/${owner}/${name}/issues/comments/${comment.id}`,
          });
          return;
        }
        sendJson(res, 404, errJson('Not Found'));
        return;
      }
      sendJson(res, 404, errJson('Not Found'));
      return;
    }
    if (head === 'pulls') {
      if (tail.length === 1) {
        if (method === 'GET') {
          const state = query.get('state') ?? 'open';
          let list = r.pulls.slice();
          if (state !== 'all') list = list.filter((p) => p.state === state);
          list.sort((a, b) => b.number - a.number);
          const { page } = paginate(list, query, req, `/api/v3/repos/${owner}/${name}/pulls`);
          sendJson(res, 200, page.map((p) => prJson(r, p)));
          return;
        }
        if (method === 'POST') {
          const b = (body ?? {}) as Record<string, unknown>;
          const title = typeof b.title === 'string' ? b.title : '';
          const bodyText = typeof b.body === 'string' ? b.body : '';
          const headRef = typeof b.head === 'string' ? b.head : '';
          const baseRef = typeof b.base === 'string' ? b.base : r.defaultBranch;
          const { pr, error } = createPR(r, title, bodyText, headRef, baseRef);
          if (!pr) { sendJson(res, 422, errJson(error ?? 'cannot create pull request')); return; }
          sendJson(res, 201, prJson(r, pr));
          return;
        }
        sendJson(res, 404, errJson('Not Found'));
        return;
      }
      const num = parseInt(tail[1], 10);
      const pr = Number.isFinite(num) ? r.pulls.find((p) => p.number === num) : undefined;
      if (tail.length === 2) {
        if (!pr) { sendJson(res, 404, errJson('Not Found')); return; }
        if (method === 'GET') { sendJson(res, 200, prJson(r, pr)); return; }
        sendJson(res, 404, errJson('Not Found'));
        return;
      }
      if (tail.length === 3 && tail[2] === 'files') {
        if (!pr) { sendJson(res, 404, errJson('Not Found')); return; }
        if (method === 'GET') {
          sendJson(res, 200, pr.files.map((f) => ({
            sha: '', filename: f.filename, status: f.status, additions: f.additions, deletions: f.deletions,
            changes: f.changes, blob_url: '', raw_url: '', contents_url: '',
          })));
          return;
        }
        sendJson(res, 404, errJson('Not Found'));
        return;
      }
      if (tail.length === 3 && tail[2] === 'merge') {
        if (!pr) { sendJson(res, 404, errJson('Not Found')); return; }
        if (method === 'PUT') {
          if (pr.merged) {
            sendJson(res, 200, { sha: pr.mergeCommitSha, merged: true, message: 'Pull request successfully merged' });
            return;
          }
          const sha = mergePR(r, pr);
          if (sha === null) {
            sendJson(res, 405, errJson('Merge conflict'));
            return;
          }
          sendJson(res, 200, { sha, merged: true, message: 'Pull request successfully merged' });
          return;
        }
        sendJson(res, 404, errJson('Not Found'));
        return;
      }
      sendJson(res, 404, errJson('Not Found'));
      return;
    }
    sendJson(res, 404, errJson('Not Found'));
    return;
  }
  sendJson(res, 404, errJson('Not Found'));
}

// -------------------------------------------------------------------------------------------
// GraphQL v4 (hand-written parser + executor for the documents gh / github-mcp-server send)
// -------------------------------------------------------------------------------------------

type GQLValue = unknown;
interface GQLField {
  alias: string; name: string; args: Record<string, GQLValue>; children: GQLField[] | null; fragmentOn: string | null;
}
interface GQLOp { type: 'query' | 'mutation'; name: string | null; vars: Record<string, GQLValue>; fields: GQLField[] }

function gqlTokenize(src: string): string[] {
  const tokens: string[] = [];
  let i = 0;
  while (i < src.length) {
    const ch = src[i];
    if (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r' || ch === ',') { i++; continue; }
    if (ch === '#') { while (i < src.length && src[i] !== '\n') i++; continue; }
    if (ch === '.' && src.slice(i, i + 3) === '...') { tokens.push('...'); i += 3; continue; }
    if ('{}()[]:!=$@='.includes(ch)) { tokens.push(ch); i++; continue; }
    if (ch === '"') {
      let j = i + 1;
      let out = '';
      while (j < src.length && src[j] !== '"') {
        if (src[j] === '\\' && j + 1 < src.length) {
          const esc = src[j + 1];
          if (esc === 'n') out += '\n';
          else if (esc === 't') out += '\t';
          else if (esc === 'r') out += '\r';
          else out += esc;
          j += 2;
        } else { out += src[j]; j++; }
      }
      tokens.push(JSON.stringify(out));
      i = j + 1;
      continue;
    }
    if (/[0-9-]/.test(ch)) {
      let j = i;
      while (j < src.length && /[0-9.eE+-]/.test(src[j])) j++;
      tokens.push(src.slice(i, j));
      i = j;
      continue;
    }
    if (/[A-Za-z_]/.test(ch)) {
      let j = i;
      while (j < src.length && /[A-Za-z0-9_]/.test(src[j])) j++;
      tokens.push(src.slice(i, j));
      i = j;
      continue;
    }
    i++;
  }
  return tokens;
}

class GQLParser {
  private pos = 0;
  private tokens: string[];
  private variables: Record<string, GQLValue>;
  constructor(tokens: string[], variables: Record<string, GQLValue>) {
    this.tokens = tokens;
    this.variables = variables;
  }
  peek(): string { return this.tokens[this.pos] ?? ''; }
  next(): string { return this.tokens[this.pos++] ?? ''; }
  expect(t: string): void { if (this.next() !== t) throw new Error(`expected ${t}`); }

  parseValue(): GQLValue {
    const tok = this.next();
    if (tok === '$') return { __var: this.next() };
    if (tok === '[') {
      const arr: GQLValue[] = [];
      while (this.peek() !== ']') arr.push(this.parseValue());
      this.expect(']');
      return arr;
    }
    if (tok === '{') {
      const obj: Record<string, GQLValue> = {};
      while (this.peek() !== '}') {
        const key = this.next();
        this.expect(':');
        obj[key] = this.parseValue();
      }
      this.expect('}');
      return obj;
    }
    if (tok.startsWith('"')) return JSON.parse(tok) as string;
    if (tok === 'true') return true;
    if (tok === 'false') return false;
    if (tok === 'null') return null;
    if (/^-?\d+$/.test(tok)) return parseInt(tok, 10);
    if (/^-?\d*\.\d+([eE][+-]?\d+)?$/.test(tok)) return parseFloat(tok);
    return { __enum: tok };
  }

  resolveValue(v: GQLValue): unknown {
    if (v !== null && typeof v === 'object') {
      if (Array.isArray(v)) return v.map((x) => this.resolveValue(x));
      const rec = v as Record<string, unknown>;
      if ('__var' in rec) return this.variables[String(rec.__var)] ?? null;
      if ('__enum' in rec) return String(rec.__enum);
      const out: Record<string, unknown> = {};
      for (const [k, val] of Object.entries(rec)) out[k] = this.resolveValue(val);
      return out;
    }
    return v;
  }

  parseArgs(): Record<string, GQLValue> {
    const args: Record<string, GQLValue> = {};
    if (this.peek() !== '(') return args;
    this.expect('(');
    while (this.peek() !== ')') {
      const key = this.next();
      this.expect(':');
      args[key] = this.resolveValue(this.parseValue());
    }
    this.expect(')');
    return args;
  }

  skipDirectives(): void {
    while (this.peek() === '@') {
      this.next();
      this.next();
      if (this.peek() === '(') {
        let depth = 0;
        while (this.pos < this.tokens.length) {
          const t = this.next();
          if (t === '(') depth++;
          else if (t === ')') { depth--; if (depth === 0) break; }
        }
      }
    }
  }

  parseSelection(): GQLField[] {
    const fields: GQLField[] = [];
    this.expect('{');
    while (this.peek() !== '}' && this.pos < this.tokens.length) {
      if (this.peek() === '...') {
        this.next();
        let fragmentOn: string | null = null;
        if (this.peek() === 'on') {
          this.next();
          fragmentOn = this.next();
        } else {
          // Named fragment spread: unsupported in the documents used; skip its name.
          this.next();
          continue;
        }
        this.skipDirectives();
        const children = this.parseSelection();
        for (const c of children) fields.push({ ...c, fragmentOn });
        continue;
      }
      let name = this.next();
      let alias = name;
      if (this.peek() === ':') {
        this.next();
        alias = name;
        name = this.next();
      }
      const args = this.parseArgs();
      this.skipDirectives();
      const children = this.peek() === '{' ? this.parseSelection() : null;
      fields.push({ alias, name, args, children, fragmentOn: null });
    }
    this.expect('}');
    return fields;
  }

  parseDocument(): GQLOp[] {
    const ops: GQLOp[] = [];
    while (this.pos < this.tokens.length) {
      const tok = this.peek();
      if (tok === 'fragment') {
        // Skip a fragment definition.
        this.next(); this.next();
        if (this.peek() === 'on') { this.next(); this.next(); }
        this.skipDirectives();
        this.parseSelection();
        continue;
      }
      let type: 'query' | 'mutation' = 'query';
      if (tok === 'query' || tok === 'mutation') { type = tok as 'query' | 'mutation'; this.next(); }
      let name: string | null = null;
      if (this.peek() !== '{' && this.peek() !== '(') name = this.next();
      if (this.peek() === '(') {
        // Variable definitions: ($x: Type = default, ...)
        this.expect('(');
        while (this.peek() !== ')') {
          this.next();
          if (this.peek() === ':') {
            this.next();
            // type may be [T!]! etc; consume tokens until ',' or ')' at depth 0.
            let depth = 0;
            while (this.pos < this.tokens.length) {
              const t = this.peek();
              if (t === '[') depth++;
              else if (t === ']') depth--;
              else if (depth === 0 && (t === ',' || t === ')')) break;
              this.next();
            }
          }
          if (this.peek() === '=') { this.next(); this.parseValue(); }
          if (this.peek() === ',') this.next();
        }
        this.expect(')');
      }
      this.skipDirectives();
      const fields = this.parseSelection();
      ops.push({ type, name, vars: {}, fields });
    }
    return ops;
  }
}

const TYPE_FIELD_NAMES: Record<string, string[]> = {
  Issue: ['id', 'number', 'title', 'state', 'body', 'url', 'createdAt', 'updatedAt', 'closedAt', 'author', 'labels', 'comments'],
  PullRequest: ['id', 'number', 'title', 'body', 'state', 'merged', 'mergedAt', 'headRefName', 'baseRefName', 'headRefOid', 'baseRefOid', 'mergeable', 'files', 'author', 'url', 'createdAt', 'updatedAt'],
  Repository: ['id', 'databaseId', 'name', 'nameWithOwner', 'owner', 'defaultBranchRef', 'viewerPermission', 'hasIssuesEnabled', 'issue', 'issues', 'pullRequests', 'pullRequest', 'description', 'url', 'createdAt', 'updatedAt', 'isPrivate'],
  User: ['id', 'login', 'name', 'email', 'url'],
  IssueConnection: ['nodes', 'edges', 'pageInfo', 'totalCount'],
  PullRequestConnection: ['nodes', 'edges', 'pageInfo', 'totalCount'],
  FileConnection: ['nodes', 'edges', 'pageInfo', 'totalCount'],
  PageInfo: ['hasNextPage', 'hasPreviousPage', 'startCursor', 'endCursor'],
  BranchRef: ['name', 'prefix', 'target'],
  Label: ['name', 'color', 'description'],
  Comment: ['id', 'body', 'author', 'createdAt', 'updatedAt'],
  Type: ['name', 'kind', 'fields', 'enumValues', 'description'],
  TypeField: ['name', 'description'],
  EnumValue: ['name', 'description'],
  Schema: ['types', 'queryType', 'mutationType'],
};

const TYPE_ENUM_VALUES: Record<string, string[]> = {
  SearchType: ['ISSUE', 'REPOSITORY', 'USER', 'DISCUSSION', 'COMMIT', 'CODE'],
  IssueState: ['OPEN', 'CLOSED'],
  IssueOrderField: ['CREATED_AT', 'UPDATED_AT', 'COMMENTS'],
  OrderDirection: ['ASC', 'DESC'],
  MergeableState: ['MERGEABLE', 'CONFLICTING', 'UNKNOWN'],
  PullRequestState: ['OPEN', 'CLOSED', 'MERGED'],
  PullRequestMergeMethod: ['MERGE', 'SQUASH', 'REBASE'],
  RepositoryPermission: ['ADMIN', 'MAINTAIN', 'WRITE', 'TRIAGE', 'READ'],
};

interface GQLNode { kind: string; value: unknown }

function findRepoByNodeId(nodeId: string): Repo | null {
  for (const r of repos.values()) if (r.nodeId === nodeId) return r;
  return null;
}

function decodeNodeId(nodeId: string): { kind: string; repoId: number; number: number } | null {
  try {
    const text = Buffer.from(nodeId, 'base64').toString('utf8');
    const parts = text.split(':');
    if (parts[0] === 'issue' || parts[0] === 'pr') {
      return { kind: parts[0], repoId: parseInt(parts[1], 10), number: parseInt(parts[2], 10) };
    }
    if (parts[0] === 'repo') return { kind: 'repo', repoId: -1, number: -1 };
  } catch { /* ignore */ }
  return null;
}

function cursorFor(n: number): string { return b64e(`cursor:${n}`); }
function numberFromCursor(c: string | null): number | null {
  if (!c) return null;
  try {
    const text = Buffer.from(c, 'base64').toString('utf8');
    const m = /^cursor:(\d+)$/.exec(text);
    return m ? parseInt(m[1], 10) : null;
  } catch { return null; }
}

function issueConnection(r: Repo, args: Record<string, unknown>): { nodes: Issue[]; hasNextPage: boolean; endCursor: string | null } {
  let list = r.issues.slice();
  const states = args.states as string[] | undefined;
  if (Array.isArray(states) && states.length) {
    const wanted = new Set(states.map((s) => String(s).toLowerCase()));
    list = list.filter((i) => wanted.has(i.state));
  }
  const orderBy = args.orderBy as Record<string, unknown> | undefined;
  const direction = orderBy && typeof orderBy.direction === 'string' ? String(orderBy.direction) : 'ASC';
  list.sort((a, b) => (direction === 'DESC' ? b.number - a.number : a.number - b.number));
  const afterNum = numberFromCursor(typeof args.after === 'string' ? args.after : null);
  if (afterNum !== null) {
    const idx = list.findIndex((i) => i.number === afterNum);
    if (idx >= 0) list = list.slice(idx + 1);
  }
  const first = typeof args.first === 'number' ? args.first : list.length;
  const nodes = list.slice(0, first);
  const hasNextPage = list.length > nodes.length;
  return { nodes, hasNextPage, endCursor: nodes.length ? cursorFor(nodes[nodes.length - 1].number) : null };
}

function prConnection(r: Repo, args: Record<string, unknown>): { nodes: PR[]; hasNextPage: boolean; endCursor: string | null } {
  let list = r.pulls.slice();
  const states = args.states as string[] | undefined;
  if (Array.isArray(states) && states.length) {
    const wanted = new Set(states.map((s) => String(s).toLowerCase()));
    list = list.filter((p) => wanted.has(p.state === 'open' ? 'open' : p.merged ? 'merged' : 'closed'));
  } else {
    list = list.filter((p) => p.state === 'open');
  }
  list.sort((a, b) => b.number - a.number);
  const afterNum = numberFromCursor(typeof args.after === 'string' ? args.after : null);
  if (afterNum !== null) {
    const idx = list.findIndex((p) => p.number === afterNum);
    if (idx >= 0) list = list.slice(idx + 1);
  }
  const first = typeof args.first === 'number' ? args.first : list.length;
  const nodes = list.slice(0, first);
  const hasNextPage = list.length > nodes.length;
  return { nodes, hasNextPage, endCursor: nodes.length ? cursorFor(nodes[nodes.length - 1].number) : null };
}

class GQLExecutor {
  errors: { message: string }[] = [];

  private typeFields(kind: string): string[] | null {
    return TYPE_FIELD_NAMES[kind] ?? null;
  }

  execFields(kind: string, value: unknown, fields: GQLField[]): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    const allowed = this.typeFields(kind);
    for (const f of fields) {
      if (f.fragmentOn !== null && f.fragmentOn !== kind) continue;
      if (allowed !== null && !allowed.includes(f.name)) {
        this.errors.push({ message: `Cannot query field "${f.name}" on type "${kind}".` });
        continue;
      }
      const resolved = this.resolveField(kind, value, f);
      if (resolved !== undefined) out[f.alias] = resolved;
    }
    return out;
  }

  private child(kind: string, value: unknown, f: GQLField): unknown {
    if (!f.children) return value;
    return this.execFields(kind, value, f.children);
  }

  private resolveField(kind: string, value: unknown, f: GQLField): unknown {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const v: any = value;
    switch (kind) {
      case 'Query': {
        if (f.name === 'viewer') return this.child('User', { login: VIEWER_LOGIN, id: VIEWER_ID, name: VIEWER_NAME, email: VIEWER_EMAIL }, f);
        if (f.name === 'repository') {
          const owner = String(f.args.owner ?? '');
          const name = String(f.args.name ?? '');
          const r = findRepo(owner, name);
          if (!r) return null;
          return this.child('Repository', r, f);
        }
        if (f.name === 'node') {
          const id = String(f.args.id ?? '');
          const decoded = decodeNodeId(id);
          if (decoded && decoded.kind === 'issue') {
            for (const r of repos.values()) {
              if (r.id === decoded.repoId) {
                const issue = r.issues.find((i) => i.number === decoded.number);
                if (issue) return this.child('Issue', { repo: r, issue }, f);
              }
            }
            return null;
          }
          if (decoded && decoded.kind === 'pr') {
            for (const r of repos.values()) {
              if (r.id === decoded.repoId) {
                const pr = r.pulls.find((p) => p.number === decoded.number);
                if (pr) return this.child('PullRequest', { repo: r, pr }, f);
              }
            }
            return null;
          }
          const r = findRepoByNodeId(id);
          if (!r) return null;
          return this.child('Repository', r, f);
        }
        if (f.name === '__type') {
          const name = String(f.args.name ?? '');
          return this.child('Type', name, f);
        }
        if (f.name === '__schema') return this.child('Schema', {}, f);
        return undefined;
      }
      case 'Mutation': {
        if (f.name === 'createIssue') {
          const input = (f.args.input ?? {}) as Record<string, unknown>;
          const r = findRepoByNodeId(String(input.repositoryId ?? ''));
          if (!r) { this.errors.push({ message: 'Repository not found.' }); return null; }
          const issue = createIssue(r, String(input.title ?? ''), typeof input.body === 'string' ? input.body : '', []);
          return this.child('CreateIssuePayload', { repo: r, issue }, f);
        }
        if (f.name === 'closeIssue') {
          const input = (f.args.input ?? {}) as Record<string, unknown>;
          const decoded = decodeNodeId(String(input.issueId ?? ''));
          let found: { repo: Repo; issue: Issue } | null = null;
          if (decoded && decoded.kind === 'issue') {
            for (const r of repos.values()) {
              if (r.id !== decoded.repoId) continue;
              const issue = r.issues.find((i) => i.number === decoded.number);
              if (issue) { found = { repo: r, issue }; break; }
            }
          }
          if (!found) { this.errors.push({ message: 'Issue not found.' }); return null; }
          found.issue.state = 'closed';
          found.issue.updatedAt = FIXED_TS;
          touchIssue(found.issue);
          touchRepoJson(found.repo); // open_issues_count moved
          return this.child('CloseIssuePayload', found, f);
        }
        if (f.name === 'createPullRequest') {
          const input = (f.args.input ?? {}) as Record<string, unknown>;
          const r = findRepoByNodeId(String(input.repositoryId ?? ''));
          if (!r) { this.errors.push({ message: 'Repository not found.' }); return null; }
          const { pr, error } = createPR(r, String(input.title ?? ''), typeof input.body === 'string' ? input.body : '', String(input.headRefName ?? ''), String(input.baseRefName ?? ''));
          if (!pr) { this.errors.push({ message: error ?? 'Cannot create pull request.' }); return null; }
          return this.child('CreatePullRequestPayload', { repo: r, pr }, f);
        }
        if (f.name === 'mergePullRequest') {
          const input = (f.args.input ?? {}) as Record<string, unknown>;
          const decoded = decodeNodeId(String(input.pullRequestId ?? ''));
          let found: { repo: Repo; pr: PR } | null = null;
          if (decoded && decoded.kind === 'pr') {
            for (const r of repos.values()) {
              if (r.id !== decoded.repoId) continue;
              const pr = r.pulls.find((p) => p.number === decoded.number);
              if (pr) { found = { repo: r, pr }; break; }
            }
          }
          if (!found) { this.errors.push({ message: 'Pull request not found.' }); return null; }
          const sha = mergePR(found.repo, found.pr);
          if (sha === null) { this.errors.push({ message: 'Merge conflict.' }); }
          return this.child('MergePullRequestPayload', found, f);
        }
        if (f.name === 'addComment') {
          const input = (f.args.input ?? {}) as Record<string, unknown>;
          const decoded = decodeNodeId(String(input.subjectId ?? ''));
          let found: { repo: Repo; issue: Issue } | null = null;
          if (decoded && decoded.kind === 'issue') {
            for (const r of repos.values()) {
              if (r.id !== decoded.repoId) continue;
              const issue = r.issues.find((i) => i.number === decoded.number);
              if (issue) { found = { repo: r, issue }; break; }
            }
          }
          if (!found) { this.errors.push({ message: 'Subject not found.' }); return null; }
          const comment: Comment = { id: nextCommentSeq++, nodeId: '', body: String(input.body ?? ''), createdAt: FIXED_TS };
          comment.nodeId = globalId('comment', found.repo.id, found.issue.number, comment.id);
          const list = found.repo.comments.get(found.issue.number) ?? [];
          list.push(comment);
          found.repo.comments.set(found.issue.number, list);
          touchIssue(found.issue);
          return this.child('AddCommentPayload', { comment }, f);
        }
        return undefined;
      }
      case 'CreateIssuePayload': {
        if (f.name === 'issue') return this.child('Issue', { repo: v.repo, issue: v.issue }, f);
        if (f.name === 'clientMutationId') return null;
        return undefined;
      }
      case 'CloseIssuePayload': {
        if (f.name === 'issue') return this.child('Issue', { repo: v.repo, issue: v.issue }, f);
        if (f.name === 'clientMutationId') return null;
        return undefined;
      }
      case 'CreatePullRequestPayload': {
        if (f.name === 'pullRequest') return this.child('PullRequest', { repo: v.repo, pr: v.pr }, f);
        if (f.name === 'clientMutationId') return null;
        return undefined;
      }
      case 'MergePullRequestPayload': {
        if (f.name === 'pullRequest') return this.child('PullRequest', { repo: v.repo, pr: v.pr }, f);
        if (f.name === 'clientMutationId') return null;
        return undefined;
      }
      case 'AddCommentPayload': {
        if (f.name === 'commentEdge') {
          const node = { id: v.comment.nodeId, body: v.comment.body };
          if (!f.children) return { node };
          const out: Record<string, unknown> = {};
          for (const c of f.children) {
            if (c.name === 'node') out.node = this.child('Comment', node, c);
            else if (c.name === 'cursor') out.cursor = cursorFor(0);
          }
          return out;
        }
        if (f.name === 'clientMutationId') return null;
        return undefined;
      }
      case 'User': {
        if (f.name === 'login') return v.login;
        if (f.name === 'id') return globalId('user', String(v.login ?? ''));
        if (f.name === 'name') return v.name ?? null;
        if (f.name === 'email') return v.email ?? null;
        if (f.name === 'url') return `https://example.invalid/${v.login}`;
        return undefined;
      }
      case 'Repository': {
        const r = value as Repo;
        switch (f.name) {
          case 'id': return r.nodeId;
          case 'databaseId': return r.id;
          case 'name': return r.name;
          case 'nameWithOwner': return `${r.owner}/${r.name}`;
          case 'owner': return this.child('User', { login: r.owner, id: VIEWER_ID, name: VIEWER_NAME, email: VIEWER_EMAIL }, f);
          case 'defaultBranchRef': {
            if (!r.refs.get(`refs/heads/${r.defaultBranch}`)) return null;
            return this.child('BranchRef', { name: r.defaultBranch }, f);
          }
          case 'viewerPermission': return 'ADMIN';
          case 'hasIssuesEnabled': return true;
          case 'description': return r.description;
          case 'url': return `https://example.invalid/${r.owner}/${r.name}`;
          case 'createdAt': case 'updatedAt': return FIXED_TS;
          case 'isPrivate': return r.isPrivate;
          case 'issue': {
            const n = f.args.number as number;
            const issue = r.issues.find((i) => i.number === n);
            if (!issue) return null;
            return this.child('Issue', { repo: r, issue }, f);
          }
          case 'issues': {
            const conn = issueConnection(r, f.args);
            return this.child('IssueConnection', { repo: r, nodes: conn.nodes, hasNextPage: conn.hasNextPage, endCursor: conn.endCursor }, f);
          }
          case 'pullRequests': {
            const conn = prConnection(r, f.args);
            return this.child('PullRequestConnection', { repo: r, nodes: conn.nodes, hasNextPage: conn.hasNextPage, endCursor: conn.endCursor }, f);
          }
          case 'pullRequest': {
            const n = f.args.number as number;
            const pr = r.pulls.find((p) => p.number === n);
            if (!pr) return null;
            return this.child('PullRequest', { repo: r, pr }, f);
          }
          default: return undefined;
        }
      }
      case 'BranchRef': {
        if (f.name === 'name') return v.name;
        if (f.name === 'prefix') return 'refs/heads/';
        if (f.name === 'target') return null;
        return undefined;
      }
      case 'Issue': {
        const wrapped = value as { repo: Repo; issue: Issue };
        const issue = wrapped.issue;
        switch (f.name) {
          case 'id': return issue.nodeId;
          case 'number': return issue.number;
          case 'title': return issue.title;
          case 'state': return issue.state === 'open' ? 'OPEN' : 'CLOSED';
          case 'body': return issue.body;
          case 'url': return `https://example.invalid/${wrapped.repo.owner}/${wrapped.repo.name}/issues/${issue.number}`;
          case 'createdAt': case 'updatedAt': return issue.createdAt;
          case 'closedAt': return issue.state === 'closed' ? FIXED_TS : null;
          case 'author': return this.child('User', { login: VIEWER_LOGIN, id: VIEWER_ID, name: VIEWER_NAME, email: VIEWER_EMAIL }, f);
          case 'labels': {
            const nodes = issue.labels.map((name) => ({ name, color: 'ededed', description: '' }));
            return this.child('LabelConnection', { nodes }, f);
          }
          case 'comments': {
            const nodes = (wrapped.repo.comments.get(issue.number) ?? []).map((c) => ({ id: c.nodeId, body: c.body, createdAt: c.createdAt, updatedAt: c.createdAt }));
            return this.child('CommentConnection', { nodes }, f);
          }
          default: return undefined;
        }
      }
      case 'LabelConnection': {
        if (f.name === 'nodes') return (v.nodes as unknown[]).map((n) => this.child('Label', n, f));
        if (f.name === 'totalCount') return (v.nodes as unknown[]).length;
        return undefined;
      }
      case 'CommentConnection': {
        if (f.name === 'nodes') return (v.nodes as unknown[]).map((n) => this.child('Comment', n, f));
        if (f.name === 'totalCount') return (v.nodes as unknown[]).length;
        return undefined;
      }
      case 'Label': {
        if (f.name === 'name') return v.name;
        if (f.name === 'color') return v.color;
        if (f.name === 'description') return v.description ?? '';
        return undefined;
      }
      case 'Comment': {
        if (f.name === 'id') return v.id;
        if (f.name === 'body') return v.body;
        if (f.name === 'createdAt' || f.name === 'updatedAt') return v.createdAt ?? FIXED_TS;
        if (f.name === 'author') return this.child('User', { login: VIEWER_LOGIN, id: VIEWER_ID, name: VIEWER_NAME, email: VIEWER_EMAIL }, f);
        return undefined;
      }
      case 'IssueConnection': {
        const wrapped = value as { repo: Repo; nodes: Issue[]; hasNextPage: boolean; endCursor: string | null };
        if (f.name === 'nodes') return wrapped.nodes.map((issue) => this.child('Issue', { repo: wrapped.repo, issue }, f));
        if (f.name === 'edges') return wrapped.nodes.map((issue) => ({ node: this.child('Issue', { repo: wrapped.repo, issue }, f), cursor: cursorFor(issue.number) }));
        if (f.name === 'pageInfo') return this.child('PageInfo', wrapped, f);
        if (f.name === 'totalCount') return wrapped.nodes.length;
        return undefined;
      }
      case 'PullRequestConnection': {
        const wrapped = value as { repo: Repo; nodes: PR[]; hasNextPage: boolean; endCursor: string | null };
        if (f.name === 'nodes') return wrapped.nodes.map((pr) => this.child('PullRequest', { repo: wrapped.repo, pr }, f));
        if (f.name === 'edges') return wrapped.nodes.map((pr) => ({ node: this.child('PullRequest', { repo: wrapped.repo, pr }, f), cursor: cursorFor(pr.number) }));
        if (f.name === 'pageInfo') return this.child('PageInfo', wrapped, f);
        if (f.name === 'totalCount') return wrapped.nodes.length;
        return undefined;
      }
      case 'FileConnection': {
        const wrapped = value as { nodes: { path: string; additions: number; deletions: number }[] };
        if (f.name === 'nodes') return wrapped.nodes.map((n) => this.child('PullRequestChangedFile', n, f));
        if (f.name === 'totalCount') return wrapped.nodes.length;
        if (f.name === 'pageInfo') return this.child('PageInfo', { nodes: wrapped.nodes, hasNextPage: false, endCursor: null }, f);
        return undefined;
      }
      case 'PullRequestChangedFile': {
        if (f.name === 'path') return v.path;
        if (f.name === 'additions') return v.additions;
        if (f.name === 'deletions') return v.deletions;
        if (f.name === 'changeType') return 'ADDED';
        return undefined;
      }
      case 'PageInfo': {
        const wrapped = value as { hasNextPage: boolean; endCursor: string | null; nodes?: unknown[] };
        if (f.name === 'hasNextPage') return wrapped.hasNextPage;
        if (f.name === 'hasPreviousPage') return false;
        if (f.name === 'endCursor') return wrapped.endCursor;
        if (f.name === 'startCursor') return wrapped.nodes && wrapped.nodes.length ? cursorFor(0) : null;
        return undefined;
      }
      case 'PullRequest': {
        const wrapped = value as { repo: Repo; pr: PR };
        const pr = wrapped.pr;
        switch (f.name) {
          case 'id': return pr.nodeId;
          case 'number': return pr.number;
          case 'title': return pr.title;
          case 'body': return pr.body;
          case 'state': return pr.merged ? 'MERGED' : pr.state === 'open' ? 'OPEN' : 'CLOSED';
          case 'merged': return pr.merged;
          case 'mergedAt': return pr.mergedAt;
          case 'headRefName': return pr.headRef;
          case 'baseRefName': return pr.baseRef;
          case 'headRefOid': return pr.headSha;
          case 'baseRefOid': return pr.baseSha;
          case 'mergeable': return prMergeable(wrapped.repo, pr);
          case 'files': {
            const nodes = pr.files.map((file) => ({ path: file.filename, additions: file.additions, deletions: file.deletions }));
            const first = typeof f.args.first === 'number' ? f.args.first : nodes.length;
            return this.child('FileConnection', { nodes: nodes.slice(0, first) }, f);
          }
          case 'author': return this.child('User', { login: VIEWER_LOGIN, id: VIEWER_ID, name: VIEWER_NAME, email: VIEWER_EMAIL }, f);
          case 'url': return `https://example.invalid/${wrapped.repo.owner}/${wrapped.repo.name}/pull/${pr.number}`;
          case 'createdAt': case 'updatedAt': return pr.createdAt;
          default: return undefined;
        }
      }
      case 'Type': {
        const name = value as string;
        if (f.name === 'name') return name;
        if (f.name === 'kind') return TYPE_ENUM_VALUES[name] ? 'ENUM' : 'OBJECT';
        if (f.name === 'description') return null;
        if (f.name === 'fields') {
          const names = TYPE_FIELD_NAMES[name] ?? [];
          const nodes = names.map((n) => ({ name: n, description: null }));
          if (!f.children) return nodes;
          return nodes.map((n) => this.child('TypeField', n, f));
        }
        if (f.name === 'enumValues') {
          const names = TYPE_ENUM_VALUES[name] ?? [];
          const nodes = names.map((n) => ({ name: n, description: null }));
          if (!f.children) return nodes;
          return nodes.map((n) => this.child('EnumValue', n, f));
        }
        return undefined;
      }
      case 'TypeField': case 'EnumValue': {
        if (f.name === 'name') return v.name;
        if (f.name === 'description') return v.description ?? null;
        return undefined;
      }
      case 'Schema': {
        if (f.name === 'types') return [];
        if (f.name === 'queryType') return this.child('Type', 'Query', f);
        if (f.name === 'mutationType') return this.child('Type', 'Mutation', f);
        return undefined;
      }
      default:
        return undefined;
    }
  }

  execute(op: GQLOp): Record<string, unknown> {
    this.errors = [];
    const rootKind = op.type === 'mutation' ? 'Mutation' : 'Query';
    const data = this.execFields(rootKind, {}, op.fields);
    const out: Record<string, unknown> = { data };
    if (this.errors.length) out.errors = this.errors;
    return out;
  }
}

function handleGraphQL(body: unknown): { status: number; payload: unknown } {
  const runOne = (entry: unknown): Record<string, unknown> => {
    const e = entry as Record<string, unknown>;
    const query = typeof e.query === 'string' ? e.query : '';
    const variables = (e.variables ?? {}) as Record<string, GQLValue>;
    const operationName = typeof e.operationName === 'string' ? e.operationName : null;
    try {
      const parser = new GQLParser(gqlTokenize(query), variables);
      const ops = parser.parseDocument();
      if (ops.length === 0) return { data: null, errors: [{ message: 'No operation found.' }] };
      let op = ops[0];
      if (operationName) {
        const found = ops.find((o) => o.name === operationName);
        if (found) op = found;
      }
      return new GQLExecutor().execute(op);
    } catch (err) {
      return { data: null, errors: [{ message: err instanceof Error ? err.message : 'GraphQL parse error' }] };
    }
  };
  if (Array.isArray(body)) return { status: 200, payload: body.map(runOne) };
  if (body !== null && typeof body === 'object') return { status: 200, payload: runOne(body) };
  return { status: 400, payload: { errors: [{ message: 'Invalid GraphQL body.' }] } };
}

// -------------------------------------------------------------------------------------------
// MCP (JSON-RPC 2.0, Streamable HTTP)
// -------------------------------------------------------------------------------------------

interface MCPToolDef { name: string; description: string; inputSchema: Record<string, unknown> }

const MCP_TOOLS: MCPToolDef[] = [
  { name: 'create_repo', description: 'Create a new repository for the authenticated user.', inputSchema: { type: 'object', properties: { name: { type: 'string', description: 'Repository name' }, description: { type: 'string' }, private: { type: 'boolean' } }, required: ['name'] } },
  { name: 'get_repo', description: 'Get a repository by owner and name.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' } }, required: ['owner', 'repo'] } },
  { name: 'create_issue', description: 'Create an issue in a repository.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, title: { type: 'string' }, body: { type: 'string' } }, required: ['owner', 'repo', 'title'] } },
  { name: 'list_issues', description: 'List issues in a repository.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, state: { type: 'string', description: 'open, closed or all' } }, required: ['owner', 'repo'] } },
  { name: 'create_pull_request', description: 'Create a pull request from a head branch into a base branch.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, title: { type: 'string' }, body: { type: 'string' }, head: { type: 'string' }, base: { type: 'string' } }, required: ['owner', 'repo', 'title', 'head', 'base'] } },
  { name: 'merge_pull_request', description: 'Merge a pull request by number.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, pull_number: { type: 'integer' } }, required: ['owner', 'repo', 'pull_number'] } },
  { name: 'get_issue', description: 'Get one issue by number.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, issue_number: { type: 'integer' } }, required: ['owner', 'repo', 'issue_number'] } },
  { name: 'list_pull_requests', description: 'List pull requests in a repository.', inputSchema: { type: 'object', properties: { owner: { type: 'string' }, repo: { type: 'string' }, state: { type: 'string' } }, required: ['owner', 'repo'] } },
];

function mcpToolResult(payload: unknown): Record<string, unknown> {
  const text = JSON.stringify(payload);
  return { content: [{ type: 'text', text }], structuredContent: payload };
}
function mcpToolError(message: string): Record<string, unknown> {
  return { content: [{ type: 'text', text: message }], isError: true };
}

function mcpCallTool(name: string, args: Record<string, unknown>): { result?: Record<string, unknown>; error?: { code: number; message: string } } {
  const tool = MCP_TOOLS.find((t) => t.name === name);
  if (!tool) return { error: { code: -32602, message: `Unknown tool: ${name}` } };
  const required = ((tool.inputSchema.required as string[] | undefined) ?? []);
  for (const req of required) {
    if (args[req] === undefined || args[req] === null || args[req] === '') {
      return { error: { code: -32602, message: `Missing required argument: ${req}` } };
    }
  }
  const owner = String(args.owner ?? VIEWER_LOGIN);
  const repoName = String(args.repo ?? args.name ?? '');
  const getRepo = (): Repo | null => (owner === VIEWER_LOGIN ? repos.get(repoKey(owner, repoName)) ?? null : null);
  switch (name) {
    case 'create_repo': {
      const rname = String(args.name ?? '');
      if (rname === '') return { result: mcpToolError('name is required') };
      if (repos.has(repoKey(VIEWER_LOGIN, rname))) return { result: mcpToolError(`Repository already exists: ${rname}`) };
      const r = newRepo(rname, args.private === true, typeof args.description === 'string' ? args.description : '');
      repos.set(repoKey(VIEWER_LOGIN, rname), r);
      return { result: mcpToolResult(repoJson(r)) };
    }
    case 'get_repo': {
      const r = getRepo();
      if (!r) return { result: mcpToolError(`Repository not found: ${owner}/${repoName}`) };
      return { result: mcpToolResult(repoJson(r)) };
    }
    case 'create_issue': {
      const r = getRepo();
      if (!r) return { result: mcpToolError(`Repository not found: ${owner}/${repoName}`) };
      const issue = createIssue(r, String(args.title ?? ''), typeof args.body === 'string' ? args.body : '', []);
      return { result: mcpToolResult(issueJson(r, issue)) };
    }
    case 'list_issues': {
      const r = getRepo();
      if (!r) return { result: mcpToolError(`Repository not found: ${owner}/${repoName}`) };
      const state = typeof args.state === 'string' ? args.state : 'open';
      let list = r.issues.slice();
      if (state !== 'all') list = list.filter((i) => i.state === state);
      return { result: mcpToolResult(list.map((i) => issueJson(r, i))) };
    }
    case 'create_pull_request': {
      const r = getRepo();
      if (!r) return { result: mcpToolError(`Repository not found: ${owner}/${repoName}`) };
      const { pr, error } = createPR(r, String(args.title ?? ''), typeof args.body === 'string' ? args.body : '', String(args.head ?? ''), String(args.base ?? ''));
      if (!pr) return { result: mcpToolError(error ?? 'Cannot create pull request') };
      return { result: mcpToolResult(prJson(r, pr)) };
    }
    case 'merge_pull_request': {
      const r = getRepo();
      if (!r) return { result: mcpToolError(`Repository not found: ${owner}/${repoName}`) };
      const num = Number(args.pull_number);
      const pr = r.pulls.find((p) => p.number === num);
      if (!pr) return { result: mcpToolError(`Pull request not found: ${num}`) };
      if (pr.merged) return { result: mcpToolResult({ sha: pr.mergeCommitSha, merged: true, number: pr.number }) };
      const sha = mergePR(r, pr);
      if (sha === null) return { result: mcpToolError('Merge conflict') };
      return { result: mcpToolResult({ sha, merged: true, number: pr.number }) };
    }
    case 'get_issue': {
      const r = getRepo();
      if (!r) return { result: mcpToolError(`Repository not found: ${owner}/${repoName}`) };
      const num = Number(args.issue_number);
      const issue = r.issues.find((i) => i.number === num);
      if (!issue) return { result: mcpToolError(`Issue not found: ${num}`) };
      return { result: mcpToolResult(issueJson(r, issue)) };
    }
    case 'list_pull_requests': {
      const r = getRepo();
      if (!r) return { result: mcpToolError(`Repository not found: ${owner}/${repoName}`) };
      const state = typeof args.state === 'string' ? args.state : 'open';
      let list = r.pulls.slice();
      if (state !== 'all') list = list.filter((p) => p.state === state);
      return { result: mcpToolResult(list.map((p) => prJson(r, p))) };
    }
    default:
      return { error: { code: -32602, message: `Unknown tool: ${name}` } };
  }
}

function handleMCP(body: unknown, res: ForgeRes): void {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    sendJson(res, 400, { jsonrpc: '2.0', id: null, error: { code: -32600, message: 'Invalid Request' } });
    return;
  }
  const req = body as Record<string, unknown>;
  const id = req.id ?? null;
  const method = typeof req.method === 'string' ? req.method : '';
  const params = (req.params ?? {}) as Record<string, unknown>;
  if (id === null || id === undefined) {
    // Notification: acknowledge, no body.
    res.writeHead(202, { 'x-github-request-id': `req-${++requestSeq}` });
    res.end();
    return;
  }
  const ok = (result: unknown): void => sendJson(res, 200, { jsonrpc: '2.0', id, result });
  const fail = (code: number, message: string): void => sendJson(res, 200, { jsonrpc: '2.0', id, error: { code, message } });
  switch (method) {
    case 'server/discover':
      ok({
        serverInfo: { name: 'github-forge', version: '1.0.0' },
        capabilities: { tools: {} },
        supportedVersions: ['2026-07-28'],
        instructions: 'A GitHub-compatible git forge: repositories, issues, pull requests, git smart HTTP, GraphQL and MCP tools.',
      });
      return;
    case 'initialize': {
      const requested = typeof params.protocolVersion === 'string' ? params.protocolVersion : '2025-11-25';
      const version = /^\d{4}-\d{2}-\d{2}$/.test(requested) ? requested : '2025-11-25';
      ok({
        protocolVersion: version,
        capabilities: { tools: {} },
        serverInfo: { name: 'github-forge', version: '1.0.0' },
      });
      return;
    }
    case 'ping':
      ok({});
      return;
    case 'tools/list':
      ok({ tools: MCP_TOOLS });
      return;
    case 'tools/call': {
      const toolName = typeof params.name === 'string' ? params.name : '';
      const args = (params.arguments ?? {}) as Record<string, unknown>;
      if (toolName === '') { fail(-32602, 'Missing tool name'); return; }
      const out = mcpCallTool(toolName, args);
      if (out.error) { fail(out.error.code, out.error.message); return; }
      ok(out.result);
      return;
    }
    default:
      fail(-32601, `Method not found: ${method}`);
  }
}

// -------------------------------------------------------------------------------------------
// Server
// -------------------------------------------------------------------------------------------

/** Build the server. It binds nothing until the caller listens; the entry listens on an ephemeral port. */
export function createApp(): Server {
  return createServer((socket: Socket) => {
    serveSocket(socket, (req, res) => handleForgeRequest(req, res));
  });
}

/**
 * One request through the forge: routing, auth, handlers. Transport-agnostic (ForgeReq/ForgeRes).
 *
 * v3c: the dispatcher is synchronous on every path that needs no request body (healthz, git
 * advertisements, bodyless REST reads — the timed read workload is 2,000 of those in a row).
 * The old shape wrapped every request in an async IIFE and awaited a body promise even when
 * there was no body, costing an async frame plus microtask hops per request. Paths that do
 * need a body delegate to the small async helpers below, which keep the exact same order of
 * checks (404/auth before the body read) the async version had.
 */
function handleForgeRequest(req: ForgeReq, res: ForgeRes): void {
  try {
    const rawUrl = req.url ?? '/';
    const qIndex = rawUrl.indexOf('?');
    const rawPath = qIndex >= 0 ? rawUrl.slice(0, qIndex) : rawUrl;
    const query = new LazyQuery(qIndex >= 0 ? rawUrl.slice(qIndex + 1) : '');
    const method = req.method ?? 'GET';
    const parts = rawPath.split('/').map((p) => {
      if (p.indexOf('%') === -1) return p;
      try { return decodeURIComponent(p); } catch { return p; }
    });

    if (method === 'GET' && rawPath === '/healthz') {
      sendJsonBuf(res, 200, HEALTHZ_BUF);
      return;
    }

        // Git smart HTTP: /{owner}/{repo}.git/...
        if (parts.length >= 3 && parts[2].endsWith('.git')) {
          const owner = parts[1];
          const repoName = parts[2].slice(0, -4);
          const repo = findRepo(owner, repoName);
          const gitTail = parts.slice(3).join('/');
          if (method === 'GET' && gitTail === 'info/refs') {
            if (!repo) { sendJson(res, 404, errJson('Not Found')); return; }
            const service = query.get('service') ?? '';
            const isV2 = req.headers['git-protocol'] === 'version=2' && service === 'git-upload-pack';
            if (service === 'git-upload-pack') {
              sendBuffer(res, 200, isV2 ? V2_ADVERTISEMENT : cachedV0UploadAd(repo), 'application/x-git-upload-pack-advertisement');
              return;
            }
            if (service === 'git-receive-pack') {
              sendBuffer(res, 200, cachedV0ReceiveAd(repo), 'application/x-git-receive-pack-advertisement');
              return;
            }
            sendJson(res, 400, errJson('Unknown service'));
            return;
          }
          if (method === 'POST' && gitTail === 'git-upload-pack') {
            if (!repo) { sendJson(res, 404, errJson('Not Found')); return; }
            void handleGitUploadPack(req, res, repo);
            return;
          }
          if (method === 'POST' && gitTail === 'git-receive-pack') {
            if (!repo) { sendJson(res, 404, errJson('Not Found')); return; }
            void handleGitReceivePack(req, res, repo);
            return;
          }
          sendJson(res, 404, errJson('Not Found'));
          return;
        }

        // MCP
        if (rawPath === '/mcp') {
          void handleMcpRequest(req, res, method);
          return;
        }

        // GraphQL
        if (rawPath === '/api/graphql') {
          void handleGraphqlRequest(req, res, method);
          return;
        }

        // REST v3
        if (parts[1] === 'api' && parts[2] === 'v3') {
          if (!isAuthed(req)) {
            sendJson(res, 401, errJson('Requires authentication'));
            return;
          }
          const restParts = parts.slice(3).filter((p) => p !== '');
          // Bodyless reads run straight through, synchronously: the timed read workload is
          // 2,000 of these on one connection, and none of them has a body to wait for.
          if ((method === 'GET' || method === 'HEAD' || method === 'DELETE') && !hasDeclaredBody(req)) {
            handleRest(req, res, restParts, query, undefined, EMPTY_BODY);
            return;
          }
          void handleRestWithBody(req, res, restParts, query);
          return;
        }

        sendJson(res, 404, errJson('Not Found'));
  } catch (err) {
    try {
      sendJson(res, 500, errJson(err instanceof Error ? err.message : 'Internal error'));
    } catch { /* socket gone */ }
  }
}

/** A GET/HEAD/DELETE that declares a body (content-length or transfer-encoding) is not bodyless. */
function hasDeclaredBody(req: ForgeReq): boolean {
  const len = req.headers['content-length'];
  return (typeof len === 'string' && len !== '0') || req.headers['transfer-encoding'] !== undefined;
}

/** The 500 answer a helper sends when its handler throws, socket failures swallowed. */
function sendInternalError(res: ForgeRes, err: unknown): void {
  try {
    sendJson(res, 500, errJson(err instanceof Error ? err.message : 'Internal error'));
  } catch { /* socket gone */ }
}

async function handleGitUploadPack(req: ForgeReq, res: ForgeRes, repo: Repo): Promise<void> {
  try {
    const body = await readBodyFast(req);
    const isV2 = req.headers['git-protocol'] === 'version=2';
    const out = handleUploadPack(repo, body, isV2);
    sendBuffer(res, out.status, out.body, out.contentType);
  } catch (err) {
    sendInternalError(res, err);
  }
}

async function handleGitReceivePack(req: ForgeReq, res: ForgeRes, repo: Repo): Promise<void> {
  try {
    const body = await readBodyFast(req);
    const out = handleReceivePack(repo, body);
    sendBuffer(res, out.status, out.body, out.contentType);
  } catch (err) {
    sendInternalError(res, err);
  }
}

async function handleMcpRequest(req: ForgeReq, res: ForgeRes, method: string): Promise<void> {
  try {
    if (method !== 'POST') { sendJson(res, 404, errJson('Not Found')); return; }
    if (!isAuthed(req)) {
      sendJson(res, 401, { jsonrpc: '2.0', id: null, error: { code: -32001, message: 'Unauthorized' } });
      return;
    }
    const raw = await readBodyFast(req);
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw.toString('utf8')) as unknown;
    } catch {
      sendJson(res, 400, { jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } });
      return;
    }
    handleMCP(parsed, res);
  } catch (err) {
    sendInternalError(res, err);
  }
}

async function handleGraphqlRequest(req: ForgeReq, res: ForgeRes, method: string): Promise<void> {
  try {
    if (method !== 'POST') { sendJson(res, 404, errJson('Not Found')); return; }
    if (!isAuthed(req)) {
      sendJson(res, 401, errJson('Requires authentication'));
      return;
    }
    const raw = await readBodyFast(req);
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw.toString('utf8')) as unknown;
    } catch {
      sendJson(res, 400, errJson('Invalid JSON'));
      return;
    }
    const out = handleGraphQL(parsed);
    sendJson(res, out.status, out.payload);
  } catch (err) {
    sendInternalError(res, err);
  }
}

async function handleRestWithBody(req: ForgeReq, res: ForgeRes, restParts: string[], query: QueryParams): Promise<void> {
  try {
    const raw = await readBodyIfAny(req);
    let parsedBody: unknown;
    if (raw.length > 0) {
      try {
        parsedBody = JSON.parse(raw.toString('utf8')) as unknown;
      } catch {
        sendJson(res, 400, errJson('Invalid JSON'));
        return;
      }
    }
    handleRest(req, res, restParts, query, parsedBody, raw);
  } catch (err) {
    sendInternalError(res, err);
  }
}
