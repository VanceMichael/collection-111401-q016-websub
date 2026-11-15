import { createServer } from 'node:http';
import { once } from 'node:events';
import { createHmac, randomUUID, timingSafeEqual } from 'node:crypto';
import { mkdir, open, readFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { Archive } from './archive.mjs';

// 单份推送上限与人工领取保持一致
const MAX_BODY = 8 * 1024 * 1024;
const DEFAULT_LEASE_SECONDS = 30;
const STATE_FILE = 'inbox-state.json';
const JOURNAL_FILE = 'inbox-rejections.jsonl';

export class InboxError extends Error {
  constructor(message, status = 400) {
    super(message);
    this.status = status;
  }
}

// 资料身份属于发布者：去重键必须包含 publisher，不能跨发布者只按 id 去重
const keyOf = (publisher, record) => `${publisher}\n${record}`;

async function syncDirectory(path) {
  const handle = await open(path, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}

function peekRecord(body) {
  try {
    const payload = JSON.parse(body.toString('utf8'));
    const peek = {};
    if (typeof payload?.publisher === 'string' && payload.publisher) peek.publisher = payload.publisher;
    if (typeof payload?.id === 'string' && payload.id) peek.record = payload.id;
    return peek;
  } catch { return {}; }
}

function parseJson(body, optional = false) {
  if (!body.length) return optional ? null : {};
  try {
    const payload = JSON.parse(body.toString('utf8'));
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) throw new Error();
    return payload;
  } catch { throw new InboxError('请求体必须是 JSON 对象', 400); }
}

export class Inbox {
  constructor({ directory, now = () => Date.now(), fetchImpl = fetch, callbackBase = null }) {
    if (!directory) throw new Error('收件服务需要 directory 持久目录');
    this.directory = directory;
    this.now = now;
    this.fetch = fetchImpl;
    this.callbackBase = callbackBase;
    this.archive = new Archive(directory);
    this.subscriptions = new Map();
    this.delivered = new Set();
    this.journal = [];
    this.sequence = 0;
    this.persistenceQueue = Promise.resolve();
  }

  static async load(options) {
    const inbox = new Inbox(options);
    await mkdir(inbox.directory, { recursive: true });
    try {
      const state = JSON.parse(await readFile(join(inbox.directory, STATE_FILE), 'utf8'));
      for (const record of state.subscriptions ?? []) {
        inbox.subscriptions.set(record.id, record);
        const seq = /^inbox-(\d+)$/.exec(record.id);
        if (seq) inbox.sequence = Math.max(inbox.sequence, Number(seq[1]));
      }
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
    try {
      const lines = (await readFile(join(inbox.directory, JOURNAL_FILE), 'utf8')).split('\n').filter(Boolean);
      inbox.journal = lines.map(line => JSON.parse(line));
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
    // 已收资料身份从收件凭据重建，重启后同一资料仍不重复交付
    for (const receipt of await inbox.archive.list()) {
      const source = receipt.source ?? {};
      if (source.type === 'push' && source.publisher && source.record) inbox.delivered.add(keyOf(source.publisher, source.record));
    }
    return inbox;
  }

  enqueuePersistence(task) {
    const run = this.persistenceQueue.then(task, task);
    this.persistenceQueue = run.catch(() => {});
    return run;
  }

  async saveState() {
    const snapshot = JSON.stringify({ subscriptions: [...this.subscriptions.values()] });
    await this.enqueuePersistence(async () => {
      const target = join(this.directory, STATE_FILE);
      const staging = target + '.pending';
      const handle = await open(staging, 'w', 0o600);
      try { await handle.writeFile(snapshot); await handle.sync(); } finally { await handle.close(); }
      await rename(staging, target);
      await syncDirectory(this.directory);
    });
  }

  async recordRejection(entry) {
    const full = { at: new Date(this.now()).toISOString(), ...entry };
    this.journal.push(full);
    const line = JSON.stringify(full) + '\n';
    await this.enqueuePersistence(async () => {
      const handle = await open(join(this.directory, JOURNAL_FILE), 'a', 0o600);
      try { await handle.writeFile(line); await handle.sync(); } finally { await handle.close(); }
    });
  }

  require(id) {
    const record = this.subscriptions.get(id);
    if (!record) throw new InboxError('订阅不存在', 404);
    return record;
  }

  publicRecord(record) {
    const { secret, ...rest } = record;
    return { ...rest, leaseExpired: record.leaseExpires !== null && this.now() >= record.leaseExpires };
  }

  list() {
    return [...this.subscriptions.values()].map(record => this.publicRecord(record));
  }

  rejections(subscription) {
    return subscription ? this.journal.filter(entry => entry.subscription === subscription) : [...this.journal];
  }

  materials() {
    return this.archive.list();
  }

  // ---- 订阅生命周期：接入、续租、轮换、停止 ----

  async subscribe({ hub, topic, secret, replaces = null } = {}) {
    if (!hub || !topic) throw new InboxError('订阅需要 hub 与 topic', 400);
    if (!this.callbackBase) throw new InboxError('收件地址尚未就绪', 503);
    const id = `inbox-${++this.sequence}`;
    const record = {
      id, hub, topic, secret: secret ?? randomUUID(), replaces,
      callback: `${this.callbackBase}/callback/${id}`,
      state: 'pending', hubId: null, confirmedHubId: null, hubStopId: null,
      leaseSeconds: DEFAULT_LEASE_SECONDS, leaseExpires: null,
      createdAt: this.now(), confirmedAt: null, stopRequestedAt: null, stoppedAt: null, lastVerificationAt: null,
      received: 0, duplicates: 0, rejected: 0,
    };
    this.subscriptions.set(id, record);
    let answer;
    try { answer = await this.hubRequest(record, 'subscribe'); }
    catch (error) { this.subscriptions.delete(id); await this.saveState(); throw error; }
    record.hubId = answer.id;
    await this.saveState();
    await this.confirmWithHub(record, answer);
    return record;
  }

  async renew(id) {
    const record = this.require(id);
    if (record.state !== 'active') throw new InboxError('仅已生效的订阅可以续租', 409);
    // 续租沿用原回调与密钥向来源重新登记，确认后租约顺延，期间接收不中断
    const answer = await this.hubRequest(record, 'subscribe');
    record.hubId = answer.id;
    await this.saveState();
    await this.confirmWithHub(record, answer);
    return record;
  }

  async rotate(id, secret) {
    const record = this.require(id);
    if (record.state !== 'active') throw new InboxError('仅已生效的订阅可以轮换密钥', 409);
    // 轮换建立携带新密钥的接替订阅；旧订阅在停止或过期前仍会投递，由接收端去重
    return this.subscribe({ hub: record.hub, topic: record.topic, secret: secret ?? randomUUID(), replaces: record.id });
  }

  async stop(id) {
    const record = this.require(id);
    if (record.state === 'stopped' || record.state === 'stopping') return record;
    const answer = await this.hubRequest(record, 'unsubscribe');
    record.hubStopId = answer.id;
    record.state = 'stopping';
    record.stopRequestedAt = this.now();
    await this.saveState();
    await this.confirmWithHub(record, answer);
    return record;
  }

  async sync(id) {
    const record = this.require(id);
    await this.syncFromHub(record);
    await this.saveState();
    return record;
  }

  async confirmWithHub(record, answer) {
    // 本地演练中来源返回验证触发地址时，请来源执行回调验证；随后以来源的订阅记录对账为准
    try {
      if (answer?.verify) await this.driveVerification(record.hub, answer.verify);
      await this.syncFromHub(record);
    } catch { /* 来源暂时不可达：保留当前状态，之后可再对账 */ }
    await this.saveState();
  }

  async hubRequest(record, mode) {
    const form = new URLSearchParams({
      'hub.mode': mode, 'hub.topic': record.topic,
      'hub.callback': record.callback, 'hub.secret': record.secret,
    });
    let response;
    try {
      response = await this.fetch(record.hub, { method: 'POST', body: form, signal: AbortSignal.timeout(5000) });
    } catch (error) { throw new InboxError(`来源不可达：${error.message}`, 502); }
    if (response.status !== 202) throw new InboxError(`来源拒绝${mode === 'subscribe' ? '订阅' : '停止'}请求：${response.status}`, 502);
    return response.json();
  }

  async driveVerification(hub, verifyPath) {
    const response = await this.fetch(new URL(verifyPath, hub), { method: 'POST', signal: AbortSignal.timeout(5000) });
    if (!response.ok) throw new InboxError(`来源验证执行失败：${response.status}`, 502);
    return response.json();
  }

  async syncFromHub(record) {
    let response;
    try {
      response = await this.fetch(new URL('/subscriptions', record.hub), { signal: AbortSignal.timeout(5000) });
    } catch (error) { throw new InboxError(`来源不可达：${error.message}`, 502); }
    if (!response.ok) throw new InboxError(`来源对账失败：${response.status}`, 502);
    const list = await response.json();
    const byId = new Map(list.map(item => [item.id, item]));
    if (record.hubStopId) {
      // 停止只以来源的退订记录为准，且绝不因对账恢复已停止的订阅
      const stop = byId.get(record.hubStopId);
      if (stop?.status === 'stopped' && record.state !== 'stopped') {
        record.state = 'stopped';
        record.stoppedAt = this.now();
      }
      return record;
    }
    const current = byId.get(record.hubId);
    if (!current) return record;
    if (current.status === 'active' && record.state !== 'stopping' && record.state !== 'stopped') {
      if (record.confirmedHubId !== record.hubId) {
        record.state = 'active';
        record.confirmedHubId = record.hubId;
        record.confirmedAt = this.now();
        record.leaseExpires = this.now() + record.leaseSeconds * 1000;
      }
    } else if (current.status === 'failed' && record.state === 'pending') {
      record.state = 'failed';
    }
    return record;
  }

  // ---- 接收入口：回调验证与推送投递 ----

  // 验证请求只证明回调可达，不改变本地订阅状态；生效只能通过对账确认
  async handleVerification(id, params) {
    const record = this.subscriptions.get(id);
    if (!record) return { status: 404, body: '订阅不存在' };
    const mode = params.get('hub.mode');
    const challenge = params.get('hub.challenge');
    if (!mode || !challenge || params.get('hub.topic') !== record.topic) return { status: 400, body: '验证参数不完整' };
    if (mode === 'subscribe') {
      if (record.state === 'stopping' || record.state === 'stopped') return { status: 409, body: '订阅已停止，不再接受验证' };
      const lease = Number(params.get('hub.lease_seconds'));
      if (Number.isFinite(lease) && lease > 0) record.leaseSeconds = lease;
      record.lastVerificationAt = this.now();
      await this.saveState();
      return { status: 200, body: challenge };
    }
    if (mode === 'unsubscribe') {
      if (!record.hubStopId) return { status: 409, body: '订阅未请求停止' };
      return { status: 200, body: challenge };
    }
    return { status: 400, body: '不支持的验证模式' };
  }

  signatureValid(record, body, header) {
    if (typeof header !== 'string' || !header.startsWith('sha256=')) return false;
    const expected = createHmac('sha256', record.secret).update(body).digest('hex');
    const presented = header.slice('sha256='.length);
    if (expected.length !== presented.length) return false;
    return timingSafeEqual(Buffer.from(expected), Buffer.from(presented));
  }

  async handleDelivery(id, body, signatureHeader) {
    const record = this.subscriptions.get(id);
    if (!record) {
      await this.recordRejection({ subscription: id, reason: '订阅不存在', ...peekRecord(body) });
      return { status: 404, body: { error: '订阅不存在' } };
    }
    const reject = async (status, reason) => {
      record.rejected += 1;
      await this.saveState();
      await this.recordRejection({
        subscription: id, reason,
        signature: this.signatureValid(record, body, signatureHeader) ? 'valid' : 'invalid',
        ...peekRecord(body),
      });
      return { status, body: { error: reason } };
    };
    // 过期或尚未确认的订阅不能冒充正常来源：先按本地状态判断，必要时与来源对账一次
    if (record.state === 'pending' || record.state === 'failed') {
      try { await this.syncFromHub(record); await this.saveState(); } catch { /* 来源不可达时按本地状态判断 */ }
    }
    if (record.state !== 'active') {
      const reason = record.state === 'pending' ? '订阅尚未确认'
        : record.state === 'failed' ? '订阅验证未通过'
        : '订阅已停止';
      return reject(409, reason);
    }
    if (record.leaseExpires === null || this.now() >= record.leaseExpires) return reject(409, '租约已过期');
    if (!this.signatureValid(record, body, signatureHeader)) return reject(401, '签名不符');
    let payload;
    try { payload = JSON.parse(body.toString('utf8')); }
    catch { return reject(400, '推送体不是有效 JSON'); }
    if (typeof payload?.publisher !== 'string' || !payload.publisher || typeof payload?.id !== 'string' || !payload.id) {
      return reject(400, '资料身份不完整');
    }
    const key = keyOf(payload.publisher, payload.id);
    if (this.delivered.has(key)) {
      record.duplicates += 1;
      await this.saveState();
      return { status: 200, body: { status: 'duplicate', publisher: payload.publisher, record: payload.id } };
    }
    // 先登记身份再落盘，避免并发重复收存；落盘失败则撤销登记，由来源重投
    this.delivered.add(key);
    let receipt;
    try {
      receipt = await this.archive.save(body, {
        type: 'push', subscription: record.id, hubSubscription: record.hubId,
        publisher: payload.publisher, record: payload.id,
        edition: typeof payload.edition === 'number' ? payload.edition : null,
      });
    } catch (error) {
      this.delivered.delete(key);
      throw new InboxError('本地收存失败：' + error.message, 500);
    }
    // 只有本地真正收妥（内容写入并 fsync）才返回成功
    record.received += 1;
    await this.saveState();
    return { status: 200, body: { status: 'stored', receipt: receipt.id, publisher: payload.publisher, record: payload.id } };
  }
}

async function route(inbox, request, url, body) {
  const method = request.method;
  const path = url.pathname;
  const json = (value, status = 200) => ({ status, body: JSON.stringify(value) });
  const callback = /^\/callback\/([\w-]+)$/.exec(path);
  if (callback) {
    if (method === 'GET') {
      const result = await inbox.handleVerification(callback[1], url.searchParams);
      return { status: result.status, body: result.body, type: 'text/plain; charset=utf-8' };
    }
    if (method === 'POST') {
      const result = await inbox.handleDelivery(callback[1], body, request.headers['x-hub-signature-256']);
      return json(result.body, result.status);
    }
    return json({ error: '方法不允许' }, 405);
  }
  if (path === '/health' && method === 'GET') return json({ service: 'collection-inbox', subscriptions: inbox.subscriptions.size });
  if (path === '/subscriptions' && method === 'GET') return json(inbox.list());
  if (path === '/subscriptions' && method === 'POST') return json(inbox.publicRecord(await inbox.subscribe(parseJson(body))), 201);
  const subscription = /^\/subscriptions\/([\w-]+)(?:\/(renew|rotate|stop|sync))?$/.exec(path);
  if (subscription) {
    const [, id, action] = subscription;
    if (!action && method === 'GET') return json(inbox.publicRecord(inbox.require(id)));
    if (action === 'renew' && method === 'POST') return json(inbox.publicRecord(await inbox.renew(id)));
    if (action === 'rotate' && method === 'POST') return json(inbox.publicRecord(await inbox.rotate(id, parseJson(body, true)?.secret)), 201);
    if (action === 'stop' && method === 'POST') return json(inbox.publicRecord(await inbox.stop(id)));
    if (action === 'sync' && method === 'POST') return json(inbox.publicRecord(await inbox.sync(id)));
    return json({ error: '方法不允许' }, 405);
  }
  if (path === '/rejections' && method === 'GET') return json(inbox.rejections(url.searchParams.get('subscription')));
  if (path === '/materials' && method === 'GET') return json(await inbox.materials());
  return json({ error: '路径不存在' }, 404);
}

export async function createInbox(options = {}) {
  const inbox = await Inbox.load(options);
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      const pieces = [];
      let size = 0;
      for await (const piece of request) {
        size += piece.length;
        if (size > MAX_BODY) { response.writeHead(413).end(); return; }
        pieces.push(piece);
      }
      const body = Buffer.concat(pieces);
      const result = await route(inbox, request, url, body);
      response.writeHead(result.status, { 'cache-control': 'no-store', 'content-type': result.type ?? 'application/json; charset=utf-8' });
      response.end(result.body);
    } catch (error) {
      const status = error instanceof InboxError ? error.status : 500;
      response.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
      response.end(JSON.stringify({ error: error.message }));
    }
  });
  server.listen(options.port ?? 0, options.host ?? '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;
  inbox.callbackBase ??= base;
  return {
    inbox, base,
    close: () => new Promise((resolve, reject) => {
      server.close(error => error ? reject(error) : resolve());
      server.closeAllConnections();
    }),
  };
}
