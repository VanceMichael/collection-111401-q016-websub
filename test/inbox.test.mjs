import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createHmac } from 'node:crypto';
import { createProvider } from '../provider/server.mjs';
import { createInbox } from '../src/inbox.mjs';
import { Archive } from '../src/archive.mjs';

const TOPIC = 'http://local.invalid/topics/catalog';

async function subscribe(base, payload) {
  const response = await fetch(base + '/subscriptions', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) });
  return { status: response.status, body: await response.json() };
}

async function action(base, id, name, payload) {
  const response = await fetch(`${base}/subscriptions/${id}/${name}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload ?? {}) });
  return { status: response.status, body: await response.json() };
}

const deliver = (provider, hubId) => fetch(`${provider.base}/deliver/${hubId}`, { method: 'POST' }).then(response => response.json());

async function push(base, localId, secret, payload) {
  const bytes = Buffer.from(JSON.stringify(payload));
  const signature = 'sha256=' + createHmac('sha256', secret).update(bytes).digest('hex');
  const response = await fetch(`${base}/callback/${localId}`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-hub-signature-256': signature }, body: bytes });
  return { status: response.status, body: await response.json() };
}

const get = (base, path) => fetch(base + path).then(response => response.json());

// 不广告验证触发地址的桩来源：模拟生产 hub 自行安排验证，便于观察尚未确认的订阅
async function createStubHub() {
  const records = new Map();
  let sequence = 0;
  const server = createServer(async (request, response) => {
    const url = new URL(request.url, 'http://127.0.0.1');
    const pieces = [];
    for await (const piece of request) pieces.push(piece);
    if (url.pathname === '/hub' && request.method === 'POST') {
      const form = new URLSearchParams(Buffer.concat(pieces).toString());
      const id = 'stub-' + (++sequence);
      records.set(id, { id, status: 'pending', mode: form.get('hub.mode'), callback: form.get('hub.callback'), topic: form.get('hub.topic') });
      response.writeHead(202, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ id, status: 'pending' }));
      return;
    }
    if (url.pathname === '/subscriptions') {
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify([...records.values()]));
      return;
    }
    response.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return {
    base: `http://127.0.0.1:${server.address().port}`, records,
    close: () => new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }),
  };
}

async function setup(options = {}) {
  const provider = await createProvider();
  const directory = await mkdtemp(join(tmpdir(), 'inbox-'));
  const inbox = await createInbox({ directory, ...options });
  return {
    provider, directory, inbox,
    async cleanup() { await inbox.close(); await provider.close(); await rm(directory, { recursive: true, force: true }); },
  };
}

test('接入后真实收妥推送，重复投递不重复交付', async () => {
  const { provider, directory, inbox, cleanup } = await setup();
  try {
    const created = await subscribe(inbox.base, { hub: provider.base + '/hub', topic: TOPIC, secret: 'first-secret' });
    assert.equal(created.status, 201);
    assert.equal(created.body.state, 'active');
    assert.ok(created.body.leaseExpires > 0);
    assert.equal(created.body.secret, undefined, '接口不得返回密钥');

    const accepted = await deliver(provider, created.body.hubId);
    assert.equal(accepted.status, 200);
    const again = await deliver(provider, created.body.hubId);
    assert.equal(again.status, 200, '重复投递仍回成功，但不再重复收存');

    const materials = await get(inbox.base, '/materials');
    assert.equal(materials.length, 1);
    assert.equal(materials[0].source.publisher, 'museum-a');
    assert.equal(materials[0].source.subscription, created.body.id);
    const archive = new Archive(directory);
    assert.ok((await archive.read(materials[0])).length > 0);

    const view = await get(inbox.base, '/subscriptions/' + created.body.id);
    assert.equal(view.received, 1);
    assert.equal(view.duplicates, 1);
  } finally { await cleanup(); }
});

test('验证请求不等于订阅生效，尚未确认的订阅不能投递', async () => {
  const stub = await createStubHub();
  const directory = await mkdtemp(join(tmpdir(), 'inbox-'));
  const inbox = await createInbox({ directory });
  try {
    const created = await subscribe(inbox.base, { hub: stub.base + '/hub', topic: TOPIC, secret: 'pending-secret' });
    assert.equal(created.body.state, 'pending', '202 只是收到请求');

    const wrongTopic = await fetch(`${inbox.base}/callback/${created.body.id}?hub.mode=subscribe&hub.topic=http://local.invalid/topics/other&hub.challenge=x`);
    assert.equal(wrongTopic.status, 400);

    const verify = await fetch(`${inbox.base}/callback/${created.body.id}?hub.mode=subscribe&hub.topic=${encodeURIComponent(TOPIC)}&hub.challenge=proof&hub.lease_seconds=30`);
    assert.equal(verify.status, 200);
    assert.equal(await verify.text(), 'proof', '验证响应必须原样返回 challenge');
    assert.equal((await get(inbox.base, '/subscriptions/' + created.body.id)).state, 'pending', '验证请求本身不让订阅生效');

    const early = await push(inbox.base, created.body.id, 'pending-secret', { publisher: 'museum-a', id: 'record-0' });
    assert.equal(early.status, 409);
    assert.match(early.body.error, /尚未确认/);

    stub.records.get(created.body.hubId).status = 'active';
    const synced = await action(inbox.base, created.body.id, 'sync');
    assert.equal(synced.body.state, 'active');
    assert.ok(synced.body.leaseExpires > 0, '对账确认后按本地时钟起租');

    const accepted = await push(inbox.base, created.body.id, 'pending-secret', { publisher: 'museum-a', id: 'record-0' });
    assert.equal(accepted.status, 200);
    assert.equal(accepted.body.status, 'stored');

    const missing = await fetch(`${inbox.base}/callback/inbox-999?hub.mode=subscribe&hub.topic=${encodeURIComponent(TOPIC)}&hub.challenge=x`);
    assert.equal(missing.status, 404);
    const unknown = await push(inbox.base, 'inbox-999', 'pending-secret', { publisher: 'museum-a', id: 'record-x' });
    assert.equal(unknown.status, 404);
    assert.ok((await get(inbox.base, '/rejections')).some(entry => entry.reason === '订阅不存在'));
  } finally {
    await inbox.close();
    await stub.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('验证失败的订阅不能冒充正常来源', async () => {
  const stub = await createStubHub();
  const directory = await mkdtemp(join(tmpdir(), 'inbox-'));
  const inbox = await createInbox({ directory });
  try {
    const created = await subscribe(inbox.base, { hub: stub.base + '/hub', topic: TOPIC, secret: 'failed-secret' });
    stub.records.get(created.body.hubId).status = 'failed';
    const synced = await action(inbox.base, created.body.id, 'sync');
    assert.equal(synced.body.state, 'failed');
    const rejected = await push(inbox.base, created.body.id, 'failed-secret', { publisher: 'museum-a', id: 'record-1' });
    assert.equal(rejected.status, 409);
    assert.match(rejected.body.error, /验证未通过/);
    assert.equal((await get(inbox.base, '/materials')).length, 0);
  } finally {
    await inbox.close();
    await stub.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('租约过期拒收，续租确认后顺延且接收不中断', async () => {
  let time = 1_700_000_000_000;
  const { provider, inbox, cleanup } = await setup({ now: () => time });
  try {
    const created = await subscribe(inbox.base, { hub: provider.base + '/hub', topic: TOPIC, secret: 'lease-secret' });
    assert.equal(created.body.leaseExpires, time + 30_000);

    assert.equal((await deliver(provider, created.body.hubId)).status, 200);
    await fetch(provider.base + '/_control', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ generation: 2 }) });

    time += 31_000;
    const expired = await deliver(provider, created.body.hubId);
    assert.equal(expired.status, 409);
    const view = await get(inbox.base, '/subscriptions/' + created.body.id);
    assert.equal(view.leaseExpired, true);
    assert.ok((await get(inbox.base, '/rejections')).some(entry => entry.reason === '租约已过期' && entry.record === 'record-2'));

    const renewed = await action(inbox.base, created.body.id, 'renew');
    assert.equal(renewed.body.state, 'active');
    assert.equal(renewed.body.leaseExpires, time + 30_000, '续租确认后租约顺延');

    const accepted = await deliver(provider, created.body.hubId);
    assert.equal(accepted.status, 200);
    const materials = await get(inbox.base, '/materials');
    assert.deepEqual(materials.map(receipt => receipt.source.record).sort(), ['record-1', 'record-2']);
  } finally { await cleanup(); }
});

test('轮换期间重叠订阅去重，不同发布者同名记录都保留，坏签名被拒', async () => {
  const { provider, inbox, cleanup } = await setup();
  try {
    const first = await subscribe(inbox.base, { hub: provider.base + '/hub', topic: TOPIC, secret: 'alpha-secret' });
    const rotated = await action(inbox.base, first.body.id, 'rotate', { secret: 'beta-secret' });
    assert.equal(rotated.status, 201);
    assert.equal(rotated.body.state, 'active');
    assert.equal(rotated.body.replaces, first.body.id);

    assert.equal((await deliver(provider, first.body.hubId)).status, 200);
    const overlap = await deliver(provider, rotated.body.hubId);
    assert.equal(overlap.status, 200, '旧订阅在轮换期仍会投递，接替订阅收到的同一资料不重复交付');
    assert.equal((await get(inbox.base, '/materials')).length, 1);

    const sibling = await push(inbox.base, rotated.body.id, 'beta-secret', { publisher: 'museum-b', id: 'record-1', edition: 1, title: '兄弟机构同名记录' });
    assert.equal(sibling.status, 200);
    assert.equal(sibling.body.status, 'stored', '不同发布者的同名记录必须保留');

    const forged = await push(inbox.base, rotated.body.id, 'wrong-secret', { publisher: 'museum-b', id: 'record-2' });
    assert.equal(forged.status, 401);

    const materials = await get(inbox.base, '/materials');
    assert.equal(materials.length, 2);
    assert.deepEqual(materials.map(receipt => receipt.source.publisher).sort(), ['museum-a', 'museum-b']);

    const successor = await get(inbox.base, '/subscriptions/' + rotated.body.id);
    assert.equal(successor.received, 1);
    assert.equal(successor.duplicates, 1);
    assert.equal(successor.rejected, 1);
    assert.ok((await get(inbox.base, '/rejections')).some(entry => entry.reason === '签名不符'));

    const listing = JSON.stringify(await get(inbox.base, '/subscriptions'));
    assert.doesNotMatch(listing, /alpha-secret|beta-secret/);
  } finally { await cleanup(); }
});

test('停订后迟到请求被拒绝并可见，重启不恢复已停止订阅', async () => {
  const { provider, directory, inbox, cleanup } = await setup();
  const port = Number(new URL(inbox.base).port);
  let localId;
  let hubId;
  try {
    const created = await subscribe(inbox.base, { hub: provider.base + '/hub', topic: TOPIC, secret: 'stop-secret' });
    localId = created.body.id;
    hubId = created.body.hubId;
    assert.equal((await deliver(provider, hubId)).status, 200);

    const stopped = await action(inbox.base, localId, 'stop');
    assert.equal(stopped.body.state, 'stopped');

    const late = await deliver(provider, hubId);
    assert.equal(late.status, 409, '停订后来源仍可能迟到投递，接收端必须拒绝');
    const rejections = await get(inbox.base, '/rejections?subscription=' + localId);
    assert.equal(rejections.length, 1);
    assert.equal(rejections[0].reason, '订阅已停止');
    assert.equal(rejections[0].record, 'record-1');
    assert.equal(rejections[0].publisher, 'museum-a');
    assert.equal(rejections[0].signature, 'valid');
    assert.equal((await get(inbox.base, '/materials')).length, 1);
  } finally { await inbox.close(); }

  const restarted = await createInbox({ directory, port });
  try {
    const view = (await get(restarted.base, '/subscriptions')).find(record => record.id === localId);
    assert.equal(view.state, 'stopped', '重启不得自动恢复已停止的订阅');
    const late = await deliver(provider, hubId);
    assert.equal(late.status, 409);
    assert.equal((await get(restarted.base, '/rejections?subscription=' + localId)).length, 2, '拒绝记录随目录持久保存');
    assert.equal((await get(restarted.base, '/materials')).length, 1);
  } finally {
    await restarted.close();
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('停止中的订阅重启后仍处于停止状态，对账确认后落定', async () => {
  const stub = await createStubHub();
  const directory = await mkdtemp(join(tmpdir(), 'inbox-'));
  const inbox = await createInbox({ directory });
  let localId;
  let stopHubId;
  try {
    const created = await subscribe(inbox.base, { hub: stub.base + '/hub', topic: TOPIC, secret: 'halting-secret' });
    localId = created.body.id;
    const stopping = await action(inbox.base, localId, 'stop');
    assert.equal(stopping.body.state, 'stopping', '退订验证未完成前处于停止中');
    stopHubId = stopping.body.hubStopId;
    const rejected = await push(inbox.base, localId, 'halting-secret', { publisher: 'museum-a', id: 'record-9' });
    assert.equal(rejected.status, 409);
  } finally { await inbox.close(); }

  const restarted = await createInbox({ directory });
  try {
    const view = (await get(restarted.base, '/subscriptions')).find(record => record.id === localId);
    assert.equal(view.state, 'stopping', '重启不自动恢复停止中的订阅');
    const rejected = await push(restarted.base, localId, 'halting-secret', { publisher: 'museum-a', id: 'record-9' });
    assert.equal(rejected.status, 409);

    stub.records.get(stopHubId).status = 'stopped';
    const synced = await action(restarted.base, localId, 'sync');
    assert.equal(synced.body.state, 'stopped', '对账确认退订记录后才落定停止');
  } finally {
    await restarted.close();
    await stub.close();
    await rm(directory, { recursive: true, force: true });
  }
});
