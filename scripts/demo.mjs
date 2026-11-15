import { createProvider } from '../provider/server.mjs';
import { createInbox } from '../src/inbox.mjs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const TOPIC = 'http://local.invalid/topics/catalog';
const directory = await mkdtemp(join(tmpdir(), 'collection-'));
const provider = await createProvider();
let inbox = await createInbox({ directory });
const port = Number(new URL(inbox.base).port);

const post = async (base, path, payload) => {
  const response = await fetch(base + path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload ?? {}) });
  return { status: response.status, body: await response.json() };
};
const get = (base, path) => fetch(base + path).then(response => response.json());
const deliver = hubId => fetch(`${provider.base}/deliver/${hubId}`, { method: 'POST' }).then(response => response.json());

try {
  // 接入：登记订阅、来源执行回调验证、对账确认生效
  const first = await post(inbox.base, '/subscriptions', { hub: provider.base + '/hub', topic: TOPIC, secret: 'first-secret' });
  // 来源推送第一批资料
  const accepted = await deliver(first.body.hubId);
  // 续租：确认新租约后租约顺延，接收不中断
  const renewed = await post(inbox.base, `/subscriptions/${first.body.id}/renew`);
  // 轮换：新密钥的接替订阅生效，旧订阅短暂重叠仍可能投递
  const rotated = await post(inbox.base, `/subscriptions/${first.body.id}/rotate`, { secret: 'rotated-secret' });
  const overlapOld = await deliver(first.body.hubId);
  const overlapNew = await deliver(rotated.body.hubId);
  // 新一批资料经接替订阅到达
  await post(provider.base, '/_control', { generation: 2 });
  const secondBatch = await deliver(rotated.body.hubId);
  // 停止旧订阅：迟到推送被拒绝并留痕
  const stopped = await post(inbox.base, `/subscriptions/${first.body.id}/stop`);
  const late = await deliver(first.body.hubId);

  const summary = {
    接入: first.body.state,
    续租: { state: renewed.body.state, leaseExpires: renewed.body.leaseExpires },
    轮换: { 接替订阅: rotated.body.id, 接替自: rotated.body.replaces },
    投递回执: { accepted, overlapOld, overlapNew, secondBatch, late: { ...late, 说明: '旧订阅停止后的迟到投递被拒绝' } },
    订阅视图: (await get(inbox.base, '/subscriptions')).map(({ id, state, received, duplicates, rejected, replaces }) => ({ id, state, received, duplicates, rejected, replaces })),
    已收资料: (await get(inbox.base, '/materials')).map(receipt => ({ publisher: receipt.source.publisher, record: receipt.source.record, receipt: receipt.id })),
    拒绝记录: await get(inbox.base, '/rejections'),
  };

  // 重启：已停止的订阅不恢复，迟到投递继续被拒绝
  await inbox.close();
  inbox = await createInbox({ directory, port });
  const lateAfterRestart = await deliver(first.body.hubId);
  summary.重启后 = {
    订阅状态: (await get(inbox.base, '/subscriptions')).map(({ id, state }) => ({ id, state })),
    迟到投递: lateAfterRestart,
    拒绝记录数: (await get(inbox.base, '/rejections')).length,
  };
  console.log(JSON.stringify(summary, null, 2));
} finally {
  await inbox.close();
  await provider.close();
  await rm(directory, { recursive: true, force: true });
}
