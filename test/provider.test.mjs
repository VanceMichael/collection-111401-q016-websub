import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createProvider } from '../provider/server.mjs';
import { receiveCurrent } from '../src/domain.mjs';
import { Archive } from '../src/archive.mjs';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createHmac } from 'node:crypto';

async function controlled(provider, change) {
  const response = await fetch(provider.base + '/_control', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(change) });
  assert.equal(response.status, 200);
}

test('来源执行真实回调验证并按原始推送体签名', async () => {
 const provider=await createProvider();const deliveries=[];
 const callback=createServer(async(request,response)=>{
  const url=new URL(request.url,'http://local.invalid');
  if(request.method==='GET'){response.end(url.searchParams.get('hub.challenge'));return;}
  const pieces=[];for await(const piece of request)pieces.push(piece);
  const bytes=Buffer.concat(pieces);deliveries.push({bytes,signature:request.headers['x-hub-signature-256']});
  response.writeHead(204).end();
 });
 callback.listen(0,'127.0.0.1');await once(callback,'listening');
 try {
  const form=new URLSearchParams({'hub.mode':'subscribe','hub.topic':'http://local.invalid/topics/catalog','hub.callback':`http://127.0.0.1:${callback.address().port}/receive`,'hub.secret':'callback-secret'});
  const subscription=await(await fetch(provider.base+'/hub',{method:'POST',body:form})).json();
  await controlled(provider,{clock:1010});
  assert.equal((await(await fetch(provider.base+'/verify/'+subscription.id,{method:'POST'})).json()).status,'active');
  const leases=await(await fetch(provider.base+'/subscriptions')).json();assert.equal(leases[0].expires,1040);
  await fetch(provider.base+'/deliver/'+subscription.id,{method:'POST'});
  assert.equal(deliveries.length,1);assert.equal(deliveries[0].signature,'sha256='+createHmac('sha256','callback-secret').update(deliveries[0].bytes).digest('hex'));
 } finally {await provider.close();await new Promise(resolve=>callback.close(resolve));callback.closeAllConnections();}
});

test('现有业务入口真实收存来源资料并可重新打开', async () => {
  const provider = await createProvider();
  const directory = await mkdtemp(join(tmpdir(), 'business-'));
  try {
    const result = await receiveCurrent(provider.base, directory);
    assert.ok(result);
    const archive = new Archive(directory);
    const receipts = await archive.list();
    assert.ok(receipts.length > 0);
    for (const receipt of receipts) assert.ok((await archive.read(receipt)).length > 0);
    assert.ok(provider.audit.some(request => !request.path.startsWith('/_')));
  } finally { await provider.close(); await rm(directory, { recursive: true, force: true }); }
});

test('提交订阅不等于完成验证', async () => {
 const provider = await createProvider();
 try {
const response=await fetch(provider.base+'/hub',{method:'POST',headers:{'content-type':'application/x-www-form-urlencoded'},body:new URLSearchParams({'hub.mode':'subscribe','hub.topic':'http://local.invalid/topics/catalog','hub.callback':'http://127.0.0.1:1234/callback','hub.secret':'demo'})});assert.equal(response.status,202);assert.equal((await response.json()).status,'pending');
 } finally { await provider.close(); }
});

test('订阅状态不泄露共享密钥', async () => {
 const provider = await createProvider();
 try {
await fetch(provider.base+'/hub',{method:'POST',body:new URLSearchParams({'hub.mode':'subscribe','hub.topic':'http://local.invalid/topics/catalog','hub.callback':'http://127.0.0.1:1234/callback','hub.secret':'never-print-this'})});assert.doesNotMatch(await(await fetch(provider.base+'/subscriptions')).text(),/never-print-this/);
 } finally { await provider.close(); }
});
