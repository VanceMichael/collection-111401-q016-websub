import { createHmac } from 'node:crypto';
export const initialState = { generation: 1, mode: 'normal', clock: 1000, sequence: 0, subscriptions: {} };
export async function stop(state) {  }
export async function routes(context) {
 const { request, url, body, payload, state, json, text, binary } = context;

 if(url.pathname==='/publisher')return json({publisher:'museum-a',topic:'http://local.invalid/topics/catalog',hub:'/hub',leaseSeconds:30});
 if(url.pathname==='/hub'&&request.method==='POST'){
  const form=new URLSearchParams(body.toString());const callback=form.get('hub.callback');const mode=form.get('hub.mode');const topic=form.get('hub.topic');
  if(!['subscribe','unsubscribe'].includes(mode)||!callback||topic!=='http://local.invalid/topics/catalog')return json({error:'订阅参数不完整'},400);
  const id='subscription-'+(++state.sequence);const challenge='verify-'+id;state.subscriptions[id]={id,callback,mode,topic,secret:form.get('hub.secret'),challenge,status:'pending',expires:state.clock+30};return json({id,status:'pending',verify:'/verify/'+id},202);
 }
 if(url.pathname.startsWith('/verify/')&&request.method==='POST'){
  const subscription=state.subscriptions[url.pathname.split('/').at(-1)];if(!subscription)return json({error:'无订阅'},404);
  const target=new URL(subscription.callback);if(!['127.0.0.1','localhost'].includes(target.hostname))return json({error:'仅允许本地回调'},400);
  for(const [key,value]of Object.entries({'hub.mode':subscription.mode,'hub.topic':subscription.topic,'hub.challenge':subscription.challenge,'hub.lease_seconds':'30'}))target.searchParams.set(key,value);
  const response=await fetch(target,{signal:AbortSignal.timeout(2000)});const answer=await response.text();subscription.status=response.ok&&answer===subscription.challenge?(subscription.mode==='subscribe'?'active':'stopped'):'failed';if(subscription.status==='active')subscription.expires=state.clock+30;return json({id:subscription.id,status:subscription.status});
 }
 if(url.pathname.startsWith('/deliver/')&&request.method==='POST'){
  const subscription=state.subscriptions[url.pathname.split('/').at(-1)];if(!subscription)return json({error:'无订阅'},404);
  const bytes=Buffer.from(JSON.stringify({publisher:'museum-a',id:'record-'+state.generation,edition:state.generation,title:'公开馆藏'}));
  const signature=createHmac('sha256',subscription.secret??'').update(bytes).digest('hex');
  const target=new URL(subscription.callback);if(!['127.0.0.1','localhost'].includes(target.hostname))return json({error:'仅允许本地回调'},400);
  const response=await fetch(target,{method:'POST',headers:{'content-type':'application/json','x-hub-signature-256':'sha256='+signature},body:bytes,signal:AbortSignal.timeout(2000)});return json({status:response.status,subscription:subscription.id});
 }
 if(url.pathname==='/subscriptions')return json(Object.values(state.subscriptions).map(({secret,challenge,...subscription})=>subscription));

}
