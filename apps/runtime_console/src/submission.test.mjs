import test from 'node:test';
import assert from 'node:assert/strict';
import {existsSync} from 'node:fs';

async function registry(){
  assert.ok(existsSync(new URL('./submission.mjs',import.meta.url)),
    'A single mutable requestKey is lost when switching sessions');
  const {PendingSubmissions}=await import('./submission.mjs');
  let count=0; return new PendingSubmissions(()=>`key-${++count}`);
}
test('lost response keeps a stable key through another session and back',async()=>{
  const r=await registry(),keys=[];
  const post=async body=>{keys.push(body.request_key);if(keys.length===1)throw new TypeError('response lost');return {execution_id:'e1'}};
  await assert.rejects(r.submit('s1','hello',post));
  await r.submit('s2','elsewhere',async()=>({execution_id:'e2'}));
  assert.equal(r.pending('s1').message,'hello');
  assert.deepEqual(await r.submit('s1','hello',post),{execution_id:'e1'});
  assert.equal(keys[0],keys[1]); assert.equal(r.pending('s1'),null);
});
test('concurrent clicks share one in-flight POST',async()=>{
  const r=await registry();let calls=0,finish;
  const post=()=>{calls++;return new Promise(resolve=>{finish=resolve})};
  const one=r.submit('s','a',post),two=r.submit('s','a',post);
  await Promise.resolve();finish({execution_id:'e'});
  assert.deepEqual(await one,await two);assert.equal(calls,1);
});
test('different input cannot overwrite an ambiguous previous submission',async()=>{
  const r=await registry();await assert.rejects(r.submit('s','first',async()=>{throw new TypeError('lost')}));
  let sent=false;
  await assert.rejects(r.submit('s','different',async()=>{sent=true}),/确认/);
  assert.equal(sent,false);assert.equal(r.pending('s').message,'first');
});
test('definitive client rejection releases a key but server failure does not',async()=>{
  const r=await registry();
  await assert.rejects(r.submit('s','bad',async()=>{throw Object.assign(new Error('bad'),{status:422})}));
  assert.equal(r.pending('s'),null);
  await assert.rejects(r.submit('s','a',async()=>{throw Object.assign(new Error('server'),{status:503})}));
  const key=r.pending('s').key;
  await r.submit('s','a',async body=>{assert.equal(body.request_key,key);return {execution_id:'e'}});
});
test('acknowledged identical future message is a new user operation',async()=>{
  const r=await registry(),keys=[];const post=async b=>{keys.push(b.request_key);return {execution_id:b.request_key}};
  await r.submit('s','same',post);await r.submit('s','same',post);assert.notEqual(keys[0],keys[1]);
});
test('logout clears memory and an old response cannot remove a new pending request',async()=>{
  const r=await registry();let finish;
  const old=r.submit('s','hello',()=>new Promise(resolve=>{finish=resolve}));
  await Promise.resolve();r.clear();assert.equal(r.pending('s'),null);
  await assert.rejects(r.submit('s','new',async()=>{throw new TypeError('lost')}));
  finish({execution_id:'old'});await old;
  assert.equal(r.pending('s').message,'new');
});

test('truncated or malformed acknowledgement preserves the request key',async()=>{
  const r=await registry();await assert.rejects(r.submit('s','a',async()=>({status:'QUEUED'})),/回执/);
  const key=r.pending('s').key;
  await r.submit('s','a',async b=>{assert.equal(b.request_key,key);return {execution_id:'e'}});
});
test('HTTP timeout and throttling do not erase a potentially accepted request',async()=>{
  for(const status of [408,425,429]){
    const r=await registry();await assert.rejects(r.submit('s','a',async()=>{throw Object.assign(new Error('retry'),{status})}));
    assert.equal(r.pending('s').message,'a');
  }
});
