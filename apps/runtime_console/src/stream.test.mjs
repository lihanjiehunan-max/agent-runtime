import {test} from 'node:test';
import assert from 'node:assert/strict';
import {SSEParser} from './stream.mjs';

test('SSE parser preserves split frames and ignores heartbeat comments',()=>{
  const p=new SSEParser();
  assert.deepEqual(p.feed(': heartbeat\n\nid: 12\nevent: model.de'),[]);
  assert.deepEqual(p.feed('lta\ndata: {"text":"船舶"}\n\n'),
    [{id:'12',event:'model.delta',data:'{"text":"船舶"}'}]);
});
test('SSE parser accepts CRLF across chunks and multiline data',()=>{
  const p=new SSEParser();
  assert.deepEqual(p.feed('id: 3\r'),[]);
  assert.deepEqual(p.feed('\ndata: first\r\ndata: second\r\n\r\n'),
    [{id:'3',event:'message',data:'first\nsecond'}]);
});
test('SSE parser refuses unbounded unterminated frames',()=>{
  const p=new SSEParser();
  assert.throws(()=>p.feed('x'.repeat(300000)),/large/);
});
