"""Deterministic HTTP model and business-tool fixture; NEVER a production model.

Only responses are scripted. Runtime scheduling, SQL checkpoints, Redis, S3,
HTTP transport and the DeepAgents graph are real in the acceptance environment.
"""
import asyncio
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse

app=FastAPI(title='TEST ONLY: deterministic model/tool fixture')
DB=os.environ.get('FIXTURE_DB','/tmp/dh-fixture.db')
Path(DB).parent.mkdir(parents=True,exist_ok=True)
with sqlite3.connect(DB) as c:
    c.execute('CREATE TABLE IF NOT EXISTS effects (key TEXT PRIMARY KEY,value TEXT,result TEXT,requests INTEGER)')

@app.get('/healthz')
def health():return {'status':'ok','fixture':True}

@app.get('/effects')
def effects():
    with sqlite3.connect(DB) as c:
        c.row_factory=sqlite3.Row
        return [dict(x) for x in c.execute('SELECT * FROM effects')]

@app.post('/tools/{name}/invoke')
def invoke(name:str,body:dict,idempotency_key:str=Header(alias='Idempotency-Key')):
    args=body['arguments']
    if name=='query_metric':return {'metric':args['metric'],'value':110,'unit':'million'}
    if name!='record_metric':raise HTTPException(404)
    value=args['value']
    with sqlite3.connect(DB,timeout=10) as c:
        c.execute('BEGIN IMMEDIATE')
        old=c.execute('SELECT result FROM effects WHERE key=?',(idempotency_key,)).fetchone()
        if old:
            c.execute('UPDATE effects SET requests=requests+1 WHERE key=?',(idempotency_key,))
            result=json.loads(old[0])
        else:
            result={'receipt':'receipt-'+idempotency_key[:12],'value':value}
            c.execute('INSERT INTO effects VALUES (?,?,?,1)',(idempotency_key,value,json.dumps(result)))
    if value.startswith('drop:') and not old:
        raise HTTPException(503,'Injected response loss AFTER committed business effect')
    return result

@app.post('/v1/chat/completions')
async def completion(body:dict):
    messages=body['messages']
    users=[m['content'] for m in messages if m['role']=='user']
    current=users[-1] if users else ''
    last_user=max((i for i,m in enumerate(messages) if m['role']=='user'),default=-1)
    receipts=[m.get('content','') for m in messages[last_user+1:] if m['role']=='tool']
    calls=[]
    if not receipts and current.startswith('TEAM:'):
        calls=[{'id':'delegate-'+hashlib.sha256(current.encode()).hexdigest()[:12],'type':'function',
                'function':{'name':'delegate','arguments':json.dumps({'children':[
                    {'agent_id':'child','version':'1','input':{'message':'SLOW:0.3:child-A:'+current}},
                    {'agent_id':'child','version':'1','input':{'message':'SLOW:0.3:child-B:'+current}}]})}}]
    elif not receipts and current.startswith('WRITE:'):
        calls=[{'id':'write-'+hashlib.sha256(current.encode()).hexdigest()[:12],'type':'function',
                'function':{'name':'record_metric','arguments':json.dumps({'value':current[6:]})}}]
    elif not receipts and current.startswith('METRIC:'):
        calls=[{'id':'metric-'+hashlib.sha256(current.encode()).hexdigest()[:12],'type':'function',
                'function':{'name':'query_metric','arguments':json.dumps({'metric':current[7:]})}}]
    delay=0
    if current.startswith('SLOW:'):
        try:delay=min(60,max(0,float(current.split(':',2)[1])))
        except ValueError: pass
    text='complete: '+receipts[-1] if receipts else 'users: '+json.dumps(users,ensure_ascii=False)
    stamp=int(time.time())
    ident='chatcmpl-fixture-'+hashlib.sha256(current.encode()).hexdigest()[:12]
    def chunk(delta,finish=None):
        return {'id':ident,'object':'chat.completion.chunk','created':stamp,'model':body.get('model','fixture'),
                'choices':[{'index':0,'delta':delta,'finish_reason':finish}]}
    async def stream():
        yield 'data: '+json.dumps(chunk({'role':'assistant'}))+'\n\n'
        if delay: await asyncio.sleep(delay)
        if calls:
            yield 'data: '+json.dumps(chunk({'tool_calls':[dict(c,index=i) for i,c in enumerate(calls)]}))+'\n\n'
            yield 'data: '+json.dumps(chunk({},'tool_calls'))+'\n\n'
        else:
            for start in range(0,len(text),24):
                await asyncio.sleep(.015)
                yield 'data: '+json.dumps(chunk({'content':text[start:start+24]}))+'\n\n'
            yield 'data: '+json.dumps(chunk({},'stop'))+'\n\n'
        yield 'data: [DONE]\n\n'
    if body.get('stream'):
        return StreamingResponse(stream(),media_type='text/event-stream')
    if delay:await asyncio.sleep(delay)
    return {'id':ident,'object':'chat.completion','created':stamp,'model':body.get('model','fixture'),
            'choices':[{'index':0,'message':{'role':'assistant','content':text if not calls else '',
                **({'tool_calls':calls} if calls else {})},'finish_reason':'tool_calls' if calls else 'stop'}],
            'usage':{'prompt_tokens':1,'completion_tokens':1,'total_tokens':2}}
