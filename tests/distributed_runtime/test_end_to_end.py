import asyncio
import os
import socket
import subprocess
import sys
import time
import httpx
import pytest
from apps.distributed_runtime.store import Store
from apps.distributed_runtime.artifacts import FileArtifacts
from .test_recovered_store import package

@pytest.fixture(scope='module')
def gateway(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    tmp=tmp_path_factory.mktemp('gateway')
    log=(tmp/'server.log').open('w')
    env=dict(os.environ,FIXTURE_DB=str(tmp/'effects.db'))
    proc=subprocess.Popen([sys.executable,'-m','uvicorn','tests.distributed_runtime.gateway:app',
        '--host','127.0.0.1','--port',str(port)],env=env,stdout=log,stderr=log)
    url=f'http://127.0.0.1:{port}'
    try:
        for _ in range(100):
            try:
                if httpx.get(url+'/healthz',timeout=.2).status_code==200: break
            except httpx.HTTPError: pass
            if proc.poll() is not None: pytest.fail((tmp/'server.log').read_text())
            time.sleep(.05)
        else: pytest.fail('Fixture gateway failed to start')
        yield url
    finally:
        proc.terminate();proc.wait(timeout=10);log.close()

async def finish(store,eid,timeout=25):
    start=time.monotonic()
    while time.monotonic()-start<timeout:
        e=await asyncio.to_thread(store.execution,eid)
        if e['status']=='COMPLETED': return e
        assert e['status'] not in {'INTERRUPTED','FAILED','CANCELLED','TIMED_OUT'}, e
        await asyncio.sleep(.1)
    pytest.fail(str(store.execution(eid)))

@pytest.mark.asyncio
async def test_actual_harness_memory_and_delegation(gateway,tmp_path):
    from apps.distributed_runtime.harness import DeepAgentsHarness
    from apps.distributed_runtime.worker import Worker
    store=Store('sqlite:///'+str(tmp_path/'rt.db'),lease_seconds=3)
    artifacts=FileArtifacts(tmp_path/'artifacts')
    store.deploy(package('chat',allowed_agents=['child']))
    store.deploy(package('child'))
    harness=DeepAgentsHarness(store,artifacts,model_url=gateway+'/v1',model_key='fixture-only')
    wa=Worker(store,harness,worker_id='node-a',slots=1)
    wb=Worker(store,harness,worker_id='node-b',slots=1)
    runners=[asyncio.create_task(w.run()) for w in (wa,wb)]
    try:
        s=store.create_session('chat')
        first=store.submit(s['id'],'one',{'message':'remember:blue-harbor'})
        await finish(store,first['id'])
        wa.stop_event.set();await runners[0]
        second=store.submit(s['id'],'two',{'message':'what did I say?'})
        done=await finish(store,second['id'])
        assert 'blue-harbor' in artifacts.get_json(done['output_ref'])['message']
        assert store.attempts(second['id'])[0]['worker_id']=='node-b'
        team=store.submit(store.create_session('chat')['id'],'team',{'message':'TEAM:report'})
        done=await finish(store,team['id'])
        tree=store.tree(team['id'])
        assert len(tree)==3 and all(e['status']=='COMPLETED' for e in tree)
        events=store.events(team['id'])
        assert any(e['type']=='execution.waiting_children' for e in events)
        assert any(e['type']=='team.ready' for e in events)
        assert any(e['type']=='model.delta' for e in events)
        assert len(store.attempts(team['id']))==2
    finally:
        for w in (wa,wb): w.stop_event.set()
        await asyncio.gather(*runners,return_exceptions=True)
        store.db.close()

@pytest.mark.asyncio
async def test_stateless_api_and_auth(tmp_path):
    from apps.distributed_runtime.api import create_app
    from apps.distributed_runtime.config import Settings
    config=Settings('sqlite:///'+str(tmp_path/'api.db'),'test-token-minimum-16',artifact_dir=str(tmp_path/'artifacts'))
    app=create_app(config)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://api') as client:
        path='/api/v1/runtime'
        assert (await client.get(path+'/ops/workers')).status_code==401
        client.headers['Authorization']='Bearer '+config.token
        assert (await client.post(path+'/ops/agents/deploy',json=package())).status_code==200
        s=(await client.post(path+'/agents/chat/sessions')).json()
        assert 'instance_id' not in s
        r=await client.post(path+'/sessions/'+s['session_id']+'/executions',json={'message':'hello','request_key':'api-one'})
        assert r.status_code==202
        e=r.json()
        assert (await client.get(path+'/executions/'+e['execution_id'])).json()['status']=='QUEUED'
    app.state.store.db.close()

@pytest.mark.asyncio
async def test_uncertain_external_effect_blocks_resume(gateway,tmp_path):
    from apps.distributed_runtime.harness import DeepAgentsHarness
    from apps.distributed_runtime.worker import Worker
    from apps.distributed_runtime.tools import EffectLedger
    from apps.distributed_runtime.store import Conflict
    store=Store('sqlite:///'+str(tmp_path/'effect.db'),lease_seconds=3)
    artifacts=FileArtifacts(tmp_path/'artifacts')
    store.deploy(package('writer',tools=['record_metric']))
    harness=DeepAgentsHarness(store,artifacts,model_url=gateway+'/v1',model_key='fixture-only',tool_url=gateway)
    w=Worker(store,harness,worker_id='write-node',slots=1)
    runner=asyncio.create_task(w.run())
    try:
        e=store.submit(store.create_session('writer')['id'],'write',{'message':'WRITE:drop:local-test'})
        for _ in range(200):
            if store.execution(e['id'])['status']=='INTERRUPTED': break
            await asyncio.sleep(.1)
        assert store.execution(e['id'])['status']=='INTERRUPTED'
        ledger=EffectLedger(store)
        receipt=ledger.list(e['id'])[0]
        assert receipt['status']=='UNKNOWN'
        with pytest.raises(Conflict): store.resume(e['id'])
        remote=httpx.get(gateway+'/effects').json()[-1]
        import json
        ledger.reconcile(e['id'],receipt['call_id'],True,json.loads(remote['result']),'Verified test fixture business receipt')
        store.resume(e['id']);await finish(store,e['id'])
        rows=httpx.get(gateway+'/effects').json()
        assert next(x for x in rows if x['key']==remote['key'])['requests']==1
    finally:
        w.stop_event.set();await runner;store.db.close()


@pytest.mark.asyncio
async def test_actual_harness_large_child_result_is_paged(gateway,tmp_path):
    from apps.distributed_runtime.harness import DeepAgentsHarness
    from apps.distributed_runtime.worker import Worker
    from apps.distributed_runtime.store import Claim
    store=Store('sqlite:///'+str(tmp_path/'large.db'),lease_seconds=6)
    artifacts=FileArtifacts(tmp_path/'artifacts')
    store.deploy(package('parent',allowed_agents=['child']));store.deploy(package('child'))
    harness=DeepAgentsHarness(store,artifacts,model_url=gateway+'/v1',model_key='fixture-only')
    worker=Worker(store,harness,worker_id='large-result-worker',slots=1)
    runner=asyncio.create_task(worker.run())
    try:
        sid=store.create_session('parent')['id']
        e=store.submit(sid,'large-result',{'message':'TEAM_LARGE:report'})
        done=await finish(store,e['id'])
        assert 'tail-proof' in artifacts.get_json(done['output_ref'])['message']
        events=store.events(e['id'])
        assert any(x['type']=='child_result.summarized' for x in events)
        assert any(x['type']=='child_result.read' and x['payload']['offset']==80000 for x in events)
        graph=harness.build(Claim(e['id'],'large-result-worker',done['epoch']))
        # Let LangGraph reconstruct delta-channel snapshots via its public API.
        state=await graph.aget_state({'configurable':{'thread_id':sid}})
        tool_messages=[m for m in state.values['messages'] if getattr(m,'type',None)=='tool']
        delegated=next(m for m in tool_messages if m.name=='delegate')
        assert len(delegated.content.encode())<20000
        assert 'tail-proof' not in delegated.content
        assert any(m.name=='read_child_result' and 'tail-proof' in m.content for m in tool_messages)
    finally:
        worker.stop_event.set();await runner;store.db.close()
