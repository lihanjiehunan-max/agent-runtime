import asyncio
import httpx
import pytest
from apps.distributed_runtime.api import create_app
from apps.distributed_runtime.config import Settings
from apps.distributed_runtime.store import Store
from apps.distributed_runtime.artifacts import FileArtifacts
from .test_recovered_store import package

@pytest.fixture
def runtime(tmp_path):
    config=Settings('sqlite:///'+str(tmp_path/'api.db'),'business-token-000000',
                    artifact_dir=str(tmp_path/'objects'))
    # Assignment keeps RED about the authorization behavior, not construction.
    config.ops_token='operations-token-000000'
    app=create_app(config)
    yield app, config, FileArtifacts(tmp_path/'objects')
    app.state.store.db.close()

@pytest.mark.asyncio
async def test_operations_credential_cannot_be_replaced_by_business_token(runtime):
    app,cfg,_=runtime
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://api') as client:
        client.headers['Authorization']='Bearer '+cfg.token
        assert (await client.post('/api/v1/runtime/ops/agents/deploy',json=package())).status_code==401
        client.headers['Authorization']='Bearer '+cfg.ops_token
        assert (await client.post('/api/v1/runtime/ops/agents/deploy',json=package())).status_code==200
        assert (await client.post('/api/v1/runtime/agents/chat/sessions')).status_code==201

@pytest.mark.asyncio
async def test_sync_timeout_leaves_durable_task_and_can_be_replayed(runtime):
    app,cfg,objects=runtime
    store=app.state.store; store.deploy(package())
    sid=store.create_session('chat')['id']
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://api',
        headers={'Authorization':'Bearer '+cfg.token}) as client:
        path='/api/v1/runtime/sessions/'+sid+'/invoke?wait_seconds=0'
        r=await client.post(path,json={'message':'hello','request_key':'sync-key'})
        assert r.status_code==202
        eid=r.json()['execution_id']
        store.register_worker('w',['deepagents'],[],1); claim=store.claim('w')
        store.complete(claim,objects.put_json({'message':'answer','session_id':sid}))
        r=await client.post(path,json={'message':'hello','request_key':'sync-key'})
        assert r.status_code==200 and r.json()['result']['message']=='answer'
        history=(await client.get('/api/v1/runtime/sessions/'+sid+'/history')).json()
        assert history['items'][0]['execution_id']==eid
        assert history['items'][0]['input']=='hello'
        assert history['items'][0]['result']['message']=='answer'
        assert 'instance_id' not in str(history)

@pytest.mark.asyncio
async def test_operator_trace_pagination_metrics_and_drain(runtime):
    app,cfg,objects=runtime
    store=app.state.store; ins=store.deploy(package()); sid=store.create_session('chat')['id']
    e=store.submit(sid,'task',{'message':'hello'})
    store.register_worker('w',['deepagents'],[],1); claim=store.claim('w')
    store.emit(claim,'model.delta',{'text':'hello'})
    store.complete(claim,objects.put_json({'message':'answer'}))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://api',
        headers={'Authorization':'Bearer '+cfg.ops_token}) as client:
        path='/api/v1/runtime/ops/executions/'+e['id']+'/trace'
        r=await client.get(path+'?limit=2'); assert r.status_code==200
        trace=r.json(); assert trace['packages'][0]['digest']==ins['digest']
        assert trace['thread_id']==sid and len(trace['events'])==2
        next_page=(await client.get(path+'?after='+str(trace['next_cursor']))).json()
        assert all(x['id']>trace['next_cursor'] for x in next_page['events'])
        m=await client.get('/metrics'); assert m.status_code==200
        assert 'runtime_executions{status="COMPLETED"} 1' in m.text
        r=await client.post('/api/v1/runtime/ops/workers/w/drain',json={'draining':True,'reason':'rollout'})
        assert r.status_code==200
        store.submit(store.create_session('chat')['id'],'pending',{'message':'later'})
        assert store.claim('w') is None
        await client.post('/api/v1/runtime/ops/workers/w/drain',json={'draining':False,'reason':'resume'})
        assert store.claim('w') is not None

@pytest.mark.asyncio
async def test_package_activation_keeps_existing_session_pinned(runtime):
    app,cfg,_=runtime; store=app.state.store
    first=store.deploy(package()); old=store.create_session('chat')
    second=store.deploy(dict(package(),version='2'))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://api',
        headers={'Authorization':'Bearer '+cfg.ops_token}) as client:
        r=await client.post('/api/v1/runtime/ops/agents/chat/activate',json={'version':'1','reason':'rollback'})
        assert r.status_code==200
        assert store.create_session('chat')['instance_id']==first['id']
        assert store.session(old['id'])['instance_id']==first['id']
        assert store.create_session('chat','2')['instance_id']==second['id']

@pytest.mark.asyncio
async def test_bad_package_is_a_client_error_and_secret_fields_are_rejected(runtime):
    app,cfg,_=runtime
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://api',
        headers={'Authorization':'Bearer '+cfg.token}) as client:
        # The legacy shared-token mode is used here to isolate package validation.
        cfg.ops_token=''
        for body in [dict(package(),agent_id=7),dict(package(),prompt={}),dict(package(),api_key='secret')]:
            r=await client.post('/api/v1/runtime/ops/agents/deploy',json=body)
            assert r.status_code in (409,422), r.text
