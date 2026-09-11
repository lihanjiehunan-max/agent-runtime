import asyncio
import os
import socket
import subprocess
import sys
import time
import httpx
import pytest
from apps.distributed_runtime.store import Store, Conflict
from apps.distributed_runtime.tools import EffectLedger, HttpToolGateway
from .test_recovered_store import package

@pytest.fixture(scope='module')
def tool_server(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
    folder = tmp_path_factory.mktemp('delivery-gateway')
    with (folder/'server.log').open('w') as log:
        proc = subprocess.Popen([sys.executable, '-m', 'uvicorn',
            'tests.distributed_runtime.delivery_gateway:app', '--host', '127.0.0.1', '--port', str(port)],
            env=dict(os.environ, FIXTURE_DB=str(folder/'effects.db')), stdout=log, stderr=log)
        url = f'http://127.0.0.1:{port}'
        try:
            for _ in range(100):
                try:
                    if httpx.get(url+'/healthz', timeout=.2).status_code == 200: break
                except httpx.HTTPError: pass
                if proc.poll() is not None: pytest.fail((folder/'server.log').read_text())
                time.sleep(.05)
            else: pytest.fail('Contract fixture did not start')
            yield url
        finally:
            proc.terminate(); proc.wait(timeout=10)

@pytest.fixture
def owned(tmp_path):
    store = Store('sqlite:///'+str(tmp_path/'tools.db'), lease_seconds=20)
    store.deploy(package('metric'))
    e = store.submit(store.create_session('metric')['id'], 'call', {'message':'test'})
    store.register_worker('worker', ['deepagents'], [], 1)
    claim = store.claim('worker')
    yield store, claim
    store.db.close()

@pytest.mark.asyncio
async def test_tool_credential_and_business_parameters_are_propagated(tool_server, owned):
    store, claim = owned
    gateway = HttpToolGateway(EffectLedger(store), tool_server, token='separate-tool-secret')
    args = {'metric':'revenue','period':'2026-08','org':'shipping','group_by':['route'],'comparison':'yoy'}
    result = await gateway.invoke(claim, 'call-1', 'query_metric', args)
    assert result['arguments'] == args
    assert result['execution_id'] == claim.execution_id
    assert result['call_id'] == 'call-1'
    # A receipt replay must keep its original business identity.
    assert await gateway.invoke(claim, 'call-1', 'query_metric', args) == result
    assert len(EffectLedger(store).list(claim.execution_id)) == 1

@pytest.mark.asyncio
async def test_failed_read_is_retryable_but_never_misclassified_as_unknown_write(tool_server, owned):
    store, claim = owned
    gateway = HttpToolGateway(EffectLedger(store), tool_server, token='separate-tool-secret')
    with pytest.raises(Exception):
        await gateway.invoke(claim, 'read-1', 'query_metric', {'metric':'unavailable'})
    assert EffectLedger(store).list(claim.execution_id)[0]['status'] == 'NOT_EXECUTED'

@pytest.mark.asyncio
async def test_nonobject_tool_receipt_is_rejected(tool_server, owned):
    store, claim = owned
    gateway = HttpToolGateway(EffectLedger(store), tool_server, token='separate-tool-secret')
    with pytest.raises(Conflict, match='object'):
        await gateway.invoke(claim, 'bad', 'query_metric', {'metric':'badjson'})
    assert EffectLedger(store).list(claim.execution_id)[0]['status'] != 'SUCCEEDED'

@pytest.mark.asyncio
async def test_gateway_rejects_unregistered_tools_before_any_http(owned, tool_server):
    store, claim = owned
    with pytest.raises(Conflict, match='registered'):
        await HttpToolGateway(EffectLedger(store), tool_server).invoke(claim, 'bad', 'arbitrary_tool', {})
    assert EffectLedger(store).list(claim.execution_id) == []

@pytest.mark.asyncio
async def test_real_harness_accepts_separate_tool_credential(tool_server, tmp_path):
    from apps.distributed_runtime.artifacts import FileArtifacts
    from apps.distributed_runtime.harness import DeepAgentsHarness
    from apps.distributed_runtime.worker import Worker
    from .test_end_to_end import finish
    store = Store('sqlite:///'+str(tmp_path/'graph.db'), lease_seconds=6)
    artifacts = FileArtifacts(tmp_path/'objects')
    store.deploy(package('metric', tools=['query_metric']))
    harness = DeepAgentsHarness(store, artifacts, model_url=tool_server+'/v1', model_key='model-fixture-secret',
        tool_url=tool_server, tool_token='separate-tool-secret')
    worker = Worker(store, harness, slots=1)
    runner = asyncio.create_task(worker.run())
    try:
        e = store.submit(store.create_session('metric')['id'], 'one', {'message':'METRIC:revenue'})
        await finish(store, e['id'])
        assert EffectLedger(store).list(e['id'])[0]['status'] == 'SUCCEEDED'
        events=store.events(e['id'])
        assert any(x['type']=='checkpoint.saved' for x in events)
        assert any(x['type']=='model.call.started' for x in events)
        assert any(x['type']=='model.call.completed' for x in events)
        assert all('model-fixture-secret' not in str(x) and 'separate-tool-secret' not in str(x) for x in events)
    finally:
        worker.stop_event.set(); await runner; store.db.close()
