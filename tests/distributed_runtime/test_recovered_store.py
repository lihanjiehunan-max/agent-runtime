import pytest
from apps.distributed_runtime.store import Store, Conflict, LostLease
from apps.distributed_runtime.artifacts import FileArtifacts


def package(agent='chat', **extra):
    return dict(agent_id=agent, version='1',engine='deepagents', engine_version='0.7.7',
                prompt='You are a helpful assistant.', capabilities=[], timeout=90, **extra)

@pytest.fixture
def store(tmp_path):
    s=Store('sqlite:///'+str(tmp_path/'runtime.db'),lease_seconds=2)
    s.deploy(package())
    yield s
    s.db.close()

def test_session_idempotency_and_single_writer(store):
    ses=store.create_session('chat')
    e=store.submit(ses['id'],'key',{'message':'hello'})
    assert store.submit(ses['id'],'key',{'message':'hello'})['id']==e['id']
    with pytest.raises(Conflict):store.submit(ses['id'],'key',{'message':'changed'})
    with pytest.raises(Conflict):store.submit(ses['id'],'other',{'message':'hello'})
    for name in ['a','b']:store.register_worker(name,['deepagents'],[],2)
    a=store.claim('a');assert a.execution_id==e['id']
    assert store.claim('b') is None
    store.interrupt(a)
    store.resume(e['id']);b=store.claim('b')
    with pytest.raises(LostLease):store.emit(a,'stale',{})
    assert b.epoch>a.epoch

def test_early_child_completion_does_not_lose_wakeup(store,tmp_path):
    store.deploy(package('child'))
    e=store.submit(store.create_session('chat')['id'],'parent',{'message':'delegate'})
    store.register_worker('a',['deepagents'],[],2);a=store.claim('a')
    specs=[{'agent_id':'child','version':'1','input':{'message':'one'}}]
    kids=store.spawn(a,'call-1',specs)
    assert store.spawn(a,'call-1',specs)[0]['id']==kids[0]['id']
    kid=store.claim('a');assert kid.execution_id==kids[0]['id']
    store.complete(kid,FileArtifacts(tmp_path/'artifacts').put_json({'message':'done'}))
    store.wait_children(a,'call-1')
    assert store.execution(e['id'])['status']=='QUEUED'
    assert store.claim('a').execution_id==e['id']

def test_missing_runtime_components():
    from pathlib import Path
    root=Path(__file__).resolve().parents[2]/'apps'/'distributed_runtime'
    assert all((root/(name+'.py')).exists() for name in ['checkpoints','harness','worker','api'])

def test_checkpoint_survives_reopening_and_rejects_stale_writer(store):
    from apps.distributed_runtime.checkpoints import FencedSQLSaver
    from langgraph.checkpoint.base import empty_checkpoint
    s=store.create_session('chat'); e=store.submit(s['id'],'cp',{'message':'hello'})
    store.register_worker('a',['deepagents'],[],1);a=store.claim('a')
    saver=FencedSQLSaver(store,a)
    cfg={'configurable':{'thread_id':s['id'],'checkpoint_ns':''}}
    cp=empty_checkpoint();cp['channel_values']={'memo':'private-memory'}
    saved=saver.put(cfg,cp,{'step':0},{})
    saver.put_writes(saved,[('memo','write-before-restart')],'task')
    store.interrupt(a);store.resume(e['id'])
    store.register_worker('b',['deepagents'],[],1);b=store.claim('b')
    reloaded=FencedSQLSaver(store,b).get_tuple(cfg)
    assert reloaded.checkpoint['channel_values']['memo']=='private-memory'
    assert reloaded.pending_writes[0][2]=='write-before-restart'
    with pytest.raises(LostLease):saver.put(cfg,empty_checkpoint(),{'step':1},{})
