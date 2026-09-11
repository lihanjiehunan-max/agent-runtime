import pytest
import sqlalchemy as sa
from langgraph.checkpoint.base import empty_checkpoint
from apps.distributed_runtime.store import Store, Conflict
from apps.distributed_runtime.checkpoints import FencedSQLSaver
from apps.distributed_runtime import db as t
from .test_recovered_store import package

@pytest.fixture
def checkpoint(tmp_path):
    store=Store('sqlite:///'+str(tmp_path/'checkpoint.db'),lease_seconds=20)
    store.deploy(package()); sid=store.create_session('chat')['id']
    store.submit(sid,'task',{'message':'remember'})
    store.register_worker('w',['deepagents'],[],1); claim=store.claim('w')
    saver=FencedSQLSaver(store,claim)
    cfg={'configurable':{'thread_id':sid,'checkpoint_ns':''}}
    cp=empty_checkpoint(); cp['channel_values']={'memo':'unchanged'}
    saved=saver.put(cfg,cp,{'step':0},{})
    saver.put_writes(saved,[('memo','receipt')],'task')
    yield store,saver,saved
    store.db.close()

@pytest.mark.parametrize('field,value',[('parent_id','other-parent'),('execution_id','other-execution'),
                                       ('meta_type','other-serializer')])
def test_checkpoint_identity_tampering_is_detected_before_decode(checkpoint,field,value):
    store,saver,cfg=checkpoint
    with store.db.tx() as c: c.execute(t.checkpoints.update().values(**{field:value}))
    with pytest.raises(Conflict,match='integrity'): saver.get_tuple(cfg)

def test_pending_write_tampering_is_detected(checkpoint):
    store,saver,cfg=checkpoint
    with store.db.tx() as c: c.execute(t.checkpoint_writes.update().values(channel='forged'))
    with pytest.raises(Conflict,match='integrity'): saver.get_tuple(cfg)

def test_integrity_protected_checkpoint_reopens(checkpoint):
    _,saver,cfg=checkpoint
    row=saver.get_tuple(cfg)
    assert row.checkpoint['channel_values']['memo']=='unchanged'
    assert row.pending_writes[0][2]=='receipt'

def test_missing_seal_never_gets_silently_backfilled(checkpoint):
    store,saver,cfg=checkpoint
    with store.db.tx() as c: c.execute(t.seals.delete())
    with pytest.raises(Conflict,match='integrity'): saver.get_tuple(cfg)
    from apps.distributed_runtime.maintenance import adopt_integrity
    with pytest.raises(Conflict,match='Stop'):
        adopt_integrity(store,reason='approved migration',acknowledge=True)
    # Simulates the operator having stopped all runtime processes before migration.
    store.interrupt(saver.claim)
    with store.db.tx() as c: c.execute(t.workers.update().values(last_seen=0))
    report=adopt_integrity(store,reason='restored offline backup validated by operator',acknowledge=True)
    assert report['adopted']==2 and report['retroactive_proof'] is False
    assert saver.get_tuple(cfg).checkpoint['channel_values']['memo']=='unchanged'
