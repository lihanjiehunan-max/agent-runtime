"""Bounded delegation results, direct-child access and fencing regression tests."""
import json
import pytest
from apps.distributed_runtime.artifacts import FileArtifacts
from apps.distributed_runtime.child_results import ChildResultAccess
from apps.distributed_runtime.store import Store, Conflict, LostLease, canonical
from .test_recovered_store import package

@pytest.fixture
def access(tmp_path):
    store=Store('sqlite:///'+str(tmp_path/'rt.db'),lease_seconds=60)
    objects=FileArtifacts(tmp_path/'objects')
    store.deploy(package('parent',allowed_agents=['child']))
    store.deploy(package('child'))
    store.register_worker('w',['deepagents'],[],8)
    parent=store.submit(store.create_session('parent')['id'],'root',{'message':'parent'})
    claim=store.claim('w',parent['id'])
    children=store.spawn(claim,'group',[
        {'agent_id':'child','version':'1','input':{'message':'small'}},
        {'agent_id':'child','version':'1','input':{'message':'large'}}])
    text='大结果🌊'*20000+':tail-proof'
    for child,value in zip(children,[{'message':'small answer'}, {'message':text}]):
        cc=store.claim('w',child['id'])
        store.complete(cc,objects.put_json(value))
    try: yield store,objects,ChildResultAccess(store,objects),claim,children,text
    finally: store.db.close()

def test_delegation_large_output_never_enters_parent_whole(access):
    store,objects,reader,claim,kids,text=access
    rows=reader.summaries(claim,'group')
    assert rows[0]['result']=={'message':'small answer'}
    assert 'result' not in rows[1]
    assert rows[1]['summary_kind']=='prefix_excerpt' and rows[1]['truncated']
    assert rows[1]['content_chars']==len(text)
    assert len(canonical(rows))<20000
    assert 'tail-proof' not in json.dumps(rows,ensure_ascii=False)
    assert objects.get_json(rows[1]['output_ref'])['message']==text

def test_unicode_page_bounds_and_audited_references(access):
    store,objects,reader,claim,kids,text=access
    page=reader.read(claim,kids[1]['id'],offset=79000,limit=1000)
    assert page['text']==text[79000:80000]
    assert page['next_offset']==80000 and not page['eof']
    last=reader.read(claim,kids[1]['id'],offset=80000,limit=2048)
    assert last['text']==':tail-proof' and last['eof'] and last['next_offset'] is None
    event=store.events(claim.execution_id)[-1]
    assert event['type']=='child_result.read'
    assert event['payload']['output_ref']==last['output_ref']
    assert 'text' not in event['payload']

@pytest.mark.parametrize('offset,limit',[(-1,10),(0,0),(0,2049),(True,5),(0,False),(999999,10)])
def test_invalid_read_bounds_are_rejected(access,offset,limit):
    _,_,reader,claim,kids,_=access
    with pytest.raises(Conflict):reader.read(claim,kids[1]['id'],offset,limit)

def test_unrelated_and_sibling_results_are_forbidden(access):
    store,objects,reader,claim,kids,_=access
    other=store.submit(store.create_session('child')['id'],'other',{'message':'secret'})
    other_claim=store.claim('w',other['id']);store.complete(other_claim,objects.put_json({'message':'secret'}))
    with pytest.raises(Conflict):reader.read(claim,other['id'])
    # Use a genuinely active child: rejection must be by parent membership,
    # not merely because a completed caller has no lease.
    active=store.spawn(claim,'another-group',[
        {'agent_id':'child','version':'1','input':{'message':'active child'}}])[0]
    active_claim=store.claim('w',active['id'])
    with pytest.raises(Conflict,match='direct child'):reader.read(active_claim,kids[1]['id'])

def test_stale_claim_cannot_read_or_summarize(access):
    store,_,reader,claim,kids,_=access
    store.interrupt(claim)
    with pytest.raises(LostLease):reader.read(claim,kids[1]['id'])
    with pytest.raises(LostLease):reader.summaries(claim,'group')

def test_corrupt_child_artifact_is_rejected(access):
    store,objects,reader,claim,kids,_=access
    ref=store.execution(kids[1]['id'])['output_ref']
    objects.path(ref).write_bytes(b'{}')
    with pytest.raises(Conflict,match='integrity'):reader.read(claim,kids[1]['id'])

def test_reader_tool_only_available_to_delegating_package(access):
    store,objects,_,claim,kids,_=access
    from apps.distributed_runtime.harness import DeepAgentsHarness
    harness=DeepAgentsHarness(store,objects,model_url='http://127.0.0.1:1/v1',model_key='fixture-only')
    graph=harness.build(claim)
    assert 'read_child_result' in graph.nodes['tools'].bound.tools_by_name
    from apps.distributed_runtime.store import Claim
    child_graph=harness.build(Claim(kids[0]['id'],'w',1))
    assert 'read_child_result' not in child_graph.nodes['tools'].bound.tools_by_name


def test_claim_revoked_during_object_read_never_releases_content(access):
    store,objects,_,claim,kids,_=access
    from apps.distributed_runtime.child_results import ChildResultAccess
    class RevokingObjects:
        def get_json(self,ref):
            value=objects.get_json(ref)
            store.interrupt(claim)
            return value
    reader=ChildResultAccess(store,RevokingObjects())
    with pytest.raises(LostLease):reader.read(claim,kids[1]['id'])
    assert not any(e['type']=='child_result.read' for e in store.events(claim.execution_id))
