"""Destructive acceptance tests confined to the dh-validation Compose project.

Run only against the synthetic fixture topology. Never accepts a target URL,
credentials, container IDs or network names from a business request.
"""
from __future__ import annotations
import concurrent.futures
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import threading
import time
import traceback
from uuid import uuid4
import httpx

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'validation-results'
PROJECT='dh-validation'
COMPOSE=['docker','compose','-p',PROJECT,'-f',str(ROOT/'deploy/compose.validation.yml')]
URLS=['http://127.0.0.1:28080','http://127.0.0.1:28081']
GATEWAY='http://127.0.0.1:28090'
BASE='/api/v1/runtime'
TOKEN='docker-validation-token-only'
TERMINAL={'COMPLETED','FAILED','CANCELLED','TIMED_OUT','INTERRUPTED'}
CLIENT=httpx.Client(headers={'Authorization':'Bearer '+TOKEN},timeout=10,
                    limits=httpx.Limits(max_connections=100,max_keepalive_connections=30))
REPORT={'scope':'single-host, independently isolated Docker containers',
        'model':'deterministic HTTP fixture with actual DeepAgents 0.7.7 graph',
        'live_model_verified':False,'multi_host_ha_verified':False,'cases':[]}


def shell(args,timeout=90):
    p=subprocess.run(args,cwd=ROOT,text=True,capture_output=True,timeout=timeout)
    if p.returncode:raise RuntimeError(f'Command failed: {args}\n{p.stdout[-3000:]}\n{p.stderr[-3000:]}')
    return p.stdout.strip()


def compose(*args,timeout=90):return shell(COMPOSE+list(args),timeout)


def cid(service):
    ident=compose('ps','-aq',service)
    assert ident and '\n' not in ident, (service,ident)
    label=shell(['docker','inspect','--format','{{ index .Config.Labels "com.docker.compose.project" }}',ident])
    assert label==PROJECT,'Refusing to act on a container outside the isolated validation project'
    return ident


def request(method,path,*,data=None,node=0,expected=200):
    r=CLIENT.request(method,URLS[node]+BASE+path,json=data)
    assert r.status_code==expected,(method,path,r.status_code,r.text[:2000])
    return r.json()


def poll(fn,timeout=40,interval=.15):
    until=time.monotonic()+timeout
    last=None
    while time.monotonic()<until:
        try:
            last=fn()
            if last:return last
        except (httpx.HTTPError,RuntimeError):pass
        time.sleep(interval)
    raise AssertionError(f'Condition not met in {timeout}s; last={last}')


def health(node=0):
    def check():
        r=CLIENT.get(URLS[node]+'/healthz')
        return r.status_code==200
    return poll(check,40)


def deploy(agent='chat',**extra):
    p=dict(agent_id=agent,version='1',engine='deepagents',engine_version='0.7.7',
           prompt='Use authorized tools to answer the user.',capabilities=[],timeout=240)
    p.update(extra)
    return request('POST','/ops/agents/deploy',data=p)


def submit(message,agent='chat',session=None,key=None,node=0):
    if not session:
        session=request('POST',f'/agents/{agent}/sessions',node=node,expected=201)['session_id']
    e=request('POST',f'/sessions/{session}/executions',node=node,
              data={'message':message,'request_key':key or uuid4().hex},expected=202)
    return e['execution_id'],session


def state(eid,node=0):return request('GET','/executions/'+eid,node=node)

def ops(eid):return request('GET','/ops/executions/'+eid)

def result(eid,node=0):return request('GET','/executions/'+eid+'/result',node=node)


def wait(eid,desired='COMPLETED',timeout=50,node=0):
    def done():
        e=state(eid,node)
        if e['status']==desired:return e
        assert e['status'] not in TERMINAL,(eid,e,'expected',desired)
        return False
    return poll(done,timeout)


def running(eid):
    def active():
        item=ops(eid)['execution']
        return item if item['status']=='RUNNING' and item['checkpoint_id'] else None
    return poll(active,30)


def restore():
    for service in ('worker-a','worker-b','worker-c'):
        try:compose('unpause',service)
        except Exception:pass
    compose('up','-d','--no-build','--wait',timeout=120)


def save():
    OUT.mkdir(exist_ok=True)
    (OUT/'acceptance.json').write_text(json.dumps(REPORT,ensure_ascii=False,indent=2))


def case(name,fn):
    start=time.monotonic()
    item={'name':name}
    try:
        item['evidence']=fn() or {}
        item['status']='PASS'
    except Exception as exc:
        item.update(status='FAIL',error=str(exc),traceback=traceback.format_exc())
        print(item['traceback'],flush=True)
        try:restore()
        except Exception as recovery:item['recovery_error']=str(recovery)
    item['seconds']=round(time.monotonic()-start,3)
    REPORT['cases'].append(item);save()
    print(f"{item['status']}: {name} ({item['seconds']}s)",flush=True)


SAMPLES=[]

def topology():
    health(0);health(1)
    workers=poll(lambda:[w for w in request('GET','/ops/workers') if time.time()-w['last_seen']<15]
                 if len(request('GET','/ops/workers'))>=3 else None,40)
    assert {'worker-a','worker-b','worker-c'} <= {w['id'].split(':')[0] for w in workers}
    ids=[cid(x) for x in ['api-a','api-b','worker-a','worker-b','worker-c','postgres','redis','minio','gateway']]
    inspect=json.loads(shell(['docker','inspect']+ids))
    safe=[{'id':x['Id'],'hostname':x['Config']['Hostname'],'image_id':x['Image'],
        'status':x['State']['Status'],'pid':x['State']['Pid'],'memory_limit':x['HostConfig']['Memory'],
        'networks':{k:v['IPAddress'] for k,v in x['NetworkSettings']['Networks'].items()},
        'mount_types':[v['Type'] for v in x['Mounts']]} for x in inspect]
    REPORT['topology']=safe
    assert len({x['id'] for x in safe})==9
    assert all(not x['mount_types'] for x in safe[:5]),'API/Workers must not share mutable host volumes'
    deploy();deploy('child',capabilities=['analysis'])
    deploy('team',capabilities=['coordinator'],allowed_agents=['child'])
    deploy('writer',tools=['record_metric','query_metric'])
    return {'containers':9,'api_replicas':2,'worker_replicas':3,'execution_slots':6}


def concurrent_sessions():
    stop=threading.Event();samples=[]
    def monitor():
        while not stop.wait(.2):
            try:samples.append(request('GET','/ops/stats')['active_per_worker'])
            except Exception:pass
    thread=threading.Thread(target=monitor);thread.start()
    def one(i):
        start=time.monotonic();marker='isolation-'+uuid4().hex
        eid,sid=submit('SLOW:0.4:'+marker,node=i%2)
        wait(eid,timeout=180,node=(i+1)%2)
        answer=result(eid,node=(i+1)%2)['message']
        assert marker in answer
        assert answer.count('isolation-')==1,'Cross-session context leakage'
        info=ops(eid)
        assert len(info['attempts'])==1 and info['attempts'][0]['outcome']=='COMPLETED'
        return {'execution_id':eid,'session_id':sid,'worker_id':info['attempts'][0]['worker_id'],
                'instance_id':info['execution']['instance_id'],'marker':marker,'seconds':time.monotonic()-start}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
            runs=list(pool.map(one,range(50)))
    finally:stop.set();thread.join()
    SAMPLES.extend(runs)
    counts={}
    for x in runs:counts[x['worker_id']]=counts.get(x['worker_id'],0)+1
    assert len({k.split(':')[0] for k in counts})==3,counts
    assert samples and max(sum(s.values()) for s in samples)<=6,samples
    assert max(v for s in samples for v in s.values())<=2
    latency=sorted(x['seconds'] for x in runs)
    return {'submitted_concurrently':50,'completed':len(runs),'worker_distribution':counts,
        'observed_max_running':max(sum(s.values()) for s in samples),
        'latency_p50_seconds':round(statistics.median(latency),3),'latency_p95_seconds':round(latency[47],3),
        'latency_max_seconds':round(max(latency),3),'runs':runs,'capacity_samples':samples}


def version_and_checkpoint():
    original=SAMPLES[0]
    node=original['worker_id'].split(':')[0]
    compose('stop',node)
    try:
        new=deploy(version='2',prompt='Version two, existing sessions remain on version one.')
        eid,_=submit('What did I say before?',session=original['session_id'],node=1)
        wait(eid,timeout=40)
        assert original['marker'] in result(eid)['message']
        info=ops(eid)
        assert info['execution']['instance_id']==original['instance_id']!=new['id']
        assert not info['attempts'][0]['worker_id'].startswith(node+':')
        other,_=submit('new session version')
        wait(other)
        assert ops(other)['execution']['instance_id']==new['id']
        return {'previous_execution':original['execution_id'],'continued_execution':eid,
            'previous_worker':original['worker_id'],'continuing_worker':info['attempts'][0]['worker_id'],
            'pinned_instance':original['instance_id'],'new_instance':new['id']}
    finally:compose('start',node)


def idempotency_and_busy():
    sid=request('POST','/agents/chat/sessions',expected=201)['session_id']
    key=uuid4().hex
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        ids=list(pool.map(lambda n:submit('SLOW:2:idempotent',session=sid,key=key,node=n%2)[0],range(12)))
    assert len(set(ids))==1
    request('POST',f'/sessions/{sid}/executions',data={'message':'changed','request_key':key},expected=409)
    request('POST',f'/sessions/{sid}/executions',data={'message':'concurrent','request_key':'different'},expected=409)
    wait(ids[0]);assert len(ops(ids[0])['attempts'])==1
    return {'duplicate_submissions':12,'unique_execution':ids[0],'changed_payload':409,'concurrent_turn':409}


def delegation():
    eid,_=submit('TEAM:monthly-report',agent='team')
    wait(eid,timeout=50)
    info=ops(eid);tree=info['tree'];events=request('GET','/executions/'+eid+'/events')
    assert len(tree)==3 and len({e['session_id'] for e in tree})==3
    assert all(e['status']=='COMPLETED' for e in tree)
    assert len(info['attempts'])==2
    assert info['attempts'][0]['outcome']=='WAITING_CHILDREN'
    assert any(x['type']=='team.ready' for x in events)
    nodes={}
    for e in tree:
        nodes[e['id']]=[a['worker_id'] for a in ops(e['id'])['attempts']]
    assert all(a.startswith('worker-a:') for a in nodes[eid])
    assert all(not a.startswith('worker-a:') for e,attempts in nodes.items() if e!=eid for a in attempts)
    return {'root_execution':eid,'independent_sessions':[x['session_id'] for x in tree],
            'attempt_nodes':nodes,'parent_attempts':info['attempts']}


def kill_and_resume():
    eid,_=submit('SLOW:8:kill-recovery')
    old=running(eid);node=old['owner'].split(':')[0]
    compose('kill','-s','KILL',node)
    start=time.monotonic()
    try:
        wait(eid,'INTERRUPTED',timeout=25)
        detected=time.monotonic()-start
        request('POST','/ops/executions/'+eid+'/resume')
        wait(eid,timeout=40)
        a=ops(eid)['attempts']
        assert len(a)==2 and a[1]['worker_id']!=old['owner'] and a[1]['epoch']>old['epoch']
        assert 'kill-recovery' in result(eid)['message']
        return {'execution_id':eid,'interruption_detected_seconds':round(detected,3),'attempts':a}
    finally:compose('start',node)


def stale_fencing():
    eid,_=submit('SLOW:8:stale-worker')
    old=running(eid);node=old['owner'].split(':')[0]
    compose('pause',node)
    try:
        wait(eid,'INTERRUPTED',timeout=25)
        request('POST','/ops/executions/'+eid+'/resume')
        new=running(eid)
        assert new['owner']!=old['owner'] and new['epoch']>old['epoch']
        compose('unpause',node)
        probe=f'''
import os
from apps.distributed_runtime.store import Store,Claim,LostLease
from apps.distributed_runtime.checkpoints import FencedSQLSaver
from langgraph.checkpoint.base import empty_checkpoint
s=Store(os.environ['RUNTIME_DATABASE_URL']);claim=Claim({eid!r},{old['owner']!r},{old['epoch']!r})
saver=FencedSQLSaver(s,claim)
checks=[lambda:s.emit(claim,'stale_probe',{{}}),lambda:s.complete(claim,'a'*64),lambda:saver.put({{'configurable':{{'thread_id':{old['session_id']!r}}}}},empty_checkpoint(),{{}},{{}})]
for check in checks:
    try: check()
    except LostLease: continue
    raise AssertionError('Stale attempt accepted')
print('3 stale writes rejected')
'''
        output=compose('exec','-T','api-b','python','-c',probe)
        wait(eid,timeout=40)
        assert not any(x['type']=='stale_probe' for x in request('GET','/executions/'+eid+'/events'))
        return {'execution_id':eid,'old_epoch':old['epoch'],'new_epoch':new['epoch'],'probe':output,
                'attempts':ops(eid)['attempts']}
    finally:
        try:compose('unpause',node)
        except Exception:pass


def network_partition():
    eid,_=submit('SLOW:8:partition')
    old=running(eid);node=old['owner'].split(':')[0];ident=cid(node)
    info=json.loads(shell(['docker','inspect',ident]))[0]
    networks=list(info['NetworkSettings']['Networks'])
    assert networks==[PROJECT+'_default'],networks
    network=networks[0]
    shell(['docker','network','disconnect','-f',network,ident])
    try:
        wait(eid,'INTERRUPTED',timeout=30)
        other,_=submit('healthy-node-during-partition');wait(other,timeout=35)
        request('POST','/ops/executions/'+eid+'/resume')
        new=running(eid);assert new['owner']!=old['owner']
        shell(['docker','network','connect','--alias',node,network,ident])
        wait(eid,timeout=45)
        return {'partitioned_container':ident,'execution_id':eid,'healthy_execution':other,
                'attempts':ops(eid)['attempts']}
    finally:
        current=json.loads(shell(['docker','inspect',ident]))[0]['NetworkSettings']['Networks']
        if network not in current:shell(['docker','network','connect','--alias',node,network,ident])


def api_restart_and_sse():
    eid,_=submit('SLOW:2:sse-replay')
    cursor=None
    with CLIENT.stream('GET',URLS[0]+BASE+'/executions/'+eid+'/stream') as stream:
        assert stream.status_code==200
        for line in stream.iter_lines():
            if line.startswith('id: '):cursor=int(line[4:]);break
    assert cursor is not None
    compose('stop','api-a')
    try:
        wait(eid,node=1)
        assert 'sse-replay' in result(eid,node=1)['message']
    finally:compose('start','api-a');health(0)
    r=CLIENT.get(URLS[0]+BASE+'/executions/'+eid+'/stream',headers={'Last-Event-ID':str(cursor)})
    assert r.status_code==200
    ids=[int(x[4:]) for x in r.text.splitlines() if x.startswith('id: ')]
    assert ids and min(ids)>cursor and ids==sorted(set(ids))
    assert 'execution.completed' in r.text
    return {'execution_id':eid,'last_acknowledged_event':cursor,'replayed_events':len(ids),'replay_node':'api-a'}


def redis_outage():
    compose('stop','redis')
    try:
        ids=[submit('redis-outage-'+str(i),node=i%2)[0] for i in range(8)]
        for eid in ids:wait(eid,timeout=55)
        pending=request('GET','/ops/stats')['outbox_pending']
        assert pending>=8,pending
    finally:compose('start','redis')
    poll(lambda:request('GET','/ops/stats')['outbox_pending']==0,50)
    return {'completed_without_redis':ids,'pending_before_recovery':pending,'pending_after_recovery':0}


def postgres_restart():
    eid,_=submit('SLOW:8:postgres-restart');running(eid)
    compose('stop','postgres')
    try:
        r=CLIENT.get(URLS[1]+'/healthz',timeout=12)
        assert r.status_code==503,r.text
        time.sleep(7)
    finally:compose('start','postgres')
    health(0);health(1)
    wait(eid,'INTERRUPTED',timeout=35)
    request('POST','/ops/executions/'+eid+'/resume')
    wait(eid,timeout=45)
    old=SAMPLES[0]
    assert old['marker'] in result(old['execution_id'])['message']
    return {'execution_id':eid,'health_while_down':503,'old_result_preserved':old['execution_id'],
            'attempts':ops(eid)['attempts']}


def minio_outage():
    compose('stop','minio')
    try:
        eid,_=submit('artifact-write-during-minio-outage')
        wait(eid,'INTERRUPTED',timeout=40)
        assert ops(eid)['execution']['checkpoint_id']
    finally:compose('start','minio')
    poll(lambda:shell(['docker','inspect','--format','{{.State.Health.Status}}',cid('minio')])=='healthy',40)
    request('POST','/ops/executions/'+eid+'/resume')
    wait(eid,timeout=40)
    assert 'artifact-write-during-minio-outage' in result(eid)['message']
    return {'execution_id':eid,'attempts':ops(eid)['attempts'],'artifact_ref':state(eid)['output_ref']}


def cancellation_timeout_and_capabilities():
    eid,_=submit('SLOW:10:cancel');running(eid)
    started=time.monotonic();request('POST','/executions/'+eid+'/cancel');wait(eid,'CANCELLED',timeout=8)
    cancelled=time.monotonic()-started
    assert cancelled<=2, cancelled
    deploy('short',timeout=2)
    timed,_=submit('SLOW:10:deadline',agent='short');wait(timed,'TIMED_OUT',timeout=10)
    deploy('unsupported',capabilities=['not-registered'])
    queued,_=submit('must not execute',agent='unsupported')
    time.sleep(1)
    assert state(queued)['status']=='QUEUED' and not ops(queued)['attempts']
    request('POST','/executions/'+queued+'/cancel');wait(queued,'CANCELLED')
    return {'cancelled_execution':eid,'cancel_seconds':round(cancelled,3),'timed_out_execution':timed,
            'capability_filtered_execution':queued}


def unknown_effect():
    value='drop:docker-'+uuid4().hex
    eid,_=submit('WRITE:'+value,agent='writer')
    wait(eid,'INTERRUPTED',timeout=40)
    effects=ops(eid)['effects'];assert len(effects)==1 and effects[0]['status']=='UNKNOWN',effects
    request('POST','/ops/executions/'+eid+'/resume',expected=409)
    receipts=CLIENT.get(GATEWAY+'/effects').json();remote=next(x for x in receipts if x['value']==value)
    assert remote['requests']==1
    request('POST','/ops/executions/'+eid+'/effects/'+effects[0]['call_id']+'/reconcile',data={
        'executed':True,'result':json.loads(remote['result']),'evidence':'Verified fixture receipt '+json.loads(remote['result'])['receipt']})
    request('POST','/ops/executions/'+eid+'/resume');wait(eid,timeout=40)
    remote_after=next(x for x in CLIENT.get(GATEWAY+'/effects').json() if x['key']==remote['key'])
    assert remote_after['requests']==1
    return {'execution_id':eid,'call_id':effects[0]['call_id'],'resume_before_reconcile':409,
            'external_requests_before':1,'external_requests_after':remote_after['requests'],
            'reconciled_status':ops(eid)['effects'][0]['status']}


def metric_and_authorization():
    eid,_=submit('METRIC:revenue',agent='writer');wait(eid)
    assert '110' in result(eid)['message']
    assert ops(eid)['effects'][0]['status']=='SUCCEEDED'
    response=httpx.get(URLS[0]+BASE+'/ops/workers',timeout=5)
    assert response.status_code==401
    response=httpx.post(URLS[1]+BASE+'/ops/agents/deploy',json={},timeout=5)
    assert response.status_code==401
    return {'metric_execution':eid,'real_http_tool_call':True,'unauthenticated_read':401,'unauthenticated_write':401}


def main():
    if os.environ.get('RUN_DOCKER_ACCEPTANCE')!='1':
        raise SystemExit('Set RUN_DOCKER_ACCEPTANCE=1 only for the isolated synthetic Compose topology.')
    OUT.mkdir(exist_ok=True)
    REPORT['started_at']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
    REPORT['tested_commit']=shell(['git','rev-parse','HEAD'])
    REPORT['docker_version']=shell(['docker','version','--format','{{.Server.Version}}'])
    REPORT['compose_version']=compose('version','--short')
    REPORT['host']={'cpu_count':os.cpu_count(),'platform':sys.platform}
    for name,fn in [
        ('isolated_topology',topology),('50_concurrent_sessions',concurrent_sessions),
        ('cross_worker_checkpoint_and_version_pinning',version_and_checkpoint),
        ('idempotency_and_session_mutex',idempotency_and_busy),('durable_cross_node_delegation',delegation),
        ('worker_sigkill_manual_recovery',kill_and_resume),('paused_old_worker_fencing',stale_fencing),
        ('worker_network_partition',network_partition),('api_restart_and_sse_replay',api_restart_and_sse),
        ('redis_outage_sql_fallback',redis_outage),('postgres_outage_and_restart',postgres_restart),
        ('minio_outage_checkpoint_recovery',minio_outage),('cancel_deadline_capability_filters',cancellation_timeout_and_capabilities),
        ('unknown_effect_no_duplicate_write',unknown_effect),('http_metric_and_authentication',metric_and_authorization)]:
        case(name,fn)
        if name=='isolated_topology' and REPORT['cases'][-1]['status']=='FAIL':break
    REPORT['finished_at']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
    REPORT['passed']=sum(c['status']=='PASS' for c in REPORT['cases'])
    REPORT['failed']=sum(c['status']=='FAIL' for c in REPORT['cases'])
    REPORT['status']='PASS' if REPORT['failed']==0 and len(REPORT['cases'])==15 else 'FAIL'
    save();print(json.dumps({k:REPORT[k] for k in ['status','passed','failed','tested_commit']},indent=2))
    CLIENT.close()
    return 0 if REPORT['status']=='PASS' else 1

if __name__=='__main__':sys.exit(main())
