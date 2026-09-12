"""HTTP-only release-cluster checks; fault controls are supplied by its owner."""
from concurrent.futures import ThreadPoolExecutor
import json
import time
from uuid import uuid4

import httpx

BASE = '/api/v1/runtime'


def verify_multiport(urls, ops_token, control):
    """Use only the freshly created synthetic project; never select host containers."""
    report = {'status': 'FAIL', 'scope': 'single-host release images with dedicated PostgreSQL/Redis/MinIO',
              'live_model_verified': False, 'multi_host_ha_verified': False, 'cases': []}
    client = httpx.Client(headers={'Authorization': 'Bearer '+ops_token}, timeout=20,
                         limits=httpx.Limits(max_connections=100, max_keepalive_connections=30))

    def call(method, path, body=None, node='a', expected=200):
        response = client.request(method, urls[node]+BASE+path, json=body)
        assert response.status_code == expected, (method, path, response.status_code, response.text[:400])
        return response.json()

    def poll(fn, seconds=60):
        end = time.monotonic()+seconds
        while time.monotonic() < end:
            try:
                result = fn()
                if result:
                    return result
            except httpx.HTTPError:
                pass
            time.sleep(.15)
        raise AssertionError('Condition was not met before deadline')

    def wait(eid, state='COMPLETED', node='b', seconds=60):
        def done():
            item = call('GET', '/executions/'+eid, node=node)
            if item['status'] == state:
                return item
            assert item['status'] not in ('FAILED', 'CANCELLED', 'TIMED_OUT', 'INTERRUPTED'), item
        return poll(done, seconds)

    def deploy(agent, **extra):
        package = dict(agent_id=agent, version='1', engine='deepagents', engine_version='0.7.7',
                       prompt='Answer from the conversation. Use only authorized tools.', capabilities=[], timeout=180)
        package.update(extra)
        return call('POST', '/ops/agents/deploy', package)

    def session(agent='mp-chat', node='a'):
        return call('POST', f'/agents/{agent}/sessions', node=node, expected=201)['session_id']

    def submit(sid, message, node='a', key=None):
        return call('POST', f'/sessions/{sid}/executions',
                    {'message': message, 'request_key': key or uuid4().hex}, node=node, expected=202)['execution_id']

    def ops(eid):
        return call('GET', '/ops/executions/'+eid, node='b')

    def result(eid, node='b'):
        return call('GET', '/executions/'+eid+'/result', node=node)

    def case(name, fn):
        started = time.monotonic()
        evidence = fn()
        report['cases'].append({'name': name, 'status': 'PASS', 'seconds': round(time.monotonic()-started, 3),
                                'evidence': evidence or {}})
        print('PASS: multiport/'+name, flush=True)

    def identities():
        health = {name: client.get(url+'/healthz').json() for name, url in urls.items()}
        assert health['a']['node'] != health['b']['node'], health
        assert health['console']['node'] in {health['a']['node'], health['b']['node']}
        def registered():
            workers = call('GET', '/ops/workers')
            return workers if len([w for w in workers if w['online']]) == 3 else None
        workers = poll(registered)
        assert {w['id'] for w in workers} == {w['id'] for w in call('GET', '/ops/workers', node='b')}
        return {'health': health, 'worker_ids': [w['id'] for w in workers]}

    def continuity():
        deploy('mp-chat')
        deploy('mp-pinned', capabilities=['coordinator'], tools=['query_metric'])
        sid = session('mp-pinned')
        marker = 'continuity-'+uuid4().hex
        first = submit(sid, 'remember:'+marker)
        wait(first)
        old = ops(first)
        second = submit(sid, 'METRIC:revenue', node='b')
        wait(second, node='a')
        effects = ops(second)['effects']
        assert any(x['tool']=='query_metric' and x['status']=='SUCCEEDED' for x in effects), effects
        worker = old['attempts'][-1]['worker_id'].split(':')[0]
        control('restart', worker)
        third = submit(sid, 'what did I say?', node='b')
        wait(third, node='a')
        latest = ops(third)
        assert marker in result(third, node='console')['message']
        assert latest['attempts'][-1]['worker_id'] != old['attempts'][-1]['worker_id']
        traces = [call('GET', '/ops/executions/'+eid+'/trace', node=node)
                  for eid,node in [(first,'a'),(second,'b'),(third,'console')]]
        assert all(x['thread_id']==sid and x['packages'][0]['digest']==traces[0]['packages'][0]['digest'] for x in traces)
        assert all(ops(eid)['execution']['checkpoint_id'] for eid in (first,second,third))
        return {'session_id':sid, 'thread_id':sid, 'executions':[first,second,third],
                'digest':traces[0]['packages'][0]['digest'], 'before_worker':old['attempts'][-1]['worker_id'],
                'after_worker':latest['attempts'][-1]['worker_id'], 'fixture_http_tool_verified':True}

    def idempotency():
        sid = session()
        key = uuid4().hex
        eid = submit(sid, 'SLOW:2:replay', key=key)
        duplicate = submit(sid, 'SLOW:2:replay', node='b', key=key)
        assert eid == duplicate
        call('POST', f'/sessions/{sid}/executions', {'message':'different','request_key':key}, node='b', expected=409)
        call('POST', f'/sessions/{sid}/executions', {'message':'overlap','request_key':uuid4().hex}, expected=409)
        wait(eid)
        assert len(ops(eid)['attempts']) == 1
        replay_after_finish = submit(sid, 'SLOW:2:replay', node='b', key=key)
        assert replay_after_finish == eid
        return {'execution_id':eid, 'http_submissions':3, 'logical_executions':1, 'attempts':1}

    def modes():
        sid = session()
        body = {'message':'invoke-result','request_key':uuid4().hex}
        response = client.post(urls['console']+BASE+f'/sessions/{sid}/invoke?wait_seconds=20',json=body)
        assert response.status_code == 200 and 'invoke-result' in response.json()['result']['message']
        return {'invoke_http':200, 'execution_id':response.json()['execution_id']}

    def stream_failover():
        sid = session()
        eid = submit(sid, 'SLOW:3:api-failover')
        with client.stream('GET', urls['a']+BASE+f'/executions/{eid}/stream') as response:
            assert response.status_code == 200
            cursor = next(int(line[3:].strip()) for line in response.iter_lines() if line.startswith('id:'))
        control('stop', 'api-a')
        try:
            wait(eid)
            response = client.get(urls['b']+BASE+f'/executions/{eid}/stream', headers={'Last-Event-ID':str(cursor)})
            assert response.status_code == 200 and 'text/event-stream' in response.headers['content-type']
            ids = [int(x[3:].strip()) for x in response.text.splitlines() if x.startswith('id:')]
            expected = call('GET', f'/executions/{eid}/events?after={cursor}', node='b')
            assert ids and ids == [e['id'] for e in expected] and ids == sorted(set(ids))
            assert min(ids) > cursor
            assert 'api-failover' in result(eid)['message']
        finally:
            control('start', 'api-a')
            poll(lambda:client.get(urls['a']+'/healthz').status_code==200)
        return {'execution_id':eid,'last_observed_event':cursor,'replayed_events':len(ids),
                'observed_api':'a','resumed_api':'b','api_a_stopped_during_execution':True}

    def teams():
        deploy('child',capabilities=['analysis'])
        deploy('mp-parent',capabilities=['coordinator'],allowed_agents=['child'])
        eid = submit(session('mp-parent'), 'TEAM:multiport-report')
        wait(eid)
        details = ops(eid)
        assert len(details['tree'])==3 and len({x['session_id'] for x in details['tree']})==3
        assert all(x['status']=='COMPLETED' for x in details['tree'])
        assert details['attempts'][0]['outcome']=='WAITING_CHILDREN'
        assert len(details['attempts'])==2
        assignment = {x['id']:[a['worker_id'] for a in ops(x['id'])['attempts']] for x in details['tree']}
        assert all(w.startswith('worker-a:') for w in assignment[eid])
        assert all(not w.startswith('worker-a:') for child,workers in assignment.items() if child!=eid for w in workers)
        return {'execution_id':eid,'assignment':assignment,'parent_waited_durably':True}

    def large_results():
        eid = submit(session('mp-parent'), 'TEAM_LARGE:bounded-result')
        wait(eid)
        assert 'tail-proof' in result(eid)['message']
        events = call('GET', f'/executions/{eid}/events')
        summary = next(x['payload'] for x in events if x['type']=='child_result.summarized')
        page = next(x['payload'] for x in events if x['type']=='child_result.read')
        assert summary['inline'] is False and summary['content_chars']==80011
        assert page['offset']==80000 and page['returned_chars']==11 and page['eof']
        assert all('text' not in x for x in (summary,page))
        return {'execution_id':eid,'summary':summary,'page':page,'storage':'MinIO'}

    def concurrent_sessions():
        def one(index):
            node = 'a' if index%2==0 else 'b'
            sid = session(node=node)
            marker = 'mp-isolation-'+uuid4().hex
            eid = submit(sid, 'SLOW:0.3:'+marker, node=node)
            wait(eid, node='b' if node=='a' else 'a', seconds=120)
            text = result(eid)['message']
            assert marker in text and text.count('mp-isolation-')==1
            attempts = ops(eid)['attempts']
            assert len(attempts)==1
            return {'session_id':sid,'execution_id':eid,'worker_id':attempts[0]['worker_id']}
        with ThreadPoolExecutor(max_workers=50) as pool:
            results = list(pool.map(one,range(50)))
        workers = {x['worker_id'].split(':')[0] for x in results}
        assert workers=={'worker-a','worker-b','worker-c'}
        return {'submitted':50,'completed':len(results),'results':results}

    try:
        for name,fn in [('independent_api_identities',identities),('cross_api_session_worker_restart',continuity),
                        ('cross_api_idempotency_and_mutex',idempotency),('synchronous_invoke',modes),
                        ('api_stop_and_sse_replay',stream_failover),('durable_multi_agent',teams),
                        ('minio_large_child_result_paging',large_results),('50_cross_api_sessions',concurrent_sessions)]:
            case(name,fn)
        report['status']='PASS'
    except Exception as exc:
        import traceback
        report.update(error_type=type(exc).__name__,error=str(exc)[:1500],traceback=traceback.format_exc())
    finally:
        client.close()
    return report
