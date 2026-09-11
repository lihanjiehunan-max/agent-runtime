"""Bounded synthetic load probe. Refuses live profiles; not a long-duration SLA."""
import argparse
from concurrent.futures import ThreadPoolExecutor,as_completed
import json
import math
import os
from pathlib import Path
import statistics
import threading
import time
from uuid import uuid4
import httpx


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default='http://127.0.0.1:28080')
    parser.add_argument('--seconds',type=int,default=60)
    parser.add_argument('--concurrency',type=int,default=8)
    parser.add_argument('--max-tasks',type=int,default=500)
    args=parser.parse_args()
    if not 1<=args.seconds<=600 or not 1<=args.concurrency<=32 or not 1<=args.max_tasks<=5000:
        parser.error('Probe bounds exceeded')
    token=os.environ.get('RUNTIME_API_TOKEN','docker-validation-token-only')
    base=args.url.rstrip('/')+'/api/v1/runtime'
    client=httpx.Client(timeout=30,headers={'Authorization':'Bearer '+token},limits=httpx.Limits(max_connections=64))
    def api(method,path,body=None):
        r=client.request(method,base+path,json=body);r.raise_for_status();return r.json()
    report={'status':'FAIL','scope':'bounded synthetic load; not a production SLA','live_model_verified':False}
    try:
        if api('GET','/ops/about')['profile']=='live': raise RuntimeError('Synthetic load is forbidden against live runtime')
        agent='soak-'+uuid4().hex[:10]
        api('POST','/ops/agents/deploy',{'agent_id':agent,'version':'1','engine':'deepagents','engine_version':'0.7.7',
            'prompt':'Answer briefly.','capabilities':[],'timeout':120})
        start=time.monotonic();deadline=start+args.seconds;lock=threading.Lock();issued=0;rows=[]
        def consumer():
            nonlocal issued
            samples=[]
            while time.monotonic()<deadline:
                with lock:
                    if issued>=args.max_tasks:break
                    issued+=1;number=issued
                begun=time.monotonic()
                sid=api('POST',f'/agents/{agent}/sessions')['session_id']
                eid=api('POST',f'/sessions/{sid}/executions',{'message':'load-probe-'+str(number),'request_key':uuid4().hex})['execution_id']
                while time.monotonic()-begun<125:
                    e=api('GET',f'/executions/{eid}')
                    if e['status']=='COMPLETED':break
                    if e['status'] in {'INTERRUPTED','TIMED_OUT','FAILED','CANCELLED'}:raise RuntimeError('Probe task failed')
                    time.sleep(.1)
                else:raise RuntimeError('Probe task deadline exceeded')
                detail=api('GET',f'/ops/executions/{eid}')
                samples.append({'execution_id':eid,'latency_seconds':time.monotonic()-begun,
                    'worker_id':detail['attempts'][-1]['worker_id']})
            return samples
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            for future in as_completed([pool.submit(consumer) for _ in range(args.concurrency)]):rows.extend(future.result())
        latency=sorted(x['latency_seconds'] for x in rows)
        if not latency or len(rows)!=issued:raise RuntimeError('Completion count mismatch')
        workers={}
        for row in rows:workers[row['worker_id']]=workers.get(row['worker_id'],0)+1
        report.update(status='PASS',submitted=issued,completed=len(rows),concurrency=args.concurrency,
            elapsed_seconds=round(time.monotonic()-start,3),requested_window_seconds=args.seconds,max_tasks=args.max_tasks,
            median_seconds=round(statistics.median(latency),3),p95_seconds=round(latency[math.ceil(.95*len(latency))-1],3),
            max_seconds=round(max(latency),3),worker_distribution=workers)
    except Exception as exc:report['error_type']=type(exc).__name__
    finally:client.close()
    out=Path('validation-results');out.mkdir(exist_ok=True)
    (out/'bounded-load.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
    return 0 if report['status']=='PASS' else 1

if __name__=='__main__':raise SystemExit(main())
