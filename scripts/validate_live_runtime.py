"""Read-only live protocol acceptance; never publishes real business text.

Requires an already started live runtime. Optional Worker restart is restricted
to an explicitly named isolated dh-live-* Compose project, not production.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from uuid import uuid4
import httpx

BASE='/api/v1/runtime'


def run(args,report):
    required=('RUNTIME_API_TOKEN','RUNTIME_OPS_TOKEN','LIVE_METRIC_ID','LIVE_PERIOD','LIVE_ORG','LIVE_DIMENSION')
    missing=[key for key in required if not os.environ.get(key)]
    if missing:
        report.update(status='BLOCKED',missing=missing);return 2
    model_headers={'Authorization':'Bearer '+os.environ['RUNTIME_API_TOKEN']}
    ops_headers={'Authorization':'Bearer '+os.environ['RUNTIME_OPS_TOKEN']}
    with httpx.Client(base_url=args.url.rstrip('/'),timeout=30,follow_redirects=False) as client:
        def api(method,path,body=None,ops=False):
            r=client.request(method,BASE+path,headers=ops_headers if ops else model_headers,json=body)
            if r.status_code>=400: raise RuntimeError('Runtime HTTP status '+str(r.status_code))
            return r.json()
        about=api('GET','/ops/about',ops=True)
        if about.get('profile')!='live':
            report.update(status='BLOCKED',reason='Target runtime does not declare the live profile');return 2
        ident='live-metric-'+uuid4().hex[:12]
        package={'agent_id':ident,'version':'1','engine':'deepagents','engine_version':'0.7.7',
            'capabilities':['coordinator'] if args.restart_worker else [],'timeout':180,'tools':['query_metric'],
            'prompt':'Use query_metric for metric questions. Preserve the supplied metric ID, period and org exactly in follow-ups. '
              'For 同比 use comparison=yoy; use the requested group_by for breakdown. Never invent values or source evidence. '
              'When answering a maximum from prior rows, use only those rows.'}
        deployed=api('POST','/ops/agents/deploy',package,ops=True)
        sid=api('POST',f'/agents/{ident}/sessions')['session_id']
        report.update(agent_id=ident,session_id=sid,package_digest=deployed['digest'],turns=[])
        prompts=[f"查询指标 {os.environ['LIVE_METRIC_ID']}，期间 {os.environ['LIVE_PERIOD']}，组织 {os.environ['LIVE_ORG']}。",
                 '同比呢？',f"按 {os.environ['LIVE_DIMENSION']} 拆开看。",'其中数值最高的是哪一项？请依据刚才查询的结果回答。']
        first_worker=None
        for index,prompt in enumerate(prompts):
            if index==3 and args.restart_worker:
                if not re.fullmatch(r'dh-live-[a-z0-9-]+',args.project):
                    raise RuntimeError('Restart is restricted to an isolated dh-live-* project')
                service=str(first_worker).split(':')[0]
                if service not in {'worker-a','worker-b','worker-c'}: raise RuntimeError('Unrecognized Worker service')
                command=['docker','compose','--env-file',args.env_file,'-p',args.project,
                    '-f','deploy/compose.infrastructure.yml','-f','deploy/compose.runtime.yml']
                cid=subprocess.run(command+['ps','-q',service],capture_output=True,text=True,check=True).stdout.strip()
                label=subprocess.run(['docker','inspect','--format','{{ index .Config.Labels "com.docker.compose.project" }}',cid],
                    capture_output=True,text=True,check=True).stdout.strip()
                if label!=args.project: raise RuntimeError('Container project identity mismatch')
                subprocess.run(command+['restart',service],capture_output=True,check=True,timeout=60)
                report['restart_service']=service
            submitted=api('POST',f'/sessions/{sid}/executions',{'message':prompt,'request_key':uuid4().hex})
            eid=submitted['execution_id'];start=time.monotonic()
            while time.monotonic()-start<200:
                state=api('GET',f'/executions/{eid}')
                if state['status']=='COMPLETED': break
                if state['status'] in {'INTERRUPTED','FAILED','TIMED_OUT','CANCELLED'}:
                    raise RuntimeError('Execution status '+state['status'])
                time.sleep(.3)
            else: raise RuntimeError('Execution did not complete within the acceptance bound')
            trace=api('GET',f'/ops/executions/{eid}/trace?limit=1000',ops=True)
            detail=api('GET',f'/ops/executions/{eid}',ops=True)
            result=api('GET',f'/executions/{eid}/result')
            if trace['thread_id']!=sid or trace['packages'][0]['digest']!=deployed['digest']:
                raise RuntimeError('Session/thread or package pin changed')
            types={x['type'] for x in trace['events']}
            if not {'model.call.started','model.call.completed','checkpoint.saved'}.issubset(types):
                raise RuntimeError('Model/checkpoint trace evidence is missing')
            calls=[x for x in detail['effects'] if x['tool']=='query_metric' and x['status']=='SUCCEEDED']
            if index<3 and not calls: raise RuntimeError('Required query_metric call was not executed')
            owners=[x['worker_id'] for x in trace['attempts']]
            if index==0: first_worker=owners[0]
            if index==3 and args.restart_worker and first_worker in owners:
                raise RuntimeError('Worker process identity did not change after restart')
            report['turns'].append({'execution_id':eid,'worker_ids':owners,'checkpoint_id':detail['execution']['checkpoint_id'],
                'tool_receipt_count':len(calls),'elapsed_seconds':round(time.monotonic()-start,3),
                'result_sha256':hashlib.sha256(json.dumps(result,sort_keys=True,ensure_ascii=False).encode()).hexdigest()})
        # Separate session must not inherit an existing checkpoint or conversation.
        separate=api('POST',f'/agents/{ident}/sessions')['session_id']
        if separate==sid: raise RuntimeError('Session identity reused')
        report.update(status='PASS',live_model_verified=True,enterprise_tool_verified=True,
            same_session_thread_verified=True,package_pinning_verified=True,
            worker_restart_verified=args.restart_worker,separate_session_id=separate,
            business_numeric_correctness_verified=False,multi_host_ha_verified=False)
        return 0


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default=os.environ.get('RUNTIME_ACCEPTANCE_URL','http://127.0.0.1:3000'))
    parser.add_argument('--output',default='validation-results/live-acceptance.json')
    parser.add_argument('--restart-worker',action='store_true')
    parser.add_argument('--project',default='dh-live-validation')
    parser.add_argument('--env-file',default='deploy/runtime.env')
    args=parser.parse_args()
    report={'scope':'configured external model and read-only enterprise tool protocol',
            'live_model_verified':False,'enterprise_tool_verified':False,'status':'FAIL'}
    try: code=run(args,report)
    except Exception as exc:
        # Do not leak response bodies, business content, connection strings or tokens.
        report.update(status='FAIL',error_type=type(exc).__name__);code=1
    path=Path(args.output);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'status':report['status'],'report':str(path),'live_model_verified':report['live_model_verified']}))
    return code

if __name__=='__main__': raise SystemExit(main())
