"""Boot release images with an explicit synthetic overlay, test the proxy, clean up.

Never targets existing production infrastructure and never uses real credentials.
The fixture is a separate container: neither release image contains tests.
"""
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'validation-results'
BASE = '/api/v1/runtime'
PROJECT = 'dh-release-validation'


def main():
    OUT.mkdir(exist_ok=True)
    env_file = ROOT / 'deploy/release-smoke.env'
    report = {'status': 'FAIL', 'scope': 'release images, Nginx proxy and explicit synthetic overlay',
              'live_model_verified': False, 'multi_host_verified': False, 'checks': []}
    env = dict(os.environ)
    # Never reuse user-provided infrastructure passwords or real endpoint values.
    for key in list(env):
        if key.startswith(('MODEL_', 'TOOL_', 'RUNTIME_', 'AWS_', 'POSTGRES_', 'REDIS_')):
            env.pop(key)
    env.update(MODEL_BASE_URL='http://gateway:8000/v1', MODEL_API_KEY='release-model-fixture-only',
               MODEL_NAME='fixture', TOOL_BASE_URL='http://gateway:8000', TOOL_API_TOKEN='release-tool-fixture-only',
               RUNTIME_PORT='28082', RUNTIME_API_TOKEN=secrets.token_hex(32), RUNTIME_OPS_TOKEN=secrets.token_hex(32))
    command = ['docker', 'compose', '--env-file', str(env_file), '-p', PROJECT,
               '-f', 'deploy/compose.infrastructure.yml', '-f', 'deploy/compose.runtime.yml',
               '-f', 'deploy/compose.release-test.yml']
    try:
        subprocess.run([sys.executable, 'scripts/generate_runtime_env.py', '--output', str(env_file)],
                       cwd=ROOT, env=env, check=True, capture_output=True)
        with (OUT/'release-startup.log').open('w') as log:
            subprocess.run(command+['up', '-d', '--no-build', '--wait', '--wait-timeout', '150'],
                           cwd=ROOT, env=env, stdout=log, stderr=log, check=True, timeout=180)
        headers = {'Authorization': 'Bearer '+env['RUNTIME_OPS_TOKEN']}
        with httpx.Client(base_url='http://127.0.0.1:28082', headers=headers, timeout=30) as client:
            def api(method, path, body=None):
                r = client.request(method, BASE+path, json=body)
                r.raise_for_status()
                return r.json()
            index = client.get('/')
            assert index.status_code == 200 and '/assets/' in index.text
            assert "frame-ancestors 'none'" in index.headers['content-security-policy']
            assert index.headers['referrer-policy'] == 'no-referrer'
            assert api('GET', '/ops/about')['profile'] == 'synthetic'
            assert client.get(BASE+'/ops/workers', headers={'Authorization': 'Bearer '+env['RUNTIME_API_TOKEN']}).status_code == 401
            report['checks'].append('console-proxy-security-and-separated-api-auth')
            # Check actual Worker registration, not only container process status.
            workers=[]
            for _ in range(60):
                workers=[w for w in api('GET','/ops/workers') if w['online']]
                if len(workers)==3: break
                time.sleep(.2)
            assert len(workers)==3, workers
            package={'agent_id':'release-smoke','version':'1','engine':'deepagents','engine_version':'0.7.7',
                     'prompt':'Answer from the current conversation.', 'capabilities':['coordinator'], 'timeout':60}
            api('POST','/ops/agents/deploy',package)
            sid=api('POST','/agents/release-smoke/sessions')['session_id']
            token='release-proof-'+uuid4().hex
            for message in ('remember:'+token,'what did I say?'):
                eid=api('POST',f'/sessions/{sid}/executions',{'message':message,'request_key':uuid4().hex})['execution_id']
                for _ in range(160):
                    execution=api('GET',f'/executions/{eid}')
                    if execution['status']=='COMPLETED':break
                    assert execution['status'] not in ('FAILED','TIMED_OUT','INTERRUPTED')
                    time.sleep(.2)
                assert execution['status']=='COMPLETED'
                assert token in json.dumps(api('GET',f'/executions/{eid}/result'))
                stream=client.get(BASE+f'/executions/{eid}/stream')
                assert stream.status_code==200 and 'text/event-stream' in stream.headers['content-type']
                assert 'id:' in stream.text
            report['checks'].append('release-worker-persistent-session-and-proxied-sse')
            report.update(status='PASS',worker_ids=[w['id'] for w in workers],session_id=sid)
    except Exception as exc:
        report.update(error_type=type(exc).__name__,error=str(exc)[:1000])
    finally:
        if env_file.exists():
            with (OUT/'release-containers.log').open('w') as log:
                subprocess.run(command+['logs','--no-color'],cwd=ROOT,env=env,stdout=log,stderr=log,timeout=30)
            subprocess.run(command+['down','-v','--remove-orphans'],cwd=ROOT,env=env,capture_output=True,timeout=60)
            env_file.unlink()
    (OUT/'release-compose.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False))
    return 0 if report['status']=='PASS' else 1

if __name__=='__main__':raise SystemExit(main())
