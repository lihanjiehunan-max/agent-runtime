"""Fresh single-host multiport acceptance of release images (synthetic only).

Creates a unique Compose project, random test credentials and dedicated volumes.
Never accepts existing credentials, endpoints or container IDs. Deletes only
its own disposable project after collecting evidence, even when tests fail.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
from uuid import uuid4

import httpx
from multiport_checks import verify_multiport
from port_preflight import check_loopback_ports

ROOT = Path(__file__).resolve().parents[1]
BASE = '/api/v1/runtime'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--console-port',type=int,default=3000)
    parser.add_argument('--api-a-port',type=int,default=28080)
    parser.add_argument('--api-b-port',type=int,default=28081)
    parser.add_argument('--gateway-port',type=int,default=28090)
    parser.add_argument('--output',default='validation-results')
    args = parser.parse_args()
    ports = [args.console_port,args.api_a_port,args.api_b_port,args.gateway_port]
    if len(set(ports))!=4 or not all(1024<=p<=65535 for p in ports):
        parser.error('Four distinct unprivileged ports are required')
    out = Path(args.output).resolve()
    out.mkdir(parents=True,exist_ok=True)
    project = 'dh-preacceptance-'+uuid4().hex[:10]
    report = {'status':'FAIL','project':project,'scope':'single host, release images, four loopback ports; deterministic model',
              'live_model_verified':False,'multi_host_verified':False,'checks':[],
              'ports':dict(zip(['console','api_a','api_b','gateway'],ports)),
              'tested_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()}
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(('MODEL_','TOOL_','RUNTIME_','AWS_','POSTGRES_','REDIS_','PREACCEPTANCE_','COMPOSE_')):
            env.pop(key)
    env.update(MODEL_BASE_URL='http://gateway:8000/v1',MODEL_API_KEY='release-model-fixture-only',MODEL_NAME='fixture',
               TOOL_BASE_URL='http://gateway:8000',TOOL_API_TOKEN='release-tool-fixture-only',RUNTIME_LEASE_SECONDS='6',
               RUNTIME_BIND_ADDRESS='127.0.0.1',RUNTIME_PORT=str(args.console_port),
               PREACCEPTANCE_A_PORT=str(args.api_a_port),PREACCEPTANCE_B_PORT=str(args.api_b_port),
               PREACCEPTANCE_GATEWAY_PORT=str(args.gateway_port),
               RUNTIME_API_TOKEN=secrets.token_hex(32),RUNTIME_OPS_TOKEN=secrets.token_hex(32))
    urls = dict(console=f'http://127.0.0.1:{args.console_port}',a=f'http://127.0.0.1:{args.api_a_port}',b=f'http://127.0.0.1:{args.api_b_port}')
    with tempfile.TemporaryDirectory(prefix='runtime-preacceptance-') as private:
        env_file = Path(private)/'test.env'
        command = ['docker','compose','--env-file',str(env_file),'-p',project,
                   '-f','deploy/compose.infrastructure.yml','-f','deploy/compose.runtime.yml',
                   '-f','deploy/compose.release-test.yml','-f','deploy/compose.preacceptance.yml']
        started = False
        def compose(*parts,timeout=90):
            return subprocess.run(command+list(parts),cwd=ROOT,env=env,capture_output=True,text=True,check=True,timeout=timeout).stdout.strip()
        try:
            check_loopback_ports(ports)  # Never stop another process occupying a port.
            subprocess.run([sys.executable,'scripts/generate_runtime_env.py','--output',str(env_file)],cwd=ROOT,env=env,check=True,capture_output=True)
            assert not compose('ps','-aq'), 'Refusing an existing project'
            compose('config','--quiet')
            started = True
            with (out/'release-startup.log').open('w') as log:
                subprocess.run(command+['up','-d','--no-build','--wait','--wait-timeout','180'],cwd=ROOT,env=env,
                               stdout=log,stderr=log,check=True,timeout=210)
            ids = compose('ps','-aq').splitlines()
            assert len(ids)==10, 'Expected 2 API + 3 Worker + Console + PostgreSQL + Redis + MinIO + fixture'
            raw = json.loads(subprocess.check_output(['docker','inspect']+ids,text=True))
            assert all(x['Config']['Labels']['com.docker.compose.project']==project for x in raw)
            assert all(x['RestartCount']==0 for x in raw), 'Cold startup depended on process restart'
            summary=[]
            for item in raw:
                service=item['Config']['Labels']['com.docker.compose.service']
                summary.append({'service':service,'id':item['Id'],'pid':item['State']['Pid'],
                    'image_id':item['Image'],'restart_count':item['RestartCount'],'ports':item['NetworkSettings']['Ports'],
                    'volumes':[m['Name'] for m in item['Mounts'] if m['Type']=='volume']})
            assert len({x['pid'] for x in summary})==10
            assert all(v.startswith(project+'_') for x in summary for v in x['volumes'])
            (out/'multiport-topology.json').write_text(json.dumps(summary,indent=2)+'\n')
            report['checks'].append('ten-independent-processes-dedicated-volumes-zero-cold-restarts')
            headers={'Authorization':'Bearer '+env['RUNTIME_OPS_TOKEN']}
            with httpx.Client(base_url=urls['console'],headers=headers,timeout=20) as client:
                page=client.get('/')
                assert page.status_code==200 and '/assets/' in page.text
                assert "frame-ancestors 'none'" in page.headers['content-security-policy']
                assert page.headers['referrer-policy']=='no-referrer'
                assert client.get(BASE+'/ops/about').json()['profile']=='synthetic'
                assert client.get(BASE+'/ops/workers',headers={'Authorization':'Bearer '+env['RUNTIME_API_TOKEN']}).status_code==401
            report['checks'].append('console-proxy-security-and-separated-api-auth')
            def control(action,service):
                assert action in {'stop','start','restart'}
                assert service in {'api-a','worker-a','worker-b','worker-c'}
                compose(action,service)
            multiport = verify_multiport(urls,env['RUNTIME_OPS_TOKEN'],control)
            (out/'multiport-acceptance.json').write_text(json.dumps(dict(multiport,tested_commit=report['tested_commit']),ensure_ascii=False,indent=2)+'\n')
            assert multiport['status']=='PASS',multiport.get('error','Multiport checks failed')
            report['checks'].append('eight-multiport-integration-scenarios')
            browser_env=dict(env,PREACCEPTANCE_URL=urls['console'],PREACCEPTANCE_OPS_TOKEN=env['RUNTIME_OPS_TOKEN'],
                             RUNTIME_BROWSER_OUTPUT=str(out/'multiport-browser'))
            with (out/'multiport-browser.log').open('w') as log:
                subprocess.run([sys.executable,'scripts/validate_console_browser.py'],cwd=ROOT,env=browser_env,
                               stdout=log,stderr=log,check=True,timeout=180)
            browser=json.loads((out/'multiport-browser/console-browser.json').read_text())
            assert browser['status']=='PASS' and browser['attached_cluster'] is True
            report['checks'].append('real-browser-on-postgres-backed-release-proxy')
            report['status']='PASS'
        except Exception as exc:
            report.update(error_type=type(exc).__name__,error=str(exc)[:1500])
        finally:
            if started:
                for filename,parts in [('release-containers.log',['logs','--no-color']),('multiport-containers.txt',['ps','-a'])]:
                    with (out/filename).open('w') as log:
                        subprocess.run(command+parts,cwd=ROOT,env=env,stdout=log,stderr=log,timeout=30)
                cleanup=subprocess.run(command+['down','-v','--remove-orphans'],cwd=ROOT,env=env,capture_output=True,text=True,timeout=90)
                report['cleanup_exit_code']=cleanup.returncode
                if cleanup.returncode:
                    report.update(status='FAIL',cleanup_error='Dedicated test project cleanup failed')
    (out/'release-compose.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False))
    return 0 if report['status']=='PASS' else 1


if __name__=='__main__':
    raise SystemExit(main())
