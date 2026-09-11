"""Actual browser + HTTP API + independent Worker, using a labelled local fixture."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import httpx
from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'validation-results';OUT.mkdir(exist_ok=True)

def port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]

def main():
    report={'status':'FAIL','scope':'real Chromium / React console / actual API and DeepAgents Worker; deterministic model',
            'live_model_verified':False,'checks':[]}
    processes=[];logs=[]
    with tempfile.TemporaryDirectory(prefix='runtime-browser-') as folder:
        gateway_port,api_port=port(),port();temp=Path(folder)
        env=dict(os.environ,RUNTIME_DATABASE_URL='sqlite:///'+str(temp/'runtime.db'),
            RUNTIME_API_TOKEN='browser-business-token-000',RUNTIME_OPS_TOKEN='browser-operations-token-000',
            RUNTIME_PROFILE='synthetic',RUNTIME_ARTIFACT_DIR=str(temp/'objects'),
            RUNTIME_REDIS_URL='',RUNTIME_S3_BUCKET='',RUNTIME_LEASE_SECONDS='6',
            MODEL_BASE_URL=f'http://127.0.0.1:{gateway_port}/v1',MODEL_API_KEY='browser-model-fixture-key',MODEL_NAME='fixture',
            TOOL_BASE_URL=f'http://127.0.0.1:{gateway_port}',WORKER_SLOTS='1',FIXTURE_DB=str(temp/'effects.db'))
        def start(name,command):
            log=(temp/(name+'.log')).open('w');logs.append(log)
            p=subprocess.Popen([sys.executable]+command,cwd=ROOT,env=env,stdout=log,stderr=log);processes.append(p)
        def ready(url):
            for _ in range(160):
                try:
                    if httpx.get(url+'/healthz',timeout=.5).status_code==200:return
                except httpx.HTTPError:pass
                time.sleep(.1)
            raise AssertionError('Local browser fixture did not become ready')
        try:
            start('gateway',['-m','uvicorn','tests.distributed_runtime.gateway:app','--port',str(gateway_port)])
            ready(f'http://127.0.0.1:{gateway_port}')
            start('api',['-m','uvicorn','apps.distributed_runtime.api:create_app','--factory','--port',str(api_port)])
            ready(f'http://127.0.0.1:{api_port}')
            start('worker',['-m','apps.distributed_runtime.worker'])
            with sync_playwright() as p:
                browser=p.chromium.launch(headless=True,args=['--no-sandbox'],
                    executable_path=os.environ.get('CHROMIUM_EXECUTABLE') or None)
                page=browser.new_page(viewport={'width':1500,'height':1000},device_scale_factor=1)
                errors=[];page.on('pageerror',lambda exc:errors.append(str(exc)))
                page.goto(f'http://127.0.0.1:{api_port}')
                page.get_by_label('运维访问令牌').fill(env['RUNTIME_OPS_TOKEN'])
                page.get_by_role('button',name='连接运行时',exact=True).click()
                page.get_by_role('heading',name='运行总览',exact=True).wait_for()
                report['checks'].append('operations-login')
                page.get_by_role('button',name='Agent 发布',exact=True).click()
                page.get_by_role('button',name='发布 Agent',exact=True).click()
                page.get_by_role('status').filter(has_text='已发布 chat').wait_for()
                page.get_by_role('button',name='对话测试',exact=True).click()
                page.get_by_role('button',name='创建会话',exact=True).click()
                page.get_by_label('发送消息').fill('remember:browser-proof')
                page.get_by_role('button',name='发送',exact=True).click()
                answer=page.get_by_test_id('assistant-answer').first
                from playwright.sync_api import expect
                expect(answer).to_contain_text('browser-proof',timeout=30000)
                expect(page.get_by_role('button',name='发送',exact=True)).to_be_disabled()  # empty composer
                # Wait for final execution state before the second turn.
                expect(page.locator('.message.assistant .s-COMPLETED').first).to_be_visible(timeout=30000)
                page.get_by_label('发送消息').fill('what did I say?')
                page.get_by_role('button',name='发送',exact=True).click()
                expect(page.get_by_test_id('assistant-answer').nth(1)).to_contain_text('browser-proof',timeout=30000)
                expect(page.locator('.message.assistant .s-COMPLETED')).to_have_count(2,timeout=30000)
                report['checks'].append('two-turn-persistent-session-and-sse')
                # Commit a real POST, then drop only its HTTP response. Retry after
                # selecting another Session; a new key would silently create a duplicate.
                lost_posts=[]; dropped=[False]
                def lose_response(route):
                    request=route.request
                    body=request.post_data_json
                    response=route.fetch()
                    if body.get('message')=='remember:response-loss-proof':
                        record=response.json()
                        lost_posts.append({'key':body['request_key'],'execution_id':record.get('execution_id'),
                                           'session_id':record.get('session_id'),'status':response.status})
                        if not dropped[0]:
                            dropped[0]=True
                            route.abort('connectionreset')
                            return
                    route.fulfill(response=response)
                page.route('**/sessions/*/executions',lose_response)
                page.get_by_label('发送消息').fill('remember:response-loss-proof')
                page.get_by_role('button',name='发送',exact=True).click()
                expect(page.get_by_role('status')).to_contain_text('fetch',timeout=10000)
                assert len(lost_posts)==1 and lost_posts[0]['status']==202
                first=lost_posts[0]
                with httpx.Client(headers={'Authorization':'Bearer '+env['RUNTIME_OPS_TOKEN']},timeout=5) as client:
                    for _ in range(150):
                        state=client.get(f'http://127.0.0.1:{api_port}/api/v1/runtime/executions/'+first['execution_id']).json()
                        if state['status']=='COMPLETED':break
                        time.sleep(.1)
                    assert state['status']=='COMPLETED',state
                page.get_by_role('button',name='创建会话',exact=True).click()
                expect(page.locator('.sessionlist button')).to_have_count(2,timeout=10000)
                page.locator('.sessionlist button').filter(has_text=first['session_id'][:13]).click()
                page.get_by_label('发送消息').fill('remember:response-loss-proof')
                page.get_by_role('button',name='发送',exact=True).click()
                for _ in range(100):
                    if len(lost_posts)==2:break
                    page.wait_for_timeout(50)
                assert len(lost_posts)==2 and lost_posts[1]['status']==202,lost_posts
                assert lost_posts[0]['key']==lost_posts[1]['key'],'Session navigation changed an ambiguous submission key'
                assert lost_posts[0]['execution_id']==lost_posts[1]['execution_id'],'Duplicate execution after lost response'
                expect(page.locator('.message.assistant .s-COMPLETED')).to_have_count(3,timeout=10000)
                report['checks'].append('lost-post-response-session-switch-idempotent-replay')
                report['lost_response']={'http_posts':len(lost_posts),'logical_executions':1,
                                         'execution_id':first['execution_id'],'session_id':first['session_id']}
                page.unroute('**/sessions/*/executions',lose_response)
                page.get_by_role('button',name='查看证据',exact=True).click()
                page.get_by_role('heading',name='执行证据',exact=True).wait_for()
                expect(page.locator('.trace')).to_contain_text('LangGraph Thread')
                expect(page.locator('.trace')).to_contain_text('Epoch 1')
                page.screenshot(path=str(OUT/'console-chat.png'),full_page=True)
                report['checks'].append('trace-package-thread-attempt-checkpoint')
                page.get_by_role('button',name='Worker 管理',exact=True).click()
                page.get_by_role('button',name='排空',exact=True).first.click()
                expect(page.get_by_role('button',name='恢复接收',exact=True).first).to_be_visible(timeout=10000)
                page.screenshot(path=str(OUT/'console-workers.png'),full_page=True)
                page.get_by_role('button',name='恢复接收',exact=True).first.click()
                report['checks'].append('drain-and-enable-worker')
                assert page.evaluate('localStorage.length')==0 and page.evaluate('sessionStorage.length')==0
                page.get_by_role('button',name='断开连接',exact=True).click()
                expect(page.get_by_label('运维访问令牌')).to_have_value('')
                report['checks'].append('no-persistent-browser-credential')
                assert not errors,errors
                report['status']='PASS';browser.close()
        except Exception as exc:
            report['error_type']=type(exc).__name__
            report['error']=str(exc)[:2000]  # Deterministic fixture data only.
        finally:
            for proc in reversed(processes):
                proc.terminate()
                try:proc.wait(timeout=10)
                except subprocess.TimeoutExpired:proc.kill();proc.wait()
            for log in logs:log.close()
    (OUT/'console-browser.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    print(json.dumps(report,ensure_ascii=False))
    return 0 if report['status']=='PASS' else 1

if __name__=='__main__':raise SystemExit(main())
