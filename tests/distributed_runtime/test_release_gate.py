import json
import os
import subprocess
import sys
from pathlib import Path
import pytest

ROOT=Path(__file__).resolve().parents[2]

def test_live_gate_without_credentials_is_blocked_and_never_leaks_values(tmp_path):
    script=ROOT/'scripts'/'check_live_configuration.py'
    assert script.exists(), 'Live configuration gate is missing'
    env={k:v for k,v in os.environ.items() if not k.startswith(('MODEL_','TOOL_','RUNTIME_','AWS_'))}
    env['MODEL_API_KEY']='sentinel-secret-never-print'
    output=tmp_path/'gate.json'
    run=subprocess.run([sys.executable,str(script),'--output',str(output)],env=env,capture_output=True,text=True)
    assert run.returncode==2
    report=json.loads(output.read_text())
    assert report['status']=='BLOCKED'
    assert report['live_model_verified'] is False
    assert 'TOOL_BASE_URL' in report['missing']
    assert 'sentinel-secret-never-print' not in output.read_text()+run.stdout+run.stderr

def test_live_deployment_has_no_fixture_gateway_or_default_model_key():
    path=ROOT/'deploy'/'compose.runtime.yml'
    assert path.exists(), 'Live deployment is missing'
    text=path.read_text()
    assert 'fixture-only' not in text and 'gateway:' not in text
    assert '${MODEL_API_KEY:?' in text and '${TOOL_BASE_URL:?' in text
    assert '${RUNTIME_OPS_TOKEN:?' in text

def test_read_only_pending_crash_does_not_create_manual_write_gate(tmp_path):
    from apps.distributed_runtime.store import Store
    from apps.distributed_runtime.tools import EffectLedger
    from .test_recovered_store import package
    store=Store('sqlite:///'+str(tmp_path/'db'),lease_seconds=6)
    try:
        store.deploy(package()); e=store.submit(store.create_session('chat')['id'],'read',{'message':'x'})
        store.register_worker('w',['deepagents'],[],1); claim=store.claim('w')
        EffectLedger(store).begin(claim,'read','query_metric',{'metric':'revenue'})
        store.interrupt(claim)
        assert EffectLedger(store).list(e['id'])[0]['status']=='NOT_EXECUTED'
        assert store.resume(e['id'])['status']=='QUEUED'
    finally: store.db.close()

def test_proxy_security_headers_are_not_shadowed_by_location_headers():
    """Nginx replaces server-level add_header inheritance when a location adds one."""
    import re
    config=(ROOT/'deploy/nginx.runtime.conf').read_text()
    location=re.search(r'location / \{([^}]+)\}',config).group(1)
    assert 'add_header' not in location, 'Location shadows CSP and no-referrer headers'
