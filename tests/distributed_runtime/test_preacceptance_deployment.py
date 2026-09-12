"""Guard multi-instance cold startup and preacceptance-only host ports."""
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[2]

def test_local_infrastructure_gates_every_runtime_on_health():
    services=yaml.safe_load((ROOT/'deploy/compose.infrastructure.yml').read_text())['services']
    for name in ('api-a','api-b','worker-a','worker-b','worker-c'):
        dependencies=services.get(name,{}).get('depends_on',{})
        for dependency in ('postgres','redis','minio'):
            assert dependencies.get(dependency,{}).get('condition')=='service_healthy', (name,dependency,'starts before storage is ready')

def test_runtime_without_local_infrastructure_has_no_local_dependency():
    services=yaml.safe_load((ROOT/'deploy/compose.runtime.yml').read_text())['services']
    assert 'postgres' not in services and 'redis' not in services
    for name in ('api-a','api-b','worker-a','worker-b','worker-c'):
        assert not services[name].get('depends_on')

def test_preacceptance_ports_are_local_and_do_not_change_release():
    overlay=ROOT/'deploy/compose.preacceptance.yml'
    assert overlay.exists(), 'Missing isolated multiport override'
    services=yaml.safe_load(overlay.read_text())['services']
    for name,port in [('api-a','28080'),('api-b','28081')]:
        assert services[name]['ports']==[f'127.0.0.1:${{PREACCEPTANCE_{name[-1].upper()}_PORT:-{port}}}:8000']
    release=yaml.safe_load((ROOT/'deploy/compose.runtime.yml').read_text())['services']
    assert 'ports' not in release['api-a'] and 'ports' not in release['api-b']
