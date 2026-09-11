"""Fail closed before real integration. Reports presence, never secret values."""
import argparse
import json
import os
from pathlib import Path
from urllib.parse import urlparse

REQUIRED=('MODEL_BASE_URL','MODEL_API_KEY','MODEL_NAME','TOOL_BASE_URL','TOOL_API_TOKEN',
          'RUNTIME_DATABASE_URL','RUNTIME_REDIS_URL','RUNTIME_S3_BUCKET','RUNTIME_S3_ENDPOINT',
          'AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','RUNTIME_API_TOKEN','RUNTIME_OPS_TOKEN')


def assess(env):
    missing=[name for name in REQUIRED if not env.get(name,'').strip()]
    invalid=[]
    for name in REQUIRED:
        value=env.get(name,'')
        if value and any(s in value.lower() for s in ('change_me','replace_me','fixture-only','validation-token-only')):
            invalid.append(name)
    for name in ('MODEL_BASE_URL','TOOL_BASE_URL','RUNTIME_S3_ENDPOINT'):
        value=env.get(name,'')
        if value:
            url=urlparse(value)
            if url.scheme not in ('http','https') or not url.hostname or url.username or url.password or url.query or url.fragment:
                invalid.append(name)
    for name in ('RUNTIME_API_TOKEN','RUNTIME_OPS_TOKEN'):
        if env.get(name) and len(env[name])<24: invalid.append(name)
    if env.get('RUNTIME_API_TOKEN') and env.get('RUNTIME_API_TOKEN')==env.get('RUNTIME_OPS_TOKEN'):
        invalid.append('RUNTIME_OPS_TOKEN')
    if env.get('MODEL_API_KEY') and env.get('MODEL_API_KEY')==env.get('TOOL_API_TOKEN'):
        invalid.append('TOOL_API_TOKEN')
    if env.get('RUNTIME_DATABASE_URL') and not env['RUNTIME_DATABASE_URL'].startswith('postgresql+psycopg://'):
        invalid.append('RUNTIME_DATABASE_URL')
    if env.get('RUNTIME_REDIS_URL') and not env['RUNTIME_REDIS_URL'].startswith(('redis://','rediss://')):
        invalid.append('RUNTIME_REDIS_URL')
    if env.get('MODEL_NAME','').lower()=='fixture': invalid.append('MODEL_NAME')
    return {'status':'BLOCKED' if missing or invalid else 'CONFIGURED',
        'missing':missing,'invalid':sorted(set(invalid)),
        'live_model_verified':False,'enterprise_tool_verified':False,
        'note':'Configuration presence is not connectivity, business correctness or production acceptance.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',default='validation-results/live-configuration.json')
    args=parser.parse_args(); report=assess(os.environ)
    path=Path(args.output); path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False))
    return 2 if report['status']=='BLOCKED' else 0

if __name__=='__main__': raise SystemExit(main())
