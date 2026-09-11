"""Create a private, non-overwriting environment file. No secrets on stdout."""
import argparse
import os
import secrets
import shlex
from pathlib import Path

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',default='deploy/runtime.env')
    args=parser.parse_args()
    password=secrets.token_hex(24); redis=secrets.token_hex(24)
    values={'POSTGRES_PASSWORD':password,'REDIS_PASSWORD':redis,
        'RUNTIME_DATABASE_URL':f'postgresql+psycopg://runtime:{password}@postgres/runtime?connect_timeout=3&options=-c%20statement_timeout=8000',
        'RUNTIME_REDIS_URL':f'redis://:{redis}@redis:6379/0',
        'RUNTIME_S3_BUCKET':'agent-runtime','RUNTIME_S3_ENDPOINT':'http://minio:9000',
        'AWS_ACCESS_KEY_ID':'runtime-'+secrets.token_hex(8),'AWS_SECRET_ACCESS_KEY':secrets.token_hex(32),
        'RUNTIME_API_TOKEN':secrets.token_hex(32),'RUNTIME_OPS_TOKEN':secrets.token_hex(32),
        'RUNTIME_BIND_ADDRESS':'127.0.0.1','RUNTIME_PORT':'3000','WORKER_SLOTS':'2',
        'RUNTIME_LEASE_SECONDS':'15',
        'MODEL_BASE_URL':'','MODEL_API_KEY':'','MODEL_NAME':'','TOOL_BASE_URL':'','TOOL_API_TOKEN':''}
    # Environment injection is useful for CI secret stores. Still never print values.
    for key in values:
        if os.environ.get(key): values[key]=os.environ[key]
    path=Path(args.output); path.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:
        f.write('# Private configuration. Fill external model/tool values before deployment.\n')
        for key,value in values.items(): f.write(f'{key}={shlex.quote(value)}\n')
    print('Private environment file created. Existing files are never overwritten.')

if __name__=='__main__': main()
