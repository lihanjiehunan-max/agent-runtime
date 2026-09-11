"""Local deterministic tool-contract fixture; never an enterprise-data source."""
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
from .gateway import app as baseline
app = FastAPI()

@app.post('/tools/query_metric/invoke')
async def metric(request: Request):
    if request.headers.get('authorization') != 'Bearer separate-tool-secret':
        raise HTTPException(401, 'Tool credential required')
    args = (await request.json())['arguments']
    if args['metric'] == 'unavailable':
        raise HTTPException(503, 'Read-only outage')
    if args['metric'] == 'badjson':
        return Response('[1,2,3]', media_type='application/json')
    return {'metric': args['metric'], 'value': 110, 'unit': 'million',
            'period': args.get('period'), 'org': args.get('org'), 'source': 'local-contract-fixture',
            'arguments': args, 'execution_id': request.headers.get('x-runtime-execution-id'),
            'call_id': request.headers.get('x-runtime-call-id')}

app.mount('/', baseline)
