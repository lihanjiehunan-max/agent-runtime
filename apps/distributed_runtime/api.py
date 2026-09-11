"""Stateless HTTP API. SSE disconnect never cancels a durable execution."""
import asyncio
import hmac
import json
import socket
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError
from .store import RuntimeFault, NotFound, TERMINAL
from .config import Settings
from .tools import EffectLedger


class Submit(BaseModel):
    message: str = Field(min_length=1,max_length=30000)
    request_key: str = Field(min_length=1,max_length=160)


class Reconcile(BaseModel):
    executed: bool
    result: dict
    evidence: str = Field(min_length=1,max_length=2000)


def create_app(config=None, store=None, artifacts=None):
    config = config or Settings.from_env()
    store = store or config.store()
    artifacts = artifacts or config.artifacts()

    @asynccontextmanager
    async def lifespan(app):
        async def reaper():
            while True:
                try: await asyncio.to_thread(store.reap)
                except Exception: pass
                await asyncio.sleep(.5)
        task = asyncio.create_task(reaper())
        yield
        task.cancel()
        await asyncio.gather(task,return_exceptions=True)
        store.db.close()

    app = FastAPI(title='Distributed Agent Runtime',version='0.2.0',lifespan=lifespan)

    def auth(authorization: str = Header(default='')):
        accepted = [config.token] + ([config.ops_token] if config.ops_token else [])
        if not any(hmac.compare_digest(authorization,'Bearer '+token) for token in accepted):
            raise HTTPException(status_code=401,detail='Authentication required')

    def ops_auth(authorization: str = Header(default='')):
        if not hmac.compare_digest(authorization,'Bearer '+(config.ops_token or config.token)):
            raise HTTPException(status_code=401,detail='Operations credential required')

    @app.exception_handler(RuntimeFault)
    async def fault(_request,exc):
        return JSONResponse(status_code=404 if isinstance(exc,NotFound) else 409,
                            content={'error':{'code':exc.code,'message':str(exc)}})

    @app.exception_handler(SQLAlchemyError)
    async def unavailable(_request,_exc):
        return JSONResponse(status_code=503,content={'error':{'code':'STORAGE_UNAVAILABLE'}})

    @app.get('/healthz')
    def health():
        with store.db.tx(False) as c: c.exec_driver_sql('SELECT 1')
        return {'status':'ok','node':socket.gethostname()}

    @app.post('/api/v1/runtime/ops/agents/deploy',dependencies=[Depends(ops_auth)])
    def deploy(package:dict): return store.deploy(package)

    @app.get('/api/v1/runtime/ops/agent-instances',dependencies=[Depends(ops_auth)])
    def instances(): return store.instances()

    @app.get('/api/v1/runtime/ops/workers',dependencies=[Depends(ops_auth)])
    def workers(): return store.worker_list()

    @app.post('/api/v1/runtime/agents/{agent_id}/sessions',status_code=201,dependencies=[Depends(auth)])
    def session(agent_id:str,version:str|None=None):
        s=store.create_session(agent_id,version)
        return {'session_id':s['id'],'agent_id':s['agent_id']}

    @app.post('/api/v1/runtime/sessions/{session_id}/executions',status_code=202,dependencies=[Depends(auth)])
    def submit(session_id:str,request:Submit):
        e=store.submit(session_id,request.request_key,{'message':request.message})
        return {'execution_id':e['id'],'session_id':e['session_id'],'status':e['status']}

    @app.get('/api/v1/runtime/executions/{execution_id}',dependencies=[Depends(auth)])
    def execution(execution_id:str):
        e=store.execution(execution_id)
        return {'execution_id':e['id'],'session_id':e['session_id'],'status':e['status'],
                'output_ref':e['output_ref'],'error':e['error']}

    @app.get('/api/v1/runtime/ops/executions/{execution_id}',dependencies=[Depends(ops_auth)])
    def ops_execution(execution_id:str):
        return {'execution':store.execution(execution_id),'attempts':store.attempts(execution_id),
                'tree':store.tree(execution_id),'effects':EffectLedger(store).list(execution_id)}

    @app.get('/api/v1/runtime/executions/{execution_id}/result',dependencies=[Depends(auth)])
    def result(execution_id:str):
        e=store.execution(execution_id)
        if e['status']!='COMPLETED': raise HTTPException(409,'Result is not ready')
        return artifacts.get_json(e['output_ref'])

    @app.post('/api/v1/runtime/executions/{execution_id}/cancel',dependencies=[Depends(auth)])
    def cancel(execution_id:str):
        return {'status':store.cancel(execution_id)['status']}

    @app.post('/api/v1/runtime/ops/executions/{execution_id}/resume',dependencies=[Depends(ops_auth)])
    def resume(execution_id:str):
        return {'status':store.resume(execution_id)['status']}

    @app.post('/api/v1/runtime/ops/executions/{execution_id}/effects/{call_id}/reconcile',dependencies=[Depends(ops_auth)])
    def reconcile(execution_id:str,call_id:str,req:Reconcile):
        EffectLedger(store).reconcile(execution_id,call_id,req.executed,req.result,req.evidence)
        return {'status':'reconciled'}

    @app.get('/api/v1/runtime/executions/{execution_id}/events',dependencies=[Depends(auth)])
    def events(execution_id:str,after:int=Query(default=0,ge=0),tree:bool=False):
        return store.events(execution_id,after=after,tree=tree)

    @app.get('/api/v1/runtime/executions/{execution_id}/stream',dependencies=[Depends(auth)])
    async def stream(execution_id:str,after:int=Query(default=0,ge=0),last_event_id:str|None=Header(default=None)):
        await asyncio.to_thread(store.execution,execution_id)
        try:
            if int(last_event_id or 0)<0: raise ValueError()
            cursor=max(after,int(last_event_id or 0))
        except ValueError: raise HTTPException(422,'Invalid Last-Event-ID')
        async def generate():
            nonlocal cursor
            while True:
                rows=await asyncio.to_thread(store.events,execution_id,cursor)
                for row in rows:
                    cursor=row['id']
                    yield f"id: {cursor}\nevent: {row['type']}\ndata: {json.dumps(row,ensure_ascii=False)}\n\n"
                e=await asyncio.to_thread(store.execution,execution_id)
                if (e['status'] in TERMINAL or e['status']=='INTERRUPTED') and not rows: break
                if not rows: yield ': heartbeat\n\n'
                await asyncio.sleep(.1)
        return StreamingResponse(generate(),media_type='text/event-stream',
            headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})

    @app.get('/api/v1/runtime/ops/stats',dependencies=[Depends(ops_auth)])
    def stats():
        import sqlalchemy as sa
        from . import db as t
        with store.db.tx(False) as c:
            states=dict(c.execute(sa.select(t.executions.c.status,sa.func.count()).group_by(t.executions.c.status)).all())
            active=dict(c.execute(sa.select(t.executions.c.owner,sa.func.count()).where(
                t.executions.c.status.in_(['RUNNING','CANCEL_REQUESTED'])).group_by(t.executions.c.owner)).all())
            pending=c.execute(sa.select(sa.func.count()).select_from(t.outbox).where(t.outbox.c.delivered.is_(False))).scalar_one()
            return {'states':states,'active_per_worker':active,'outbox_pending':pending,'database_time':store.db.now(c)}

    from .operations import install_routes
    install_routes(app,store,artifacts,auth,ops_auth)
    # Optional co-hosting for local tests. The live Compose uses a separate console/proxy.
    from pathlib import Path
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse
    console=Path(__file__).resolve().parents[1]/'runtime_console'/'dist'
    if (console/'index.html').exists():
        app.mount('/assets',StaticFiles(directory=console/'assets'),name='console-assets')
        @app.get('/',include_in_schema=False)
        def console_index():
            return FileResponse(console/'index.html',headers={'Cache-Control':'no-store','X-Content-Type-Options':'nosniff',
                'Referrer-Policy':'no-referrer','Content-Security-Policy':"default-src 'self'; frame-ancestors 'none'; object-src 'none'"})
    app.state.store=store
    return app
