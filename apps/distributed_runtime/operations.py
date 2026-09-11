"""Bounded read projections and operational controls over the existing authority.

Session history is reconstructed from immutable executions and output artifacts;
it is never used as input state for the Harness. Only the Checkpointer owns that.
"""
import asyncio
import sqlalchemy as sa
from fastapi import Depends, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field
from . import db as t
from .store import TERMINAL, Conflict

class Activation(BaseModel):
    version: str = Field(min_length=1,max_length=96)
    reason: str = Field(min_length=1,max_length=1000)

class Drain(BaseModel):
    draining: bool
    reason: str = Field(min_length=1,max_length=1000)


def install_routes(app,store,artifacts,auth,ops_auth):
    from .api import Submit
    prefix='/api/v1/runtime'

    @app.get(prefix+'/ops/about',dependencies=[Depends(ops_auth)])
    def about():
        import os
        from importlib.metadata import version
        return {'profile':os.environ.get('RUNTIME_PROFILE','development'),'engine':'deepagents',
            'engine_version':version('deepagents'),'checkpointer':'FencedSQLSaver',
            'integrity':'sha256-seals-v1','scope':'single-tenant trusted internal deployment',
            'automatic_write_retry':False,'production_acceptance_verified':False}

    @app.post(prefix+'/sessions/{session_id}/invoke',dependencies=[Depends(auth)])
    async def invoke(session_id:str,request:Submit,wait_seconds:float=Query(default=20,ge=0,le=30)):
        e=await asyncio.to_thread(store.submit,session_id,request.request_key,{'message':request.message})
        deadline=asyncio.get_running_loop().time()+wait_seconds
        while True:
            e=await asyncio.to_thread(store.execution,e['id'])
            body={'execution_id':e['id'],'session_id':session_id,'status':e['status']}
            if e['status']=='COMPLETED':
                body['result']=await asyncio.to_thread(artifacts.get_json,e['output_ref'])
                return JSONResponse(body,status_code=200)
            if e['status'] in TERMINAL or e['status']=='INTERRUPTED':
                return JSONResponse(body,status_code=409)
            if asyncio.get_running_loop().time()>=deadline:
                return JSONResponse(body,status_code=202)
            await asyncio.sleep(.05)

    @app.get(prefix+'/sessions/{session_id}/history',dependencies=[Depends(auth)])
    def history(session_id:str,offset:int=Query(default=0,ge=0),limit:int=Query(default=20,ge=1,le=100)):
        store.session(session_id)
        with store.db.tx(False) as c:
            rows=[dict(x) for x in c.execute(sa.select(t.executions).where(t.executions.c.session_id==session_id)
                .order_by(t.executions.c.created,t.executions.c.id).offset(offset).limit(limit+1)).mappings()]
        items=[]
        for row in rows[:limit]:
            item={'execution_id':row['id'],'status':row['status'],'created':row['created'],
                  'input':row['input'].get('message',''),'result':None}
            if row['status']=='COMPLETED':
                try: item['result']=artifacts.get_json(row['output_ref'])
                except Exception: item['result_unavailable']=True
            items.append(item)
        return {'items':items,'next_offset':offset+limit if len(rows)>limit else None}

    @app.get(prefix+'/ops/sessions',dependencies=[Depends(ops_auth)])
    def sessions(offset:int=Query(default=0,ge=0),limit:int=Query(default=50,ge=1,le=100)):
        with store.db.tx(False) as c:
            rows=[dict(x) for x in c.execute(sa.select(t.sessions).order_by(t.sessions.c.created.desc(),
                t.sessions.c.id.desc()).offset(offset).limit(limit+1)).mappings()]
        return {'items':rows[:limit],'next_offset':offset+limit if len(rows)>limit else None}

    @app.get(prefix+'/ops/executions',dependencies=[Depends(ops_auth)])
    def executions(offset:int=Query(default=0,ge=0),limit:int=Query(default=50,ge=1,le=100)):
        with store.db.tx(False) as c:
            rows=[dict(x) for x in c.execute(sa.select(t.executions).order_by(t.executions.c.created.desc(),
                t.executions.c.id.desc()).offset(offset).limit(limit+1)).mappings()]
        return {'items':rows[:limit],'next_offset':offset+limit if len(rows)>limit else None}

    @app.get(prefix+'/ops/executions/{execution_id}/trace',dependencies=[Depends(ops_auth)])
    def trace(execution_id:str,after:int=Query(default=0,ge=0),limit:int=Query(default=200,ge=1,le=1000)):
        execution=store.execution(execution_id)
        root_id=execution['root_id']
        tree=store.tree(execution_id)
        ids={e['instance_id'] for e in tree}
        with store.db.tx(False) as c:
            packages=[dict(x) for x in c.execute(sa.select(t.instances.c.id,t.instances.c.agent_id,
                t.instances.c.version,t.instances.c.digest).where(t.instances.c.id.in_(ids))).mappings()]
            attempts=[dict(x) for x in c.execute(sa.select(t.attempts).where(t.attempts.c.execution_id.in_(
                [e['id'] for e in tree])).order_by(t.attempts.c.started)).mappings()]
        events=store.events(root_id,after=after,limit=limit,tree=True)
        return {'execution_id':execution_id,'root_id':root_id,'thread_id':execution['session_id'],
            'packages':packages,'tree':tree,'attempts':attempts,'events':events,
            'next_cursor':events[-1]['id'] if events else after}

    @app.post(prefix+'/ops/agents/{agent_id}/activate',dependencies=[Depends(ops_auth)])
    def activate(agent_id:str,req:Activation):
        with store.db.tx() as c:
            instance=c.execute(sa.select(t.instances).where(t.instances.c.agent_id==agent_id,
                t.instances.c.version==req.version)).mappings().first()
            if not instance: raise HTTPException(404,'Version has not been deployed')
            from .store import fingerprint
            if fingerprint(instance['package'])!=instance['digest']: raise Conflict('Package integrity check failed')
            c.execute(t.agents.update().where(t.agents.c.id==agent_id).values(active_instance_id=instance['id']))
            c.execute(t.admin_events.insert().values(kind='agent.activated',target=agent_id,
                payload={'version':req.version,'reason':req.reason},created=store.db.now(c)))
        return {'agent_id':agent_id,'version':req.version,'instance_id':instance['id'],'digest':instance['digest']}

    @app.post(prefix+'/ops/workers/{worker_id}/drain',dependencies=[Depends(ops_auth)])
    def drain(worker_id:str,req:Drain):
        with store.db.tx() as c:
            store._row(c,t.workers,worker_id)
            where=t.worker_controls.c.id==worker_id
            if c.execute(sa.select(t.worker_controls.c.id).where(where)).first():
                c.execute(t.worker_controls.update().where(where).values(draining=req.draining))
            else: c.execute(t.worker_controls.insert().values(id=worker_id,draining=req.draining))
            c.execute(t.admin_events.insert().values(kind='worker.drain',target=worker_id,
                payload={'draining':req.draining,'reason':req.reason},created=store.db.now(c)))
            active=c.execute(sa.select(sa.func.count()).select_from(t.executions).where(t.executions.c.owner==worker_id,
                t.executions.c.status.in_(['RUNNING','CANCEL_REQUESTED']))).scalar_one()
        return {'worker_id':worker_id,'draining':req.draining,'active':active,'safe_to_stop':active==0}

    @app.get('/metrics',dependencies=[Depends(ops_auth)],response_class=PlainTextResponse)
    def metrics():
        with store.db.tx(False) as c:
            states=c.execute(sa.select(t.executions.c.status,sa.func.count()).group_by(t.executions.c.status)).all()
            workers=c.execute(sa.select(t.workers)).mappings().all()
            controls=dict(c.execute(sa.select(t.worker_controls.c.id,t.worker_controls.c.draining)).all())
            pending=c.execute(sa.select(sa.func.count()).select_from(t.outbox).where(t.outbox.c.delivered.is_(False))).scalar_one()
            completed,duration=c.execute(sa.select(sa.func.count(),sa.func.coalesce(sa.func.sum(
                t.attempts.c.ended-t.attempts.c.started),0)).where(t.attempts.c.ended.is_not(None))).one()
            now=store.db.now(c)
        lines=['# HELP runtime_executions Durable executions by state.','# TYPE runtime_executions gauge']
        for state,count in states:
            if state in TERMINAL | {'RUNNING','QUEUED','WAITING_CHILDREN','INTERRUPTED','CANCEL_REQUESTED'}:
                lines.append(f'runtime_executions{{status="{state}"}} {count}')
        online=[w for w in workers if now-w['last_seen']<=max(store.lease_seconds*2,3)]
        lines += ['# TYPE runtime_worker_slots gauge',f'runtime_worker_slots {sum(w["slots"] for w in online if not controls.get(w["id"]))}',
                  '# TYPE runtime_workers gauge',f'runtime_workers{{state="online"}} {len(online)}',
                  f'runtime_workers{{state="offline"}} {len(workers)-len(online)}',
                  f'runtime_workers{{state="draining"}} {sum(bool(controls.get(w["id"])) for w in online)}',
                  '# TYPE runtime_outbox_pending gauge',f'runtime_outbox_pending {pending}',
                  '# TYPE runtime_attempt_duration_seconds summary',
                  f'runtime_attempt_duration_seconds_count {completed}',f'runtime_attempt_duration_seconds_sum {duration}']
        return PlainTextResponse('\n'.join(lines)+'\n',media_type='text/plain; version=0.0.4; charset=utf-8')
