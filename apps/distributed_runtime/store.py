"""Transactional execution state machine; PostgreSQL is the authority."""
from dataclasses import dataclass
import hashlib
import json
import re
from uuid import uuid4
import sqlalchemy as sa
from . import db as t

TERMINAL = frozenset({'COMPLETED','FAILED','CANCELLED','TIMED_OUT'})
ACTIVE = frozenset({'RUNNING','CANCEL_REQUESTED'})

class RuntimeFault(Exception):
    code = 'RUNTIME_ERROR'
class Conflict(RuntimeFault):
    code = 'CONFLICT'
class NotFound(RuntimeFault):
    code = 'NOT_FOUND'
class LostLease(RuntimeFault):
    code = 'LOST_LEASE'

@dataclass(frozen=True)
class Claim:
    execution_id: str
    worker_id: str
    epoch: int


def canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',',':'), allow_nan=False).encode()

def fingerprint(value):
    return hashlib.sha256(canonical(value)).hexdigest()

def uid(prefix):
    return prefix + '_' + uuid4().hex

def checked_key(key):
    if not isinstance(key,str) or not key.strip() or len(key)>160:
        raise Conflict('A nonempty idempotency key of at most 160 characters is required')

def checked_input(value):
    if not isinstance(value,dict) or len(canonical(value))>32768:
        raise Conflict('Input must be a JSON object no larger than 32768 bytes')


class Store:
    def __init__(self, url: str, *, lease_seconds: float = 15, max_fanout: int = 4,
                 max_depth: int = 3, max_team_size: int = 64, max_running: int = 32):
        if lease_seconds < .3 or min(max_fanout,max_team_size,max_running)<1 or max_depth<0:
            raise ValueError('Invalid runtime limits')
        self.db = t.Database(url)
        self.lease_seconds = lease_seconds
        self.max_fanout, self.max_depth = max_fanout, max_depth
        self.max_team_size, self.max_running = max_team_size, max_running

    def _row(self,c,table,id):
        row = c.execute(sa.select(table).where(table.c.id==id)).mappings().first()
        if row is None:
            raise NotFound(f'{table.name} record not found')
        return dict(row)

    def _event(self,c,eid,kind,payload=None):
        c.execute(t.events.insert().values(execution_id=eid,type=kind,payload=payload or {},created=self.db.now(c)))

    def _queue(self,c,e):
        c.execute(t.executions.update().where(t.executions.c.id==e['id']).values(
            status='QUEUED',owner=None,lease_until=None,updated=self.db.now(c)))
        c.execute(t.outbox.insert().values(execution_id=e['id'],delivered=False))
        self._event(c,e['id'],'execution.queued')

    def deploy(self, package: dict) -> dict:
        required={'agent_id','version','engine','engine_version','prompt','capabilities','timeout'}
        if not required.issubset(package) or len(canonical(package))>262144:
            raise Conflict('Invalid or oversized package')
        if not re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', package['agent_id']):
            raise Conflict('Invalid logical agent ID')
        if not isinstance(package['version'],str) or not 1<=len(package['version'])<=96:
            raise Conflict('Invalid version')
        if not isinstance(package['capabilities'],list) or not all(isinstance(x,str) for x in package['capabilities']):
            raise Conflict('Capabilities must be strings')
        if not isinstance(package['timeout'],(int,float)) or not 1<=package['timeout']<=86400:
            raise Conflict('Timeout must be 1..86400 seconds')
        digest=fingerprint(package)
        with self.db.tx() as c:
            old=c.execute(sa.select(t.instances).where(t.instances.c.agent_id==package['agent_id'],
                t.instances.c.version==package['version'])).mappings().first()
            if old:
                if old['digest']!=digest:
                    raise Conflict('A deployed agent version is immutable; increment its version')
                return dict(old)
            item=dict(id=uid('ins'),agent_id=package['agent_id'],version=package['version'],
                digest=digest,package=package,created=self.db.now(c))
            c.execute(t.instances.insert().values(**item))
            current=c.execute(sa.select(t.agents.c.id).where(t.agents.c.id==package['agent_id'])).first()
            if current:
                c.execute(t.agents.update().where(t.agents.c.id==package['agent_id']).values(active_instance_id=item['id']))
            else:
                c.execute(t.agents.insert().values(id=package['agent_id'],active_instance_id=item['id']))
            return item

    def instances(self):
        with self.db.tx(False) as c:
            return [dict(x) for x in c.execute(sa.select(t.instances).order_by(t.instances.c.created)).mappings()]

    def _new_session(self,c,agent_id,version=None):
        query=sa.select(t.instances).where(t.instances.c.agent_id==agent_id)
        if version:
            query=query.where(t.instances.c.version==version)
        else:
            query=query.where(t.instances.c.id.in_(sa.select(t.agents.c.active_instance_id).where(t.agents.c.id==agent_id)))
        instance=c.execute(query.order_by(t.instances.c.created.desc(),t.instances.c.id.desc())).mappings().first()
        if not instance:
            raise NotFound('Agent has not been deployed')
        item=dict(id=uid('ses'),agent_id=agent_id,instance_id=instance['id'],
            active_execution_id=None,checkpoint_id=None,created=self.db.now(c))
        c.execute(t.sessions.insert().values(**item)); return item

    def create_session(self,agent_id,version=None):
        with self.db.tx() as c:
            return self._new_session(c,agent_id,version)

    def session(self,id):
        with self.db.tx(False) as c:
            return self._row(c,t.sessions,id)

    def execution(self,id):
        with self.db.tx(False) as c:
            return self._row(c,t.executions,id)

    def package_for(self,execution_id):
        with self.db.tx(False) as c:
            e=self._row(c,t.executions,execution_id)
            item=self._row(c,t.instances,e['instance_id'])
            if fingerprint(item['package'])!=item['digest']:
                raise Conflict('Package integrity check failed')
            return item['package']

    def _submit(self,c,session,key,payload,parent=None):
        checked_key(key); checked_input(payload)
        old=c.execute(sa.select(t.executions).where(t.executions.c.session_id==session['id'],
            t.executions.c.request_key==key)).mappings().first()
        fp=fingerprint(payload)
        if old:
            if old['fingerprint']!=fp:
                raise Conflict('Idempotency key reused with different input')
            return dict(old)
        if session['active_execution_id']:
            raise Conflict('SESSION_BUSY')
        instance=self._row(c,t.instances,session['instance_id'])
        now=self.db.now(c); id=uid('exe')
        item=dict(id=id,session_id=session['id'],instance_id=session['instance_id'],
            parent_id=parent['id'] if parent else None,root_id=parent['root_id'] if parent else id,
            request_key=key,input=payload,fingerprint=fp,status='QUEUED',depth=parent['depth']+1 if parent else 0,
            epoch=0,owner=None,lease_until=None,deadline=now+instance['package']['timeout'],join_key=None,
            checkpoint_id=None,base_checkpoint_id=session['checkpoint_id'],output_ref=None,error=None,
            cancel_reason=None,created=now,updated=now)
        c.execute(t.executions.insert().values(**item))
        c.execute(t.sessions.update().where(t.sessions.c.id==session['id']).values(active_execution_id=id))
        self._queue(c,item); return item

    def submit(self,session_id,key,payload):
        with self.db.tx() as c:
            return self._submit(c,self._row(c,t.sessions,session_id),key,payload)

    def register_worker(self,id,engines,capabilities,slots):
        if not isinstance(slots,int) or not 1<=slots<=32 or not engines:
            raise Conflict('Worker needs engines and 1..32 slots')
        with self.db.tx() as c:
            if c.execute(sa.select(t.workers.c.id).where(t.workers.c.id==id)).first():
                raise Conflict('Use a new unique worker ID for each process incarnation')
            c.execute(t.workers.insert().values(id=id,engines=list(engines),capabilities=list(capabilities),
                slots=slots,last_seen=self.db.now(c)))

    def ping(self,id):
        with self.db.tx() as c:
            self._row(c,t.workers,id)
            c.execute(t.workers.update().where(t.workers.c.id==id).values(last_seen=self.db.now(c)))

    def worker_list(self):
        with self.db.tx(False) as c:
            return [dict(x) for x in c.execute(sa.select(t.workers)).mappings()]

    def claim(self,worker_id,execution_id=None):
        with self.db.tx() as c:
            w=self._row(c,t.workers,worker_id); now=self.db.now(c)
            c.execute(t.workers.update().where(t.workers.c.id==worker_id).values(last_seen=now))
            owned=c.execute(sa.select(sa.func.count()).select_from(t.executions).where(
                t.executions.c.owner==worker_id,t.executions.c.status.in_(ACTIVE))).scalar_one()
            active=c.execute(sa.select(sa.func.count()).select_from(t.executions).where(t.executions.c.status.in_(ACTIVE))).scalar_one()
            if owned>=w['slots'] or active>=self.max_running:
                return None
            query=sa.select(t.executions).where(t.executions.c.status=='QUEUED',t.executions.c.deadline>now)
            if execution_id:
                query=query.where(t.executions.c.id==execution_id)
            # Scan all eligible rows; a prefix of mismatched tasks must not starve a capable worker.
            for row in c.execute(query.order_by(t.executions.c.created,t.executions.c.id)).mappings():
                e=dict(row); package=self._row(c,t.instances,e['instance_id'])['package']
                if package['engine'] not in w['engines'] or not set(package['capabilities']).issubset(w['capabilities']):
                    continue
                session=self._row(c,t.sessions,e['session_id'])
                if session['active_execution_id']!=e['id']:
                    raise Conflict('Session reservation invariant violated')
                epoch=e['epoch']+1
                c.execute(t.executions.update().where(t.executions.c.id==e['id']).values(
                    status='RUNNING',owner=worker_id,epoch=epoch,lease_until=now+self.lease_seconds,updated=now))
                c.execute(t.attempts.insert().values(id=uid('att'),execution_id=e['id'],epoch=epoch,
                    worker_id=worker_id,started=now))
                self._event(c,e['id'],'execution.started',{'epoch':epoch,'worker_id':worker_id})
                return Claim(e['id'],worker_id,epoch)
            return None

    def owned(self,c,claim):
        e=self._row(c,t.executions,claim.execution_id); now=self.db.now(c)
        if (e['status']!='RUNNING' or e['epoch']!=claim.epoch or e['owner']!=claim.worker_id
                or (e['lease_until'] or 0)<=now or e['deadline']<=now):
            raise LostLease('Execution no longer authorizes this attempt')
        return e

    def heartbeat(self,claim):
        with self.db.tx() as c:
            self.owned(c,claim)
            c.execute(t.executions.update().where(t.executions.c.id==claim.execution_id).values(
                lease_until=self.db.now(c)+self.lease_seconds))

    def emit(self,claim,kind,payload):
        if len(canonical(payload))>32768:
            raise Conflict('Event payload too large; use an artifact reference')
        with self.db.tx() as c:
            self.owned(c,claim); self._event(c,claim.execution_id,kind,payload)

    def events(self,id,after=0,limit=1000,tree=False):
        with self.db.tx(False) as c:
            self._row(c,t.executions,id)
            q=sa.select(t.events).where(t.events.c.id>after)
            if tree:
                q=q.where(t.events.c.execution_id.in_(sa.select(t.executions.c.id).where(t.executions.c.root_id==id)))
            else:
                q=q.where(t.events.c.execution_id==id)
            return [dict(x) for x in c.execute(q.order_by(t.events.c.id).limit(min(limit,1000))).mappings()]

    def attempts(self,id):
        with self.db.tx(False) as c:
            return [dict(x) for x in c.execute(sa.select(t.attempts).where(
                t.attempts.c.execution_id==id).order_by(t.attempts.c.epoch)).mappings()]

    def tree(self,id):
        with self.db.tx(False) as c:
            e=self._row(c,t.executions,id)
            return [dict(x) for x in c.execute(sa.select(t.executions).where(
                t.executions.c.root_id==e['root_id']).order_by(t.executions.c.created)).mappings()]

    def _end_attempt(self,c,e,status):
        c.execute(t.attempts.update().where(t.attempts.c.execution_id==e['id'],
            t.attempts.c.epoch==e['epoch'],t.attempts.c.ended.is_(None)).values(ended=self.db.now(c),outcome=status))

    def _uncertain_effects(self,c,e):
        c.execute(t.effects.update().where(t.effects.c.execution_id==e['id'],t.effects.c.status=='PENDING').values(status='UNKNOWN'))

    def _finish(self,c,e,status,output_ref=None,error=None):
        self._end_attempt(c,e,status)
        if status!='COMPLETED':
            self._uncertain_effects(c,e)
        c.execute(t.executions.update().where(t.executions.c.id==e['id']).values(
            status=status,owner=None,lease_until=None,output_ref=output_ref,error=error,updated=self.db.now(c)))
        if status in TERMINAL:
            values={'active_execution_id':None}
            if status=='COMPLETED' and e['checkpoint_id']:
                values['checkpoint_id']=e['checkpoint_id']
            c.execute(t.sessions.update().where(t.sessions.c.id==e['session_id'],
                t.sessions.c.active_execution_id==e['id']).values(**values))
        payload={}
        if output_ref: payload['output_ref']=output_ref
        if error: payload['code']=error
        self._event(c,e['id'],'execution.'+status.lower(),payload)
        if e['parent_id']:
            self._wake(c,e['parent_id'])

    def complete(self,claim,output_ref):
        if not isinstance(output_ref,str) or not re.fullmatch('[a-f0-9]{64}',output_ref):
            raise Conflict('Result must be a content-addressed artifact')
        with self.db.tx() as c:
            e=self.owned(c,claim)
            pending=c.execute(sa.select(t.effects.c.call_id).where(t.effects.c.execution_id==e['id'],
                t.effects.c.status.in_(['PENDING','UNKNOWN']))).first()
            if pending: raise Conflict('Uncertain effect must be reconciled')
            self._finish(c,e,'COMPLETED',output_ref)

    def fail(self,claim,code='EXECUTION_FAILED'):
        with self.db.tx() as c:
            e=self.owned(c,claim)
            uncertain=c.execute(sa.select(t.effects.c.call_id).where(t.effects.c.execution_id==e['id'],
                t.effects.c.status.in_(['PENDING','UNKNOWN']))).first()
            self._finish(c,e,'INTERRUPTED' if uncertain else 'FAILED',error=code)
            if not uncertain:
                kids=c.execute(sa.select(t.executions.c.id).where(t.executions.c.parent_id==e['id'])).scalars().all()
                for kid in kids: self._cancel_tree(c,kid,'PARENT_FAILED')

    def interrupt(self,claim,code='WORKER_SHUTDOWN'):
        with self.db.tx() as c:
            e=self.owned(c,claim)
            self._finish(c,e,'INTERRUPTED',error=code)

    def resume(self,id):
        with self.db.tx() as c:
            e=self._row(c,t.executions,id)
            if e['status']!='INTERRUPTED': raise Conflict('Only INTERRUPTED executions may be resumed')
            if c.execute(sa.select(t.effects.c.call_id).where(t.effects.c.execution_id==id,
                    t.effects.c.status.in_(['PENDING','UNKNOWN']))).first():
                raise Conflict('Reconcile UNKNOWN tool effects before resuming')
            timeout=self._row(c,t.instances,e['instance_id'])['package']['timeout']
            c.execute(t.executions.update().where(t.executions.c.id==id).values(deadline=self.db.now(c)+timeout,error=None))
            self._queue(c,e)
            return self._row(c,t.executions,id)

    def spawn(self,claim,key,specs):
        checked_key(key)
        if not isinstance(specs,list) or not 1<=len(specs)<=self.max_fanout:
            raise Conflict('Child fanout limit exceeded')
        if len(canonical(specs))>65536: raise Conflict('Child inputs too large')
        with self.db.tx() as c:
            parent=self.owned(c,claim); fp=fingerprint(specs)
            old=c.execute(sa.select(t.groups).where(t.groups.c.parent_id==parent['id'],t.groups.c.key==key)).mappings().first()
            if old:
                if old['fingerprint']!=fp: raise Conflict('Delegation key reused with different children')
                return self._children(c,parent['id'],key)
            if parent['depth']>=self.max_depth: raise Conflict('Maximum delegation depth exceeded')
            total=c.execute(sa.select(sa.func.count()).select_from(t.executions).where(t.executions.c.root_id==parent['root_id'])).scalar_one()
            if total+len(specs)>self.max_team_size: raise Conflict('Team size limit exceeded')
            if parent['join_key']:
                previous=self._children(c,parent['id'],parent['join_key'])
                if any(x['status'] not in TERMINAL for x in previous):
                    raise Conflict('A delegation group is still in flight')
            allowed=self._row(c,t.instances,parent['instance_id'])['package'].get('allowed_agents')
            c.execute(t.groups.insert().values(parent_id=parent['id'],key=key,fingerprint=fp))
            result=[]
            for index,spec in enumerate(specs):
                if not isinstance(spec,dict) or not isinstance(spec.get('agent_id'),str):
                    raise Conflict('Child needs an agent_id and input')
                if allowed is not None and spec['agent_id'] not in allowed:
                    raise Conflict('Agent is not in this package delegation allowlist')
                session=self._new_session(c,spec['agent_id'],spec.get('version'))
                child=self._submit(c,session,key,spec.get('input',{}),parent)
                c.execute(t.dependencies.insert().values(parent_id=parent['id'],group_key=key,ordinal=index,child_id=child['id']))
                result.append(child)
            c.execute(t.executions.update().where(t.executions.c.id==parent['id']).values(join_key=key))
            self._event(c,parent['id'],'team.spawned',{'group':key,'children':[x['id'] for x in result]})
            return result

    def _children(self,c,id,key):
        q=sa.select(t.executions).join(t.dependencies,t.dependencies.c.child_id==t.executions.c.id).where(
            t.dependencies.c.parent_id==id,t.dependencies.c.group_key==key).order_by(t.dependencies.c.ordinal)
        return [dict(x) for x in c.execute(q).mappings()]

    def child_results(self,id,key):
        with self.db.tx(False) as c:
            return [dict(execution_id=x['id'],session_id=x['session_id'],status=x['status'],
                output_ref=x['output_ref'],error=x['error']) for x in self._children(c,id,key)]

    def _wake(self,c,parent_id):
        parent=self._row(c,t.executions,parent_id)
        if parent['status']!='WAITING_CHILDREN' or not parent['join_key']: return
        children=self._children(c,parent_id,parent['join_key'])
        if children and all(x['status'] in TERMINAL for x in children):
            self._queue(c,parent)
            self._event(c,parent_id,'team.ready',{'group':parent['join_key']})

    def wait_children(self,claim,key):
        with self.db.tx() as c:
            e=self.owned(c,claim)
            children=self._children(c,e['id'],key)
            if not children or e['join_key']!=key: raise Conflict('Unknown delegation group')
            self._end_attempt(c,e,'WAITING_CHILDREN')
            c.execute(t.executions.update().where(t.executions.c.id==e['id']).values(
                status='WAITING_CHILDREN',owner=None,lease_until=None,updated=self.db.now(c)))
            self._event(c,e['id'],'execution.waiting_children',{'group':key})
            self._wake(c,e['id'])

    def _cancel_tree(self,c,id,reason):
        e=self._row(c,t.executions,id)
        if e['status'] not in TERMINAL:
            if e['status'] in ACTIVE:
                c.execute(t.executions.update().where(t.executions.c.id==id).values(status='CANCEL_REQUESTED',
                    cancel_reason=reason,updated=self.db.now(c)))
                if e['status']!='CANCEL_REQUESTED': self._event(c,id,'execution.cancel_requested',{'reason':reason})
            else:
                self._finish(c,e,'TIMED_OUT' if reason=='DEADLINE' else 'CANCELLED')
        kids=c.execute(sa.select(t.executions.c.id).where(t.executions.c.parent_id==id)).scalars().all()
        for kid in kids: self._cancel_tree(c,kid,reason)

    def cancel(self,id):
        with self.db.tx() as c:
            self._cancel_tree(c,id,'USER')
            return self._row(c,t.executions,id)

    def ack_cancel(self,claim):
        with self.db.tx() as c:
            e=self._row(c,t.executions,claim.execution_id)
            if e['status'] in TERMINAL: return
            if e['status']!='CANCEL_REQUESTED' or e['epoch']!=claim.epoch or e['owner']!=claim.worker_id:
                raise LostLease('Cancellation belongs to another attempt')
            self._finish(c,e,'TIMED_OUT' if e['cancel_reason']=='DEADLINE' else 'CANCELLED')

    def reap(self):
        with self.db.tx() as c:
            now=self.db.now(c)
            due=c.execute(sa.select(t.executions.c.id).where(t.executions.c.deadline<=now,
                t.executions.c.status.not_in(TERMINAL|{'CANCEL_REQUESTED'}))).scalars().all()
            for id in due:
                self._cancel_tree(c,id,'DEADLINE')
            expired=c.execute(sa.select(t.executions).where(t.executions.c.status.in_(ACTIVE),t.executions.c.lease_until<=now)).mappings().all()
            for row in expired:
                e=dict(row)
                if e['status']=='CANCEL_REQUESTED':
                    self._finish(c,e,'TIMED_OUT' if e['cancel_reason']=='DEADLINE' else 'CANCELLED')
                else:
                    self._finish(c,e,'INTERRUPTED',error='WORKER_LEASE_EXPIRED')
                c.execute(t.executions.update().where(t.executions.c.id==e['id']).values(epoch=e['epoch']+1))
            return len(due)+len(expired)
