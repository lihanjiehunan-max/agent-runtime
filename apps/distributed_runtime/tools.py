"""Stable tool idempotency identities and explicit uncertain-effect reconciliation.

This is not an exactly-once claim. External write tools must honor the supplied
idempotency key. Cancellation cannot undo an already committed remote operation.
"""
import hashlib
import sqlalchemy as sa
from . import db as t
from .store import Conflict, canonical, checked_key, fingerprint


class EffectLedger:
    def __init__(self,store): self.store=store

    def begin(self,claim,call_id,tool,args):
        checked_key(call_id)
        fp=fingerprint({'tool':tool,'args':args})
        identity=hashlib.sha256((claim.execution_id+'|'+call_id).encode()).hexdigest()
        with self.store.db.tx() as c:
            self.store.owned(c,claim)
            old=c.execute(sa.select(t.effects).where(t.effects.c.execution_id==claim.execution_id,
                t.effects.c.call_id==call_id)).mappings().first()
            if old:
                if old['fingerprint']!=fp: raise Conflict('Tool call identity reused with different arguments')
                if old['status']=='SUCCEEDED':
                    return {'cached':True,'result':old['result'],'idempotency_key':identity}
                if old['status']!='NOT_EXECUTED':
                    raise Conflict('Tool effect is uncertain; reconcile before reissuing')
                c.execute(t.effects.update().where(t.effects.c.execution_id==claim.execution_id,
                    t.effects.c.call_id==call_id).values(status='PENDING'))
            else:
                c.execute(t.effects.insert().values(execution_id=claim.execution_id,call_id=call_id,
                    tool=tool,fingerprint=fp,status='PENDING'))
            self.store._event(c,claim.execution_id,'tool.started',{'call_id':call_id,'tool':tool,'arguments_sha256':fingerprint(args)})
            return {'cached':False,'idempotency_key':identity}

    def succeed(self,claim,call_id,result):
        if len(canonical(result))>32768: raise Conflict('Tool receipt too large; store an artifact reference')
        with self.store.db.tx() as c:
            self.store.owned(c,claim)
            row=c.execute(t.effects.update().where(t.effects.c.execution_id==claim.execution_id,
                t.effects.c.call_id==call_id,t.effects.c.status=='PENDING').values(status='SUCCEEDED',result=result))
            if row.rowcount!=1: raise Conflict('Tool effect is not pending')
            self.store._event(c,claim.execution_id,'tool.completed',{'call_id':call_id,'result_sha256':fingerprint(result)})

    def unknown(self,claim,call_id):
        with self.store.db.tx() as c:
            self.store.owned(c,claim)
            c.execute(t.effects.update().where(t.effects.c.execution_id==claim.execution_id,
                t.effects.c.call_id==call_id,t.effects.c.status=='PENDING').values(status='UNKNOWN'))
            self.store._event(c,claim.execution_id,'tool.unknown',{'call_id':call_id})

    def read_failed(self,claim,call_id):
        """Only the registered read-only operation may release a failed receipt."""
        with self.store.db.tx() as c:
            self.store.owned(c,claim)
            row=c.execute(t.effects.update().where(t.effects.c.execution_id==claim.execution_id,
                t.effects.c.call_id==call_id,t.effects.c.tool=='query_metric',
                t.effects.c.status=='PENDING').values(status='NOT_EXECUTED'))
            if row.rowcount:
                self.store._event(c,claim.execution_id,'tool.read_failed',{'call_id':call_id})

    def reconcile(self,execution_id,call_id,executed,result,evidence):
        if not isinstance(evidence,str) or not evidence.strip() or len(evidence)>2000:
            raise Conflict('A reconciliation evidence statement is required')
        if len(canonical(result))>32768: raise Conflict('Reconciliation result too large')
        with self.store.db.tx() as c:
            effect=c.execute(sa.select(t.effects).where(t.effects.c.execution_id==execution_id,
                t.effects.c.call_id==call_id)).mappings().first()
            if not effect or effect['status']!='UNKNOWN': raise Conflict('Only UNKNOWN effects can be reconciled')
            status='SUCCEEDED' if executed else 'NOT_EXECUTED'
            c.execute(t.effects.update().where(t.effects.c.execution_id==execution_id,
                t.effects.c.call_id==call_id).values(status=status,result=result,evidence=evidence))
            self.store._event(c,execution_id,'tool.reconciled',{'call_id':call_id,'status':status,'evidence':evidence})

    def list(self,execution_id):
        with self.store.db.tx(False) as c:
            return [dict(x) for x in c.execute(sa.select(t.effects).where(t.effects.c.execution_id==execution_id)).mappings()]


class HttpToolGateway:
    """A small contract: POST /tools/{name}/invoke with Idempotency-Key.

    Never forwards the model API key. Register only trusted URLs on the Worker;
    tool URLs cannot be supplied by the model or the business request.
    """
    def __init__(self,ledger,base_url,token=None):
        self.ledger,self.base_url,self.token=ledger,base_url.rstrip('/'),token

    async def invoke(self,claim,call_id,name,args):
        import httpx
        if name not in {'query_metric','record_metric'}:
            raise Conflict('Tool is not registered in this runtime')
        if not isinstance(args,dict) or len(canonical(args))>16384:
            raise Conflict('Tool arguments must be a bounded JSON object')
        receipt=self.ledger.begin(claim,call_id,name,args)
        if receipt['cached']: return receipt['result']
        headers={'Idempotency-Key':receipt['idempotency_key'],
                 'X-Runtime-Execution-Id':claim.execution_id,'X-Runtime-Call-Id':call_id}
        if self.token: headers['Authorization']='Bearer '+self.token
        try:
            async with httpx.AsyncClient(timeout=20,follow_redirects=False) as client:
                async with client.stream('POST',self.base_url+'/tools/'+name+'/invoke',
                        headers=headers,json={'arguments':args}) as response:
                    response.raise_for_status()
                    body=b''
                    async for part in response.aiter_bytes():
                        body+=part
                        if len(body)>32768: raise Conflict('Tool response too large')
            import json
            result=json.loads(body)
            if not isinstance(result,dict):
                raise Conflict('Tool receipt must be a JSON object')
            self.ledger.succeed(claim,call_id,result)
            return result
        except BaseException:
            # A dropped response does not prove that the remote operation failed.
            try:
                if name=='query_metric': self.ledger.read_failed(claim,call_id)
                else: self.ledger.unknown(claim,call_id)
            except Exception: pass  # The reaper also converts fenced PENDING effects to UNKNOWN.
            raise
