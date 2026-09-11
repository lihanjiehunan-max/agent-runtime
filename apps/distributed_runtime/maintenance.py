"""Offline additive integrity migration. Stop APIs/workers and back up SQL first."""
import argparse
import json
import os
import sqlalchemy as sa
from . import db as t
from .integrity import identity, seal, verify
from .store import Store, Conflict


def adopt_integrity(store, *, reason, acknowledge=False):
    if not acknowledge or not isinstance(reason,str) or not reason.strip():
        raise Conflict('Explicit acknowledgement of a new, non-retroactive integrity baseline is required')
    count=0
    with store.db.tx() as c:
        now=store.db.now(c)
        busy=c.execute(sa.select(sa.func.count()).select_from(t.executions).where(
            t.executions.c.status.in_(['RUNNING','CANCEL_REQUESTED']))).scalar_one()
        live=c.execute(sa.select(sa.func.count()).select_from(t.workers).where(
            t.workers.c.last_seen>=now-max(30,store.lease_seconds*2))).scalar_one()
        if busy or live: raise Conflict('Stop all APIs and Workers before adopting a legacy database')
        for kind,table in [('checkpoint',t.checkpoints),('write',t.checkpoint_writes)]:
            for row in c.execute(sa.select(table)).mappings():
                exists=c.execute(sa.select(t.seals.c.key).where(t.seals.c.key==identity(kind,row))).first()
                if exists: verify(c,kind,row)  # Never repair a mismatch by blessing it.
                else: seal(c,kind,row); count+=1
        c.execute(t.admin_events.insert().values(kind='integrity.legacy_adopted',target='database',
            payload={'adopted':count,'reason':reason,'retroactive_proof':False},created=now))
    return {'adopted':count,'retroactive_proof':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['adopt-integrity'])
    parser.add_argument('--acknowledge-new-baseline',action='store_true')
    parser.add_argument('--reason',required=True)
    args=parser.parse_args()
    store=Store(os.environ['RUNTIME_DATABASE_URL'])
    try:
        print(json.dumps(adopt_integrity(store,reason=args.reason,acknowledge=args.acknowledge_new_baseline)))
    finally: store.db.close()

if __name__=='__main__': main()
