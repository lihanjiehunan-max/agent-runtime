"""Corruption detection, not a signature against a database administrator.

Hashes cover typed bytes and their identity/metadata. Missing seals fail closed.
An explicit offline adoption is necessary for pre-seal databases; it establishes
only a new baseline, and cannot retroactively prove old data has not changed.
"""
import hashlib
import sqlalchemy as sa
from . import db as t
from .store import canonical, Conflict

IDENTITIES={
    'checkpoint':('thread_id','namespace','checkpoint_id'),
    'write':('execution_id','thread_id','namespace','checkpoint_id','task_id','idx'),
}

def identity(kind,row):
    return hashlib.sha256(canonical({'kind':kind,**{k:row[k] for k in IDENTITIES[kind]}})).hexdigest()

def digest(row):
    value={k:({'bytes_sha256':hashlib.sha256(bytes(v)).hexdigest()} if isinstance(v,(bytes,bytearray,memoryview)) else v)
           for k,v in dict(row).items()}
    return hashlib.sha256(canonical(value)).hexdigest()

def seal(c,kind,row):
    key=identity(kind,row)
    values={'key':key,'kind':kind,'digest':digest(row)}
    if c.execute(sa.select(t.seals.c.key).where(t.seals.c.key==key)).first():
        c.execute(t.seals.update().where(t.seals.c.key==key).values(digest=values['digest']))
    else: c.execute(t.seals.insert().values(**values))

def verify(c,kind,row):
    expected=c.execute(sa.select(t.seals.c.digest).where(t.seals.c.key==identity(kind,row))).scalar()
    if not expected or expected!=digest(row):
        raise Conflict('Checkpoint integrity check failed or legacy seal is missing')
