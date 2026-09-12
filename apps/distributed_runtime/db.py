"""Shared SQL schema and short, serialized coordination transactions.

The advisory lock is deliberately a coarse MVP lock, not a claim of an
unbounded-throughput scheduler. It is never held across model/tool network I/O.
"""
from contextlib import contextmanager
from pathlib import Path
import sqlalchemy as sa
from sqlalchemy import event

metadata = sa.MetaData()
ID = sa.String(96)

def table(name, *cols, **kw):
    return sa.Table('dh_' + name, metadata, *cols, **kw)

def col(name, type_=ID, **kw):
    return sa.Column(name, type_, **kw)

instances = table('instances', col('id', primary_key=True), col('agent_id', nullable=False),
    col('version', nullable=False), col('digest', nullable=False), col('package', sa.JSON, nullable=False),
    col('created', sa.Float, nullable=False), sa.UniqueConstraint('agent_id', 'version'))
agents = table('agents',col('id',primary_key=True),col('active_instance_id',nullable=False))
sessions = table('sessions', col('id', primary_key=True), col('agent_id', nullable=False),
    col('instance_id', nullable=False), col('active_execution_id'), col('checkpoint_id'),
    col('created', sa.Float, nullable=False))
executions = table('executions', col('id', primary_key=True), col('session_id', nullable=False, index=True),
    col('instance_id', nullable=False), col('parent_id', index=True), col('root_id', nullable=False, index=True),
    col('request_key', sa.String(160), nullable=False), col('input', sa.JSON, nullable=False),
    col('fingerprint', nullable=False), col('status', sa.String(32), nullable=False, index=True),
    col('depth', sa.Integer, nullable=False), col('epoch', sa.Integer, nullable=False, default=0),
    col('owner'), col('lease_until', sa.Float), col('deadline', sa.Float, nullable=False),
    col('join_key', sa.String(160)), col('checkpoint_id'), col('base_checkpoint_id'),
    col('output_ref'), col('error', sa.String(128)), col('cancel_reason', sa.String(32)),
    col('created', sa.Float, nullable=False), col('updated', sa.Float, nullable=False),
    sa.UniqueConstraint('session_id','request_key'))
workers = table('workers', col('id', primary_key=True), col('engines', sa.JSON, nullable=False),
    col('capabilities', sa.JSON, nullable=False), col('slots', sa.Integer, nullable=False),
    col('last_seen', sa.Float, nullable=False))
attempts = table('attempts', col('id', primary_key=True), col('execution_id', nullable=False,index=True),
    col('epoch',sa.Integer,nullable=False),col('worker_id',nullable=False),col('started',sa.Float,nullable=False),
    col('ended',sa.Float), col('outcome',sa.String(32)),sa.UniqueConstraint('execution_id','epoch'))
groups = table('groups',col('parent_id',primary_key=True),col('key',sa.String(160),primary_key=True),
    col('fingerprint',nullable=False))
dependencies = table('dependencies',col('parent_id',primary_key=True),col('group_key',sa.String(160),primary_key=True),
    col('ordinal',sa.Integer,primary_key=True),col('child_id',nullable=False,unique=True))
events = table('events',col('id',sa.Integer,primary_key=True,autoincrement=True),
    col('execution_id',nullable=False,index=True),col('type',sa.String(64),nullable=False),
    col('payload',sa.JSON,nullable=False),col('created',sa.Float,nullable=False))
outbox = table('outbox',col('id',sa.Integer,primary_key=True,autoincrement=True),
    col('execution_id',nullable=False),col('delivered',sa.Boolean,nullable=False,default=False))
checkpoints = table('checkpoints',col('thread_id',primary_key=True),col('namespace',sa.String(512),primary_key=True),
    col('checkpoint_id',primary_key=True),col('execution_id',nullable=False,index=True),col('parent_id'),
    col('type',sa.String(32),nullable=False),col('blob',sa.LargeBinary,nullable=False),
    col('meta_type',sa.String(32),nullable=False),col('meta',sa.LargeBinary,nullable=False))
checkpoint_writes = table('checkpoint_writes',col('execution_id',primary_key=True),col('thread_id',primary_key=True),
    col('namespace',sa.String(512),primary_key=True),col('checkpoint_id',primary_key=True),
    col('task_id',primary_key=True),col('idx',sa.Integer,primary_key=True),col('channel',sa.String(256),nullable=False),
    col('type',sa.String(32),nullable=False),col('blob',sa.LargeBinary,nullable=False))
effects = table('effects',col('execution_id',primary_key=True),col('call_id',sa.String(160),primary_key=True),
    col('tool',sa.String(96),nullable=False),col('fingerprint',nullable=False),col('status',sa.String(32),nullable=False),
    col('result',sa.JSON),col('evidence',sa.Text))

# Additive delivery schema. Existing rows are not silently blessed with hashes.
seals = table('integrity_seals',col('key',primary_key=True),col('kind',sa.String(16),nullable=False),
              col('digest',nullable=False))
worker_controls = table('worker_controls',col('id',primary_key=True),col('draining',sa.Boolean,nullable=False))
admin_events = table('admin_events',col('id',sa.Integer,primary_key=True,autoincrement=True),
                     col('kind',sa.String(64),nullable=False),col('target',nullable=False),
                     col('payload',sa.JSON,nullable=False),col('created',sa.Float,nullable=False))


class Database:
    def __init__(self, url: str, *, initialize: bool = True):
        self.sqlite = url.startswith('sqlite')
        self.engine = sa.create_engine(url, pool_pre_ping=True,
            **({'connect_args': {'timeout': 30, 'check_same_thread': False}} if self.sqlite else {}))
        if self.sqlite:
            @event.listens_for(self.engine, 'connect')
            def configure(dbapi_connection, _record):
                dbapi_connection.execute('PRAGMA busy_timeout=30000')
                dbapi_connection.execute('PRAGMA foreign_keys=ON')
        if initialize:
            with self.tx() as c:
                metadata.create_all(c)
            if self.sqlite:
                with self.engine.connect() as c:
                    c.exec_driver_sql('PRAGMA journal_mode=WAL')

    @contextmanager
    def tx(self, write: bool = True):
        with self.engine.connect() as c:
            try:
                if self.sqlite and write:
                    c.exec_driver_sql('BEGIN IMMEDIATE')
                else:
                    c.begin()
                    if not self.sqlite:
                        # A disconnected client may leave a server transaction
                        # holding the coordinator lock. Bound idle ownership on
                        # the server; statement_timeout alone cannot release it.
                        c.exec_driver_sql("SET LOCAL idle_in_transaction_session_timeout = '5s'")
                    if write:
                        c.execute(sa.text('SELECT pg_advisory_xact_lock(927016431)'))
                yield c
                c.commit()
            except BaseException:
                c.rollback()
                raise

    def now(self, c) -> float:
        sql = "SELECT (julianday('now') - 2440587.5) * 86400.0" if self.sqlite else 'SELECT EXTRACT(EPOCH FROM clock_timestamp())'
        return float(c.exec_driver_sql(sql).scalar_one())

    def close(self):
        self.engine.dispose()
