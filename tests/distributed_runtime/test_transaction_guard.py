"""PostgreSQL transaction guard ordering, without substituting SQLite semantics."""
from contextlib import contextmanager
import pytest
from apps.distributed_runtime.db import Database

class Connection:
    def __init__(self): self.commands=[]
    def begin(self): self.commands.append('BEGIN')
    def exec_driver_sql(self, sql): self.commands.append(str(sql))
    def execute(self, sql): self.commands.append(str(sql))
    def commit(self): self.commands.append('COMMIT')
    def rollback(self): self.commands.append('ROLLBACK')

class Engine:
    def __init__(self, connection): self.connection=connection
    @contextmanager
    def connect(self): yield self.connection

def fake_database(sqlite=False):
    db=object.__new__(Database)
    db.sqlite=sqlite
    c=Connection();db.engine=Engine(c)
    return db,c

def test_server_guard_is_installed_before_taking_coordinator_lock():
    db,c=fake_database()
    with db.tx(): c.commands.append('BODY')
    assert c.commands == ['BEGIN',
        "SET LOCAL idle_in_transaction_session_timeout = '5s'",
        'SELECT pg_advisory_xact_lock(927016431)', 'BODY', 'COMMIT']

def test_postgres_read_transaction_also_bounds_abandoned_snapshot():
    db,c=fake_database()
    with db.tx(False): pass
    assert c.commands == ['BEGIN', "SET LOCAL idle_in_transaction_session_timeout = '5s'", 'COMMIT']

def test_error_still_rolls_back_guarded_transaction():
    db,c=fake_database()
    with pytest.raises(ValueError,match='test'):
        with db.tx(): raise ValueError('test')
    assert c.commands[-1]=='ROLLBACK'
    assert 'COMMIT' not in c.commands

@pytest.mark.parametrize('write',[True,False])
def test_sqlite_never_receives_postgres_guard_or_advisory_lock(write):
    db,c=fake_database(sqlite=True)
    with db.tx(write): pass
    assert c.commands==['BEGIN IMMEDIATE' if write else 'BEGIN','COMMIT']
