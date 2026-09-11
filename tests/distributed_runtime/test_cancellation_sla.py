import asyncio
import time
import pytest
from apps.distributed_runtime.store import Store
from .test_recovered_store import package

@pytest.mark.asyncio
async def test_cancellation_does_not_wait_for_lease_renewal(tmp_path):
    """A six-second lease must not impose a two-second cancel polling floor."""
    from apps.distributed_runtime.worker import Worker
    store = Store('sqlite:///' + str(tmp_path/'cancel.db'), lease_seconds=6)
    store.deploy(package('cancel-agent'))
    started = asyncio.Event()

    class BlockingHarness:
        async def execute(self, claim):
            started.set()
            await asyncio.Event().wait()

    worker = Worker(store, BlockingHarness(), worker_id='cancel-node', slots=1)
    runner = asyncio.create_task(worker.run())
    try:
        execution = store.submit(store.create_session('cancel-agent')['id'], 'cancel-test', {'message': 'wait'})
        await asyncio.wait_for(started.wait(), timeout=5)
        await asyncio.to_thread(store.cancel, execution['id'])
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline:
            if (await asyncio.to_thread(store.execution, execution['id']))['status'] == 'CANCELLED':
                break
            await asyncio.sleep(.03)
        assert store.execution(execution['id'])['status'] == 'CANCELLED', 'Cancellation incorrectly waits for lease renewal'
    finally:
        worker.stop_event.set()
        await runner
        store.db.close()
