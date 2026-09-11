"""Independent Worker process. SQL is authoritative; Redis only accelerates wakeups."""
import asyncio
import logging
import os
import signal
import socket
from uuid import uuid4
from .store import LostLease
from .config import Settings
from .harness import DeepAgentsHarness
from .bus import RedisBus, OutboxPublisher

log = logging.getLogger(__name__)


class Worker:
    def __init__(self, store, harness, *, worker_id=None, slots=2, capabilities=(), bus=None):
        self.store, self.harness, self.slots = store, harness, slots
        self.id = worker_id or socket.gethostname()+':'+uuid4().hex[:12]
        self.capabilities, self.bus = list(capabilities), bus
        self.stop_event = asyncio.Event()
        self.tasks = set()

    async def _heartbeat(self, claim, task):
        while True:
            # Cancellation must not wait for a full lease-renewal interval.
            await asyncio.sleep(min(.25, self.store.lease_seconds/3))
            try:
                await asyncio.to_thread(self.store.heartbeat, claim)
            except Exception as exc:
                log.warning('heartbeat_stopped execution=%s error=%s',claim.execution_id,type(exc).__name__)
                task.cancel()
                return

    async def _execute(self, claim):
        pulse = asyncio.create_task(self._heartbeat(claim, asyncio.current_task()))
        try:
            await self.harness.execute(claim)
        except asyncio.CancelledError:
            try:
                e = await asyncio.to_thread(self.store.execution, claim.execution_id)
                if e['status'] == 'CANCEL_REQUESTED':
                    await asyncio.to_thread(self.store.ack_cancel, claim)
                else:
                    await asyncio.to_thread(self.store.interrupt, claim)
            except Exception:
                pass
        except LostLease:
            log.warning('attempt_fenced execution=%s epoch=%s',claim.execution_id,claim.epoch)
        except Exception as exc:
            log.exception('execution_interrupted execution=%s error=%s',claim.execution_id,type(exc).__name__)
            try:
                await asyncio.to_thread(self.store.interrupt, claim, type(exc).__name__)
            except Exception:
                pass
        finally:
            pulse.cancel()
            await asyncio.gather(pulse,return_exceptions=True)

    async def run(self):
        await asyncio.to_thread(self.store.register_worker,self.id,['deepagents'],self.capabilities,self.slots)
        log.info('worker_registered worker=%s slots=%s',self.id,self.slots)
        publisher = OutboxPublisher(self.store,self.bus) if self.bus else None
        try:
            while not self.stop_event.is_set():
                self.tasks = {t for t in self.tasks if not t.done()}
                try:
                    await asyncio.to_thread(self.store.reap)
                    await asyncio.to_thread(self.store.ping,self.id)
                    while len(self.tasks)<self.slots:
                        claim = await asyncio.to_thread(self.store.claim,self.id)
                        if not claim:
                            break
                        self.tasks.add(asyncio.create_task(self._execute(claim)))
                    if publisher:
                        try:
                            await asyncio.to_thread(publisher.flush,10)
                            for delivery in await asyncio.to_thread(self.bus.poll,10,10):
                                await asyncio.to_thread(self.bus.ack,delivery)
                        except Exception as exc:
                            log.warning('redis_degraded error=%s',type(exc).__name__)
                except Exception as exc:
                    log.warning('coordination_unavailable error=%s',type(exc).__name__)
                try:
                    await asyncio.wait_for(self.stop_event.wait(),timeout=.15)
                except TimeoutError:
                    pass
        finally:
            for task in self.tasks:
                task.cancel()
            await asyncio.gather(*self.tasks,return_exceptions=True)


async def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    config = Settings.from_env()
    store, artifacts = config.store(), config.artifacts()
    ident = socket.gethostname()+':'+uuid4().hex[:12]
    bus = RedisBus(config.redis_url,consumer=ident) if config.redis_url else None
    harness = DeepAgentsHarness(store, artifacts, model_url=os.environ['MODEL_BASE_URL'],
        model_key=os.environ['MODEL_API_KEY'],model_name=os.environ.get('MODEL_NAME','fixture'),
        tool_url=os.environ.get('TOOL_BASE_URL'))
    worker = Worker(store,harness,worker_id=ident,slots=int(os.environ.get('WORKER_SLOTS','2')),
                    capabilities=os.environ.get('WORKER_CAPABILITIES','').split(',') if os.environ.get('WORKER_CAPABILITIES') else [],bus=bus)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT,signal.SIGTERM):
        loop.add_signal_handler(sig,worker.stop_event.set)
    try:
        await worker.run()
    finally:
        if bus: bus.close()
        store.db.close()

if __name__ == '__main__':
    asyncio.run(main())
