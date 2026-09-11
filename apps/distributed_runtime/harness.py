"""Actual DeepAgents graph execution, with durable remote child tasks."""
import asyncio
from importlib.metadata import version
from deepagents import create_deep_agent, register_harness_profile, HarnessProfile, GeneralPurposeSubagentProfile
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langchain.tools import ToolRuntime
from langchain_openai import ChatOpenAI
from langgraph.types import Command, interrupt
from .checkpoints import FencedSQLSaver
from .store import Conflict, TERMINAL
from .tools import EffectLedger, HttpToolGateway
from .telemetry import RuntimeCallbacks

DISABLED = frozenset({'ls','read_file','write_file','edit_file','delete','glob','grep','execute','task'})


class DeepAgentsHarness:
    def __init__(self, store, artifacts, *, model_url, model_key, model_name='fixture', tool_url=None, model_factory=None, tool_token=None):
        self.store, self.artifacts = store, artifacts
        self.model_url, self.model_key, self.model_name = model_url, model_key, model_name
        self.tool_url, self.model_factory = tool_url, model_factory
        self.tool_token = tool_token

    def build(self, claim):
        package = self.store.package_for(claim.execution_id)
        if package['engine'] != 'deepagents' or package['engine_version'] != version('deepagents'):
            raise Conflict('Package runtime version does not match installed DeepAgents')
        model = self.model_factory(package) if self.model_factory else ChatOpenAI(
            model=self.model_name, api_key=self.model_key, base_url=self.model_url,
            streaming=True, max_retries=0, timeout=30)
        name = getattr(model, 'model_name', self.model_name)
        register_harness_profile('openai:'+name, HarnessProfile(excluded_tools=DISABLED,
            general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False)))
        store, artifacts = self.store, self.artifacts
        tools = []
        if package.get('allowed_agents'):
            @tool
            def delegate(children: list[dict], runtime: ToolRuntime) -> list[dict]:
                """Delegate independent tasks. Each child specifies agent_id, version and input."""
                key = runtime.tool_call_id
                store.spawn(claim, key, children)
                results = store.child_results(claim.execution_id, key)
                if any(x['status'] not in TERMINAL for x in results):
                    interrupt({'group': key})
                    results = store.child_results(claim.execution_id, key)
                if any(x['status'] != 'COMPLETED' for x in results):
                    raise Conflict('A delegated child did not complete successfully')
                return [dict(x, result=artifacts.get_json(x['output_ref'])) for x in results]
            tools.append(delegate)
        if package.get('tools'):
            if set(package['tools']) - {'query_metric', 'record_metric'}:
                raise Conflict('Tool is not registered in this runtime')
            if not self.tool_url:
                raise Conflict('Tool gateway is not configured')
            gateway = HttpToolGateway(EffectLedger(store), self.tool_url, token=self.tool_token)
            @tool
            async def query_metric(metric: str, runtime: ToolRuntime, period: str | None = None,
                                   org: str | None = None, comparison: str | None = None,
                                   group_by: list[str] | None = None) -> dict:
                """Read an approved metric. Reuse the conversation's period/org for follow-ups.

                comparison may be yoy or mom. group_by names business dimensions,
                such as route. Never invent a numeric answer when the tool fails.
                """
                from .contracts import MetricQuery
                args = MetricQuery(metric=metric, period=period, org=org,
                                   comparison=comparison, group_by=group_by).model_dump(exclude_none=True)
                return await gateway.invoke(claim, runtime.tool_call_id, 'query_metric', args)
            @tool
            async def record_metric(value: str, runtime: ToolRuntime) -> dict:
                """Record a metric with a durable idempotency key at the configured gateway."""
                return await gateway.invoke(claim, runtime.tool_call_id, 'record_metric', {'value': value})
            tools.extend(x for x in (query_metric, record_metric) if x.name in package['tools'])
        return create_deep_agent(model=model, tools=tools, system_prompt=package['prompt'],
            subagents=[], checkpointer=FencedSQLSaver(store, claim))

    async def execute(self, claim):
        e = await asyncio.to_thread(self.store.execution, claim.execution_id)
        graph = self.build(claim)
        cfg = {'configurable': {'thread_id': e['session_id']}, 'recursion_limit': 50,
               'callbacks':[RuntimeCallbacks(self.store,claim,self.model_name)]}
        from .store import fingerprint
        package=await asyncio.to_thread(self.store.package_for,claim.execution_id)
        await asyncio.to_thread(self.store.emit,claim,'package.loaded',
            {'agent_id':package['agent_id'],'version':package['version'],'digest':fingerprint(package)})
        if e['checkpoint_id']:
            cfg['configurable']['checkpoint_id'] = e['checkpoint_id']
            snapshot = await graph.aget_state(cfg)
            has_interrupt = any(getattr(t, 'interrupts', ()) for t in snapshot.tasks)
            inputs = Command(resume='children-ready') if has_interrupt else None
        else:
            if e['base_checkpoint_id']:
                cfg['configurable']['checkpoint_id'] = e['base_checkpoint_id']
            message = e['input'].get('message')
            if not isinstance(message, str) or not message.strip():
                raise Conflict('Input requires a nonempty message')
            inputs = {'messages': [HumanMessage(content=message, id=e['id']+':input')]}
        await asyncio.to_thread(self.store.emit, claim, 'model.started', {'engine':'deepagents','version':version('deepagents')})
        async for kind, payload in graph.astream(inputs, cfg, stream_mode=['messages','updates']):
            if kind == 'messages':
                msg, meta = payload
                if getattr(msg, 'type', None) == 'AIMessageChunk' and isinstance(msg.content, str) and msg.content:
                    await asyncio.to_thread(self.store.emit, claim, 'model.delta', {'text': msg.content})
        latest = {'configurable': {'thread_id': e['session_id']}}
        snapshot = await graph.aget_state(latest)
        interruptions = [i for task in snapshot.tasks for i in getattr(task, 'interrupts', ())]
        if interruptions:
            group = interruptions[0].value['group']
            await asyncio.to_thread(self.store.wait_children, claim, group)
            return
        messages = snapshot.values.get('messages', [])
        text = next((m.content for m in reversed(messages) if getattr(m,'type',None)=='ai' and not getattr(m,'tool_calls',None)), '')
        if not isinstance(text, str):
            text = str(text)
        ref = await asyncio.to_thread(self.artifacts.put_json, {'message':text, 'session_id':e['session_id']})
        await asyncio.to_thread(self.store.complete, claim, ref)
