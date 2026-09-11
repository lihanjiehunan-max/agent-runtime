"""Correlated, payload-minimized LangChain callback events.

Do not record prompts, serialized model configuration, authorization headers or
raw exceptions here. Runtime data and real acceptance reports have different
privacy boundaries; public CI reports contain only redacted evidence.
"""
import asyncio
import time
from langchain_core.callbacks import AsyncCallbackHandler

class RuntimeCallbacks(AsyncCallbackHandler):
    raise_error=True
    def __init__(self,store,claim,model_name):
        self.store,self.claim,self.model_name=store,claim,model_name
        self.started={}

    async def on_chat_model_start(self,serialized,messages,*,run_id,parent_run_id=None,**kwargs):
        self.started[str(run_id)]=time.monotonic()
        await asyncio.to_thread(self.store.emit,self.claim,'model.call.started',
            {'call_id':str(run_id),'parent_call_id':str(parent_run_id) if parent_run_id else None,
             'model':self.model_name,'epoch':self.claim.epoch})

    async def on_llm_end(self,response,*,run_id,**kwargs):
        usage={}
        for generation in response.generations:
            for item in generation:
                candidate=getattr(getattr(item,'message',None),'usage_metadata',None)
                if candidate:
                    usage={key:int(candidate[key]) for key in ('input_tokens','output_tokens','total_tokens')
                           if isinstance(candidate.get(key),int)}
        start=self.started.pop(str(run_id),None)
        await asyncio.to_thread(self.store.emit,self.claim,'model.call.completed',
            {'call_id':str(run_id),'duration_seconds':max(0,time.monotonic()-start) if start else None,
             'usage':usage,'usage_reported':bool(usage)})

    async def on_llm_error(self,error,*,run_id,**kwargs):
        self.started.pop(str(run_id),None)
        await asyncio.to_thread(self.store.emit,self.claim,'model.call.failed',
            {'call_id':str(run_id),'error_type':type(error).__name__})
