import React,{useEffect,useRef,useState} from 'react';
import {createRoot} from 'react-dom/client';
import {SSEParser} from './stream.mjs';
import {PendingSubmissions} from './submission.mjs';
import './style.css';

type Row=Record<string,any>;
type RuntimeEvent={id:number;type:string;payload:Row;execution_id:string;created:number};
const base='/api/v1/runtime';
const terminal=new Set(['COMPLETED','FAILED','CANCELLED','TIMED_OUT','INTERRUPTED']);
const labels:Record<string,string>={QUEUED:'排队中',RUNNING:'执行中',WAITING_CHILDREN:'等待子任务',
  COMPLETED:'已完成',INTERRUPTED:'已中断',FAILED:'失败',CANCEL_REQUESTED:'取消中',CANCELLED:'已取消',TIMED_OUT:'已超时'};
const sample={agent_id:'chat',version:'1',engine:'deepagents',engine_version:'0.7.7',
  prompt:'你是企业智能助手。依据可验证的信息回答问题；没有依据时说明不确定性。',capabilities:[],timeout:180};
function Status({value}:{value:string}){return <span className={'status s-'+value}>{labels[value]||value}</span>}
function when(value:number){return new Date(value*1000).toLocaleTimeString('zh-CN',{hour12:false})}
function small(value:string){return value?.length>25?value.slice(0,13)+'…'+value.slice(-8):value}

function App(){
  const [credential,setCredential]=useState(''),[connected,setConnected]=useState(false),[notice,setNotice]=useState('');
  const [instances,setInstances]=useState<Row[]>([]),[workers,setWorkers]=useState<Row[]>([]),[stats,setStats]=useState<Row>({});
  const [sessions,setSessions]=useState<Row[]>([]),[executions,setExecutions]=useState<Row[]>([]);
  const [agent,setAgent]=useState('chat'),[session,setSession]=useState(''),[input,setInput]=useState('');
  const [history,setHistory]=useState<Row[]>([]),[current,setCurrent]=useState(''),[liveText,setLiveText]=useState('');
  const [events,setEvents]=useState<RuntimeEvent[]>([]),[trace,setTrace]=useState<Row|null>(null),[detail,setDetail]=useState<Row|null>(null);
  const [packageText,setPackageText]=useState(JSON.stringify(sample,null,2)),[view,setView]=useState('chat');
  const [busy,setBusy]=useState(false),[streaming,setStreaming]=useState(false);
  const [evidence,setEvidence]=useState(''),[receipt,setReceipt]=useState('{}');
  const token=useRef(''),generation=useRef(0),abort=useRef<AbortController|null>(null),cursor=useRef(0);
  const submissions=useRef(new PendingSubmissions()),submitting=useRef(false);
  const activeSession=useRef(''); activeSession.current=session;

  async function call(path:string,method='GET',body?:unknown){
    const epoch=generation.current;
    const r=await fetch(path.startsWith('/')?path:base+path,{method,
      headers:{Authorization:'Bearer '+token.current,...(body===undefined?{}:{'Content-Type':'application/json'})},
      body:body===undefined?undefined:JSON.stringify(body)});
    if(epoch!==generation.current) throw new DOMException('Old connection','AbortError');
    if(!r.ok){const text=await r.text();throw Object.assign(new Error(`${r.status} · ${text.slice(0,400)}`),{status:r.status})}
    return r.json();
  }
  async function act(fn:()=>Promise<void>){setBusy(true);setNotice('');try{await fn()}catch(e){
    if(!(e instanceof DOMException&&e.name==='AbortError')) setNotice(e instanceof Error?e.message:String(e));
  }finally{setBusy(false)}}
  async function refresh(){
    const [s,w,ins,ss,ee,about]=await Promise.all([call(base+'/ops/stats'),call(base+'/ops/workers'),
      call(base+'/ops/agent-instances'),call(base+'/ops/sessions'),call(base+'/ops/executions'),call(base+'/ops/about')]);
    setStats({...s,about});setWorkers(w);setInstances(ins);setSessions(ss.items);setExecutions(ee.items);
  }
  async function connect(){token.current=credential.trim();await refresh();setConnected(true);setNotice('已连接。凭据仅保存在当前页面内存。')}
  function logout(){generation.current++;submissions.current.clear();abort.current?.abort();token.current='';setCredential('');setConnected(false);
    setInstances([]);setWorkers([]);setStats({});setSessions([]);setExecutions([]);setHistory([]);setTrace(null);setDetail(null);
    setEvents([]);setSession('');setCurrent('');setLiveText('');setStreaming(false);setNotice('已断开；服务器中的任务不会因此取消。')}
  useEffect(()=>{if(!connected)return;const timer=setInterval(()=>refresh().catch(()=>{}),2500);
    return()=>clearInterval(timer)},[connected]);
  useEffect(()=>()=>{abort.current?.abort()},[]);
  async function loadHistory(sid:string){const data=await call(base+'/sessions/'+encodeURIComponent(sid)+'/history?limit=100');
    if(activeSession.current===sid)setHistory(data.items)}
  async function pickSession(sid:string){abort.current?.abort();setStreaming(false);activeSession.current=sid;setSession(sid);
    setCurrent('');setLiveText('');setEvents([]);setTrace(null);setInput(submissions.current.pending(sid)?.message||'');await loadHistory(sid)}
  async function createSession(){const s=await call(base+'/agents/'+encodeURIComponent(agent)+'/sessions','POST');
    await pickSession(s.session_id);await refresh()}
  async function inspect(eid:string){const data=await call(base+'/ops/executions/'+encodeURIComponent(eid)+'/trace');
    setTrace(data);setDetail(await call(base+'/ops/executions/'+encodeURIComponent(eid)))}
  async function readStream(eid:string,sid:string,reset=true){
    abort.current?.abort();const control=new AbortController();abort.current=control;
    if(reset){cursor.current=0;setEvents([]);setLiveText('')}
    const epoch=generation.current;setStreaming(true);
    try{
      for(let retry=0;retry<4;retry++){
        try{
          const r=await fetch(base+'/executions/'+encodeURIComponent(eid)+'/stream?after='+cursor.current,
            {headers:{Authorization:'Bearer '+token.current},signal:control.signal});
          if(!r.ok||!r.body)throw new Error('流连接失败：'+r.status);
          const reader=r.body.getReader(),decoder=new TextDecoder(),parser=new SSEParser();
          while(true){const {done,value}=await reader.read();if(done)break;
            for(const frame of parser.feed(decoder.decode(value,{stream:true}))){
              const item=JSON.parse(frame.data) as RuntimeEvent;
              if(item.id<=cursor.current)continue;cursor.current=item.id;
              if(epoch!==generation.current||control.signal.aborted)return;
              setEvents(old=>[...old.slice(-199),item]);
              if(item.type==='model.delta')setLiveText(old=>old+String(item.payload.text||''));
            }
          }
          if(control.signal.aborted)return;
          const e=await call(base+'/executions/'+encodeURIComponent(eid));
          if(terminal.has(e.status)){await loadHistory(sid);await inspect(eid);await refresh();return}
          throw new Error('事件流提前结束');
        }catch(e){if(control.signal.aborted||epoch!==generation.current)return;
          if(retry===3)throw e;await new Promise(resolve=>setTimeout(resolve,250*2**retry))}
      }
    }finally{if(abort.current===control)setStreaming(false)}
  }
  async function send(){
    if(!session||!input.trim()||streaming||submitting.current)return;
    const sid=session,message=input.trim(),epoch=generation.current;
    submitting.current=true;
    try{
      const e=await submissions.current.submit(sid,message,(body:unknown)=>
        call(base+'/sessions/'+encodeURIComponent(sid)+'/executions','POST',body));
      if(epoch!==generation.current||activeSession.current!==sid)return;
      setInput(value=>value.trim()===message?'':value);setCurrent(e.execution_id);await loadHistory(sid);
      // Do not attach an old submission's stream after navigating away.
      if(epoch!==generation.current||activeSession.current!==sid)return;
      readStream(e.execution_id,sid).catch(e=>setNotice('连接已中断，任务仍在服务器执行。'+String(e)));
      await refresh();
    }finally{submitting.current=false}
  }
  async function deploy(){const p=JSON.parse(packageText);await call(base+'/ops/agents/deploy','POST',p);
    setAgent(p.agent_id);await refresh();setNotice('已发布 '+p.agent_id+' @ '+p.version+'；已有会话仍使用原版本。')}
  async function cancel(eid:string){await call(base+'/executions/'+encodeURIComponent(eid)+'/cancel','POST');await refresh();await inspect(eid)}
  async function resume(eid:string){await call(base+'/ops/executions/'+encodeURIComponent(eid)+'/resume','POST');await refresh();
    const state=await call(base+'/executions/'+encodeURIComponent(eid));
    if(session===state.session_id){setCurrent(eid);readStream(eid,session).catch(e=>setNotice(String(e)))}}
  const agentNames=[...new Set(instances.map(x=>String(x.agent_id)))];
  const totals=stats.states||{},total=Object.values(totals).reduce<number>((a,x)=>a+Number(x),0);
  return <div className="shell">
    <header><div className="brand"><span className="brandmark">AR</span><div><strong>Agent Runtime</strong><small>分布式智能体运行控制台</small></div></div>
      <div className="connection"><span className={'dot '+(connected?'online':'')}></span>{connected?(stats.about?.profile==='live'?'外部网关环境':'开发 / 确定性测试环境'):'未连接'}
        {connected?<button className="quiet" onClick={logout}>断开连接</button>:null}</div></header>
    <main>
      {!connected?<section className="login panel"><span className="eyebrow">RUNTIME CONTROL PLANE</span><h1>把智能体任务，交给可靠的运行时。</h1>
        <p>部署版本、创建会话、观察跨 Worker 执行，并在同一处查看检查点与调用证据。</p>
        <form onSubmit={e=>{e.preventDefault();void act(connect)}}><label htmlFor="credential">运维访问令牌</label>
          <div className="inline"><input id="credential" type="password" autoComplete="off" value={credential} onChange={e=>setCredential(e.target.value)} placeholder="输入 RUNTIME_OPS_TOKEN"/>
          <button disabled={busy||!credential.trim()} type="submit">连接运行时</button></div></form>
        <small>令牌不会写入浏览器存储；模型与工具密钥只配置在 Worker 服务端。本控制台面向可信内部运维，不替代企业 SSO。</small>
      </section>:<>
        <section className="intro"><div><span className="eyebrow">EXECUTION OVERVIEW</span><h1>运行总览</h1><p>版本固定 · 会话隔离 · 持久任务 · 可追溯执行</p></div>
          <button className="secondary" disabled={busy} onClick={()=>void act(refresh)}>刷新数据</button></section>
        <section className="cards"><article><span>累计任务</span><strong>{total}</strong><small>持久化执行记录</small></article>
          <article><span>正在执行</span><strong>{totals.RUNNING||0}</strong><small>{totals.QUEUED||0} 个任务排队</small></article>
          <article><span>已完成</span><strong>{totals.COMPLETED||0}</strong><small>{totals.INTERRUPTED||0} 个任务待恢复</small></article>
          <article><span>在线 Worker</span><strong>{workers.filter(w=>w.online).length}<em> / {workers.length}</em></strong><small>按服务端心跳判断</small></article></section>
        <nav aria-label="控制台页面">{[['chat','对话测试'],['tasks','任务与证据'],['agents','Agent 发布'],['workers','Worker 管理']].map(([id,label])=>
          <button key={id} className={view===id?'selected':''} onClick={()=>setView(id)}>{label}</button>)}</nav>
        {view==='chat'?<div className="workspace"><aside className="panel"><h2>会话</h2><label htmlFor="agent">逻辑 Agent</label>
          <select id="agent" value={agent} onChange={e=>setAgent(e.target.value)}>{agentNames.length?agentNames.map(id=><option key={id}>{id}</option>):<option value="chat">请先发布 Agent</option>}</select>
          <button className="wide" disabled={busy||!agentNames.includes(agent)} onClick={()=>void act(createSession)}>创建会话</button>
          <div className="sessionlist">{sessions.filter(s=>s.agent_id===agent).map(s=><button key={s.id} className={'session '+(s.id===session?'active':'')}
            onClick={()=>void act(()=>pickSession(s.id))}><span>{small(s.id)}</span><small>{when(s.created)} · {s.active_execution_id?'执行中':'可继续'}</small></button>)}</div>
          <small>新版本只用于新会话。重启 Worker 不会改变会话已绑定的版本。</small></aside>
          <section className="panel chatpanel"><div className="panelhead"><div><h2>对话测试</h2><small>{session||'选择 Agent 后创建一个会话'}</small></div>
            {streaming?<span className="stream-tag">流式接收中</span>:null}</div>
            <div className="messages" aria-live="polite">{!history.length&&!liveText?<div className="empty"><b>从一个真实任务开始</b><p>会话与执行分别持久化，关闭页面不会终止任务。</p></div>:null}
              {history.map(h=><React.Fragment key={h.execution_id}><article className="message user"><small>你</small><div>{h.input}</div></article>
                <article className="message assistant"><small>Agent <Status value={h.status}/></small><div data-testid="assistant-answer">
                  {h.result?.message||(h.execution_id===current?liveText:'')||(h.result_unavailable?'结果存储暂不可用，可稍后重试。':'等待执行结果…')}</div></article></React.Fragment>)}</div>
            <form className="composer" onSubmit={e=>{e.preventDefault();void act(send)}}><label className="sr-only" htmlFor="message">发送消息</label>
              <textarea id="message" rows={3} value={input} onChange={e=>setInput(e.target.value)} placeholder="输入任务；Ctrl / ⌘ + Enter 发送" disabled={!session}
                onKeyDown={e=>{if(e.key==='Enter'&&(e.ctrlKey||e.metaKey)){e.preventDefault();void act(send)}}}/>
              <div className="composerbar"><small>模型与工具按服务器配置执行，不自动切换到演示模式。</small><button disabled={busy||streaming||!session||!input.trim()}>发送</button></div></form>
            {current?<div className="runbar"><code>{small(current)}</code><button className="quiet" onClick={()=>void act(()=>inspect(current))}>查看证据</button>
              <button className="quiet" disabled={streaming} onClick={()=>void act(()=>readStream(current,session,false))}>重新连接</button>
              <button className="danger quiet" onClick={()=>void act(()=>cancel(current))}>取消任务</button></div>:null}</section>
          <aside className="panel eventpanel"><h2>实时事件</h2><small>展示最近 200 条；完整记录可在任务 Trace 中分页读取。</small>
            <div className="timeline">{events.map(e=><div key={e.id}><span className="dot online"></span><b>{e.type}</b><small>#{e.id} · {when(e.created)}</small>
              {e.type==='model.delta'?null:<code>{JSON.stringify(e.payload).slice(0,130)}</code>}</div>)}</div></aside></div>:null}
        {view==='tasks'?<section className="panel"><div className="panelhead"><h2>最近任务</h2><small>展示最近 50 条；按任务查看完整协作树。</small></div>
          <div className="tablewrap"><table><thead><tr><th>Execution</th><th>Session</th><th>状态</th><th>Worker / Epoch</th><th>操作</th></tr></thead><tbody>{executions.map(e=><tr key={e.id}>
            <td><code>{small(e.id)}</code><small>{e.parent_id?'子任务':'根任务'} · {when(e.created)}</small></td><td><code>{small(e.session_id)}</code></td><td><Status value={e.status}/></td>
            <td>{small(e.owner||'—')} / {e.epoch}</td><td><button className="quiet" onClick={()=>void act(()=>inspect(e.id))}>Trace</button>
              {e.status==='INTERRUPTED'?<button className="quiet" onClick={()=>void act(()=>resume(e.id))}>人工恢复</button>:null}</td></tr>)}</tbody></table></div></section>:null}
        {view==='agents'?<div className="twocol"><section className="panel"><h2>发布 Agent 定义</h2><p>相同 Agent 和版本的内容不可修改。更改提示词或工具后，请递增版本。</p>
          <label htmlFor="package">Agent Package JSON</label><textarea className="codeinput" id="package" rows={17} value={packageText} onChange={e=>setPackageText(e.target.value)}/>
          <button disabled={busy} onClick={()=>void act(deploy)}>发布 Agent</button></section><section className="panel"><h2>已发布版本</h2>
            {instances.map(i=><article className="version" key={i.id}><strong>{i.agent_id} <span>@ {i.version}</span></strong><p>{i.package.engine} / {i.package.engine_version}</p>
              <code title={i.digest}>{small(i.digest)}</code><small>工具：{(i.package.tools||[]).join('、')||'无业务工具'}</small>
              <button className="quiet" onClick={()=>void act(async()=>{await call(base+'/ops/agents/'+encodeURIComponent(i.agent_id)+'/activate','POST',
                {version:i.version,reason:'Explicit activation from operations console'});setNotice('已激活 '+i.agent_id+' @ '+i.version+'，已有会话不变。');await refresh()})}>设为新会话版本</button></article>)}</section></div>:null}
        {view==='workers'?<section className="panel"><h2>Worker 资源</h2><p>排空只阻止新任务领取，已有任务继续执行。确认 active=0 后再停止该进程。</p>
          <div className="tablewrap"><table><thead><tr><th>Worker</th><th>状态</th><th>能力</th><th>槽位</th><th>最后心跳</th><th>操作</th></tr></thead><tbody>{workers.map(w=><tr key={w.id}>
            <td><code>{w.id}</code></td><td>{w.draining?'排空中':w.online?'在线':'离线'}</td><td>{w.capabilities.join(' / ')||'通用'}</td><td>{stats.active_per_worker?.[w.id]||0} / {w.slots}</td><td>{when(w.last_seen)}</td>
            <td><button className="secondary" disabled={busy||!w.online} onClick={()=>void act(async()=>{const r=await call(base+'/ops/workers/'+encodeURIComponent(w.id)+'/drain','POST',
              {draining:!w.draining,reason:'Explicit operator rollout action'});setNotice(r.draining?(r.safe_to_stop?'已排空，可停止该 Worker。':'已停止接收新任务；仍有 '+r.active+' 个执行。'):'已恢复领取任务。');await refresh()})}>{w.draining?'恢复接收':'排空'}</button></td></tr>)}</tbody></table></div></section>:null}
        {trace?<section className="panel trace"><div className="panelhead"><div><h2>执行证据</h2><code>{trace.execution_id}</code></div><button className="quiet" onClick={()=>{setTrace(null);setDetail(null)}}>收起</button></div>
          <div className="tracefacts"><div><small>根任务</small><code>{trace.root_id}</code></div><div><small>LangGraph Thread</small><code>{trace.thread_id}</code></div></div>
          <h3>固定版本与内容摘要</h3>{trace.packages.map((p:Row)=><p key={p.id}><strong>{p.agent_id} @ {p.version}</strong> <code>{p.digest}</code></p>)}
          <h3>协作树与执行尝试</h3><div className="tablewrap"><table><thead><tr><th>任务</th><th>关系</th><th>状态</th><th>Checkpoint</th></tr></thead><tbody>{trace.tree.map((e:Row)=><tr key={e.id}>
            <td><code>{small(e.id)}</code></td><td>{e.parent_id?'子任务 → '+small(e.parent_id):'根任务'}</td><td><Status value={e.status}/></td><td><code>{small(e.checkpoint_id||'—')}</code></td></tr>)}</tbody></table></div>
          {trace.attempts.map((a:Row)=><p className="attempt" key={a.id}><code>{small(a.execution_id)}</code> · {a.worker_id} · Epoch {a.epoch} · {a.outcome||'RUNNING'}</p>)}
          {detail?.effects?.filter((e:Row)=>e.status==='UNKNOWN').map((e:Row)=><div className="reconcile" key={e.call_id}><h3>需要人工对账：{e.tool}</h3>
            <p>先在下游核实此调用结果。取消或重启不能撤销已发生的业务操作。</p><code>{e.call_id}</code>
            <label>对账依据<input value={evidence} onChange={x=>setEvidence(x.target.value)}/></label><label>已执行的结果 JSON<textarea value={receipt} onChange={x=>setReceipt(x.target.value)}/></label>
            <button disabled={!evidence.trim()||busy} onClick={()=>void act(async()=>{await call(base+'/ops/executions/'+encodeURIComponent(trace.execution_id)+'/effects/'+encodeURIComponent(e.call_id)+'/reconcile','POST',
              {executed:true,result:JSON.parse(receipt),evidence});await inspect(trace.execution_id)})}>确认下游已执行</button></div>)}
          <details><summary>事件记录（已加载 {trace.events.length} 条）</summary><pre>{JSON.stringify(trace.events,null,2)}</pre></details>
          <button className="secondary" onClick={()=>void act(async()=>{const next=await call(base+'/ops/executions/'+encodeURIComponent(trace.execution_id)+'/trace?after='+trace.next_cursor);
            setTrace({...next,events:[...trace.events,...next.events]})})}>继续读取事件</button></section>:null}
      </>}
      {notice?<div className="notice" role="status">{notice}</div>:null}
      <footer>Agent Runtime · DeepAgents 0.7.7 <span>执行证据不等于业务结果正确性；真实联调与生产验收需单独确认。</span></footer>
    </main>
  </div>
}
createRoot(document.getElementById('root')!).render(<App/>);
