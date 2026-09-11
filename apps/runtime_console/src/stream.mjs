/** Incremental, bounded SSE framing. Feed strings produced by a streaming TextDecoder. */
export class SSEParser {
  buffer='';
  feed(chunk) {
    this.buffer+=chunk;
    if(this.buffer.length>262144) throw new Error('SSE frame too large');
    const frames=[];
    while(true){
      const match=/\r?\n\r?\n/.exec(this.buffer);
      if(!match) break;
      const raw=this.buffer.slice(0,match.index);
      this.buffer=this.buffer.slice(match.index+match[0].length);
      let id='',event='message'; const data=[];
      for(const line of raw.split(/\r?\n/)){
        if(line.startsWith(':')) continue;
        const index=line.indexOf(':');
        const field=index<0?line:line.slice(0,index);
        const value=index<0?'':line.slice(index+1).replace(/^ /,'');
        if(field==='id'&&!value.includes('\0')) id=value;
        else if(field==='event') event=value;
        else if(field==='data') data.push(value);
      }
      if(data.length) frames.push({id,event,data:data.join('\n')});
    }
    return frames;
  }
}
