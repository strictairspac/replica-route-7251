import {table} from '../components/common/table.js';import {esc} from '../utils/html.js';import {session} from '../hooks/session.js';
const sample=[{at_ms:0,kind:'write',session:'lab-A',keys:['order:QY-42','order:QY-43']},{at_ms:100,kind:'read',session:'lab-A',key:'order:QY-42'},{at_ms:200,kind:'read',session:'lab-A',key:'order:QY-42'},{at_ms:300,kind:'read_begin',session:'lab-A',key:'order:QY-43'},{at_ms:400,kind:'write',session:'lab-A',keys:['order:QY-42','order:QY-43']},{at_ms:500,kind:'read_return',session:'lab-A',key:'order:QY-43'},{at_ms:600,kind:'invalidate',key:'order:QY-43',version:2},{at_ms:700,kind:'invalidate',key:'order:QY-43',version:2}];
const kinds=[['write','write 写入'],['read','read 同步读'],['read_begin','read_begin 发起'],['read_return','read_return 返回'],['invalidate','invalidate 失效'],['replicate','replicate 追赶'],['node_down','node_down 下线'],['node_up','node_up 恢复']];
const allowed={write:['keys'],read:['key'],read_begin:['key'],read_return:['key'],invalidate:['key','version'],replicate:['node','position','delay_ms'],node_down:['node'],node_up:['node']};
const numeric=['at_ms','position','delay_ms','version'];
const defaults={write:{keys:['order:QY-42']},read:{key:'order:QY-42'},read_begin:{key:'order:QY-42'},read_return:{key:'order:QY-42'},invalidate:{key:'order:QY-42',version:1},replicate:{node:'south-replica',position:106,delay_ms:20},node_down:{node:'south-replica'},node_up:{node:'south-replica'}};
export function cleanEvent(event){
  const kind=event.kind,base={at_ms:Number(event.at_ms),kind,session:event.session||'default'};
  for(const field of allowed[kind]||[]){
    if(field==='keys'){
      const keys=(Array.isArray(event.keys)?event.keys:String(event.keys??'').split(',')).map(x=>String(x).trim()).filter(Boolean);
      if(keys.length)base.keys=keys;
    }else if(event[field]!==undefined&&event[field]!==null&&event[field]!==''){
      base[field]=numeric.includes(field)?Number(event[field]):String(event[field]);
    }
  }
  return base;
}
export function parseTimeline(text){
  let value;
  try{value=JSON.parse(text)}catch{throw Error('事件JSON格式错误，请检查引号、逗号与括号')}
  if(!Array.isArray(value)||!value.length)throw Error('时序必须是非空数组');
  const events=value.map(cleanEvent);
  if(events.some((event,index)=>index>0&&event.at_ms<events[index-1].at_ms))throw Error('事件必须按at_ms非递减排列，可点击「按时间排序」调整顺序');
  return events;
}
function input(event,index,field,type='text',extra=''){
  const value=field==='keys'?(Array.isArray(event.keys)?event.keys.join(', '):event.keys??''):event[field]??'';
  return `<input data-index="${index}" data-field="${field}" ${type==='number'?`type="number" min="${field==='version'?1:0}"`:''} value="${esc(value)}" ${extra}>`;
}
export function timelineRows(events){
  const lock=session.user.role==='viewer';
  return events.map((event,index)=>{
    const on=field=>(allowed[event.kind]||[]).includes(field);
    const dis=field=>lock||!on(field)?'disabled':'';
    return `<tr><td class="idx">${index+1}</td><td class="narrow">${input(event,index,'at_ms','number',lock?'disabled':'')}</td><td><select data-index="${index}" data-field="kind" ${lock?'disabled':''}>${kinds.map(([k,label])=>`<option value="${k}" ${k===event.kind?'selected':''}>${label}</option>`).join('')}</select></td><td>${input(event,index,'session','text',lock?'disabled':'')}</td><td>${input(event,index,'keys','text',dis('keys'),'placeholder="a, b"')}</td><td>${input(event,index,'key','text',dis('key'))}</td><td>${input(event,index,'node','text',dis('node'))}</td><td class="narrow">${input(event,index,'position','number',dis('position'))}</td><td class="narrow">${input(event,index,'delay_ms','number',dis('delay_ms'))}</td><td class="narrow">${input(event,index,'version','number',dis('version'))}</td><td class="row-actions"><button type="button" class="small move-event" data-index="${index}" data-dir="-1" ${lock||index===0?'disabled':''} title="上移">↑</button><button type="button" class="small move-event" data-index="${index}" data-dir="1" ${lock||index===events.length-1?'disabled':''} title="下移">↓</button><button type="button" class="small remove-event" data-index="${index}" ${lock?'disabled':''}>删除</button></td></tr>`;
  }).join('');
}
export function timelineSection(){
  const lock=session.user.role==='viewer';
  return `<div class="timeline"><div class="timeline-head"><span class="eyebrow">TIMELINE EDITOR / 事件按at_ms非递减</span><div class="timeline-actions">${kinds.map(([k])=>`<button type="button" class="small insert-event" data-kind="${k}" ${lock?'disabled':''}>+ ${k.split(' ')[0]}</button>`).join('')}<button type="button" class="small sort-events" ${lock?'disabled':''}>按时间排序</button></div></div><div class="table-wrap"><table class="edit-table"><thead><tr><th>#</th><th>at_ms</th><th>事件</th><th>会话</th><th>关联键 keys</th><th>查询键 key</th><th>节点 node</th><th>position</th><th>delay_ms</th><th>version</th><th>操作</th></tr></thead><tbody id="timeline-body">${timelineRows(sample)}</tbody></table></div><p class="timeline-error" id="timeline-error"></p><p class="help">在表格中直接编辑事件并按需增删、调序；文本框是同一份JSON，修改空格或注释后失焦即同步回表格。只有write可填关联键keys；read_begin与read_return必须共用同一个key。</p></div>`;
}
export function mountTimeline(form){
  const area=form?.elements?.events,host=form?.querySelector('#timeline-host');
  if(!area||!host)return;
  host.innerHTML=timelineSection();
  const body=host.querySelector('#timeline-body'),error=host.querySelector('#timeline-error');
  let events;
  const fail=message=>{error.textContent=message||''};
  const sync=()=>{area.value=JSON.stringify(events.map(cleanEvent),null,2)};
  const load=()=>{try{events=parseTimeline(area.value);sync();fail('');return true}catch(err){fail(err.message);return false}};
  const paint=()=>{body.innerHTML=timelineRows(events);bindRows()};
  const commit=()=>{events=events.map(cleanEvent).sort((a,b)=>a.at_ms-b.at_ms);sync();paint();fail('')};
  function bindRows(){
    body.querySelectorAll('input,select').forEach(el=>{
      el.onchange=()=>{
        const index=Number(el.dataset.index),field=el.dataset.field;
        const value=field==='keys'?el.value.split(',').map(x=>x.trim()).filter(Boolean):numeric.includes(field)?Number(el.value):el.value;
        if(field==='kind'){
          events[index]={at_ms:events[index].at_ms,kind:el.value,session:events[index].session,...defaults[el.value]};
          sync();paint();return;
        }
        events[index][field]=value;
        sync();
      };
    });
    body.querySelectorAll('.remove-event').forEach(b=>b.onclick=()=>{events.splice(Number(b.dataset.index),1);if(!events.length)events=[{at_ms:0,kind:'read',session:'default'}];commit()});
    body.querySelectorAll('.move-event').forEach(b=>b.onclick=()=>{
      const i=Number(b.dataset.index),j=i+Number(b.dataset.dir);
      if(j<0||j>=events.length)return;
      if(events[i].at_ms===events[j].at_ms)[events[i],events[j]]=[events[j],events[i]];
      else{const at=events[i].at_ms;events[i].at_ms=events[j].at_ms;events[j].at_ms=at;}
      events=[...events].sort((a,b)=>a.at_ms-b.at_ms);
      sync();paint();
    });
  }
  host.querySelectorAll('.insert-event').forEach(b=>b.onclick=()=>{
    const kind=b.dataset.kind,last=events[events.length-1];
    events.push({at_ms:(last?.at_ms??-100)+100,kind,session:last?.session||'lab-A',...defaults[kind]});
    commit();
  });
  host.querySelector('.sort-events').onclick=()=>{commit();events.sort((a,b)=>a.at_ms-b.at_ms);sync();paint()};
  area.addEventListener('blur',()=>{if(load())paint()});
  if(load())paint();
}
export function requestSetsPage(items){return `<div class="split"><section><div class="section-top"><h2>会话请求轨迹</h2><span>${items.length} SETS / REPLAY INPUT</span></div>${table(['请求集 / 说明','事件数','操作'],items.map(r=>[`<strong>${esc(r.code)}</strong><small>${esc(r.title)}</small><small>${esc(r.note)}</small>`,`${r.events.length} 个`,`<button class="small edit-request" data-id="${r.id}" ${session.user.role==='viewer'?'disabled':''}>编辑</button>`]))}<div class="protocol"><h3>事件字段速查</h3><p>write：at_ms、kind、session，可选keys（写入关联的查询键，推进版本并登记失效消息）。<br>read：可选key；带key时先查共享缓存，未命中再按原规则安全选路。<br>read_begin / read_return：需key，共享发起时的版本，分开发起与返回。<br>invalidate：需key、version，消息可延迟、乱序、重复。<br>node_down / node_up：需node。replicate：需node、position、delay_ms。</p><p>事件按at_ms非递减排列，同一时刻按输入先后。最多100条。复制位点不得倒退或超过主库已提交位点。invalidate的version必须来自先前写入登记的失效消息。</p></div></section><section class="form-panel"><span class="eyebrow">SESSION REQUEST SET</span><h2 id="request-heading">编排会话请求集</h2><form id="request-form"><input type="hidden" name="id"><label>请求集编号<input name="code" placeholder="shift-20261117" required></label><label>工作场景<input name="title" value="交接班写后读复核" required></label><label>时序事件JSON<textarea name="events" rows="11" required>${esc(JSON.stringify(sample,null,2))}</textarea></label><div id="timeline-host"></div><label>备注<input name="note" value="09:15 值班交接复核"></label><button class="primary" ${session.user.role==='viewer'?'disabled':''}>保存会话请求集 +</button></form></section></div>`;}
