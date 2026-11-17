"""Deterministic routing replay: integer log positions in a named lineage, never a live replica probe.

The replay also models a query-key shared read cache. Cache entries carry the log position
they were fetched at, a fill version and an expiry. Writes advance a per-key mutation version
and register invalidation messages; delivering a message is idempotent and can be delayed,
replayed or reordered. A split read (read_begin/read_return) keeps the version it started with
so an old read cannot backfill a key that was written or invalidated while it was in flight.
"""
import copy,hashlib,json

READ_UNITS=('read','read_begin')
EVENT_KINDS=('write','read','read_begin','read_return','replicate','node_down','node_up','invalidate')


def simulate(node_inputs,events,max_delay_ms,sticky_ms,cache_ttl_ms=1000):
    if not 1<=len(node_inputs)<=12:raise ValueError('需要1–12个节点')
    if not 1<=len(events)<=100:raise ValueError('需要1–100个时序事件')
    if max_delay_ms<0 or sticky_ms<0:raise ValueError('延迟门槛和粘滞时间不得为负')
    if cache_ttl_ms<0:raise ValueError('缓存有效期不得为负')
    if len({x['name'] for x in node_inputs})!=len(node_inputs):raise ValueError('节点名必须唯一')
    primaries=[x for x in node_inputs if x['role']=='primary']
    if len(primaries)!=1:raise ValueError('必须且只能配置一个主节点')
    nodes={x['name']:copy.deepcopy(x) for x in node_inputs};primary=nodes[primaries[0]['name']];lineage=primary['lineage'];sessions={};trace=[];last_time=-1
    if any(n['role']=='replica' and n['lineage']==lineage and n['position']>primary['position'] for n in nodes.values()):raise ValueError('同日志谱系副本位点不得高于主库初始位点')

    cache_entries={};key_states={};messages={};changes=[];pending={}
    cache_stats={'hits':0,'misses':0,'bypasses':0,'origin_reads':0,'backfills':0,'skipped_backfills':0,'expirations':0,'returned_hits':0,'invalidations':{'received':0,'applied':0,'duplicate':0,'stale':0}}

    def key_state(key):
        return key_states.setdefault(key,{'version':0,'invalidated_version':0,'invalidated_position':0,'applied_versions':[],'last_write_position':None})

    def cache_change(at,key,action,reason,entry=None,**extra):
        item={'at_ms':at,'key':key,'action':action,'reason':reason}
        item.update({k:v for k,v in extra.items() if v is not None})
        if entry is not None:item['entry']=entry_view(entry)
        changes.append(item)

    def eligible_replicas(required,session_lineage):
        return [n for n in nodes.values() if n['role']=='replica' and n['status']=='healthy' and n['lineage']==session_lineage and n['position']>=required and n['delay_ms']<=max_delay_ms]

    def select_route(required,session_lineage,at,last_write_ms):
        sticky=last_write_ms is not None and at-last_write_ms<sticky_ms
        candidates=sorted(eligible_replicas(required,session_lineage),key=lambda n:(n['delay_ms'],-n['position'],n['name']))
        if sticky:
            if primary['status']=='healthy' and primary['lineage']==session_lineage and primary['position']>=required:return primary,'写后读粘滞：保持主库'
            return None,'粘滞期主库不可用，拒绝不确定读取'
        if candidates:return candidates[0],'副本位点满足会话且延迟达标'
        if primary['status']=='healthy' and primary['lineage']==session_lineage and primary['position']>=required:return primary,'无安全达标副本，回退主库'
        return None,'主库不可用且无安全达标副本，拒绝读取'

    def replica_checks(required,session_lineage):
        return [{'name':n['name'],'healthy':n['status']=='healthy','same_lineage':n['lineage']==session_lineage,'position_sufficient':n['position']>=required,'delay_within_limit':n['delay_ms']<=max_delay_ms} for n in nodes.values() if n['role']=='replica']

    def lookup(key,at,required,session_lineage):
        entry=cache_entries.get(key)
        if not entry:return None,'未命中：查询键没有缓存条目'
        if at>=entry['expires_at_ms']:
            del cache_entries[key];cache_stats['expirations']+=1
            cache_change(at,key,'expire','缓存条目到达有效期边界，读取按未命中处理',entry=entry,version=entry['version'],position=entry['position'])
            return None,f"未命中：缓存已过有效期（{entry['expires_at_ms']}ms）"
        ks=key_state(key)
        if entry['lineage']!=session_lineage:return None,'未命中：缓存谱系与会话日志谱系不一致'
        if entry['position']<required:return None,f"未命中：缓存位点 {entry['position']} 低于会话所需位点 {required}"
        if entry['version']<ks['invalidated_version'] or entry['position']<ks['invalidated_position']:
            return None,'未命中：缓存条目已被失效消息标记'
        if entry['version']<ks['version'] or (ks['last_write_position'] is not None and entry['position']<ks['last_write_position']):
            return entry,'命中：缓存尚未收到更新失效消息，返回的是写入前的旧快照（延迟失效窗口）'
        return entry,'命中：缓存未过期且位点满足会话要求'

    def entry_view(entry):
        return {'key':entry['key'],'position':entry['position'],'version':entry['version'],'lineage':entry['lineage'],'source':entry['source'],'filled_at_ms':entry['filled_at_ms'],'expires_at_ms':entry['expires_at_ms']}

    def fill_cache(at,key,node_name,position,session_lineage,version):
        ks=key_state(key)
        if position<ks['invalidated_position']:
            cache_stats['skipped_backfills']+=1
            cache_change(at,key,'skip_fill',f'读取位点 {position} 早于失效水位 {ks["invalidated_position"]}，不放入缓存',position=position,version=version)
            return False,f'读取位点 {position} 早于失效水位 {ks["invalidated_position"]}，不回填'
        existing=cache_entries.get(key)
        if existing and (existing['version']>version or (existing['version']==version and existing['position']>position)):
            cache_stats['skipped_backfills']+=1
            reason=f"缓存已有更新内容 v{existing['version']}/位点 {existing['position']}，不回退为 v{version}/位点 {position}"
            cache_change(at,key,'skip_fill',reason,position=position,version=version,entry=existing)
            return False,reason
        entry={'key':key,'position':position,'version':version,'lineage':session_lineage,'source':node_name,'filled_at_ms':at,'expires_at_ms':at+cache_ttl_ms}
        cache_entries[key]=entry;cache_stats['backfills']+=1
        cache_change(at,key,'fill',f'读取结果写入共享缓存（来源 {node_name}，位点 {position}）',entry=entry)
        return True,'缓存已回填'

    for index,event in enumerate(events):
        at=event.get('at_ms');kind=event.get('kind');session=event.get('session','default')
        if kind not in EVENT_KINDS:raise ValueError('未知事件类型：'+str(kind))
        if not isinstance(at,int) or at<0 or at>86400000:raise ValueError('at_ms必须为0–86400000的整数')
        if at<last_time:raise ValueError('事件必须按时间非递减排列')
        last_time=at
        state=sessions.get(session,{'required':0,'lineage':lineage,'last_write_ms':None})
        if kind in ('write','replicate','node_down','node_up') and event.get('key'):raise ValueError('只有读取和失效事件可以指定key')
        if kind in ('write','read','read_begin','read_return','invalidate') and event.get('node'):raise ValueError('读取、写入与失效事件不接受node，读取目标由发起时的安全选路决定')
        if kind in ('write','read','read_begin','read_return','invalidate') and (event.get('position') is not None or event.get('delay_ms') is not None):raise ValueError('读取与写入的位点由模型计算，不接受position/delay_ms')
        if kind!='invalidate' and event.get('version') is not None:raise ValueError('只有失效事件可以携带version')
        if kind=='write' and event.get('keys') is not None:
            keys=event.get('keys')
            if not isinstance(keys,list) or any(not isinstance(k,str) or not k.strip() or len(k)>80 for k in keys):raise ValueError('write的keys必须是由1–80字符查询键组成的数组')
            if len(keys)>20:raise ValueError('单个写入最多关联20个查询键')
        row={'index':index+1,'at_ms':at,'kind':kind,'session':session,'node':None,'decision':'applied','reason':'','required_position':state.get('required',0),'observed_position':None,'safe':True}

        if kind in ('node_down','node_up','replicate'):
            target=nodes.get(event.get('node'))
            if not target:raise ValueError('事件引用未知节点：'+str(event.get('node')))
            row['node']=target['name']
            if kind=='replicate':
                if target['role']!='replica':raise ValueError('replicate仅允许副本节点')
                if target['status']!='healthy':raise ValueError('停机副本不能应用日志，请先node_up')
                if target['lineage']!=lineage:raise ValueError('日志谱系不匹配，不得复制当前主库日志')
                pos=event.get('position');delay=event.get('delay_ms')
                if pos is None or delay is None or delay<0 or not target['position']<=pos<=primary['position']:raise ValueError('复制位点必须单调且不超过已提交主库位点')
                target['position']=pos;target['delay_ms']=delay;row['observed_position']=pos;row['reason']='副本应用已提交日志'
            else:
                target['status']='down' if kind=='node_down' else 'healthy';row['reason']='节点下线' if kind=='node_down' else '节点恢复'
        elif kind=='write':
            if primary['status']!='healthy':row.update(decision='rejected',reason='主库不可用，拒绝写入')
            else:
                primary['position']+=1;sessions[session]={'required':primary['position'],'lineage':lineage,'last_write_ms':at}
                row.update(node=primary['name'],decision='routed',reason='写入主库并记录会话位点',required_position=primary['position'],observed_position=primary['position'])
                emitted=[]
                for key in dict.fromkeys(event.get('keys') or []):
                    ks=key_state(key);ks['version']+=1;ks['last_write_position']=primary['position']
                    version=ks['version'];message={'key':key,'version':version,'message_id':f'{key}@v{version}','position':primary['position'],'emitted_at_ms':at,'session':session,'write_index':index+1,'deliveries':[]}
                    messages[(key,version)]=message
                    emitted.append({'key':key,'version':version,'message_id':message['message_id'],'position':primary['position']})
                    cache_change(at,key,'version_advanced','写入推进查询键版本并登记失效消息',version=version,position=primary['position'],message_id=message['message_id'])
                if emitted:row['cache_messages']=emitted
        if kind in ('read','read_begin'):
            key=event.get('key');required=state.get('required',0);session_lineage=state.get('lineage',lineage)
            row['required_position']=required
            if kind=='read_begin' and not key:raise ValueError('read_begin需要key')
            if not key:
                cache_stats['bypasses']+=1
                selected,reason=select_route(required,session_lineage,at,state.get('last_write_ms'))
                row['reason']=reason;row['cache']={'key':None,'status':'bypass','reason':'未指定查询键，不经过共享缓存','backfill':'not_applicable'}
                if selected:row.update(node=selected['name'],decision='routed',observed_position=selected['position'],safe=selected['position']>=required and selected['lineage']==session_lineage)
                else:row['decision']='rejected'
            else:
                entry,lookup_reason=lookup(key,at,required,session_lineage)
                version=key_state(key)['version']
                if entry:
                    cache_stats['hits']+=1
                    row.update(node='cache',decision='routed',observed_position=entry['position'],reason='缓存命中：按查询键返回共享缓存',safe=entry['position']>=required and entry['lineage']==session_lineage)
                    row['cache']={'key':key,'status':'hit','reason':lookup_reason,'entry_position':entry['position'],'entry_version':entry['version'],'entry_source':entry['source'],'expires_at_ms':entry['expires_at_ms'],'current_key_version':key_state(key)['version'],'invalidated_version':key_state(key)['invalidated_version'],'backfill':'not_applicable'}
                    if kind=='read_begin':
                        pending.setdefault(session,{}).setdefault(key,[]).append({'at_ms':at,'key':key,'required':required,'lineage':session_lineage,'hit':True,'entry':entry_view(entry),'key_version':version,'invalidated_version':key_state(key)['invalidated_version'],'decision':'routed','node':'cache','origin_position':entry['position'],'reason':'缓存命中'})
                else:
                    cache_stats['misses']+=1
                    selected,reason=select_route(required,session_lineage,at,state.get('last_write_ms'))
                    row['cache']={'key':key,'status':'miss','reason':lookup_reason,'backfill':'deferred' if kind=='read_begin' else 'not_applicable'}
                    row['reason']=reason
                    if selected:
                        row.update(node=selected['name'],decision='routed',observed_position=selected['position'],safe=selected['position']>=required and selected['lineage']==session_lineage)
                        if kind=='read':
                            filled,fill_reason=fill_cache(at,key,selected['name'],selected['position'],session_lineage,version)
                            row['cache']['backfill']='filled' if filled else 'skipped';row['cache']['backfill_reason']=fill_reason
                        else:
                            pending.setdefault(session,{}).setdefault(key,[]).append({'at_ms':at,'key':key,'required':required,'lineage':session_lineage,'hit':False,'entry':None,'key_version':version,'invalidated_version':key_state(key)['invalidated_version'],'decision':'routed','node':selected['name'],'origin_position':selected['position'],'reason':reason})
                    else:
                        row['decision']='rejected'
                        row['cache']['backfill']='not_applicable'
                        if kind=='read_begin':
                            pending.setdefault(session,{}).setdefault(key,[]).append({'at_ms':at,'key':key,'required':required,'lineage':session_lineage,'hit':False,'entry':None,'key_version':version,'invalidated_version':key_state(key)['invalidated_version'],'decision':'rejected','node':None,'origin_position':None,'reason':reason})
                    if selected:cache_stats['origin_reads']+=1
            row['key']=key
            row['replica_checks']=replica_checks(required,session_lineage)

        elif kind=='read_return':
            key=event.get('key')
            if not key:raise ValueError('read_return需要key')
            queued=pending.get(session,{}).get(key)
            if not queued:raise ValueError(f'read_return 在 {at}ms 没有对应的 read_begin（会话 {session} / 查询键 {key}）')
            item=queued.pop(0)
            if not queued:del pending[session][key]
            if not pending.get(session):pending.pop(session,None)
            row['required_position']=item['required'];row['key']=key
            if item['decision']=='rejected':
                row.update(decision='rejected',reason='发起时已拒绝：'+item['reason'])
                row['cache']={'key':key,'status':'return','reason':'读取返回：发起时已拒绝，无回源结果','backfill':'not_applicable'}
            else:
                row.update(node=item['node'],decision='routed',observed_position=item['origin_position'],reason='读取返回：'+item['reason'],safe=item['origin_position'] is not None and item['origin_position']>=item['required'] and item['lineage']==lineage)
                if item['hit']:
                    ks=key_state(key);stale_hit=ks['invalidated_version']>item['key_version'] or ks['invalidated_position']>item['origin_position']
                    row['cache']={'key':key,'status':'hit','reason':'读取返回：按发起时命中值返回'+('；发起后该键已被失效消息更新，返回值仅代表发起时快照，不回填' if stale_hit else ''),'entry_position':item['origin_position'],'entry_version':item['entry'].get('version'),'entry_source':item['entry'].get('source'),'expires_at_ms':item['entry'].get('expires_at_ms'),'entry_invalidated_after_begin':stale_hit,'backfill':'not_applicable'}
                else:
                    ks=key_state(key)
                    if ks['version']!=item['key_version']:
                        cache_stats['skipped_backfills']+=1;backfill='skipped';backfill_reason=f"发起后查询键版本已从 v{item['key_version']} 推进到 v{ks['version']}，旧读取不回填";cache_change(at,key,'skip_backfill',backfill_reason,position=item['origin_position'],version=item['key_version'])
                    elif ks['invalidated_version']!=item['invalidated_version']:
                        cache_stats['skipped_backfills']+=1;backfill='skipped';backfill_reason='等待期间收到更新失效消息，旧读取不回填';cache_change(at,key,'skip_backfill',backfill_reason,position=item['origin_position'],version=item['key_version'])
                    elif item['origin_position'] is None or item['origin_position']<ks['invalidated_position']:
                        cache_stats['skipped_backfills']+=1;backfill='skipped';backfill_reason=f'读取位点早于失效水位 {ks["invalidated_position"]}，不回填';cache_change(at,key,'skip_backfill',backfill_reason,position=item['origin_position'],version=item['key_version'])
                    elif cache_entries.get(key) and (cache_entries[key]['version']>item['key_version'] or (cache_entries[key]['version']==item['key_version'] and cache_entries[key]['position']>item['origin_position'])):
                        existing=cache_entries[key];cache_stats['skipped_backfills']+=1;backfill='skipped';backfill_reason=f"缓存已有更新内容 v{existing['version']}/位点 {existing['position']}，旧读取不回填";cache_change(at,key,'skip_backfill',backfill_reason,position=item['origin_position'],version=item['key_version'],entry=existing)
                    else:
                        filled,fill_reason=fill_cache(at,key,item['node'],item['origin_position'],item['lineage'],item['key_version'])
                        backfill='filled' if filled else 'skipped';backfill_reason=fill_reason
                    row['cache']={'key':key,'status':'return','reason':'读取返回：'+item['reason'],'origin_position':item['origin_position'],'origin_node':item['node'],'backfill':backfill,'backfill_reason':backfill_reason}
                if item['hit']:cache_stats['returned_hits']+=1
            row['replica_checks']=replica_checks(item['required'],item['lineage'])

        elif kind=='invalidate':
            key=event.get('key');version=event.get('version')
            if not key or not isinstance(version,int) or version<1:raise ValueError('invalidate需要key和正整数version')
            message=messages.get((key,version))
            if not message:raise ValueError(f'失效消息不存在：{key}@v{version}，需先写入关联该查询键')
            ks=key_state(key);cache_stats['invalidations']['received']+=1
            row['key']=key;row['observed_position']=message['position']
            message['deliveries'].append({'at_ms':at,'result':'duplicate' if version in ks['applied_versions'] else ('stale' if version<ks['invalidated_version'] else 'applied')})
            if version in ks['applied_versions']:
                cache_stats['invalidations']['duplicate']+=1
                row.update(decision='ignored',reason='重复失效消息：幂等忽略，版本未再次推进')
                row['cache']={'key':key,'status':'duplicate','message_id':f'{key}@v{version}','version':version,'position':message['position'],'reason':row['reason'],'backfill':'not_applicable'}
            elif version<ks['invalidated_version']:
                cache_stats['invalidations']['stale']+=1
                row.update(decision='ignored',reason=f'旧失效消息 v{version} 晚于已应用的 v{ks["invalidated_version"]}，不覆盖新状态')
                row['cache']={'key':key,'status':'stale','message_id':f'{key}@v{version}','version':version,'position':message['position'],'reason':row['reason'],'backfill':'not_applicable'}
            else:
                cache_stats['invalidations']['applied']+=1
                ks['invalidated_version']=version;ks['invalidated_position']=max(ks['invalidated_position'],message['position']);ks['applied_versions'].append(version)
                entry=cache_entries.get(key);removed=False
                if entry and (entry['version']<version or entry['position']<message['position']):
                    del cache_entries[key];removed=True
                    reason=f'缓存条目失效：v{entry["version"]}/位点 {entry["position"]} 早于写入 v{version}/位点 {message["position"]}'
                    cache_change(at,key,'invalidate',reason,entry=entry,version=version,position=message['position'],message_id=message['message_id'])
                else:
                    reason='失效水位已登记；当前缓存内容已包含该写入，条目保留' if entry else '失效水位已登记；当前没有缓存条目'
                    cache_change(at,key,'invalidate_recorded',reason,entry=entry,version=version,position=message['position'],message_id=message['message_id'])
                row.update(decision='applied',reason=reason)
                row['cache']={'key':key,'status':'invalidated','message_id':f'{key}@v{version}','version':version,'position':message['position'],'entry_removed':removed,'reason':reason,'backfill':'not_applicable'}

        if 'cache' not in row:
            row['cache']={'key':event.get('key'),'status':'not_applicable','reason':'该事件不读取共享缓存'}
        trace.append(row)

    leftovers=[f'{s}/{k}' for s,keys in sorted(pending.items()) for k,q in sorted(keys.items()) if q]
    if leftovers:raise ValueError('存在未配对 read_return 的分开发起读取：'+'、'.join(leftovers))

    requests=[r for r in trace if r['kind'] in ('read','write') or r['kind']=='read_begin'];routed=[r for r in requests if r['decision']=='routed'];rejected=[r for r in requests if r['decision']=='rejected'];safe=all(r['safe'] for r in routed)
    cache_stats['pending_invalidations']=len(messages)-sum(1 for m in messages.values() if m['deliveries'])
    cache_view={'ttl_ms':cache_ttl_ms,'summary':cache_stats,'chronology':'写入推进查询键版本并登记失效消息；invalidate事件投递消息；read_begin/read_return共享发起时版本','messages':sorted(messages.values(),key=lambda m:m['emitted_at_ms']),'keys':[{'key':k,'version':v['version'],'invalidated_version':v['invalidated_version'],'invalidated_position':v['invalidated_position'],'applied_versions':v['applied_versions'],'last_write_position':v['last_write_position']} for k,v in sorted(key_states.items())],'entries':[entry_view(cache_entries[k]) for k in sorted(cache_entries)],'changes':changes}
    result={'mode':'受控复制路由模型 / 非真实复制集群','lineage':lineage,'trace':trace,'sessions':sessions,'final_nodes':list(nodes.values()),'summary':{'requests':len(requests),'routed':len(routed),'rejected':len(rejected),'replica_reads':sum(r['kind'] in READ_UNITS and r['decision']=='routed' and r['node'] not in (primary['name'],'cache') for r in trace),'primary_reads':sum(r['kind'] in READ_UNITS and r['decision']=='routed' and r['node']==primary['name'] for r in trace),'cache_hits':cache_stats['hits'],'cache_misses':cache_stats['misses'],'read_returns':sum(r['kind']=='read_return' for r in trace),'all_routed_reads_safe':safe},'cache':cache_view,'policy':{'max_delay_ms':max_delay_ms,'sticky_ms':sticky_ms,'cache_ttl_ms':cache_ttl_ms,'position_model':'同一命名日志谱系的单调整数位点，不是完整GTID集合','sticky_boundary':'at_ms - last_write_ms < sticky_ms','cache_expiry_boundary':'at_ms < filled_at_ms + cache_ttl_ms','cache_hit_requirements':'条目未过期、同谱系、位点≥会话所需、未被更新失效消息标记','replica_tie_break':'delay升序 / position降序 / name字典序'}}
    result['checksum']=hashlib.sha256(json.dumps(result,sort_keys=True,ensure_ascii=False).encode()).hexdigest();return result
