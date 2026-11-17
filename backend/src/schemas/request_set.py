from typing import Literal
from pydantic import BaseModel,Field,model_validator
class Event(BaseModel):
    at_ms:int=Field(ge=0,le=86400000)
    kind:Literal['write','read','read_begin','read_return','replicate','node_down','node_up','invalidate']
    session:str=Field(default='default',min_length=1,max_length=60)
    node:str|None=Field(default=None,max_length=80)
    position:int|None=Field(default=None,ge=0,le=1000000000)
    delay_ms:int|None=Field(default=None,ge=0,le=3600000)
    key:str|None=Field(default=None,min_length=1,max_length=80)
    keys:list[str]=Field(default_factory=list,max_length=20)
    version:int|None=Field(default=None,ge=1,le=1000000)
    @model_validator(mode='after')
    def valid_event(self):
        if self.kind in ('replicate','node_down','node_up') and not self.node:raise ValueError('节点事件需要node')
        if self.kind=='replicate' and (self.position is None or self.delay_ms is None):raise ValueError('replicate需要position和delay_ms')
        if self.kind=='invalidate' and (not self.key or self.version is None):raise ValueError('invalidate需要key和version')
        if self.kind=='read_return' and not self.key:raise ValueError('read_return需要key')
        if self.kind=='read_begin' and not self.key:raise ValueError('read_begin需要key')
        if self.kind in ('read','write','invalidate','read_begin','read_return') and (self.position is not None or self.delay_ms is not None):raise ValueError('读取与写入的位点由模型计算，不接受position/delay_ms')
        if self.kind in ('read','write','invalidate','read_begin','read_return') and self.node:raise ValueError('读取与写入不需要node，读取目标由发起时的安全选路决定')
        if self.kind!='write' and self.keys:raise ValueError('只有write可以携带关联查询键keys')
        if self.kind=='write' and any(not k.strip() or len(k)>80 for k in self.keys):raise ValueError('keys中的查询键需为1–80字符')
        if self.kind!='invalidate' and self.version is not None:raise ValueError('只有invalidate可以携带version')
        return self
class RequestSetInput(BaseModel):
    code:str=Field(min_length=2,max_length=80)
    title:str=Field(min_length=2,max_length=160)
    events:list[Event]=Field(min_length=1,max_length=100)
    note:str=Field(default='',max_length=300)
    @model_validator(mode='after')
    def chronological(self):
        if any(a.at_ms>b.at_ms for a,b in zip(self.events,self.events[1:])):raise ValueError('事件必须按at_ms非递减排列')
        return self
