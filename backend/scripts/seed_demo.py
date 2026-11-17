from sqlalchemy import select,func
from src.config.database import Session,Base,engine
from src.models.entities import User,Node,RequestSet,Trial
from src.utils.security import password_hash

def seed():
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@qiyuan.test'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,password=password_hash('Demo2026!'),role=role))
        for name,role,pos,delay,note in [('harbor-primary','primary',105,0,'栖原主站 · 写入航道'),('north-replica','replica',104,150,'北站副本 · 延迟偏高'),('south-replica','replica',105,20,'南站副本 · 常用读航道')]:
            if not db.scalar(select(Node).where(Node.name==name)):db.add(Node(name=name,role=role,status='healthy',lineage='qiyuan-log-01',position=pos,delay_ms=delay,note=note))
        mixed=[{'at_ms':0,'kind':'write','session':'lab-A'},{'at_ms':100,'kind':'read','session':'lab-A'},{'at_ms':600,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},{'at_ms':700,'kind':'read','session':'lab-A'},{'at_ms':800,'kind':'node_down','node':'south-replica'},{'at_ms':900,'kind':'read','session':'lab-A'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-A'}]
        cache=[{'at_ms':0,'kind':'write','session':'lab-C','keys':['order:QY-42','order:QY-43']},{'at_ms':50,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},{'at_ms':100,'kind':'read','session':'lab-D','key':'order:QY-42'},{'at_ms':200,'kind':'read','session':'lab-D','key':'order:QY-42'},{'at_ms':250,'kind':'read_begin','session':'lab-D','key':'order:QY-43'},{'at_ms':300,'kind':'read_begin','session':'lab-D','key':'order:QY-43'},{'at_ms':400,'kind':'write','session':'lab-C','keys':['order:QY-42','order:QY-43']},{'at_ms':500,'kind':'read_return','session':'lab-D','key':'order:QY-43'},{'at_ms':600,'kind':'read_return','session':'lab-D','key':'order:QY-43'},{'at_ms':700,'kind':'invalidate','key':'order:QY-43','version':2},{'at_ms':800,'kind':'invalidate','key':'order:QY-43','version':2},{'at_ms':900,'kind':'invalidate','key':'order:QY-43','version':1},{'at_ms':1000,'kind':'read','session':'lab-D','key':'order:QY-42'},{'at_ms':1100,'kind':'invalidate','key':'order:QY-42','version':2},{'at_ms':1150,'kind':'replicate','node':'south-replica','position':107,'delay_ms':20},{'at_ms':1200,'kind':'read','session':'lab-D','key':'order:QY-42'}]
        cases=[('QY-1117-A','交接班写后读与双节点故障',mixed),('QY-1117-B','延迟超标回退主库',[{'at_ms':0,'kind':'node_down','node':'south-replica'},{'at_ms':10,'kind':'read','session':'visitor'}]),('QY-1117-C','双副本不可读时拒绝',[{'at_ms':0,'kind':'write','session':'lab-B'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-B'}]),('QY-1117-D','共享读缓存与乱序失效',cache)]
        for code,title,events in cases:
            if not db.scalar(select(RequestSet).where(RequestSet.code==code)):db.add(RequestSet(code=code,title=title,events=events,note='栖原站点值班交接 · 09:15 请求轨迹'))
        db.commit();print({cls.__tablename__:db.scalar(select(func.count()).select_from(cls)) for cls in (User,Node,RequestSet,Trial)})
if __name__=='__main__':seed()
