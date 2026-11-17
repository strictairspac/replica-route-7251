import json,traceback
from pathlib import Path
from src.config.database import Session
from src.models.entities import Trial
from src.services.trials import queue
from src.services.routing import simulate

def process(ident):
    """Replay one frozen trial. Inputs come solely from the stored snapshot and policy columns."""
    with Session() as db:
        obj=db.get(Trial,ident)
        if not obj:return False
        obj.status='running';db.commit()
        snapshot=obj.input_snapshot;delay=obj.max_delay_ms;sticky=obj.sticky_ms;ttl=obj.cache_ttl_ms
    try:
        result=simulate(snapshot['nodes'],snapshot['request_set']['events'],delay,sticky,ttl)
    except Exception as e:
        traceback.print_exc()
        with Session() as db:
            obj=db.get(Trial,ident)
            if obj:obj.status='failed';obj.error=str(e);db.commit()
        return False
    with Session() as db:
        obj=db.get(Trial,ident);obj.result=result;obj.status='completed';db.commit()
    try:Path(f'/artifacts/route-{ident}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    except OSError:traceback.print_exc()
    return True

def main():
    while True:
        Path('/tmp/worker-heartbeat').touch()
        item=queue.blpop('replicaroute:queue',timeout=2)
        if not item:continue
        process(int(item[1]))

if __name__=='__main__':main()
