import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text,inspect
from src.config.database import Base,engine
from src.models import entities
from src.controllers.auth import router as auth
from src.routes import nodes,request_sets,trials,audit
from src.middlewares.errors import ErrorMiddleware
from src.middlewares.request_id import RequestIdMiddleware
def ensure_columns():
    """create_all only creates missing tables; add post-initial columns to existing databases."""
    columns={c['name'] for c in inspect(engine).get_columns('trials')}
    if 'cache_ttl_ms' not in columns:
        with engine.begin() as connection: connection.execute(text('ALTER TABLE trials ADD COLUMN cache_ttl_ms INT NOT NULL DEFAULT 1000'))
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(engine)
    ensure_columns()
    if os.getenv('SEED_DEMO','true')=='true':
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception: logging.exception('seed failed; manual retry is available')
    yield
app=FastAPI(title='站点读流量路由演练',lifespan=lifespan)
app.add_middleware(ErrorMiddleware);app.add_middleware(RequestIdMiddleware)
for r in (auth,nodes.router,request_sets.router,trials.router,audit.router): app.include_router(r)
@app.get('/health')
def health():
    with engine.connect() as c: c.execute(text('SELECT 1'))
    from src.services.trials import queue
    queue.ping()
    return {'status':'healthy'}
