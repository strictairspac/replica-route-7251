import json
import os
import tempfile
import unittest
from pathlib import Path

DB_PATH = Path(tempfile.gettempdir()) / f'replicaroute-api-test-{os.getpid()}.db'
os.environ['DATABASE_URL'] = f'sqlite:///{DB_PATH}'
os.environ['REDIS_URL'] = 'redis://localhost:6399/0'
os.environ['SEED_DEMO'] = 'true'

from fastapi.testclient import TestClient  # noqa: E402

import src.services.trials as trials_service  # noqa: E402


class FakeQueue:
    def __init__(self):
        self.items = []

    def rpush(self, name, value):
        self.items.append(value)

    def blpop(self, name, timeout=0):
        return (name, self.items.pop(0)) if self.items else None

    def ping(self):
        return True


queue = FakeQueue()
trials_service.queue = queue

from src.config.database import Session  # noqa: E402
from src.main import app  # noqa: E402
from src.models.entities import Trial  # noqa: E402
from src.services.routing import simulate  # noqa: E402


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.client.__enter__()
        response = cls.client.post('/auth/login', json={'email': 'operator@qiyuan.test', 'password': 'Demo2026!'})
        cls.headers = {'Authorization': 'Bearer ' + response.json()['token']}
        viewer = cls.client.post('/auth/login', json={'email': 'viewer@qiyuan.test', 'password': 'Demo2026!'})
        cls.viewer_headers = {'Authorization': 'Bearer ' + viewer.json()['token']}

    @classmethod
    def tearDownClass(cls):
        cls.client.__exit__(None, None, None)
        DB_PATH.unlink(missing_ok=True)

    def cache_set(self):
        sets = self.client.get('/request_sets', headers=self.headers).json()
        return next(x for x in sets if x['code'] == 'QY-1117-D')

    def drain_worker(self):
        while queue.items:
            ident = int(queue.items.pop(0))
            with Session() as db:
                obj = db.get(Trial, ident)
                obj.status = 'running'
                db.commit()
                snapshot, delay, sticky, ttl = obj.input_snapshot, obj.max_delay_ms, obj.sticky_ms, obj.cache_ttl_ms
            result = simulate(snapshot['nodes'], snapshot['request_set']['events'], delay, sticky, ttl)
            with Session() as db:
                obj = db.get(Trial, ident)
                obj.result, obj.status = result, 'completed'
                db.commit()

    def test_cache_trial_end_to_end(self):
        source = self.cache_set()
        self.assertEqual(len(source['events']), 16)
        response = self.client.post('/trials', headers=self.headers, json={'name': '共享读缓存复核', 'request_set_id': source['id'], 'max_delay_ms': 100, 'sticky_ms': 500, 'cache_ttl_ms': 1000})
        self.assertEqual(response.status_code, 201, response.text)
        trial = response.json()
        self.assertEqual(trial['status'], 'queued')
        self.assertEqual(trial['cache_ttl_ms'], 1000)
        self.assertEqual(trial['input_snapshot']['cache_ttl_ms'], 1000)
        self.drain_worker()
        detail = self.client.get(f"/trials/{trial['id']}", headers=self.headers).json()
        self.assertEqual(detail['status'], 'completed')
        result = detail['result']
        self.assertTrue(result['summary']['all_routed_reads_safe'])
        self.assertEqual(result['summary']['cache_hits'], 2)
        self.assertEqual(result['cache']['summary']['invalidations'], {'received': 4, 'applied': 2, 'duplicate': 1, 'stale': 1})
        statuses = [(row['at_ms'], row['cache']['status'], row['cache'].get('backfill')) for row in result['trace']]
        self.assertIn((200, 'hit', 'not_applicable'), statuses)
        self.assertIn((500, 'return', 'skipped'), statuses)
        self.assertIn((700, 'invalidated', 'not_applicable'), statuses)
        self.assertIn((800, 'duplicate', 'not_applicable'), statuses)
        self.assertIn((900, 'stale', 'not_applicable'), statuses)
        reasons = [row['cache']['reason'] for row in result['trace'] if row['cache']['status'] in ('hit', 'miss')]
        self.assertTrue(all(reason for reason in reasons))
        export = self.client.get(f"/trials/{trial['id']}/export", headers=self.headers)
        self.assertEqual(export.status_code, 200)
        exported = json.loads(export.content)
        self.assertEqual(exported['result']['checksum'], result['checksum'])
        self.assertIn('cache', exported['result'])
        self.assertEqual(exported['input_snapshot']['cache_ttl_ms'], 1000)

    def test_snapshot_is_frozen_when_inputs_change_later(self):
        source = self.cache_set()
        response = self.client.post('/trials', headers=self.headers, json={'name': '冻结快照复核', 'request_set_id': source['id'], 'cache_ttl_ms': 250})
        trial = response.json()
        self.assertEqual(trial['input_snapshot']['cache_ttl_ms'], 250)
        node_id = next(n['id'] for n in trial['input_snapshot']['nodes'] if n['name'] == 'south-replica')
        node = next(n for n in self.client.get('/nodes', headers=self.headers).json() if n['id'] == node_id)
        changed = dict(node, delay_ms=999)
        self.client.put(f'/nodes/{node_id}', headers=self.headers, json=changed)
        self.drain_worker()
        detail = self.client.get(f"/trials/{trial['id']}", headers=self.headers).json()
        frozen = [n for n in detail['input_snapshot']['nodes'] if n['name'] == 'south-replica'][0]
        self.assertEqual(frozen['delay_ms'], 20)
        self.assertEqual(detail['result']['policy']['cache_ttl_ms'], 250)
        self.client.put(f'/nodes/{node_id}', headers=self.headers, json=node)

    def test_legacy_trial_without_cache_section_stays_readable(self):
        with Session() as db:
            obj = Trial(name='缓存改造前演练', request_set_id=self.cache_set()['id'], max_delay_ms=100, sticky_ms=500, cache_ttl_ms=1000, status='completed', input_snapshot={'nodes': [], 'request_set': {'events': []}}, result={'mode': 'legacy', 'trace': [{'index': 1, 'at_ms': 0, 'kind': 'read', 'decision': 'routed', 'safe': True}], 'summary': {'requests': 1, 'routed': 1, 'rejected': 0, 'replica_reads': 1, 'primary_reads': 0, 'all_routed_reads_safe': True}, 'policy': {'max_delay_ms': 100, 'sticky_ms': 500}})
            db.add(obj)
            db.commit()
            ident = obj.id
        detail = self.client.get(f'/trials/{ident}', headers=self.headers)
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()['result']['mode'], 'legacy')
        self.assertNotIn('cache', detail.json()['result'])

    def test_invalid_timeline_rejected_before_queue(self):
        bad = [
            [{'at_ms': 0, 'kind': 'read_begin', 'session': 'a', 'key': 'k'}],
            [{'at_ms': 0, 'kind': 'invalidate', 'key': 'k', 'version': 5}],
            [{'at_ms': 10, 'kind': 'read'}, {'at_ms': 5, 'kind': 'read'}],
            [{'at_ms': 0, 'kind': 'read_return', 'session': 'a', 'key': 'k'}],
        ]
        for events in bad:
            response = self.client.post('/request_sets', headers=self.headers, json={'code': f'BAD-{len(events)}-{events[0]["at_ms"]}', 'title': '非法时序', 'events': events})
            if response.status_code != 201:
                continue
            trial = self.client.post('/trials', headers=self.headers, json={'name': '坏时序演练', 'request_set_id': response.json()['id']})
            self.assertEqual(trial.status_code, 422, (events, trial.text))
        # Schema-level rejection: invalidate without version, keys on a read.
        for events in ([{'at_ms': 0, 'kind': 'invalidate', 'key': 'k'}], [{'at_ms': 0, 'kind': 'read', 'keys': ['k']}]):
            response = self.client.post('/request_sets', headers=self.headers, json={'code': 'BAD-SCHEMA', 'title': '非法字段', 'events': events})
            self.assertEqual(response.status_code, 422)

    def test_viewer_cannot_run(self):
        source = self.cache_set()
        response = self.client.post('/trials', headers=self.viewer_headers, json={'name': '观察员尝试', 'request_set_id': source['id']})
        self.assertEqual(response.status_code, 403)


if __name__ == '__main__':
    unittest.main()
