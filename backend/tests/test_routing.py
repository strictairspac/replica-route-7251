import json
import unittest

from src.services.routing import simulate

NODES = [
    {'name': 'harbor-primary', 'role': 'primary', 'status': 'healthy', 'lineage': 'qiyuan-log-01', 'position': 105, 'delay_ms': 0},
    {'name': 'north-replica', 'role': 'replica', 'status': 'healthy', 'lineage': 'qiyuan-log-01', 'position': 104, 'delay_ms': 150},
    {'name': 'south-replica', 'role': 'replica', 'status': 'healthy', 'lineage': 'qiyuan-log-01', 'position': 105, 'delay_ms': 20},
]


def run(events, **policy):
    options = {'max_delay_ms': 100, 'sticky_ms': 500, 'cache_ttl_ms': 1000}
    options.update(policy)
    return simulate(NODES, events, options['max_delay_ms'], options['sticky_ms'], options['cache_ttl_ms'])


def cache_row(result, at_ms, kind=None):
    for row in result['trace']:
        if row['at_ms'] == at_ms and (kind is None or row['kind'] == kind):
            return row
    raise AssertionError(f'no trace row at {at_ms}')


class RoutingWithoutCacheTest(unittest.TestCase):
    def test_original_scenario_still_routes_and_rejects(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'session': 'lab-A'},
            {'at_ms': 100, 'kind': 'read', 'session': 'lab-A'},
            {'at_ms': 600, 'kind': 'replicate', 'node': 'south-replica', 'position': 106, 'delay_ms': 20},
            {'at_ms': 700, 'kind': 'read', 'session': 'lab-A'},
            {'at_ms': 800, 'kind': 'node_down', 'node': 'south-replica'},
            {'at_ms': 900, 'kind': 'read', 'session': 'lab-A'},
            {'at_ms': 1000, 'kind': 'node_down', 'node': 'harbor-primary'},
            {'at_ms': 1100, 'kind': 'read', 'session': 'lab-A'},
        ]
        result = run(events)
        rows = result['trace']
        self.assertEqual(rows[1]['node'], 'harbor-primary')       # sticky read
        self.assertEqual(rows[3]['node'], 'south-replica')        # caught up replica
        self.assertEqual(rows[5]['node'], 'harbor-primary')       # replica down
        self.assertEqual(rows[7]['decision'], 'rejected')         # primary down
        self.assertEqual(result['summary']['cache_hits'], 0)
        self.assertTrue(result['summary']['all_routed_reads_safe'])

    def test_keyless_read_bypasses_cache_and_keeps_checksum_deterministic(self):
        events = [{'at_ms': 0, 'kind': 'read', 'session': 'visitor'}]
        first = run(events)
        second = run(events)
        self.assertEqual(first['checksum'], second['checksum'])
        row = cache_row(first, 0)
        self.assertEqual(row['cache']['status'], 'bypass')
        self.assertEqual(row['node'], 'south-replica')


class CacheHitTest(unittest.TestCase):
    def test_fill_then_hit_then_expiry_boundary(self):
        events = [
            {'at_ms': 0, 'kind': 'read', 'session': 'a', 'key': 'order:1'},
            {'at_ms': 999, 'kind': 'read', 'session': 'b', 'key': 'order:1'},
            {'at_ms': 1000, 'kind': 'read', 'session': 'b', 'key': 'order:1'},
        ]
        result = run(events, cache_ttl_ms=1000)
        self.assertEqual(cache_row(result, 0)['cache']['status'], 'miss')
        self.assertEqual(cache_row(result, 0)['cache']['backfill'], 'filled')
        self.assertEqual(cache_row(result, 999)['cache']['status'], 'hit')
        self.assertEqual(cache_row(result, 999)['node'], 'cache')
        expired = cache_row(result, 1000)
        self.assertEqual(expired['cache']['status'], 'miss')
        self.assertIn('有效期', expired['cache']['reason'])
        self.assertEqual(result['cache']['summary']['expirations'], 1)

    def test_zero_ttl_is_never_a_hit(self):
        events = [{'at_ms': 0, 'kind': 'read', 'session': 'a', 'key': 'k'}]
        result = run(events, cache_ttl_ms=0)
        self.assertEqual(cache_row(result, 0)['cache']['status'], 'miss')

    def test_hit_requires_session_position_and_lineage(self):
        events = [
            {'at_ms': 0, 'kind': 'read', 'session': 'a', 'key': 'k'},
            {'at_ms': 50, 'kind': 'write', 'session': 'a'},
            {'at_ms': 100, 'kind': 'read', 'session': 'a', 'key': 'k'},
        ]
        result = run(events)
        self.assertEqual(cache_row(result, 100)['cache']['status'], 'miss')
        self.assertIn('低于会话所需位点', cache_row(result, 100)['cache']['reason'])
        self.assertEqual(cache_row(result, 100)['node'], 'harbor-primary')

    def test_hit_is_allowed_when_no_replica_is_eligible(self):
        down = [dict(node) for node in NODES]
        down[2]['status'] = 'down'
        down[1]['position'] = 10
        result = simulate(down, [
            {'at_ms': 0, 'kind': 'read', 'session': 'a', 'key': 'k'},
        ], 100, 500, 1000)
        self.assertEqual(cache_row(result, 0)['node'], 'harbor-primary')
        self.assertEqual(cache_row(result, 0)['cache']['status'], 'miss')


class MissFallbackTest(unittest.TestCase):
    def test_miss_keeps_safe_selection_and_rejection(self):
        events = [
            {'at_ms': 0, 'kind': 'read', 'session': 'a', 'key': 'k'},
            {'at_ms': 10, 'kind': 'node_down', 'node': 'south-replica'},
            {'at_ms': 20, 'kind': 'node_down', 'node': 'harbor-primary'},
            {'at_ms': 30, 'kind': 'read', 'session': 'b', 'key': 'fresh'},
        ]
        result = run(events)
        self.assertEqual(cache_row(result, 0)['node'], 'south-replica')
        rejected = cache_row(result, 30)
        self.assertEqual(rejected['decision'], 'rejected')
        self.assertEqual(rejected['cache']['status'], 'miss')
        self.assertEqual(rejected['cache']['backfill'], 'not_applicable')
        self.assertEqual(len(rejected['replica_checks']), 2)

    def test_replica_checks_report_all_four_conditions(self):
        result = run([{'at_ms': 0, 'kind': 'read', 'session': 'a', 'key': 'k'}])
        checks = cache_row(result, 0)['replica_checks']
        self.assertEqual([c['name'] for c in checks], ['north-replica', 'south-replica'])
        self.assertEqual(set(checks[1]), {'name', 'healthy', 'same_lineage', 'position_sufficient', 'delay_within_limit'})


class OldReadBackfillTest(unittest.TestCase):
    def test_read_return_after_write_does_not_refill(self):
        events = [
            {'at_ms': 0, 'kind': 'read_begin', 'session': 'a', 'key': 'order:1'},
            {'at_ms': 50, 'kind': 'write', 'session': 'b', 'keys': ['order:1']},
            {'at_ms': 100, 'kind': 'read_return', 'session': 'a', 'key': 'order:1'},
            {'at_ms': 200, 'kind': 'read', 'session': 'c', 'key': 'order:1'},
        ]
        result = run(events)
        returned = cache_row(result, 100)
        self.assertEqual(returned['cache']['backfill'], 'skipped')
        self.assertIn('版本', returned['cache']['backfill_reason'])
        later = cache_row(result, 200)
        self.assertEqual(later['cache']['status'], 'miss')
        self.assertEqual(result['cache']['summary']['skipped_backfills'], 1)

    def test_read_return_after_invalidation_does_not_refill(self):
        events = [
            {'at_ms': 0, 'kind': 'read_begin', 'session': 'a', 'key': 'k'},
            {'at_ms': 10, 'kind': 'write', 'session': 'b', 'keys': ['k']},
            {'at_ms': 20, 'kind': 'invalidate', 'key': 'k', 'version': 1},
            {'at_ms': 30, 'kind': 'read_return', 'session': 'a', 'key': 'k'},
        ]
        result = run(events)
        returned = cache_row(result, 30)
        self.assertEqual(returned['cache']['backfill'], 'skipped')
        self.assertTrue(any(c['action'] == 'skip_backfill' for c in result['cache']['changes']))

    def test_old_read_never_overwrites_newer_cache_entry(self):
        events = [
            {'at_ms': 0, 'kind': 'read_begin', 'session': 'a', 'key': 'k'},
            {'at_ms': 10, 'kind': 'write', 'session': 'b', 'keys': ['k']},
            {'at_ms': 20, 'kind': 'replicate', 'node': 'south-replica', 'position': 106, 'delay_ms': 20},
            {'at_ms': 30, 'kind': 'read', 'session': 'c', 'key': 'k'},
            {'at_ms': 40, 'kind': 'read_return', 'session': 'a', 'key': 'k'},
        ]
        result = run(events)
        newest = cache_row(result, 30)
        self.assertEqual(newest['cache']['status'], 'miss')
        self.assertEqual(newest['cache']['backfill'], 'filled')
        returned = cache_row(result, 40)
        self.assertEqual(returned['cache']['backfill'], 'skipped')
        self.assertIn('不回填', returned['cache']['backfill_reason'])
        entries = result['cache']['entries']
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]['position'], 106)
        self.assertEqual(entries[0]['version'], 1)

    def test_split_read_shares_version_at_begin(self):
        events = [
            {'at_ms': 0, 'kind': 'read_begin', 'session': 'a', 'key': 'k'},
            {'at_ms': 10, 'kind': 'read', 'session': 'b', 'key': 'k'},
            {'at_ms': 20, 'kind': 'read_return', 'session': 'a', 'key': 'k'},
            {'at_ms': 21, 'kind': 'read', 'session': 'b', 'key': 'k'},
        ]
        result = run(events)
        self.assertEqual(cache_row(result, 10)['cache']['status'], 'miss')
        self.assertEqual(cache_row(result, 20)['cache']['backfill'], 'filled')
        self.assertEqual(cache_row(result, 21)['cache']['status'], 'hit')

    def test_unpaired_read_begin_is_invalid(self):
        with self.assertRaisesRegex(ValueError, '未配对'):
            run([{'at_ms': 0, 'kind': 'read_begin', 'session': 'a', 'key': 'k'}])
        with self.assertRaisesRegex(ValueError, '没有对应的 read_begin'):
            run([{'at_ms': 0, 'kind': 'read_return', 'session': 'a', 'key': 'k'}])


class InvalidationTest(unittest.TestCase):
    def test_delayed_invalidation_keeps_returning_old_value_until_delivered(self):
        events = [
            {'at_ms': 0, 'kind': 'read', 'session': 'a', 'key': 'k'},
            {'at_ms': 10, 'kind': 'write', 'session': 'b', 'keys': ['k']},
            {'at_ms': 20, 'kind': 'read', 'session': 'c', 'key': 'k'},
            {'at_ms': 30, 'kind': 'invalidate', 'key': 'k', 'version': 1},
            {'at_ms': 40, 'kind': 'read', 'session': 'c', 'key': 'k'},
        ]
        result = run(events)
        self.assertEqual(cache_row(result, 20)['cache']['status'], 'hit')
        self.assertEqual(cache_row(result, 30)['cache']['entry_removed'] if 'entry_removed' in cache_row(result, 30)['cache'] else cache_row(result, 30)['cache'].get('entry_removed'), True)
        self.assertEqual(cache_row(result, 40)['cache']['status'], 'miss')
        self.assertEqual(result['cache']['summary']['pending_invalidations'], 0)

    def test_duplicate_delivery_is_idempotent(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'session': 'b', 'keys': ['k']},
            {'at_ms': 10, 'kind': 'invalidate', 'key': 'k', 'version': 1},
            {'at_ms': 20, 'kind': 'invalidate', 'key': 'k', 'version': 1},
        ]
        result = run(events)
        self.assertEqual(cache_row(result, 10)['decision'], 'applied')
        self.assertEqual(cache_row(result, 20)['decision'], 'ignored')
        key = result['cache']['keys'][0]
        self.assertEqual(key['invalidated_version'], 1)
        self.assertEqual(key['applied_versions'], [1])
        self.assertEqual(result['cache']['summary']['invalidations']['duplicate'], 1)

    def test_out_of_order_old_message_does_not_overwrite_new_state(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'session': 'b', 'keys': ['k']},
            {'at_ms': 10, 'kind': 'write', 'session': 'b', 'keys': ['k']},
            {'at_ms': 20, 'kind': 'invalidate', 'key': 'k', 'version': 2},
            {'at_ms': 30, 'kind': 'invalidate', 'key': 'k', 'version': 1},
            {'at_ms': 40, 'kind': 'read', 'session': 'c', 'key': 'k'},
        ]
        result = run(events)
        self.assertEqual(cache_row(result, 20)['decision'], 'applied')
        self.assertEqual(cache_row(result, 30)['decision'], 'ignored')
        key = result['cache']['keys'][0]
        self.assertEqual(key['invalidated_version'], 2)
        self.assertEqual(key['invalidated_position'], 107)
        self.assertEqual(result['cache']['summary']['invalidations']['stale'], 1)

    def test_unknown_message_version_is_rejected_before_running(self):
        with self.assertRaisesRegex(ValueError, '失效消息不存在'):
            run([{'at_ms': 0, 'kind': 'invalidate', 'key': 'k', 'version': 9}])

    def test_invalidation_evidence_lists_deliveries(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'session': 'b', 'keys': ['k']},
            {'at_ms': 100, 'kind': 'invalidate', 'key': 'k', 'version': 1},
            {'at_ms': 150, 'kind': 'invalidate', 'key': 'k', 'version': 1},
        ]
        result = run(events)
        message = result['cache']['messages'][0]
        self.assertEqual(message['message_id'], 'k@v1')
        self.assertEqual([(d['at_ms'], d['result']) for d in message['deliveries']], [(100, 'applied'), (150, 'duplicate')])


class CacheChangeEvidenceTest(unittest.TestCase):
    def test_every_cache_transition_has_a_reason(self):
        events = [
            {'at_ms': 0, 'kind': 'read', 'session': 'a', 'key': 'k'},
            {'at_ms': 999, 'kind': 'read', 'session': 'a', 'key': 'k'},
            {'at_ms': 1000, 'kind': 'read', 'session': 'a', 'key': 'k'},
            {'at_ms': 1001, 'kind': 'write', 'session': 'b', 'keys': ['k']},
            {'at_ms': 1002, 'kind': 'invalidate', 'key': 'k', 'version': 1},
        ]
        result = run(events)
        self.assertGreaterEqual(len(result['cache']['changes']), 4)
        for change in result['cache']['changes']:
            self.assertTrue(change['reason'])
            self.assertIn(change['action'], {'fill', 'expire', 'version_advanced', 'invalidate', 'invalidate_recorded', 'skip_fill', 'skip_backfill'})
        # Frozen input and deterministic output
        self.assertEqual(result['checksum'], run(events)['checksum'])
        self.assertEqual(result['policy']['cache_ttl_ms'], 1000)
        self.assertIn('cache_expiry_boundary', result['policy'])

    def test_summary_counts_cache_and_read_units(self):
        events = [
            {'at_ms': 0, 'kind': 'read', 'session': 'a', 'key': 'k'},
            {'at_ms': 10, 'kind': 'read', 'session': 'a', 'key': 'k'},
            {'at_ms': 20, 'kind': 'read_begin', 'session': 'a', 'key': 'j'},
            {'at_ms': 30, 'kind': 'read_return', 'session': 'a', 'key': 'j'},
        ]
        result = run(events)
        self.assertEqual(result['summary']['cache_hits'], 1)
        self.assertEqual(result['summary']['requests'], 3)
        self.assertEqual(result['summary']['read_returns'], 1)


class ValidationTest(unittest.TestCase):
    def test_invalid_cache_policy(self):
        with self.assertRaisesRegex(ValueError, '缓存有效期'):
            run([{'at_ms': 0, 'kind': 'read', 'session': 'a'}], cache_ttl_ms=-1)

    def test_write_keys_must_be_string_array(self):
        with self.assertRaisesRegex(ValueError, 'keys'):
            run([{'at_ms': 0, 'kind': 'write', 'session': 'a', 'keys': 'k'}])
        with self.assertRaisesRegex(ValueError, 'keys'):
            run([{'at_ms': 0, 'kind': 'write', 'session': 'a', 'keys': ['']}])
        with self.assertRaisesRegex(ValueError, '20'):
            run([{'at_ms': 0, 'kind': 'write', 'session': 'a', 'keys': [f'k{i}' for i in range(21)]}])

    def test_key_on_non_read_events_is_rejected(self):
        with self.assertRaisesRegex(ValueError, '只有读取和失效事件'):
            run([{'at_ms': 0, 'kind': 'write', 'session': 'a', 'key': 'k'}])

    def test_event_time_must_be_monotonic_integer(self):
        with self.assertRaisesRegex(ValueError, '非递减'):
            run([{'at_ms': 10, 'kind': 'read'}, {'at_ms': 5, 'kind': 'read'}])
        with self.assertRaisesRegex(ValueError, 'at_ms'):
            run([{'at_ms': '0', 'kind': 'read'}])


if __name__ == '__main__':
    unittest.main()
