import uuid
from datetime import timedelta
from unittest import mock
from urllib.parse import parse_qs, urlsplit

from django.conf import settings
from django.test import SimpleTestCase

from celery import Celery
from redbeat.schedulers import RedBeatSchedulerEntry, get_redis

from proco.taskapp import REDBEAT_REDIS_URL_OPTIONS, add_redis_url_options
from proco.taskapp.schedulers import SelfHealingRedBeatScheduler


class AddRedisUrlOptionsTestCase(SimpleTestCase):

    def test_adds_options_to_redis_url(self):
        url = add_redis_url_options('redis://redis:6379/1', REDBEAT_REDIS_URL_OPTIONS)

        parts = urlsplit(url)
        self.assertEqual(parts.netloc, 'redis:6379')
        self.assertEqual(parts.path, '/1')
        self.assertEqual(parse_qs(parts.query), {name: [value] for name, value in REDBEAT_REDIS_URL_OPTIONS.items()})

    def test_keeps_credentials_and_options_already_in_url(self):
        url = add_redis_url_options(
            'rediss://:se%2Fcret@host:6380/4?ssl_cert_reqs=required&socket_timeout=5',
            REDBEAT_REDIS_URL_OPTIONS,
        )

        parts = urlsplit(url)
        query = parse_qs(parts.query)
        self.assertEqual(parts.netloc, ':se%2Fcret@host:6380')
        self.assertEqual(query['ssl_cert_reqs'], ['required'])
        self.assertEqual(query['socket_timeout'], ['5'])
        self.assertEqual(query['health_check_interval'], ['30'])

    def test_leaves_other_urls_unchanged(self):
        self.assertEqual(add_redis_url_options('memory://', REDBEAT_REDIS_URL_OPTIONS), 'memory://')
        self.assertIsNone(add_redis_url_options(None, REDBEAT_REDIS_URL_OPTIONS))


class SelfHealingRedBeatSchedulerTestCase(SimpleTestCase):
    """
    Runs against the Redis of the test settings' cache, under a key prefix of its own that is deleted afterwards
    """

    def setUp(self):
        self.prefix = 'gigamaps-test-{0}:'.format(uuid.uuid4().hex)
        self.app = Celery('proco-test', set_as_current=False)
        self.app.conf.update(
            timezone='UTC',
            broker_url='memory://',
            redbeat_redis_url=settings.CACHES['default']['LOCATION'],
            redbeat_key_prefix=self.prefix,
            beat_schedule={
                'hourly': {'task': 'proco.tests.hourly', 'schedule': 3600},
                'daily': {'task': 'proco.tests.daily', 'schedule': 86400},
            },
        )
        self.scheduler = SelfHealingRedBeatScheduler(app=self.app)
        self.conf = self.app.redbeat_conf
        self.redis = get_redis(self.app)
        self.addCleanup(self.delete_test_keys)

    def delete_test_keys(self):
        keys = list(self.redis.scan_iter(match=self.prefix + '*'))
        if keys:
            self.redis.delete(*keys)

    def entry_key(self, name):
        return self.conf.key_prefix + name

    def test_nothing_to_restore_after_startup(self):
        self.assertEqual(self.redis.zcard(self.conf.schedule_key), 2)
        self.assertEqual(self.scheduler.restore_missing_static_entries(), [])

    def test_restores_all_entries_when_redis_lost_them(self):
        self.redis.delete(
            self.conf.schedule_key, self.conf.statics_key, self.entry_key('hourly'), self.entry_key('daily'),
        )

        restored = self.scheduler.restore_missing_static_entries()

        self.assertEqual(sorted(restored), ['daily', 'hourly'])
        self.assertEqual(self.redis.zcard(self.conf.schedule_key), 2)
        self.assertEqual(self.redis.smembers(self.conf.statics_key), {'hourly', 'daily'})
        for name in ('hourly', 'daily'):
            self.assertTrue(self.redis.hexists(self.entry_key(name), 'definition'))

    def test_restores_only_the_evicted_entry(self):
        daily_score = self.redis.zscore(self.conf.schedule_key, self.entry_key('daily'))
        self.redis.delete(self.entry_key('hourly'))

        self.assertEqual(self.scheduler.restore_missing_static_entries(), ['hourly'])
        self.assertEqual(self.redis.zscore(self.conf.schedule_key, self.entry_key('daily')), daily_score)

    def test_entry_missing_from_schedule_keeps_last_run_at_and_is_due_now(self):
        last_run_at = self.app.now() - timedelta(hours=2)
        RedBeatSchedulerEntry.from_key(self.entry_key('hourly'), app=self.app).reschedule(last_run_at=last_run_at)
        self.redis.zrem(self.conf.schedule_key, self.entry_key('hourly'))

        self.assertEqual(self.scheduler.restore_missing_static_entries(), ['hourly'])

        meta = RedBeatSchedulerEntry.load_meta(self.entry_key('hourly'), app=self.app)
        self.assertEqual(meta['last_run_at'], last_run_at)
        score = self.redis.zscore(self.conf.schedule_key, self.entry_key('hourly'))
        self.assertLessEqual(score, self.app.now().timestamp())

    def test_tick_restores_lost_schedule_and_sends_the_missed_run(self):
        last_run_at = self.app.now() - timedelta(hours=2)
        RedBeatSchedulerEntry.from_key(self.entry_key('hourly'), app=self.app).reschedule(last_run_at=last_run_at)
        self.redis.delete(self.conf.schedule_key)
        self.scheduler.lock = mock.Mock()

        with mock.patch.object(self.scheduler, 'apply_async') as apply_async:
            self.scheduler.tick()

        self.scheduler.lock.extend.assert_called_once_with(self.scheduler.lock_timeout)
        self.assertEqual([call.args[0].name for call in apply_async.call_args_list], ['hourly'])
        self.assertEqual(self.redis.zcard(self.conf.schedule_key), 2)
