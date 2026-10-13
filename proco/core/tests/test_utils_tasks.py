
import gc
import weakref
from unittest import mock

from django.core.cache import cache
from django.test import TestCase
from django.urls import reverse

from proco.background.models import BackgroundTask
from proco.core.config import app_config as core_configs
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests import factories as schools_test_models
from proco.utils import tasks as utils_tasks
from proco.utils.cache import cache_manager
from proco.utils.tests import TestAPIViewSetMixin


class UtilsTasksTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default',]

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school_one = schools_test_models.SchoolFactory(country=cls.country)
        cls.school_two = schools_test_models.SchoolFactory(country=cls.country)

        BackgroundTask.objects.all().delete()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_update_cached_value(self):
        self.assertIsNone(utils_tasks.update_cached_value(url=reverse('locations:countries-list')))

    def _countries_list_cache_entries(self):
        return [cache.get(key) for key in cache.keys('SOFT_CACHE_COUNTRIES_LIST_*')]

    def test_update_cached_value_keeps_url_query_string(self):
        utils_tasks.update_cached_value(url=reverse('locations:countries-list') + '?marker=one')

        keys = cache.keys('SOFT_CACHE_COUNTRIES_LIST_*')
        self.assertEqual(len(keys), 1)
        self.assertIn('marker', keys[0])
        self.assertNotIn('cache', keys[0])

    def test_update_cached_value_rebuilds_invalidated_entry(self):
        url = reverse('locations:countries-list') + '?marker=one'
        utils_tasks.update_cached_value(url=url)
        cache_manager.invalidate('COUNTRIES_LIST_*')
        self.assertTrue(all(entry['invalidated'] for entry in self._countries_list_cache_entries()))

        utils_tasks.update_cached_value(url=url)

        entries = self._countries_list_cache_entries()
        self.assertEqual(len(entries), 1)
        self.assertFalse(entries[0]['invalidated'])

    def test_call_get_view_does_not_leak_signal_finalizers(self):
        url = reverse('locations:countries-list')
        utils_tasks.call_get_view(url)

        gc.collect()
        before = len(weakref.finalize._registry)
        for _ in range(20):
            utils_tasks.call_get_view(url, {'cache': 'false'})
        gc.collect()

        self.assertEqual(len(weakref.finalize._registry), before)

    def test_call_get_view_returns_view_response(self):
        response = utils_tasks.call_get_view(reverse('locations:countries-list'), {'cache': 'false'})

        self.assertEqual(response.status_code, 200)
        self.assertIn(self.country.id, [row['id'] for row in response.data])

    def _stale_countries_list_url(self, marker='one'):
        url = '{0}?marker={1}'.format(reverse('locations:countries-list'), marker)
        utils_tasks.update_cached_value(url=url)
        cache_manager.invalidate('COUNTRIES_LIST_*')
        return url

    @mock.patch.object(utils_tasks.update_cached_value, 'delay')
    def test_stale_reads_queue_one_refresh_per_url(self, delay):
        url = self._stale_countries_list_url()

        for _ in range(3):
            utils_tasks.call_get_view(url)

        delay.assert_called_once_with(url=url)
        queued_ttl = cache.ttl(utils_tasks.get_refresh_queued_key(url))
        self.assertTrue(0 < queued_ttl <= core_configs.cache_refresh_queued_timeout)

    @mock.patch.object(utils_tasks.update_cached_value, 'delay')
    def test_stale_reads_of_different_urls_queue_their_own_refresh(self, delay):
        url_one = self._stale_countries_list_url(marker='one')
        url_two = self._stale_countries_list_url(marker='two')

        for url in (url_one, url_two, url_one, url_two):
            utils_tasks.call_get_view(url)

        self.assertEqual(delay.call_args_list, [mock.call(url=url_one), mock.call(url=url_two)])

    @mock.patch.object(utils_tasks.update_cached_value, 'delay')
    def test_stale_read_while_refresh_runs_queues_a_new_refresh(self, delay):
        url = self._stale_countries_list_url()
        utils_tasks.call_get_view(url)
        self.assertEqual(delay.call_count, 1)

        call_get_view = utils_tasks.call_get_view

        def read_stale_entry_during_rebuild(*args, **kwargs):
            # A user reads the entry after the refresh started and before it stored the new value
            call_get_view(url)
            return call_get_view(*args, **kwargs)

        with mock.patch.object(utils_tasks, 'call_get_view', side_effect=read_stale_entry_during_rebuild):
            utils_tasks.update_cached_value(url=url)

        self.assertEqual(delay.call_args_list, [mock.call(url=url), mock.call(url=url)])

    @mock.patch.object(utils_tasks.update_cached_value, 'delay')
    def test_stale_read_after_refresh_and_new_invalidation_queues_again(self, delay):
        url = self._stale_countries_list_url()
        utils_tasks.call_get_view(url)
        utils_tasks.update_cached_value(url=url)
        cache_manager.invalidate('COUNTRIES_LIST_*')

        utils_tasks.call_get_view(url)

        self.assertEqual(delay.call_count, 2)

    def test_update_all_cached_values(self):
        self.assertIsNone(utils_tasks.update_all_cached_values(clean_cache=True))

    def test_update_country_related_cache(self):
        self.assertIsNone(utils_tasks.update_country_related_cache(self.country.code))

    def test_rebuild_school_index(self):
        self.assertIsNone(utils_tasks.rebuild_school_index())

    def test_populate_school_registration_data(self):
        self.assertIsNone(utils_tasks.populate_school_registration_data())

    def test_redo_aggregations_task(self):
        self.assertIsNone(utils_tasks.redo_aggregations_task(self.country.id, 2025, 1))

    def test_populate_school_new_fields_task(self):
        self.assertIsNone(utils_tasks.populate_school_new_fields_task(1, 1000, self.country.id))
