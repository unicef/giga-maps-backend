
from django.core.cache import cache
from django.test import TestCase
from django.urls import reverse

from proco.background.models import BackgroundTask
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests import factories as schools_test_models
from proco.utils import tasks as utils_tasks
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
