from django.core.cache import cache
from django.test import TestCase

from proco.background.models import BackgroundTask
from proco.locations.tests.factories import CountryFactory
from proco.schools import models as schools_models
from proco.schools import tasks as schools_tasks
from proco.schools.tests import factories as schools_test_models
from proco.utils.tests import TestAPIViewSetMixin


class SchoolsTasksTestCase(TestAPIViewSetMixin, TestCase):
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

    def test_update_school_records(self):
        self.assertEqual(schools_models.School.objects.all().count(), 2)
        schools_tasks.update_school_records()
        self.assertEqual(schools_models.School.objects.all().count(), 2)
