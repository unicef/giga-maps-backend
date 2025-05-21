from django.core.cache import cache
from django.test import TestCase

from proco.background.models import BackgroundTask
from proco.giga_meter import models as giga_meter_models
from proco.giga_meter import tasks as giga_meter_tasks
from proco.giga_meter.tests import factories as giga_meter_test_models
from proco.utils.tests import TestAPIViewSetMixin


class GigaMeterTasksTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', 'gigameter_database',]

    @classmethod
    def setUpTestData(cls):
        cls.country = giga_meter_test_models.GigaMeter_CountryFactory()

        cls.school_one = giga_meter_test_models.GigaMeter_SchoolFactory(country=cls.country)
        cls.school_two = giga_meter_test_models.GigaMeter_SchoolFactory(country=cls.country)

        # School details updated
        cls.row_one = giga_meter_test_models.GigaMeter_SchoolMasterDataFactory(
            country=cls.country,
            school_id_giga=cls.school_one.giga_id_school,
            status=giga_meter_models.GigaMeter_SchoolMasterData.ROW_STATUS_PUBLISHED,
        )

        # New school
        cls.row_two = giga_meter_test_models.GigaMeter_SchoolMasterDataFactory(
            country=cls.country,
            status=giga_meter_models.GigaMeter_SchoolMasterData.ROW_STATUS_PUBLISHED,
        )

        # delete 2nd school
        cls.row_two = giga_meter_test_models.GigaMeter_SchoolMasterDataFactory(
            country=cls.country,
            school_id_giga=cls.school_two.giga_id_school,
            status=giga_meter_models.GigaMeter_SchoolMasterData.ROW_STATUS_DELETED,
        )

        BackgroundTask.objects.all().delete()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_handle_giga_meter_school_master_data_sync(self):
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 3)
        giga_meter_tasks.handle_giga_meter_school_master_data_sync()
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 3)

    def test_giga_meter_update_static_data(self):
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 3)
        giga_meter_tasks.giga_meter_update_static_data(country_iso3_format=self.country.iso3_format)
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 3)

    def test_giga_meter_handle_published_school_master_data_row(self):
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 3)
        giga_meter_tasks.giga_meter_handle_published_school_master_data_row(force_tasks=True)
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 1)

    def test_giga_meter_handle_published_school_master_data_row_for_country(self):
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 3)
        giga_meter_tasks.giga_meter_handle_published_school_master_data_row(country_ids=[self.country.id], force_tasks=True)
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 1)

    def test_giga_meter_handle_deleted_school_master_data_row(self):
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 3)
        giga_meter_tasks.giga_meter_handle_deleted_school_master_data_row(force_tasks=True)
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 2)

    def test_giga_meter_handle_deleted_school_master_data_row_for_country(self):
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 3)
        giga_meter_tasks.giga_meter_handle_deleted_school_master_data_row(country_ids=[self.country.id], force_tasks=True)
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 2)
