from django.core.cache import cache
from django.test import TestCase

from proco.background.models import BackgroundTask
from proco.core.utils import get_current_datetime_object
from proco.data_sources import models as sources_models
from proco.data_sources import tasks as sources_tasks
from proco.data_sources.tests import factories as sources_test_models
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory
from proco.utils.tests import TestAPIViewSetMixin


class DataSourcesTasksTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school_one = SchoolFactory(country=cls.country)
        cls.school_two = SchoolFactory(country=cls.country)

        # School details updated
        cls.row_one = sources_test_models.SchoolMasterDataFactory(
            country=cls.country,
            school_id_giga=cls.school_one.giga_id_school,
            status=sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED,
            is_read=False,
        )

        # New school
        cls.row_two = sources_test_models.SchoolMasterDataFactory(
            country=cls.country,
            status=sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED,
            is_read=False,
        )

        # delete 2nd school
        cls.row_two = sources_test_models.SchoolMasterDataFactory(
            country=cls.country,
            school_id_giga=cls.school_two.giga_id_school,
            status=sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
            is_read=False,
        )

        BackgroundTask.objects.all().delete()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_finalize_task(self):
        self.assertEqual(sources_tasks.finalize_task(), 'Done')

    def test_load_data_from_school_master_apis(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        sources_tasks.load_data_from_school_master_apis(country_iso3_format=self.country.iso3_format)
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)

    def test_handle_published_school_master_data_row(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        sources_tasks.handle_published_school_master_data_row()
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 1)

    def test_handle_published_school_master_data_row_for_country(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        sources_tasks.handle_published_school_master_data_row(country_ids=[self.country.id])
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 1)

    def test_handle_deleted_school_master_data_row(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        sources_tasks.handle_deleted_school_master_data_row()
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 2)

    def test_giga_meter_handle_deleted_school_master_data_row_for_country(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        sources_tasks.handle_deleted_school_master_data_row(country_ids=[self.country.id])
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 2)

    def test_email_reminder_to_editor_and_publisher_for_review_waiting_records(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        sources_tasks.email_reminder_to_editor_and_publisher_for_review_waiting_records()
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)

    def test_load_data_from_daily_check_app_api(self):
        self.assertEqual(sources_models.DailyCheckAppMeasurementData.objects.all().count(), 0)
        sources_tasks.load_data_from_daily_check_app_api()
        self.assertEqual(sources_models.DailyCheckAppMeasurementData.objects.all().count(), 0)

    def test_load_data_from_qos_apis(self):
        self.assertEqual(sources_models.QoSData.objects.all().count(), 0)
        sources_tasks.load_data_from_qos_apis()
        self.assertEqual(sources_models.QoSData.objects.all().count(), 0)

    def test_cleanup_school_master_rows(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        sources_tasks.cleanup_school_master_rows()
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)

    def test_clean_old_live_data(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        self.assertEqual(sources_models.DailyCheckAppMeasurementData.objects.all().count(), 0)
        self.assertEqual(sources_models.QoSData.objects.all().count(), 0)
        sources_tasks.clean_old_live_data()
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        self.assertEqual(sources_models.DailyCheckAppMeasurementData.objects.all().count(), 0)
        self.assertEqual(sources_models.QoSData.objects.all().count(), 0)

    def test_clean_historic_data(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        self.assertEqual(sources_models.SchoolMasterData.history.model.objects.all().count(), 3)
        sources_tasks.clean_historic_data()
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        self.assertEqual(sources_models.SchoolMasterData.history.model.objects.all().count(), 0)

    def test_update_static_data(self):
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)
        sources_tasks.update_static_data(country_iso3_format=self.country.iso3_format)
        self.assertEqual(sources_models.SchoolMasterData.objects.all().count(), 3)

    def test_finalize_previous_day_data(self):
        self.assertEqual(sources_tasks.finalize_previous_day_data(
            None,
            self.country.id,
            get_current_datetime_object().date()
        ),
            None)

    def test_update_live_data(self):
        self.assertEqual(sources_tasks.update_live_data(), None)

    def test_data_loss_recovery_for_pcdc_weekly_task(self):
        self.assertEqual(sources_tasks.data_loss_recovery_for_pcdc_weekly_task(
            1, 2, 2025, True
        ), None)

    def test_scheduler_for_data_loss_recovery_for_qos_dates(self):
        self.assertEqual(sources_tasks.scheduler_for_data_loss_recovery_for_qos_dates(
            self.country.iso3_format, '01-01-2025', '07-01-2025', True, True, True
        ), None)
