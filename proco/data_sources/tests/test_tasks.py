from datetime import timedelta
from unittest.mock import patch

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

    def test_clean_old_live_data_purging(self):
        from django.utils import timezone
        from proco.schools.models import School
        from proco.connection_statistics.models import SchoolDailyStatus, SchoolWeeklyStatus, RealTimeConnectivity
        from proco.entities.models import EntityType, Entity
        from proco.entities.factories import EntityFactory

        school = SchoolFactory(country=self.country, deleted=timezone.now())
        SchoolDailyStatus.objects.create(school=school, date=timezone.now().date(), connectivity_speed=100)
        SchoolWeeklyStatus.objects.create(school=school, year=2026, week=1, connectivity_speed=100)
        RealTimeConnectivity.objects.create(school=school, connectivity_speed=100)

        entity_type, _ = EntityType.objects.get_or_create(
            code='health',
            name='Health Facility',
            master_data_model='data_sources.HealthEntityMasterIntermediateData',
            detail_model='entities.HealthEntity',
            detail_related_name='health_entity',
        )
        entity, health_entity = EntityFactory.create_entity(
            'health',
            country=self.country,
            name='Test Clinic',
            giga_id='test-clinic-id',
            deleted=timezone.now(),
            detail_kwargs={'facility_id_govt': 'govt-123'},
        )

        sources_tasks.clean_old_live_data()

        self.assertFalse(School.objects.all_records().filter(id=school.id).exists())
        self.assertFalse(Entity.objects.all_records().filter(id=entity.id).exists())

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

    def test_handle_published_entity_master_data_row(self):
        from proco.entities.models import EntityType, Entity
        entity_type, _ = EntityType.objects.get_or_create(code='health', name='Health Facility', master_data_model='data_sources.HealthEntityMasterIntermediateData', detail_model='entities.HealthEntity')

        row = sources_models.HealthEntityMasterIntermediateData.objects.create(
            country=self.country,
            health_id_giga='test-health-giga-id',
            facility_name='Test Health Facility',
            latitude=10.0,
            longitude=20.0,
            status=sources_models.HealthEntityMasterIntermediateData.ROW_STATUS_PUBLISHED,
            is_read=False,
            facility_hours='24_7',
        )

        self.assertEqual(sources_models.HealthEntityMasterIntermediateData.objects.filter(is_read=False).count(), 1)
        sources_tasks.handle_published_entity_master_data_row()
        self.assertEqual(sources_models.HealthEntityMasterIntermediateData.objects.filter(is_read=False).count(), 0)
        self.assertTrue(Entity.objects.filter(giga_id='test-health-giga-id').exists())


# The task keys include the clock hour: pin it so that every run in these tests happens in "the same hour"
@patch('proco.data_sources.tasks.format_date', return_value='26102026_10')
class SchoolMasterCountryPublishTasksTestCase(TestCase):
    """
    SchoolMasterCountryPublishTasksTestCase
        Country publish ("Publish all") passes published_at to the publish tasks, so that each request is handled
        by its own task run limited to its own rows, while the scheduled runs (no arguments) keep handling
        everything that is left.
    """
    databases = ['default']

    def setUp(self):
        cache.clear()
        BackgroundTask.objects.all().delete()
        self.country = CountryFactory()
        self.first_publish_at = get_current_datetime_object() - timedelta(minutes=20)
        self.second_publish_at = get_current_datetime_object() - timedelta(minutes=5)
        super().setUp()

    def _create_rows(self, row_status, published_at, count=1):
        return [
            sources_test_models.SchoolMasterDataFactory(
                country=self.country,
                status=row_status,
                is_read=False,
                published_at=published_at,
            )
            for _ in range(count)
        ]

    def _unread_ids(self, rows):
        return set(
            sources_models.SchoolMasterData.objects.filter(
                id__in=[row.id for row in rows],
                is_read=False,
            ).values_list('id', flat=True),
        )

    def test_published_rows_of_two_publishes_in_the_same_hour_are_both_handled(self, _):
        first_rows = self._create_rows(sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED, self.first_publish_at, 2)
        second_rows = self._create_rows(sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED, self.second_publish_at)

        sources_tasks.handle_published_school_master_data_row(
            country_ids=[self.country.id], publish_source='admin_portal_country',
            published_at=self.first_publish_at.isoformat())

        # Only the rows of the first publish are handled
        self.assertEqual(self._unread_ids(first_rows), set())
        self.assertEqual(self._unread_ids(second_rows), {row.id for row in second_rows})

        sources_tasks.handle_published_school_master_data_row(
            country_ids=[self.country.id], publish_source='admin_portal_country',
            published_at=self.second_publish_at.isoformat())

        # The second publish of the same country in the same hour is not skipped
        self.assertEqual(self._unread_ids(second_rows), set())
        self.assertEqual(BackgroundTask.objects.filter(
            name__startswith='handle_published_school_master_data_row_status_26102026_10_country_ids_',
        ).count(), 2)

    def test_deleted_rows_of_two_publishes_in_the_same_hour_are_both_handled(self, _):
        first_rows = self._create_rows(
            sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED, self.first_publish_at, 2)
        second_rows = self._create_rows(
            sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED, self.second_publish_at)

        sources_tasks.handle_deleted_school_master_data_row(
            country_ids=[self.country.id], publish_source='admin_portal_country',
            published_at=self.first_publish_at.isoformat())

        self.assertEqual(self._unread_ids(first_rows), set())
        self.assertEqual(self._unread_ids(second_rows), {row.id for row in second_rows})

        sources_tasks.handle_deleted_school_master_data_row(
            country_ids=[self.country.id], publish_source='admin_portal_country',
            published_at=self.second_publish_at.isoformat())

        self.assertEqual(self._unread_ids(second_rows), set())
        self.assertTrue(all(row.school.deleted is not None for row in sources_models.SchoolMasterData.objects.filter(
            id__in=[row.id for row in first_rows + second_rows]).select_related('school')))

    def test_country_runs_without_published_at_keep_the_hourly_dedup_and_scheduled_run_handles_leftovers(self, _):
        # CLI and data recovery commands call the tasks with country_ids only: unchanged behaviour
        first_rows = self._create_rows(sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED, self.first_publish_at)
        sources_tasks.handle_published_school_master_data_row(country_ids=[self.country.id])
        self.assertEqual(self._unread_ids(first_rows), set())

        second_rows = self._create_rows(sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED, self.second_publish_at)
        sources_tasks.handle_published_school_master_data_row(country_ids=[self.country.id])
        self.assertEqual(self._unread_ids(second_rows), {row.id for row in second_rows})

        # The scheduled run (no arguments) still picks up whatever was left
        sources_tasks.handle_published_school_master_data_row()
        self.assertEqual(self._unread_ids(second_rows), set())

    def test_scheduled_deleted_run_handles_rows_left_by_a_country_publish(self, _):
        # A country publish whose task never ran (worker down, task lost) is handled by the next scheduled run
        rows = self._create_rows(sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED, self.first_publish_at)

        sources_tasks.handle_deleted_school_master_data_row()

        self.assertEqual(self._unread_ids(rows), set())

    def test_task_keys(self, _):
        # Keys and descriptions without published_at are the ones the tasks used before country publish filters
        row = sources_test_models.SchoolMasterDataFactory(
            country=self.country,
            status=sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
        )
        self.assertEqual(sources_tasks.get_school_master_publish_task_details('published', country_ids=[7, 9]), (
            'handle_published_school_master_data_row_status_26102026_10_country_ids_7_9',
            'Handle published school master data rows for countries',
        ))
        self.assertEqual(sources_tasks.get_school_master_publish_task_details('deleted', row=row), (
            'handle_deleted_school_master_data_row_status_26102026_10_row_id_{0}'.format(row.id),
            'Handle deleted school master data row for single record',
        ))
        self.assertEqual(sources_tasks.get_school_master_publish_task_details('published'), (
            'handle_published_school_master_data_row_status_26102026_10',
            'Handle published school master data rows',
        ))
        self.assertEqual(
            sources_tasks.get_school_master_country_task_key(
                'handle_published_school_master_data_row', [7], published_at=self.first_publish_at),
            'handle_published_school_master_data_row_status_26102026_10_country_ids_7_at_{0}'.format(
                self.first_publish_at.strftime('%Y%m%d%H%M%S%f')),
        )

        many_country_ids = list(range(1000, 1100))
        long_key = sources_tasks.get_school_master_country_task_key(
            'handle_published_school_master_data_row', many_country_ids, published_at=self.first_publish_at)
        self.assertLessEqual(len(long_key), BackgroundTask._meta.get_field('name').max_length)
        self.assertEqual(long_key, sources_tasks.get_school_master_country_task_key(
            'handle_published_school_master_data_row', many_country_ids, published_at=self.first_publish_at))

    def test_parse_published_at(self, _):
        self.assertEqual(sources_tasks.parse_published_at(self.first_publish_at.isoformat()), self.first_publish_at)
        self.assertEqual(sources_tasks.parse_published_at(self.first_publish_at), self.first_publish_at)
        self.assertIsNone(sources_tasks.parse_published_at(None))
        self.assertIsNone(sources_tasks.parse_published_at('not a date'))
