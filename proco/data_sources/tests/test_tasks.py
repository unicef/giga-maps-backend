from datetime import timedelta
from unittest import mock
from unittest.mock import patch

from django.core.cache import cache
from django.test import TestCase

import requests
from celery.exceptions import SoftTimeLimitExceeded

from proco.background.models import BackgroundTask
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.tests.factories import CountryDailyStatusFactory
from proco.core.utils import get_current_datetime_object
from proco.data_sources import models as sources_models
from proco.data_sources import tasks as sources_tasks
from proco.data_sources import utils as source_utilities
from proco.data_sources.tests import factories as sources_test_models
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory
from proco.taskapp import app as celery_app
from proco.taskapp import finalize_setup
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

    def assert_school_master_task_stopped(self, task_name_prefix, log_message):
        """The task stopped by the soft time limit is closed, with the reason in its log"""
        self.assertFalse(BackgroundTask.objects.filter(status=BackgroundTask.STATUSES.running).exists())
        task = BackgroundTask.objects.get(name__startswith=task_name_prefix)
        self.assertEqual(task.status, BackgroundTask.STATUSES.completed)
        self.assertIn(log_message, task.log)

    def test_school_master_tasks_hard_time_limit_is_later_than_soft(self):
        # With equal limits the worker is killed right after the soft signal, before any stop handler runs
        for task in (
            sources_tasks.handle_published_school_master_data_row,
            sources_tasks.handle_deleted_school_master_data_row,
        ):
            self.assertGreater(task.time_limit, task.soft_time_limit, task.name)

    @mock.patch('proco.data_sources.tasks.populate_school_new_fields_task.delay')
    @mock.patch('proco.data_sources.tasks.send_slack_notifications')
    @mock.patch('proco.data_sources.tasks.compare_target_model_changes', side_effect=SoftTimeLimitExceeded())
    def test_handle_published_school_master_data_row_stops_on_soft_time_limit(
        self, mock_compare, mock_slack, mock_populate,
    ):
        with self.assertRaises(SoftTimeLimitExceeded):
            sources_tasks.handle_published_school_master_data_row()

        # The task must stop at the first row instead of moving on to the next one
        self.assertEqual(mock_compare.call_count, 1)
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 3)
        mock_populate.assert_not_called()
        mock_slack.assert_called_once()
        self.assert_school_master_task_stopped(
            'handle_published_school_master_data_row_status_',
            'Stopped on soft time limit after 0 published records.',
        )

    @mock.patch('proco.data_sources.tasks.populate_school_new_fields_task.delay')
    @mock.patch('proco.data_sources.tasks.send_slack_notifications')
    @mock.patch('proco.data_sources.tasks.compare_target_model_changes', side_effect=[
        {'school_model': {'school_name': 1}, 'school_weekly': {}, 'rt_registration': {}},
        SoftTimeLimitExceeded(),
    ])
    def test_handle_published_school_master_data_row_soft_time_limit_after_first_row(
        self, mock_compare, mock_slack, mock_populate,
    ):
        with self.assertRaises(SoftTimeLimitExceeded):
            sources_tasks.handle_published_school_master_data_row()

        self.assertEqual(mock_compare.call_count, 2)
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 2)

        # The first row was published, so its school must still be sent to populate
        published_school_id = sources_models.SchoolMasterData.objects.get(pk=self.row_one.pk).school_id
        mock_populate.assert_called_once_with(None, None, None, school_ids=[published_school_id])

        # The Slack summary counts only the row that was saved, not the interrupted one
        mock_slack.assert_called_once()
        country_summary = mock_slack.call_args[0][0][self.country.iso3_format]
        self.assertEqual(country_summary['updated_schools'], 1)
        self.assertEqual(country_summary['new_schools'], 0)
        self.assertEqual(dict(country_summary['school_model_changes']), {'school_name': 1})

        self.assert_school_master_task_stopped(
            'handle_published_school_master_data_row_status_',
            'Stopped on soft time limit after 1 published records.',
        )

    def patch_save_then_soft_time_limit(self):
        """
        Make SchoolMasterData.save() store the row and then raise SoftTimeLimitExceeded, as when the limit
        hits while save() waits on the DB (seen with a real worker)
        """
        real_save = sources_models.SchoolMasterData.save

        def save_then_soft_time_limit(instance, *args, **kwargs):
            real_save(instance, *args, **kwargs)
            raise SoftTimeLimitExceeded()

        return mock.patch.object(
            sources_models.SchoolMasterData, 'save', autospec=True, side_effect=save_then_soft_time_limit,
        )

    @mock.patch('proco.data_sources.tasks.populate_school_new_fields_task.delay')
    @mock.patch('proco.data_sources.tasks.send_slack_notifications')
    @mock.patch('proco.data_sources.tasks.compare_target_model_changes', return_value={
        'school_model': {'school_name': 1}, 'school_weekly': {}, 'rt_registration': {},
    })
    def test_handle_published_school_master_data_row_soft_time_limit_inside_row_save(
        self, mock_compare, mock_slack, mock_populate,
    ):
        with self.patch_save_then_soft_time_limit(), self.assertRaises(SoftTimeLimitExceeded):
            sources_tasks.handle_published_school_master_data_row()

        # The row was stored as read before the limit hit, so it must be reported and populated now
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 2)
        published_school_id = sources_models.SchoolMasterData.objects.get(pk=self.row_one.pk).school_id
        mock_populate.assert_called_once_with(None, None, None, school_ids=[published_school_id])
        self.assertEqual(mock_slack.call_args[0][0][self.country.iso3_format]['updated_schools'], 1)
        self.assert_school_master_task_stopped(
            'handle_published_school_master_data_row_status_',
            'Stopped on soft time limit after 1 published records.',
        )

    @mock.patch('proco.data_sources.tasks.populate_school_new_fields_task.delay')
    @mock.patch('proco.data_sources.tasks.send_slack_notifications', side_effect=requests.HTTPError('500'))
    @mock.patch('proco.data_sources.tasks.compare_target_model_changes', side_effect=SoftTimeLimitExceeded())
    def test_handle_published_school_master_data_row_soft_time_limit_with_slack_error(
        self, mock_compare, mock_slack, mock_populate,
    ):
        # A Slack error while stopping must not replace the soft time limit as the reason the task stopped
        with self.assertRaises(SoftTimeLimitExceeded):
            sources_tasks.handle_published_school_master_data_row()

        mock_slack.assert_called_once()
        self.assert_school_master_task_stopped(
            'handle_published_school_master_data_row_status_',
            'Stopped on soft time limit after 0 published records.',
        )

    def create_second_deleted_row(self):
        """A second deleted row, so stopping before or after the first one is observable"""
        return sources_test_models.SchoolMasterDataFactory(
            country=self.country,
            school=SchoolFactory(country=self.country),
            status=sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
            is_read=False,
        )

    @mock.patch('proco.data_sources.tasks.send_slack_notifications')
    @mock.patch('proco.schools.models.School.delete', side_effect=SoftTimeLimitExceeded())
    def test_handle_deleted_school_master_data_row_stops_on_soft_time_limit(self, mock_delete, mock_slack):
        self.create_second_deleted_row()

        with self.assertRaises(SoftTimeLimitExceeded):
            sources_tasks.handle_deleted_school_master_data_row()

        # The task must stop at the first row instead of moving on to the next one
        self.assertEqual(mock_delete.call_count, 1)
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 4)
        mock_slack.assert_called_once()
        self.assertEqual(mock_slack.call_args[0][0][self.country.iso3_format]['deleted_schools'], 0)
        self.assert_school_master_task_stopped(
            'handle_deleted_school_master_data_row_status_',
            'Stopped on soft time limit. Remaining records: 2',
        )

    @mock.patch('proco.data_sources.tasks.send_slack_notifications')
    @mock.patch('proco.schools.models.School.delete', side_effect=[None, SoftTimeLimitExceeded()])
    def test_handle_deleted_school_master_data_row_soft_time_limit_after_first_row(self, mock_delete, mock_slack):
        self.create_second_deleted_row()

        with self.assertRaises(SoftTimeLimitExceeded):
            sources_tasks.handle_deleted_school_master_data_row()

        self.assertEqual(mock_delete.call_count, 2)
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 3)

        # The school deleted before the stop must still be reported, no later run will do it
        mock_slack.assert_called_once()
        self.assertEqual(mock_slack.call_args[0][0][self.country.iso3_format]['deleted_schools'], 1)
        self.assert_school_master_task_stopped(
            'handle_deleted_school_master_data_row_status_',
            'Stopped on soft time limit. Remaining records: 1',
        )

    @mock.patch('proco.data_sources.tasks.send_slack_notifications')
    def test_handle_deleted_school_master_data_row_soft_time_limit_inside_row_save(self, mock_slack):
        self.create_second_deleted_row()

        with self.patch_save_then_soft_time_limit(), self.assertRaises(SoftTimeLimitExceeded):
            sources_tasks.handle_deleted_school_master_data_row()

        # The row was stored as read before the limit hit, so its school must be reported now
        self.assertEqual(sources_models.SchoolMasterData.objects.filter(is_read=False).count(), 3)
        self.assertEqual(mock_slack.call_args[0][0][self.country.iso3_format]['deleted_schools'], 1)
        self.assert_school_master_task_stopped(
            'handle_deleted_school_master_data_row_status_',
            'Stopped on soft time limit. Remaining records: 1',
        )

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


class CheckQoSDataFreshnessTaskTestCase(TestCase):
    databases = ['default']

    logger_name = 'gigamaps.proco.data_sources.tasks'

    @classmethod
    def setUpTestData(cls):
        cls.now = get_current_datetime_object()
        cls.today = cls.now.date()

        cls.fresh_country = cls.create_qos_country(
            'BRA', cls.now - timedelta(hours=2), cls.today - timedelta(days=1))
        cls.no_new_version_country = cls.create_qos_country(
            'KEN', cls.now - timedelta(days=3), cls.today - timedelta(days=1))
        cls.not_aggregated_country = cls.create_qos_country(
            'MNG', cls.now - timedelta(hours=2), cls.today - timedelta(days=5))
        cls.never_aggregated_country = cls.create_qos_country('ZAF', cls.now - timedelta(hours=2), None)
        cls.inactive_country = cls.create_qos_country('UZB', cls.now - timedelta(days=40), None)

    @classmethod
    def create_qos_country(cls, iso3_format, pulled_at, last_aggregated_date):
        country = CountryFactory(iso3_format=iso3_format)
        school = SchoolFactory(country=country)
        sources_models.QoSData.objects.create(
            school=school,
            country=country,
            timestamp=pulled_at,
            date=pulled_at.date(),
            school_id_giga=school.giga_id_school,
            version=1,
            pulled_at=pulled_at,
        )

        if last_aggregated_date:
            CountryDailyStatusFactory(
                country=country,
                date=last_aggregated_date,
                live_data_source=statistics_configs.QOS_SOURCE,
            )
        # Data from another live source must not count as aggregated QoS data
        CountryDailyStatusFactory(
            country=country,
            date=cls.today,
            live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
        )
        return country

    def setUp(self):
        cache.clear()
        super().setUp()

    def get_error_messages(self, logs):
        return [record.getMessage() for record in logs.records if record.levelname == 'ERROR']

    def test_reports_each_stale_country_once(self):
        with self.assertLogs(self.logger_name, level='INFO') as logs:
            sources_tasks.check_qos_data_freshness()

        error_messages = self.get_error_messages(logs)
        self.assertEqual(len(error_messages), 3)
        self.assertTrue(error_messages[0].startswith('QoS data is stale for KEN: '))
        self.assertTrue(error_messages[1].startswith('QoS data is stale for MNG: '))
        self.assertTrue(error_messages[2].startswith('QoS data is stale for ZAF: '))
        self.assertFalse(any('BRA' in message or 'UZB' in message for message in error_messages))

        task = BackgroundTask.objects.get(name__startswith='check_qos_data_freshness_status_')
        self.assertEqual(task.status, BackgroundTask.STATUSES.completed)
        self.assertIn('QoS data is stale for KEN', task.log)

    def test_message_names_last_version_and_last_aggregated_day(self):
        with self.assertLogs(self.logger_name, level='INFO') as logs:
            sources_tasks.check_qos_data_freshness()

        error_messages = self.get_error_messages(logs)
        self.assertEqual(
            error_messages[0],
            'QoS data is stale for KEN: last new upstream version pulled at {0} UTC, '
            'last aggregated QoS day {1}.'.format(
                (self.now - timedelta(days=3)).strftime('%Y-%m-%d %H:%M'),
                (self.today - timedelta(days=1)).strftime('%Y-%m-%d'),
            ),
        )
        self.assertTrue(error_messages[2].endswith('last aggregated QoS day none.'))

    def test_message_avoids_patterns_dropped_before_sending_to_sentry(self):
        with self.assertLogs(self.logger_name, level='INFO') as logs:
            sources_tasks.check_qos_data_freshness()

        ignored_patterns = [
            'Country with ISO3 Format', 'not found in DB', 'skipping current iteration', 'does not exist to use',
            'Could not find version for', 'Provided Start version',
        ]
        for message in self.get_error_messages(logs):
            for ignored_pattern in ignored_patterns:
                self.assertNotIn(ignored_pattern, message)

    def test_countries_in_exclusion_list_are_not_reported(self):
        with mock.patch.dict(source_utilities.ds_settings['QOS'], {'COUNTRY_EXCLUSION_LIST': ['KEN', 'ZAF', '']}):
            with self.assertLogs(self.logger_name, level='INFO') as logs:
                sources_tasks.check_qos_data_freshness()

        error_messages = self.get_error_messages(logs)
        self.assertEqual(len(error_messages), 1)
        self.assertTrue(error_messages[0].startswith('QoS data is stale for MNG: '))

    def test_exclusion_entries_match_the_loader_without_stripping(self):
        # The loader compares entries as they are, so it still pulls ' ZAF': the check must keep monitoring it
        with mock.patch.dict(source_utilities.ds_settings['QOS'], {'COUNTRY_EXCLUSION_LIST': ['KEN', ' ZAF']}):
            with self.assertLogs(self.logger_name, level='INFO') as logs:
                sources_tasks.check_qos_data_freshness()

        error_messages = self.get_error_messages(logs)
        self.assertEqual(len(error_messages), 2)
        self.assertTrue(error_messages[0].startswith('QoS data is stale for MNG: '))
        self.assertTrue(error_messages[1].startswith('QoS data is stale for ZAF: '))

    def test_uses_the_latest_pull_of_each_country(self):
        school = self.no_new_version_country.schools.first()
        for hours_ago in [1, 2]:
            pulled_at = self.now - timedelta(hours=hours_ago)
            sources_models.QoSData.objects.create(
                school=school,
                country=self.no_new_version_country,
                timestamp=pulled_at,
                date=pulled_at.date(),
                school_id_giga=school.giga_id_school,
                version=1 + hours_ago,
                pulled_at=pulled_at,
            )

        freshness = source_utilities.get_qos_data_freshness()

        self.assertEqual([country['iso3_format'] for country in freshness], ['BRA', 'KEN', 'MNG', 'ZAF'])
        self.assertEqual(freshness[1]['last_pulled_at'], self.now - timedelta(hours=1))
        self.assertFalse(freshness[1]['is_stale'])

    def test_no_error_when_all_monitored_countries_are_fresh(self):
        sources_models.QoSData.objects.exclude(country=self.fresh_country).delete()

        with self.assertLogs(self.logger_name, level='INFO') as logs:
            sources_tasks.check_qos_data_freshness()

        self.assertEqual(self.get_error_messages(logs), [])
        task = BackgroundTask.objects.get(name__startswith='check_qos_data_freshness_status_')
        self.assertIn('QoS data is fresh for all 1 monitored countries', task.log)

    def test_skips_when_already_run_in_the_same_hour(self):
        sources_tasks.check_qos_data_freshness()

        with self.assertLogs(self.logger_name, level='INFO') as logs:
            sources_tasks.check_qos_data_freshness()

        self.assertEqual(self.get_error_messages(logs), [])
        self.assertTrue(any('skipping current iteration' in record.getMessage() for record in logs.records))
        self.assertEqual(BackgroundTask.objects.filter(name__startswith='check_qos_data_freshness_status_').count(), 1)

    def test_scheduled_daily_after_qos_load_and_cleanup(self):
        finalize_setup(sender=celery_app)

        entry = celery_app.conf.beat_schedule['proco.data_sources.tasks.check_qos_data_freshness']
        self.assertEqual(entry['task'], 'proco.data_sources.tasks.check_qos_data_freshness')
        self.assertEqual(entry['schedule'].hour, {6})
        self.assertEqual(entry['schedule'].minute, {30})
