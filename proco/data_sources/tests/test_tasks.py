from unittest import mock

from django.core.cache import cache
from django.test import TestCase

import requests
from celery.exceptions import SoftTimeLimitExceeded

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
