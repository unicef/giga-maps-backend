import json
import logging
import os
import uuid
from collections import defaultdict
from datetime import timedelta, datetime

import requests
from celery import chain, chord, group, current_task
from celery.exceptions import SoftTimeLimitExceeded
from django.apps import apps
from django.conf import settings
from django.contrib.gis.geos import Point
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.db import connection, transaction
from django.db.models import Count, Q
from django.db.utils import DataError
from requests.exceptions import HTTPError
from rest_framework import status

from proco.accounts import utils as account_utilities
from proco.background import utils as background_task_utilities
from proco.background.models import BackgroundTask
from proco.connection_statistics import models as statistics_models
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.utils import (
    aggregate_real_time_data_to_school_daily_status,
    aggregate_school_daily_status_to_school_weekly_status,
    aggregate_school_daily_to_country_daily,
    update_country_weekly_status, aggregate_entity_daily_status_to_entity_weekly_status,
)
from proco.core import utils as core_utilities
from proco.core.config import app_config as core_configs
from proco.custom_auth import models as auth_models
from proco.custom_auth.utils import get_user_emails_for_permissions
from proco.data_sources import models as sources_models
from proco.data_sources import utils as source_utilities
from proco.data_sources.config import app_config as sources_config
from proco.data_sources.constants import SOFT_TIME_LIMIT, TIME_LIMIT
from proco.data_sources.models import QoSData
from proco.entities.models import EntityType, Entity
from proco.locations.models import Country, CountryAdminMetadata
from proco.schools.models import School
from proco.taskapp import app
from proco.utils.dates import format_date
from proco.utils.slack_notification_service import SlackNotificationService
from proco.utils.slack_notification_utils import (
    calculate_school_master_delta_changes, compare_target_model_changes, format_changes_for_slack
)
from proco.utils.tasks import populate_school_new_fields_task

logger = logging.getLogger('gigamaps.' + __name__)


@app.task
def finalize_task():
    return 'Done'


def load_data_from_school_master_apis(country_iso3_format=None):
    """
    Background task which handles School Master Data source changes from APIs to PROCO DB

    Execution Frequency: Once in a week
    """
    logger.info('Starting loading the school master data from API to DB.')

    errors = []
    ds_settings = settings.DATA_SOURCE_CONFIG.get('SCHOOL_MASTER')
    share_name = ds_settings['SHARE_NAME']
    schema_name = ds_settings['SCHEMA_NAME']
    dashboard_url = ds_settings['DASHBOARD_URL']
    country_codes_for_exclusion = ds_settings['COUNTRY_EXCLUSION_LIST']

    profile_json = {
        'shareCredentialsVersion': ds_settings.get('SHARE_CREDENTIALS_VERSION', 1),
        'endpoint': ds_settings.get('ENDPOINT'),
        'bearerToken': ds_settings.get('BEARER_TOKEN'),
        'expirationTime': ds_settings.get('EXPIRATION_TIME')
    }
    profile_file = os.path.join(
        settings.BASE_DIR,
        'school_master_profile_{dt}.share'.format(
            dt=format_date(core_utilities.get_current_datetime_object())
        )
    )
    open(profile_file, 'w').write(json.dumps(profile_json))

    # Create a SharingClient.
    client = source_utilities.ProcoSharingClient(profile_file)
    school_master_share = client.get_share(share_name)

    changes_for_countries = {}
    deleted_schools = []

    if school_master_share:
        school_master_schema = client.get_schema(school_master_share, schema_name)

        if school_master_schema:
            schema_tables = client.list_tables(school_master_schema)

            logger.debug('All tables ready to access: {0}'.format(schema_tables))

            school_master_fields = [f.name for f in sources_models.SchoolMasterData._meta.get_fields()]

            for schema_table in schema_tables:
                logger.debug('#' * 10)
                logger.debug('Table: %s', schema_table)

                if country_iso3_format and country_iso3_format != schema_table.name:
                    continue

                if len(country_codes_for_exclusion) > 0 and schema_table.name in country_codes_for_exclusion:
                    logger.warning('Country with ISO3 Format ({0}) configured to exclude from School Master data pull. '
                                   'Hence skipping the load for this country code.'.format(schema_table.name))
                    continue

                try:
                    source_utilities.sync_school_master_data(
                        profile_file, share_name, schema_name, schema_table.name, changes_for_countries,
                        deleted_schools, school_master_fields)
                except (HTTPError, DataError, ValueError) as ex:
                    logger.error('Exception caught for "{0}": {1}'.format(schema_table.name, str(ex)))
                    errors.append('{0} : {1} - {2}'.format(schema_table.name, type(ex).__name__, str(ex)))
                except Exception as ex:
                    logger.error('Exception caught for "{0}": {1}'.format(schema_table.name, str(ex)))
                    errors.append('{0} : {1} - {2}'.format(schema_table.name, type(ex).__name__, str(ex)))

        else:
            logger.error('School Master schema ({0}) does not exist to use for share ({1}).'.format(schema_name,
                                                                                                    share_name))
    else:
        logger.error('School Master share ({0}) does not exist to use.'.format(share_name))

    try:
        os.remove(profile_file)
    except OSError:
        pass

    has_data_changes = len(list(filter(lambda val: val, list(changes_for_countries.values())))) > 0

    if has_data_changes or len(errors) > 0 or len(deleted_schools) > 0:
        # 2. For Change Detection:
        # a) When a change is detected in the external data source (School master data source),
        # then system should trigger an email notification to the designated editor and publisher.
        # b) The email notification should include information about name of the changed data source and
        # a link to the interface in which reviewer can view the updated data.

        editors_and_publishers = get_user_emails_for_permissions([
            auth_models.RolePermission.CAN_UPDATE_SCHOOL_MASTER_DATA,
            auth_models.RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA,
        ])

        if len(editors_and_publishers) > 0:
            email_subject = sources_config.school_master_update_email_subject_format % (
                core_utilities.get_project_title()
            )

            email_message = sources_config.school_master_update_email_message_format
            delete_msg = ''

            if len(deleted_schools) > 0:
                if len(deleted_schools) > 5:
                    delete_msg = """

                    Deleted schools count: {0}
                    """.format(len(deleted_schools))
                else:
                    delete_msg = """

                    Deleted school details from school master data source:
                        {0}
                    """.format('\n'.join(deleted_schools))

            error_msg = ''
            if len(errors) > 0:
                error_msg = """

                Few records failed due to the following errors in the School Master Data Source. We kindly request you to correct these errors so that the skipped records will be available for preview and publish next time:

                {}
                """.format('\n'.join(['{0}) {1}'.format(index, errors[index]) for index in range(len(errors))]))
            email_message = email_message.format(
                delete_msg=delete_msg,
                dashboard_url='Dashboard url: {}'.format(dashboard_url) if dashboard_url else '',
                error_msg=error_msg,
            )

            email_content = {'subject': email_subject, 'message': email_message}
            account_utilities.send_email_over_mailjet_service(editors_and_publishers, **email_content)

        # Send Slack Notification
        for country_iso3_format, _ in changes_for_countries.items():
            send_school_master_data_change_slack_notification.delay(country_iso3_format)


@app.task(soft_time_limit=3 * 55 * 60, time_limit=3 * 60 * 60)
def handle_published_school_master_data_row(published_row=None, country_ids=None, publish_source='auto_publish'):
    """
    Background task to handle all the published rows of school master data source

    Execution Frequency: Every 12 hours
    """
    logger.info('Handling the published school master data rows.')

    environment_map = {
        'urban': 'urban',
        'urbana': 'urban',
        'rural': 'rural',
    }
    change_summary = {}
    coverage_type_choices = dict(statistics_models.SchoolWeeklyStatus.COVERAGE_TYPES).keys()

    if country_ids and len(country_ids) > 0:
        task_key = 'handle_published_school_master_data_row_status_{current_time}_country_ids_{ids}'.format(
            current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'),
            ids='_'.join([str(c_id) for c_id in country_ids]),
        )
        task_description = 'Handle published school master data rows for countries'
    elif published_row:
        task_key = 'handle_published_school_master_data_row_status_{current_time}_row_id_{ids}'.format(
            current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'),
            ids=published_row.id,
        )
        task_description = 'Handle published school master data row for single record'
    else:
        task_key = 'handle_published_school_master_data_row_status_{current_time}'.format(
            current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))
        task_description = 'Handle published school master data rows'

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, task_description)

    if task_instance:
        try:
            logger.debug('Not found running job for published rows handler task: {}'.format(task_key))
            updated_school_ids = []
            created_school_ids = []

            new_published_records = sources_models.SchoolMasterData.objects.filter(
                status=sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED, is_read=False,
            )

            if published_row:
                new_published_records = new_published_records.filter(pk=published_row.id)

            if country_ids and len(country_ids) > 0:
                new_published_records = new_published_records.filter(country_id__in=country_ids)

            task_instance.info('Total published records to update: {}'.format(new_published_records.count()))

            for data_chunk in core_utilities.queryset_iterator(new_published_records, chunk_size=100, print_msg=False):
                for row in data_chunk:
                    try:
                        # Detecting changes for Slack Notification
                        country_code = row.country.iso3_format
                        if country_code not in change_summary:
                            change_summary[country_code] = {
                                'country_name': row.country.name,
                                'school_model_changes': defaultdict(),
                                'school_weekly_changes': defaultdict(),
                                'rt_registration_changes': defaultdict(),
                                'pulled_at_datetime': None,
                                'new_schools': 0,
                                'updated_schools': 0,
                                'deleted_schools': 0
                            }
                        if row.school_id is None:
                            # New Schools
                            change_summary[country_code]['new_schools'] += 1

                        if row.status == sources_models.SchoolMasterData.ROW_STATUS_DELETED:
                            # Deleted Schools
                            change_summary[country_code]['deleted_schools'] += 1

                        school_changes = compare_target_model_changes(row, row.school)

                        if any(school_changes.values()):
                            change_summary[country_code]['updated_schools'] += 1
                            # Increase column count
                            for model_type, field_changes in school_changes.items():
                                for field, count in field_changes.items():
                                    change_summary[country_code][f'{model_type}_changes'].setdefault(field, 0)
                                    change_summary[country_code][f'{model_type}_changes'][field] += count

                        environment = row.school_area_type.lower() if not core_utilities.is_blank_string(
                            row.school_area_type) else ''
                        environment = environment_map.get(environment, '')

                        admin1_instance = None
                        if not core_utilities.is_blank_string(row.admin1_id_giga):
                            admin1_instance = CountryAdminMetadata.objects.filter(
                                country=row.country,
                                giga_id_admin=row.admin1_id_giga,
                                layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN1,
                            ).first()

                        admin2_instance = None
                        if not core_utilities.is_blank_string(row.admin2_id_giga):
                            admin2_instance = CountryAdminMetadata.objects.filter(
                                country=row.country,
                                giga_id_admin=row.admin2_id_giga,
                                layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN2,
                            ).first()

                        school, created = School.objects.update_or_create(
                            giga_id_school=row.school_id_giga,
                            country=row.country,
                            defaults={
                                'external_id': row.school_id_govt,
                                'name': row.school_name,
                                'geopoint': Point(x=row.longitude, y=row.latitude),
                                'education_level': '' if core_utilities.is_blank_string(
                                    row.education_level) else row.education_level,
                                'education_level_govt': row.education_level_govt,
                                'environment': environment,
                                'school_type': '' if core_utilities.is_blank_string(
                                    row.school_funding_type) else row.school_funding_type,
                                'establishment_year': row.school_establishment_year,
                                'admin1': admin1_instance,
                                'admin2': admin2_instance,
                            },
                        )

                        date = core_utilities.get_current_datetime_object().date()
                        school_weekly = statistics_models.SchoolWeeklyStatus.objects.filter(
                            school=school, week=date.isocalendar()[1], year=date.isocalendar()[0],
                        ).last()

                        if not school_weekly:
                            school_weekly = statistics_models.SchoolWeeklyStatus.objects.filter(school=school).last()

                            if school_weekly:
                                # copy latest available one
                                school_weekly.id = None
                                school_weekly.year = date.isocalendar()[0]
                                school_weekly.week = date.isocalendar()[1]
                                school_weekly.modified = core_utilities.get_current_datetime_object()
                                school_weekly.created = core_utilities.get_current_datetime_object()

                                school_weekly.connectivity_speed = None
                                school_weekly.connectivity_upload_speed = None
                                school_weekly.connectivity_latency = None
                                school_weekly.roundtrip_time = None
                                school_weekly.jitter_download = None
                                school_weekly.jitter_upload = None
                                school_weekly.rtt_packet_loss_pct = None
                                school_weekly.connectivity_speed_probe = None
                                school_weekly.connectivity_upload_speed_probe = None
                                school_weekly.connectivity_latency_probe = None
                                school_weekly.connectivity_speed_mean = None
                                school_weekly.connectivity_upload_speed_mean = None
                            else:
                                school_weekly = statistics_models.SchoolWeeklyStatus.objects.create(
                                    school=school,
                                    year=date.isocalendar()[0],
                                    week=date.isocalendar()[1],
                                )

                        school_weekly.num_students = row.num_students
                        school_weekly.num_teachers = row.num_teachers
                        school_weekly.num_classroom = row.num_classrooms
                        school_weekly.num_latrines = row.num_latrines
                        school_weekly.running_water = False \
                            if core_utilities.is_blank_string(row.water_availability) \
                            else str(row.water_availability).lower() in core_configs.true_choices
                        school_weekly.electricity_availability = False \
                            if core_utilities.is_blank_string(row.electricity_availability) \
                            else str(row.electricity_availability).lower() in core_configs.true_choices
                        school_weekly.computer_lab = False \
                            if core_utilities.is_blank_string(row.computer_lab) \
                            else str(row.computer_lab).lower() in core_configs.true_choices
                        school_weekly.num_computers = row.num_computers

                        if (core_utilities.is_blank_string(row.connectivity_govt) or
                            str(row.connectivity_govt).lower() == 'unknown'):
                            school_weekly.connectivity = None
                        else:
                            school_weekly.connectivity = str(row.connectivity_govt).lower() in core_configs.true_choices

                        school_weekly.connectivity_type = row.connectivity_type_govt or 'unknown'

                        if core_utilities.is_blank_string(row.cellular_coverage_availability):
                            school_weekly.coverage_availability = None
                        else:
                            school_weekly.coverage_availability = str(
                                row.cellular_coverage_availability).lower() in core_configs.true_choices

                        coverage_type = statistics_models.SchoolWeeklyStatus.COVERAGE_UNKNOWN
                        if not core_utilities.is_blank_string(row.cellular_coverage_type):
                            coverage_type_in_lower = str(row.cellular_coverage_type).lower()
                            if coverage_type_in_lower in coverage_type_choices:
                                coverage_type = coverage_type_in_lower
                            elif coverage_type_in_lower in ['no service', 'no coverage', 'no']:
                                coverage_type = statistics_models.SchoolWeeklyStatus.COVERAGE_NO

                        school_weekly.coverage_type = coverage_type

                        school_weekly.download_speed_contracted = row.download_speed_contracted
                        school_weekly.num_computers_desired = row.num_computers_desired
                        school_weekly.electricity_type = row.electricity_type
                        school_weekly.num_adm_personnel = row.num_adm_personnel

                        school_weekly.fiber_node_distance = row.fiber_node_distance
                        school_weekly.microwave_node_distance = row.microwave_node_distance

                        school_weekly.schools_within_1km = row.schools_within_1km
                        school_weekly.schools_within_2km = row.schools_within_2km
                        school_weekly.schools_within_3km = row.schools_within_3km

                        school_weekly.nearest_lte_distance = row.nearest_LTE_distance
                        school_weekly.nearest_umts_distance = row.nearest_UMTS_distance
                        school_weekly.nearest_gsm_distance = row.nearest_GSM_distance
                        school_weekly.nearest_nr_distance = row.nearest_NR_distance

                        school_weekly.pop_within_1km = row.pop_within_1km
                        school_weekly.pop_within_2km = row.pop_within_2km
                        school_weekly.pop_within_3km = row.pop_within_3km

                        school_weekly.school_data_source = row.school_data_source
                        school_weekly.school_data_collection_year = row.school_data_collection_year
                        school_weekly.school_data_collection_modality = row.school_data_collection_modality
                        school_weekly.school_location_ingestion_timestamp = row.school_location_ingestion_timestamp
                        school_weekly.connectivity_govt_ingestion_timestamp = row.connectivity_govt_ingestion_timestamp
                        school_weekly.connectivity_govt_collection_year = row.connectivity_govt_collection_year
                        school_weekly.disputed_region = False if core_utilities.is_blank_string(
                            row.disputed_region) else str(row.disputed_region).lower() in core_configs.true_choices

                        download_speed_benchmark = row.download_speed_benchmark
                        if download_speed_benchmark:
                            # convert Mbps to bps
                            school_weekly.download_speed_benchmark = download_speed_benchmark * 1000 * 1000

                        school_weekly.num_students_girls = row.num_students_girls
                        school_weekly.num_students_boys = row.num_students_boys
                        school_weekly.num_students_other = row.num_students_other
                        school_weekly.num_teachers_female = row.num_teachers_female
                        school_weekly.num_teachers_male = row.num_teachers_male
                        school_weekly.num_tablets = row.num_tablets
                        school_weekly.num_robotic_equipment = row.num_robotic_equipment

                        school_weekly.computer_availability = None \
                            if core_utilities.is_blank_string(row.computer_availability) \
                            else str(row.computer_availability).lower() in core_configs.true_choices
                        school_weekly.teachers_trained = None \
                            if core_utilities.is_blank_string(row.teachers_trained) \
                            else str(row.teachers_trained).lower() in core_configs.true_choices
                        school_weekly.sustainable_business_model = None \
                            if core_utilities.is_blank_string(row.sustainable_business_model) \
                            else str(row.sustainable_business_model).lower() in core_configs.true_choices
                        school_weekly.device_availability = None \
                            if core_utilities.is_blank_string(row.device_availability) \
                            else str(row.device_availability).lower() in core_configs.true_choices

                        school_weekly.building_id_govt = row.building_id_govt
                        school_weekly.num_schools_per_building = row.num_schools_per_building

                        school_weekly.save()

                        rt_registered = None
                        if (
                            not core_utilities.is_blank_string(row.connectivity_RT) and
                            row.connectivity_RT_ingestion_timestamp is not None
                        ):
                            rt_registered = str(row.connectivity_RT).lower() in core_configs.true_choices

                        if rt_registered is not None:
                            school_rt_qs = statistics_models.SchoolRealTimeRegistration.objects.filter(school=school)
                            if school_rt_qs.exists():
                                school_rt_instance = school_rt_qs.order_by('-created').first()

                                school_rt_instance.rt_registered = rt_registered
                                school_rt_instance.rt_registration_date = row.connectivity_RT_ingestion_timestamp
                                school_rt_instance.rt_source = row.connectivity_RT_datasource

                                school_rt_instance.save()
                            else:
                                statistics_models.SchoolRealTimeRegistration.objects.create(
                                    school=school,
                                    rt_registered=rt_registered,
                                    rt_registration_date=row.connectivity_RT_ingestion_timestamp,
                                    rt_source=row.connectivity_RT_datasource,
                                )

                        row.is_read = True
                        row.school = school
                        row.save()

                        updated_school_ids.append(school.id)
                        if created:
                            created_school_ids.append(school.id)
                    except Exception as ex:
                        logger.error('Error reported on publishing: {0}'.format(ex))
                        logger.error('Record: {0}'.format(row.__dict__))
                        task_instance.info('Error reported for ID ({0}) on publishing: {1}'.format(row.id, ex))

            if len(updated_school_ids) > 0:
                for i in range(0, len(updated_school_ids), 20):
                    populate_school_new_fields_task.delay(None, None, None, school_ids=updated_school_ids[i:i + 20])

            for new_school_id in created_school_ids:
                # As it's a new school added through School Master record publishing, add the school to search index
                cmd_args = ['--update_index', '-school_id={0}'.format(new_school_id)]
                call_command('index_rebuild_schools', *cmd_args)

            send_slack_notifications(change_summary, publish_source=publish_source)
            background_task_utilities.task_on_complete(task_instance)
        except SoftTimeLimitExceeded:
            send_slack_notifications(change_summary, publish_source=publish_source)
            raise
        except Exception as e:
            raise
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=2 * 55 * 60, time_limit=2 * 55 * 60)
def handle_deleted_school_master_data_row(deleted_row=None, country_ids=None, publish_source='auto_publish'):
    """
    Background task to handle all the deleted rows of school master data source

    Execution Frequency: Every day
    """
    change_summary = {}
    logger.info('Handling the deleted school master data rows.')
    if country_ids and len(country_ids) > 0:
        task_key = 'handle_deleted_school_master_data_row_status_{current_time}_country_ids_{ids}'.format(
            current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'),
            ids='_'.join([str(c_id) for c_id in country_ids]),
        )
        task_description = 'Handle deleted school master data rows for countries'
    elif deleted_row:
        task_key = 'handle_deleted_school_master_data_row_status_{current_time}_row_id_{ids}'.format(
            current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'),
            ids=deleted_row.id,
        )
        task_description = 'Handle deleted school master data row for single record'
    else:
        task_key = 'handle_deleted_school_master_data_row_status_{current_time}'.format(
            current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))
        task_description = 'Handle deleted school master data rows'

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, task_description)

    if task_instance:
        logger.debug('Not found running job for deleted rows handler: {}'.format(task_key))
        new_deleted_records = sources_models.SchoolMasterData.objects.filter(
            status=sources_models.SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
            is_read=False,
            school__isnull=False,
        )

        if deleted_row:
            new_deleted_records = new_deleted_records.filter(pk=deleted_row.id)

        if country_ids and len(country_ids) > 0:
            new_deleted_records = new_deleted_records.filter(country_id__in=country_ids)

        current_date = core_utilities.get_current_datetime_object()
        task_instance.info('Total records to update: {}'.format(new_deleted_records.count()))

        for data_chunk in core_utilities.queryset_iterator(new_deleted_records, chunk_size=1000):
            for row in data_chunk:
                try:
                    country_code = row.country.iso3_format
                    if country_code not in change_summary:
                        change_summary[country_code] = {
                            'country_name': row.country.name,
                            'school_model_changes': defaultdict(),
                            'school_weekly_changes': defaultdict(),
                            'rt_registration_changes': defaultdict(),
                            'pulled_at_datetime': None,
                            'new_schools': 0,
                            'updated_schools': 0,
                            'deleted_schools': 0
                        }
                    # Deleted Schools
                    change_summary[country_code]['deleted_schools'] += 1

                    row.school.delete()

                    statistics_models.SchoolWeeklyStatus.objects.filter(school=row.school).update(deleted=current_date)

                    statistics_models.SchoolDailyStatus.objects.filter(school=row.school).update(deleted=current_date)

                    statistics_models.SchoolRealTimeRegistration.objects.filter(school=row.school).update(
                        deleted=current_date)

                    row.is_read = True
                    row.save()

                except Exception as ex:
                    logger.error('Error reported on deletion: {0}'.format(ex))
                    logger.error('Record: {0}'.format(row.__dict__))
                    task_instance.info('Error reported for ID ({0}) on deletion: {1}'.format(row.id, ex))

        task_instance.info('Remaining records: {}'.format(new_deleted_records.count()))
        send_slack_notifications(change_summary, publish_source=publish_source)
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task
def email_reminder_to_editor_and_publisher_for_review_waiting_records():
    """
    Background task which send the Email reminders to Editor and Publisher if
    there are School Master records which are waiting for the review from
    more than 48 hours

    Execution Frequency: Every day only once
    """
    task_key = 'email_reminder_to_editor_and_publisher_for_review_waiting_records_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y'))
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Send reminder email to Editor and Publisher to review the school master rows')

    if task_instance:
        task_instance.info('Not found running job for reminder email task: {}'.format(task_key))

        ds_settings = settings.DATA_SOURCE_CONFIG.get('SCHOOL_MASTER')
        review_grace_period = core_utilities.convert_to_int(ds_settings['REVIEW_GRACE_PERIOD_IN_HRS'], default='48')

        logger.info('Sending email reminder to Editor/Publisher if records are waiting for more '
                    'than {0} hrs'.format(review_grace_period))
        task_instance.info('Sending email reminder to Editor/Publisher if records are waiting for '
                           'more than {0} hrs'.format(review_grace_period))

        if not settings.ENABLED_DATA_SOURCES_EMAILS:
            logger.error('School Master data source email notification is disabled.')
            task_instance.info('ERROR: School Master data source email notification is disabled.')
        elif (
            core_utilities.is_blank_string(settings.ANYMAIL.get('MAILJET_API_KEY')) or
            core_utilities.is_blank_string(settings.ANYMAIL.get('MAILJET_SECRET_KEY'))
        ):
            logger.error('MailJet creds are not configured to send the email. Hence email notification is disabled.')
            task_instance.info('ERROR: MailJet creds are not configured to send the email. Hence email notification is '
                               'disabled.')
        else:
            current_time = core_utilities.get_current_datetime_object()
            check_time = current_time - timedelta(hours=review_grace_period)
            email_user_list = []

            # If there are records for all editor to review which collected date is more than 48 hrs
            has_records_to_review_for_all_editors = sources_models.SchoolMasterData.objects.filter(
                status=sources_models.SchoolMasterData.ROW_STATUS_DRAFT,
                modified__lt=check_time,
            ).exists()

            # If there are records for all publishers to review which are sent to publishers
            # to publish more than 48 hrs back
            has_records_to_review_for_all_publishers = sources_models.SchoolMasterData.objects.filter(
                status__in=[
                    sources_models.SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
                    sources_models.SchoolMasterData.ROW_STATUS_DELETED,
                ],
                is_read=False,
                modified__lt=check_time,
            ).exists()

            # If it has records for all editors and publishers to review than send the reminder email to all
            if has_records_to_review_for_all_editors and has_records_to_review_for_all_publishers:
                logger.info('All Editors and Publishers has records to review')
                task_instance.info('All Editors and Publishers has records to review')
                email_user_list.extend(get_user_emails_for_permissions([
                    auth_models.RolePermission.CAN_UPDATE_SCHOOL_MASTER_DATA,
                    auth_models.RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA,
                ]))
            else:
                # If all editors have records to review, then send reminder email
                if has_records_to_review_for_all_editors:
                    logger.info('All Editors has records to review')
                    task_instance.info('All Editors has records to review')
                    email_user_list.extend(
                        get_user_emails_for_permissions([auth_models.RolePermission.CAN_UPDATE_SCHOOL_MASTER_DATA]))
                else:
                    # Else send the email to those editors who have updated the DRAFT records but not touched
                    # it in last 48 hrs
                    editor_ids_who_has_old_updated_records = list(sources_models.SchoolMasterData.objects.filter(
                        status=sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
                        modified__lt=check_time,
                    ).values_list('modified_by_id', flat=True).order_by('modified_by_id').distinct('modified_by_id'))

                    if len(editor_ids_who_has_old_updated_records) > 0:
                        logger.info('Only few Editors has records to review')
                        task_instance.info('Only few Editors has records to review')
                        email_user_list.extend(
                            get_user_emails_for_permissions(
                                [auth_models.RolePermission.CAN_UPDATE_SCHOOL_MASTER_DATA],
                                ids_to_filter=editor_ids_who_has_old_updated_records)
                        )

                # If all publishers have records to review, then send reminder email to all
                if has_records_to_review_for_all_publishers:
                    logger.info('All Publishers has records to review')
                    task_instance.info('All Publishers has records to review')
                    email_user_list.extend(
                        get_user_emails_for_permissions([auth_models.RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA]))
                else:
                    # Else send the email to those publishers who have updated the records
                    # but not touched it in last 48 hrs
                    publisher_ids_who_has_old_updated_records = list(sources_models.SchoolMasterData.objects.filter(
                        status=sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
                        modified__lt=check_time,
                    ).values_list('modified_by_id', flat=True).order_by('modified_by_id').distinct('modified_by_id'))

                    if len(publisher_ids_who_has_old_updated_records) > 0:
                        logger.info('Only few Publishers has records to review')
                        task_instance.info('Only few Publishers has records to review')
                        email_user_list.extend(
                            get_user_emails_for_permissions(
                                [auth_models.RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA],
                                ids_to_filter=publisher_ids_who_has_old_updated_records)
                        )

            if len(email_user_list) > 0:
                # Get the unique email IDs so it sends only 1 email
                unique_email_ids = set(email_user_list)

                email_subject = sources_config.school_master_records_to_review_email_subject_format % (
                    core_utilities.get_project_title()
                )

                dashboard_url = ds_settings['DASHBOARD_URL']
                email_message = sources_config.school_master_records_to_review_email_message_format.format(
                    dashboard_url='Dashboard url: {}'.format(dashboard_url) if dashboard_url else '',
                )

                email_content = {'subject': email_subject, 'message': email_message}
                logger.info('Sending the below emails:\n'
                            'To: {0}\n'
                            'Subject: {1}\n'
                            'Body: {2}'.format(unique_email_ids, email_subject, email_message))
                task_instance.info('Sending the below emails:\tTo: {0}\tSubject: {1}\tBody: {2}'.format(
                    unique_email_ids, email_subject, email_message))
                account_utilities.send_email_over_mailjet_service(unique_email_ids, **email_content)

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=60 * 60, time_limit=60 * 60)
def load_data_from_daily_check_app_api(*args):
    logger.info('Loading the DailyCheckApp data to DB.')
    source_utilities.sync_dailycheckapp_realtime_data()
    logger.info('Loaded the DailyCheckApp data to DB successfully.')


@app.task(soft_time_limit=4 * 60 * 60, time_limit=4 * 60 * 60)
def load_data_from_qos_apis(*args):
    logger.info('Loading the QoS data to DB.')
    changes_for_countries = {}

    source_utilities.load_qos_data_source_response_to_model(changes_for_countries)

    countries_ids = list(Country.objects.all().filter(
        iso3_format__in=list(changes_for_countries.keys())
    ).values_list('id', flat=True).order_by('id').distinct('id'))

    for country_id in countries_ids:
        source_utilities.sync_qos_realtime_data(country_id)
    logger.info('Loaded the QoS data to DB successfully.')


@app.task(soft_time_limit=2 * 60 * 60, time_limit=2 * 60 * 60)
def cleanup_school_master_rows():
    task_key = 'cleanup_school_master_rows_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Cleanup school master rows')

    if task_instance:
        logger.debug('Not found running job for school master cleanup task: {}'.format(task_key))
        country_ids = list(sources_models.SchoolMasterData.objects.values_list('country_id', flat=True).distinct())
        for country_id in country_ids:
            if not country_id:
                continue
            with connection.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM data_sources_schoolmasterdata
                    WHERE country_id = %s AND id IN (
                        SELECT id FROM (
                            SELECT id, ROW_NUMBER() OVER (
                                PARTITION BY school_id_giga
                                ORDER BY created DESC
                            ) as rn
                            FROM data_sources_schoolmasterdata
                            WHERE country_id = %s AND status IN ('DRAFT', 'UPDATED_IN_DRAFT', 'DRAFT_LOCKED', 'UPDATED_IN_DRAFT_LOCKED', 'DELETED', 'DELETED_PUBLISHED', 'DISCARDED')
                        ) t
                        WHERE t.rn > 1
                    )
                """, [country_id, country_id])

                cursor.execute("""
                    DELETE FROM data_sources_schoolmasterdata
                    WHERE country_id = %s AND id IN (
                        SELECT id FROM (
                            SELECT id, ROW_NUMBER() OVER (
                                PARTITION BY school_id_giga
                                ORDER BY published_at DESC
                            ) as rn
                            FROM data_sources_schoolmasterdata
                            WHERE country_id = %s AND status = 'PUBLISHED'
                        ) t
                        WHERE t.rn > 1
                    )
                """, [country_id, country_id])

                cursor.execute("""
                    DELETE FROM data_sources_schoolmasterdata
                    WHERE country_id = %s AND id IN (
                        SELECT id FROM (
                            SELECT id, ROW_NUMBER() OVER (
                                PARTITION BY school_id_giga
                                ORDER BY created DESC
                            ) as rn
                            FROM data_sources_schoolmasterdata
                            WHERE country_id = %s AND is_read = True AND status != 'PUBLISHED'
                        ) t
                        WHERE t.rn > 1
                    )
                """, [country_id, country_id])
        task_instance.info('Deleted duplicate rows for same School GIGA ID chunked by country')
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=2 * 60 * 60, time_limit=2 * 60 * 60)
def cleanup_health_entity_master_rows():
    task_key = 'cleanup_health_entity_master_rows_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Cleanup health master rows')

    if task_instance:
        logger.debug('Not found running job for health master cleanup task: {}'.format(task_key))
        country_ids = list(sources_models.HealthEntityMasterIntermediateData.objects.values_list('country_id', flat=True).distinct())
        for country_id in country_ids:
            if not country_id:
                continue
            with connection.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM data_sources_healthentitymasterintermediatedata
                    WHERE country_id = %s AND id IN (
                        SELECT id FROM (
                            SELECT id, ROW_NUMBER() OVER (
                                PARTITION BY health_id_giga
                                ORDER BY created DESC
                            ) as rn
                            FROM data_sources_healthentitymasterintermediatedata
                            WHERE country_id = %s AND status IN ('DRAFT', 'UPDATED_IN_DRAFT', 'DRAFT_LOCKED', 'UPDATED_IN_DRAFT_LOCKED', 'DELETED', 'DELETED_PUBLISHED', 'DISCARDED')
                        ) t
                        WHERE t.rn > 1
                    )
                """, [country_id, country_id])

                cursor.execute("""
                    DELETE FROM data_sources_healthentitymasterintermediatedata
                    WHERE country_id = %s AND id IN (
                        SELECT id FROM (
                            SELECT id, ROW_NUMBER() OVER (
                                PARTITION BY health_id_giga
                                ORDER BY published_at DESC
                            ) as rn
                            FROM data_sources_healthentitymasterintermediatedata
                            WHERE country_id = %s AND status = 'PUBLISHED'
                        ) t
                        WHERE t.rn > 1
                    )
                """, [country_id, country_id])

                cursor.execute("""
                    DELETE FROM data_sources_healthentitymasterintermediatedata
                    WHERE country_id = %s AND id IN (
                        SELECT id FROM (
                            SELECT id, ROW_NUMBER() OVER (
                                PARTITION BY health_id_giga
                                ORDER BY created DESC
                            ) as rn
                            FROM data_sources_healthentitymasterintermediatedata
                            WHERE country_id = %s AND is_read = True AND status != 'PUBLISHED'
                        ) t
                        WHERE t.rn > 1
                    )
                """, [country_id, country_id])
        task_instance.info('Deleted duplicate rows for same Health GIGA ID chunked by country')
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=6 * 60 * 60, time_limit=6 * 60 * 60)
def update_static_data(*args, country_iso3_format=None):
    """
    Background task to Get Static data to Proco DB

    1. School Master Data source

    Execution Frequency: Once in a week/once in 2 weeks
    """
    task_key = 'update_static_data_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Sync Static Data from School Master sources', check_previous=True)

    if task_instance:
        logger.debug('Not found running job for static data pull handler: {}'.format(task_key))
        load_data_from_school_master_apis(country_iso3_format=country_iso3_format)
        task_instance.info('Completed the load data from School Master API call')
        cleanup_school_master_rows.delay()
        task_instance.info('Scheduled cleanup school master rows')
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=60 * 60, time_limit=60 * 60)
def finalize_previous_day_data(_prev_result, country_id, date, *args):
    country = Country.objects.get(id=country_id)

    aggregate_real_time_data_to_school_daily_status(country, date)
    aggregate_school_daily_to_country_daily(country, date)

    weekly_data_available = aggregate_school_daily_status_to_school_weekly_status(country, date)
    if weekly_data_available:
        update_country_weekly_status(country, date)

    country.invalidate_country_related_cache()


@app.task(soft_time_limit=2 * 60 * 60, time_limit=2 * 60 * 60)
def update_live_data(*args, today=True):
    """
    Background task executed multiple times a day to get the real time data to Proco DB

    1. Daily Check App + MLab
    2. QoS

    Execution Frequency: 4-5 times a day
    """
    task_key = 'update_live_data_status_{current_time}_{today}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'),
        today=today,
    )
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Sync Realtime Data from Live sources')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))
        countries_ids = Country.objects.values_list('id', flat=True)

        if today:
            today_date = core_utilities.get_current_datetime_object().date()
            chain(
                load_data_from_daily_check_app_api.s(),
                load_data_from_qos_apis.s(),
                chord(
                    group([
                        finalize_previous_day_data.s(country_id, today_date)
                        for country_id in countries_ids
                    ]),
                    finalize_task.si(),
                ),
            ).delay()

        else:
            yesterday_date = core_utilities.get_current_datetime_object().date() - timedelta(days=1)
            chain(
                load_data_from_daily_check_app_api.s(),
                load_data_from_qos_apis.s(),
                chord(
                    group([
                        finalize_previous_day_data.s(country_id, yesterday_date)
                        for country_id in countries_ids
                    ]),
                    finalize_task.si(),
                ),

            ).delay()

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=2 * 60 * 60, time_limit=2 * 60 * 60)
def update_qos_data(*args, today=True):
    """
    Background task executed multiple once a day to get the QoS data to Giga DB
    """
    task_key = 'update_qos_data_status_{current_time}_{today}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'),
        today=today,
    )
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key,
                                                            'Sync QoS Realtime Data from Live source')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))
        countries_ids = list(QoSData.objects.all().order_by('country_id').values_list(
            'country_id', flat=True).distinct('country_id'))

        if today:
            aggr_date = core_utilities.get_current_datetime_object().date()
        else:
            aggr_date = core_utilities.get_current_datetime_object().date() - timedelta(days=1)

        chain(
            load_data_from_qos_apis.s(),
            chord(
                group([
                    finalize_previous_day_data.s(country_id, aggr_date)
                    for country_id in countries_ids
                ]),
                finalize_task.si(),
            ),

        ).delay()

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=1 * 60 * 60, time_limit=1 * 60 * 60)
def clean_old_live_data():
    current_datetime = core_utilities.get_current_datetime_object()
    task_key = 'clean_old_live_data_status_{current_time}'.format(
        current_time=format_date(current_datetime, frmt='%d%m%Y_%H'),
    )
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Clean live data older than 30 days')

    if task_instance:
        logger.debug('Not found running job for live data cleanup handler: {}'.format(task_key))
        older_then_date = current_datetime - timedelta(days=30)

        logger.debug('Deleting all the rows from "RealTimeConnectivity" Data Table which is older than: {0}'.format(
            older_then_date))
        statistics_models.RealTimeConnectivity.objects.filter(created__lt=older_then_date).delete()
        task_instance.info('"RealTimeConnectivity" data table completed')

        logger.debug(
            'Deleting all the rows from "DailyCheckAppMeasurementData" Data Table which is older than: {0}'.format(
                older_then_date))
        # Delete all entries from DailyCheckApp Data Table which is older than 7 days
        sources_models.DailyCheckAppMeasurementData.objects.filter(created_at__lt=older_then_date).delete()
        task_instance.info('"DailyCheckAppMeasurementData" data table completed')

        logger.debug('Deleting all the rows from "QoSData" Data Table which is older than: {0}'.format(older_then_date))
        # Delete all entries from QoS Data Table which is older than 30 days
        sources_models.QoSData.objects.filter(timestamp__lt=older_then_date).delete()
        task_instance.info('"QoSData" data table completed')

        logger.debug('Deleting all the rows from "EntityRealTimeConnectivity" Data Table which is older than: '
                     '{0}'.format(older_then_date))
        # Delete all entries from Entity RealTime Connectivity Table which is older than 30 days
        statistics_models.EntityRealTimeConnectivity.objects.filter(created__lt=older_then_date).delete()
        task_instance.info('"EntityRealTimeConnectivity" data table completed')

        logger.debug('Deleting all the rows from "BackgroundTask" Data Table which is older than: '
                     '{0}'.format(older_then_date))
        # Delete all entries from BackgroundTask Table which are older than 30 days
        # We use ._raw_delete to bypass loading objects into memory for faster execution
        background_tasks_qs = BackgroundTask.objects.filter(created_at__lt=older_then_date)
        background_tasks_qs._raw_delete(background_tasks_qs.db)
        task_instance.info('"BackgroundTask" data table completed')

        # Purge soft-deleted schools and related statuses to reclaim DB space and prevent bloat
        logger.info('Purging soft-deleted schools and statuses...')
        deleted_school_ids = list(School.objects.all_records().filter(deleted__isnull=False).values_list('id', flat=True)[:50000])
        logger.info('Found {0} soft-deleted schools to purge.'.format(len(deleted_school_ids)))
        for i in range(0, len(deleted_school_ids), 5000):
            chunk = deleted_school_ids[i:i+5000]
            School.objects.all_records().filter(id__in=chunk).update(last_weekly_status=None)
            sources_models.SchoolMasterData.objects.filter(school_id__in=chunk).update(school_id=None)
            statistics_models.SchoolDailyStatus.objects.all_records().filter(school_id__in=chunk)._raw_delete(connection.alias)
            statistics_models.SchoolWeeklyStatus.objects.all_records().filter(school_id__in=chunk)._raw_delete(connection.alias)
            statistics_models.RealTimeConnectivity.objects.all_records().filter(school_id__in=chunk)._raw_delete(connection.alias)
            statistics_models.SchoolRealTimeRegistration.objects.all_records().filter(school_id__in=chunk)._raw_delete(connection.alias)
            sources_models.QoSData.objects.filter(school_id__in=chunk)._raw_delete(connection.alias)
            School.objects.all_records().filter(id__in=chunk)._raw_delete(connection.alias)
        logger.info('Purged soft-deleted schools successfully.')

        # Purge any orphan soft-deleted school statuses
        statistics_models.SchoolDailyStatus.objects.all_records().filter(deleted__isnull=False)._raw_delete(connection.alias)
        School.objects.all_records().filter(
            last_weekly_status__in=statistics_models.SchoolWeeklyStatus.objects.all_records().filter(deleted__isnull=False)
        ).update(last_weekly_status=None)
        statistics_models.SchoolWeeklyStatus.objects.all_records().filter(deleted__isnull=False)._raw_delete(connection.alias)

        # Purge soft-deleted entities and related statuses
        logger.info('Purging soft-deleted entities and statuses...')
        deleted_entity_ids = list(Entity.objects.all_records().filter(deleted__isnull=False).values_list('id', flat=True)[:50000])
        logger.info('Found {0} soft-deleted entities to purge.'.format(len(deleted_entity_ids)))
        for i in range(0, len(deleted_entity_ids), 5000):
            chunk = deleted_entity_ids[i:i+5000]
            Entity.objects.all_records().filter(id__in=chunk).update(last_weekly_status=None)
            sources_models.HealthEntityMasterIntermediateData.objects.filter(entity_id__in=chunk).update(entity_id=None)
            statistics_models.EntityDailyStatus.objects.all_records().filter(entity_id__in=chunk)._raw_delete(connection.alias)
            statistics_models.EntityWeeklyStatus.objects.all_records().filter(entity_id__in=chunk)._raw_delete(connection.alias)
            statistics_models.EntityRealTimeConnectivity.objects.all_records().filter(entity_id__in=chunk)._raw_delete(connection.alias)
            statistics_models.EntityRealTimeRegistration.objects.all_records().filter(entity_id__in=chunk)._raw_delete(connection.alias)

            from proco.entities.models import HealthEntity
            HealthEntity.objects.all_records().filter(entity_id__in=chunk)._raw_delete(connection.alias)
            Entity.objects.all_records().filter(id__in=chunk)._raw_delete(connection.alias)
        logger.info('Purged soft-deleted entities successfully.')

        # Purge any orphan soft-deleted entity statuses
        statistics_models.EntityDailyStatus.objects.all_records().filter(deleted__isnull=False)._raw_delete(connection.alias)
        Entity.objects.all_records().filter(
            last_weekly_status__in=statistics_models.EntityWeeklyStatus.objects.all_records().filter(deleted__isnull=False)
        ).update(last_weekly_status=None)
        statistics_models.EntityWeeklyStatus.objects.all_records().filter(deleted__isnull=False)._raw_delete(connection.alias)

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=10 * 60 * 60, time_limit=10 * 60 * 60)
def data_loss_recovery_for_pcdc_weekly_task(start_week_no, end_week_no, year, pull_data, *args):
    """
    data_loss_recovery_for_pcdc_weekly_task
        Task to schedule manually from Console.
    """
    if not start_week_no or not end_week_no or not year:
        logger.error('Required args not provided: [start_week_no, end_week_no, year]')
        return

    logger.info('Starting data loss recovery for pcdc task: start_week_no "{0}" - end_week_no "{1}" - '
                'year "{2}"'.format(start_week_no, end_week_no, year))

    task_key = 'data_loss_recovery_for_pcdc_weekly_task_start_week_no_{0}_end_week_no_{1}_year_{2}_on_{3}'.format(
        start_week_no, end_week_no, year, format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Recover the data for PCDC live source')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))
        cmd_args = [
            '-start_week_no={}'.format(start_week_no),
            '-end_week_no={}'.format(end_week_no),
            '-year={}'.format(year),
        ]

        if pull_data:
            cmd_args.append('--pull_data')

        call_command('data_loss_recovery_for_pcdc_weekly', *cmd_args)

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=1 * 60 * 60, time_limit=1 * 60 * 60)
def clean_historic_data():
    current_datetime = core_utilities.get_current_datetime_object()
    task_key = 'clean_historic_data_status_{current_time}'.format(
        current_time=format_date(current_datetime, frmt='%d%m%Y_%H%M%S'),
    )
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Clean historic data')

    if task_instance:
        logger.debug('Not found running job for historic data cleanup handler: {}'.format(task_key))
        cmd_args = [
            '--clean_school_master_historical_rows',
        ]

        call_command('data_source_additional_steps', *cmd_args)
        task_instance.info('Completed school master historical record cleanup.')

        cmd_args = [
            '--clean_health_entity_master_historical_rows',
        ]

        call_command('data_source_additional_steps', *cmd_args)
        task_instance.info('Completed health entity master historical record cleanup.')

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=2 * 60 * 60, time_limit=2 * 60 * 60)
def scheduler_for_data_loss_recovery_for_qos_dates(
    country_iso3_format,
    start_date,
    end_date,
    check_missing_dates,
    pull_data,
    aggregate_data
):
    current_datetime = core_utilities.get_current_datetime_object()
    task_key = 'scheduler_for_data_loss_recovery_for_qos_dates_{current_time}'.format(
        current_time=format_date(current_datetime, frmt='%d%m%Y_%H%M%S'),
    )
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Data loss recovery for QoS dates')

    if task_instance:
        logger.debug('Not found running job for qos data loss utility handler: {}'.format(task_key))
        cmd_args = []
        if country_iso3_format:
            cmd_args.append('-country_code={}'.format(country_iso3_format))

        if start_date:
            cmd_args.append('-start_date={}'.format(start_date))
        if end_date:
            cmd_args.append('-end_date={}'.format(end_date))

        if check_missing_dates is True:
            cmd_args.append('--check_missing_dates')
        if pull_data is True:
            cmd_args.append('--pull_data')
        if aggregate_data is True:
            cmd_args.append('--aggregate')

        call_command('data_loss_recovery_for_qos_dates', *cmd_args)
        task_instance.info('Completed QoS data loss recovery utility handler.')

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=4 * 60 * 60, time_limit=4 * 60 * 60)
def scheduler_for_data_loss_recovery_for_school_master_version(
    country_iso3_format,
    pull_data,
    pull_version,
):
    current_datetime = core_utilities.get_current_datetime_object()
    task_key = 'scheduler_for_data_loss_recovery_for_school_master_version_{code}_{current_time}'.format(
        current_time=format_date(current_datetime, frmt='%d%m%Y_%H'),
        code=country_iso3_format,
    )
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key,
                                                            'Data loss recovery for School Master versions')

    if task_instance:
        logger.debug('Not found running job for school master data loss utility handler: {0}'.format(task_key))
        cmd_args = []
        if country_iso3_format:
            cmd_args.append('-country_code={0}'.format(country_iso3_format))

        if pull_data is True:
            cmd_args.append('--pull_data')

        if pull_version:
            cmd_args.append('-pull_version={0}'.format(pull_version))

        call_command('data_loss_recovery_for_school_master_version', *cmd_args)
        task_instance.info('Completed School Master data loss recovery utility handler.')

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


def validate_config(config: dict, parent: str, *children: str):
    if not config:
        raise ImproperlyConfigured(
            "DATA_SOURCE_CONFIG is missing from settings."
        )
    if parent not in config:
        raise ImproperlyConfigured(
            f"Missing required config section: DATA_SOURCE_CONFIG['{parent}']"
        )
    parent_config = config[parent]
    missing = [key for key in children if key not in parent_config]
    if missing:
        raise ImproperlyConfigured(
            f"Missing required config keys in DATA_SOURCE_CONFIG['{parent}']: "
            f"{', '.join(missing)}"
        )
    return parent_config


def validate_schema_and_sync_schema_table_data(profile_file, schema_name, share_name, country_iso3_format,
                                               country_codes_for_exclusion, errors):
    # Create a SharingClient.
    client = source_utilities.ProcoSharingClient(profile_file)
    health_master_share = client.get_share(share_name)
    changes_for_countries = {}
    deleted_entities = []
    if health_master_share:
        health_master_schema = client.get_schema(health_master_share, schema_name)
        if health_master_schema:
            schema_tables = client.list_tables(health_master_schema)
            logger.debug('All tables ready to access: {0}'.format(schema_tables))

            health_master_fields = [f.name for f in
                                    sources_models.HealthEntityMasterIntermediateData._meta.get_fields()]
            for schema_table in schema_tables:
                logger.debug('#' * 10)
                logger.debug('Table: %s', schema_table)
                if country_iso3_format and country_iso3_format != schema_table.name:
                    continue
                if len(country_codes_for_exclusion) > 0 and schema_table.name in country_codes_for_exclusion:
                    logger.warning('Country with ISO3 Format ({0}) configured to exclude from Health Master data pull. '
                                   'Hence skipping the load for this country code.'.format(schema_table.name))
                    continue
                try:
                    source_utilities.vaildate_master_version_and_sync_health_master_data(
                        profile_file, share_name, schema_name, schema_table.name, changes_for_countries,
                        deleted_entities, health_master_fields)
                except (HTTPError, DataError, ValueError) as ex:
                    logger.error('Exception caught for "{0}": {1}'.format(schema_table.name, str(ex)))
                    errors.append('{0} : {1} - {2}'.format(schema_table.name, type(ex).__name__, str(ex)))
                except Exception as ex:
                    logger.error('Exception caught for "{0}": {1}'.format(schema_table.name, str(ex)))
                    errors.append('{0} : {1} - {2}'.format(schema_table.name, type(ex).__name__, str(ex)))
            return changes_for_countries, deleted_entities, errors
        else:
            logger.error('Health Master schema ({0}) does not exist to use for share ({1}).'.format(schema_name,
                                                                                                    share_name))
    else:
        logger.error('Health Master share ({0}) does not exist to use.'.format(share_name))


def load_entity_data_from_health_master_apis(country_iso3_format=None):
    """
    Background task which handles Health Master Data source changes from APIs to PROCO DB
    Execution Frequency: Once in a week
    """
    logger.info('Starting loading the health master data from API to DB.')

    errors = []
    ds_settings = validate_config(settings.DATA_SOURCE_CONFIG,
                                  "HEALTH_MASTER", "SHARE_NAME", "SCHEMA_NAME", "DASHBOARD_URL",
                                  "COUNTRY_EXCLUSION_LIST",
                                  "SHARE_CREDENTIALS_VERSION", "ENDPOINT", "BEARER_TOKEN", "EXPIRATION_TIME"
                                  )
    share_name = ds_settings['SHARE_NAME']
    schema_name = ds_settings['SCHEMA_NAME']
    dashboard_url = ds_settings['DASHBOARD_URL']
    country_codes_for_exclusion = ds_settings['COUNTRY_EXCLUSION_LIST']
    profile_json = {
        'shareCredentialsVersion': ds_settings.get('SHARE_CREDENTIALS_VERSION', 1),
        'endpoint': ds_settings.get('ENDPOINT'),
        'bearerToken': ds_settings.get('BEARER_TOKEN'),
        'expirationTime': ds_settings.get('EXPIRATION_TIME')
    }
    profile_file = os.path.join(
        settings.BASE_DIR,
        'health_master_profile_{dt}.share'.format(
            dt=format_date(core_utilities.get_current_datetime_object())
        )
    )
    open(profile_file, 'w').write(json.dumps(profile_json))

    changes_for_countries, deleted_entities, errors = validate_schema_and_sync_schema_table_data(profile_file,
                                                                                                 schema_name,
                                                                                                 share_name,
                                                                                                 country_iso3_format,
                                                                                                 country_codes_for_exclusion,
                                                                                                 errors)

    try:
        os.remove(profile_file)
    except OSError:
        pass

    has_data_changes = len(list(filter(lambda val: val, list(changes_for_countries.values())))) > 0

    if has_data_changes or len(errors) > 0 or len(deleted_entities) > 0:
        # 2. For Change Detection:
        # a) When a change is detected in the external data source (Health master data source),
        # then system should trigger an email notification to the designated editor and publisher.
        # b) The email notification should include information about name of the changed data source and
        # a link to the interface in which reviewer can view the updated data.

        editors_and_publishers = get_user_emails_for_permissions([
            auth_models.RolePermission.CAN_UPDATE_SCHOOL_MASTER_DATA,
            auth_models.RolePermission.CAN_PUBLISH_SCHOOL_MASTER_DATA,
        ])

        if len(editors_and_publishers) > 0:
            email_subject = sources_config.health_master_update_email_subject_format % (
                core_utilities.get_project_title()
            )

            email_message = sources_config.health_master_update_email_message_format
            delete_msg = ''

            if len(deleted_entities) > 0:
                if len(deleted_entities) > 5:
                    delete_msg = """
                    Deleted Entities count: {0}
                    """.format(len(deleted_entities))
                else:
                    delete_msg = """
                    Deleted entities details from health master data source:
                        {0}
                    """.format('\n'.join(deleted_entities))

            error_msg = ''
            if len(errors) > 0:
                error_msg = """
                Few records failed due to the following errors in the Health Master Data Source. We kindly request you to correct these errors so that the skipped records will be available for preview and publish next time:
                {}
                """.format('\n'.join(['{0}) {1}'.format(index, errors[index]) for index in range(len(errors))]))
            email_message = email_message.format(
                delete_msg=delete_msg,
                dashboard_url='Dashboard url: {}'.format(dashboard_url) if dashboard_url else '',
                error_msg=error_msg,
            )

            email_content = {'subject': email_subject, 'message': email_message}
            account_utilities.send_email_over_mailjet_service(editors_and_publishers, **email_content)


@app.task(soft_time_limit=SOFT_TIME_LIMIT, time_limit=TIME_LIMIT)
def update_entity_static_data(*args, country_iso3_format=None):
    """
    Background task to Get Static data to Proco DB
    1. Health Master Data source
    Execution Frequency: Once in a week/once in 2 weeks
    """
    task_key = 'update_entity_static_data{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Sync Static Data from Health Master sources', check_previous=True)
    if task_instance:
        logger.debug('Not found running job for static data pull handler: {}'.format(task_key))
        load_entity_data_from_health_master_apis(country_iso3_format=country_iso3_format)
        task_instance.info('Completed the load data from Health Master API call')
        cleanup_health_entity_master_rows.delay()
        task_instance.info('Scheduled cleanup health master rows')
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=3 * 55 * 60, time_limit=3 * 55 * 60)
def handle_published_entity_master_data_row(published_row=None, country_ids=None):
    """
    Background task to handle all the published rows of school master data source

    Execution Frequency: Every 12 hours
    """
    logger.info('Handling the published health master data rows.')

    entity_type = EntityType.objects.get(code='health')

    master_model = apps.get_model(*entity_type.master_data_model.split('.'))
    detail_model = (
        apps.get_model(*entity_type.detail_model.split('.'))
        if entity_type.detail_model else None
    )

    if country_ids and len(country_ids) > 0:
        task_key = 'handle_published_entity_master_data_row_status_{current_time}_country_ids_{ids}'.format(
            current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'),
            ids='_'.join([str(c_id) for c_id in country_ids]),
        )
        task_description = 'Handle published entity master data rows for countries'
    elif published_row:
        task_key = 'handle_published_entity_master_data_row_status_{current_time}_row_id_{ids}'.format(
            current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'),
            ids=published_row.id,
        )
        task_description = 'Handle published entity master data row for single record'
    else:
        task_key = 'handle_published_entity_master_data_row_status_{current_time}'.format(
            current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))
        task_description = 'Handle published entity master data rows'

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, task_description)

    if task_instance:
        logger.debug('Not found running job for published rows handler task: {}'.format(task_key))
        updated_entity_ids = []
        created_entity_ids = []

        # Manually updating the records as PUBLISHED
        master_model.objects.filter(
            is_read=False
        ).update(
            status=master_model.ROW_STATUS_PUBLISHED
        )

        new_published_records = master_model.objects.filter(
            status=master_model.ROW_STATUS_PUBLISHED,
            is_read=False,
        )

        if published_row:
            new_published_records = new_published_records.filter(pk=published_row.id)

        if country_ids and len(country_ids) > 0:
            new_published_records = new_published_records.filter(country_id__in=country_ids)

        new_published_record_ids = list(new_published_records.values_list('id', flat=True))
        task_instance.info('Total published records to update: {}'.format(len(new_published_record_ids)))

        entity_field_names = {f.name for f in Entity._meta.fields}
        lookup_fields = {
            'giga_id', 'country', 'entity_type',
            'country_id', 'entity_type_id', 'id',
            'admin1', 'admin2', 'admin1_id', 'admin2_id',
            'geopoint', 'external_id', 'name'
        }
        if detail_model:
            detail_entity_field_names = {f.name for f in detail_model._meta.fields}
            detail_lookup_fields = {'entity', 'entity_id', 'id', 'deleted'}

        for i in range(0, len(new_published_record_ids), 2000):
            chunk_ids = new_published_record_ids[i:i+2000]
            data_chunk = list(master_model.objects.filter(id__in=chunk_ids))
            if not data_chunk:
                continue

            # Pre-fetch Admin1 and Admin2 metadata to prevent N+1 queries
            admin1_giga_ids = [row.admin1_id_giga for row in data_chunk if not core_utilities.is_blank_string(row.admin1_id_giga)]
            admin2_giga_ids = [row.admin2_id_giga for row in data_chunk if not core_utilities.is_blank_string(row.admin2_id_giga)]
            chunk_country_ids = list(set([row.country_id for row in data_chunk]))

            admin1_map = {}
            if admin1_giga_ids:
                admin1_qs = CountryAdminMetadata.objects.filter(
                    country_id__in=chunk_country_ids,
                    giga_id_admin__in=admin1_giga_ids,
                    layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN1,
                )
                admin1_map = {(a.country_id, a.giga_id_admin): a for a in admin1_qs}

            admin2_map = {}
            if admin2_giga_ids:
                admin2_qs = CountryAdminMetadata.objects.filter(
                    country_id__in=chunk_country_ids,
                    giga_id_admin__in=admin2_giga_ids,
                    layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN2,
                )
                admin2_map = {(a.country_id, a.giga_id_admin): a for a in admin2_qs}

            # Batch fetch existing Entity records to avoid N+1 SELECT queries
            health_giga_ids = [row.health_id_giga for row in data_chunk]
            existing_entities = {
                e.giga_id: e for e in Entity.objects.filter(
                    giga_id__in=health_giga_ids,
                    entity_type=entity_type,
                    deleted__isnull=True
                )
            }

            local_entities = {}
            entities_to_create = []
            entities_to_update = []

            # Find latest row per unique health_id_giga within the chunk
            latest_row_by_giga = {}
            for row in data_chunk:
                latest_row_by_giga[row.health_id_giga] = row

            row_field_names = {f.name for f in data_chunk[0]._meta.fields}
            common_entity_fields = (entity_field_names & row_field_names) - lookup_fields

            for giga_id, row in latest_row_by_giga.items():
                try:
                    admin1_instance = None
                    if not core_utilities.is_blank_string(row.admin1_id_giga):
                        admin1_instance = admin1_map.get((row.country_id, row.admin1_id_giga))

                    admin2_instance = None
                    if not core_utilities.is_blank_string(row.admin2_id_giga):
                        admin2_instance = admin2_map.get((row.country_id, row.admin2_id_giga))

                    entity_defaults = {
                        name: getattr(row, name)
                        for name in common_entity_fields
                    }
                    entity_defaults['geopoint'] = Point(row.longitude, row.latitude)
                    entity_defaults['admin1'] = admin1_instance
                    entity_defaults['admin2'] = admin2_instance
                    entity_defaults['external_id'] = row.facility_id_govt
                    entity_defaults['name'] = row.facility_name

                    # Map connectivity/connectivity_govt to connectivity_status for Entity
                    connectivity_govt = str(getattr(row, 'connectivity_govt', '') or '').lower().strip()
                    connectivity = str(getattr(row, 'connectivity', '') or '').lower().strip()
                    if connectivity_govt in ['yes', 'true', 'good', 'moderate'] or connectivity in ['yes', 'true',
                                                                                                    'good', 'moderate']:
                        entity_defaults['connectivity_status'] = 'good'
                    elif connectivity_govt in ['no', 'false'] or connectivity in ['no', 'false']:
                        entity_defaults['connectivity_status'] = 'no'
                    else:
                        entity_defaults['connectivity_status'] = 'unknown'

                    if giga_id in existing_entities:
                        entity = existing_entities[giga_id]
                        for k, v in entity_defaults.items():
                            setattr(entity, k, v)
                        entity.name_lower = str(entity.name).lower()
                        if entity.external_id:
                            entity.external_id = str(entity.external_id).lower()
                        entities_to_update.append(entity)
                    else:
                        entity = Entity(
                            giga_id=giga_id,
                            country_id=row.country_id,
                            entity_type=entity_type,
                        )
                        for k, v in entity_defaults.items():
                            setattr(entity, k, v)
                        entity.name_lower = str(entity.name).lower()
                        if entity.external_id:
                            entity.external_id = str(entity.external_id).lower()
                        entities_to_create.append(entity)

                    local_entities[giga_id] = entity
                except Exception as ex:
                    logger.debug('Error building Entity record: {0}'.format(ex))
                    task_instance.info('Error building Entity record for ID ({0}) on publishing: {1}'.format(row.id, ex))

            # Perform Entity bulk creation
            if entities_to_create:
                Entity.objects.bulk_create(entities_to_create)
                # Populate IDs back into local_entities map
                for entity in entities_to_create:
                    local_entities[entity.giga_id] = entity

            # Perform Entity bulk update
            if entities_to_update:
                entity_fields_to_update = [
                    'geopoint', 'admin1', 'admin2', 'external_id', 'name', 'name_lower', 'connectivity_status'
                ]
                # Also include dynamic fields
                dummy_row = data_chunk[0]
                for f in dummy_row._meta.fields:
                    if f.name in entity_field_names and f.name not in lookup_fields:
                        entity_fields_to_update.append(f.name)
                entity_fields_to_update = list(set(entity_fields_to_update))
                Entity.objects.bulk_update(entities_to_update, fields=entity_fields_to_update)

            # Process detail models (e.g. HealthEntity) in bulk
            if detail_model:
                existing_details = {
                    d.entity_id: d for d in detail_model.objects.filter(
                        entity_id__in=[e.id for e in local_entities.values() if e.id],
                        deleted__isnull=True
                    )
                }

                details_to_create = []
                details_to_update = []
                common_detail_fields = (detail_entity_field_names & row_field_names) - detail_lookup_fields

                for giga_id, row in latest_row_by_giga.items():
                    entity = local_entities.get(giga_id)
                    if not entity or not entity.id:
                        continue

                    try:
                        detail_entity_defaults = {
                            name: getattr(row, name)
                            for name in common_detail_fields
                        }

                        if entity.id in existing_details:
                            detail = existing_details[entity.id]
                            for k, v in detail_entity_defaults.items():
                                setattr(detail, k, v)
                            details_to_update.append(detail)
                        else:
                            detail = detail_model(entity=entity)
                            for k, v in detail_entity_defaults.items():
                                setattr(detail, k, v)
                            details_to_create.append(detail)
                    except Exception as ex:
                        logger.debug('Error building detail model record: {0}'.format(ex))
                        task_instance.info('Error building detail record for ID ({0}) on publishing: {1}'.format(row.id, ex))

                if details_to_create:
                    detail_model.objects.bulk_create(details_to_create)

                if details_to_update:
                    detail_fields_to_update = [
                        f.name for f in detail_model._meta.fields
                        if f.name not in detail_lookup_fields
                    ]
                    detail_model.objects.bulk_update(details_to_update, fields=detail_fields_to_update)

            # Link master rows to their updated Entity
            rows_to_update = []
            for row in data_chunk:
                entity = local_entities.get(row.health_id_giga)
                if entity and entity.id:
                    row.is_read = True
                    row.entity = entity
                    rows_to_update.append(row)

            if rows_to_update:
                master_model.objects.bulk_update(rows_to_update, fields=['is_read', 'entity'])


        # Need to update the below tasks once ready
        # if len(updated_entity_ids) > 0:
        #     for i in range(0, len(updated_entity_ids), 20):
        #         populate_school_new_fields_task.delay(None, None, None, school_ids=updated_entity_ids[i:i + 20])
        #
        #
        # for new_entity_id in created_entity_ids:
        #     # As it's a new entity added through Entity Master record publishing, add the school to search index
        #     cmd_args = ['--update_index', '-entity_id={0}'.format(new_entity_id)]
        #     call_command('index_rebuild_schools', *cmd_args)

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=2 * 60 * 60, time_limit=2 * 60 * 60)
def send_school_master_data_change_slack_notification(country_iso3_format):
    """
    Background task to calculate delta changes and send Slack notification for school master data updates.
    """
    current_datetime = core_utilities.get_current_datetime_object()
    task_key = 'send_school_master_data_change_slack_notification_{country}_{current_time}'.format(
        country=country_iso3_format,
        current_time=format_date(current_datetime, frmt='%d%m%Y_%H%M%S'),
    )
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key,
                                                            'Send School Master Data Changes Slack Notification')

    if task_instance:
        try:
            logger.info(f'Calculating delta changes for {country_iso3_format}.')

            country = Country.objects.filter(iso3_format=country_iso3_format).first()
            if not country:
                logger.error(f'Country with ISO3 Format ({country_iso3_format}) not found in DB.')
                return None

            # Get New SchoolMasterData rows
            pulled_at_date = core_utilities.get_current_datetime_object().date()
            school_master_data = sources_models.SchoolMasterData.objects.filter(
                country=country,
                pulled_at__gt=pulled_at_date
            ).select_related('school')

            # Calculate delta changes from SchoolMasterData table
            change_summary = calculate_school_master_delta_changes(school_master_data, country)

            if change_summary:
                # Send Slack notification
                slack_service = SlackNotificationService()
                slack_service.send_school_master_update_notification(change_summary, publish_source='pre_review')

                task_instance.info(f'Slack notification sent for {change_summary["country"]} - '
                                   f'New: {change_summary["new_rows_count"]}, '
                                   f'Updated: {change_summary["updated_rows_count"]}, '
                                   f'Deleted: {change_summary["deleted_rows_count"]}')
            else:
                task_instance.info(f'No changes found for {country_iso3_format}')

            background_task_utilities.task_on_complete(task_instance)

        except Exception as e:
            logger.error(f'Error in school master data change notification task: {str(e)}')
            task_instance.error(f'Failed to send notification: {str(e)}')
            background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error(f'Found running Job with "{task_key}" name so skipping current iteration')


def send_slack_notifications(change_summary, publish_source='auto_publish'):
    """Send Slack notifications for published school master data changes."""
    for country_iso3, summary in change_summary.items():
        if not summary['new_schools'] and not summary['updated_schools'] and not summary['deleted_schools']:
            continue
        country = Country.objects.filter(iso3_format=country_iso3).first()
        summary = format_changes_for_slack(country, summary)
        slack_service = SlackNotificationService()
        slack_service.send_school_master_update_notification(summary, publish_source=publish_source)


# ############################# Entity Live Data Tasks #############################

class EntityAggregationOngoingException(Exception):
    """Raised when the Giga Meter entity API reports aggregation is still in progress."""
    pass


ENTITY_GIGA_METER_MAX_RETRIES = 3


def fetch_entity_giga_meter_ping_data(entity_type_code, start_date, end_date, country_iso3, logger):
    """
    Fetch live measurement data from the entity Giga Meter API for a specific country.
    Iterates through pages and yields records.

    Endpoint format:
        {BASE_URL}/api/v1/measurements/v2/sandbox
            ?entity_type={entity_type_code}
            &country_iso3_code={country_iso3}
            &filterBy=timestamp&filterCondition=gt&filterValue={start_date}T00:00:00Z
            &orderBy=-timestamp
            &size={page_size}
            &page={page}

    This mirrors sync_dailycheckapp_realtime_data() query parameter pattern from
    data_sources/utils.py but adapted for the entity endpoint.

    Args:
        entity_type_code: The entity type code (e.g. 'health').
        start_date: Start date for the data range.
        end_date: End date for the data range.
        country_iso3: ISO3 country code (e.g. 'KEN').
        logger: Logger instance.

    Yields:
        dict: Record dicts with entity giga_id and connectivity metrics.
    """
    entity_gm_settings = settings.DATA_SOURCE_CONFIG.get('DAILY_CHECK_APP')
    if not entity_gm_settings:
        logger.warning('DAILY_CHECK_APP config not found in DATA_SOURCE_CONFIG. Skipping.')
        return

    base_url = entity_gm_settings.get('BASE_URL')
    if not base_url:
        logger.warning('DAILY_CHECK_APP BASE_URL not configured. Skipping.')
        return

    measurement_path = entity_gm_settings.get('MEASUREMENT_PATH', '/api/v1/measurements/v2/sandbox')
    api_code = entity_gm_settings.get('API_CODE', 'DAILY_CHECK_APP')
    page_size = entity_gm_settings.get('PAGE_SIZE', 50)

    # Build the full endpoint URL
    api_endpoint = '{base_url}{path}'.format(
        base_url=base_url.rstrip('/'),
        path=measurement_path,
    )

    # Build auth headers using reusable utility with entity-specific API code
    request_config = {
        'url': api_endpoint,
        'method': 'GET',
        'auth_token_required': True,
        'headers': {
            'Content-Type': 'application/json',
        },
    }
    headers = source_utilities.get_request_headers(request_config, api_code=api_code)

    # Filter value: start of the date range in ISO 8601 format
    filter_value = '{date}T00:00:00Z'.format(date=start_date.strftime('%Y-%m-%d'))

    page = 0

    while True:
        params = {
            'entity_type': entity_type_code,
            'country_iso3_code': country_iso3,
            'filterBy': 'timestamp',
            'filterCondition': 'gt',
            'filterValue': filter_value,
            'orderBy': '-timestamp',
            'size': page_size,
            'page': page,
        }

        try:
            # Build full URL for logging so parameters are visible
            log_url = f"{api_endpoint}?page={page}&size={page_size}&country={country_iso3}"
            logger.info(
                'Entity Giga Meter - Fetching page %d for country %s from %s',
                page, country_iso3, log_url,
            )
            response = requests.get(api_endpoint, params=params, headers=headers)

            if response.status_code != status.HTTP_200_OK:
                logger.error(
                    'Entity Giga Meter - Failed to fetch data: %s - %s',
                    response.status_code, response.text,
                )
                raise HTTPError('API returned {0}'.format(response.status_code))

            response_data = response.json()

            # The response may be a list (like the school endpoint) or a dict with 'data' key
            if isinstance(response_data, dict):
                meta = response_data.get('meta', {})
                if meta.get('aggregationSchedulerStatus') == 'on_going':
                    logger.info('Entity Giga Meter - API indicated ongoing aggregation. Will retry.')
                    raise EntityAggregationOngoingException('Aggregation is currently on_going.')
                data = response_data.get('data', [])
            else:
                # Response is a flat list of records
                data = response_data

            if not data:
                logger.info('Entity Giga Meter - No more data for country %s.', country_iso3)
                break

            for record in data:
                giga_id = (
                    record.get('giga_id')
                    or record.get('entity_id_giga')
                    or record.get('giga_id_health')
                    or record.get('giga_id_school')
                    or record.get('school_id')
                )
                if not giga_id:
                    continue

                # Parse timestamp to date
                timestamp_raw = record.get('timestamp') or record.get('created_at')
                if timestamp_raw:
                    try:
                        record_date = datetime.fromisoformat(
                            timestamp_raw.replace('Z', '+00:00')
                        ).date()
                    except (ValueError, AttributeError):
                        record_date = start_date
                else:
                    record_date = start_date

                # Respect the end_date bound (the API filterCondition=gt only bounds the start)
                if record_date > end_date:
                    continue

                yield {
                    'timestamp_date__date': record_date,
                    'giga_id': giga_id,
                    'download_speed': record.get('download'),
                    'upload_speed': record.get('upload'),
                    'avg_latency': record.get('latency'),
                    'country_code': record.get('country_code', country_iso3),
                }

            # If we received fewer records than page_size, there's no more data
            if len(data) < page_size:
                break

            page += 1

        except EntityAggregationOngoingException:
            raise
        except Exception as e:
            logger.error('Entity Giga Meter - Error fetching page %d for country %s: %s', page, country_iso3, e)
            raise


def fetch_entity_map(giga_ids, entity_type_code='health'):
    """
    Fetch Entity objects for the given list of giga_ids.

    Args:
        giga_ids: List of entity giga IDs.
        entity_type_code: Entity type code to filter by.

    Returns:
        Dict mapping giga_id -> Entity instance.
    """
    entity_map = {}
    if not giga_ids:
        return entity_map

    chunk_size = 5000
    for i in range(0, len(giga_ids), chunk_size):
        chunk_ids = giga_ids[i:i + chunk_size]
        for entity in (
            Entity.objects
                .filter(
                giga_id__in=chunk_ids,
                entity_type__code=entity_type_code,
                deleted__isnull=True,
            )
                .only('id', 'giga_id')
                .iterator(chunk_size=1000)
        ):
            entity_map[entity.giga_id] = entity

    return entity_map


def aggregate_entity_ping_rows(rows, entity_map):
    """
    Aggregate raw entity measurement API rows by (entity_id, date).

    Groups records by entity and date, computing averages for download speed,
    upload speed, and latency. This mirrors aggregate_ping_rows() in
    giga_meter/tasks.py but adapted for entity measurement data.

    Args:
        rows: List of row dicts from the API.
        entity_map: Dict mapping giga_id -> Entity.

    Returns:
        List of aggregated result dicts.
    """
    aggregated = {}
    for row in rows:
        entity = entity_map.get(row.get('giga_id'))
        if entity is None:
            continue

        key = (entity.id, row['timestamp_date__date'])

        download = float(row.get('download_speed')) if row.get('download_speed') is not None else None
        upload = float(row.get('upload_speed')) if row.get('upload_speed') is not None else None
        latency = float(row.get('avg_latency')) if row.get('avg_latency') is not None else None

        if key not in aggregated:
            aggregated[key] = {
                'entity': entity,
                'date': row['timestamp_date__date'],
                'downloads': [download] if download is not None else [],
                'uploads': [upload] if upload is not None else [],
                'latencies': [latency] if latency is not None else [],
            }
        else:
            if download is not None:
                aggregated[key]['downloads'].append(download)
            if upload is not None:
                aggregated[key]['uploads'].append(upload)
            if latency is not None:
                aggregated[key]['latencies'].append(latency)

    results = []
    for data in aggregated.values():
        downloads = data['downloads']
        avg_download = sum(downloads) / len(downloads) if downloads else None

        uploads = data['uploads']
        avg_upload = sum(uploads) / len(uploads) if uploads else None

        latencies = data['latencies']
        avg_latency = sum(latencies) / len(latencies) if latencies else None

        # Determine connectivity: entity is connected if we have at least one measurement
        is_connected = 1.0 if downloads or uploads or latencies else 0.0

        results.append({
            'entity': data['entity'],
            'date': data['date'],
            'connectivity_speed': int(avg_download) if avg_download is not None else None,
            'connectivity_upload_speed': int(avg_upload) if avg_upload is not None else None,
            'connectivity_latency': avg_latency,
            'is_connected': is_connected,
        })

    return results


def bulk_upsert_entity_daily_status(batch):
    """
    Bulk upsert EntityDailyStatus records.

    Mirrors bulk_upsert_school_status() in giga_meter/tasks.py but for entities.

    Args:
        batch: List of EntityDailyStatus instances.
    """
    if not batch:
        return

    deduplicated_batch = {}
    for item in batch:
        key = (item.entity_id, item.date, item.live_data_source)
        deduplicated_batch[key] = item

    unique_batch = list(deduplicated_batch.values())

    existing_qs = statistics_models.EntityDailyStatus.objects.filter(
        entity_id__in=[s.entity_id for s in unique_batch],
        date__in=list(set(s.date for s in unique_batch)),
        live_data_source__in=list(set(s.live_data_source for s in unique_batch)),
    )

    existing_records = {}
    for record in existing_qs:
        key = (record.entity_id, record.date, record.live_data_source)
        existing_records[key] = record

    to_create = []
    to_update = []

    update_fields = [
        'connectivity_speed', 'connectivity_upload_speed', 'connectivity_latency',
        'is_connected_true', 'is_connected_all',
    ]

    for item in unique_batch:
        key = (item.entity_id, item.date, item.live_data_source)
        if key in existing_records:
            existing = existing_records[key]
            existing.connectivity_speed = item.connectivity_speed
            existing.connectivity_upload_speed = item.connectivity_upload_speed
            existing.connectivity_latency = item.connectivity_latency
            existing.is_connected_true = item.is_connected_true
            existing.is_connected_all = item.is_connected_all
            to_update.append(existing)
        else:
            to_create.append(item)

    try:
        with transaction.atomic():
            if to_update:
                statistics_models.EntityDailyStatus.objects.bulk_update(
                    to_update,
                    fields=update_fields,
                )
            if to_create:
                statistics_models.EntityDailyStatus.objects.bulk_create(to_create)
    except (DataError, Exception) as e:
        logger.warning('Entity Giga Meter - Bulk operation failed (%s), falling back to iterative update_or_create.', e)
        for item in unique_batch:
            statistics_models.EntityDailyStatus.objects.update_or_create(
                entity_id=item.entity_id,
                date=item.date,
                live_data_source=item.live_data_source,
                defaults={
                    'connectivity_speed': item.connectivity_speed,
                    'connectivity_upload_speed': item.connectivity_upload_speed,
                    'connectivity_latency': item.connectivity_latency,
                    'is_connected_true': item.is_connected_true,
                    'is_connected_all': item.is_connected_all,
                }
            )


def run_entity_ping_aggregation(entity_type_code, start_date, end_date, task_instance, logger):
    """
    Run entity ping aggregation: fetch measurements per-country from the Giga Meter API,
    aggregate by (entity, date), and upsert into EntityDailyStatus.

    The API requires a country_iso3_code parameter, so we iterate over all countries
    that have entities of the given type.

    Args:
        entity_type_code: Entity type code (e.g. 'health').
        start_date: Start date.
        end_date: End date.
        task_instance: Background task instance for logging.
        logger: Logger instance.
    """
    logger.info('Entity Giga Meter - Aggregating measurement data from %s to %s', start_date, end_date)
    task_instance.info('Entity Giga Meter - Aggregating {0} data from {1} to {2}'.format(
        entity_type_code, start_date, end_date,
    ))

    # Get all countries that have entities of this type
    country_iso3_list = list(
        Country.objects.filter(
            entities__entity_type__code=entity_type_code,
            entities__deleted__isnull=True,
        ).distinct().values_list('iso3_format', flat=True)
    )

    if not country_iso3_list:
        logger.info('Entity Giga Meter - No countries found with %s entities.', entity_type_code)
        task_instance.info('Entity Giga Meter - No countries found with {0} entities.'.format(entity_type_code))
        return

    logger.info('Entity Giga Meter - Processing %d countries: %s', len(country_iso3_list), country_iso3_list)
    task_instance.info('Entity Giga Meter - Processing {0} countries'.format(len(country_iso3_list)))

    total_upserted = 0

    for country_iso3 in country_iso3_list:
        try:
            raw_rows = list(fetch_entity_giga_meter_ping_data(
                entity_type_code, start_date, end_date, country_iso3, logger,
            ))

            if not raw_rows:
                logger.info('Entity Giga Meter - No records for country %s.', country_iso3)
                continue

            # Build entity map from the fetched giga_ids
            giga_ids = list(set(r.get('giga_id') for r in raw_rows if r.get('giga_id')))
            entity_map = fetch_entity_map(giga_ids, entity_type_code)

            logger.info(
                'Entity Giga Meter - Country %s: fetched %d rows, mapped %d entities',
                country_iso3, len(raw_rows), len(entity_map),
            )

            # Aggregate
            aggregated_records = aggregate_entity_ping_rows(raw_rows, entity_map)

            # Convert to EntityDailyStatus instances
            batch = []
            for data in aggregated_records:
                batch.append(statistics_models.EntityDailyStatus(
                    entity=data['entity'],
                    date=data['date'],
                    live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
                    connectivity_speed=data['connectivity_speed'],
                    connectivity_upload_speed=data['connectivity_upload_speed'],
                    connectivity_latency=data['connectivity_latency'],
                    is_connected_true=data['is_connected'],
                    is_connected_all=data['is_connected'],
                ))

            # Upsert
            bulk_upsert_entity_daily_status(batch)
            total_upserted += len(batch)

            logger.info('Entity Giga Meter - Country %s: upserted %d records', country_iso3, len(batch))

            # Auto-aggregate to WeeklyStatus and ensure RealTimeRegistration
            country_obj = Country.objects.get(iso3_format=country_iso3)
            current_date = start_date
            while current_date <= end_date:
                aggregate_entity_daily_status_to_entity_weekly_status(country_obj, current_date, entity_type_code)
                current_date += timedelta(days=7)
            # Ensure the final week is covered
            aggregate_entity_daily_status_to_entity_weekly_status(country_obj, end_date, entity_type_code)

            # Auto-register entities for realtime data
            from django.utils import timezone
            existing_regs = {
                reg.entity_id: reg
                for reg in statistics_models.EntityRealTimeRegistration.objects.all_records().filter(
                    entity__in=entity_map.values()
                )
            }

            to_create = []
            to_update = []
            for entity_obj in entity_map.values():
                reg = existing_regs.get(entity_obj.id)
                if reg:
                    updated = False
                    if not reg.rt_registered:
                        reg.rt_registered = True
                        updated = True
                    if reg.rt_source != statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE:
                        reg.rt_source = statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE
                        updated = True
                    if not reg.rt_registration_date or reg.rt_registration_date.date() != start_date:
                        if reg.rt_registration_date and timezone.is_aware(reg.rt_registration_date):
                            start_datetime = timezone.make_aware(datetime.combine(start_date, datetime.min.time()))
                        else:
                            start_datetime = datetime.combine(start_date, datetime.min.time())
                        reg.rt_registration_date = start_datetime
                        updated = True

                    if updated:
                        to_update.append(reg)
                else:
                    if settings.USE_TZ:
                        start_datetime = timezone.make_aware(datetime.combine(start_date, datetime.min.time()))
                    else:
                        start_datetime = datetime.combine(start_date, datetime.min.time())

                    to_create.append(statistics_models.EntityRealTimeRegistration(
                        entity=entity_obj,
                        rt_registered=True,
                        rt_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
                        rt_registration_date=start_datetime
                    ))

            if to_create:
                statistics_models.EntityRealTimeRegistration.objects.bulk_create(to_create, batch_size=5000)
            if to_update:
                statistics_models.EntityRealTimeRegistration.objects.bulk_update(
                    to_update,
                    ['rt_registered', 'rt_source', 'rt_registration_date'],
                    batch_size=5000
                )

        except EntityAggregationOngoingException:
            # Re-raise to let the task retry
            raise
        except Exception as ex:
            logger.error('Entity Giga Meter - Error processing country %s: %s', country_iso3, ex)
            task_instance.info('Entity Giga Meter - Error for country {0}: {1}'.format(country_iso3, ex))

    logger.info('Entity Giga Meter - Total upserted: %d EntityDailyStatus records', total_upserted)
    task_instance.info('Entity Giga Meter - Total upserted: {0} records'.format(total_upserted))


@app.task(
    soft_time_limit=4 * 60 * 60,
    time_limit=4 * 60 * 60,
)
def update_entity_live_data_from_giga_meter(
    entity_type_code='health',
    date_str=None,
    force_tasks=False,
    retry_attempt=0,
):
    """
    Celery task: Fetch aggregated ping data for entities from the Giga Meter API and store it.

    This is the entity equivalent of fetch_and_aggregate_ping_data in giga_meter/tasks.py.
    It is entity-type generic — pass any entity_type_code (default: 'health').

    Args:
        entity_type_code: The entity type code (e.g. 'health', 'library'). Default: 'health'.
        date_str: Optional date string in YYYY-MM-DD format. Defaults to today.
        force_tasks: If True, generates a unique task key to bypass deduplication.
        retry_attempt: Current retry attempt count (for internal retry logic).

    Execution Frequency: 3 times a day (configured in taskapp/__init__.py)
    """
    if not settings.ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC:
        logger.warning(
            'Entity live data sync is disabled. '
            'To enable, update "ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC" to True.'
        )
        return

    timestamp = core_utilities.get_current_datetime_object()
    timestamp_str = format_date(
        timestamp,
        frmt='%d%m%Y_%H%M%S' if force_tasks else '%d%m%Y_%H%M',
    )

    task_key = f'update_entity_live_data_giga_meter_{entity_type_code}_{timestamp_str}'
    task_id = current_task.request.id or str(uuid.uuid4())

    task_instance = background_task_utilities.task_on_start(
        task_id,
        task_key,
        f'Entity Giga Meter - Fetch and aggregate {entity_type_code} ping data',
    )

    if not task_instance:
        logger.error(f'Found running job with key "{task_key}", skipping.')
        return

    if date_str:
        target_date = core_utilities.get_timezone_converted_value(
            datetime.strptime(date_str, '%Y-%m-%d')
        ).date()
    else:
        target_date = core_utilities.get_current_datetime_object().date()

    try:
        logger.info(
            f"Entity Giga Meter - Starting {entity_type_code} ping aggregation for {target_date} "
            f"(attempt {retry_attempt}/{ENTITY_GIGA_METER_MAX_RETRIES})"
        )

        run_entity_ping_aggregation(
            entity_type_code=entity_type_code,
            start_date=target_date,
            end_date=target_date,
            task_instance=task_instance,
            logger=logger,
        )

        task_instance.info("Entity Giga Meter - Ping aggregation completed successfully.")

    except EntityAggregationOngoingException:
        next_attempt = retry_attempt + 1
        logger.info(
            f"Entity Giga Meter - Aggregation still ongoing. Attempt {next_attempt}/{ENTITY_GIGA_METER_MAX_RETRIES}"
        )
        task_instance.info(
            f"Entity Giga Meter - Aggregation ongoing. Scheduling retry {next_attempt}/{ENTITY_GIGA_METER_MAX_RETRIES}"
        )
        if next_attempt > ENTITY_GIGA_METER_MAX_RETRIES:
            logger.error("Entity Giga Meter - Maximum retry attempts reached. Stopping.")
            task_instance.info("Entity Giga Meter - Maximum retries reached. Task will not be rescheduled.")
            return

        update_entity_live_data_from_giga_meter.apply_async(
            kwargs={
                'entity_type_code': entity_type_code,
                'force_tasks': force_tasks,
                'retry_attempt': next_attempt,
            },
            countdown=15 * 60,
        )
        return

    except Exception as exc:
        logger.exception("Entity Giga Meter - Error during ping aggregation")
        task_instance.info(f"Entity Giga Meter - Error occurred: {exc}")
        raise

    finally:
        background_task_utilities.task_on_complete(task_instance)


@app.task(soft_time_limit=2 * 60 * 60, time_limit=2 * 60 * 60)
def update_entity_qos_data(entity_type_code='health', today=True):
    """
    Celery task: Fetch QoS data for entities from Delta Sharing and aggregate to EntityDailyStatus.

    This is the entity equivalent of update_qos_data in data_sources/tasks.py.
    It is entity-type generic — pass any entity_type_code (default: 'health').

    Args:
        entity_type_code: The entity type code (e.g. 'health', 'library'). Default: 'health'.
        today: If True, aggregate for today. If False, aggregate for yesterday.

    Execution Frequency: Once a day (configured in taskapp/__init__.py)
    """
    if not settings.ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC:
        logger.warning(
            'Entity live data sync is disabled. '
            'To enable, update "ENTITY_LIVE_DATA_ENABLE_AUTO_SYNC" to True.'
        )
        return

    task_key = 'update_entity_qos_data_{type}_{current_time}_{today}'.format(
        type=entity_type_code,
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'),
        today=today,
    )
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key,
        f'Sync Entity QoS Realtime Data ({entity_type_code})',
    )

    if task_instance:
        logger.debug('Entity QoS - Not found running job: %s', task_key)

        try:
            # Step 1: Load entity QoS data from Delta Sharing
            logger.info('Entity QoS - Loading %s QoS data from Delta Sharing...', entity_type_code)
            task_instance.info(f'Entity QoS - Loading {entity_type_code} QoS data from Delta Sharing...')
            source_utilities.load_entity_qos_data_source_response_to_model(entity_type_code=entity_type_code)
            task_instance.info(f'Entity QoS - Completed loading {entity_type_code} QoS data.')

            # Step 2: Get country IDs that have entity realtime data
            countries_ids = list(
                statistics_models.EntityRealTimeConnectivity.objects.filter(
                    live_data_source=statistics_configs.QOS_SOURCE,
                    entity__entity_type__code=entity_type_code,
                    entity__deleted__isnull=True,
                ).order_by('entity__country_id').values_list(
                    'entity__country_id', flat=True
                ).distinct('entity__country_id')
            )

            logger.info('Entity QoS - Found %d countries with %s QoS data.', len(countries_ids), entity_type_code)
            task_instance.info(f'Entity QoS - Found {len(countries_ids)} countries with {entity_type_code} QoS data.')

            # Step 3: Sync realtime data to daily status for each country
            for country_id in countries_ids:
                try:
                    source_utilities.sync_entity_qos_realtime_data(country_id, entity_type_code=entity_type_code)
                except Exception as ex:
                    logger.error(
                        'Entity QoS - Error syncing realtime data for country %s: %s', country_id, str(ex)
                    )
                    task_instance.info(
                        f'Entity QoS - Error for country {country_id}: {ex}'
                    )

            task_instance.info(f'Entity QoS - Completed {entity_type_code} QoS sync.')

        except Exception as exc:
            logger.exception('Entity QoS - Error during QoS data sync')
            task_instance.info(f'Entity QoS - Error occurred: {exc}')
            raise

        finally:
            background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "%s" name so skipping current iteration', task_key)
