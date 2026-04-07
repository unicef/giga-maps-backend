import json
import logging
import os
import uuid
from collections import defaultdict
from datetime import timedelta

from celery import chain, chord, group, current_task
from celery.exceptions import SoftTimeLimitExceeded
from django.conf import settings
from django.apps import apps
from django.contrib.gis.geos import Point
from django.core.management import call_command
from django.core.exceptions import ImproperlyConfigured
from django.db.models import Count
from django.db.utils import DataError
from requests.exceptions import HTTPError

from proco.accounts import utils as account_utilities
from proco.background import utils as background_task_utilities
from proco.connection_statistics import models as statistics_models
from proco.connection_statistics.utils import (
    aggregate_real_time_data_to_school_daily_status,
    aggregate_school_daily_status_to_school_weekly_status,
    aggregate_school_daily_to_country_daily,
    update_country_weekly_status,
)
from proco.core import utils as core_utilities
from proco.core.config import app_config as core_configs
from proco.custom_auth import models as auth_models
from proco.custom_auth.utils import get_user_emails_for_permissions
from proco.data_sources import models as sources_models, constants
from proco.data_sources import utils as source_utilities
from proco.data_sources.config import app_config as sources_config
from proco.data_sources.constants import SOFT_TIME_LIMIT, TIME_LIMIT
from proco.data_sources.models import QoSData
from proco.entities.models import EntityType, Entity
from proco.locations.models import Country, CountryAdminMetadata
from proco.schools.models import School
from proco.taskapp import app
from proco.utils.dates import format_date
from proco.utils.tasks import populate_school_new_fields_task
from proco.utils.slack_notification_utils import (
    calculate_school_master_delta_changes, compare_target_model_changes, format_changes_for_slack
)
from proco.utils.slack_notification_service import SlackNotificationService

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
                            str(row.connectivity_govt).lower()  == 'unknown'):
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
        # Delete all the old records where more than 1 record are in DRAFT/UPDATED_IN_DRAFT or
        # ROW_STATUS_DRAFT_LOCKED/ROW_STATUS_UPDATED_IN_DRAFT_LOCKED/ROW_STATUS_DELETED for same School GIGA ID
        rows_with_more_than_1_record_in_draft = sources_models.SchoolMasterData.objects.filter(
            status__in=[
                sources_models.SchoolMasterData.ROW_STATUS_DRAFT,
                sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
                sources_models.SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
                sources_models.SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
                sources_models.SchoolMasterData.ROW_STATUS_DELETED,
            ]
        ).values('school_id_giga', 'country_id').annotate(
            total_records=Count('school_id_giga', distinct=False),
        ).order_by('-total_records', 'school_id_giga', 'country_id').filter(total_records__gt=1)

        logger.debug('Queryset to get all the old records to delete where more than 1 record are in DRAFT/'
                     'UPDATED_IN_DRAFT/ROW_STATUS_DRAFT_LOCKED/ROW_STATUS_UPDATED_IN_DRAFT_LOCKED/ROW_STATUS_DELETED '
                     'for same School GIGA ID: {0}'.format(rows_with_more_than_1_record_in_draft.query))

        for row in rows_with_more_than_1_record_in_draft:
            for row_to_delete in sources_models.SchoolMasterData.objects.filter(
                school_id_giga=row['school_id_giga'],
                country_id=row['country_id'],
            ).order_by('-created')[1:]:
                row_to_delete.delete()
        task_instance.info('Deleted rows where more than 1 record are in DRAFT/'
                           'UPDATED_IN_DRAFT/ROW_STATUS_DRAFT_LOCKED/ROW_STATUS_UPDATED_IN_DRAFT_LOCKED/ROW_STATUS_DELETED '
                           'for same School GIGA ID')

        # At least keep 1 PUBLISHED row for each school in the school master table
        rows_with_more_than_1_record_in_published = sources_models.SchoolMasterData.objects.filter(
            status=sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED,
        ).values('school_id_giga', 'country_id').annotate(
            total_records=Count('school_id_giga', distinct=False),
        ).order_by('-total_records').filter(total_records__gt=1)

        logger.debug('Queryset to get all the old records to delete where more than 1 record are in PUBLISHED '
                     'for same School GIGA ID: {0}'.format(rows_with_more_than_1_record_in_published.query))

        for row in rows_with_more_than_1_record_in_published:
            for row_to_delete in sources_models.SchoolMasterData.objects.filter(
                school_id_giga=row['school_id_giga'],
                country_id=row['country_id'],
            ).order_by('-published_at')[1:]:
                row_to_delete.delete()
        task_instance.info('Deleted rows where more than 1 record are in PUBLISHED for same School GIGA ID')

        # Delete all the old records where more than 1 record are in is_read=True
        # by keeping at least 1 PUBLISHED row for same School GIGA ID
        rows_with_more_than_1_record_in_read = sources_models.SchoolMasterData.objects.filter(
            is_read=True,
        ).values('school_id_giga', 'country_id').annotate(
            total_records=Count('school_id_giga', distinct=False),
        ).order_by('-total_records').filter(total_records__gt=1)

        logger.debug('Queryset to get all the old records to delete where more than 1 record are in is_read=True'
                     ' by keeping at least 1 PUBLISHED row for same School GIGA ID: {0}'.format(
            rows_with_more_than_1_record_in_read.query))

        for row in rows_with_more_than_1_record_in_read:
            for row_to_delete in sources_models.SchoolMasterData.objects.filter(
                school_id_giga=row['school_id_giga'],
                country_id=row['country_id'],
            ).order_by('-published_at')[1:]:
                if row_to_delete.status != sources_models.SchoolMasterData.ROW_STATUS_PUBLISHED:
                    row_to_delete.delete()
        task_instance.info('Deleted rows where more than 1 record are in is_read=True '
                           'by keeping at least 1 PUBLISHED row for same School GIGA ID')

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
        cleanup_school_master_rows.s()
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
        # Delete all entries from QoS Data Table which is older than 7 days
        sources_models.QoSData.objects.filter(timestamp__lt=older_then_date).delete()
        task_instance.info('"QoSData" data table completed')

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
    task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Data loss recovery for School Master versions')

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


def validate_schema_and_sync_schema_table_data(profile_file, schema_name, share_name, country_iso3_format, country_codes_for_exclusion, errors):
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

            health_master_fields = [f.name for f in sources_models.HealthEntityMasterIntermediateData._meta.get_fields()]
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
        "HEALTH_MASTER", "SHARE_NAME", "SCHEMA_NAME", "DASHBOARD_URL", "COUNTRY_EXCLUSION_LIST",
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

    changes_for_countries, deleted_entities, errors = validate_schema_and_sync_schema_table_data(profile_file, schema_name, share_name, country_iso3_format, country_codes_for_exclusion, errors)

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
        cleanup_school_master_rows.s()
        task_instance.info('Scheduled cleanup school master rows')
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

        task_instance.info('Total published records to update: {}'.format(new_published_records.count()))

        for data_chunk in core_utilities.queryset_iterator(new_published_records, chunk_size=100, print_msg=False):
            for row in data_chunk:
                try:
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

                    row_data = {
                        f.name: getattr(row, f.name)
                        for f in row._meta.fields
                    }
                    entity_field_names = {f.name for f in Entity._meta.fields}
                    entity_defaults = {
                        k: v for k, v in row_data.items()
                        if k in entity_field_names
                    }
                    entity_defaults['entity_type'] = entity_type
                    entity_defaults['geopoint'] = Point(row.longitude, row.latitude)
                    entity_defaults['admin1']  = admin1_instance
                    entity_defaults['admin2'] = admin2_instance

                    entity, created = Entity.objects.update_or_create(
                        giga_id=row.health_id_giga,
                        external_id=row.facility_id_govt,
                        name= row.facility_name,
                        defaults=entity_defaults
                    )

                    # To update HealthEntity
                    detail_entity_field_names = {f.name for f in detail_model._meta.fields}
                    detail_entity_defaults = {
                        k: v for k, v in row_data.items()
                        if k in detail_entity_field_names
                    }
                    # detail_entity_defaults['entity'] = entity
                    detail_entity_defaults.pop('entity', None)

                    if detail_model:
                        detail_model.objects.update_or_create(
                            entity=entity,
                            defaults=detail_entity_defaults,
                        )

                    # date = core_utilities.get_current_datetime_object().date()

                    row.is_read = True
                    row.entity = entity
                    row.save()

                    # updated_entity_ids.append(entity.id)
                    # if created:
                    #     created_entity_ids.append(entity.id)
                except Exception as ex:
                    logger.debug('Error reported on publishing: {0}'.format(ex))
                    logger.debug('Record: {0}'.format(row.__dict__))
                    task_instance.info('Error reported for ID ({0}) on publishing: {1}'.format(row.id, ex))

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
    task_instance = background_task_utilities.task_on_start(task_id, task_key, 'Send School Master Data Changes Slack Notification')

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
