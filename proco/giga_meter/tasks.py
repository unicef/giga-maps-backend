import json
import logging
import os
import uuid

from celery import current_task
from django.conf import settings
from django.db.models import Count
from django.db.utils import DataError
from requests.exceptions import HTTPError

from proco.background import utils as background_task_utilities
from proco.core import utils as core_utilities
from proco.giga_meter import models as giga_meter_models
from proco.giga_meter import utils as giga_meter_utilities
from proco.data_sources import utils as data_sources_utilities
from proco.taskapp import app
from proco.utils.dates import format_date

logger = logging.getLogger('gigamaps.' + __name__)


def giga_meter_load_data_from_school_master_apis(country_iso3_format=None):
    """
    Background task which handles School Master Data source changes from APIs to GigaMeter DB

    Execution Frequency: Once in a day
    """
    logger.info('Starting loading the school master data from API to GigaMeter DB.')

    ds_settings = settings.DATA_SOURCE_CONFIG.get('SCHOOL_MASTER')
    share_name = ds_settings['SHARE_NAME']
    schema_name = ds_settings['SCHEMA_NAME']
    country_codes_for_exclusion = ds_settings['COUNTRY_EXCLUSION_LIST']

    profile_json = {
        'shareCredentialsVersion': ds_settings.get('SHARE_CREDENTIALS_VERSION', 1),
        'endpoint': ds_settings.get('ENDPOINT'),
        'bearerToken': ds_settings.get('BEARER_TOKEN'),
        'expirationTime': ds_settings.get('EXPIRATION_TIME')
    }
    profile_file = os.path.join(
        settings.BASE_DIR,
        'giga_meter_school_master_profile_{dt}.share'.format(
            dt=format_date(core_utilities.get_current_datetime_object())
        )
    )
    open(profile_file, 'w').write(json.dumps(profile_json))

    # Create a SharingClient.
    client = data_sources_utilities.ProcoSharingClient(profile_file)
    school_master_share = client.get_share(share_name)

    if school_master_share:
        school_master_schema = client.get_schema(school_master_share, schema_name)

        if school_master_schema:
            schema_tables = client.list_tables(school_master_schema)

            logger.debug('All tables ready to access for Giga Meter Sync: {0}'.format(schema_tables))

            school_master_fields = [f.name for f in giga_meter_models.GigaMeter_SchoolMasterData._meta.get_fields()]

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
                    giga_meter_utilities.sync_school_master_data(
                        profile_file, share_name, schema_name, schema_table.name, school_master_fields)
                except (HTTPError, DataError, ValueError) as ex:
                    logger.error('Exception caught for "{0}": {1}'.format(schema_table.name, str(ex)))
                except Exception as ex:
                    logger.error('Exception caught for "{0}": {1}'.format(schema_table.name, str(ex)))

        else:
            logger.error('School Master schema ({0}) does not exist to use for share ({1}).'.format(schema_name,
                                                                                                    share_name))
    else:
        logger.error('School Master share ({0}) does not exist to use.'.format(share_name))

    try:
        os.remove(profile_file)
    except OSError:
        pass


@app.task(soft_time_limit=10 * 55 * 60, time_limit=10 * 55 * 60)
def giga_meter_handle_published_school_master_data_row(*args, country_ids=None, force_tasks=False):
    """
    Background task to handle all the published rows of school master data source for Giga Meter Sync

    Execution Frequency: Every 4 hour
    """
    logger.info('Giga Meter - Handling the published school master data rows.')

    environment_map = {
        'urban': 'urban',
        'urbana': 'urban',
        'rural': 'rural',
    }

    true_choices = ['true', 'yes', '1']

    timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H')
    if force_tasks:
        timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H%M%S')

    if country_ids and len(country_ids) > 0:
        task_key = 'giga_meter_handle_published_school_master_data_row_status_{current_time}_country_ids_{ids}'.format(
            current_time=timestamp_str,
            ids='_'.join([str(c_id) for c_id in country_ids]),
        )
        task_description = 'Giga Meter - Handle published school master data rows for countries'
    else:
        task_key = 'giga_meter_handle_published_school_master_data_row_status_{current_time}'.format(
            current_time=timestamp_str)
        task_description = 'Giga Meter - Handle published school master data rows'

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, task_description)

    if task_instance:
        logger.debug('Not found running job for Giga Meter published rows handler task: {}'.format(task_key))

        new_published_records = giga_meter_models.GigaMeter_SchoolMasterData.objects.filter(
            status=giga_meter_models.GigaMeter_SchoolMasterData.ROW_STATUS_PUBLISHED,
        )

        if country_ids and len(country_ids) > 0:
            new_published_records = new_published_records.filter(country_id__in=country_ids)

        task_instance.info('Giga Meter - Total published records to update: {}'.format(new_published_records.count()))

        for data_chunk in core_utilities.queryset_iterator(new_published_records, chunk_size=100, print_msg=False):
            for row in data_chunk:
                try:
                    environment = row.school_area_type.lower() if not core_utilities.is_blank_string(
                        row.school_area_type) else ''
                    environment = environment_map.get(environment, '')

                    school, created = giga_meter_models.GigaMeter_School.objects.update_or_create(
                        giga_id_school=row.school_id_giga,
                        country=row.country,
                        defaults={
                            'name': row.school_name,
                            'country_code': row.country.code,
                            # 'timezone': row.timezone,
                            # 'geopoint': Point(x=row.longitude, y=row.latitude),
                            # 'gps_confidence': row.gps_confidence,
                            # 'altitude' : row.altitude,
                            # 'address': row.address,
                            # 'postal_code': row.postal_code,
                            # 'email': row.email,
                            'education_level': ''
                            if core_utilities.is_blank_string(row.education_level) else row.education_level,
                            'environment': environment,
                            'school_type': ''
                            if core_utilities.is_blank_string(row.school_funding_type) else row.school_funding_type,
                            'admin_1_name': row.admin1,
                            'admin_2_name': row.admin2,
                            # 'admin_3_name': row.admin_3_name,
                            # 'admin_4_name': row.admin_4_name,
                            'external_id': row.school_id_govt,
                            'name_lower': str(row.school_name).lower(),
                            # 'education_level_regional': row.education_level_regional,
                        },
                    )

                    school_static, school_static_created = giga_meter_models.GigaMeter_SchoolStatic.objects.update_or_create(
                        school=school,
                        version=row.version,
                        defaults={
                            'latitude': row.latitude,
                            'longitude': row.longitude,
                            'admin1_id_giga': row.admin1_id_giga,
                            'admin2_id_giga': row.admin2_id_giga,
                            'school_establishment_year': row.school_establishment_year,
                            'download_speed_contracted': row.download_speed_contracted,
                            'num_computers_desired': row.num_computers_desired,
                            'electricity_type': row.electricity_type,
                            'num_adm_personnel': row.num_adm_personnel,
                            'num_students': row.num_students,
                            'num_teachers' : row.num_teachers,
                            'num_classrooms' : row.num_classrooms,
                            'num_latrines' : row.num_latrines,
                            'water_availability' : None
                            if core_utilities.is_blank_string(row.water_availability)
                            else str(row.water_availability).lower() in true_choices,
                            'electricity_availability' : None
                            if core_utilities.is_blank_string(row.electricity_availability)
                            else str(row.electricity_availability).lower() in true_choices,
                            'computer_lab' : None
                            if core_utilities.is_blank_string(row.computer_lab)
                            else str(row.computer_lab).lower() in true_choices,
                            'num_computers' : row.num_computers,
                            'connectivity_govt': None
                            if core_utilities.is_blank_string(row.connectivity_govt)
                            else str(row.connectivity_govt).lower() in true_choices,
                            'connectivity_type_govt': row.connectivity_type_govt,
                            'connectivity_type':  row.connectivity_type,
                            'connectivity_type_root': row.connectivity_type_root,
                            'cellular_coverage_availability': None
                            if core_utilities.is_blank_string(row.cellular_coverage_availability)
                            else str(row.cellular_coverage_availability).lower() in true_choices,
                            'cellular_coverage_type': str(row.cellular_coverage_type).lower(),
                            'fiber_node_distance': row.fiber_node_distance,
                            'microwave_node_distance': row.microwave_node_distance,
                            'schools_within_1km': row.schools_within_1km,
                            'schools_within_2km': row.schools_within_2km,
                            'schools_within_3km': row.schools_within_3km,
                            'nearest_lte_distance': row.nearest_LTE_distance,
                            'nearest_umts_distance': row.nearest_UMTS_distance,
                            'nearest_gsm_distance': row.nearest_GSM_distance,
                            'nearest_nr_distance': row.nearest_NR_distance,
                            'pop_within_1km': row.pop_within_1km,
                            'pop_within_2km': row.pop_within_2km,
                            'pop_within_3km': row.pop_within_3km,
                            'school_data_source': row.school_data_source,
                            'school_data_collection_year': row.school_data_collection_year,
                            'school_data_collection_modality': row.school_data_collection_modality,
                            'school_location_ingestion_timestamp': row.school_location_ingestion_timestamp,
                            'connectivity_govt_ingestion_timestamp': row.connectivity_govt_ingestion_timestamp,
                            'connectivity_govt_collection_year': row.connectivity_govt_collection_year,
                            'disputed_region': str(row.disputed_region).lower() in true_choices,
                            'connectivity_rt': None if core_utilities.is_blank_string(
                                row.connectivity_RT) else str(row.connectivity_RT).lower() in true_choices,
                            'connectivity_rt_datasource': row.connectivity_RT_datasource,
                            'connectivity_rt_ingestion_timestamp': row.connectivity_RT_ingestion_timestamp,
                            'connectivity': None if core_utilities.is_blank_string(
                                row.connectivity_RT) else str(row.connectivity_RT).lower() in true_choices,
                            'download_speed_benchmark': row.download_speed_benchmark * 1000 * 1000
                            if row.download_speed_benchmark else None,
                            'computer_availability': None if core_utilities.is_blank_string(
                                row.computer_availability) else str(row.computer_availability).lower() in true_choices,
                            'num_students_girls': row.num_students_girls,
                            'num_students_boys': row.num_students_boys,
                            'num_students_other': row.num_students_other,
                            'num_teachers_female': row.num_teachers_female,
                            'num_teachers_male': row.num_teachers_male,
                            'teachers_trained': None if core_utilities.is_blank_string(
                                row.teachers_trained) else str(row.teachers_trained).lower() in true_choices,
                            'sustainable_business_model': None if core_utilities.is_blank_string(
                                row.sustainable_business_model) else str(
                                row.sustainable_business_model).lower() in true_choices,
                            'device_availability': None if core_utilities.is_blank_string(
                                row.device_availability) else str(row.device_availability).lower() in true_choices,
                            'num_tablets': row.num_tablets,
                            'num_robotic_equipment': row.num_robotic_equipment,
                            'building_id_govt': row.building_id_govt,
                            'num_schools_per_building': row.num_schools_per_building,
                        },
                    )

                    if school_static_created:
                        school.last_school_static = school_static
                        school.save(update_fields=['last_school_static',])

                    row.delete()

                except Exception as ex:
                    logger.error('Error reported on publishing: {0}'.format(ex))
                    logger.error('Record: {0}'.format(row.__dict__))
                    task_instance.info('Error reported for ID ({0}) on publishing: {1}'.format(row.id, ex))

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=10 * 55 * 60, time_limit=10 * 55 * 60)
def giga_meter_handle_deleted_school_master_data_row(*args, country_ids=None, force_tasks=False):
    """
    Background task to handle all the deleted rows of school master data source for Giga Meter DB

    Execution Frequency: Every day
    """
    logger.info('Giga Meter - Handling the deleted school master data rows.')
    timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H')
    if force_tasks:
        timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H%M%S')

    if country_ids and len(country_ids) > 0:
        task_key = 'giga_meter_handle_deleted_school_master_data_row_status_{current_time}_country_ids_{ids}'.format(
            current_time=timestamp_str,
            ids='_'.join([str(c_id) for c_id in country_ids]),
        )
        task_description = 'Giga Meter - Handle deleted school master data rows for countries'
    else:
        task_key = 'giga_meter_handle_deleted_school_master_data_row_status_{current_time}'.format(
            current_time=timestamp_str)
        task_description = 'Giga Meter - Handle deleted school master data rows'

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, task_description)

    if task_instance:
        logger.debug('Not found running job for deleted rows handler: {}'.format(task_key))
        new_deleted_records = giga_meter_models.GigaMeter_SchoolMasterData.objects.filter(
            status=giga_meter_models.GigaMeter_SchoolMasterData.ROW_STATUS_DELETED,
        )

        if country_ids and len(country_ids) > 0:
            new_deleted_records = new_deleted_records.filter(country_id__in=country_ids)

        current_date = core_utilities.get_current_datetime_object()
        task_instance.info('Total records to update: {}'.format(new_deleted_records.count()))

        for data_chunk in core_utilities.queryset_iterator(new_deleted_records, chunk_size=1000):
            for row in data_chunk:
                try:
                    school = giga_meter_models.GigaMeter_School.objects.filter(
                        giga_id_school=row.school_id_giga,
                        country=row.country,
                    ).first()

                    if school:
                        school.deleted = current_date
                        school.save(update_fields=['deleted',])

                    row.delete()
                except Exception as ex:
                    logger.error('Error reported on deletion: {0}'.format(ex))
                    logger.error('Record: {0}'.format(row.__dict__))
                    task_instance.info('Error reported for ID ({0}) on deletion: {1}'.format(row.id, ex))

        task_instance.info('Remaining records: {}'.format(new_deleted_records.count()))
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=6 * 60 * 60, time_limit=6 * 60 * 60)
def giga_meter_update_static_data(*args, country_iso3_format=None, force_tasks=False):
    """
    Background task to Get Static data to Giga Meter DB

    Execution Frequency: Once in a day
    """
    timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H')
    if force_tasks:
        timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H%M%S')

    task_key = 'giga_meter_update_static_data_status_{current_time}'.format(current_time=timestamp_str)
    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Giga Meter - Sync Static Data from School Master sources', check_previous=True)

    if task_instance:
        logger.debug('Not found running job for static data pull handler: {}'.format(task_key))
        giga_meter_load_data_from_school_master_apis(country_iso3_format=country_iso3_format)
        task_instance.info('Completed the load data from School Master API call')

        # Delete all the old records where more than 1 record for same School GIGA ID
        rows_with_more_than_1_record = giga_meter_models.GigaMeter_SchoolMasterData.objects.all().values(
            'school_id_giga', 'country_id').annotate(
            total_records=Count('school_id_giga', distinct=False),
        ).order_by('-total_records', 'school_id_giga', 'country_id').filter(total_records__gt=1)

        logger.debug('Queryset to get all the old records to delete where more than 1 record '
                     'for same School GIGA ID: {0}'.format(rows_with_more_than_1_record.query))

        for row in rows_with_more_than_1_record:
            for row_to_delete in giga_meter_models.GigaMeter_SchoolMasterData.objects.filter(
                school_id_giga=row['school_id_giga'],
                country_id=row['country_id'],
            ).order_by('-version', '-created')[1:]:
                row_to_delete.delete()
        task_instance.info('Deleted rows where more than 1 record for same School GIGA ID')

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=6 * 60 * 60, time_limit=6 * 60 * 60)
def handle_giga_meter_school_master_data_sync(*args):
    if settings.GIGA_METER_ENABLE_AUTO_SYNC:
        timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H')
        task_key = 'handle_giga_meter_school_master_data_sync_status_{current_time}'.format(current_time=timestamp_str)
        task_id = current_task.request.id or str(uuid.uuid4())
        task_instance = background_task_utilities.task_on_start(
            task_id, task_key, 'Giga Meter - Auto task to handle GigaMeter - School Master data sync',
            check_previous=True)

        if task_instance:
            logger.debug('Not found running job for data sync handler: {}'.format(task_key))
            giga_meter_update_static_data()
            task_instance.info('GigaMeter - School Master Data pull completed.')

            giga_meter_handle_published_school_master_data_row.s()
            task_instance.info('GigaMeter - School Master Data published record handle scheduled.')

            giga_meter_handle_deleted_school_master_data_row.s()
            task_instance.info('GigaMeter - School Master Data deleted record handle scheduled.')

            background_task_utilities.task_on_complete(task_instance)
        else:
            logger.error('Found Job with "{0}" name so skipping current iteration'.format(task_key))
    else:
        logger.warning('Giga Meter - School Master data synch disabled from config. '
                     'To enable it, please update "GIGA_METER_ENABLE_AUTO_SYNC" configuration.')
