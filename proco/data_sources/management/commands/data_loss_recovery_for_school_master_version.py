import json
import logging
import os

import delta_sharing
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.utils import DataError
from requests.exceptions import HTTPError

from proco.background.models import BackgroundTask
from proco.core.utils import get_current_datetime_object
from proco.data_sources import tasks as sources_tasks
from proco.data_sources import utils as sources_utilities
from proco.data_sources.models import SchoolMasterData
from proco.locations.models import Country
from proco.schools.models import School
from proco.utils import dates as date_utilities

logger = logging.getLogger('gigamaps.' + __name__)

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
    'school_master_profile_{dt}.share'.format(
        dt=date_utilities.format_date(get_current_datetime_object())
    )
)
open(profile_file, 'w').write(json.dumps(profile_json))


def sync_school_master_data(country, school_master_fields, pull_version):
    table_name = country.iso3_format
    # Create a url to access a shared table.
    # A table path is the profile file path following with `#` and the fully qualified name of a table
    # (`<share-name>.<schema-name>.<table-name>`).
    table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
        share_name=share_name,
        schema_name=schema_name,
        table_name=table_name,
    )
    logger.debug('Table URL: %s', table_url)

    loaded_data_df = delta_sharing.load_as_pandas(
        table_url,
        None,
        pull_version,
        None,
        None,
    )
    logger.debug('Total count of rows in the data: {0}'.format(len(loaded_data_df)))
    pull_datetime = get_current_datetime_object()

    if len(loaded_data_df) > 0:
        loaded_data_df.drop_duplicates(
            subset=['school_id_giga'],
            keep='last',
            inplace=True,
        )
        logger.debug('Total count of rows in the data after duplicate cleanup: {0}'.format(len(loaded_data_df)))

        df_columns = list(loaded_data_df.columns.tolist())
        cols_to_delete = list(set(df_columns) - set(school_master_fields)) + ['id', 'created', 'modified', 'school_id',
                                                                              'country_id', 'status',
                                                                              'modified_by', 'published_by',
                                                                              'published_at', 'is_read', ]
        logger.debug('All School Master API response columns: {}'.format(df_columns))
        logger.debug('All School Master API response columns to delete: {}'.format(
            list(set(df_columns) - set(school_master_fields))))

        insert_entries = []
        remove_entries = []

        loaded_data_df = sources_utilities.normalize_school_master_data_frame(loaded_data_df)

        loaded_data_df['version'] = pull_version
        loaded_data_df['country'] = country
        loaded_data_df['pulled_at'] = pull_datetime

        for _, row in loaded_data_df.iterrows():
            row.drop(
                labels=cols_to_delete,
                inplace=True,
                errors='ignore',
            )

            school = School.objects.filter(
                country=country,
                giga_id_school=row['school_id_giga'],
            ).first()

            # 1. If it is a new school, then it has to go through review process
            # 2. If it is an existing school, then check for required field if it has changed.
            # if changes, then only send for review otherwise publish it directly
            if school:
                row['school_id'] = school.id
                review_required = sources_utilities.has_changes_for_review(row, school)
                if not review_required:
                    row['status'] = SchoolMasterData.ROW_STATUS_PUBLISHED
                    row['published_at'] = get_current_datetime_object()

            row_as_dict = sources_utilities.parse_row(row)
            insert_entries.append(SchoolMasterData(**row_as_dict))

            if len(insert_entries) == 5000:
                logger.debug('Loading the data to "SchoolMasterData" table as it has reached 5000 benchmark.')
                SchoolMasterData.objects.bulk_create(insert_entries)
                insert_entries = []
                logger.debug('#' * 10)
                logger.debug('\n\n')

        logger.info('Loading the remaining ({0}) data to "SchoolMasterData" table.'.format(len(insert_entries)))
        if len(insert_entries) > 0:
            SchoolMasterData.objects.bulk_create(insert_entries)
    else:
        logger.info('No data to update in current table: {0}.'.format(table_name))


def load_data_from_school_master_apis(country, pull_version):
    """
    Background task which handles School Master Data source changes from APIs to GigaMaps DB

    Execution Frequency: Once in a week
    """
    logger.info('Starting loading the school master data from API to DB.')

    errors = []

    # Create a SharingClient.
    client = sources_utilities.ProcoSharingClient(profile_file)
    school_master_share = client.get_share(share_name)

    if school_master_share:
        school_master_schema = client.get_schema(school_master_share, schema_name)

        if school_master_schema:
            schema_tables = client.list_tables(school_master_schema)

            logger.debug('All tables ready to access: {0}'.format(schema_tables))

            school_master_fields = [f.name for f in SchoolMasterData._meta.get_fields()]

            for schema_table in schema_tables:
                logger.debug('#' * 10)
                logger.debug('Table: %s', schema_table)

                if country.iso3_format != schema_table.name:
                    continue

                if len(country_codes_for_exclusion) > 0 and schema_table.name in country_codes_for_exclusion:
                    logger.warning('Country with ISO3 Format ({0}) configured to exclude from School Master data pull. '
                                   'Hence skipping the load for this country code.'.format(schema_table.name))
                    continue

                try:
                    sync_school_master_data(country, school_master_fields, pull_version)
                    return
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


def get_latest_api_version(country):
    # Create a SharingClient.
    client = sources_utilities.ProcoSharingClient(profile_file)
    school_master_share = client.get_share(share_name)

    table_current_version = None

    if school_master_share:
        school_master_schema = client.get_schema(school_master_share, schema_name)

        if school_master_schema:
            schema_tables = client.list_tables(school_master_schema)
            logger.info('\nAll tables ready to access: {0}'.format(schema_tables))

            for schema_table in schema_tables:
                table_name = schema_table.name

                if country.iso3_format != table_name:
                    continue

                try:
                    # Create a url to access a shared table.
                    # A table path is the profile file path following with `#` and the fully qualified name of a table
                    # (`<share-name>.<schema-name>.<table-name>`).
                    table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
                        share_name=share_name,
                        schema_name=schema_name,
                        table_name=table_name,
                    )

                    table_current_version = delta_sharing.get_table_version(table_url)
                    logger.info(
                        'Country "{0}" current version from API: {1}\n'.format(table_name, table_current_version))
                except Exception as ex:
                    logger.error('Exception caught for "{0}": {1}\n'.format(table_name, str(ex)))

    return table_current_version


def clean_school_master_version_data(country, pull_version):
    SchoolMasterData.objects.filter(
        country=country,
        version=pull_version,
    ).delete()
    logger.info(
        f'Deleted all data_sources_schoolmasterdata table entries for country "{country}" with version "{pull_version}"')

    SchoolMasterData.history.model.objects.filter(
        country=country,
        version=pull_version,
    ).delete()
    logger.info(
        f'Deleted all data_sources_historicalschoolmasterdata table entries for country "{country}" with version "{pull_version}"')


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--check_latest_version', action='store_true', dest='check_latest_version', default=False,
            help='If provided, latest version will be identified from API.'
        )

        parser.add_argument(
            '-country_code', dest='country_iso3_format', type=str,
            required=True,
            help='Valid country ISO3 format code. For eg. for Brazil - BRA'
        )

        parser.add_argument(
            '--pull_data', action='store_true', dest='pull_data', default=False,
            help='If provided, data will be fetched from QoS API with given start and end version.'
        )

        parser.add_argument(
            '-pull_version', dest='pull_version', type=int,
            help='Start version to fetch the data. It should be a valid version value in integer format.'
        )

        parser.add_argument(
            '--schedule', action='store_true', dest='schedule_tasks', default=False,
            help='If provided, it will schedule the task on Celery.'
        )

    def handle(self, **options):
        logger.info('Executing data loss recovery for SchoolMasterData for given/latest version" ....\n')

        country_iso3_format = options.get('country_iso3_format')
        country = Country.objects.filter(iso3_format=country_iso3_format).first()
        logger.info('Country object: {0}'.format(country))

        if not country:
            logger.error('Country with ISO3 format ({0}) not found in giga-maps db. '
                         'Hence stopping the load.'.format(country_iso3_format))
            exit(0)

        schedule_tasks = options.get('schedule_tasks')

        if schedule_tasks:
            sources_tasks.scheduler_for_data_loss_recovery_for_school_master_version.delay(
                country_iso3_format,
                options.get('pull_data'),
                options.get('pull_version')
            )
            logger.info('Completed scheduling the data loss recovery for school master successfully.\n')
            exit(0)

        check_latest_version = options.get('check_latest_version')
        if check_latest_version:
            logger.info('\nLatest version from SchoolMaster API of country: {0}, is: {1}'.format(
                country,
                get_latest_api_version(country)
            ))

            logger.info('Checking the latest version action completed successfully.\n')

        pull_data = options.get('pull_data')
        if pull_data:
            pull_version = options.get('pull_version')
            if not pull_version:
                pull_version = get_latest_api_version(country)

            clean_school_master_version_data(country, pull_version)
            logger.info(f'Deleted SchoolMaster records for version {pull_version} successfully.\n')

            load_data_from_school_master_apis(country, pull_version)
            logger.info(f'SchoolMaster data load for version {pull_version} completed successfully.\n')

            latest_records = SchoolMasterData.objects.filter(
                country=country,
                version__isnull=False,
            ).order_by('-version').first()
            SchoolMasterData.set_last_version(latest_records.version, country.iso3_format)
            logger.info(f'SchoolMaster data version updated in cache.\n')

            BackgroundTask.objects.filter(name='cleanup_school_master_rows_status_{current_time}'.format(
                current_time=date_utilities.format_date(get_current_datetime_object(), frmt='%d%m%Y_%H'))
            ).update(deleted=get_current_datetime_object())
            sources_tasks.cleanup_school_master_rows()
            logger.info(f'SchoolMaster data clean up completed successfully.\n')

            sources_tasks.handle_published_school_master_data_row(country_ids=[country.id, ])
            logger.info(f'SchoolMaster data publishing completed successfully.\n')

            sources_tasks.handle_deleted_school_master_data_row(country_ids=[country.id, ])
            logger.info(f'SchoolMaster data publish for deleted schools completed successfully.\n')

        try:
            os.remove(profile_file)
        except OSError:
            pass

        logger.info('Completed data loss recovery for School Master successfully.\n')
