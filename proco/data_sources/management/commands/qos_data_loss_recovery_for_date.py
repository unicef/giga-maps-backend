import json
import os
import logging
from datetime import timedelta

import delta_sharing
from delta_sharing.reader import DeltaSharingReader
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Avg

from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import RealTimeConnectivity
from proco.core.utils import get_current_datetime_object, bulk_create_or_update
from proco.data_sources import utils as sources_utilities
from proco.data_sources.models import QoSData
from proco.data_sources.tasks import finalize_previous_day_data
from proco.locations.models import Country
from proco.schools.models import School
from proco.utils import dates as date_utilities

logger = logging.getLogger('gigamaps.' + __name__)

ds_settings = settings.DATA_SOURCE_CONFIG

today_date = get_current_datetime_object().date()

qos_ds_settings = ds_settings.get('QOS')

share_name = qos_ds_settings['SHARE_NAME']
schema_name = qos_ds_settings['SCHEMA_NAME']

profile_json = {
    'shareCredentialsVersion': qos_ds_settings.get('SHARE_CREDENTIALS_VERSION', 1),
    'endpoint': qos_ds_settings.get('ENDPOINT'),
    'bearerToken': qos_ds_settings.get('BEARER_TOKEN'),
    'expirationTime': qos_ds_settings.get('EXPIRATION_TIME')
}

profile_file = os.path.join(
    settings.BASE_DIR,
    'qos_profile_file_{dt}.share'.format(dt=date_utilities.format_date(today_date))
)
open(profile_file, 'w').write(json.dumps(profile_json))


def load_qos_data_source_response_to_model(version_number, country):
    # Create a SharingClient.
    client = sources_utilities.ProcoSharingClient(profile_file)
    qos_share = client.get_share(share_name)

    changes_for_countries = {}

    if qos_share:
        qos_schema = client.get_schema(qos_share, schema_name)

        if qos_schema:
            schema_tables = client.list_tables(qos_schema)

            qos_model_fields = [f.name for f in QoSData._meta.get_fields()]

            for schema_table in schema_tables:
                table_name = schema_table.name

                if country.iso3_format != table_name:
                    continue

                logger.info('#' * 10)
                try:
                    if QoSData.objects.all().filter(
                        country=country,
                        version=version_number,
                    ).exists():
                        logger.warning('QoSData table has given version data already in the table. '
                                       'To re collect, please clean this version data first then retry again.'
                                       'Country code: {0}, \t\tVersion: {1}'.format(table_name, version_number))
                        continue

                    logger.info(
                        'Current version data not available in the table. Hence fetching the data from QoS api.')

                    # Create an url to access a shared table.
                    # A table path is the profile file path following with `#` and the fully qualified name of a table
                    # (`<share-name>.<schema-name>.<table-name>`).
                    table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
                        share_name=share_name,
                        schema_name=schema_name,
                        table_name=table_name,
                    )

                    api_current_version = delta_sharing.get_table_version(table_url)
                    logger.info('Current version from api: {0}'.format(api_current_version))

                    if version_number > api_current_version:
                        logger.error('Given version must not be higher then latest api version. '
                                     'Hence skipping current data pull.')
                        exit(0)

                    loaded_data_df = delta_sharing.load_table_changes_as_pandas(
                        table_url,
                        version_number,
                        version_number,
                        None,
                        None,
                    )
                    logger.info('Total count of rows in the {0} version data: {1}'.format(
                        version_number, len(loaded_data_df)))

                    loaded_data_df = loaded_data_df[loaded_data_df[DeltaSharingReader._change_type_col_name()].isin(
                        ['insert', 'update_postimage'])]
                    logger.info('Total count of rows after filtering only ["insert", "update_postimage"] in the "{0}" '
                                'version data: {1}'.format(version_number, len(loaded_data_df)))

                    if len(loaded_data_df) > 0:
                        insert_entries = []

                        changes_for_countries[table_name] = True

                        df_columns = list(loaded_data_df.columns.tolist())
                        cols_to_delete = list(set(df_columns) - set(qos_model_fields)) + ['id', 'created',
                                                                                          'modified', 'school_id',
                                                                                          'country_id',
                                                                                          'modified_by', ]
                        logger.info('All QoS api response columns: {}'.format(df_columns))
                        logger.info('All QoS api response columns to delete: {}'.format(
                            list(set(df_columns) - set(qos_model_fields))))

                        loaded_data_df.drop(columns=cols_to_delete, inplace=True, errors='ignore', )

                        loaded_data_df = sources_utilities.normalize_qos_data_frame(loaded_data_df)

                        loaded_data_df['version'] = version_number
                        loaded_data_df['country'] = country

                        for _, row in loaded_data_df.iterrows():
                            school = School.objects.filter(
                                country=country, giga_id_school=row['school_id_giga'],
                            ).first()

                            if not school:
                                logger.error('School with giga ID ({0}) not found in proco db. Hence skipping the '
                                             'load for current school.'.format(row['school_id_giga']))
                                continue

                            row['school'] = school

                            row_as_dict = sources_utilities.parse_row(row)
                            duplicate_higher_version_records = QoSData.objects.filter(
                                school_id=school.id,
                                timestamp=row_as_dict['timestamp'],
                                version__gt=version_number,
                            )
                            if duplicate_higher_version_records.exists():
                                logger.error('Higher version for same school ID and timestamp already exists. '
                                             'Hence skipping the update for current row.')
                                qos_instance = duplicate_higher_version_records.first()
                                logger.info('School ID: {0},\tTimestamp: {1},\tCurrent Version: {2},\t'
                                            'Higher Version: {3}'.format(qos_instance.school_id,
                                                                         qos_instance.timestamp,
                                                                         version_number, qos_instance.version))
                                continue

                            insert_entries.append(row_as_dict)

                            if len(insert_entries) == 5000:
                                logger.info('Loading the data to "QoSData" table as it has reached 5000 benchmark.')
                                bulk_create_or_update(insert_entries, QoSData, ['school', 'timestamp'])
                                insert_entries = []
                                logger.info('#\n' * 10)

                        logger.info('Loading the remaining ({0}) data to "QoSData" table.'.format(len(insert_entries)))
                        if len(insert_entries) > 0:
                            bulk_create_or_update(insert_entries, QoSData, ['school', 'timestamp'])
                    else:
                        logger.info('No data to update in current table: {0}.'.format(table_name))
                except Exception as ex:
                    logger.error('Exception caught for "{0}": {1}'.format(schema_table.name, str(ex)))
        else:
            logger.error('QoS schema ({0}) does not exist to use for share ({1}).'.format(schema_name, share_name))
            exit(0)
    else:
        logger.error('QoS share ({0}) does not exist to use.'.format(share_name))
        exit(0)


def sync_qos_realtime_data(date, country):
    qos_measurements = QoSData.objects.filter(
        timestamp__date=date,
        country=country,
    ).values(
        'school'
    ).annotate(
        download_avg=Avg('speed_download'),
        latency_avg=Avg('latency'),
        upload_avg=Avg('speed_upload'),
        roundtrip_time_avg=Avg('roundtrip_time'),
        jitter_download_avg=Avg('jitter_download'),
        jitter_upload_avg=Avg('jitter_upload'),
        rtt_packet_loss_pct_avg=Avg('rtt_packet_loss_pct'),
        speed_download_probe_avg=Avg('speed_download_probe'),
        speed_upload_probe_avg=Avg('speed_upload_probe'),
        latency_probe_avg=Avg('latency_probe'),
        speed_download_mean_avg=Avg('speed_download_mean'),
        speed_upload_mean_avg=Avg('speed_upload_mean'),
    ).order_by('school')

    if not qos_measurements.exists():
        logger.info('No records to aggregate on provided date: "{0}". Hence skipping for the given date.'.format(date))
        return

    logger.info('Migrating the records from "QoSData" to "RealTimeConnectivity" with date: {0} '.format(date))

    realtime = []

    for qos_measurement in qos_measurements:
        connectivity_speed = qos_measurement.get('download_avg')
        if connectivity_speed:
            # convert Mbps to bps
            connectivity_speed = connectivity_speed * 1000 * 1000

        connectivity_upload_speed = qos_measurement.get('upload_avg')
        if connectivity_upload_speed:
            # convert Mbps to bps
            connectivity_upload_speed = connectivity_upload_speed * 1000 * 1000

        connectivity_speed_probe = qos_measurement.get('speed_download_probe_avg')
        if connectivity_speed_probe:
            # convert Mbps to bps
            connectivity_speed_probe = connectivity_speed_probe * 1000 * 1000

        connectivity_upload_speed_probe = qos_measurement.get('speed_upload_probe_avg')
        if connectivity_upload_speed_probe:
            # convert Mbps to bps
            connectivity_upload_speed_probe = connectivity_upload_speed_probe * 1000 * 1000

        connectivity_speed_mean = qos_measurement.get('speed_download_mean_avg')
        if connectivity_speed_mean:
            # convert Mbps to bps
            connectivity_speed_mean = connectivity_speed_mean * 1000 * 1000

        connectivity_upload_speed_mean = qos_measurement.get('speed_upload_mean_avg')
        if connectivity_upload_speed_mean:
            # convert Mbps to bps
            connectivity_upload_speed_mean = connectivity_upload_speed_mean * 1000 * 1000

        realtime.append(RealTimeConnectivity(
            created=date,
            connectivity_speed=connectivity_speed,
            connectivity_upload_speed=connectivity_upload_speed,
            connectivity_latency=qos_measurement.get('latency_avg'),
            roundtrip_time=qos_measurement.get('roundtrip_time_avg'),
            jitter_download=qos_measurement.get('jitter_download_avg'),
            jitter_upload=qos_measurement.get('jitter_upload_avg'),
            rtt_packet_loss_pct=qos_measurement.get('rtt_packet_loss_pct_avg'),
            connectivity_speed_probe=connectivity_speed_probe,
            connectivity_upload_speed_probe=connectivity_upload_speed_probe,
            connectivity_latency_probe=qos_measurement.get('latency_probe_avg'),
            connectivity_speed_mean=connectivity_speed_mean,
            connectivity_upload_speed_mean=connectivity_upload_speed_mean,
            school_id=qos_measurement.get('school'),
            live_data_source=statistics_configs.QOS_SOURCE,
        ))

        if len(realtime) == 5000:
            logger.info('Loading the data to "RealTimeConnectivity" table as it has reached 5000 benchmark.')
            RealTimeConnectivity.objects.bulk_create(realtime)
            realtime = []

    logger.info('Loading the remaining ({0}) data to "RealTimeConnectivity" table.'.format(len(realtime)))
    if len(realtime) > 0:
        RealTimeConnectivity.objects.bulk_create(realtime)


def get_latest_api_version(country_code=None):
    # Create a SharingClient.
    client = sources_utilities.ProcoSharingClient(profile_file)
    qos_share = client.get_share(share_name)

    version_for_countries = {}

    if qos_share:
        qos_schema = client.get_schema(qos_share, schema_name)

        if qos_schema:
            schema_tables = client.list_tables(qos_schema)
            logger.info('\nAll tables ready to access: {0}'.format(schema_tables))

            for schema_table in schema_tables:
                table_name = schema_table.name

                if country_code and country_code != table_name:
                    continue

                try:
                    # Create an url to access a shared table.
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

                    version_for_countries[table_name] = table_current_version
                except Exception as ex:
                    logger.error('Exception caught for "{0}": {1}\n'.format(table_name, str(ex)))

    return version_for_countries


def check_missing_dates_from_table(country, dates):
    qos_version_qry = QoSData.objects.all()

    if country:
        qos_version_qry = qos_version_qry.filter(country__iso3_format=country.iso3_format)

    qos_version_qry = qos_version_qry.values(
        'country__iso3_format', 'timestamp__date'
    ).order_by('country__iso3_format', 'timestamp__date').distinct('country__iso3_format', 'timestamp__date')

    db_dates_for_countries = {}
    for qry_data in qos_version_qry:
        country_iso3_format = qry_data.get('country__iso3_format')
        date_list = db_dates_for_countries.get(country_iso3_format, [])
        date_list.append(qry_data.get('timestamp__date'))
        db_dates_for_countries[country_iso3_format] = date_list


    for country_iso3_code, available_dates in db_dates_for_countries.items():
        missing_dates = list(set(dates) - set(list(available_dates)))

        if len(missing_dates) > 0:
            logger.info('Missing dates are between {0} - {1}: '.format(dates[0], dates[-1]))

            for missing_date in sorted(missing_dates):
                # print missing date in string format
                logger.info(date_utilities.format_date(missing_date))


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument(
            '-country_code', dest='country_iso3_format', type=str,
            default=None,
            help='Valid country ISO3 format code. For eg. for Brazil - BRA'
        )

        parser.add_argument(
            '-start_date', dest='start_date', type=str,
            default=date_utilities.format_date(today_date - timedelta(days=30)),  # 30 days from today
            help='Date from we need to check the missing data. Default is 30 days from Current date.'
        )

        parser.add_argument(
            '-end_date', dest='end_date', type=str,
            default=date_utilities.format_date(today_date),  # current date
            help='Date till we need to check the missing data. Default is today date.'
        )

        parser.add_argument(
            '--check_missing_dates', action='store_true', dest='check_missing_dates', default=False,
            help='List down all the missing dates for qOs live data source.'
        )

        parser.add_argument(
            '--pull_data', action='store_true', dest='pull_data', default=False,
            help='Pull the QoS live data for specified date.'
        )

        parser.add_argument(
            '--aggregate', action='store_true', dest='aggregate_data', default=False,
            help='If provided, aggregation on QoS data will be performed and all the related tables will be updated.'
        )

    def handle(self, **options):
        logger.info('Executing data loss recovery for QoS for given dates" ....\n')

        country_iso3_format = options.get('country_iso3_format')
        country = None
        if country_iso3_format:
            country = Country.objects.filter(iso3_format=country_iso3_format).first()
            logger.info('Country object: {0}'.format(country))

            if not country:
                logger.error('Country with ISO3 format ({0}) not found in DB "locations_country". '
                             'Hence stopping the load.'.format(country_iso3_format))
                exit(0)

        check_missing_dates = options.get('check_missing_dates')
        start_date = date_utilities.to_date(options.get('start_date'))
        end_date = date_utilities.to_date(options.get('end_date'))

        if start_date > end_date:
            logger.error('Start date value can not be greater than end_date.')
            exit(0)

        date_list = sorted([(start_date + timedelta(days=x)).date() for x in range((end_date - start_date).days)] + [
            end_date.date()])

        if check_missing_dates:
            logger.info('\nChecking the missing versions.')
            check_missing_dates_from_table(country, date_list)
            logger.info('Checking the missing versions action completed successfully.\n')

        pull_data = options.get('pull_data')
        if pull_data:
            if len(date_list) > 0:
                logger.info('\nLoading the api data to "data_sources_qosdata" table ***\n')
                for data_pull_date in date_list:
                    load_qos_data_source_response_to_model(data_pull_date, country)
                logger.info('\nData load completed successfully.\n')
            else:
                logger.error('Please provide valid required parameters as:'
                             ' -start_date=<START_DATE> -end_date=<END_DATE>\n')
                exit(0)

        try:
            os.remove(profile_file)
        except OSError:
            pass

        aggregate_data = options.get('aggregate_data')
        if aggregate_data:
            if len(date_list) > 0:
                for pull_data_date in date_list:
                    logger.info(
                        '\nSyncing the "data_sources_qosdata" data to "connection_statistics_realtimeconnectivity" '
                        'for date: {0}'.format(pull_data_date))
                    sync_qos_realtime_data(pull_data_date, country)
                    logger.info('Data synced successfully.\n\n')

                    logger.info('Starting finalizing the records to actual proco tables.')
                    monday_date = pull_data_date - timedelta(days=pull_data_date.weekday())
                    monday_week_no = date_utilities.get_week_from_date(monday_date)
                    monday_year = date_utilities.get_year_from_date(monday_date)
                    logger.info('Weekly record details. \tWeek No: {0}\tYear: {1}'.format(monday_week_no, monday_year))

                    logger.info('\n\nFinalizing the records for country ID: {0}'.format(country.id))
                    finalize_previous_day_data(None, country.id, pull_data_date)
                    logger.info('Finalized records successfully to actual proco tables.\n\n')
            else:
                logger.error('Please pass required parameters as:'
                             ' -pull_start_version=<VERSION-NUMBER> -pull_end_version=<VERSION-NUMBER>')

        logger.info('Completed data loss recovery for qos successfully.\n')
        exit(0)
