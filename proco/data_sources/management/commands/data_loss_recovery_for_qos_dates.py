import json
import logging
import os
from datetime import timedelta

import delta_sharing
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db.models import Avg

from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import RealTimeConnectivity
from proco.connection_statistics.utils import (
    aggregate_real_time_data_to_school_daily_status,
    aggregate_school_daily_status_to_school_weekly_status,
    aggregate_school_daily_to_country_daily,
    update_country_weekly_status,
)
from proco.core.utils import get_current_datetime_object, bulk_create_or_update
from proco.data_sources import utils as sources_utilities
from proco.data_sources import tasks as sources_tasks
from proco.data_sources.models import QoSData
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
    'qos_profile_file_for_data_pull_{dt}.share'.format(dt=date_utilities.format_date(today_date))
)
open(profile_file, 'w').write(json.dumps(profile_json))


def load_qos_data_source_response_to_model(country, data_pull_date):
    qos_model_fields = [f.name for f in QoSData._meta.get_fields()]
    table_name = country.iso3_format

    logger.info('#' * 10)

    if QoSData.objects.all().filter(
        country=country,
        timestamp__date=data_pull_date,
    ).exists():
        logger.warning('QoSData table has given date data already in the table. '
                       'To re collect, please clean this date data first then retry again.'
                       'Country code: {0}, \t\tDate: {1}'.format(
            table_name, date_utilities.format_date(data_pull_date)))
        return

    # Create a url to access a shared table.
    # A table path is the profile file path following with `#` and the fully qualified name of a table
    # (`<share-name>.<schema-name>.<table-name>`).
    table_url = profile_file + "#{share_name}.{schema_name}.{table_name}".format(
        share_name=share_name,
        schema_name=schema_name,
        table_name=table_name,
    )

    predicate = {
        "op": "equal",
        "children": [
            {"op": "column", "name": "date", "valueType": "date"},
            {"op": "literal", "value": date_utilities.format_date(data_pull_date, frmt='%Y-%m-%d'), "valueType": "date"}
        ]
    }

    try:
        loaded_data_df = delta_sharing.load_as_pandas(
            table_url,
            jsonPredicateHints=json.dumps(predicate)
        )
        logger.info('Total count of rows in the "{0}" date data: {1}'.format(
            date_utilities.format_date(data_pull_date),
            len(loaded_data_df))
        )
        pull_datetime = get_current_datetime_object()

        if len(loaded_data_df) > 0:
            insert_entries = []
            unregistered_school_giga_ids = []

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

            loaded_data_df['version'] = None
            loaded_data_df['country'] = country
            loaded_data_df['pulled_at'] = pull_datetime

            for _, row in loaded_data_df.iterrows():
                school = School.objects.filter(country=country, giga_id_school=row['school_id_giga']).first()

                if not school:
                    unregistered_school_giga_ids.append(row['school_id_giga'])
                    continue

                row['school'] = school

                row_as_dict = sources_utilities.parse_row(row)
                insert_entries.append(row_as_dict)

                if len(insert_entries) == 5000:
                    logger.info('Loading the data to "QoSData" table as it has reached 5000 benchmark.')
                    bulk_create_or_update(insert_entries, QoSData, ['school', 'timestamp'])
                    if len(unregistered_school_giga_ids) > 0:
                        logger.error('School with giga IDs not found in DB. Hence skipping the '
                                     'load for these schools at batch: {0}'.format(', '.join(unregistered_school_giga_ids)))

                    insert_entries = []
                    unregistered_school_giga_ids = []
                    logger.info('#\n' * 2)

            logger.info('Loading the remaining ({0}) data to "QoSData" table.'.format(len(insert_entries)))
            if len(insert_entries) > 0:
                bulk_create_or_update(insert_entries, QoSData, ['school', 'timestamp'])

                if len(unregistered_school_giga_ids) > 0:
                    logger.error('School with giga IDs not found in DB. Hence skipping the '
                                 'load for these school at last: {0}'.format(', '.join(unregistered_school_giga_ids)))
        else:
            logger.info('No data to update in current table: {0}.'.format(table_name))
    except Exception as ex:
        logger.error('Exception caught for "{0}": {1}'.format(table_name, str(ex)))


def sync_qos_realtime_data(country, aggr_date):
    qos_measurements = QoSData.objects.filter(
        timestamp__date=aggr_date,
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
        speed_download_max_avg=Avg('speed_download_max'),
        speed_upload_max_avg=Avg('speed_upload_max'),
        pe_ingress_avg=Avg('pe_ingress'),
        pe_egress_avg=Avg('pe_egress'),
        inbound_traffic_sum_avg=Avg('inbound_traffic_sum'),
        outbound_traffic_sum_avg=Avg('outbound_traffic_sum'),
        latency_min_avg=Avg('latency_min'),
        latency_mean_avg=Avg('latency_mean'),
        latency_max_avg=Avg('latency_max'),
        signal_mean_avg=Avg('signal_mean'),
        signal_max_avg=Avg('signal_max'),
        is_connected_all_avg=Avg('is_connected_all'),
        is_connected_true_avg=Avg('is_connected_true'),
    ).order_by('school')

    if not qos_measurements.exists():
        logger.info('No records to aggregate on provided date: "{0}". Hence skipping for the given date.'.format(
            date_utilities.format_date(aggr_date)))
        return

    logger.info('Migrating the records from "QoSData" to "RealTimeConnectivity" with date: {0} '.format(
        date_utilities.format_date(aggr_date)))

    realtime = []
    # convert Mbps to bps
    fields_for_mb_conversion = [
        'download_avg',
        'upload_avg',
        'speed_download_probe_avg',
        'speed_upload_probe_avg',
        'speed_download_mean_avg',
        'speed_upload_mean_avg',
        'speed_download_max_avg',
        'speed_upload_max_avg',
        'pe_ingress_avg',
        'pe_egress_avg',
        'inbound_traffic_sum_avg',
        'outbound_traffic_sum_avg',
    ]

    for qos_measurement in qos_measurements:

        for field_name in fields_for_mb_conversion:
            if qos_measurement.get(field_name):
                qos_measurement[field_name] = qos_measurement[field_name] * 1000 * 1000

        realtime.append(RealTimeConnectivity(
            created=aggr_date,
            connectivity_speed=qos_measurement.get('download_avg'),
            connectivity_upload_speed=qos_measurement.get('upload_avg'),
            connectivity_latency=qos_measurement.get('latency_avg'),
            roundtrip_time=qos_measurement.get('roundtrip_time_avg'),
            jitter_download=qos_measurement.get('jitter_download_avg'),
            jitter_upload=qos_measurement.get('jitter_upload_avg'),
            rtt_packet_loss_pct=qos_measurement.get('rtt_packet_loss_pct_avg'),
            connectivity_speed_probe=qos_measurement.get('speed_download_probe_avg'),
            connectivity_upload_speed_probe=qos_measurement.get('speed_upload_probe_avg'),
            connectivity_latency_probe=qos_measurement.get('latency_probe_avg'),
            connectivity_speed_mean=qos_measurement.get('speed_download_mean_avg'),
            connectivity_upload_speed_mean=qos_measurement.get('speed_upload_mean_avg'),
            speed_download_max=qos_measurement.get('speed_download_max_avg'),
            speed_upload_max=qos_measurement.get('speed_upload_max_avg'),
            pe_ingress=qos_measurement.get('pe_ingress_avg'),
            pe_egress=qos_measurement.get('pe_egress_avg'),
            inbound_traffic_sum=qos_measurement.get('inbound_traffic_sum_avg'),
            outbound_traffic_sum=qos_measurement.get('outbound_traffic_sum_avg'),
            latency_min=qos_measurement.get('latency_min_avg'),
            latency_mean=qos_measurement.get('latency_mean_avg'),
            latency_max=qos_measurement.get('latency_max_avg'),
            signal_mean=qos_measurement.get('signal_mean_avg'),
            signal_max=qos_measurement.get('signal_max_avg'),
            is_connected_all=qos_measurement.get('is_connected_all_avg'),
            is_connected_true=qos_measurement.get('is_connected_true_avg'),
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


def check_missing_dates_from_table(country, dates):
    qos_version_qry = QoSData.objects.filter(
        country__iso3_format=country.iso3_format,
    ).values(
        'country__iso3_format', 'timestamp__date',
    ).order_by(
        'country__iso3_format', 'timestamp__date',
    ).distinct(
        'country__iso3_format', 'timestamp__date',
    )

    db_dates_for_countries = {}
    for qry_data in qos_version_qry:
        country_iso3_format = qry_data.get('country__iso3_format')
        date_list = db_dates_for_countries.get(country_iso3_format, [])
        date_list.append(qry_data.get('timestamp__date'))
        db_dates_for_countries[country_iso3_format] = date_list

    if len(db_dates_for_countries) > 0:
        for country_iso3_code, available_dates in db_dates_for_countries.items():
            missing_dates = list(set(dates) - set(list(available_dates)))

            if len(missing_dates) > 0:
                logger.info('Missing dates between "{0}" to "{1}" are: {2}'.format(
                    date_utilities.format_date(dates[0]),
                    date_utilities.format_date(dates[-1]),
                    ', '.join([date_utilities.format_date(missing_date) for missing_date in sorted(missing_dates)])))
    else:
        logger.info('All provided dates are missing for country "{0}". 0 records in table for this date range.'
                    ''.format(country))

def delete_qos_realtime_data(country, date):
    RealTimeConnectivity.objects.filter(
        created__date=date,
        live_data_source=statistics_configs.QOS_SOURCE,
        school__country=country,
    ).delete()

    QoSData.objects.filter(timestamp__date=date, school__country=country).delete()


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

        parser.add_argument(
            '--schedule', action='store_true', dest='schedule_tasks', default=False,
            help='If provided, it will schedule the task on Celery.'
        )

    def handle(self, **options):
        logger.info('Executing data loss recovery for QoS for given dates" ....\n')
        schedule_tasks = options.get('schedule_tasks')

        if schedule_tasks:
            sources_tasks.scheduler_for_data_loss_recovery_for_qos_dates.delay(
                options.get('country_iso3_format'),
                options.get('start_date'),
                options.get('end_date'),
                options.get('check_missing_dates'),
                options.get('pull_data'),
                options.get('aggregate_data')
            )
            logger.info('Completed scheduling the data loss recovery for qos successfully.\n')
            exit(0)

        country_iso3_format = options.get('country_iso3_format')
        countries = []
        if country_iso3_format:
            country_obj = Country.objects.filter(iso3_format=country_iso3_format).first()
            if not country_obj:
                logger.error('Country with ISO3 format ({0}) not found in DB "locations_country". '
                             'Hence stopping the load.'.format(country_iso3_format))
                exit(0)

            countries.append(country_obj)
        else:
            # Create a SharingClient.
            client = sources_utilities.ProcoSharingClient(profile_file)
            qos_share = client.get_share(share_name)
            qos_schema = client.get_schema(qos_share, schema_name)
            schema_tables = client.list_tables(qos_schema)

            for schema_table in schema_tables:
                country_obj = Country.objects.filter(iso3_format=schema_table.name).first()
                if not country_obj:
                    logger.error('Country with ISO3 format ({0}) not found in DB "locations_country". '
                                 'Hence skipping this country.'.format(schema_table.name))
                    continue

                countries.append(country_obj)

        logger.info('Country objects: {0}'.format(countries))
        if len(countries) == 0:
            exit(0)

        start_date = date_utilities.to_date(options.get('start_date'))
        end_date = date_utilities.to_date(options.get('end_date'))

        if start_date > end_date:
            logger.error('Start date value can not be greater than end_date.')
            exit(0)

        date_list = sorted([(start_date + timedelta(days=x)).date() for x in range((end_date - start_date).days)] + [
            end_date.date()])
        if len(date_list) == 0:
            logger.error('Please provide valid required parameters as: -start_date=<START_DATE> -end_date=<END_DATE>')
            exit(0)

        for country in countries:
            logger.info('############ Process started for country: "{}" ############'.format(country))

            check_missing_dates = options.get('check_missing_dates')
            if check_missing_dates:
                logger.info('\nChecking the missing dates.')
                check_missing_dates_from_table(country, date_list)
                logger.info('Checking the missing date action completed successfully.\n')

            pull_data = options.get('pull_data')
            if pull_data:
                logger.info('\nLoading the api data to "data_sources_qosdata" table ***\n')

                for data_pull_date in date_list:
                    logger.info('Deleting QoS data for date: {0}'.format(date_utilities.format_date(data_pull_date)))
                    delete_qos_realtime_data(country, data_pull_date)
                    logger.info('Data deleted successfully.\n\n')

                    logger.info('Syncing the QoS api data to QoSData table for date: "{0}"'.format(
                        date_utilities.format_date(data_pull_date)))
                    load_qos_data_source_response_to_model(country, data_pull_date)
                    logger.info('Data synced successfully for date "{0}".\n\n'.format(
                        date_utilities.format_date(data_pull_date)))

                logger.info('\nData load completed successfully for all dates.\n')


            aggregate_data = options.get('aggregate_data')
            if aggregate_data:
                week_no_with_week_date_mapping = {}

                for aggr_date in date_list:
                    logger.info('\nSyncing the "data_sources_qosdata" data to '
                                '"connection_statistics_realtimeconnectivity" for country: "{0}" '
                                'and date: "{1}"'.format(country, date_utilities.format_date(aggr_date)))
                    sync_qos_realtime_data(country, aggr_date)
                    logger.info('Data synced successfully.\n\n')

                    aggr_date_week_no = date_utilities.get_week_from_date(aggr_date)
                    week_no_with_week_date_mapping[aggr_date_week_no] = aggr_date

                logger.info('Starting finalizing the records to Daily, Weekly tables.')
                for aggregate_daily_date in date_list:
                    aggregate_real_time_data_to_school_daily_status(country, aggregate_daily_date)
                    aggregate_school_daily_to_country_daily(country, aggregate_daily_date)

                for week_no, any_week_date in week_no_with_week_date_mapping.items():
                    year = date_utilities.get_year_from_date(any_week_date)
                    logger.info('Weekly record details. \tWeek No: {0}\tYear: {1}'.format(week_no, year))

                    weekly_data_available = aggregate_school_daily_status_to_school_weekly_status(
                        country, any_week_date)
                    if weekly_data_available:
                        update_country_weekly_status(country, any_week_date)
                    logger.info('Finalized records successfully to actual DB tables.\n\n')

                cmd_args = ['--reset', f'-country_id={country.id}']
                call_command('populate_school_registration_data', *cmd_args)

                country.invalidate_country_related_cache()

        try:
            os.remove(profile_file)
        except OSError:
            pass

        logger.info('Completed data loss recovery for qos successfully.\n')
