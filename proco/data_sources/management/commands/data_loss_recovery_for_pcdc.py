from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Avg
from django.db.models import F
from django.db.models import Q

from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import RealTimeConnectivity
from proco.core.utils import get_current_datetime_object
from proco.data_sources.models import DailyCheckAppMeasurementData
from proco.data_sources.tasks import finalize_previous_day_data
from proco.data_sources.utils import load_daily_check_app_data_source_response_to_model
from proco.locations.models import Country
from proco.schools.models import School
from proco.utils import dates as date_utilities

ds_settings = settings.DATA_SOURCE_CONFIG

today_date = get_current_datetime_object().date()


def check_missing_dates_to_table(date_list):
    pcdc_timestamp_qry = DailyCheckAppMeasurementData.objects.all().filter(
        timestamp__date__gte=date_list[0],
        timestamp__date__lte=date_list[-1],
    ).values_list('timestamp__date', flat=True).distinct('timestamp__date').order_by('timestamp__date')

    print('Missing dates are between {0} - {1}: '.format(date_list[0], date_list[-1]))
    missing_dates = list(set(date_list) - set(list(pcdc_timestamp_qry)))

    for missing_date in sorted(missing_dates):
        # print missing date in string format
        print(date_utilities.format_date(missing_date))


def delete_dailycheckapp_realtime_data(date):
    print('Deleting all the PCDC rows only, from "RealTimeConnectivity" Data Table for date: {0}'.format(date))
    RealTimeConnectivity.objects.filter(
        created__date=date,
        live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
    ).delete()

    print('Deleting all the rows from "DailyCheckAppMeasurementData" Data Table for date: {0}'.format(date))
    DailyCheckAppMeasurementData.objects.filter(timestamp__date=date).delete()


def sync_dailycheckapp_realtime_data(date):
    request_configs = {
        'url': '{0}/measurements/v2'.format(ds_settings.get('DAILY_CHECK_APP').get('BASE_URL')),
        'method': 'GET',
        'data_limit': 1000,
        'query_params': {
            'page': '{page_no}',
            'size': '{page_size}',
            'orderBy': 'timestamp',
            'filterBy': 'timestamp',
            'filterCondition': 'eq',
            'filterValue': '{0}'.format(date),
        },
        'auth_token_required': True,
        'headers': {
            'Content-Type': 'application/json'
        }
    }
    load_daily_check_app_data_source_response_to_model(DailyCheckAppMeasurementData, request_configs)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--check_missing_dates', action='store_true', dest='check_missing_dates', default=False,
            help='List down all the missing dates for PCDC live data source.'
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
            '--pull_data', action='store_true', dest='pull_data', default=False,
            help='Pull the PCDC live data from API for specified date.'
        )

        parser.add_argument(
            '-pull_data_date', dest='pull_data_date', type=str,
            default=None,
            help='Date for which data pull will be performed.'
        )

    def handle(self, **options):
        print('Executing "data_loss_recovery_for_pcdc" ....')
        check_missing_dates = options.get('check_missing_dates')
        start_date = date_utilities.to_date(options.get('start_date'))
        end_date = date_utilities.to_date(options.get('end_date'))

        if start_date > end_date:
            print('ERROR: start_date value can not be greater than end_date.')
            exit(0)

        date_list = sorted([(start_date + timedelta(days=x)).date() for x in range((end_date - start_date).days)] + [
            end_date.date()])

        if check_missing_dates:
            check_missing_dates_to_table(date_list)

        pull_data = options.get('pull_data')
        pull_data_date = date_utilities.to_date(options.get('pull_data_date'))

        if pull_data and pull_data_date:
            pull_data_date = pull_data_date.date()

            print('Deleting PCDC data for date: {}'.format(pull_data_date))
            delete_dailycheckapp_realtime_data(pull_data_date)
            print('Data deleted successfully.\n\n')

            print('Syncing the PCDC API data to Proco PCDC table for date: {}'.format(pull_data_date))
            sync_dailycheckapp_realtime_data(pull_data_date)
            print('Data synced successfully.\n\n')

            print('Aggregating the pulled data by giga_id_school + country_code and '
                  'storing in RealTimeConnectivity table.')
            dailycheckapp_measurements = DailyCheckAppMeasurementData.objects.filter(
                timestamp__date=pull_data_date,
            ).filter(
                (Q(download__isnull=True) | Q(download__gte=0)) &
                (Q(upload__isnull=True) | Q(upload__gte=0)) &
                (Q(latency__isnull=True) | Q(latency__gte=0)),
            ).values(
                'giga_id_school', 'country_code', 'school_id', 'source',
            ).annotate(
                download_avg=Avg('download'),
                latency_avg=Avg('latency'),
                upload_avg=Avg('upload'),
            ).order_by('country_code', 'giga_id_school', 'school_id', 'source')

            if not dailycheckapp_measurements.exists():
                print('ERROR: No records to aggregate on provided date: "{0}". '
                      'Hence stopping the execution here.'.format(pull_data_date))
                return

            realtime = []

            countries = set(dailycheckapp_measurements.values_list(
                'country_code', flat=True,
            ).order_by('country_code'))
            for country_code in countries:
                print('Current Country Code: {}'.format(country_code))
                if country_code:
                    country = Country.objects.filter(code=country_code).first()
                else:
                    country = None

                schools_qs = School.objects
                if country:
                    schools_qs = schools_qs.filter(country=country)

                dcm_giga_ids = set(dailycheckapp_measurements.filter(
                    country_code=country_code,
                    source__iexact='DailyCheckApp',
                ).values_list(
                    'giga_id_school', flat=True,
                ).order_by('giga_id_school'))

                dcm_schools = {
                    school.giga_id_school: school
                    for school in schools_qs.filter(giga_id_school__in=dcm_giga_ids)
                }
                print('Total schools in DailyCheckApp: {0}, Successfully mapped schools: {1}'.format(
                    len(dcm_giga_ids), len(dcm_schools)))

                mlab_school_ids = set(dailycheckapp_measurements.filter(
                    country_code=country_code,
                    source__iexact='MLab',
                ).values_list(
                    'school_id', flat=True,
                ).order_by('school_id'))

                mlab_schools = {
                    school.external_id: school
                    for school in schools_qs.filter(external_id__in=mlab_school_ids)
                }
                print('Total schools in MLab: {0}, Successfully mapped schools: {1}'.format(
                    len(mlab_school_ids), len(mlab_schools)))

                unknown_schools = []

                for dailycheckapp_measurement in dailycheckapp_measurements.filter(country_code=country_code):
                    if str(dailycheckapp_measurement['source']).lower() == 'dailycheckapp':
                        giga_id_school = dailycheckapp_measurement.get('giga_id_school')
                        if giga_id_school not in dcm_schools:
                            unknown_schools.append(giga_id_school)
                            continue
                        school = dcm_schools[giga_id_school]
                    else:
                        school_id = dailycheckapp_measurement.get('school_id')
                        if school_id not in mlab_schools:
                            unknown_schools.append(school_id)
                            continue
                        school = mlab_schools[school_id]

                    connectivity_speed = dailycheckapp_measurement.get('download_avg')
                    if connectivity_speed:
                        # kb/s -> b/s
                        connectivity_speed = connectivity_speed * 1000

                    connectivity_upload_speed = dailycheckapp_measurement.get('upload_avg')
                    if connectivity_upload_speed:
                        # kb/s -> b/s
                        connectivity_upload_speed = connectivity_upload_speed * 1000

                    realtime.append(RealTimeConnectivity(
                        created=pull_data_date,
                        connectivity_speed=connectivity_speed,
                        connectivity_upload_speed=connectivity_upload_speed,
                        connectivity_latency=dailycheckapp_measurement.get('latency_avg'),
                        school=school,
                        live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
                    ))

                    if len(realtime) == 5000:
                        print('Loading the data to "RealTimeConnectivity" table as it has reached 5000 benchmark.')
                        RealTimeConnectivity.objects.bulk_create(realtime)
                        realtime = []

                if len(unknown_schools) > 0:
                    print('Skipped dailycheckapp_measurement for country: "{0}" unknown school: {1}'.format(
                        country_code, unknown_schools))

            print('Loading the remaining ({0}) data to "RealTimeConnectivity" table.'.format(len(realtime)))
            if len(realtime) > 0:
                RealTimeConnectivity.objects.bulk_create(realtime)

            print('Aggregated successfully to RealTimeConnectivity table.\n\n')

            print('Starting finalizing the records to actual proco tables.')
            countries_ids = RealTimeConnectivity.objects.filter(
                created__date=pull_data_date,
                live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
                school__deleted__isnull=True,
            ).annotate(
                country_id=F('school__country_id'),
            ).order_by('country_id').values_list('country_id', flat=True).distinct('country_id')

            monday_date = pull_data_date - timedelta(days=pull_data_date.weekday())

            monday_week_no = date_utilities.get_week_from_date(monday_date)
            monday_year = date_utilities.get_year_from_date(monday_date)

            print('Weekly record details. \tWeek No: {0}\tYear: {1}'.format(monday_week_no, monday_year))

            for country_id in countries_ids:
                print('Finalizing the records for Country ID: {0}'.format(country_id))
                finalize_previous_day_data(None, country_id, pull_data_date)

            print('Finalized records successfully to actual proco tables.\n\n')

        print('Completed "data_loss_recovery_for_pcdc" successfully ....\n')
