import logging
from datetime import timedelta

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db.models import Avg
from django.db.models import F
from django.db.models import Q

from proco.connection_statistics import models as statistics_models
from proco.connection_statistics.config import app_config as statistics_configs
from proco.core.utils import get_current_datetime_object
from proco.data_sources.models import DailyCheckAppMeasurementData
from proco.data_sources.tasks import finalize_previous_day_data
from proco.data_sources.utils import load_daily_check_app_data_source_response_to_model
from proco.locations.models import Country
from proco.schools.models import School
from proco.utils import dates as date_utilities

logger = logging.getLogger('gigamaps.' + __name__)

ds_settings = settings.DATA_SOURCE_CONFIG


def delete_dailycheckapp_realtime_data(start_date, end_date, week_no, year):
    logger.info('Deleting all the PCDC rows only, from "RealTimeConnectivity" data table for dates: {0} - {1}'.format(
        start_date, end_date))
    statistics_models.RealTimeConnectivity.objects.filter(
        created__date__gte=start_date,
        created__date__lte=end_date,
        live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
    ).delete()

    logger.info('Deleting all the rows from "DailyCheckAppMeasurementData" data table for for dates: {0} - {1}'.format(
        start_date, end_date))
    DailyCheckAppMeasurementData.objects.filter(
        timestamp__date__gte=start_date,
        timestamp__date__lte=end_date,
    ).delete()

    logger.info('Updating live PCDC live fields from "SchoolWeeklyStatus" data table for year - week: {0} - {1}'.format(
        year, week_no))
    statistics_models.SchoolWeeklyStatus.objects.filter(
        week=week_no,
        year=year,
        school_id__in=list(statistics_models.SchoolDailyStatus.objects.filter(
            date__gte=start_date,
            date__lte=end_date,
            live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
        ).values_list('school', flat=True).order_by('school_id').distinct('school_id'))
    ).update(
        connectivity_speed=None,
        connectivity_upload_speed=None,
        connectivity_latency=None,
        connectivity=False
    )

    logger.info('Deleting all the rows from "SchoolDailyStatus" data table for dates: {0} - {1}'.format(
        start_date, end_date))
    statistics_models.SchoolDailyStatus.objects.filter(
        date__gte=start_date,
        date__lte=end_date,
        live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
    ).update(
        deleted=get_current_datetime_object()
    )

    impacted_country_ids = (statistics_models.CountryDailyStatus.objects.filter(
        date__gte=start_date,
        date__lte=end_date,
        live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
    ).values_list('country', flat=True).order_by('country_id').distinct('country_id'))

    logger.info(
        'Updating live PCDC live fields from "CountryWeeklyStatus" data table for year - week: {0} - {1}'.format(
            year, week_no))
    statistics_models.CountryWeeklyStatus.objects.filter(
        week=week_no,
        year=year,
        country_id__in=impacted_country_ids,
    ).update(
        connectivity_availability='no_connectivity',
        schools_connectivity_good=0,
        schools_connectivity_moderate=0,
        schools_connectivity_no=0,
        schools_connected=0,
        schools_with_data_percentage=0,
        connectivity_speed=None,
        connectivity_latency=None,
        connectivity_upload_speed=None,
    )

    logger.info('Deleting all the rows from "CountryDailyStatus" data table for dates: {0} - {1}'.format(
        start_date, end_date))
    statistics_models.CountryDailyStatus.objects.filter(
        date__gte=start_date,
        date__lte=end_date,
        live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
    ).update(
        deleted=get_current_datetime_object()
    )

    return impacted_country_ids


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
            '-start_week_no', dest='start_week_no', type=int,
            required=True,
            help='Date from we need to check the missing data. Default is 30 days from Current date.'
        )

        parser.add_argument(
            '-end_week_no', dest='end_week_no', type=int,
            required=True,
            help='Date till we need to check the missing data. Default is today date.'
        )

        parser.add_argument(
            '-year', dest='year', type=int,
            required=True,
            help='Date till we need to check the missing data. Default is today date.'
        )

        parser.add_argument(
            '--pull_data', action='store_true', dest='pull_data', default=False,
            help='Pull the PCDC live data from API for specified date.'
        )

    def handle(self, **options):
        logger.info('Executing data loss recovery for pcdc.')
        start_week_no = options.get('start_week_no')
        end_week_no = options.get('end_week_no')
        year = options.get('year')
        pull_data = options.get('pull_data')

        impacted_country_ids = []

        if start_week_no > end_week_no:
            logger.error('Start date value can not be greater than end_date.')
            exit(0)

        for week_no in range(start_week_no, end_week_no + 1):
            start_date = date_utilities.get_first_date_of_week(year, week_no)
            end_date = start_date + timedelta(days=6)

            if pull_data:
                country_ids = delete_dailycheckapp_realtime_data(start_date, end_date, week_no, year)
                impacted_country_ids.extend(country_ids)

                date_list = sorted(
                    [(start_date + timedelta(days=x)) for x in range((end_date - start_date).days)] + [end_date])
                logger.info('date_list: {}'.format(date_list))

                for pull_data_date in date_list:
                    logger.info('Syncing the PCDC api data to proco PCDC table for date: {}'.format(pull_data_date))
                    sync_dailycheckapp_realtime_data(pull_data_date)
                    logger.info('Data synced successfully.\n\n')

                    logger.info('Aggregating the pulled data by giga_id_school + country_code and storing in '
                                'RealTimeConnectivity table.')
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
                        logger.error('No records to aggregate on provided date: "{0}". '
                                     'Hence stopping the execution here.'.format(pull_data_date))
                        return

                    realtime = []

                    countries = set(dailycheckapp_measurements.values_list(
                        'country_code', flat=True,
                    ).order_by('country_code'))
                    for country_code in countries:
                        logger.info('Current country code: {}'.format(country_code))
                        if country_code:
                            country = Country.objects.filter(code=country_code).first()
                        else:
                            country = None

                        dcm_giga_ids = set(dailycheckapp_measurements.filter(
                            country_code=country_code,
                            source__iexact='DailyCheckApp',
                        ).values_list(
                            'giga_id_school', flat=True,
                        ).order_by('giga_id_school'))

                        dcm_schools = {
                            school.giga_id_school: school
                            for school in School.objects.filter(giga_id_school__in=dcm_giga_ids)
                        }
                        logger.info('Total schools in dailycheckapp: {0}, Successfully mapped schools: {1}'.format(
                            len(dcm_giga_ids), len(dcm_schools)))

                        mlab_school_ids = set(dailycheckapp_measurements.filter(
                            country_code=country_code,
                            source__iexact='MLab',
                        ).values_list(
                            'school_id', flat=True,
                        ).order_by('school_id'))

                        schools_qs = School.objects
                        if country:
                            schools_qs = schools_qs.filter(country=country)

                        mlab_schools = {
                            school.external_id: school
                            for school in schools_qs.filter(external_id__in=mlab_school_ids)
                        }
                        logger.info('Total schools in MLab: {0}, Successfully mapped schools: {1}'.format(
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

                            realtime.append(statistics_models.RealTimeConnectivity(
                                created=pull_data_date,
                                connectivity_speed=connectivity_speed,
                                connectivity_upload_speed=connectivity_upload_speed,
                                connectivity_latency=dailycheckapp_measurement.get('latency_avg'),
                                school=school,
                                live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
                            ))

                            if len(realtime) == 5000:
                                logger.info('Loading the data to "RealTimeConnectivity" table as it '
                                            'has reached 5000 benchmark.')
                                statistics_models.RealTimeConnectivity.objects.bulk_create(realtime)
                                realtime = []

                        if len(unknown_schools) > 0:
                            logger.info('Skipped dailycheckapp measurement for country: "{0}" unknown school:'
                                        ' {1}'.format(country_code, unknown_schools))

                    logger.info(
                        'Loading the remaining ({0}) data to "RealTimeConnectivity" table.'.format(len(realtime)))
                    if len(realtime) > 0:
                        statistics_models.RealTimeConnectivity.objects.bulk_create(realtime)

            logger.info('Aggregated successfully to RealTimeConnectivity table.\n\n')

            logger.info('Starting finalizing the records to actual proco tables.')
            countries_ids = statistics_models.RealTimeConnectivity.objects.filter(
                created__date__gte=start_date,
                created__date__lte=end_date,
                live_data_source=statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE,
                school__deleted__isnull=True,
            ).annotate(
                country_id=F('school__country_id'),
            ).order_by('country_id').values_list('country_id', flat=True).distinct('country_id')

            logger.info('Weekly record details. \tWeek No: {0}\tYear: {1}'.format(week_no, year))

            for country_id in countries_ids:
                logger.info('Finalizing the records for Country ID: {0}'.format(country_id))
                finalize_previous_day_data(None, country_id, end_date)

            logger.info('Finalized records successfully to actual proco tables.\n\n')

        for impacted_country_id in set(impacted_country_ids):
            cmd_args = ['--reset', f'-country_id={impacted_country_id}']
            call_command('populate_school_registration_data', *cmd_args)

        logger.info('Completed data loss recovery utility for pcdc successfully.\n')
