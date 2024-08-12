import datetime
import logging

from django.core.management.base import BaseCommand

from proco.connection_statistics.utils import (
    aggregate_school_daily_status_to_school_weekly_status,
    aggregate_school_daily_to_country_daily,
    update_country_weekly_status,
)
from proco.core import utils as core_utilities
from proco.locations.models import Country
from proco.utils import dates as date_utilities
from proco.connection_statistics.models import SchoolWeeklyStatus

logger = logging.getLogger('gigamaps.' + __name__)


def get_date_list(year, week_no):
    if week_no:
        start_date = date_utilities.get_first_date_of_week(year, week_no)
        end_date = start_date + datetime.timedelta(days=6)
    else:
        start_date = date_utilities.get_first_date_of_month(year, 1)
        if year == date_utilities.get_current_year():
            end_date = core_utilities.get_current_datetime_object().date() - datetime.timedelta(days=1)
        else:
            end_date = date_utilities.get_last_date_of_month(year, 12)

    return date_utilities.date_range_list(start_date, end_date)


def get_all_monday_dates(date_list):
    days_of_week = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

    for date in date_list:
        weekday_name = days_of_week[date.isocalendar()[2] - 1]
        if weekday_name == 'monday':
            yield date


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '-country_id', dest='country_id', required=True, type=int,
            help='Pass the Country ID to control the update.'
        )

        parser.add_argument(
            '-year', dest='year', default=date_utilities.get_current_year(), type=int,
            help='Pass the School ID in case want to control the update.'
        )

        parser.add_argument(
            '-week_no', dest='week_no', required=False, type=int,
            help='Pass the School ID in case want to control the update.'
        )

        parser.add_argument(
            '--update_country_daily', action='store_true',
            dest='update_country_daily', default=False,
            help='If provided, run the School Master data publish task manually in real time.'
        )

        parser.add_argument(
            '--update_school_weekly', action='store_true',
            dest='update_school_weekly', default=False,
            help='If provided, run the School Master data publish task manually in real time.'
        )

        parser.add_argument(
            '--update_country_weekly', action='store_true',
            dest='update_country_weekly', default=False,
            help='If provided, run the School Master data publish task manually in real time.'
        )

    def handle(self, **options):
        logger.info('Executing redo aggregations utility.\n')
        logger.info('Options: {}\n\n'.format(options))

        country_id = options.get('country_id', None)
        country = Country.objects.get(id=country_id)

        year = options.get('year', None)
        week_no = options.get('week_no', None)

        dates_list = list(get_date_list(year, week_no))
        monday_date_list = list(get_all_monday_dates(dates_list))

        if options.get('update_school_weekly'):
            logger.info('Performing school weekly aggregations for date range: {0} - {1}'.format(
                monday_date_list[0], monday_date_list[-1]))
            for monday_date in monday_date_list:
                aggregate_school_daily_status_to_school_weekly_status(country, monday_date)
            logger.info('Completed school weekly aggregations.\n\n')

        if options.get('update_country_daily'):
            logger.info('Performing country daily aggregations for date range: {0} - {1}'.format(
                dates_list[0], dates_list[-1]))
            for date in dates_list:
                aggregate_school_daily_to_country_daily(country, date)
            logger.info('Completed country daily aggregations.\n\n')

        if options.get('update_country_weekly'):
            logger.info('Performing country weekly aggregations for date range: {0} - {1}'.format(
                monday_date_list[0], monday_date_list[-1]))
            for monday_date in monday_date_list:
                monday_week_no = date_utilities.get_week_from_date(monday_date)
                if SchoolWeeklyStatus.objects.filter(
                    school__country=country, week=monday_week_no, year=year,
                ).exists():
                    update_country_weekly_status(country, monday_date)
                else:
                    logger.info('Country weekly aggregations skipped as school weekly has no record for same data:'
                                ' Year - {0}, Week No - {1}'.format(year, monday_week_no))
            logger.info('Completed country weekly aggregations.\n\n')

        country.invalidate_country_related_cache()

        logger.info('Completed redo aggregations successfully.\n')
