import datetime
import logging

from django.core.management.base import BaseCommand

from proco.connection_statistics.utils import (
    aggregate_entity_daily_status_to_entity_weekly_status,
)
from proco.core import utils as core_utilities
from proco.locations.models import Country
from proco.utils import dates as date_utilities

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
            help='Pass the Year to control the update.'
        )

        parser.add_argument(
            '-week_no', dest='week_no', required=False, type=int,
            help='Pass the Week No in case want to control the update.'
        )
        
        parser.add_argument(
            '-entity_type_code', dest='entity_type_code', required=False, type=str,
            help='Pass the entity type code to filter the update.'
        )

        parser.add_argument(
            '--update_entity_weekly', action='store_true',
            dest='update_entity_weekly', default=False,
            help='If provided, run the Entity weekly aggregations manually in real time.'
        )

    def handle(self, **options):
        logger.info('Executing redo entity aggregations utility.\\n')
        logger.info('Options: {}\\n\\n'.format(options))

        country_id = options.get('country_id', None)
        country = Country.objects.get(id=country_id)

        year = options.get('year', None)
        week_no = options.get('week_no', None)
        entity_type_code = options.get('entity_type_code', None)

        dates_list = list(get_date_list(year, week_no))
        monday_date_list = list(get_all_monday_dates(dates_list))

        if options.get('update_entity_weekly'):
            logger.info('Performing entity weekly aggregations for date range: {0} - {1}'.format(
                monday_date_list[0], monday_date_list[-1]))
            for monday_date in monday_date_list:
                aggregate_entity_daily_status_to_entity_weekly_status(country, monday_date, entity_type_code)
            logger.info('Completed entity weekly aggregations.\\n\\n')

        country.invalidate_country_related_cache()

        logger.info('Completed redo entity aggregations successfully.\\n')
