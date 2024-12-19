import logging

from django.core.management.base import BaseCommand

from proco.daily_check_app import tasks as daily_check_app_tasks
from proco.daily_check_app.models import DailyCheckApp_Country

logger = logging.getLogger('gigamaps.' + __name__)


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument(
            '-country_iso3_format', dest='iso3_format', type=str,
            required=True,
            help='Country ISO3 Format Code.'
        )


    def handle(self, **options):
        iso3_format = options.get('iso3_format')
        country = DailyCheckApp_Country.objects.get(iso3_format=iso3_format)
        logger.info('Executing School Master Data Sync utility for country: "{}".'.format(country.name))

        daily_check_app_tasks.daily_check_app_update_static_data(country_iso3_format=iso3_format)
        logger.info('Data Sync Completed.')

        daily_check_app_tasks.daily_check_app_handle_published_school_master_data_row([country.id,])
        logger.info('Published rows Sync Completed.')

        daily_check_app_tasks.daily_check_app_handle_deleted_school_master_data_row([country.id,])
        logger.info('Deleted rows Sync Completed.')

        logger.info('Completed School Master Data Sync utility for country: "{}" successfully.\n'.format(country.name))
