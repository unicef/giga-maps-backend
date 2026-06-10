import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from proco.core.utils import get_current_datetime_object
from proco.data_sources.tasks import run_entity_ping_aggregation
from proco.utils import dates as date_utilities

logger = logging.getLogger('gigamaps.' + __name__)

class MockTaskInstance:
    def info(self, msg):
        logger.info(msg)


class Command(BaseCommand):
    help = 'Run Giga Meter API data sync for entities.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-start_date', dest='start_date', type=str,
            default=date_utilities.format_date(get_current_datetime_object().date() - timedelta(days=1)),
            help='Start date in YYYY-MM-DD format. Default is yesterday.'
        )
        parser.add_argument(
            '-end_date', dest='end_date', type=str,
            default=date_utilities.format_date(get_current_datetime_object().date()),
            help='End date in YYYY-MM-DD format. Default is today.'
        )
        parser.add_argument(
            '-entity_type', dest='entity_type', type=str,
            default='health',
            help='Entity type code to process (e.g. health).'
        )

    def handle(self, **options):
        logger.info('Executing Giga Meter sync for entities...')
        start_date = date_utilities.to_date(options.get('start_date'))
        end_date = date_utilities.to_date(options.get('end_date'))
        entity_type_code = options.get('entity_type')
        
        if start_date > end_date:
            logger.error('Start date value can not be greater than end_date.')
            return

        # run_entity_ping_aggregation inherently handles date ranges and paginates through all data.
        logger.info(f"Running Giga Meter sync from {start_date} to {end_date} for entity type {entity_type_code}")
        
        try:
            run_entity_ping_aggregation(
                entity_type_code=entity_type_code,
                start_date=start_date.date() if hasattr(start_date, 'date') else start_date,
                end_date=end_date.date() if hasattr(end_date, 'date') else end_date,
                task_instance=MockTaskInstance(),
                logger=logger,
            )
            logger.info("Completed Giga Meter sync command successfully.")
        except Exception as e:
            logger.error(f"Error executing command: {e}")
