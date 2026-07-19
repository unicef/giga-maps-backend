import logging

from django.core.management.base import BaseCommand

from proco.data_sources.tasks import run_entity_ping_aggregation

logger = logging.getLogger('gigamaps.' + __name__)

class MockTaskInstance:
    def info(self, msg):
        logger.info(msg)


class Command(BaseCommand):
    help = 'Run Giga Meter API data sync for entities.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-entity_type', dest='entity_type', type=str,
            default='health',
            help='Entity type code to process (e.g. health).'
        )

    def handle(self, **options):
        logger.info('Executing Giga Meter sync for entities...')
        entity_type_code = options.get('entity_type')

        # run_entity_ping_aggregation inherently handles pagination through all available data.
        logger.info(f"Running Giga Meter sync for entity type {entity_type_code}")

        try:
            run_entity_ping_aggregation(
                entity_type_code=entity_type_code,
                task_instance=MockTaskInstance(),
                logger=logger,
            )
            logger.info("Completed Giga Meter sync command successfully.")
        except Exception as e:
            logger.error(f"Error executing command: {e}")
