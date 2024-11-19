import logging

from django.core.management.base import BaseCommand

from proco.data_sources.models import SchoolMasterData

logger = logging.getLogger('gigamaps.' + __name__)


def delete_school_master_historical_rows(country_id):
    history_qs = SchoolMasterData.history.model.objects.all()

    if country_id:
        history_qs = history_qs.filter(country_id=country_id)

    logger.info('Hard deleting records from School Master History table for query: {}'.format(history_qs.query))
    history_qs.delete()


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--clean_school_master_historical_rows', action='store_true', dest='clean_school_master_historical_rows',
            default=False,
            help='If provided, School Master historical records will be deleted based on passed options.'
        )

        parser.add_argument(
            '-country_id', dest='country_id', required=False, type=int,
            help='Pass the Country ID in case want to control the update.'
        )

        parser.add_argument(
            '-start_school_id', dest='start_school_id', required=False, type=int,
            help='Pass the school id in case want to control the update.'
        )
        parser.add_argument(
            '-end_school_id', dest='end_school_id', required=False, type=int,
            help='Pass the school id in case want to control the update.'
        )

    def handle(self, **options):
        logger.info('Executing data source module additional utility.\n')
        logger.info('Options: {}\n\n'.format(options))

        country_id = options.get('country_id', None)
        # start_school_id = options.get('start_school_id', None)
        # end_school_id = options.get('end_school_id', None)

        if options.get('clean_school_master_historical_rows'):
            logger.info('Performing school master historical record cleanup.')
            delete_school_master_historical_rows(country_id)
            logger.info('Completed school master historical record cleanup.\n\n')

        logger.info('Completed data source utility successfully.\n')
