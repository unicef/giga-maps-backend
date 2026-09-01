import logging
from datetime import timedelta, datetime

from django.core.management.base import BaseCommand
from proco.core.utils import get_current_datetime_object
from proco.data_sources.utils import (
    load_entity_qos_data_source_response_to_model,
    sync_entity_qos_realtime_data
)
from proco.connection_statistics.models import EntityRealTimeConnectivity
from proco.connection_statistics.config import app_config as statistics_configs
from proco.utils import dates as date_utilities

logger = logging.getLogger('gigamaps.' + __name__)

class Command(BaseCommand):
    help = 'Run QoS Delta Sharing data sync for entities.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-start_date', dest='start_date', type=str,
            default=get_current_datetime_object().date().strftime('%Y-%m-%d'),
            help='Start date in YYYY-MM-DD format. Default is today.'
        )
        parser.add_argument(
            '-end_date', dest='end_date', type=str,
            default=get_current_datetime_object().date().strftime('%Y-%m-%d'),
            help='End date in YYYY-MM-DD format. Default is today.'
        )
        parser.add_argument(
            '-entity_type', dest='entity_type', type=str,
            default='health',
            help='Entity type code to process (e.g. health).'
        )

    def handle(self, **options):
        logger.info('Executing QoS Delta Sharing sync for entities...')
        try:
            start_date = datetime.strptime(options.get('start_date'), '%Y-%m-%d').date() if isinstance(options.get('start_date'), str) else options.get('start_date')
            end_date = datetime.strptime(options.get('end_date'), '%Y-%m-%d').date() if isinstance(options.get('end_date'), str) else options.get('end_date')
        except ValueError:
            logger.error('Invalid date format. Please use YYYY-MM-DD.')
            return
        entity_type_code = options.get('entity_type')
        
        if start_date > end_date:
            logger.error('Start date value can not be greater than end_date.')
            return

        # Delta sharing loads all changes since last processed version natively.
        logger.info(f"Loading latest {entity_type_code} QoS data from Delta Sharing...")
        try:
            load_entity_qos_data_source_response_to_model(entity_type_code=entity_type_code)
            logger.info("Delta Sharing fetch completed.")
        except Exception as e:
            logger.error(f"Error loading data from Delta Sharing: {e}")
            return

        date_list = sorted([(start_date + timedelta(days=x)) for x in range((end_date - start_date).days)] + [end_date])
        
        countries_ids = list(
            set(
                EntityRealTimeConnectivity.objects.filter(
                    live_data_source=statistics_configs.QOS_SOURCE,
                    entity__entity_type__code=entity_type_code,
                    entity__deleted__isnull=True,
                ).order_by().values_list(
                    'entity__country_id', flat=True
                ).distinct()
            )
        )

        logger.info(f"Aggregating entity QoS data for {len(countries_ids)} countries...")
        
        for dt in date_list:
            logger.info(f"Syncing daily aggregation for {dt}...")
            for country_id in countries_ids:
                try:
                    sync_entity_qos_realtime_data(
                        country_id,
                        entity_type_code=entity_type_code,
                        start_date=dt,
                        end_date=dt
                    )
                except Exception as ex:
                    logger.error(f"Error syncing realtime data for country ID {country_id} on {dt}: {ex}")

        logger.info("Completed QoS Delta Sharing sync command successfully.")
