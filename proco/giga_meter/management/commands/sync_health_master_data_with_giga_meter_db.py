import logging

from celery import chain
from django.core.management.base import BaseCommand

from proco.giga_meter import tasks as giga_meter_tasks
from proco.giga_meter.models import GigaMeter_Country

logger = logging.getLogger('gigamaps.' + __name__)


class Command(BaseCommand):
    help = 'Sync Health Master data into Giga Meter DB (pull CDC, publish, soft-delete).'

    def add_arguments(self, parser):
        parser.add_argument(
            '-country_iso3_format', dest='iso3_format', type=str,
            required=False,
            help='Country ISO3 Format Code.',
        )
        parser.add_argument(
            '--force', action='store_true', dest='force_tasks', default=False,
            help='If provided, it will skip the duplicate task check.',
        )
        parser.add_argument(
            '--schedule', action='store_true', dest='schedule_tasks', default=False,
            help='If provided, it will schedule the task on Celery.',
        )

    def handle(self, **options):
        iso3_format = options.get('iso3_format')
        force_tasks = options.get('force_tasks')
        schedule_tasks = options.get('schedule_tasks')

        country = None
        country_id = None
        country_label = 'all'

        if iso3_format:
            country = GigaMeter_Country.objects.get(iso3_format=iso3_format)
            logger.info('Executing Health Master Data Sync utility for country: "{}".'.format(country.name))
            country_id = [country.id]
            country_label = country.name

        if schedule_tasks:
            chain(
                giga_meter_tasks.giga_meter_update_health_static_data.s(
                    country_iso3_format=iso3_format, force_tasks=force_tasks),
                giga_meter_tasks.giga_meter_handle_published_health_master_data_row.s(
                    country_ids=country_id, force_tasks=force_tasks),
                giga_meter_tasks.giga_meter_handle_deleted_health_master_data_row.s(
                    country_ids=country_id, force_tasks=force_tasks),
            ).delay()
        else:
            giga_meter_tasks.giga_meter_update_health_static_data(
                country_iso3_format=iso3_format, force_tasks=force_tasks)
            logger.info('Health Data Sync Completed.')

            giga_meter_tasks.giga_meter_handle_published_health_master_data_row(
                country_ids=country_id, force_tasks=force_tasks)
            logger.info('Published health rows Sync Completed.')

            giga_meter_tasks.giga_meter_handle_deleted_health_master_data_row(
                country_ids=country_id, force_tasks=force_tasks)
            logger.info('Deleted health rows Sync Completed.')

        logger.info(
            'Completed Health Master Data Sync utility for country: "{}" successfully.\n'.format(country_label),
        )
