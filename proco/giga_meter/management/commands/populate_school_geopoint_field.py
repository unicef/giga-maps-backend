import logging

from django.contrib.gis.geos import Point
from django.core.management.base import BaseCommand

from proco.giga_meter import tasks as giga_meter_tasks
from proco.giga_meter.models import GigaMeter_School, GigaMeter_Country

logger = logging.getLogger('gigamaps.' + __name__)


def convert_to_point(country_id):
    school_qry = GigaMeter_School.objects.filter(country_id=country_id)

    for instance in school_qry:
        if instance.last_school_static:
            try:
                latitude = instance.last_school_static.latitude
                longitude = instance.last_school_static.longitude
                instance.geopoint = Point(longitude, latitude, srid=4326)
                instance.save()
            except (ValueError, IndexError):
                # Handle cases where lat/long is not in expected format or its NULL
                pass

class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument(
            '-country_iso3_format', dest='iso3_format', type=str,
            required=False,
            help='Country ISO3 Format Code.'
        )

        parser.add_argument(
            '--schedule', action='store_true', dest='schedule_tasks', default=False,
            help='If provided, it will schedule the task on Celery.'
        )


    def handle(self, **options):
        logger.info('Started Populate school geopoint field utility.\n')

        country_iso3_format = options.get('iso3_format')

        schedule_tasks = options.get('schedule_tasks')
        if schedule_tasks:
            giga_meter_tasks.scheduler_for_populate_school_geopoint_field.delay(country_iso3_format)
            logger.info('Completed scheduling the Populate school geopoint field successfully.\n')
            exit(0)

        if country_iso3_format:
            country = GigaMeter_Country.objects.filter(iso3_format=country_iso3_format).first()
            logger.info('Country object: {0}'.format(country))

            if not country:
                logger.error('Country with ISO3 format ({0}) not found in giga-meter db. '
                             'Hence stopping the load.'.format(country_iso3_format))
                exit(0)
            country_ids = [country.id, ]
        else:
            country_ids = list(GigaMeter_Country.objects.all().values_list('id', flat=True).order_by('id'))

        for country_id in country_ids:
            convert_to_point(country_id)
            logger.info(f'Populate school geopoint field completed for Country Id: {country_id}.')

        logger.info('Completed Populate school geopoint field utility successfully.\n')