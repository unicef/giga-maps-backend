import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Prefetch

from proco.core import utils as core_utilities
from proco.locations.models import Country
from proco.schools import utils as school_utilities
from proco.schools.models import School

logger = logging.getLogger('gigamaps.' + __name__)


def populate_school_new_fields(school_id, start_school_id, end_school_id, country_id, school_ids):
    """ """
    schools_qry = School.objects.all()

    schools_qry = schools_qry.prefetch_related(
        Prefetch('country', Country.objects.defer('geometry')),
    )
    if school_id and isinstance(school_id, int):
        schools_qry = schools_qry.filter(id=school_id, )

    if start_school_id:
        schools_qry = schools_qry.filter(id__gte=start_school_id, )

    if end_school_id:
        schools_qry = schools_qry.filter(id__lte=end_school_id, )

    if country_id:
        schools_qry = schools_qry.filter(country_id=country_id, )

    if school_ids and len(school_ids) > 0:
        schools_qry = schools_qry.filter(id__in=school_ids.split(','))

    logger.info('Starting the process: {}'.format(schools_qry.query))
    count = 1
    for data_chunk in core_utilities.queryset_iterator(schools_qry, chunk_size=20000):
        with transaction.atomic():
            for school in data_chunk:
                school.coverage_type = school_utilities.get_coverage_type(school)
                school.coverage_status = school_utilities.get_coverage_status(school)
                school.connectivity_status = school_utilities.get_connectivity_status_by_master_api(school)
                school.save(update_fields=['coverage_type', 'coverage_status', 'connectivity_status'])

        logger.info("Update on school records succeeded for count '{0}'".format(count))
        count += 1
    logger.info('Completed the process.')


class Command(BaseCommand):
    help = 'Populate the school columns with latest values: CoverageType, CoverageStatus, ConnectivityStatus'

    def add_arguments(self, parser):
        parser.add_argument(
            '-school_id', dest='school_id', required=False, type=int,
            help='Pass the school id in case want to control the update.'
        )

        parser.add_argument(
            '-start_school_id', dest='start_school_id', required=False, type=int,
            help='Pass the school id in case want to control the update.'
        )
        parser.add_argument(
            '-end_school_id', dest='end_school_id', required=False, type=int,
            help='Pass the school id in case want to control the update.'
        )

        parser.add_argument(
            '-country_id', dest='country_id', required=False, type=int,
            help='Pass the Country ID in case want to control the update.'
        )
        parser.add_argument(
            '-school_ids', dest='school_ids', required=False, type=str,
            help='Pass the School IDs in case want to control the update.'
        )

    def handle(self, **options):
        school_id = options.get('school_id')
        school_ids = options.get('school_ids')
        start_school_id = options.get('start_school_id')
        end_school_id = options.get('end_school_id')
        country_id = options.get('country_id')

        logger.info('School update operation started ({0})'.format(options))

        populate_school_new_fields(school_id, start_school_id, end_school_id, country_id, school_ids)
