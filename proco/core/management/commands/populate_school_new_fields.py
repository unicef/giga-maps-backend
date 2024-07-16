from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Prefetch

from proco.core import utils as core_utilities
from proco.locations.models import Country
from proco.schools import utils as school_utilities
from proco.schools.models import School


def populate_school_new_fields(school_id, start_school_id, end_school_id, country_id):
    """ """
    schools_qry = School.objects.all()

    schools_qry = schools_qry.prefetch_related(
        Prefetch('country', Country.objects.defer('geometry', 'geometry_simplified')),
    )
    if school_id and isinstance(school_id, int):
        schools_qry = schools_qry.filter(id=school_id,)

    if start_school_id:
        schools_qry = schools_qry.filter(id__gte=start_school_id,)

    if end_school_id:
        schools_qry = schools_qry.filter(id__lte=end_school_id,)

    if country_id:
        schools_qry = schools_qry.filter(country_id=country_id,)

    print('Starting the process: ', schools_qry.query)
    count = 1
    for data_chunk in core_utilities.queryset_iterator(schools_qry, chunk_size=20000):
        with transaction.atomic():
            for school in data_chunk:
                school.coverage_type = school_utilities.get_coverage_type(school)
                school.coverage_status = school_utilities.get_coverage_status(school)
                school.connectivity_status = school_utilities.get_connectivity_status_by_master_api(school)
                school.save(update_fields=['coverage_type', 'coverage_status', 'connectivity_status'])

        print("Update on school records SUCCEEDED for count '{0}'".format(count))
        count += 1
    print('Completed the process.')


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

    def handle(self, **options):
        school_id = options.get('school_id')
        start_school_id = options.get('start_school_id')
        end_school_id = options.get('end_school_id')
        country_id = options.get('country_id')

        print('*** School update operation STARTED ({0}) ***'.format(school_id))

        populate_school_new_fields(school_id, start_school_id, end_school_id, country_id)
