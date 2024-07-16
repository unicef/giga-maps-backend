import logging
import sys

from django.core.management.base import BaseCommand
from django.db import connection
from django.db import transaction
from django.utils import timezone

logger = logging.getLogger('gigamaps.' + __name__)


@transaction.atomic
def create_and_execute_update_query(stmt):
    logger.debug('Current update query: {}'.format(stmt))
    with connection.cursor() as cursor:
        cursor.execute(stmt)


def populate_ui_label_for_admin1_data(country_id, parent_id):
    """ """
    ts = timezone.now()

    query = """
    WITH r AS
    (
        SELECT DISTINCT ON(country_id) country_id,
            description,
            count(id) AS no_of_admin1
        FROM locations_countryadminmetadata
        WHERE layer_name = 'adm1' AND description IS NOT NULL AND deleted IS NULL
        GROUP BY description, country_id
        ORDER BY country_id, no_of_admin1 DESC
    )
    UPDATE locations_countryadminmetadata AS t
        SET description_ui_label = r.description
    FROM r
    WHERE t.country_id = r.country_id
        AND t.layer_name = 'adm1'
        AND r.description IS NOT NULL
        {country_condition}
        {parent_condition}
    """

    country_condition = ''
    parent_condition = ''

    if country_id:
        country_condition = f' AND t.country_id = {country_id}'
    if parent_id:
        parent_condition = f' AND t.parent_id = {parent_id}'

    query = query.format(country_condition=country_condition, parent_condition=parent_condition)

    create_and_execute_update_query(query)

    te = timezone.now()
    logger.debug('Executed the function in {} seconds'.format((te - ts).seconds))


def populate_ui_label_for_admin2_data(country_id, parent_id):
    """ """
    ts = timezone.now()

    query = """
    WITH r AS (
        SELECT DISTINCT ON(parent_id) parent_id,
            description,
            count(id) AS no_of_admin2
        FROM locations_countryadminmetadata
        WHERE layer_name = 'adm2' AND description IS NOT NULL AND deleted IS NULL
        GROUP BY description, parent_id
        ORDER BY parent_id, no_of_admin2 DESC
    )
    UPDATE locations_countryadminmetadata AS t
        SET description_ui_label = r.description
    FROM r
    WHERE t.parent_id = r.parent_id
        AND t.layer_name = 'adm2'
        AND r.description IS NOT NULL
        {country_condition}
        {parent_condition}
    """

    country_condition = ''
    parent_condition = ''

    if country_id:
        country_condition = f' AND t.country_id = {country_id}'
    if parent_id:
        parent_condition = f' AND t.parent_id = {parent_id}'

    query = query.format(country_condition=country_condition, parent_condition=parent_condition)

    create_and_execute_update_query(query)

    te = timezone.now()
    logger.debug('Executed the function in {} seconds'.format((te - ts).seconds))


class Command(BaseCommand):
    help = 'Populate the Admin table with Admin1 and Admin2 UI Labels.'

    def add_arguments(self, parser):

        parser.add_argument(
            '-at',
            '--admin-type',
            type=str,
            dest='admin_type',
            help='admin1 or admin2.',
        )

        parser.add_argument(
            '-country_id',
            dest='country_id',
            required=False,
            type=int,
            help='Pass the Country ID in case want to control the update.'
        )

        parser.add_argument(
            '-parent_id',
            dest='parent_id',
            required=False,
            type=int,
            help='Pass the Parent ID in case want to control the update.'
        )

    def handle(self, **options):
        admin_type = options.get('admin_type', 'both')
        if admin_type not in ['admin1', 'admin2', 'both']:
            sys.exit("Mandatory argument '--admin-type/-at' is missing. Available options: {0}".format(
                ['admin1', 'admin2', 'both']))

        country_id = options.get('country_id')
        parent_id = options.get('parent_id')

        logger.debug('Admin update operation started ({0} - {1})'.format(country_id, parent_id))

        if admin_type == 'admin1':
            populate_ui_label_for_admin1_data(country_id, parent_id)
        elif admin_type == 'admin2':
            populate_ui_label_for_admin2_data(country_id, parent_id)
        else:
            populate_ui_label_for_admin1_data(country_id, parent_id)
            populate_ui_label_for_admin2_data(country_id, parent_id)

        logger.info('Data loaded successfully!\n')
