import logging

from collections import OrderedDict

from django.core.management.base import BaseCommand
from django.db import connection
from django.db import transaction
from django.utils import timezone

logger = logging.getLogger('gigamaps.' + __name__)


@transaction.atomic
def create_and_execute_update_query(value, data_dict_list):
    logger.debug('Executing update statement for: {0} records'.format(len(data_dict_list)))

    # create update query
    stmt = ("UPDATE public.connection_statistics_schooldailystatus "
            "SET live_data_source = '{value}' WHERE school_id = {school_id}")
    with connection.cursor() as cursor:
        for data_dict in data_dict_list:
            update_query = stmt.format(value=value, school_id=data_dict['school_id'])
            logger.debug('Current update query: {}'.format(update_query))
            cursor.execute(update_query)


@transaction.atomic
def create_and_execute_update_query_v2(stmt):
    logger.debug('Current update query: {}'.format(stmt))
    with connection.cursor() as cursor:
        cursor.execute(stmt)


def populate_live_data_source_as_qos(start_school_id, end_school_id):
    """ """
    ts = timezone.now()

    query = """
    UPDATE public.connection_statistics_countrydailystatus
        SET live_data_source = 'QOS'
    WHERE country_id IN (SELECT id FROM public.locations_country WHERE LOWER(code) = 'br')
    AND live_data_source <> 'QOS'
    """

    create_and_execute_update_query_v2(query)
    te = timezone.now()
    logger.debug('Executed the function in {} seconds'.format((te - ts).seconds))

    query = """
    SELECT DISTINCT s.id AS school_id
    FROM public.schools_school s
    INNER JOIN public.locations_country c ON s.country_id = c.id
        AND LOWER(c.code) = 'br'
    INNER JOIN public.connection_statistics_schooldailystatus sds ON sds.school_id = s.id
        AND sds.live_data_source <> 'QOS'
    {where_condition}
    ORDER BY s.id ASC
    """

    where_condition = ''
    if start_school_id and end_school_id:
        where_condition = f' WHERE s.id >= {start_school_id} AND s.id <= {end_school_id}'
    elif start_school_id:
        where_condition = f' WHERE s.id >= {start_school_id}'
    elif end_school_id:
        where_condition = f' WHERE s.id <= {end_school_id}'

    query = query.format(where_condition=where_condition)

    logger.info('Getting select statement query result from "schools_school" table for live_data_source records.')
    logger.debug('Query: {}'.format(query))
    data_list = []

    with connection.cursor() as cursor:
        cursor.execute(query)
        description = cursor.description
        for row in cursor:
            data_dict = OrderedDict()
            for index, data in enumerate(row):
                data_dict[description[index][0]] = data
            data_list.append(data_dict)

    create_and_execute_update_query('QOS', data_list)

    te2 = timezone.now()
    logger.debug('Executed the function in {} seconds'.format((te2 - te).seconds))


def populate_live_data_source_as_daily_check_app(start_school_id, end_school_id):
    """ """
    ts = timezone.now()

    query = """
    UPDATE public.connection_statistics_countrydailystatus
        SET live_data_source = 'DAILY_CHECK_APP_MLAB'
    WHERE country_id IN (SELECT id FROM public.locations_country WHERE LOWER(code) <> 'br')
        AND live_data_source <> 'DAILY_CHECK_APP_MLAB'
    """

    create_and_execute_update_query_v2(query)
    te = timezone.now()
    logger.debug('Executed the function in {} seconds'.format((te - ts).seconds))

    query = """
    SELECT DISTINCT s.id AS school_id
    FROM public.schools_school s
    INNER JOIN public.locations_country c ON s.country_id = c.id
        AND LOWER(c.code) <> 'br'
    INNER JOIN public.connection_statistics_schooldailystatus sds ON sds.school_id = s.id
        AND sds.live_data_source <> 'DAILY_CHECK_APP_MLAB'
    {where_condition}
    ORDER BY s.id ASC
    """

    where_condition = ''
    if start_school_id and end_school_id:
        where_condition = f' WHERE s.id >= {start_school_id} AND s.id <= {end_school_id}'
    elif start_school_id:
        where_condition = f' WHERE s.id >= {start_school_id}'
    elif end_school_id:
        where_condition = f' WHERE s.id <= {end_school_id}'

    query = query.format(where_condition=where_condition)

    logger.info('Getting select statement query result from "schools_school" table for live_data_source records.')
    logger.debug('Query: {}'.format(query))
    data_list = []

    with connection.cursor() as cursor:
        cursor.execute(query)
        description = cursor.description
        for row in cursor:
            data_dict = OrderedDict()
            for index, data in enumerate(row):
                data_dict[description[index][0]] = data
            data_list.append(data_dict)

    create_and_execute_update_query('DAILY_CHECK_APP_MLAB', data_list)

    te2 = timezone.now()
    logger.debug('Executed the function in {} seconds'.format((te2 - te).seconds))


class Command(BaseCommand):
    help = 'Update the School Daily and Country Daily tables with correct live_data_source name.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--update_brasil_live_data_source_name', action='store_true', dest='update_brasil_live_data_source',
            default=False,
            help='If provided, Brasil country school statistics will be updated to "QOS".'
        )

        parser.add_argument(
            '--update_non_brasil_live_data_source_name', action='store_true',
            dest='update_non_brasil_live_data_source_name', default=False,
            help='If provided, Other than Brasil country school statistics will be '
                 'updated to "DAILY_CHECK_APP_MLAB".'
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
        start_school_id = options.get('start_school_id')
        end_school_id = options.get('end_school_id')

        if options.get('update_brasil_live_data_source', False):
            logger.info('Update brasil live data source - start')
            populate_live_data_source_as_qos(start_school_id, end_school_id)

        if options.get('update_non_brasil_live_data_source_name', False):
            logger.info('Update non brasil live data source name - start')
            populate_live_data_source_as_daily_check_app(start_school_id, end_school_id)

        logger.info('Data updated successfully!\n')
