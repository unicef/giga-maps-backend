import logging
from collections import OrderedDict

from django.core.management.base import BaseCommand
from django.db import connection
from django.db import transaction
from django.utils import timezone
from proco.core.utils import get_current_datetime_object
from proco.connection_statistics import models as statistics_models

logger = logging.getLogger('gigamaps.' + __name__)


def delete_relationships(country_id, entity_id):
    relationships = statistics_models.EntityRealTimeRegistration.objects.all()

    if country_id:
        relationships = relationships.filter(entity__country_id=country_id)

    if entity_id:
        relationships = relationships.filter(entity_id=entity_id)

    relationships.update(deleted=get_current_datetime_object())


def get_insert_prepared_statement(table_name, table_column):
    stmt = "INSERT INTO {table_name} ({columns}) VALUES \n ({values})"
    columns = ", ".join(table_column)
    values = ['%s'] * len(table_column)
    values = ", ".join(values)
    return stmt.format(table_name=table_name, columns=columns, values=values)


@transaction.atomic
def create_and_execute_insert_query(table_columns, insert_statement_list):
    with connection.cursor() as cursor:
        # create insert query
        insert_statement = get_insert_prepared_statement(
            table_name='connection_statistics_entityrealtimeregistration',
            table_column=table_columns,
        )

        insert_ts = timezone.now()
        logger.info('Executing bulk insert for total: {} records'.format(len(insert_statement_list)))
        logger.info(insert_statement)
        cursor.executemany(insert_statement, insert_statement_list)
        insert_te = timezone.now()
        logger.info('Bulk insert time is "{}" second'.format((insert_te - insert_ts).seconds))


def populate_entity_registration_data(country_id, entity_id):
    """ """
    ts = timezone.now()
    last_processed_id = None
    current_rows_num = 0

    insert_statement_list = []
    table_columns = []

    query = """
    SELECT DISTINCT dailystat.entity_id as entity_id,
        true rt_registered,
        FIRST_VALUE(dailystat.date) OVER (PARTITION BY dailystat.entity_id ORDER BY dailystat.date)
        rt_registration_date,
        NULL rt_source
    FROM connection_statistics_entitydailystatus dailystat
    INNER JOIN entities_entity entity ON entity.id = dailystat.entity_id
    WHERE entity.deleted IS NULL AND dailystat.deleted IS NULL
    """

    if entity_id:
        query += f' AND dailystat.entity_id = {entity_id}'

    if country_id:
        query += f' AND entity.country_id = {country_id}'

    with connection.cursor() as cursor:
        logger.info('Getting select statement query result from Entity + EntityDailyStatus tables')
        logger.info('Query: {}'.format(query))

        cursor.execute(query)
        description = cursor.description
        for row in cursor:
            rt_data_dict = OrderedDict()
            for index, data in enumerate(row):
                rt_data_dict[description[index][0]] = data

            rt_data_dict['created'] = ts
            rt_data_dict['last_modified_at'] = ts

            if not table_columns:
                table_columns = list(rt_data_dict.keys())

            insert_statement_list.append(tuple(rt_data_dict.values()))
            current_rows_num += 1

    if insert_statement_list:
        create_and_execute_insert_query(table_columns, insert_statement_list)

    te = timezone.now()
    logger.info('Executed the function in "{}" seconds'.format((te - ts).seconds))
    return current_rows_num, last_processed_id


class Command(BaseCommand):
    help = 'Populate the Entity Real Time Registration table'

    def add_arguments(self, parser):
        parser.add_argument(
            '--reset', action='store_true', dest='reset_mapping', default=False,
            help='If provided, already created registration data will be deleted.'
        )

        parser.add_argument(
            '-country_id', dest='country_id', required=False, type=int,
            help='Pass the Country ID in case want to control the update.'
        )

        parser.add_argument(
            '-entity_id', dest='entity_id', required=False, type=int,
            help='Pass the entity id in case want to control the update.'
        )

    def handle(self, **options):
        country_id = options.get('country_id', None)
        entity_id = options.get('entity_id')

        if options.get('reset_mapping', False):
            logger.info('Starting deleting old records.')
            delete_relationships(country_id, entity_id)
            logger.info('Deleted old records.')

        logger.info('Entity Registration update operation STARTED ({0})'.format(options))
        populate_entity_registration_data(country_id, entity_id)
        logger.info('Entity Registration update operation ENDED ({0})'.format(options))
