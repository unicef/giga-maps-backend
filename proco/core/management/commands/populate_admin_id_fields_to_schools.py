from collections import OrderedDict

from django.core.management.base import BaseCommand
from django.db import connection
from django.db import transaction
from django.utils import timezone
import sys


@transaction.atomic
def create_and_execute_update_query(column, data_dict_list):
    print('Executing update statement for: {0} records'.format(len(data_dict_list)))

    # create update query
    stmt = "UPDATE schools_school SET {column} = {value} WHERE id = {school_id}"
    with connection.cursor() as cursor:
        for data_dict in data_dict_list:
            update_query = stmt.format(column=column, value=data_dict[column], school_id=data_dict['school_id'])
            # print('Current record: {}'.format(data_dict))
            print('Current Update Query: {}'.format(update_query))
            cursor.execute(update_query)


def populate_school_admin1_data(start_school_id, end_school_id):
    """ """
    ts = timezone.now()

    query = """
    SELECT ca.id as admin1_id,
       ca.name as admin1_name,
       ca.name_en as admin1_name_en,
       t.giga_id_school as from_t_giga_id,
       c.id as country_id,
       c.name as country_name,
       s.id as school_id,
       s.giga_id_school as from_school_giga_id
    FROM public.schools_with_admin_data t
    INNER JOIN public.locations_country c ON (LOWER(c.code) = LOWER(t.country_iso2_code)
                                              OR LOWER(c.iso3_format) = LOWER(t.country_iso3_code))
                                              AND c."deleted" IS NULL
    INNER JOIN public.schools_school s ON s.giga_id_school = t.giga_id_school AND s.country_id = c.id
    INNER JOIN public.locations_countryadminmetadata ca ON ca.giga_id_admin = t.admin1_id_giga
        AND ca.layer_name = 'adm1'
        AND ca.country_id = c.id
    {where_condition} ORDER BY s.id ASC
    """

    where_condition = ''
    if start_school_id and end_school_id:
        where_condition = f' WHERE s.id >= {start_school_id} AND s.id <= {end_school_id}'
    elif start_school_id:
        where_condition = f' WHERE s.id >= {start_school_id}'
    elif end_school_id:
        where_condition = f' WHERE s.id <= {end_school_id}'

    query = query.format(where_condition=where_condition)

    print('Getting select statement query result from "schools_with_admin_data" table for Admin1 records.')
    print('Query: {}'.format(query))
    data_list = []

    with connection.cursor() as cursor:
        cursor.execute(query)
        description = cursor.description
        for row in cursor:
            data_dict = OrderedDict()
            for index, data in enumerate(row):
                data_dict[description[index][0]] = data
            data_list.append(data_dict)

    create_and_execute_update_query('admin1_id', data_list)

    te = timezone.now()
    print('Executed the function in {} seconds'.format((te - ts).seconds))


def populate_school_admin2_data(start_school_id, end_school_id):
    """ """
    ts = timezone.now()

    query = """
    SELECT ca.id as admin2_id,
       ca.name as admin2_name,
       ca.name_en as admin2_name_en,
       t.giga_id_school as from_t_giga_id,
       c.id as country_id,
       c.name as country_name,
       s.id as school_id,
       s.giga_id_school as from_school_giga_id
    FROM public.schools_with_admin_data t
    INNER JOIN public.locations_country c ON (LOWER(c.code) = LOWER(t.country_iso2_code)
                                              OR LOWER(c.iso3_format) = LOWER(t.country_iso3_code))
                                              AND c."deleted" IS NULL
    INNER JOIN public.schools_school s ON s.giga_id_school = t.giga_id_school AND s.country_id = c.id
    INNER JOIN public.locations_countryadminmetadata ca ON ca.giga_id_admin = t.admin2_id_giga
        AND ca.layer_name = 'adm2'
        AND ca.country_id = c.id
    {where_condition} ORDER BY s.id ASC
    """
    where_condition = ''
    if start_school_id and end_school_id:
        where_condition = f' WHERE s.id >= {start_school_id} AND s.id <= {end_school_id}'
    elif start_school_id:
        where_condition = f' WHERE s.id >= {start_school_id}'
    elif end_school_id:
        where_condition = f' WHERE s.id <= {end_school_id}'

    query = query.format(where_condition=where_condition)

    print('Getting select statement query result from "schools_with_admin_data" table for Admin2.')
    print('Query: {}'.format(query))
    data_list = []

    with connection.cursor() as cursor:
        cursor.execute(query)
        description = cursor.description
        for row in cursor:
            data_dict = OrderedDict()
            for index, data in enumerate(row):
                data_dict[description[index][0]] = data
            data_list.append(data_dict)

    create_and_execute_update_query('admin2_id', data_list)

    te = timezone.now()
    print('Executed the function in {} seconds'.format((te - ts).seconds))


class Command(BaseCommand):
    help = 'Populate the School table with Admin1 and Admin2 data.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-start_school_id', dest='start_school_id', required=False, type=int,
            help='Pass the school id in case want to control the update.'
        )
        parser.add_argument(
            '-end_school_id', dest='end_school_id', required=False, type=int,
            help='Pass the school id in case want to control the update.'
        )

        parser.add_argument(
            '-at',
            '--admin-type',
            type=str,
            dest='admin_type',
            help='admin1 or admin2.',
        )

    def handle(self, **options):
        start_school_id = options.get('start_school_id')
        end_school_id = options.get('end_school_id')

        admin_type = options.get('admin_type', 'both')
        if admin_type not in ['admin1', 'admin2', 'both']:
            sys.exit("Mandatory argument '--admin-type/-at' is missing. Available options: {0}".format(
                ['admin1', 'admin2', 'both']))

        print('*** School update operation STARTED ({0} - {1}) ***'.format(start_school_id, end_school_id))

        if admin_type == 'admin1':
            populate_school_admin1_data(start_school_id, end_school_id)
        elif admin_type == 'admin2':
            populate_school_admin2_data(start_school_id, end_school_id)
        else:
            populate_school_admin1_data(start_school_id, end_school_id)
            populate_school_admin2_data(start_school_id, end_school_id)

        print('Data loaded successfully!\n')
