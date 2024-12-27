import logging

from django.core.management.base import BaseCommand
from django.db import connection
from django.db import transaction
from django.utils import timezone
from proco.data_sources.models import SchoolMasterData


logger = logging.getLogger('gigamaps.' + __name__)


@transaction.atomic
def create_and_execute_update_query(stmt):
    logger.debug('Current update query: {}'.format(stmt))
    with connection.cursor() as cursor:
        cursor.execute(stmt)


# 1. Set the latest version in country table for all countries
def populate_country_latest_school_master_data_version(country_id, start_country_id, end_country_id):
    logger.info(f'Running the UPDATE query on Country table to set the "latest_school_master_data_version" '
                f'field for country: {country_id}')

    ts = timezone.now()

    query = """
        WITH r AS
        (
            SELECT country_id,
                MAX(version) AS max_version
            FROM data_sources_schoolmasterdata
            {country_condition}
            GROUP BY country_id
            ORDER BY country_id ASC
        )
        UPDATE locations_country AS t
            SET latest_school_master_data_version = r.max_version
        FROM r
        WHERE t.deleted IS NULL AND t.id = r.country_id
    """

    country_condition = ''
    if country_id:
        country_condition = f' WHERE country_id = {country_id}'

    elif start_country_id:
        country_condition = f' WHERE country_id >= {start_country_id}'

        if end_country_id:
            country_condition += f' AND country_id <= {end_country_id}'
    elif end_country_id:
        country_condition = f' WHERE country_id <= {end_country_id}'

    query = query.format(country_condition=country_condition)

    create_and_execute_update_query(query)

    te = timezone.now()
    logger.debug('Executed the function in {} seconds'.format((te - ts).seconds))


# 2. Copy all published rows into SchoolMasterStatus table and set latest field id into School table
def populate_last_master_status_id_field_for_schools(country_id):
    logger.info(f'Running the UPDATE query on School table to set the "last_master_status_id" field'
                f' for country: {country_id}')

    ts = timezone.now()

    query = """
        WITH r AS
        (
            SELECT DISTINCT ON(sms.school_id) school_id,
                sms.id,
                sms.version
            FROM schools_schoolmasterstatus sms
            INNER JOIN schools_school s ON sms.school_id = s.id
            WHERE sms.deleted IS NULL
                AND s.deleted IS NULL
                AND s.country_id = {country_id}
            ORDER BY school_id ASC, sms.version DESC
        )
        UPDATE schools_school AS t
            SET last_master_status_id = r.id
        FROM r
        WHERE t.id = r.school_id
    """

    query = query.format(country_id=country_id)

    create_and_execute_update_query(query)

    te = timezone.now()
    logger.debug('Executed the function in {} seconds'.format((te - ts).seconds))

def populate_school_master_status_model_data(country_id, start_country_id, end_country_id, exclude_country_ids):
    logger.info(f'Running the INSERT query on SchoolMasterStatus table to set the fields for country: {country_id}')

    ts = timezone.now()

    country_ids = []
    if country_id:
        country_ids.append(country_id)
    else:
        country_id_qs = SchoolMasterData.objects.filter(
            status = 'PUBLISHED',
            is_read = True,
            school_id__isnull=False,
        ).values_list('country_id', flat=True).order_by('country_id').distinct('country_id')

        if start_country_id:
            country_id_qs = country_id_qs.filter(country_id__gte=start_country_id)

        if end_country_id:
            country_id_qs = country_id_qs.filter(country_id__lte=end_country_id)

        country_ids = list(country_id_qs)

    for c_id in country_ids:
        if country_id in exclude_country_ids:
            continue

        logger.info('Running the SYNC query for country ID: {}'.format(c_id))

        query = """
            INSERT INTO schools_schoolmasterstatus
            (
            school_id,
            establishment_year, download_speed_contracted, electricity_type,
            num_adm_personnel, num_students, num_teachers, num_classrooms, num_latrines, num_computers_desired,
            num_computers,
            num_robotic_equipment, num_students_boys, num_students_girls, num_students_other, num_tablets,
            num_teachers_female, num_teachers_male,
            connectivity_type_govt,
            connectivity, connectivity_govt_ingestion_timestamp,
            connectivity_govt_collection_year,
            data_source, data_collection_year, data_collection_modality,
            fiber_node_distance, microwave_node_distance,
            schools_within_1km, schools_within_2km, schools_within_3km,
            nearest_lte_distance, nearest_umts_distance, nearest_gsm_distance, nearest_nr_distance,
            pop_within_1km, pop_within_2km, pop_within_3km,
            location_ingestion_timestamp,
            building_id_govt, num_schools_per_building,
            water_availability, electricity_availability, computer_lab, connectivity_govt, disputed_region,
            download_speed_benchmark, computer_availability, device_availability, sustainable_business_model,
            teachers_trained,
             version, last_modified_at, created, last_modified_by_id, created_by_id
            )
            SELECT
                DISTINCT ON(school_id) school_id,
                school_establishment_year, download_speed_contracted, electricity_type,
                num_adm_personnel, num_students, num_teachers, num_classrooms, num_latrines, num_computers_desired,
                 num_computers,
                num_robotic_equipment, num_students_boys, num_students_girls, num_students_other, num_tablets,
                 num_teachers_female, num_teachers_male,
                connectivity_type_govt,
                CASE
                    WHEN connectivity IS NULL THEN NULL
                    WHEN LOWER(connectivity) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS connectivity, connectivity_govt_ingestion_timestamp,
                 connectivity_govt_collection_year,
                school_data_source, school_data_collection_year, school_data_collection_modality,
                fiber_node_distance, microwave_node_distance,
                schools_within_1km, schools_within_2km, schools_within_3km,
                "nearest_LTE_distance", "nearest_UMTS_distance", "nearest_GSM_distance", "nearest_NR_distance",
                pop_within_1km, pop_within_2km, pop_within_3km,
                school_location_ingestion_timestamp,
                building_id_govt, num_schools_per_building,
                CASE
                    WHEN water_availability IS NULL THEN NULL
                    WHEN LOWER(water_availability) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS water_availability,
                CASE
                    WHEN electricity_availability IS NULL THEN NULL
                    WHEN LOWER(electricity_availability) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS electricity_availability,
                CASE
                    WHEN computer_lab IS NULL THEN NULL
                    WHEN LOWER(computer_lab) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS computer_lab,
                CASE
                    WHEN connectivity_govt IS NULL THEN NULL
                    WHEN LOWER(connectivity_govt) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS connectivity_govt,
                CASE
                    WHEN LOWER(disputed_region) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS disputed_region,
                CASE
                    WHEN download_speed_benchmark IS NULL THEN NULL
                    ELSE download_speed_benchmark * 1000 * 100
                END AS download_speed_benchmark,
                CASE
                    WHEN computer_availability IS NULL THEN NULL
                    WHEN LOWER(computer_availability) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS computer_availability,
                CASE
                    WHEN device_availability IS NULL THEN NULL
                    WHEN LOWER(device_availability) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS device_availability,
                CASE
                    WHEN sustainable_business_model IS NULL THEN NULL
                    WHEN LOWER(sustainable_business_model) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS sustainable_business_model,
                CASE
                    WHEN teachers_trained IS NULL THEN NULL
                    WHEN LOWER(teachers_trained) IN ('true', 'yes', '1') THEN true
                    ELSE false
                END AS teachers_trained,
                version, published_at, published_at, published_by_id, published_by_id
            FROM data_sources_schoolmasterdata
            WHERE status = 'PUBLISHED'
                AND is_read = true
                AND school_id IS NOT NULL
                AND country_id = {country_id}
            ORDER BY school_id ASC, VERSION DESC
            ON CONFLICT DO NOTHING;
        """

        query = query.format(country_id=c_id)

        create_and_execute_update_query(query)

        populate_last_master_status_id_field_for_schools(c_id)

    te = timezone.now()
    logger.debug('Executed the function in {} seconds'.format((te - ts).seconds))

# 3. Delete all the weekly rows where daily data row not present


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '-country_id', dest='country_id', required=False, type=int,
            help='Pass the Country ID in case want to control the update.'
        )

        parser.add_argument(
            '-start_country_id', dest='start_country_id', required=False, type=int,
            help='Pass the country id in case want to control the update.'
        )
        parser.add_argument(
            '-end_country_id', dest='end_country_id', required=False, type=int,
            help='Pass the country id in case want to control the update.'
        )

        parser.add_argument(
            '-exclude_country_ids', dest='exclude_country_ids', required=False, type=str,
            help='Pass the Country IDs in case want to skip these ids.'
        )

        parser.add_argument(
            '--populate_country_latest_school_master_data_version',
            action='store_true',
            dest='populate_country_latest_school_master_data_version',
            default=False,
            help='If provided, School Master version for the country/s.'
        )

        parser.add_argument(
            '--populate_school_master_status_model_data',
            action='store_true',
            dest='populate_school_master_status_model_data',
            default=False,
            help='If provided, SchoolMasterStatus table will be updated with latest PUBLISHED records for the country.'
        )

    def handle(self, **options):
        logger.info('Executing "populate_school_master_data_for_all_schools" utility.\n')
        logger.info('Options: {}\n\n'.format(options))

        country_id = options.get('country_id', None)
        start_country_id = options.get('start_country_id', None)
        end_country_id = options.get('end_country_id', None)

        exclude_country_ids = options.get('exclude_country_ids', None)
        if exclude_country_ids:
            exclude_country_ids = [int(val) for val in str(exclude_country_ids).split(',')]
        else:
            exclude_country_ids = []

        if options.get('populate_country_latest_school_master_data_version'):
            logger.info('Populating country latest school master data version field from SchoolMasterData table.')
            populate_country_latest_school_master_data_version(country_id, start_country_id, end_country_id)
            logger.info('Completed country latest school master data version field from SchoolMasterData table.\n\n')

        if options.get('populate_school_master_status_model_data'):
            logger.info('Populating populate_school_master_status_model_data from SchoolMasterData table.')
            populate_school_master_status_model_data(country_id, start_country_id, end_country_id, exclude_country_ids)
            logger.info('Completed country latest school master data version field from SchoolMasterData table.\n\n')

        logger.info('Completed utility successfully.\n')
