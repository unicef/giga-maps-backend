import logging
from collections import defaultdict
from django.conf import settings
from proco.core import utils as core_utilities
from proco.core.config import app_config as core_configs
from proco.locations.models import Country, CountryAdminMetadata
from proco.data_sources import models as sources_models
from proco.connection_statistics import models as statistics_models
from proco.utils.dates import format_datetime

logger = logging.getLogger('gigamaps.' + __name__)


# School & SchoolMasterData model field mappings (Key: School, Value: SchoolMasterData)
SCHOOL_SCHOOLMASTERDATA_FIELD_MAPPING = {
    'giga_id_school': 'school_id_giga',
    'external_id': 'school_id_govt',
    'name': 'school_name',
    'education_level': 'education_level',
    'education_level_govt': 'education_level_govt',
    'environment': 'school_area_type',
    'school_type': 'school_funding_type',
    'establishment_year': 'school_establishment_year',
}

# SchoolWeeklyStatus & SchoolMasterData field mappings (Key: SchoolWeeklyStatus, Value: SchoolMasterData)
SCHOOLWEEKLYSTATUS_SCHOOLMASTERDATA_FIELD_MAPPING = {
    'num_students': 'num_students',
    'num_teachers': 'num_teachers',
    'num_classroom': 'num_classrooms',
    'num_latrines': 'num_latrines',
    'running_water': 'water_availability',
    'electricity_availability': 'electricity_availability',
    'computer_lab': 'computer_lab',
    'num_computers': 'num_computers',
    'connectivity': 'connectivity_govt',
    'connectivity_type': 'connectivity_type_govt',
    'coverage_availability': 'cellular_coverage_availability',
    'coverage_type': 'cellular_coverage_type',
    'download_speed_contracted': 'download_speed_contracted',
    'num_computers_desired': 'num_computers_desired',
    'electricity_type': 'electricity_type',
    'num_adm_personnel': 'num_adm_personnel',
    'fiber_node_distance': 'fiber_node_distance',
    'microwave_node_distance': 'microwave_node_distance',
    'schools_within_1km': 'schools_within_1km',
    'schools_within_2km': 'schools_within_2km',
    'schools_within_3km': 'schools_within_3km',
    'nearest_lte_distance': 'nearest_LTE_distance',
    'nearest_umts_distance': 'nearest_UMTS_distance',
    'nearest_gsm_distance': 'nearest_GSM_distance',
    'nearest_nr_distance': 'nearest_NR_distance',
    'pop_within_1km': 'pop_within_1km',
    'pop_within_2km': 'pop_within_2km',
    'pop_within_3km': 'pop_within_3km',
    'school_data_source': 'school_data_source',
    'school_data_collection_year': 'school_data_collection_year',
    'school_data_collection_modality': 'school_data_collection_modality',
    'disputed_region': 'disputed_region',
    'download_speed_benchmark': 'download_speed_benchmark',
    'num_students_girls': 'num_students_girls',
    'num_students_boys': 'num_students_boys',
    'num_students_other': 'num_students_other',
    'num_teachers_female': 'num_teachers_female',
    'num_teachers_male': 'num_teachers_male',
    'num_tablets': 'num_tablets',
    'num_robotic_equipment': 'num_robotic_equipment',
    'computer_availability': 'computer_availability',
    'teachers_trained': 'teachers_trained',
    'sustainable_business_model': 'sustainable_business_model',
    'device_availability': 'device_availability',
    'building_id_govt': 'building_id_govt',
    'num_schools_per_building': 'num_schools_per_building'
}

# SchoolRealTimeRegistration & SchoolMasterData field mappings (Key: SchoolRealTimeRegistration, Value: SchoolMasterData)
SCHOOLREALTIMEREGISTRATION_SCHOOLMASTERDATA_FIELD_MAPPING = {
    'rt_registered': 'connectivity_RT',
    'rt_source': 'connectivity_RT_datasource',
    'rt_registration_date': 'connectivity_RT_ingestion_timestamp'
}

def calculate_school_master_delta_changes(school_master_data, country):
    """Calculate Delta for School Master Static Data Sync"""
    try:
        if not school_master_data.exists():
            logger.error(f'calculate_school_master_delta_changes - No SchoolMasterData found for {country.iso3_format}')
            return None

        # Initialize change tracking
        changes = {
            'school_model_changes': defaultdict(),
            'school_weekly_changes': defaultdict(),
            'rt_registration_changes': defaultdict(),
            'pulled_at_datetime': None,
            'new_schools': 0,
            'updated_schools': 0,
            'deleted_schools': 0
        }
        changes['pulled_at_datetime'] = school_master_data.first().pulled_at
        if changes['pulled_at_datetime']:
            changes['pulled_at_datetime'] = format_datetime(changes['pulled_at_datetime'])

        for data_chunk in core_utilities.queryset_iterator(school_master_data, chunk_size=100, print_msg=False):
            for row in data_chunk:
                if row.school_id is None:
                    # New Schools
                    changes['new_schools'] += 1
                    continue

                if row.status == sources_models.SchoolMasterData.ROW_STATUS_DELETED:
                    # Deleted Schools
                    changes['deleted_schools'] += 1
                    continue

                school_changes = compare_target_model_changes(row, row.school)

                if any(school_changes.values()):
                    changes['updated_schools'] += 1

                    # Increase column count
                    for model_type, field_changes in school_changes.items():
                        for field, count in field_changes.items():
                            changes[f'{model_type}_changes'].setdefault(field, 0)
                            changes[f'{model_type}_changes'][field] += count

        return format_changes_for_slack(country, changes)
    except Exception as e:
        logger.error(f'Error calculating delta changes for {country.iso3_format}: {str(e)}')
        return None

def compare_target_model_changes(current_row, school):
    """
    target_field: School, SchoolWeeklyStatus, SchoolRealTimeRegistration
    source_field: SchoolMasterData
    """
    changes = {
        'school_model': {},
        'school_weekly': {},
        'rt_registration': {}
    }

    if not school:
        return changes

    # Handle Scpecial Case for Lat/Lon & admin1_id_giga/admin2_id_giga School Model
    if current_row.longitude != school.geopoint.x:
        changes['school_model']['longitude'] = 1
    if current_row.latitude != school.geopoint.y:
        changes['school_model']['latitude'] = 1

    admin1_instance = None
    if not core_utilities.is_blank_string(current_row.admin1_id_giga):
        admin1_instance = CountryAdminMetadata.objects.filter(
            country=current_row.country,
            giga_id_admin=current_row.admin1_id_giga,
            layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN1,
        ).first()

    admin2_instance = None
    if not core_utilities.is_blank_string(current_row.admin2_id_giga):
        admin2_instance = CountryAdminMetadata.objects.filter(
            country=current_row.country,
            giga_id_admin=current_row.admin2_id_giga,
            layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN2,
        ).first()
    if admin1_instance != school.admin1:
        changes['school_model']['admin1_id_giga'] = 1

    if admin2_instance != school.admin2:
        changes['school_model']['admin2_id_giga'] = 1

    # Check School model changes
    for target_field, source_field in SCHOOL_SCHOOLMASTERDATA_FIELD_MAPPING.items():
        current_val = getattr(current_row, source_field, None)
        previous_val = getattr(school, target_field, None)

        if is_different_value(current_val, previous_val, source_field):
            changes['school_model'][source_field] = 1

    # Check SchoolWeeklyStatus changes
    if school.last_weekly_status:
        for target_field, source_field in SCHOOLWEEKLYSTATUS_SCHOOLMASTERDATA_FIELD_MAPPING.items():
            current_val = getattr(current_row, source_field, None)
            previous_val = getattr(school.last_weekly_status, target_field, None)

            if is_different_value(current_val, previous_val, source_field):
                changes['school_weekly'][source_field] = 1
    else:
        # school_weekly obj doesn't exist make all columns as updated if there value exists
        for target_field, source_field in SCHOOLWEEKLYSTATUS_SCHOOLMASTERDATA_FIELD_MAPPING.items():
            current_val = getattr(current_row, source_field, None)
            if not core_utilities.is_blank_string(current_val):
                changes['school_weekly'][source_field] = 1

    # Check SchoolRealTimeRegistration changes
    school_rt = statistics_models.SchoolRealTimeRegistration.objects.filter(school=school).last()
    if school_rt:
        for target_field, source_field in SCHOOLREALTIMEREGISTRATION_SCHOOLMASTERDATA_FIELD_MAPPING.items():
            current_val = getattr(current_row, source_field, None)
            previous_val = getattr(school_rt, target_field, None)

            if is_different_value(current_val, previous_val, source_field):
                changes['rt_registration'][source_field] = 1
    else:
        rt_registered = None
        if not core_utilities.is_blank_string(current_row.connectivity_RT):
            rt_registered = str(current_row.connectivity_RT).lower() in core_configs.true_choices
        if rt_registered is not None and current_row.connectivity_RT_ingestion_timestamp is not None:
            # school_rt obj doesn't exist make all columns as updated if there value exists
            for target_field, source_field in SCHOOLREALTIMEREGISTRATION_SCHOOLMASTERDATA_FIELD_MAPPING.items():
                changes['rt_registration'][source_field] = 1
    return changes


def is_different_value(new_value, previous_value, field_name):
    """Check if values are same or different"""
    # Special Cases
    # Case 1: Modify value based on environment_map
    if field_name == 'school_area_type':
        environment_map = { 'urban': 'urban', 'urbana': 'urban', 'rural': 'rural'}
        environment = new_value.lower() if not core_utilities.is_blank_string(new_value) else ''
        environment = environment_map.get(environment, '')
        if previous_value == environment:
            return False

    # Case 2: Modify value as either empty string or actual value if valid
    if field_name in ['education_level', 'school_type']:
        new_calculated_value = '' if core_utilities.is_blank_string(new_value) else new_value
        if previous_value == new_calculated_value:
            return False

    # Case 3: Modify value as True / False
    if field_name in ['water_availability', 'electricity_availability', 'computer_lab', 'disputed_region']:
        new_calculated_value = False if core_utilities.is_blank_string(new_value) else str(new_value).lower() in core_configs.true_choices
        if previous_value == new_calculated_value:
            return False

    # Case 4: Modify value as either None or True or False
    if field_name in [
        'connectivity_govt', 'cellular_coverage_availability', 'computer_availability',
        'teachers_trained', 'sustainable_business_model', 'device_availability', 'connectivity_RT'
    ]:
        new_calculated_value = None if core_utilities.is_blank_string(new_value) else str(new_value).lower() in core_configs.true_choices
        if previous_value == new_calculated_value:
            return False

    # Case 5: Modify value as value if valid else unknown
    if field_name == 'connectivity_type_govt':
        new_calculated_value = new_value if new_value else 'unknown'
        if previous_value == new_calculated_value:
            return False

    # Case 6: Modify value for cellular_coverage_type
    if field_name == 'cellular_coverage_type':
        new_calculated_value = statistics_models.SchoolWeeklyStatus.COVERAGE_UNKNOWN
        if not core_utilities.is_blank_string(new_value):
            new_calculated_value = str(new_value).lower()
        if new_calculated_value in ['no service', 'no coverage', 'no']:
            new_calculated_value = statistics_models.SchoolWeeklyStatus.COVERAGE_NO
        if previous_value == new_calculated_value:
            return False

    # Case 7: Modify value for download_speed_benchmark
    if field_name == 'download_speed_benchmark':
        new_calculated_value = new_value * 1000 * 1000 if new_value else None
        if previous_value == new_calculated_value:
            return False

    # Case 8: Exact Copy
    if previous_value == new_value:
        return False
    return True

def format_changes_for_slack(country, changes):
    """Format the comprehensive changes into the expected output format."""

    # Combine all field changes for the column_changes list
    all_changes = []

    for model_type in ['school_model_changes', 'school_weekly_changes', 'rt_registration_changes']:
        for field, count in changes[model_type].items():
            all_changes.append({
                'column': f"{field}",
                'operation': 'update',
                'count': count
            })

    return {
        'country': country.name,
        'pulled_at': changes['pulled_at_datetime'],
        'updated': format_datetime(core_utilities.get_current_datetime_object()),
        'new_rows_count': changes['new_schools'],
        'updated_rows_count': changes['updated_schools'],
        'deleted_rows_count': changes['deleted_schools'],
        'modified': changes['updated_schools'] + changes['deleted_schools'],
        'column_changes': all_changes
    }
