import sys

from django.core.management.base import BaseCommand

from proco.accounts import models as accounts_models
from proco.entities.models import EntityType
from proco.core.utils import get_current_datetime_object

# Health Entity Column Configurations
health_configuration_json = [
    # Entity table fields (entities_entity)
    {
        'name': 'environment',
        'label': 'Region (environment)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'entities_entity',
        'table_alias': 'entities',
        'table_label': 'Entity',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "environment IN ('urban', 'rural')",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_DROPDOWN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IEXACT,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT
                ],
                accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_CONTAINS,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ICONTAINS
                ],
            },
        },
    },
    {
        'name': 'water_availability',
        'label': 'Water Availability (water_availability)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'entities_entity',
        'table_alias': 'entities',
        'table_label': 'Entity',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "water_availability IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'electricity_availability',
        'label': 'Electricity Availability (electricity_availability)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'entities_entity',
        'table_alias': 'entities',
        'table_label': 'Entity',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "electricity_availability IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'electricity_type',
        'label': 'Electricity Type (electricity_type)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'entities_entity',
        'table_alias': 'entities',
        'table_label': 'Entity',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "electricity_type IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_DROPDOWN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IEXACT,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT
                ],
                accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_CONTAINS,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ICONTAINS
                ],
            },
        },
    },
    {
        'name': 'connectivity_govt',
        'label': 'Government Connectivity (connectivity_govt)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'entities_entity',
        'table_alias': 'entities',
        'table_label': 'Entity',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "connectivity_govt IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },

    # HealthEntity table fields (entities_health_entity)
    {
        'name': 'facility_type',
        'label': 'Facility Type (facility_type)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "facility_type IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_DROPDOWN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IEXACT,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT
                ],
                accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_CONTAINS,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ICONTAINS
                ],
            },
        },
    },
    {
        'name': 'facility_ownership',
        'label': 'Facility Ownership (facility_ownership)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "facility_ownership IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_DROPDOWN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IEXACT,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT
                ],
                accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_CONTAINS,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ICONTAINS
                ],
            },
        },
    },
    {
        'name': 'facility_level',
        'label': 'Facility Level (facility_level)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "facility_level IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_DROPDOWN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IEXACT,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT
                ],
                accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_CONTAINS,
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ICONTAINS
                ],
            },
        },
    },
    {
        'name': 'is_facility_open',
        'label': 'Facility Open (is_facility_open)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "is_facility_open IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'num_beds_total',
        'label': 'Total Beds (num_beds_total)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_beds_total IS NOT NULL AND num_beds_total > 0",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_RANGE: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT,
                ],
            },
        },
    },
    {
        'name': 'num_beds_icu',
        'label': 'ICU Beds (num_beds_icu)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_beds_icu IS NOT NULL AND num_beds_icu > 0",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_RANGE: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT,
                ],
            },
        },
    },
    {
        'name': 'num_healthworkers',
        'label': 'Number of Health Workers (num_healthworkers)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_healthworkers IS NOT NULL AND num_healthworkers > 0",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_RANGE: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT,
                ],
            },
        },
    },
    {
        'name': 'staff_doctors',
        'label': 'Number of Doctors (staff_doctors)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "staff_doctors IS NOT NULL AND staff_doctors > 0",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_RANGE: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT,
                ],
            },
        },
    },
    {
        'name': 'staff_nurses',
        'label': 'Number of Nurses (staff_nurses)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "staff_nurses IS NOT NULL AND staff_nurses > 0",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_RANGE: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT,
                ],
            },
        },
    },
    {
        'name': 'emergency_services_available',
        'label': 'Emergency Services (emergency_services_available)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "emergency_services_available IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'power_backup_system',
        'label': 'Power Backup System (power_backup_system)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "power_backup_system IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'cold_chain_available',
        'label': 'Cold Chain Available (cold_chain_available)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "cold_chain_available IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'catchment_population',
        'label': 'Catchment Population (catchment_population)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'entities_health_entity',
        'table_alias': 'health',
        'table_label': 'Health Facility',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "catchment_population IS NOT NULL AND catchment_population > 0",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_RANGE: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT,
                ],
            },
        },
    },

    # EntityWeeklyStatus table fields (connection_statistics_entityweeklystatus)
    {
        'name': 'connectivity_speed',
        'label': 'Download Speed (connectivity_speed)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_entityweeklystatus',
        'table_alias': 'entity_static',
        'table_label': 'Connectivity Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "connectivity_speed IS NOT NULL",
            'downcast_aggr_str': '{val} / (1000 * 1000)',
            'upcast_aggr_str': '{val} * 1000 * 1000',
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_RANGE: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
                ],
                accounts_models.AdvanceFilter.TYPE_INPUT: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_EXACT,
                ],
            },
        },
    },
]


def update_health_configurations_data():
    """
    Create or update health entity column configurations.
    Links them to the 'health' EntityType.
    """
    try:
        health_entity_type = EntityType.objects.get(code='health')
    except EntityType.DoesNotExist:
        sys.stdout.write('\n✗ Error: Health EntityType not found. Please run seed_entity_types command first.')
        return

    for row_data in health_configuration_json:
        try:
            instance, created = accounts_models.ColumnConfiguration.objects.update_or_create(
                name=row_data['name'],
                entity_type=health_entity_type,
                defaults={
                    'label': row_data['label'],
                    'type': row_data['type'],
                    'description': row_data['description'],
                    'table_name': row_data['table_name'],
                    'table_alias': row_data['table_alias'],
                    'table_label': row_data['table_label'],
                    'is_filter_applicable': row_data['is_filter_applicable'],
                    'options': row_data['options'],
                    'last_modified_at': get_current_datetime_object(),
                },
            )
            if created:
                sys.stdout.write(f'\n✓ Created health column configuration: {instance.name}')
            else:
                sys.stdout.write(f'\n✓ Updated health column configuration: {instance.name}')
        except Exception as e:
            sys.stdout.write(f'\n✗ Error processing {row_data["name"]}: {e}')


class Command(BaseCommand):
    help = 'Load health entity column configurations'

    def handle(self, **options):
        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\nLoading Health Entity Column Configurations...')
        sys.stdout.write('\n' + '=' * 60)

        update_health_configurations_data()

        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\n✓ Health entity column configurations loaded successfully!')
        sys.stdout.write('\n' + '=' * 60 + '\n')
