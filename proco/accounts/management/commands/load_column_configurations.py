import sys

from django.core.management.base import BaseCommand

from proco.accounts import models as accounts_models
from proco.core.utils import get_current_datetime_object

configuration_json = [
    {
        'name': 'environment',
        'label': 'Region (environment)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'schools_school',
        'table_alias': 'schools',
        'table_label': 'School',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "LOWER(environment) IN ('urban', 'rural')",
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
        'name': 'school_type',
        'label': 'School Type (school_type)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'schools_school',
        'table_alias': 'schools',
        'table_label': 'School',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "LOWER(school_type) IN ('private', 'public')",
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
        }
    },
    {
        'name': 'education_level',
        'label': 'Education Level (education_level)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'schools_school',
        'table_alias': 'schools',
        'table_label': 'School',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "LOWER(education_level) IN ('primary', 'secondary')",
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
        }
    },
    {
        'name': 'num_computers',
        'label': 'Number of Computers (num_computers)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_computers IS NOT NULL AND num_computers > 0",
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
        'name': 'num_students',
        'label': 'Number of Students (num_students)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_students IS NOT NULL AND num_students > 0",
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
        'name': 'num_teachers',
        'label': 'Number of Teachers (num_teachers)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_teachers IS NOT NULL AND num_teachers > 0",
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
        'name': 'connectivity_speed',
        'label': 'Download Speed (connectivity_speed)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
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
    {
        'name': 'computer_lab',
        'label': 'Has Computer Lab (computer_lab)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "computer_lab = true",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'connectivity',
        'label': 'Has Live Connectivity (connectivity)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "connectivity IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'coverage_type',
        'label': 'Coverage Type (coverage_type)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter':
                """LOWER("connection_statistics_schoolweeklystatus"."coverage_type") != 'unknown'""",
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
        'name': 'connectivity_type',
        'label': 'Connectivity Type (connectivity_type)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter':
                """LOWER("connection_statistics_schoolweeklystatus"."connectivity_type") != 'unknown'""",
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
        'name': 'fiber_node_distance',
        'label': 'Fiber Node Distance (fiber_node_distance)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "fiber_node_distance IS NOT NULL AND fiber_node_distance > 0",
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
        'name': 'microwave_node_distance',
        'label': 'Microwave Node Distance (microwave_node_distance)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "microwave_node_distance IS NOT NULL AND microwave_node_distance > 0",
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
        'name': 'nearest_nr_distance',
        'label': 'Nearest NR Distance (nearest_nr_distance)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "nearest_nr_distance IS NOT NULL AND nearest_nr_distance > 0",
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
        'name': 'nearest_lte_distance',
        'label': 'Nearest LTE Distance (nearest_lte_distance)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "nearest_lte_distance IS NOT NULL AND nearest_lte_distance > 0",
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
        'name': 'nearest_umts_distance',
        'label': 'Nearest UMTS Distance (nearest_umts_distance)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "nearest_umts_distance IS NOT NULL AND nearest_umts_distance > 0",
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
        'name': 'nearest_gsm_distance',
        'label': 'Nearest GSM Distance (nearest_gsm_distance)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "nearest_gsm_distance IS NOT NULL AND nearest_gsm_distance > 0",
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
        'name': 'computer_availability',
        'label': 'Computer Availability (computer_availability)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "computer_availability IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'num_students_girls',
        'label': 'Number of Girl Students (num_students_girls)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_students_girls IS NOT NULL AND num_students_girls > 0",
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
        'name': 'num_students_boys',
        'label': 'Number of Boy Students (num_students_boys)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_students_boys IS NOT NULL AND num_students_boys > 0",
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
        'name': 'num_students_other',
        'label': 'Number of Other Students (num_students_other)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_students_other IS NOT NULL AND num_students_other > 0",
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
        'name': 'num_teachers_female',
        'label': 'Number of Female Teachers (num_teachers_female)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_teachers_female IS NOT NULL AND num_teachers_female > 0",
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
        'name': 'num_teachers_male',
        'label': 'Number of Male Teachers (num_teachers_male)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_teachers_male IS NOT NULL AND num_teachers_male > 0",
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
        'name': 'teachers_trained',
        'label': 'Trained Teachers (teachers_trained)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "teachers_trained IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'sustainable_business_model',
        'label': 'Sustainable Business Model (sustainable_business_model)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "sustainable_business_model IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'device_availability',
        'label': 'Device Availability (device_availability)',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "device_availability IS NOT NULL",
            'applicable_filter_types': {
                accounts_models.AdvanceFilter.TYPE_BOOLEAN: [
                    accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
                ],
            },
        },
    },
    {
        'name': 'num_tablets',
        'label': 'Number of Tablets (num_tablets)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_tablets IS NOT NULL AND num_tablets > 0",
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
        'name': 'num_robotic_equipment',
        'label': 'Number of Robotic Equipment (num_robotic_equipment)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_robotic_equipment IS NOT NULL AND num_robotic_equipment > 0",
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
        'name': 'building_id_govt',
        'label': 'Building Govt ID (building_id_govt)',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter':
                """"connection_statistics_schoolweeklystatus"."building_id_govt" IS NOT NULL""",
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
        'name': 'num_schools_per_building',
        'label': 'Number of Schools per Building (num_schools_per_building)',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter': "num_schools_per_building IS NOT NULL AND num_schools_per_building > 0",
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


def update_configurations_data():
    for row_data in configuration_json:
        try:
            instance, created = accounts_models.ColumnConfiguration.objects.update_or_create(
                name=row_data['name'],
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
                sys.stdout.write('\nNew Column configuration created: {}'.format(instance.__dict__))
            else:
                sys.stdout.write('\nExisting Column configuration updated: {}'.format(instance.__dict__))
        except:
            pass


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--update_configurations', action='store_true', dest='update_configurations', default=False,
            help='If provided, already created Column configuration data will be updated again.'
        )

    def handle(self, **options):
        sys.stdout.write('\nLoading Configurations data....')

        if options.get('update_configurations', False):
            update_configurations_data()

        sys.stdout.write('\nData loaded successfully!\n')
