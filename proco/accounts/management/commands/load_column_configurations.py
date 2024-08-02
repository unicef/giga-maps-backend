import sys

from django.core.management.base import BaseCommand

from proco.accounts import models as accounts_models
from proco.core.utils import get_current_datetime_object

configuration_json = [
    {
        'name': 'environment',
        'label': 'Region',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'schools_school',
        'table_alias': 'schools',
        'table_label': 'School',
        'is_filter_applicable': True,
        'options': {'active_countries_filter': "LOWER(environment) IN ('urban', 'rural')"},
    },
    {
        'name': 'school_type',
        'label': 'School Type',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'schools_school',
        'table_alias': 'schools',
        'table_label': 'School',
        'is_filter_applicable': True,
        'options': {'active_countries_filter': "LOWER(school_type) IN ('private', 'public')"}
    },
    {
        'name': 'education_level',
        'label': 'Education Level',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'schools_school',
        'table_alias': 'schools',
        'table_label': 'School',
        'is_filter_applicable': True,
        'options': {'active_countries_filter': "LOWER(education_level) IN ('primary', 'secondary')"}
    },
    {
        'name': 'num_computers',
        'label': '# of Computers',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {'active_countries_filter': "num_computers IS NOT NULL AND num_computers > 0"},
    },
    {
        'name': 'num_students',
        'label': '# of Students',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {'active_countries_filter': "num_students IS NOT NULL AND num_students > 0"},
    },
    {
        'name': 'num_teachers',
        'label': '# of Teachers',
        'type': accounts_models.ColumnConfiguration.TYPE_INT,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {'active_countries_filter': "num_teachers IS NOT NULL AND num_teachers > 0"},
    },
    {
        'name': 'connectivity_speed',
        'label': 'Download Speed',
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
        },
    },
    {
        'name': 'computer_lab',
        'label': 'Has Computer Lab',
        'type': accounts_models.ColumnConfiguration.TYPE_BOOLEAN,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {'active_countries_filter': "computer_lab = true"},
    },
    {
        'name': 'coverage_type',
        'label': 'Coverage Type',
        'type': accounts_models.ColumnConfiguration.TYPE_STR,
        'description': None,
        'table_name': 'connection_statistics_schoolweeklystatus',
        'table_alias': 'school_static',
        'table_label': 'Static Data',
        'is_filter_applicable': True,
        'options': {
            'active_countries_filter':
                """LOWER("connection_statistics_schoolweeklystatus"."coverage_type") != 'unknown'"""
        },
    }
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
