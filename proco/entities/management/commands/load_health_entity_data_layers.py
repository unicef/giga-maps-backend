import sys

from django.core.management.base import BaseCommand
from django.db import transaction

from proco.accounts import models as accounts_models
from proco.entities.models import EntityType
from proco.core.utils import get_current_datetime_object, normalize_str


# Health Entity Data Sources Configuration
health_data_source_json = [
    {
        'name': 'Health Entity Master',
        'description': 'Health facility master data including infrastructure and service information',
        'version': 'V1.0',
        'data_source_type': 'HEALTH_MASTER',
        'request_config': {},
        'column_config': [
            {
                'name': 'connectivity_speed',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Download Speed',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': 'Average download speed',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '90th percentile of download speed',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': 'Median download speed',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'connectivity_upload_speed',
                'type': 'int',
                'unit': 'bps',
                'is_parameter': True,
                'alias': 'Upload Speed',
                'base_benchmark': 1000000,
                'display_unit': 'Mbps',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': 'Average upload speed',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '90th percentile of upload speed',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': 'Median upload speed',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'connectivity_latency',
                'type': 'int',
                'unit': 'ms',
                'is_parameter': True,
                'alias': 'Latency',
                'base_benchmark': 1,
                'display_unit': 'ms',
                'supported_functions': [
                    {
                        'name': 'avg',
                        'verbose': 'Avg',
                        'description': 'Average latency',
                        'sql': 'AVG({col_name})'
                    },
                    {
                        'name': 'min',
                        'verbose': 'Min',
                        'description': 'Minimum of all values',
                        'sql': 'MIN({col_name})'
                    },
                    {
                        'name': 'max',
                        'verbose': 'Max',
                        'description': 'Maximum of all values',
                        'sql': 'MAX({col_name})'
                    },
                    {
                        'name': 'median|90',
                        'verbose': '90th Percentile',
                        'description': '90th percentile of latency',
                        'sql': 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {col_name})'
                    },
                    {
                        'name': 'median|50',
                        'verbose': '50th Percentile',
                        'description': 'Median latency',
                        'sql': 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col_name})'
                    }
                ]
            },
            {
                'name': 'facility_type',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Facility Type (facility_type)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['hospital', 'clinic', 'pharmacy', 'health_center', 'dentist', 'doctors', 'laboratory', 'specialized', 'unknown'],
            },
            {
                'name': 'facility_level',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Facility Level (facility_level)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['primary', 'secondary', 'tertiary'],
            },
            {
                'name': 'facility_ownership',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Facility Ownership (facility_ownership)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['public', 'private', 'charitable'],
            },
            {
                'name': 'num_beds_total',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Total Beds (num_beds_total)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'num_beds_icu',
                'type': 'int',
                'is_parameter': True,
                'alias': 'ICU Beds (num_beds_icu)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'num_healthworkers',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Health Workers (num_healthworkers)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'staff_doctors',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Doctors (staff_doctors)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'staff_nurses',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Number of Nurses (staff_nurses)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'emergency_services_available',
                'type': 'boolean',
                'is_parameter': True,
                'alias': 'Emergency Services (emergency_services_available)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['available', 'not_available'],
            },
            {
                'name': 'is_facility_open',
                'type': 'boolean',
                'is_parameter': True,
                'alias': 'Facility Open (is_facility_open)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['open', 'closed'],
            },
            {
                'name': 'power_backup_system',
                'type': 'boolean',
                'is_parameter': True,
                'alias': 'Power Backup System (power_backup_system)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['available', 'not_available'],
            },
            {
                'name': 'cold_chain_available',
                'type': 'boolean',
                'is_parameter': True,
                'alias': 'Cold Chain Available (cold_chain_available)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['available', 'not_available'],
            },
            {
                'name': 'hmis_system',
                'type': 'boolean',
                'is_parameter': True,
                'alias': 'HMIS System (hmis_system)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['available', 'not_available'],
            },
            {
                'name': 'catchment_population',
                'type': 'int',
                'is_parameter': True,
                'alias': 'Catchment Population (catchment_population)',
                'unit': '',
                'display_unit': '',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'fiber_node_distance',
                'type': 'float',
                'is_parameter': True,
                'alias': 'Fiber Node Distance (fiber_node_distance)',
                'unit': 'km',
                'display_unit': 'km',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'nearest_lte_distance',
                'type': 'float',
                'is_parameter': True,
                'alias': 'Nearest LTE Distance (nearest_lte_distance)',
                'unit': 'km',
                'display_unit': 'km',
                'count_labels': ['good', 'moderate', 'bad'],
            },
            {
                'name': 'connectivity_status',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Connectivity Status (connectivity_status)',
                'unit': '',
                'display_unit': '',
                'table_name': 'entities_entity',
                'count_labels': ['good', 'moderate', 'no', 'unknown'],
            },
            {
                'name': 'coverage_status',
                'type': 'str',
                'is_parameter': True,
                'alias': 'Coverage Status (coverage_status)',
                'unit': '',
                'display_unit': '',
                'table_name': 'entities_entity',
                'count_labels': ['good', 'moderate', 'no', 'unknown'],
            },
        ],
        'status': 'PUBLISHED'
    },
]


# Health Entity Data Layers Configuration
health_data_layer_json = [
    {
        'code': 'HEALTH_DOWNLOAD',
        'name': 'Health Facility Download Speed',
        'icon': """<svg id="icon" xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32"><defs><style>.cls-1 {fill: none;}</style></defs><path d="M26,24v4H6V24H4v4H4a2,2,0,0,0,2,2H26a2,2,0,0,0,2-2h0V24Z"/><polygon points="26 14 24.59 12.59 17 20.17 17 2 15 2 15 20.17 7.41 12.59 6 14 16 24 26 14"/><g id="_Transparent_Rectangle_" data-name="&lt;Transparent Rectangle&gt;"><rect class="cls-1" width="32" height="32"/></g></svg>""",
        'description': 'Download speed for health facilities',
        'version': 'V 1.0',
        'type': 'LIVE',
        'category': 'CONNECTIVITY',
        'applicable_countries': [],
        'global_benchmark': {'value': '20000000', 'unit': 'bps', 'convert_unit': 'mbps'},
        'legend_configs': [],
        'is_reverse': False,
        'status': 'PUBLISHED',
        'data_sources': [
            {
                'name': 'Health Entity Master',
                'data_source_type': 'HEALTH_MASTER',
                'data_source_column': {
                    'name': 'connectivity_speed',
                    'type': 'int',
                    'unit': 'bps',
                    'is_parameter': True,
                    'alias': 'Download Speed',
                    'base_benchmark': 1000000,
                    'display_unit': 'Mbps',
                    'supported_functions': []
                }
            }
        ]
    },
    {
        'code': 'HEALTH_UPLOAD',
        'name': 'Health Facility Upload Speed',
        'icon': """<svg id="icon" xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32"><defs><style>.cls-1 {fill: none;}</style></defs><path d="M26,24v4H6V24H4v4H4a2,2,0,0,0,2,2H26a2,2,0,0,0,2-2h0V24Z"/><polygon points="6 18 7.41 19.41 15 11.83 15 30 17 30 17 11.83 24.59 19.41 26 18 16 8 6 18"/><g id="_Transparent_Rectangle_" data-name="&lt;Transparent Rectangle&gt;"><rect class="cls-1" width="32" height="32"/></g></svg>""",
        'description': 'Upload speed for health facilities',
        'version': 'V 1.0',
        'type': 'LIVE',
        'category': 'CONNECTIVITY',
        'applicable_countries': [],
        'global_benchmark': {'value': '10000000', 'unit': 'bps', 'convert_unit': 'mbps'},
        'legend_configs': [],
        'is_reverse': False,
        'status': 'PUBLISHED',
        'data_sources': [
            {
                'name': 'Health Entity Master',
                'data_source_type': 'HEALTH_MASTER',
                'data_source_column': {
                    'name': 'connectivity_upload_speed',
                    'type': 'int',
                    'unit': 'bps',
                    'is_parameter': True,
                    'alias': 'Upload Speed',
                    'base_benchmark': 1000000,
                    'display_unit': 'Mbps',
                    'supported_functions': []
                }
            }
        ]
    },
    {
        'code': 'HEALTH_LATENCY',
        'name': 'Health Facility Latency',
        'icon': """<svg id="icon" xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32"><defs><style>.cls-1 {fill: none;}</style></defs><path d="M16,28A11.0134,11.0134,0,0,1,5,17H7a9,9,0,1,0,9-9V10l-5-5,5-5V2A11.0134,11.0134,0,0,1,27,13,11.0134,11.0134,0,0,1,16,28Z"/><rect id="_Transparent_Rectangle_" data-name="&lt;Transparent Rectangle&gt;" class="cls-1" width="32" height="32"/></svg>""",
        'description': 'Network latency for health facilities',
        'version': 'V 1.0',
        'type': 'LIVE',
        'category': 'CONNECTIVITY',
        'applicable_countries': [],
        'global_benchmark': {'value': '100', 'unit': 'ms', 'convert_unit': 'ms'},
        'legend_configs': [],
        'is_reverse': True,
        'status': 'PUBLISHED',
        'data_sources': [
            {
                'name': 'Health Entity Master',
                'data_source_type': 'HEALTH_MASTER',
                'data_source_column': {
                    'name': 'connectivity_latency',
                    'type': 'int',
                    'unit': 'ms',
                    'is_parameter': True,
                    'alias': 'Latency',
                    'base_benchmark': 1,
                    'display_unit': 'ms',
                    'supported_functions': []
                }
            }
        ]
    },
    {
        'code': 'DEFAULT_COVERAGE_HEALTH',
        'name': 'Health Coverage data',
        'icon': """<svg id="icon" xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32"><defs><style>.cls-1 {fill: none;}</style></defs><path d="M10.57,30l.9333-2h8.9928l.9333,2h2.2072L17,15.7778V11H15v4.7778L8.3631,30ZM16,18.3647,17.6965,22h-3.393ZM13.37,24h5.26l.9333,2H12.4369Z" transform="translate(0 0)"/><path d="M10.7832,9.3325a7.0007,7.0007,0,0,1,10.4341,0l-1.49,1.334a5,5,0,0,0-7.4537,0Z" transform="translate(0 0)"/><path d="M7.1992,6.3994a11.0019,11.0019,0,0,1,17.6006,0L23.2,7.6a9.0009,9.0009,0,0,0-14.4014,0Z" transform="translate(0 0)"/><rect id="_Transparent_Rectangle_" data-name="&lt;Transparent Rectangle&gt;" class="cls-1" width="32" height="32"/></svg>""",
        'description': 'Coverage status for health facilities',
        'version': 'V 1.0',
        'type': 'STATIC',
        'category': 'COVERAGE',
        'applicable_countries': [],
        'global_benchmark': {},
        'legend_configs': {
            'good': {
                'values': ['good'],
                'labels': 'Covered'
            },
            'moderate': {
                'values': ['moderate'],
                'labels': 'Moderate'
            },
            'bad': {
                'values': ['no'],
                'labels': 'No Coverage'
            },
            'unknown': {
                'values': [],
                'labels': 'Unknown'
            }
        },
        'is_reverse': False,
        'status': 'PUBLISHED',
        'data_sources': [
            {
                'name': 'Health Entity Master',
                'data_source_type': 'HEALTH_MASTER',
                'data_source_column': {
                    'name': 'coverage_status',
                    'type': 'str',
                    'is_parameter': True,
                    'alias': 'Coverage Status',
                    'unit': '',
                    'display_unit': '',
                    'table_name': 'entities_entity',
                    'count_labels': ['good', 'moderate', 'no', 'unknown'],
                    'supported_functions': []
                }
            }
        ]
    },
]


def load_health_data_sources():
    """
    Load health entity data sources.
    Links them to the 'health' EntityType.
    """
    try:
        health_entity_type = EntityType.objects.get(code='health')
    except EntityType.DoesNotExist:
        sys.stdout.write('\n✗ Error: Health EntityType not found. Please run seed_entity_types command first.')
        return

    for row_data in health_data_source_json:
        try:
            instance, created = accounts_models.DataSource.objects.update_or_create(
                name=row_data['name'],
                data_source_type=row_data['data_source_type'],
                defaults={
                    'description': row_data['description'],
                    'request_config': row_data['request_config'],
                    'column_config': row_data['column_config'],
                    'version': row_data['version'],
                    'status': row_data['status'],
                    'last_modified_at': get_current_datetime_object(),
                },
            )
            if created:
                sys.stdout.write(f'\n✓ Created health data source: {instance.name}')
            else:
                sys.stdout.write(f'\n✓ Updated health data source: {instance.name}')
        except Exception as e:
            sys.stdout.write(f'\n✗ Error processing data source {row_data["name"]}: {e}')


def load_health_data_layers():
    """
    Load health entity data layers and link them to data sources.
    Links them to the 'health' EntityType.
    """
    try:
        health_entity_type = EntityType.objects.get(code='health')
    except EntityType.DoesNotExist:
        sys.stdout.write('\n✗ Error: Health EntityType not found. Please run seed_entity_types command first.')
        return

    for row_data in health_data_layer_json:
        try:
            layer_instance, created = accounts_models.DataLayer.objects.update_or_create(
                code=row_data['code'],
                defaults={
                    'name': row_data['name'],
                    'icon': row_data['icon'],
                    'description': row_data['description'],
                    'version': row_data['version'],
                    'type': row_data['type'],
                    'category': row_data['category'],
                    'applicable_countries': row_data['applicable_countries'],
                    'global_benchmark': row_data['global_benchmark'],
                    'legend_configs': row_data['legend_configs'],
                    'is_reverse': row_data['is_reverse'],
                    'status': row_data['status'],
                    'entity_type': health_entity_type,
                    'last_modified_at': get_current_datetime_object(),
                },
            )
            if created:
                sys.stdout.write(f'\n✓ Created health data layer: {layer_instance.name}')
            else:
                sys.stdout.write(f'\n✓ Updated health data layer: {layer_instance.name}')

            layer_id = layer_instance.id
            for data_source in row_data['data_sources']:
                source_id = accounts_models.DataSource.objects.filter(
                    name=data_source['name'],
                    data_source_type=data_source['data_source_type'],
                    deleted__isnull=True,
                ).first()

                if not source_id:
                    sys.stdout.write(f'\n⚠ Warning: Data source "{data_source["name"]}" not found for layer "{layer_instance.name}"')
                    continue

                relationship_instance, created = accounts_models.DataLayerDataSourceRelationship.objects.update_or_create(
                    data_layer_id=layer_id,
                    data_source=source_id,
                    defaults={
                        'data_source_column': data_source['data_source_column'],
                        'last_modified_at': get_current_datetime_object(),
                    },
                )
                if created:
                    sys.stdout.write(f'\n  ✓ Created relationship: {layer_instance.name} <-> {source_id.name}')
                else:
                    sys.stdout.write(f'\n  ✓ Updated relationship: {layer_instance.name} <-> {source_id.name}')
        except Exception as e:
            sys.stdout.write(f'\n✗ Error processing data layer {row_data["code"]}: {e}')


class Command(BaseCommand):
    help = 'Load health entity data sources and data layers'

    def add_arguments(self, parser):
        parser.add_argument(
            '--delete_data_sources', action='store_true', dest='delete_data_sources', default=False,
            help='If provided, already created health data sources will be deleted.'
        )

        parser.add_argument(
            '--update_data_sources', action='store_true', dest='update_data_sources', default=False,
            help='If provided, health data sources will be created/updated.'
        )

        parser.add_argument(
            '--delete_data_layers', action='store_true', dest='delete_data_layers', default=False,
            help='If provided, already created health data layers will be deleted.'
        )

        parser.add_argument(
            '--update_data_layers', action='store_true', dest='update_data_layers', default=False,
            help='If provided, health data layers will be created/updated.'
        )

    def handle(self, **options):
        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\nLoading Health Entity Data Layers and Sources...')
        sys.stdout.write('\n' + '=' * 60)

        with transaction.atomic():
            if options.get('delete_data_sources', False):
                accounts_models.DataSource.objects.filter(
                    data_source_type='HEALTH_MASTER'
                ).update(deleted=get_current_datetime_object())
                sys.stdout.write('\n✓ Deleted existing health data sources')

        with transaction.atomic():
            if options.get('update_data_sources', False):
                load_health_data_sources()

        with transaction.atomic():
            if options.get('delete_data_layers', False):
                health_entity_type = EntityType.objects.filter(code='health').first()
                if health_entity_type:
                    health_layer_ids = list(
                        accounts_models.DataLayer.objects.filter(
                            entity_type=health_entity_type
                        ).values_list('id', flat=True)
                    )
                    accounts_models.DataLayerDataSourceRelationship.objects.filter(
                        data_layer_id__in=health_layer_ids
                    ).update(deleted=get_current_datetime_object())

                    accounts_models.DataLayer.objects.filter(
                        entity_type=health_entity_type
                    ).update(deleted=get_current_datetime_object())
                    sys.stdout.write('\n✓ Deleted existing health data layers')

        if options.get('update_data_layers', False):
            load_health_data_layers()

        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\n✓ Health entity data layers loaded successfully!')
        sys.stdout.write('\n' + '=' * 60 + '\n')
