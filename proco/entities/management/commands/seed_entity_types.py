import sys

from django.core.management.base import BaseCommand
from django.db import transaction

from proco.entities.models import EntityType
from proco.accounts.models import DataLayer, ColumnConfiguration, AdvanceFilter
from proco.core.utils import get_current_datetime_object


ENTITY_TYPES_DATA = [
    {
        'code': 'school',
        'name': 'School',
        'description': 'Educational institution (legacy - maps to School model)',
        'is_active': True,
        'display_order': 1,
        'is_legacy': True,
        'detail_model': '',
        'detail_related_name': '',
        'master_data_model': 'data_sources.SchoolMasterData',
        'extra_config': {
            'tile_cache_prefix': 'SCHOOL_STATUS_CONNECTIVITY_TILES_MAP',
            'tile_master_data_table': 'data_sources_schoolmasterdata',
            'main_table': 'schools_school',
            'srid': '4326',
        },
    },
    {
        'code': 'health',
        'name': 'Health Facility',
        'description': 'Health facilities including hospitals, clinics, and health centers',
        'is_active': True,
        'display_order': 2,
        'is_legacy': False,
        'detail_model': 'entities.HealthEntity',
        'detail_related_name': 'entities_health_entity',
        'master_data_model': 'data_sources.HealthEntityMasterIntermediateData',
        'extra_config': {
            'tile_cache_prefix': 'HEALTH_STATUS_CONNECTIVITY_TILES_MAP',
            'tile_master_data_table': 'data_sources_healthentitymasterintermediatedata',
            'main_table': 'entities_entity',
            'srid': '4326',
        },
    },
]


def seed_entity_types():
    """
    Seed EntityType records for school (legacy) and health facilities.
    Uses update_or_create to be idempotent.
    """
    for entity_data in ENTITY_TYPES_DATA:
        try:
            instance, created = EntityType.objects.update_or_create(
                code=entity_data['code'],
                defaults={
                    'name': entity_data['name'],
                    'description': entity_data['description'],
                    'is_active': entity_data['is_active'],
                    'display_order': entity_data['display_order'],
                    'is_legacy': entity_data['is_legacy'],
                    'detail_model': entity_data['detail_model'],
                    'detail_related_name': entity_data['detail_related_name'],
                    'master_data_model': entity_data['master_data_model'],
                    'extra_config': entity_data['extra_config'],
                    'last_modified_at': get_current_datetime_object(),
                },
            )
            if created:
                sys.stdout.write(f'\n✓ Created EntityType: {instance.code} ({instance.name})')
            else:
                sys.stdout.write(f'\n✓ Updated EntityType: {instance.code} ({instance.name})')
        except Exception as e:
            sys.stdout.write(f'\n✗ Error processing EntityType {entity_data["code"]}: {e}')


def map_entity_types_to_data_sources():
    """
    Map EntityType to existing data sources.
    """
    try:
        school_entity_type = EntityType.objects.get(code='school')
        DataLayer.objects.filter(entity_type__isnull=True).update(entity_type=school_entity_type)
        ColumnConfiguration.objects.filter(entity_type__isnull=True).update(entity_type=school_entity_type)
        AdvanceFilter.objects.filter(entity_type__isnull=True).update(entity_type=school_entity_type)

        sys.stdout.write('\n✓ Mapped EntityTypes to their corresponding data sources successfully.')
    except Exception as e:
        sys.stdout.write(f'\n✗ Error mapping EntityTypes to data sources: {e}')

class Command(BaseCommand):
    help = 'Seed EntityType records for school (legacy) and health facilities'

    def handle(self, **options):
        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\nSeeding EntityType records...')
        sys.stdout.write('\n' + '=' * 60)

        with transaction.atomic():
            seed_entity_types()
            map_entity_types_to_data_sources()

        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\n✓ EntityType seeding completed successfully!')
        sys.stdout.write('\n' + '=' * 60 + '\n')
