import sys

from django.core.management.base import BaseCommand
from django.db import transaction

from proco.entities.models import EntityType
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
        'detail_related_name': 'health_entity',
        'master_data_model': 'data_sources.HealthEntityMasterIntermediateData',
        'extra_config': {
            'tile_cache_prefix': 'HEALTH_STATUS_CONNECTIVITY_TILES_MAP',
            'tile_master_data_table': 'data_sources_healthentitymasterintermediatedata',
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


class Command(BaseCommand):
    help = 'Seed EntityType records for school (legacy) and health facilities'

    def add_arguments(self, parser):
        parser.add_argument(
            '--reset',
            action='store_true',
            dest='reset',
            default=False,
            help='If provided, soft-delete all existing EntityTypes before seeding',
        )

    def handle(self, **options):
        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\nSeeding EntityType records...')
        sys.stdout.write('\n' + '=' * 60)

        with transaction.atomic():
            if options.get('reset', False):
                sys.stdout.write('\n⚠ Resetting: Soft-deleting all existing EntityTypes...')
                deleted_count = EntityType.objects.filter(deleted__isnull=True).update(
                    deleted=get_current_datetime_object()
                )
                sys.stdout.write(f'\n✓ Soft-deleted {deleted_count} EntityType(s)')

            seed_entity_types()

        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\n✓ EntityType seeding completed successfully!')
        sys.stdout.write('\n' + '=' * 60 + '\n')
