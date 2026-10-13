# encoding: utf-8
"""
Management command to create and publish AdvanceFilter records for health entities.

This command reads all health-type ColumnConfiguration records (is_filter_applicable=True)
and creates corresponding AdvanceFilter records. It then publishes them and runs
populate_entity_active_filters_for_countries to create the country relationships.

Usage:
    python manage.py load_health_entity_advance_filters
    python manage.py load_health_entity_advance_filters --reset  # Delete and recreate
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import sys

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import transaction

from proco.accounts import models as accounts_models
from proco.core import utils as core_utilities
from proco.entities.models import EntityType

# AdvanceFilter definitions for health entities.
# Each entry maps a ColumnConfiguration name to filter settings.
# 'code' and 'name' identify the filter.
# 'type' is the primary filter type (DROPDOWN, RANGE, BOOLEAN, etc.).
# 'query_param_filter' is the DRF filter lookup style.
# 'options' contain frontend rendering hints (live_choices, range_auto_compute, etc.).
HEALTH_ADVANCE_FILTERS = [
    # --- Entity table fields (entities_entity) ---
    {
        'column_config_name': 'environment',
        'code': 'HEALTH_ENVIRONMENT',
        'name': 'Region',
        'description': 'Filter health facilities by region (urban/rural)',
        'type': accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN,
        'options': {
            'live_choices': True,
        },
    },
    {
        'column_config_name': 'water_availability',
        'code': 'HEALTH_WATER_AVAILABILITY',
        'name': 'Water Availability',
        'description': 'Filter health facilities by water availability',
        'type': accounts_models.AdvanceFilter.TYPE_BOOLEAN,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
        'options': {},
    },
    {
        'column_config_name': 'electricity_availability',
        'code': 'HEALTH_ELECTRICITY_AVAILABILITY',
        'name': 'Electricity Availability',
        'description': 'Filter health facilities by electricity availability',
        'type': accounts_models.AdvanceFilter.TYPE_BOOLEAN,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
        'options': {},
    },
    {
        'column_config_name': 'electricity_type',
        'code': 'HEALTH_ELECTRICITY_TYPE',
        'name': 'Electricity Type',
        'description': 'Filter health facilities by electricity type',
        'type': accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN,
        'options': {
            'live_choices': True,
        },
    },
    {
        'column_config_name': 'connectivity_govt',
        'code': 'HEALTH_CONNECTIVITY_GOVT',
        'name': 'Government Connectivity',
        'description': 'Filter health facilities by government connectivity data',
        'type': accounts_models.AdvanceFilter.TYPE_BOOLEAN,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
        'options': {},
    },

    # --- HealthEntity table fields (entities_health_entity) ---
    {
        'column_config_name': 'facility_type',
        'code': 'HEALTH_FACILITY_TYPE',
        'name': 'Facility Type',
        'description': 'Filter by health facility type (hospital, clinic, etc.)',
        'type': accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN,
        'options': {
            'live_choices': True,
        },
    },
    {
        'column_config_name': 'facility_ownership',
        'code': 'HEALTH_FACILITY_OWNERSHIP',
        'name': 'Facility Ownership',
        'description': 'Filter by health facility ownership (public, private, etc.)',
        'type': accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN,
        'options': {
            'live_choices': True,
        },
    },
    {
        'column_config_name': 'facility_level',
        'code': 'HEALTH_FACILITY_LEVEL',
        'name': 'Facility Level',
        'description': 'Filter by health facility level (primary, secondary, tertiary)',
        'type': accounts_models.AdvanceFilter.TYPE_DROPDOWN_MULTISELECT,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_IN,
        'options': {
            'live_choices': True,
        },
    },
    {
        'column_config_name': 'is_facility_open',
        'code': 'HEALTH_IS_FACILITY_OPEN',
        'name': 'Facility Open',
        'description': 'Filter by whether the facility is currently open',
        'type': accounts_models.AdvanceFilter.TYPE_BOOLEAN,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
        'options': {},
    },
    {
        'column_config_name': 'num_beds_total',
        'code': 'HEALTH_NUM_BEDS_TOTAL',
        'name': 'Total Beds',
        'description': 'Filter by total number of beds',
        'type': accounts_models.AdvanceFilter.TYPE_RANGE,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
        'options': {
            'range_auto_compute': True,
        },
    },
    {
        'column_config_name': 'num_beds_icu',
        'code': 'HEALTH_NUM_BEDS_ICU',
        'name': 'ICU Beds',
        'description': 'Filter by number of ICU beds',
        'type': accounts_models.AdvanceFilter.TYPE_RANGE,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
        'options': {
            'range_auto_compute': True,
        },
    },
    {
        'column_config_name': 'num_healthworkers',
        'code': 'HEALTH_NUM_HEALTHWORKERS',
        'name': 'Number of Health Workers',
        'description': 'Filter by number of health workers',
        'type': accounts_models.AdvanceFilter.TYPE_RANGE,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
        'options': {
            'range_auto_compute': True,
        },
    },
    {
        'column_config_name': 'staff_doctors',
        'code': 'HEALTH_STAFF_DOCTORS',
        'name': 'Number of Doctors',
        'description': 'Filter by number of doctors',
        'type': accounts_models.AdvanceFilter.TYPE_RANGE,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
        'options': {
            'range_auto_compute': True,
        },
    },
    {
        'column_config_name': 'staff_nurses',
        'code': 'HEALTH_STAFF_NURSES',
        'name': 'Number of Nurses',
        'description': 'Filter by number of nurses',
        'type': accounts_models.AdvanceFilter.TYPE_RANGE,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
        'options': {
            'range_auto_compute': True,
        },
    },
    {
        'column_config_name': 'emergency_services_available',
        'code': 'HEALTH_EMERGENCY_SERVICES',
        'name': 'Emergency Services',
        'description': 'Filter by emergency services availability',
        'type': accounts_models.AdvanceFilter.TYPE_BOOLEAN,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
        'options': {},
    },
    {
        'column_config_name': 'power_backup_system',
        'code': 'HEALTH_POWER_BACKUP',
        'name': 'Power Backup System',
        'description': 'Filter by power backup system availability',
        'type': accounts_models.AdvanceFilter.TYPE_BOOLEAN,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
        'options': {},
    },
    {
        'column_config_name': 'cold_chain_available',
        'code': 'HEALTH_COLD_CHAIN',
        'name': 'Cold Chain Available',
        'description': 'Filter by cold chain availability',
        'type': accounts_models.AdvanceFilter.TYPE_BOOLEAN,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_ON,
        'options': {},
    },
    {
        'column_config_name': 'catchment_population',
        'code': 'HEALTH_CATCHMENT_POPULATION',
        'name': 'Catchment Population',
        'description': 'Filter by catchment population size',
        'type': accounts_models.AdvanceFilter.TYPE_RANGE,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
        'options': {
            'range_auto_compute': True,
        },
    },

    # --- EntityWeeklyStatus fields (connection_statistics_entityweeklystatus) ---
    {
        'column_config_name': 'connectivity_speed',
        'code': 'HEALTH_CONNECTIVITY_SPEED',
        'name': 'Download Speed',
        'description': 'Filter by connectivity download speed',
        'type': accounts_models.AdvanceFilter.TYPE_RANGE,
        'query_param_filter': accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_RANGE,
        'options': {
            'range_auto_compute': True,
        },
    },
]


class Command(BaseCommand):
    help = 'Create and publish AdvanceFilter records for health entities, then populate country relationships.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--reset', action='store_true', dest='reset', default=False,
            help='If provided, existing health advance filters will be soft-deleted and recreated.',
        )
        parser.add_argument(
            '--dry-run', action='store_true', dest='dry_run', default=False,
            help='If provided, show what would be created without making changes.',
        )

    def handle(self, **options):
        reset = options.get('reset', False)
        dry_run = options.get('dry_run', False)

        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\nLoading Health Entity Advance Filters...')
        sys.stdout.write('\n' + '=' * 60)

        # 1. Get the health entity type
        try:
            health_entity_type = EntityType.objects.get(code='health')
        except EntityType.DoesNotExist:
            sys.stdout.write('\n✗ Error: Health EntityType not found. Run seed_entity_types first.')
            return

        # 2. Optionally reset existing health filters
        if reset and not dry_run:
            deleted_count = accounts_models.AdvanceFilter.objects.filter(
                entity_type=health_entity_type,
            ).update(deleted=core_utilities.get_current_datetime_object())
            sys.stdout.write(f'\n⟳ Soft-deleted {deleted_count} existing health advance filters.')

        # 3. Create/update AdvanceFilter records
        created_count = 0
        updated_count = 0
        skipped_count = 0

        with transaction.atomic():
            for filter_def in HEALTH_ADVANCE_FILTERS:
                col_config_name = filter_def['column_config_name']

                # Find the matching ColumnConfiguration
                try:
                    column_config = accounts_models.ColumnConfiguration.objects.get(
                        name=col_config_name,
                        entity_type=health_entity_type,
                        is_filter_applicable=True,
                    )
                except accounts_models.ColumnConfiguration.DoesNotExist:
                    sys.stdout.write(
                        f'\n⚠ Skipping {filter_def["code"]}: '
                        f'No ColumnConfiguration found for name="{col_config_name}" with entity_type=health'
                    )
                    skipped_count += 1
                    continue

                if dry_run:
                    sys.stdout.write(f'\n  [DRY RUN] Would create/update: {filter_def["code"]}')
                    continue

                try:
                    instance, created = accounts_models.AdvanceFilter.objects.update_or_create(
                        code=filter_def['code'],
                        defaults={
                            'name': filter_def['name'],
                            'description': filter_def.get('description', ''),
                            'type': filter_def['type'],
                            'query_param_filter': filter_def['query_param_filter'],
                            'column_configuration': column_config,
                            'options': filter_def.get('options', {}),
                            'entity_type': health_entity_type,
                            'status': accounts_models.AdvanceFilter.FILTER_STATUS_PUBLISHED,
                            'published_at': core_utilities.get_current_datetime_object(),
                            'last_modified_at': core_utilities.get_current_datetime_object(),
                            'deleted': None,  # Ensure it's not soft-deleted
                        },
                    )

                    if created:
                        created_count += 1
                        sys.stdout.write(f'\n✓ Created: {instance.code} → {instance.name}')
                    else:
                        updated_count += 1
                        sys.stdout.write(f'\n✓ Updated: {instance.code} → {instance.name}')

                except Exception as e:
                    sys.stdout.write(f'\n✗ Error creating {filter_def["code"]}: {e}')

        sys.stdout.write(f'\n\nSummary: {created_count} created, {updated_count} updated, {skipped_count} skipped')

        # 4. Populate country relationships
        if not dry_run and (created_count > 0 or updated_count > 0):
            sys.stdout.write('\n\nPopulating country relationships for health filters...')
            try:
                # Get all health filter IDs to populate
                health_filter_ids = list(
                    accounts_models.AdvanceFilter.objects.filter(
                        entity_type=health_entity_type,
                        status=accounts_models.AdvanceFilter.FILTER_STATUS_PUBLISHED,
                    ).values_list('id', flat=True)
                )

                for filter_id in health_filter_ids:
                    args = ['--reset', f'-filter_id={filter_id}']
                    call_command('populate_entity_active_filters_for_countries', *args)

                sys.stdout.write(f'\n✓ Country relationships populated for {len(health_filter_ids)} health filters.')
            except Exception as e:
                sys.stdout.write(f'\n✗ Error populating country relationships: {e}')
                sys.stdout.write(
                    '\n  You can manually run: python manage.py populate_entity_active_filters_for_countries')

        sys.stdout.write('\n' + '=' * 60)
        sys.stdout.write('\n✓ Health entity advance filters loading completed!')
        sys.stdout.write('\n' + '=' * 60 + '\n')
