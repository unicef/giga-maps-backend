"""
Generic Giga Meter master-data sync engine.

Every function here is parameterized by a FacilityTypeConfig
(proco/giga_meter/facility_types.py) instead of hardcoding a facility type.
Adding a new facility type to this pipeline means writing a new config there
and a few thin @app.task wrappers in tasks.py that call these functions with
it - not new copies of load/publish/delete logic.

`school` does not run through this yet; see facility_types.py's module
docstring for why.
"""
import json
import logging
import os
import uuid

from celery import current_task
from django.conf import settings
from django.db.models import Count
from django.db.utils import DataError
from requests.exceptions import HTTPError

from proco.background import utils as background_task_utilities
from proco.core import utils as core_utilities
from proco.data_sources import utils as data_sources_utilities
from proco.giga_meter import utils as giga_meter_utilities
from proco.utils.dates import format_date

logger = logging.getLogger('gigamaps.' + __name__)


def load_data_from_master_apis(config, country_iso3_format=None):
    """
    Background pull of a facility type's master data from Delta Sharing into
    its Giga Meter staging table.
    """
    logger.info('Starting loading the {0} master data from API to GigaMeter DB.'.format(config.label))

    ds_settings = settings.DATA_SOURCE_CONFIG.get(config.data_source_config_key) or {}
    share_name = ds_settings.get('SHARE_NAME')
    schema_name = ds_settings.get('SCHEMA_NAME')
    country_codes_for_exclusion = ds_settings.get('COUNTRY_EXCLUSION_LIST') or []

    if not share_name or not schema_name:
        logger.error(
            '{0} share/schema not configured. Set DATA_SOURCE_CONFIG["{0}"] before running {1} sync.'.format(
                config.data_source_config_key, config.label.lower(),
            ),
        )
        return

    fallback_ds_settings = {}
    if config.fallback_data_source_config_key:
        fallback_ds_settings = settings.DATA_SOURCE_CONFIG.get(config.fallback_data_source_config_key) or {}

    profile_json = {
        'shareCredentialsVersion': ds_settings.get(
            'SHARE_CREDENTIALS_VERSION',
            fallback_ds_settings.get('SHARE_CREDENTIALS_VERSION', 1),
        ),
        'endpoint': ds_settings.get('ENDPOINT') or fallback_ds_settings.get('ENDPOINT'),
        'bearerToken': ds_settings.get('BEARER_TOKEN') or fallback_ds_settings.get('BEARER_TOKEN'),
        'expirationTime': ds_settings.get('EXPIRATION_TIME') or fallback_ds_settings.get('EXPIRATION_TIME'),
    }
    profile_file = os.path.join(
        settings.BASE_DIR,
        'giga_meter_{key}_master_profile_{dt}.share'.format(
            key=config.key,
            dt=format_date(core_utilities.get_current_datetime_object()),
        ),
    )
    open(profile_file, 'w').write(json.dumps(profile_json))

    client = data_sources_utilities.ProcoSharingClient(profile_file)
    master_share = client.get_share(share_name)

    if master_share:
        master_schema = client.get_schema(master_share, schema_name)

        if master_schema:
            schema_tables = client.list_tables(master_schema)

            logger.debug('All tables ready to access for Giga Meter {0} Sync: {1}'.format(
                config.label, schema_tables,
            ))

            master_fields = [f.name for f in config.intermediate_model._meta.get_fields()]

            for schema_table in schema_tables:
                logger.debug('#' * 10)
                logger.debug('%s table: %s', config.label, schema_table)

                if country_iso3_format and country_iso3_format != schema_table.name:
                    continue

                if len(country_codes_for_exclusion) > 0 and schema_table.name in country_codes_for_exclusion:
                    logger.warning(
                        'Country with ISO3 Format ({0}) configured to exclude from {1} Master data pull. '
                        'Hence skipping the load for this country code.'.format(schema_table.name, config.label),
                    )
                    continue

                try:
                    giga_meter_utilities.sync_master_data(
                        config, profile_file, share_name, schema_name, schema_table.name, master_fields,
                    )
                except (HTTPError, DataError, ValueError) as ex:
                    logger.error('Exception caught for {0} "{1}": {2}'.format(config.label, schema_table.name, str(ex)))
                except Exception as ex:
                    logger.error('Exception caught for {0} "{1}": {2}'.format(config.label, schema_table.name, str(ex)))

        else:
            logger.error(
                '{0} Master schema ({1}) does not exist to use for share ({2}).'.format(
                    config.label, schema_name, share_name,
                ),
            )
    else:
        logger.error('{0} Master share ({1}) does not exist to use.'.format(config.label, share_name))

    try:
        os.remove(profile_file)
    except OSError:
        pass


def update_static_data(config, country_iso3_format=None, force_tasks=False):
    """
    Background task body to pull a facility type's static/master data into
    Giga Meter DB and dedupe its intermediate table.
    """
    timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H')
    if force_tasks:
        timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H%M%S')

    if country_iso3_format:
        task_key = 'giga_meter_update_{key}_static_data_status_{ts}_country_code_{code}'.format(
            key=config.key, ts=timestamp_str, code=country_iso3_format,
        )
        task_description = 'Giga Meter - Sync Static Data from {0} Master sources for a country {1}'.format(
            config.label, country_iso3_format,
        )
    else:
        task_key = 'giga_meter_update_{key}_static_data_status_{ts}'.format(key=config.key, ts=timestamp_str)
        task_description = 'Giga Meter - Sync Static Data from {0} Master sources'.format(config.label)

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, task_description, check_previous=True)

    if task_instance:
        logger.debug('Not found running job for {0} static data pull handler: {1}'.format(config.label, task_key))
        load_data_from_master_apis(config, country_iso3_format=country_iso3_format)
        task_instance.info('Completed the load data from {0} Master API call'.format(config.label))

        # Delete all the old intermediate records where more than 1 record exists for the same giga ID
        rows_with_more_than_1_record = config.intermediate_model.objects.all().values(
            config.id_field, 'country_id',
        ).annotate(
            total_records=Count(config.id_field, distinct=False),
        ).order_by('-total_records', config.id_field, 'country_id').filter(total_records__gt=1)

        logger.debug(
            'Queryset to get all the old {0} intermediate records to delete where more than 1 record '
            'for same Giga ID: {1}'.format(config.label, rows_with_more_than_1_record.query),
        )

        for row in rows_with_more_than_1_record:
            for row_to_delete in config.intermediate_model.objects.filter(**{
                config.id_field: row[config.id_field],
                'country_id': row['country_id'],
            }).order_by('-version', '-created')[1:]:
                row_to_delete.delete()
        task_instance.info('Deleted {0} rows where more than 1 record for same Giga ID'.format(config.label))

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found Job with "{0}" name so skipping current iteration'.format(task_key))


def handle_published_master_data_row(config, country_ids=None, force_tasks=False):
    """
    Promote PUBLISHED intermediate rows for a facility type into its entity +
    versioned static snapshot tables.
    """
    logger.info('Giga Meter - Handling the published {0} master data rows.'.format(config.label))

    timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H')
    if force_tasks:
        timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H%M%S')

    if country_ids and len(country_ids) > 0:
        task_key = 'giga_meter_handle_published_{key}_master_data_row_status_{ts}_country_ids_{ids}'.format(
            key=config.key, ts=timestamp_str, ids='_'.join([str(c_id) for c_id in country_ids]),
        )
        task_description = 'Giga Meter - Handle published {0} master data rows for countries'.format(config.label)
    else:
        task_key = 'giga_meter_handle_published_{key}_master_data_row_status_{ts}'.format(
            key=config.key, ts=timestamp_str,
        )
        task_description = 'Giga Meter - Handle published {0} master data rows'.format(config.label)

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, task_description)

    if task_instance:
        logger.debug(
            'Not found running job for Giga Meter published {0} rows handler: {1}'.format(config.label, task_key),
        )

        new_published_records = config.intermediate_model.objects.filter(
            status=config.intermediate_model.ROW_STATUS_PUBLISHED,
        )

        if country_ids and len(country_ids) > 0:
            new_published_records = new_published_records.filter(country_id__in=country_ids)

        task_instance.info(
            'Giga Meter - Total published {0} records to update: {1}'.format(
                config.label, new_published_records.count(),
            ),
        )

        for data_chunk in core_utilities.queryset_iterator(new_published_records, chunk_size=100, print_msg=False):
            for row in data_chunk:
                try:
                    for required_field in config.required_fields:
                        if getattr(row, required_field) is None:
                            raise ValueError(
                                '{0} requires {1} ({2}={3})'.format(
                                    config.label, required_field, config.id_field, getattr(row, config.id_field),
                                ),
                            )

                    ctx = config.build_context(row)

                    entity, _created = config.entity_model.objects.update_or_create(
                        **config.entity_lookup(row),
                        defaults={spec.target: spec.resolve(row, ctx) for spec in config.entity_fields},
                    )

                    static, static_created = config.static_model.objects.update_or_create(
                        **{config.static_parent_fk_field: entity, 'version': row.version},
                        defaults={spec.target: spec.resolve(row, ctx) for spec in config.static_fields},
                    )

                    if static_created:
                        setattr(entity, config.last_static_fk_field, static)
                        entity.save(update_fields=[config.last_static_fk_field])

                    row.delete()

                except Exception as ex:
                    logger.error('Error reported on {0} publishing: {1}'.format(config.label, ex))
                    logger.error('Record: {0}'.format(row.__dict__))
                    task_instance.info(
                        'Error reported for {0} ID ({1}) on publishing: {2}'.format(config.label, row.id, ex),
                    )

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


def handle_deleted_master_data_row(config, country_ids=None, force_tasks=False):
    """Soft-delete a facility type's entities for intermediate rows with DELETED status."""
    logger.info('Giga Meter - Handling the deleted {0} master data rows.'.format(config.label))
    timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H')
    if force_tasks:
        timestamp_str = format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H%M%S')

    if country_ids and len(country_ids) > 0:
        task_key = 'giga_meter_handle_deleted_{key}_master_data_row_status_{ts}_country_ids_{ids}'.format(
            key=config.key, ts=timestamp_str, ids='_'.join([str(c_id) for c_id in country_ids]),
        )
        task_description = 'Giga Meter - Handle deleted {0} master data rows for countries'.format(config.label)
    else:
        task_key = 'giga_meter_handle_deleted_{key}_master_data_row_status_{ts}'.format(
            key=config.key, ts=timestamp_str,
        )
        task_description = 'Giga Meter - Handle deleted {0} master data rows'.format(config.label)

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(task_id, task_key, task_description)

    if task_instance:
        logger.debug('Not found running job for deleted {0} rows handler: {1}'.format(config.label, task_key))
        new_deleted_records = config.intermediate_model.objects.filter(
            status=config.intermediate_model.ROW_STATUS_DELETED,
        )

        if country_ids and len(country_ids) > 0:
            new_deleted_records = new_deleted_records.filter(country_id__in=country_ids)

        current_date = core_utilities.get_current_datetime_object()
        task_instance.info('Total {0} records to soft-delete: {1}'.format(config.label, new_deleted_records.count()))

        for data_chunk in core_utilities.queryset_iterator(new_deleted_records, chunk_size=1000):
            for row in data_chunk:
                try:
                    entity = config.entity_model.objects.filter(**config.entity_lookup(row)).first()

                    if entity:
                        entity.deleted = current_date
                        entity.save(update_fields=['deleted'])

                    row.delete()
                except Exception as ex:
                    logger.error('Error reported on {0} deletion: {1}'.format(config.label, ex))
                    logger.error('Record: {0}'.format(row.__dict__))
                    task_instance.info(
                        'Error reported for {0} ID ({1}) on deletion: {2}'.format(config.label, row.id, ex),
                    )

        task_instance.info(
            'Remaining {0} deleted intermediate records: {1}'.format(config.label, new_deleted_records.count()),
        )
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))
