import logging
import uuid

from datetime import timedelta

from celery import chain
from celery import current_task
from django.conf import settings
from django.core.management import call_command
from django.db.models import Q
from django.db.models.functions.text import Lower
from django.urls import reverse
from rest_framework.test import APIClient

from proco.background import utils as background_task_utilities
from proco.core import db_utils as db_utilities
from proco.core import utils as core_utilities
from proco.taskapp import app
from proco.utils.dates import format_date, to_date


logger = logging.getLogger('gigamaps.' + __name__)


@app.task(soft_time_limit=10 * 60, time_limit=11 * 60)
def update_cached_value(*args, url='', query_params=None, **kwargs):
    client = APIClient()
    if query_params:
        query_params['cache'] = False
        client.get(url, query_params, format='json')
    else:
        client.get(url, {'cache': False}, format='json')


@app.task(soft_time_limit=60 * 60, time_limit=65 * 60)
def update_all_cached_values(*args, clean_cache=False):
    from proco.accounts.models import DataLayerCountryRelationship, DataLayer
    from proco.locations.models import Country
    from proco.schools.models import School
    from proco.utils.cache import cache_manager

    task_key = 'update_all_cached_values_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H%M'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Update the Redis cache, allowed once in a hour')

    if task_instance:
        logger.info('Not found running job: {}'.format(task_key))

        if clean_cache:
            if settings.INVALIDATE_CACHE_HARD.lower() == 'true':
                cache_manager.invalidate(hard=True)
                logger.info('Cache cleared. Map is updated in real time.')
            else:
                cache_manager.invalidate()
                logger.info('Cache invalidation started. Maps will be updated in a few minutes.')

        update_cached_value.delay(url=reverse('locations:search-countries-admin-schools'))
        update_cached_value.delay(url=reverse('locations:countries-list'))
        update_cached_value.delay(url=reverse('connection_statistics:global-stat'))

        # Get countries which has at least has 1 school
        countries = Country.objects.filter(id__in=list(
            School.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct('country_id')
        ))

        country_wise_default_layers = {
            row['country_id']: row['data_layer_id']
            for row in DataLayerCountryRelationship.objects.filter(
                Q(is_default=True) | Q(
                    is_default=False,
                    data_layer__category=DataLayer.LAYER_CATEGORY_CONNECTIVITY,
                    data_layer__created_by__isnull=True,
                ),
                data_layer__type=DataLayer.LAYER_TYPE_LIVE,
                data_layer__status=DataLayer.LAYER_STATUS_PUBLISHED,
                data_layer__deleted__isnull=True,
                country_id__in=list(countries)).values('country_id', 'data_layer_id').order_by('country_id').distinct()
        }

        for country in countries:
            country_wise_task_list = [
                update_cached_value.s(
                    url=reverse('locations:countries-detail', kwargs={'pk': country.code.lower()})
                ),
                update_cached_value.s(
                    url=reverse('connection_statistics:global-stat'),
                    query_params={'country_id': country.id},
                ),
                update_cached_value.s(
                    url=reverse('accounts:list-published-advance-filters',
                                kwargs={'status': 'PUBLISHED', 'country_id': country.id}),
                    query_params={'expand': 'column_configuration', 'ordering': 'name'},
                ),
            ]

            if country_wise_default_layers.get(country.id, None):
                layer_id = country_wise_default_layers[country.id]

                client = APIClient()
                response = client.get(
                    reverse('connection_statistics:get-latest-week-and-month'),
                    {'country_id': country.id, 'layer_id': layer_id, 'cache': False},
                    format='json',
                )

                if response.status_code == 200 and response.data and response.data.get('week'):
                    latest_week_start_str = response.data['week']['start_date']
                    latest_week_end_str = response.data['week']['end_date']

                    country_wise_task_list.append(update_cached_value.s(
                        url=reverse('accounts:info-data-layer', kwargs={'pk': layer_id}),
                        query_params={
                            'country_id': country.id,
                            'start_date': latest_week_start_str,
                            'end_date': latest_week_end_str,
                            'is_weekly': 'true',
                            'benchmark': 'global',
                            'include_same_location_schools': 'false',
                        },
                    ))

                    if country.iso3_format == 'BRA':
                        # Case 1: Cache info API for last 4 weeks
                        latest_week_end = to_date(latest_week_end_str).date()
                        for week_offset in range(4):
                            week_end = latest_week_end - timedelta(weeks=week_offset)
                            week_start = week_end - timedelta(days=6)

                            start_date_str = week_start.strftime('%d-%m-%Y')
                            end_date_str = week_end.strftime('%d-%m-%Y')

                            country_wise_task_list.append(update_cached_value.s(
                                url=reverse('accounts:info-data-layer', kwargs={'pk': layer_id}),
                                query_params={
                                    'country_id': country.id,
                                    'start_date': start_date_str,
                                    'end_date': end_date_str,
                                    'is_weekly': 'true',
                                    'benchmark': 'global',
                                    'include_same_location_schools': 'false',
                                },
                            ))

                        # Case 2: Cache with filters
                        from proco.accounts.models import AdvanceFilter
                        country_filters = AdvanceFilter.objects.filter(
                            status=AdvanceFilter.FILTER_STATUS_PUBLISHED,
                            deleted__isnull=True,
                            active_countries__country_id=country.id,
                            active_countries__deleted__isnull=True,
                            type=AdvanceFilter.TYPE_DROPDOWN
                        ).distinct()

                        for advance_filter in country_filters:
                            query_param = advance_filter.query_param_filter
                            column_config = advance_filter.column_configuration
                            if not column_config:
                                continue

                            filter_field = column_config.name
                            filter_options = advance_filter.options or {}
                            static_choices = filter_options.get('choices', [])

                            filter_values = []
                            for choice in static_choices:
                                if isinstance(choice, dict) and 'value' in choice:
                                    filter_values.append(choice['value'])

                            # Cache global-stat and info API with each filter value
                            for value in filter_values:
                                filter_params = {f'{filter_field}__{query_param}': value}

                                # Global-stat with filter
                                global_stat_params = {'country_id': country.id}
                                global_stat_params.update(filter_params)
                                country_wise_task_list.append(update_cached_value.s(
                                    url=reverse('connection_statistics:global-stat'),
                                    query_params=global_stat_params,
                                ))

                                # Info API with filter
                                info_params = {
                                    'country_id': country.id,
                                    'start_date': latest_week_start_str,
                                    'end_date': latest_week_end_str,
                                    'is_weekly': 'true',
                                    'benchmark': 'global',
                                    'include_same_location_schools': 'false',
                                }
                                info_params.update(filter_params)
                                country_wise_task_list.append(update_cached_value.s(
                                    url=reverse('accounts:info-data-layer', kwargs={'pk': layer_id}),
                                    query_params=info_params,
                                ))

                        # Case 3: Cache admin1 views
                        from proco.locations.models import CountryAdminMetadata
                        admin1_ids = list(CountryAdminMetadata.objects.filter(
                            country_id=country.id,
                            layer_name='adm1',
                            deleted__isnull=True,
                        ).values_list('id', flat=True))

                        logger.info(f'Brazil: Queueing cache for {len(admin1_ids)} admin1 regions')
                        for adm1_id in admin1_ids:
                            # Connectivityconfigs for admin1
                            country_wise_task_list.append(update_cached_value.s(
                                url=reverse('connection_statistics:get-latest-week-and-month'),
                                query_params={
                                    'country_id': country.id,
                                    'admin1_id': adm1_id,
                                    'layer_id': layer_id,
                                },
                            ))

                            # Global-stat for admin1
                            country_wise_task_list.append(update_cached_value.s(
                                url=reverse('connection_statistics:global-stat'),
                                query_params={
                                    'country_id': country.id,
                                    'admin1_id': adm1_id,
                                },
                            ))

                            # Info API for admin1
                            country_wise_task_list.append(update_cached_value.s(
                                url=reverse('accounts:info-data-layer', kwargs={'pk': layer_id}),
                                query_params={
                                    'country_id': country.id,
                                    'start_date': latest_week_start_str,
                                    'end_date': latest_week_end_str,
                                    'is_weekly': 'true',
                                    'benchmark': 'global',
                                    'include_same_location_schools': 'false',
                                    'admin1_id': adm1_id,
                                },
                            ))

            chain(country_wise_task_list).delay()

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task
def update_country_related_cache(country_code):
    from proco.locations.models import Country

    update_cached_value.delay(url=reverse('locations:search-countries-admin-schools'))
    update_cached_value.delay(url=reverse('locations:countries-list'))
    update_cached_value.delay(url=reverse('connection_statistics:global-stat'))
    update_cached_value.delay(url=reverse('locations:countries-detail', kwargs={'pk': country_code.lower()}))

    country = Country.objects.annotate(
        code_lower=Lower('code'),
    ).filter(code_lower=country_code.lower()).first()
    if country:
        update_cached_value.delay(
            url=reverse('connection_statistics:global-stat'),
            query_params={'country_id': country.id},
        )
        update_cached_value.delay(
            url=reverse('accounts:list-published-advance-filters',
                        kwargs={'status': 'PUBLISHED', 'country_id': country.id}),
            query_params={'expand': 'column_configuration', 'ordering': 'name'},
        ),


@app.task(soft_time_limit=4 * 60 * 60, time_limit=4 * 60 * 60)
def rebuild_school_index():
    """
    rebuild_school_index
        Task which runs to rebuild the Cognitive Search Index for Schools from scratch.

        Frequency: Once in a day
        Limit: 15 minutes
    """
    logger.info('Rebuilding the school indexes.')
    task_key = 'rebuild_school_index_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Update the Cognitive Search Index for Schools')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))
        cmd_args = ['--delete_index', '--create_index', '--clean_index', '--update_index']
        call_command('index_rebuild_schools', *cmd_args)
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=1 * 60 * 60, time_limit=1 * 60 * 60)
def populate_school_registration_data():
    """
    populate_school_registration_data
        Task which runs to populate the RT table data for new schools.

        Frequency: Once in a day
        Limit: 1 hour
    """
    from proco.schools.models import School
    from proco.schools import utils as school_utilities

    logger.info('Setting RT status, RT Date for schools which start live data from sources.')

    task_key = 'populate_school_registration_data_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Populate the RT table data for new schools')

    if task_instance:
        task_instance.info(f'Not found running job with name: {task_key}')
        sql = """
        SELECT DISTINCT sds.school_id
        FROM public.connection_statistics_schooldailystatus AS sds
        INNER JOIN public.schools_school s ON s.id = sds.school_id
        LEFT JOIN public.connection_statistics_schoolrealtimeregistration AS srt
            ON sds.school_id = srt.school_id
            AND srt.deleted IS NULL
        WHERE
            s.deleted IS NULL
            AND sds.deleted IS NULL
            AND srt.school_id IS NULL
        """

        school_ids_missing_in_rt_table = db_utilities.sql_to_response(sql, label='SchoolRealtimeRegistration')
        if school_ids_missing_in_rt_table:
            task_instance.info('Total number of newly registered schools for live data in last 6 hours: {0}'.format(
                len(school_ids_missing_in_rt_table))
            )

            for missing_school_id in school_ids_missing_in_rt_table:
                cmd_args = ['--reset', '-school_id={0}'.format(missing_school_id['school_id'])]
                call_command('populate_school_registration_data', *cmd_args)

                school = School.objects.get(id=missing_school_id['school_id'])
                school.connectivity_status = school_utilities.get_connectivity_status_by_master_api(school)
                school.save(update_fields=['connectivity_status'])
                logger.info('School connectivity status updated for School Giga ID "{0}" as "{1}"'.format(
                    school.giga_id_school,
                    school.connectivity_status,
                ))

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=10 * 60 * 60, time_limit=10 * 60 * 60)
def redo_aggregations_task(country_id, year, week_no, *args):
    """
    redo_aggregations_task
        Task to schedule manually from Console.
    """
    if not country_id or not year:
        logger.error('Required args not provided: [country_id, year]')
        return

    logger.info('Starting redo aggregations task: Country ID "{0}" - Year "{1}" - Week "{2}"'.format(
        country_id, year, week_no))

    task_key = 'redo_aggregations_task_country_id_{0}_year_{1}_week_{2}_on_{3}'.format(
        country_id, year, week_no, format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Update the SchoolWeekly, CountryDaily and CountryWeekly from SchoolDaily')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))
        cmd_args = [
            '-country_id={}'.format(country_id),
            '-year={}'.format(year),
            '--update_school_weekly',
            '--update_country_daily',
            '--update_country_weekly',
        ]

        if week_no:
            cmd_args.append('-week_no={}'.format(week_no))

        call_command('redo_aggregations', *cmd_args)

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=10 * 60 * 60, time_limit=10 * 60 * 60)
def redo_entity_aggregations_task(country_id, year, week_no, entity_type_code=None, *args):
    """
    redo_entity_aggregations_task
        Task to schedule manually from Console for entities.
    """
    if not country_id or not year:
        logger.error('Required args not provided: [country_id, year]')
        return

    logger.info('Starting redo entity aggregations task: Country ID "{0}" - Year "{1}" - Week "{2}"'.format(
        country_id, year, week_no))

    task_key = 'redo_entity_aggregations_task_country_id_{0}_year_{1}_week_{2}_on_{3}'.format(
        country_id, year, week_no, format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Update the EntityWeeklyStatus from EntityDailyStatus')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))
        cmd_args = [
            '-country_id={}'.format(country_id),
            '-year={}'.format(year),
            '--update_entity_weekly',
        ]

        if week_no:
            cmd_args.append('-week_no={}'.format(week_no))

        if entity_type_code:
            cmd_args.append('-entity_type_code={}'.format(entity_type_code))

        call_command('redo_entity_aggregations', *cmd_args)

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))



@app.task(soft_time_limit=10 * 60 * 60, time_limit=10 * 60 * 60)
def populate_school_new_fields_task(start_school_id, end_school_id, country_id, *args, school_ids=None):
    """
    populate_school_new_fields_task
        Task to schedule manually from Console.
    """
    logger.info('Starting populate school new fields task: Country ID "{0}" - start_school_id "{1}" - '
                'end_school_id "{2}"'.format(country_id, start_school_id, end_school_id))

    cmd_args = []

    if country_id:
        cmd_args.append('-country_id={}'.format(country_id))

    if start_school_id:
        cmd_args.append('-start_school_id={}'.format(start_school_id))

    if end_school_id:
        cmd_args.append('-end_school_id={}'.format(end_school_id))

    if school_ids and len(school_ids) > 0:
        cmd_args.append('-school_ids={}'.format(','.join([str(school_id) for school_id in school_ids])))

    task_key = 'populate_school_new_fields_task{0}_at_{1}'.format(
        ''.join(cmd_args), format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Update the school new fields for provided records')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))

        task_instance.info('Starting the command with args: {}'.format(cmd_args))
        call_command('populate_school_new_fields', *cmd_args)
        task_instance.info('Completed the command.')

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=4 * 60 * 60, time_limit=4 * 60 * 60)
def rebuild_unified_index():
    """
    rebuild_unified_index
        Task which runs to rebuild the Cognitive Search Index for Entities from scratch.
        Frequency: Once in a day
    """
    logger.info('Rebuilding the unified entities indexes.')
    task_key = 'rebuild_unified_index_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Update the Cognitive Search Index for Entities')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))
        cmd_args = ['--delete_index', '--create_index', '--clean_index', '--update_index']
        call_command('build_unified_index', *cmd_args)
        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=1 * 60 * 60, time_limit=1 * 60 * 60)
def populate_entity_registration_data():
    from proco.entities.models import Entity

    logger.info('Setting RT status, RT Date for entities which start live data from sources.')

    task_key = 'populate_entity_registration_data_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Populate the RT table data for new entities')

    if task_instance:
        task_instance.info(f'Not found running job with name: {task_key}')
        sql = '''
        SELECT DISTINCT sds.entity_id
        FROM public.connection_statistics_entitydailystatus AS sds
        INNER JOIN public.entities_entity s ON s.id = sds.entity_id
        LEFT JOIN public.connection_statistics_entityrealtimeregistration AS srt
            ON sds.entity_id = srt.entity_id
            AND srt.deleted IS NULL
        WHERE
            s.deleted IS NULL
            AND sds.deleted IS NULL
            AND srt.entity_id IS NULL
        '''

        entity_ids_missing_in_rt_table = db_utilities.sql_to_response(sql, label='EntityRealtimeRegistration')
        if entity_ids_missing_in_rt_table:
            task_instance.info('Total number of newly registered entities for live data in last 6 hours: {0}'.format(
                len(entity_ids_missing_in_rt_table))
            )

            for missing_entity_id in entity_ids_missing_in_rt_table:
                cmd_args = ['--reset', '-entity_id={0}'.format(missing_entity_id['entity_id'])]
                call_command('populate_entity_registration_data', *cmd_args)

                entity = Entity.objects.get(id=missing_entity_id['entity_id'])
                entity.connectivity_status = 'good'
                entity.save(update_fields=['connectivity_status'])
                logger.info('Entity connectivity status updated for Entity Giga ID "{0}" as "{1}"'.format(
                    entity.giga_id,
                    entity.connectivity_status,
                ))

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=10 * 60 * 60, time_limit=10 * 60 * 60)
def update_entity_records():
    from proco.entities.models import Entity
    from proco.connection_statistics.models import EntityWeeklyStatus
    from proco.schools.constants import statuses_schema
    from datetime import timedelta

    logger.info('Updating entity records from weekly status changes.')

    task_key = 'update_entity_records_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Update the entity records from weekly status')

    if task_instance:
        time_threshold = core_utilities.get_current_datetime_object() - timedelta(hours=12)

        updated_weekly_statuses = EntityWeeklyStatus.objects.filter(
            modified__gte=time_threshold
        ).select_related('entity')

        for status in updated_weekly_statuses:
            entity = status.entity
            if not entity:
                continue

            update_fields = []

            if not entity.last_weekly_status or entity.last_weekly_status.date < status.date:
                entity.last_weekly_status = status
                update_fields.append('last_weekly_status')

            if entity.last_weekly_status_id == status.id:
                connectivity_status = 'unknown'
                if status.connectivity_speed is not None:
                    connectivity_status = statuses_schema.get_connectivity_status_by_connectivity_speed(
                        status.connectivity_speed
                    )

                if entity.connectivity_status != connectivity_status:
                    entity.connectivity_status = connectivity_status
                    update_fields.append('connectivity_status')

                if entity.coverage_status != 'unknown':
                    entity.coverage_status = 'unknown'
                    update_fields.append('coverage_status')

            if update_fields:
                entity.save(update_fields=update_fields)

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=1 * 50 * 60, time_limit=1 * 50 * 60)
def handle_deleted_entity_master_data_row(deleted_row_id=None, country_ids=None):
    from proco.data_sources.models import HealthEntityMasterIntermediateData

    logger.info('Handling deleted entity master data rows.')

    task_key = 'handle_deleted_entity_master_data_row_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    if deleted_row_id:
        task_key += f'_row_id_{deleted_row_id}'

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Handle deleted entity master data rows')

    if task_instance:
        if deleted_row_id:
            rows = HealthEntityMasterIntermediateData.objects.filter(id=deleted_row_id, is_read=False, status='DELETED_PUBLISHED')
        else:
            rows = HealthEntityMasterIntermediateData.objects.filter(is_read=False, status='DELETED_PUBLISHED')

        if country_ids:
            rows = rows.filter(country_id__in=country_ids)

        for row in rows:
            entity = row.entity
            if entity:
                entity.deleted = core_utilities.get_current_datetime_object()
                entity.save(update_fields=['deleted'])

                # Soft delete related
                from proco.connection_statistics.models import EntityDailyStatus, EntityWeeklyStatus, EntityRealTimeRegistration
                EntityDailyStatus.objects.all_records().filter(entity=entity).update(deleted=core_utilities.get_current_datetime_object())
                EntityWeeklyStatus.objects.all_records().filter(entity=entity).update(deleted=core_utilities.get_current_datetime_object())
                EntityRealTimeRegistration.objects.all_records().filter(entity=entity).update(deleted=core_utilities.get_current_datetime_object())

            row.is_read = True
            row.save(update_fields=['is_read'])

        background_task_utilities.task_on_complete(task_instance)
    else:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))


@app.task(soft_time_limit=120 * 60, time_limit=125 * 60)
def update_all_entity_cached_values(*args, clean_cache=False):
    from proco.entities.models import Entity, EntityType
    from proco.entities.constants import ALL_ENTITIES
    from proco.locations.models import Country
    from proco.accounts.models import DataLayerCountryRelationship, DataLayer
    from proco.utils.cache import cache_manager

    task_key = 'update_all_entity_cached_values_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H%M'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Update the Entity Redis cache, allowed once in a hour')

    if not task_instance:
        logger.error('Found running Job with "{0}" name so skipping current iteration'.format(task_key))
        return

    logger.info('Not found running job: {}'.format(task_key))

    if clean_cache:
        if settings.INVALIDATE_CACHE_HARD.lower() == 'true':
            cache_manager.invalidate(hard=True)
            logger.info('Cache cleared. Map is updated in real time.')
        else:
            cache_manager.invalidate()
            logger.info('Cache invalidation started. Maps will be updated in a few minutes.')

    update_cached_value.delay(url=reverse('locations:search-countries-admin-schools'))
    update_cached_value.delay(url=reverse('entities:list-entity-countries'))
    update_cached_value.delay(url=reverse('entities:global-stat-all-entities'), query_params={'entity_type__code': ALL_ENTITIES})

    active_entity_types = EntityType.get_all_active().exclude(is_legacy=True)
    entity_country_ids = Entity.objects.filter(deleted__isnull=True).values_list('country_id', flat=True).order_by('country_id').distinct()
    entity_countries = Country.objects.filter(id__in=list(entity_country_ids))

    for entity_type in active_entity_types:
        entity_wise_default_layers = {
            row['country_id']: row['data_layer_id']
            for row in DataLayerCountryRelationship.objects.filter(
                Q(is_default=True) | Q(
                    is_default=False,
                    data_layer__category=DataLayer.LAYER_CATEGORY_CONNECTIVITY,
                    data_layer__created_by__isnull=True,
                ),
                data_layer__type=DataLayer.LAYER_TYPE_LIVE,
                data_layer__status=DataLayer.LAYER_STATUS_PUBLISHED,
                data_layer__deleted__isnull=True,
                data_layer__entity_type=entity_type,
                country_id__in=list(entity_countries)
            ).values('country_id', 'data_layer_id').order_by('country_id').distinct()
        }

        for country in entity_countries:
            country_wise_task_list = [
                update_cached_value.s(
                    url=reverse('entities:global-stat-all-entities'),
                    query_params={'country_id': country.id, 'entity_type__code': entity_type.code},
                ),
                update_cached_value.s(
                    url=reverse('entities:list-published-entity-filters',
                                kwargs={'status': 'PUBLISHED', 'country_id': country.id}),
                    query_params={'expand': 'column_configuration', 'ordering': 'name', 'entity_type__code': entity_type.code},
                ),
            ]

            if entity_wise_default_layers.get(country.id, None):
                layer_id = entity_wise_default_layers[country.id]

                client = APIClient()
                response = client.get(
                    reverse('entities:entity-get-latest-week-and-month'),
                    {'country_id': country.id, 'layer_id': layer_id, 'cache': False},
                    format='json',
                )

                if response.status_code == 200 and response.data and response.data.get('week'):
                    latest_week_start_str = response.data['week']['start_date']
                    latest_week_end_str = response.data['week']['end_date']

                    country_wise_task_list.append(update_cached_value.s(
                        url=reverse('entities:entity-info-data-layer', kwargs={'pk': layer_id}),
                        query_params={
                            'country_id': country.id,
                            'entity_type__code': entity_type.code,
                            f'{entity_type.code}_start_date': latest_week_start_str,
                            f'{entity_type.code}_end_date': latest_week_end_str,
                            f'{entity_type.code}_is_weekly': 'true',
                            f'{entity_type.code}_benchmark': 'global',
                            f'{entity_type.code}_include_same_location': 'false',
                        },
                    ))

                    # Cache with filters
                    from proco.accounts.models import AdvanceFilter
                    country_filters = AdvanceFilter.objects.filter(
                        status=AdvanceFilter.FILTER_STATUS_PUBLISHED,
                        deleted__isnull=True,
                        active_countries__country_id=country.id,
                        active_countries__deleted__isnull=True,
                        type=AdvanceFilter.TYPE_DROPDOWN
                    ).distinct()

                    for advance_filter in country_filters:
                        query_param = advance_filter.query_param_filter
                        column_config = advance_filter.column_configuration
                        if not column_config:
                            continue

                        filter_field = column_config.name
                        filter_options = advance_filter.options or {}
                        static_choices = filter_options.get('choices', [])

                        filter_values = []
                        for choice in static_choices:
                            if isinstance(choice, dict) and 'value' in choice:
                                filter_values.append(choice['value'])

                        for value in filter_values:
                            filter_params = {f'{filter_field}__{query_param}': value, 'entity_type__code': entity_type.code}

                            global_stat_params = {'country_id': country.id}
                            global_stat_params.update(filter_params)
                            country_wise_task_list.append(update_cached_value.s(
                                url=reverse('entities:global-stat-all-entities'),
                                query_params=global_stat_params,
                            ))

                            info_params = {
                                'country_id': country.id,
                                'entity_type__code': entity_type.code,
                                f'{entity_type.code}_start_date': latest_week_start_str,
                                f'{entity_type.code}_end_date': latest_week_end_str,
                                f'{entity_type.code}_is_weekly': 'true',
                                f'{entity_type.code}_benchmark': 'global',
                                f'{entity_type.code}_include_same_location': 'false',
                            }
                            info_params.update(filter_params)
                            country_wise_task_list.append(update_cached_value.s(
                                url=reverse('entities:entity-info-data-layer', kwargs={'pk': layer_id}),
                                query_params=info_params,
                            ))

                    # Cache admin1 views
                    from proco.locations.models import CountryAdminMetadata
                    admin1_ids = list(CountryAdminMetadata.objects.filter(
                        country_id=country.id,
                        layer_name='adm1',
                        deleted__isnull=True,
                    ).values_list('id', flat=True))

                    for adm1_id in admin1_ids:
                        country_wise_task_list.append(update_cached_value.s(
                            url=reverse('entities:entity-get-latest-week-and-month'),
                            query_params={
                                'country_id': country.id,
                                'admin1_id': adm1_id,
                                'layer_id': layer_id,
                                'entity_type__code': entity_type.code,
                            },
                        ))

                        country_wise_task_list.append(update_cached_value.s(
                            url=reverse('entities:global-stat-all-entities'),
                            query_params={
                                'country_id': country.id,
                                'admin1_id': adm1_id,
                                'entity_type__code': entity_type.code,
                            },
                        ))

                        country_wise_task_list.append(update_cached_value.s(
                            url=reverse('entities:entity-info-data-layer', kwargs={'pk': layer_id}),
                            query_params={
                                'country_id': country.id,
                                'admin1_id': adm1_id,
                                'entity_type__code': entity_type.code,
                                f'{entity_type.code}_start_date': latest_week_start_str,
                                f'{entity_type.code}_end_date': latest_week_end_str,
                                f'{entity_type.code}_is_weekly': 'true',
                                f'{entity_type.code}_benchmark': 'global',
                                f'{entity_type.code}_include_same_location': 'false',
                            },
                        ))

            chain(country_wise_task_list).delay()

    background_task_utilities.task_on_complete(task_instance)
