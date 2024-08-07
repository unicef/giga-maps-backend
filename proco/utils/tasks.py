import logging
import time
import uuid

from celery import chain
from celery import current_task
from django.core.management import call_command
from django.db.models import Q
from django.db.models.functions.text import Lower
from django.urls import reverse
from rest_framework.test import APIClient

from proco.background import utils as background_task_utilities
from proco.core import db_utils as db_utilities
from proco.core import utils as core_utilities
from proco.taskapp import app
from proco.utils.dates import format_date

logger = logging.getLogger('gigamaps.' + __name__)


@app.task(soft_time_limit=10 * 60, time_limit=11 * 60)
def update_cached_value(*args, url='', query_params=None, **kwargs):
    client = APIClient()
    if query_params:
        query_params['cache'] = False
        client.get(url, query_params, format='json')
    else:
        client.get(url, {'cache': False}, format='json')


@app.task(soft_time_limit=15 * 60, time_limit=15 * 60)
def update_all_cached_values():
    from proco.locations.models import Country
    from proco.schools.models import School
    from proco.accounts.models import DataLayerCountryRelationship, DataLayer

    task_key = 'update_all_cached_values_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H%M'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Update the Redis cache, allowed once in a hour')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))
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
                    url=reverse('accounts:list-published-advance-filters'),
                    kwargs={'status': 'PUBLISHED', 'country_id': country.id},
                    query_params={'expand': 'column_configuration'},
                ),
            ]

            if country_wise_default_layers.get(country.id, None):
                country_wise_task_list.append(update_cached_value.s(
                    url=reverse('connection_statistics:get-latest-week-and-month'),
                    query_params={
                        'country_id': country.id,
                        'layer_id': country_wise_default_layers[country.id],
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
            url=reverse('accounts:list-published-advance-filters'),
            kwargs={'status': 'PUBLISHED', 'country_id': country.id},
            query_params={'expand': 'column_configuration'},
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
    logger.info('Setting RT status, RT Date for schools which start live data from sources.')

    task_key = 'populate_school_registration_data_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y_%H'))

    task_id = current_task.request.id or str(uuid.uuid4())
    task_instance = background_task_utilities.task_on_start(
        task_id, task_key, 'Populate the RT table data for new schools')

    if task_instance:
        logger.debug('Not found running job: {}'.format(task_key))
        sql = """
        SELECT DISTINCT sds.school_id
        FROM public.connection_statistics_schooldailystatus AS sds
        LEFT JOIN public.connection_statistics_schoolrealtimeregistration AS srt
            ON sds.school_id = srt.school_id
            AND sds.deleted IS NULL
            AND srt.deleted IS NULL
        WHERE srt.school_id IS NULL
        """

        school_ids_missing_in_rt_table = db_utilities.sql_to_response(sql, label='SchoolRealtimeRegistration')

        for missing_school_id in school_ids_missing_in_rt_table:
            cmd_args = ['--reset', '-school_id={0}'.format(missing_school_id['school_id'])]
            call_command('populate_school_registration_data', *cmd_args)

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
