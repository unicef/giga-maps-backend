from celery import chain
from django.core.cache import cache
from django.core.management import call_command
from django.db.models.functions.text import Lower
from django.urls import reverse
from rest_framework.test import APIClient

from proco.core import db_utils as db_utilities
from proco.core import utils as core_utilities
from proco.taskapp import app
from proco.utils.dates import format_date


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

    task_cache_key = 'update_all_cached_values_status_{current_time}'.format(
        current_time=format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y'),
    )
    running_task = cache.get(task_cache_key, None)

    if running_task in [None, b'completed', 'completed']:
        print('***** Not found running Job *****')
        cache.set(task_cache_key, 'running', None)

        update_cached_value.delay(url=reverse('connection_statistics:global-stat'))
        update_cached_value.delay(url=reverse('locations:countries-list'))
        update_cached_value.delay(url=reverse('locations:search-countries-admin-schools'))
        # update_cached_value.delay(url=reverse('locations:countries-boundary'))
        # update_cached_value.delay(url=reverse('schools:random-schools'))

        # Get countries which has at least has 1 school
        countries = Country.objects.filter(id__in=list(
            School.objects.all().values_list('country_id', flat=True).order_by('country_id').distinct('country_id')
        ))
        for country in countries:
            chain([
                update_cached_value.s(
                    url=reverse('connection_statistics:global-stat'),
                    query_params={'country_id': country.id},
                ),
                update_cached_value.s(
                    url=reverse('locations:countries-detail', kwargs={'pk': country.code.lower()})
                ),
                # update_cached_value.s(
                #     url=reverse('schools:schools-list', kwargs={'country_code': country.code.lower()})
                # ),
                update_cached_value.s(
                    url=reverse('connection_statistics:get-latest-week-and-month'),
                    query_params={'country_id': country.id},
                ),
            ]).delay()

        cache.set(task_cache_key, 'completed', None)
    else:
        print('***** Found running Job with "{0}" name so skipping current iteration *****'.format(task_cache_key))


@app.task
def update_country_related_cache(country_code):
    from proco.locations.models import Country

    update_cached_value.delay(url=reverse('connection_statistics:global-stat'))
    update_cached_value.delay(url=reverse('locations:countries-list'))
    update_cached_value.delay(url=reverse('locations:search-countries-admin-schools'))
    update_cached_value.delay(url=reverse('locations:countries-detail', kwargs={'pk': country_code.lower()}))
    # update_cached_value.delay(url=reverse('schools:random-schools'))
    # update_cached_value.delay(url=reverse('schools:schools-list', kwargs={'country_code': country_code.lower()}))
    country = Country.objects.annotate(
        code_lower=Lower('code'),
    ).filter(code_lower=country_code.lower())
    if country:
        update_cached_value.delay(
            url=reverse('connection_statistics:global-stat'),
            query_params={'country_id': country.id},
        )


@app.task(soft_time_limit=4 * 60 * 60, time_limit=4 * 60 * 60)
def rebuild_school_index():
    """
    rebuild_school_index
        Task which runs to rebuild the Cognitive Search Index for Schools from scratch.

        Frequency: Once in a day
        Limit: 15 mins
    """
    print('Rebuilding the School Index')
    task_cache_key = 'rebuild_school_index_status'
    running_task = cache.get(task_cache_key, None)

    if running_task in [None, b'completed', 'completed']:
        print('***** Not found running Job *****')
        cache.set(task_cache_key, 'running', None)
        args = ['--delete_index', '--create_index', '--clean_index', '--update_index']
        call_command('index_rebuild_schools', *args)

        cache.set(task_cache_key, 'completed', None)
    else:
        print('***** Found running Job with "{0}" name so skipping current iteration *****'.format(task_cache_key))


@app.task(soft_time_limit=1 * 60 * 60, time_limit=1 * 60 * 60)
def populate_school_registration_data():
    """
    populate_school_registration_data
        Task which runs to populate the RT table data for new schools.

        Frequency: Once in a day
        Limit: 1 hour
    """
    print('Setting RT status, RT Date for School which start live data from sources.')
    task_cache_key = 'populate_school_registration_data_status'
    running_task = cache.get(task_cache_key, None)

    if running_task in [None, b'completed', 'completed']:
        print('***** Not found running Job *****')
        cache.set(task_cache_key, 'running', None)
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
            args = ['--reset', '-school_id={0}'.format(missing_school_id['school_id'])]
            call_command('populate_school_registration_data', *args)

        cache.set(task_cache_key, 'completed', None)
    else:
        print('***** Found running Job with "{0}" name so skipping current iteration *****'.format(task_cache_key))


@app.task(soft_time_limit=10 * 60 * 60, time_limit=10 * 60 * 60)
def redo_aggregations_task(country_id, year, week_no, *args):
    """
    redo_aggregations_task
        Task to schedule manually from Console.
    """
    if not country_id or not year:
        print('ERROR: Required args not provided: [country_id, year]')
        return

    print('Starting redo_aggregations_task: Country ID "{0}" - Year "{1}" - Week "{2}"'.format(
        country_id, year, week_no))

    task_cache_key = 'redo_aggregations_task_country_id_{0}_year_{1}_week_{2}_on_{3}'.format(
        country_id, year, week_no, format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y'))
    running_task = cache.get(task_cache_key, None)

    if running_task in [None, b'completed', 'completed']:
        print('***** Not found running Job "{}" *****'.format(task_cache_key))
        cache.set(task_cache_key, 'running', None)
        args = [
            '-country_id={}'.format(country_id),
            '-year={}'.format(year),
            '--update_school_weekly',
            '--update_country_daily',
            '--update_country_weekly',
        ]

        if week_no:
            args.append('-week_no={}'.format(week_no))

        call_command('redo_aggregations', *args)

        cache.set(task_cache_key, 'completed', None)
    else:
        print('***** Found running Job with "{0}" name so skipping current iteration *****'.format(task_cache_key))


@app.task(soft_time_limit=10 * 60 * 60, time_limit=10 * 60 * 60)
def populate_school_new_fields_task(start_school_id, end_school_id, country_id, *args):
    """
    populate_school_new_fields_task
        Task to schedule manually from Console.
    """
    print(
        'Starting populate_school_new_fields_task: Country ID "{0}" - start_school_id "{1}" - end_school_id "{2}"'.format(
            country_id, start_school_id, end_school_id))

    task_cache_key = 'populate_school_new_fields_task_country_id_{0}_start_school_id_{1}_end_school_id_{2}_on_{3}'.format(
        country_id, start_school_id, end_school_id,
        format_date(core_utilities.get_current_datetime_object(), frmt='%d%m%Y'))
    running_task = cache.get(task_cache_key, None)

    if running_task in [None, b'completed', 'completed']:
        print('***** Not found running Job "{}" *****'.format(task_cache_key))
        cache.set(task_cache_key, 'running', None)
        args = []

        if country_id:
            args.append('-country_id={}'.format(country_id))

        if start_school_id:
            args.append('-start_school_id={}'.format(start_school_id))

        if end_school_id:
            args.append('-end_school_id={}'.format(end_school_id))

        call_command('populate_school_new_fields', *args)

        cache.set(task_cache_key, 'completed', None)
    else:
        print('***** Found running Job with "{0}" name so skipping current iteration *****'.format(task_cache_key))
