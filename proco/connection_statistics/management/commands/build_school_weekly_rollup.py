"""
P1 (perf): Build/refresh the SchoolWeeklyRollup table from SchoolDailyStatus.

Aggregates daily rows into per-(school, ISO week, live_data_source) decomposable partials
(SUM/COUNT/MIN/MAX) for every metric column in ROLLUP_METRIC_COLUMNS. Idempotent: re-running
upserts the affected weeks, so it works both as a one-time backfill and as an incremental refresh.

Examples:
    # full backfill
    python manage.py build_school_weekly_rollup
    # scope to one country / recent window (incremental)
    python manage.py build_school_weekly_rollup --country-id 144 --since 2026-01-01
"""
from django.core.management.base import BaseCommand
from django.db import connection

from proco.connection_statistics.models import ROLLUP_METRIC_COLUMNS

TABLE = 'connection_statistics_schoolweeklyrollup'
DAILY = 'connection_statistics_schooldailystatus'
SCHOOL = 'schools_school'


def _build_upsert_sql(where_extra, params):
    # Per-column partial aggregate expressions (generic over ROLLUP_METRIC_COLUMNS).
    select_partials = []
    insert_cols = []
    update_sets = []
    for col in ROLLUP_METRIC_COLUMNS:
        select_partials.append(
            'SUM(sds.{c})::float AS {c}_sum, '
            'COUNT(sds.{c}) AS {c}_count, '
            'MIN(sds.{c})::float AS {c}_min, '
            'MAX(sds.{c})::float AS {c}_max'.format(c=col)
        )
        for suffix in ('sum', 'count', 'min', 'max'):
            insert_cols.append('{0}_{1}'.format(col, suffix))
            update_sets.append('{0}_{1} = EXCLUDED.{0}_{1}'.format(col, suffix))

    partials_select = ',\n            '.join(select_partials)
    partials_insert = ', '.join(insert_cols)
    partials_update = ', '.join(update_sets)

    sql = """
    INSERT INTO {table}
        (created, modified, school_id, country_id, admin1_id, year, week, date, live_data_source,
         {partials_insert})
    SELECT now(), now(), sds.school_id, s.country_id, s.admin1_id,
           EXTRACT(isoyear FROM sds.date)::int AS year,
           EXTRACT(week FROM sds.date)::int AS week,
           date_trunc('week', sds.date)::date AS date,
           sds.live_data_source,
           {partials_select}
    FROM {daily} sds
    INNER JOIN {school} s ON s.id = sds.school_id
    WHERE sds.deleted IS NULL
      AND s.deleted IS NULL
      {where_extra}
    GROUP BY sds.school_id, s.country_id, s.admin1_id,
             EXTRACT(isoyear FROM sds.date), EXTRACT(week FROM sds.date),
             date_trunc('week', sds.date), sds.live_data_source
    ON CONFLICT (school_id, year, week, live_data_source)
    DO UPDATE SET modified = now(), country_id = EXCLUDED.country_id, admin1_id = EXCLUDED.admin1_id,
                  date = EXCLUDED.date, {partials_update}
    """.format(
        table=TABLE, daily=DAILY, school=SCHOOL,
        partials_insert=partials_insert, partials_select=partials_select,
        where_extra=where_extra, partials_update=partials_update,
    )
    return sql, params


class Command(BaseCommand):
    help = 'Build/refresh SchoolWeeklyRollup from SchoolDailyStatus.'

    def add_arguments(self, parser):
        parser.add_argument('--country-id', type=int, default=None,
                            help='Restrict to a single country (schools_school.country_id).')
        parser.add_argument('--since', type=str, default=None,
                            help='Only aggregate daily rows with date >= this ISO date (YYYY-MM-DD).')

    def handle(self, *args, **options):
        where_extra = ''
        params = []
        if options.get('country_id'):
            where_extra += ' AND s.country_id = %s'
            params.append(options['country_id'])
        if options.get('since'):
            where_extra += ' AND sds.date >= %s'
            params.append(options['since'])

        sql, params = _build_upsert_sql(where_extra, params)

        self.stdout.write('Building SchoolWeeklyRollup (country_id={0}, since={1}) ...'.format(
            options.get('country_id'), options.get('since')))
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            affected = cursor.rowcount
        self.stdout.write(self.style.SUCCESS('Done. Upserted {0} rollup rows.'.format(affected)))
