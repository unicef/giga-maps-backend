import logging
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.utils import timezone
from isoweek import Week

from proco.connection_statistics.models import SchoolRealTimeWeeklyMetric
from proco.connection_statistics.realtime_weekly_metrics import (
    get_active_published_live_aggregate_requirements,
)
from proco.locations.models import Country
from proco.utils import dates as date_utilities


logger = logging.getLogger('gigamaps.' + __name__)


LIVE_METRIC_FIELDS = {
    'connectivity_speed',
    'connectivity_upload_speed',
    'connectivity_latency',
    'uptime',
    'roundtrip_time',
    'jitter_download',
    'jitter_upload',
    'rtt_packet_loss_pct',
    'connectivity_speed_probe',
    'connectivity_upload_speed_probe',
    'connectivity_latency_probe',
    'connectivity_speed_mean',
    'connectivity_upload_speed_mean',
}

AGG_FUNCTION_SQL = {
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_AVG: 'AVG({field})',
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_MIN: 'MIN({field})',
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_MAX: 'MAX({field})',
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_SUM: 'SUM({field})',
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_MEDIAN_50: 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {field})',
    SchoolRealTimeWeeklyMetric.AGG_FUNCTION_MEDIAN_90: 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY {field})',
}


def get_iso_week_start_date(year, week_no):
    return Week(year, week_no).monday()


class Command(BaseCommand):
    help = 'Populate weekly realtime metric aggregates required by active published live layers.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-country_id', dest='country_id', required=False, type=int,
            help='Optional country ID. If omitted, all countries required by active published live layers are processed.',
        )
        parser.add_argument(
            '-layer_id', dest='layer_id', required=False, type=int,
            help='Optional data layer ID. If omitted, all active published live layers are processed.',
        )
        parser.add_argument(
            '-year', dest='year', required=False, type=int,
            help='Optional ISO year. Use with -week_no for a single week.',
        )
        parser.add_argument(
            '-week_no', dest='week_no', required=False, type=int,
            help='Optional ISO week number. If provided without -year, current ISO year is used.',
        )
        parser.add_argument(
            '-start_date', dest='start_date', required=False,
            help='Optional start date. Defaults to retention window before today.',
        )
        parser.add_argument(
            '-end_date', dest='end_date', required=False,
            help='Optional end date. Defaults to today.',
        )
        parser.add_argument(
            '--dry_run', action='store_true', dest='dry_run', default=False,
            help='Log expected work without changing data.',
        )
        parser.add_argument(
            '--schedule', action='store_true', dest='schedule_task', default=False,
            help='Submit the population work to Celery and return immediately.',
        )
        parser.add_argument(
            '--cleanup_old', action='store_true', dest='cleanup_old', default=False,
            help='Hard-delete realtime weekly metric rows older than retention_days before populating.',
        )
        parser.add_argument(
            '--cleanup_only', action='store_true', dest='cleanup_only', default=False,
            help='Only hard-delete realtime weekly metric rows older than retention_days.',
        )
        parser.add_argument(
            '-retention_days', dest='retention_days', required=False, type=int,
            default=settings.SCHOOL_REALTIME_WEEKLY_METRIC_RETENTION_DAYS,
            help='Number of days of realtime weekly metric data to retain. Defaults to settings value.',
        )

    def get_date_range(self, options):
        if options.get('week_no') is not None:
            year = options.get('year') or date_utilities.get_current_year()
            start_date = get_iso_week_start_date(year, options['week_no'])
            return start_date, start_date + timedelta(days=6)

        end_date = (
            self.parse_date_option(options['end_date'], 'end_date')
            if options.get('end_date') else timezone.localdate()
        )
        start_date = (
            self.parse_date_option(options['start_date'], 'start_date')
            if options.get('start_date') else end_date - timedelta(days=settings.SCHOOL_REALTIME_WEEKLY_METRIC_RETENTION_DAYS)
        )

        if start_date > end_date:
            raise CommandError('start_date cannot be greater than end_date.')

        return start_date, end_date

    def parse_date_option(self, value, option_name):
        parsed_date = date_utilities.to_date(value)
        if parsed_date is None:
            raise CommandError('Invalid {option_name}: {value}'.format(option_name=option_name, value=value))
        return parsed_date.date()

    def get_country_ids_filter(self, country_id):
        if country_id is None:
            return None

        if not Country.objects.filter(id=country_id, deleted__isnull=True).exists():
            raise CommandError('Country with id {} does not exist or is deleted.'.format(country_id))

        return [str(country_id)]

    def get_layer_ids_filter(self, layer_id):
        if layer_id is None:
            return None
        return [layer_id]

    def get_weeks_to_process(self, country_id, requirement, start_date, end_date):
        metric = requirement['metric_name']
        live_data_sources = requirement['live_data_sources']

        if metric not in LIVE_METRIC_FIELDS:
            logger.warning(
                'Skipping config_hash=%s because metric "%s" is not supported by SchoolDailyStatus.',
                requirement['config_hash'],
                metric,
            )
            return []

        source_placeholders = ','.join(['%s'] * len(live_data_sources))
        params = [country_id, start_date, end_date] + live_data_sources
        sql = """
            SELECT DISTINCT
                EXTRACT(ISOYEAR FROM sds.date)::integer AS year,
                EXTRACT(WEEK FROM sds.date)::integer AS week
            FROM connection_statistics_schooldailystatus sds
            INNER JOIN schools_school s ON s.id = sds.school_id
            WHERE s.country_id = %s
                AND s.deleted IS NULL
                AND sds.deleted IS NULL
                AND sds.date BETWEEN %s AND %s
                AND sds.live_data_source IN ({source_placeholders})
                AND sds."{metric}" IS NOT NULL
            ORDER BY year ASC, week ASC
        """.format(source_placeholders=source_placeholders, metric=metric)

        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()

        weeks = []
        for year, week_no in rows:
            week_start_date = get_iso_week_start_date(year, week_no)
            weeks.append({
                'year': year,
                'week_no': week_no,
                'start_date': week_start_date,
                'end_date': week_start_date + timedelta(days=6),
            })

        return weeks

    def delete_existing_rows(self, country_id, year, week_no, config_hash):
        deleted_count, _ = SchoolRealTimeWeeklyMetric.objects.filter(
            country_id=country_id,
            year=year,
            week=week_no,
            config_hash=config_hash,
        ).delete()

        return deleted_count

    def insert_aggregate_rows(self, country_id, week_to_process, requirement):
        metric = requirement['metric_name']
        agg_function = requirement['agg_function']
        live_data_sources = requirement['live_data_sources']

        if agg_function not in AGG_FUNCTION_SQL:
            logger.warning(
                'Skipping config_hash=%s because aggregate function "%s" is not supported.',
                requirement['config_hash'],
                agg_function,
            )
            return 0

        quoted_field = 'sds."{}"'.format(metric)
        agg_sql = AGG_FUNCTION_SQL[agg_function].format(field=quoted_field)
        source_placeholders = ','.join(['%s'] * len(live_data_sources))

        sql = """
            INSERT INTO {table_name} (
                created,
                modified,
                school_id,
                country_id,
                year,
                week,
                date,
                config_hash,
                live_data_sources,
                metric_name,
                agg_function,
                agg_value,
                sample_count
            )
            SELECT
                NOW(),
                NOW(),
                sds.school_id,
                s.country_id,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                {agg_sql},
                COUNT({field})
            FROM connection_statistics_schooldailystatus sds
            INNER JOIN schools_school s ON s.id = sds.school_id
            WHERE s.country_id = %s
                AND s.deleted IS NULL
                AND sds.deleted IS NULL
                AND sds.date BETWEEN %s AND %s
                AND sds.live_data_source IN ({source_placeholders})
                AND {field} IS NOT NULL
            GROUP BY sds.school_id, s.country_id
        """.format(
            table_name=SchoolRealTimeWeeklyMetric._meta.db_table,
            agg_sql=agg_sql,
            field=quoted_field,
            source_placeholders=source_placeholders,
        )

        params = [
            week_to_process['year'],
            week_to_process['week_no'],
            week_to_process['start_date'],
            requirement['config_hash'],
            requirement['live_data_sources_key'],
            metric,
            agg_function,
            country_id,
            week_to_process['start_date'],
            week_to_process['end_date'],
        ] + live_data_sources

        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount

    def populate_requirement_for_country(self, country_id, requirement, start_date, end_date, dry_run=False):
        weeks_to_process = self.get_weeks_to_process(country_id, requirement, start_date, end_date)

        if len(weeks_to_process) == 0:
            logger.info(
                'No matching SchoolDailyStatus rows for country=%s config_hash=%s metric=%s sources=%s',
                country_id,
                requirement['config_hash'],
                requirement['metric_name'],
                requirement['live_data_sources_key'],
            )
            return 0, 0, 0

        total_deleted = 0
        total_inserted = 0

        for week_to_process in weeks_to_process:
            if dry_run:
                logger.info(
                    'Dry run: would refresh country=%s year=%s week=%s config_hash=%s metric=%s '
                    'function=%s sources=%s',
                    country_id,
                    week_to_process['year'],
                    week_to_process['week_no'],
                    requirement['config_hash'],
                    requirement['metric_name'],
                    requirement['agg_function'],
                    requirement['live_data_sources_key'],
                )
                continue

            with transaction.atomic():
                deleted = self.delete_existing_rows(
                    country_id,
                    week_to_process['year'],
                    week_to_process['week_no'],
                    requirement['config_hash'],
                )
                inserted = self.insert_aggregate_rows(country_id, week_to_process, requirement)
            total_deleted += deleted
            total_inserted += inserted

            logger.info(
                'Refreshed country=%s year=%s week=%s config_hash=%s deleted=%s inserted=%s',
                country_id,
                week_to_process['year'],
                week_to_process['week_no'],
                requirement['config_hash'],
                deleted,
                inserted,
            )

        return len(weeks_to_process), total_deleted, total_inserted

    def cleanup_old_rows(self, retention_days, dry_run=False):
        if retention_days <= 0:
            raise CommandError('retention_days must be greater than zero.')

        cutoff_date = timezone.localdate() - timedelta(days=retention_days)
        queryset = SchoolRealTimeWeeklyMetric.objects.filter(date__lt=cutoff_date)
        row_count = queryset.count()

        logger.info(
            'Cleaning old SchoolRealTimeWeeklyMetric rows. retention_days=%s cutoff_date=%s rows=%s dry_run=%s',
            retention_days,
            cutoff_date,
            row_count,
            dry_run,
        )

        if dry_run:
            return row_count

        deleted_count, _ = queryset.delete()
        logger.info(
            'Deleted old SchoolRealTimeWeeklyMetric rows. retention_days=%s cutoff_date=%s deleted=%s',
            retention_days,
            cutoff_date,
            deleted_count,
        )
        return deleted_count

    def handle(self, **options):
        if options.get('schedule_task'):
            from proco.connection_statistics import tasks as statistics_tasks

            statistics_tasks.populate_school_live_weekly_metrics.delay(
                country_id=options.get('country_id'),
                layer_id=options.get('layer_id'),
                year=options.get('year'),
                week_no=options.get('week_no'),
                start_date=options.get('start_date'),
                end_date=options.get('end_date'),
                cleanup_old=options.get('cleanup_old'),
                cleanup_only=options.get('cleanup_only'),
                retention_days=options.get('retention_days'),
                dry_run=options.get('dry_run'),
            )
            self.stdout.write(
                self.style.SUCCESS('Submitted SchoolRealTimeWeeklyMetric population task to Celery.')
            )
            return

        if options.get('cleanup_old') or options.get('cleanup_only'):
            deleted = self.cleanup_old_rows(options['retention_days'], dry_run=options['dry_run'])
            self.stdout.write(
                self.style.SUCCESS('SchoolRealTimeWeeklyMetric cleanup completed. Rows: {0}.'.format(deleted))
            )

        if options.get('cleanup_only'):
            return

        start_date, end_date = self.get_date_range(options)
        country_ids_filter = self.get_country_ids_filter(options.get('country_id'))
        layer_ids_filter = self.get_layer_ids_filter(options.get('layer_id'))
        dry_run = options['dry_run']

        requirements = get_active_published_live_aggregate_requirements(
            layer_ids=layer_ids_filter,
            country_ids=country_ids_filter,
        )

        if len(requirements) == 0:
            message = 'No active published live aggregate requirements found.'
            logger.info(message)
            self.stdout.write(self.style.WARNING(message))
            return

        logger.info(
            'Populating SchoolRealTimeWeeklyMetric for date_range=%s:%s requirements=%s dry_run=%s',
            start_date,
            end_date,
            len(requirements),
            dry_run,
        )

        total_weeks = 0
        total_deleted = 0
        total_inserted = 0

        for requirement in requirements:
            logger.info(
                'Processing config_hash=%s metric=%s function=%s sources=%s countries=%s layers=%s',
                requirement['config_hash'],
                requirement['metric_name'],
                requirement['agg_function'],
                requirement['live_data_sources_key'],
                requirement['country_ids'],
                requirement['data_layer_ids'],
            )

            for country_id in requirement['country_ids']:
                weeks, deleted, inserted = self.populate_requirement_for_country(
                    country_id,
                    requirement,
                    start_date,
                    end_date,
                    dry_run=dry_run,
                )
                total_weeks += weeks
                total_deleted += deleted
                total_inserted += inserted

        message = (
            'Completed SchoolRealTimeWeeklyMetric population. Requirements: {requirements}. '
            'Week/config/country batches: {weeks}. Deleted rows: {deleted}. Inserted rows: {inserted}.'
        ).format(
            requirements=len(requirements),
            weeks=total_weeks,
            deleted=total_deleted,
            inserted=total_inserted,
        )
        logger.info(message)
        self.stdout.write(self.style.SUCCESS(message))
