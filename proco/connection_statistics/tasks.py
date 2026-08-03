import logging
from datetime import timedelta

from django.conf import settings
from django.core.management import call_command
from django.utils import timezone

from proco.taskapp import app


logger = logging.getLogger('gigamaps.' + __name__)


@app.task
def populate_current_school_live_weekly_metrics():
    today = timezone.localdate()
    year, week_no, _ = today.isocalendar()

    logger.info(
        'Scheduling current-week SchoolRealTimeWeeklyMetric population. year=%s week=%s',
        year,
        week_no,
    )
    call_command(
        'populate_school_live_weekly_metrics',
        '-year={0}'.format(year),
        '-week_no={0}'.format(week_no),
    )


@app.task
def populate_school_live_weekly_metrics(
    country_id=None,
    layer_id=None,
    year=None,
    week_no=None,
    start_date=None,
    end_date=None,
    cleanup_old=False,
    cleanup_only=False,
    retention_days=None,
    dry_run=False,
):
    cmd_args = []

    if country_id is not None:
        cmd_args.append('-country_id={0}'.format(country_id))
    if layer_id is not None:
        cmd_args.append('-layer_id={0}'.format(layer_id))
    if year is not None:
        cmd_args.append('-year={0}'.format(year))
    if week_no is not None:
        cmd_args.append('-week_no={0}'.format(week_no))
    if start_date is not None:
        cmd_args.append('-start_date={0}'.format(start_date))
    if end_date is not None:
        cmd_args.append('-end_date={0}'.format(end_date))
    if cleanup_old:
        cmd_args.append('--cleanup_old')
    if cleanup_only:
        cmd_args.append('--cleanup_only')
    if retention_days is not None:
        cmd_args.append('-retention_days={0}'.format(retention_days))
    if dry_run:
        cmd_args.append('--dry_run')

    logger.info('Scheduling SchoolRealTimeWeeklyMetric population command. args=%s', cmd_args)
    call_command('populate_school_live_weekly_metrics', *cmd_args)


@app.task
def cleanup_old_school_live_weekly_metrics(retention_days=None):
    retention_days = retention_days or settings.SCHOOL_REALTIME_WEEKLY_METRIC_RETENTION_DAYS
    logger.info(
        'Scheduling old SchoolRealTimeWeeklyMetric cleanup. retention_days=%s',
        retention_days,
    )
    call_command(
        'populate_school_live_weekly_metrics',
        '--cleanup_only',
        '-retention_days={0}'.format(retention_days),
    )


@app.task
def backfill_school_live_weekly_metrics_for_layer(
    layer_id,
    retention_days=None,
):
    retention_days = retention_days or settings.SCHOOL_REALTIME_WEEKLY_METRIC_RETENTION_DAYS
    end_date = timezone.localdate()
    start_date = end_date - timedelta(days=retention_days)

    logger.info(
        'Scheduling layer SchoolRealTimeWeeklyMetric backfill. layer_id=%s start_date=%s end_date=%s '
        'retention_days=%s',
        layer_id,
        start_date,
        end_date,
        retention_days,
    )
    call_command(
        'populate_school_live_weekly_metrics',
        '-layer_id={0}'.format(layer_id),
        '-start_date={0}'.format(start_date.isoformat()),
        '-end_date={0}'.format(end_date.isoformat()),
    )
