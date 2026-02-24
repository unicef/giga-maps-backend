import logging
from datetime import timedelta
import sys
from django.core.management.base import BaseCommand

from proco.core.utils import get_current_datetime_object
from proco.giga_meter import tasks as giga_meter_tasks
from proco.giga_meter import connectivity_ping_backup_manager as backup_manager
from proco.giga_meter.models import GigaMeter_ConnectivityPingChecks
from proco.utils import dates as date_utilities

logger = logging.getLogger('gigamaps.' + __name__)
today_date = get_current_datetime_object().date()


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--schedule', action='store_true', dest='schedule_tasks', default=False,
            help='If provided, it will schedule the task on Celery.'
        )
        parser.add_argument(
            '-start_date', dest='start_date', type=str,
            default=None,
            help='Date from we need to check the records. Default is 1 year from Current date.'
        )
        parser.add_argument(
            '-end_date', dest='end_date', type=str,
            default=None,
            help='Date till we need to check the records. Default is 6 months from Current date.'
        )
        parser.add_argument(
            '-till_date', dest='till_date', type=str,
            default=date_utilities.format_date(today_date - timedelta(days=180)),  # 6 months from now
            help='Date till we need to check the records. Default is 6 months from Current date.'
        )

    def handle(self, **options):
        logger.info('Started Backup of "GigaMeter Connectivity Ping data" utility.\n')

        start_date = date_utilities.to_date(options.get('start_date'))
        end_date = date_utilities.to_date(options.get('end_date'))

        if start_date and start_date > end_date:
            logger.error('Start date value can not be greater than end_date.')
            sys.exit(0)

        till_date = date_utilities.to_date(options.get('till_date'))

        # Use either start_date and end_date or till_date
        # Only process dates that have non-deleted records
        if start_date and end_date:
            connectivity_ping_checks_qs = GigaMeter_ConnectivityPingChecks.objects.filter(
                created_at__date__gte=start_date,
                created_at__date__lte=end_date,
                is_deleted=False,
            ).values_list('created_at__date', flat=True).order_by('created_at__date').distinct('created_at__date')
        else:
            connectivity_ping_checks_qs = GigaMeter_ConnectivityPingChecks.objects.filter(
                created_at__date__lte=till_date,
                is_deleted=False,
            ).values_list('created_at__date', flat=True).order_by('created_at__date').distinct('created_at__date')

        date_list = list(connectivity_ping_checks_qs)
        if len(date_list) == 0:
            if start_date and end_date:
                logger.error(f'Ping data is not available for date range. Start Date: {start_date}, End Date: {end_date}.')
            else:
                logger.error(f'Ping data is not available for before date: {till_date}.')
            sys.exit(0)

        schedule_tasks = options.get('schedule_tasks')
        if schedule_tasks:
            giga_meter_tasks.scheduler_for_backup_giga_meter_connectivity_ping_data.delay(
                start_date=date_utilities.format_date(date_list[0]),
                end_date=date_utilities.format_date(date_list[-1]),
            )
            logger.info('Completed scheduling the "GigaMeter Connectivity Ping data" utility successfully.\n')
            sys.exit(0)

        logger.info(f'Starting backup for date range between '
                    f'{date_utilities.format_date(date_list[0])} - {date_utilities.format_date(date_list[-1])}')

        try:
            # Task 1 & 2: Backup data to ADLS and soft delete using production utility
            successful_dates, error_messages = backup_manager.backup_connectivity_ping_data(date_list)

            if successful_dates:
                logger.info(f'Successfully backed up data for {len(successful_dates)} dates: '
                           f'{[date_utilities.format_date(d) for d in successful_dates]}')

            if error_messages:
                logger.error(f'Failed to backup data for {len(error_messages)} dates. Errors: '
                           f'{error_messages}')

            # Task 3: Hard delete old records using production utility
            deleted_count = backup_manager.hard_delete_old_ping_data(today_date)
            logger.info(f'Hard deleted {deleted_count} old records from database')

        except Exception as e:
            logger.error(f'Backup operation failed: {str(e)}')
            sys.exit(1)

        logger.info('Completed "GigaMeter Connectivity Ping data" utility successfully.\n')