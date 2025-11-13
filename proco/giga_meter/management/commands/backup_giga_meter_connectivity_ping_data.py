import logging
from datetime import timedelta

from django.core.management.base import BaseCommand

from proco.core.utils import get_current_datetime_object
from proco.giga_meter import tasks as giga_meter_tasks
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
        # the date for which backup is performed
        parser.add_argument(
            '-start_date', dest='start_date', type=str,
            default=date_utilities.format_date(today_date - timedelta(days=365)),  # 1 year from today
            help='Date from we need to check the records. Default is 1 year from Current date.'
        )

        parser.add_argument(
            '-end_date', dest='end_date', type=str,
            default=date_utilities.format_date(today_date - timedelta(days=180)),  # 6 months from now
            help='Date till we need to check the records. Default is 6 months from Current date.'
        )

        # days from now for which backup is performed
        parser.add_argument(
            '-backup_days', dest='backup_days', type=int, default=None,
            help='Beyond this time period, create a pipeline to archive this data in Delta Lake.'
        )

    def handle(self, **options):
        logger.info('Started Backup of "GigaMeter Connectivity Ping data" utility.\n')

        start_date = date_utilities.to_date(options.get('start_date'))
        end_date = date_utilities.to_date(options.get('end_date'))

        backup_days = options.get('backup_days')
        # If backup days provided then move all data beyond these days to Delta lake
        if backup_days and backup_days > 0:
            end_date = date_utilities.format_date(today_date - timedelta(days=backup_days))
            start_date = None
        elif backup_days:
            logger.error('Invalid value provided for "-backup_days" argument.')
            exit(0)

        if start_date and start_date > end_date:
            logger.error('Start date value can not be greater than end_date.')
            exit(0)

        connectivity_ping_checks_qs = GigaMeter_ConnectivityPingChecks.objects.filter(
            created_at__date__lte=end_date,
        ).values_list('created_at__date', flat=True).order_by('created_at__date').distinct('created_at__date')

        if not backup_days:
            connectivity_ping_checks_qs = connectivity_ping_checks_qs.filter(created_at__date__gte=start_date)

        date_list = list(connectivity_ping_checks_qs)
        if len(date_list) == 0:
            logger.error(
                f'Ping data is not available for the give date range. Start Date: {start_date}, End Date: {end_date}')
            exit(0)

        schedule_tasks = options.get('schedule_tasks')
        if schedule_tasks:
            giga_meter_tasks.scheduler_for_backup_giga_meter_connectivity_ping_data.delay(
                date_utilities.format_date(date_list[0]),
                date_utilities.format_date(date_list[-1]),
            )
            logger.info('Completed scheduling the "GigaMeter Connectivity Ping data" utility successfully.\n')
            exit(0)

        logger.info(f'Starting the back for date range between '
                    f'{date_utilities.format_date(start_date)} - {date_utilities.format_date(end_date)}')
        for backup_date in date_list:
            # Backup the data to delta lake

            # # delete the data from DB once successfully backed up
            # GigaMeter_ConnectivityPingChecks.objects.filter(
            #     created_at__date=backup_date,
            # ).delete()
            logger.info(f'Data deleted from DB where created_at::date == {date_utilities.format_date(backup_date)}')

        logger.info('Completed "GigaMeter Connectivity Ping data" utility successfully.\n')
