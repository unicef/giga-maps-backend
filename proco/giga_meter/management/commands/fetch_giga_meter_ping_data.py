from django.core.management.base import BaseCommand

from proco.giga_meter.tasks import fetch_and_aggregate_ping_data


class Command(BaseCommand):
    help = 'Fetches GigaMeter ping data and aggregates it into SchoolDailyStatus.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Date to fetch data for (YYYY-MM-DD). Defaults to yesterday.',
        )
        parser.add_argument(
            '--schedule',
            action='store_true',
            help='Execute the task asynchronously using Celery.',
        )

    def handle(self, *args, **options):
        date_str = options.get('date')
        use_celery = options.get('schedule')

        self.stdout.write(f"Starting GigaMeter ping data fetch for date: {date_str or 'Yesterday'}")

        try:
            if use_celery:
                fetch_and_aggregate_ping_data.delay(date_str=date_str)
                self.stdout.write(
                    self.style.SUCCESS("Successfully submitted GigaMeter ping data aggregation task to Celery."))
            else:
                fetch_and_aggregate_ping_data(date_str=date_str)
                self.stdout.write(self.style.SUCCESS("Successfully aggregated GigaMeter ping data."))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error executing GigaMeter ping data command: {e}"))