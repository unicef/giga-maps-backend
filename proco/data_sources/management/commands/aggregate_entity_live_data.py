import logging
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from proco.locations.models import Country
from proco.connection_statistics.utils import aggregate_entity_daily_status_to_entity_weekly_status
from proco.connection_statistics.models import EntityDailyStatus, EntityRealTimeRegistration
from proco.core.utils import get_current_datetime_object
from django.db.models import Min

logger = logging.getLogger('gigamaps.' + __name__)

class Command(BaseCommand):
    help = 'Aggregate EntityDailyStatus to EntityWeeklyStatus and ensure EntityRealTimeRegistration exists'

    def add_arguments(self, parser):
        parser.add_argument(
            '-start_date', dest='start_date', type=str,
            default=get_current_datetime_object().date().strftime('%Y-%m-%d'),
            help='Start date in YYYY-MM-DD format. Default is today.'
        )
        parser.add_argument(
            '-end_date', dest='end_date', type=str,
            default=get_current_datetime_object().date().strftime('%Y-%m-%d'),
            help='End date in YYYY-MM-DD format. Default is today.'
        )
        parser.add_argument(
            '-country_id', dest='country_id', type=int,
            help='Optional: Only process for a specific country ID.',
            required=False,
        )

    def handle(self, **options):
        logger.info('Starting Entity Live Data Aggregation backfill...')
        
        try:
            start_date_str = options.get('start_date')
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            
            end_date_str = options.get('end_date')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            logger.error('Invalid date format. Please use YYYY-MM-DD.')
            return

        if start_date > end_date:
            logger.error('start_date cannot be greater than end_date.')
            return

        country_id = options.get('country_id')
        
        countries = Country.objects.all()
        if country_id:
            countries = countries.filter(id=country_id)
            
        logger.info(f"Processing dates {start_date} to {end_date} for {countries.count()} countries.")

        for country in countries:
            logger.info(f"Processing Country: {country.name} ({country.code})")
            
            # Create/update EntityRealTimeRegistration for any entity in this country that has DailyStatus
            daily_stats = EntityDailyStatus.objects.filter(
                entity__country=country,
                date__gte=start_date,
                date__lte=end_date,
                entity__deleted__isnull=True
            ).values('entity', 'live_data_source').annotate(
                first_seen=Min('created')
            )
            
            regs_created = 0
            regs_updated = 0
            
            for stat in daily_stats:
                entity_id = stat['entity']
                source = stat['live_data_source']
                registration_date = stat['first_seen']
                
                reg, created = EntityRealTimeRegistration.objects.get_or_create(
                    entity_id=entity_id,
                    defaults={
                        'rt_registered': True,
                        'rt_source': source,
                        'rt_registration_date': registration_date
                    }
                )
                if created:
                    regs_created += 1
                elif not reg.rt_registered:
                    reg.rt_registered = True
                    reg.rt_source = source
                    reg.rt_registration_date = registration_date
                    reg.save()
                    regs_updated += 1
                    
            if regs_created > 0 or regs_updated > 0:
                logger.info(f"  - EntityRealTimeRegistration: {regs_created} created, {regs_updated} updated.")
            
            # Iterate through days and aggregate Weekly Status
            current_date = start_date
            while current_date <= end_date:
                # The aggregation function actually works on the whole week based on the date passed
                # So we only need to call it once per week. Let's just call it for each date for simplicity,
                # the function safely uses update_or_create logic (using .last() and save) 
                # but to be efficient we can jump by 7 days if we ensure we hit every week boundary
                aggregate_entity_daily_status_to_entity_weekly_status(country, current_date)
                
                # move to next week safely
                current_date += timedelta(days=7)
                
            # Call it one last time on the end_date just in case the +7 jump skipped the final week
            aggregate_entity_daily_status_to_entity_weekly_status(country, end_date)

        logger.info("Completed Entity Live Data Aggregation backfill.")
