from datetime import datetime, timedelta

from django.test import TestCase
from django.utils import timezone

from proco.connection_statistics.models import (
    CountryDailyStatus,
    CountryWeeklyStatus,
    SchoolDailyStatus,
    SchoolWeeklyStatus,
)
from proco.connection_statistics.tests.factories import (
    CountryDailyStatusFactory,
    RealTimeConnectivityFactory,
    SchoolDailyStatusFactory,
    SchoolWeeklyStatusFactory,
)
from proco.connection_statistics.utils import (
    aggregate_real_time_data_to_school_daily_status,
    aggregate_school_daily_status_to_school_weekly_status,
    aggregate_school_daily_to_country_daily,
    update_country_weekly_status,
)
from proco.data_sources.tasks import finalize_previous_day_data
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory
from proco.utils.dates import get_current_week, get_current_year


class AggregateConnectivityDataTestCase(TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()
        cls.school = SchoolFactory(country=cls.country)
        cls.today_datetime = datetime.now(tz=timezone.utc)

        RealTimeConnectivityFactory(school=cls.school, connectivity_speed=4000000, created=cls.today_datetime,
                                    live_data_source='DAILY_CHECK_APP_MLAB')
        RealTimeConnectivityFactory(school=cls.school, connectivity_speed=6000000, created=cls.today_datetime,
                                    live_data_source='DAILY_CHECK_APP_MLAB')

    def test_aggregate_real_time_data_to_school_daily_status(self):
        aggregate_real_time_data_to_school_daily_status(self.country, timezone.now().date())
        self.assertEqual(SchoolDailyStatus.objects.count(approx=False), 1)
        self.assertEqual(SchoolDailyStatus.objects.first().connectivity_speed, 5000000)

    def test_aggregate_real_time_data_to_country_daily_status(self):
        aggregate_real_time_data_to_school_daily_status(self.country, timezone.now().date())
        aggregate_school_daily_to_country_daily(self.country, timezone.now().date())
        self.assertEqual(CountryDailyStatus.objects.count(), 1)
        self.assertEqual(CountryDailyStatus.objects.first().connectivity_speed, 5000000)

    def test_aggregate_real_time_yesterday_data(self):
        yesterday = timezone.now() - timedelta(days=1)
        yesterday_status = SchoolDailyStatusFactory(school=self.school, date=yesterday.date(),
                                                    live_data_source='DAILY_CHECK_APP_MLAB')
        RealTimeConnectivityFactory(
            school=self.school, connectivity_speed=3000000, created=yesterday,
            live_data_source='DAILY_CHECK_APP_MLAB'
        )

        finalize_previous_day_data(None, self.country.id, yesterday.date())
        yesterday_status.refresh_from_db()

        self.assertEqual(yesterday_status.connectivity_speed, 3000000)
        self.assertEqual(self.country.daily_status.get(date=yesterday_status.date).connectivity_speed, 3000000)

    def test_aggregate_school_daily_to_country_daily(self):
        today = datetime.now().date()
        SchoolDailyStatusFactory(school__country=self.country, connectivity_speed=4000000, date=today,
                                 live_data_source='DAILY_CHECK_APP_MLAB')
        SchoolDailyStatusFactory(school__country=self.country, connectivity_speed=6000000, date=today,
                                 live_data_source='DAILY_CHECK_APP_MLAB')

        aggregate_school_daily_to_country_daily(self.country, timezone.now().date())
        self.assertEqual(CountryDailyStatus.objects.get(country=self.country, date=today).connectivity_speed, 5000000)

    def test_aggregate_country_daily_status_to_country_weekly_status(self):
        today = datetime.now().date()
        CountryDailyStatusFactory(country=self.country, date=today, live_data_source='DAILY_CHECK_APP_MLAB')
        SchoolWeeklyStatusFactory(
            school__country=self.country,
            connectivity=True, connectivity_speed=4000000,
            coverage_availability=True, coverage_type='unknown',
            year=get_current_year(), week=get_current_week(),
        )
        SchoolWeeklyStatusFactory(
            school__country=self.country, connectivity_speed=6000000, year=get_current_year(), week=get_current_week(),
        )

        country_weekly = CountryWeeklyStatus.objects.filter(country=self.country).last()
        if not country_weekly.is_verified:
            country_weekly.update_country_status_to_joined()

        update_country_weekly_status(self.country, today)
        self.assertEqual(CountryWeeklyStatus.objects.filter(country=self.country).count(), 1)
        self.assertEqual(CountryWeeklyStatus.objects.filter(country=self.country).last().connectivity_speed, 5000000)

        country_weekly = CountryWeeklyStatus.objects.filter(country=self.country).last()
        self.assertEqual(country_weekly.integration_status, CountryWeeklyStatus.REALTIME_MAPPED)
        self.assertEqual(country_weekly.connectivity_availability,
                         CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.realtime_speed)
        self.assertEqual(country_weekly.coverage_availability,
                         CountryWeeklyStatus.COVERAGE_TYPES_AVAILABILITY.coverage_availability)

    def test_aggregate_school_daily_status_to_school_weekly_status(self):
        date = datetime.now().date() - timedelta(days=6)
        monday_date = date - timedelta(days=date.weekday())
        tuesday_date = monday_date + timedelta(days=1)

        SchoolDailyStatusFactory(school=self.school, connectivity_speed=4000000, date=monday_date,
                                 live_data_source='DAILY_CHECK_APP_MLAB')
        SchoolDailyStatusFactory(school=self.school, connectivity_speed=6000000, date=tuesday_date,
                                 live_data_source='DAILY_CHECK_APP_MLAB')

        self.school.last_weekly_status = None
        self.school.save()
        aggregate_school_daily_status_to_school_weekly_status(self.country, tuesday_date)
        self.school.refresh_from_db()
        self.assertNotEqual(self.school.last_weekly_status, None)
        self.assertEqual(SchoolWeeklyStatus.objects.count(), 1)
        self.assertEqual(SchoolWeeklyStatus.objects.last().connectivity_speed, 5000000)
        self.assertEqual(SchoolWeeklyStatus.objects.last().connectivity, True)

    def test_aggregate_school_daily_status_to_school_weekly_status_connectivity_unknown(self):
        # daily status is too old, so it wouldn't be involved into country calculations
        today = datetime.now().date()
        SchoolDailyStatusFactory(school=self.school, connectivity_speed=None, date=today - timedelta(days=8),
                                 live_data_source='DAILY_CHECK_APP_MLAB')
        SchoolWeeklyStatusFactory(
            school=self.school, week=get_current_week(), year=get_current_year(), connectivity=None,
        )

        aggregate_school_daily_status_to_school_weekly_status(self.country, today)
        self.assertEqual(SchoolWeeklyStatus.objects.count(), 1)
        self.assertEqual(SchoolWeeklyStatus.objects.last().connectivity, None)

    def test_aggregate_school_daily_status_to_school_weekly_status_connectivity_no(self):
        today = datetime.now().date()
        SchoolDailyStatusFactory(school=self.school, connectivity_speed=None, date=today - timedelta(days=8),
                                 live_data_source='DAILY_CHECK_APP_MLAB')
        SchoolWeeklyStatusFactory(
            school=self.school, week=get_current_week(), year=get_current_year(), connectivity=False,
        )

        aggregate_school_daily_status_to_school_weekly_status(self.country, today)
        self.assertEqual(SchoolWeeklyStatus.objects.count(), 1)
        self.assertEqual(SchoolWeeklyStatus.objects.last().connectivity, False)
