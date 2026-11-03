from datetime import datetime, timedelta

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from proco.accounts import models as accounts_models
from proco.connection_statistics.models import (
    CountryDailyStatus,
    CountryWeeklyStatus,
    SchoolDailyStatus,
    SchoolRealTimeWeeklyMetric,
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
from proco.core.db_utils import Percentile
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


class AggregateDailySpeedP90TestCase(TestCase):
    databases = ['default']

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()
        cls.school = SchoolFactory(country=cls.country)
        cls.single_measurement_school = SchoolFactory(country=cls.country)
        cls.today_datetime = datetime.now(tz=timezone.utc).replace(hour=12)

        # 1..10 Mbps: mean and median 5.5 Mbps, P90 (linear interpolation) 9.1 Mbps
        for speed in range(1, 11):
            RealTimeConnectivityFactory(school=cls.school, connectivity_speed=speed * 1000000,
                                        created=cls.today_datetime, live_data_source='DAILY_CHECK_APP_MLAB')
        # another source of the same school is aggregated on its own; skewed so mean and median differ
        for speed in (2, 4, 12):
            RealTimeConnectivityFactory(school=cls.school, connectivity_speed=speed * 1000000,
                                        created=cls.today_datetime, live_data_source='QOS')
        # a measurement from the day before must not be counted
        RealTimeConnectivityFactory(school=cls.school, connectivity_speed=900000000,
                                    created=cls.today_datetime - timedelta(days=1),
                                    live_data_source='DAILY_CHECK_APP_MLAB')
        RealTimeConnectivityFactory(school=cls.single_measurement_school, connectivity_speed=3000000,
                                    created=cls.today_datetime, live_data_source='DAILY_CHECK_APP_MLAB')

    def test_p90_and_median_are_computed_from_raw_measurements_per_source(self):
        aggregate_real_time_data_to_school_daily_status(self.country, self.today_datetime.date())

        app_status = SchoolDailyStatus.objects.get(school=self.school, live_data_source='DAILY_CHECK_APP_MLAB')
        self.assertEqual(app_status.connectivity_speed, 5500000)
        self.assertEqual(app_status.connectivity_speed_p90, 9100000)
        self.assertEqual(app_status.connectivity_speed_median, 5500000)

        qos_status = SchoolDailyStatus.objects.get(school=self.school, live_data_source='QOS')
        self.assertEqual(qos_status.connectivity_speed, 6000000)
        self.assertEqual(qos_status.connectivity_speed_p90, 10400000)
        self.assertEqual(qos_status.connectivity_speed_median, 4000000)

    def test_p90_and_median_with_a_single_measurement_are_that_measurement(self):
        aggregate_real_time_data_to_school_daily_status(self.country, self.today_datetime.date())

        status = SchoolDailyStatus.objects.get(school=self.single_measurement_school)
        self.assertEqual(status.connectivity_speed_p90, 3000000)
        self.assertEqual(status.connectivity_speed_median, 3000000)

    def test_p90_and_median_are_empty_when_no_speed_was_measured(self):
        school = SchoolFactory(country=self.country)
        RealTimeConnectivityFactory(school=school, connectivity_speed=None, created=self.today_datetime,
                                    live_data_source='DAILY_CHECK_APP_MLAB')

        aggregate_real_time_data_to_school_daily_status(self.country, self.today_datetime.date())

        status = SchoolDailyStatus.objects.get(school=school)
        self.assertIsNone(status.connectivity_speed_p90)
        self.assertIsNone(status.connectivity_speed_median)

    def test_finalize_previous_day_data_overwrites_an_existing_daily_row(self):
        status = SchoolDailyStatusFactory(school=self.single_measurement_school, date=self.today_datetime.date(),
                                          live_data_source='DAILY_CHECK_APP_MLAB', connectivity_speed_p90=1,
                                          connectivity_speed_median=1)

        finalize_previous_day_data(None, self.country.id, self.today_datetime.date())
        status.refresh_from_db()

        self.assertEqual(status.connectivity_speed_p90, 3000000)
        self.assertEqual(status.connectivity_speed_median, 3000000)

    def test_percentile_outside_zero_and_one_is_rejected(self):
        with self.assertRaises(ValueError):
            Percentile('connectivity_speed', 90)

    def test_live_layer_on_daily_p90_gets_weekly_values(self):
        call_command('load_system_data_layers', '--update_data_sources')
        app_source = accounts_models.DataSource.objects.get(data_source_type='DAILY_CHECK_APP')
        p90_column = next(column for column in app_source.column_config if column['name'] == 'connectivity_speed_p90')
        avg_function = next(function for function in p90_column['supported_functions'] if function['name'] == 'avg')
        layer = accounts_models.DataLayer.objects.create(
            name='Download speed P90', type=accounts_models.DataLayer.LAYER_TYPE_LIVE,
            category=accounts_models.DataLayer.LAYER_CATEGORY_CONNECTIVITY,
            status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED, applicable_countries=[self.country.id],
        )
        accounts_models.DataLayerDataSourceRelationship.objects.create(
            data_layer=layer, data_source=app_source, data_source_column=p90_column,
            data_source_column_function=avg_function,
        )
        monday = self.today_datetime.date() - timedelta(days=self.today_datetime.weekday())
        SchoolDailyStatusFactory(school=self.school, date=monday, live_data_source='DAILY_CHECK_APP_MLAB',
                                 connectivity_speed_p90=8000000)
        SchoolDailyStatusFactory(school=self.school, date=monday + timedelta(days=1),
                                 live_data_source='DAILY_CHECK_APP_MLAB', connectivity_speed_p90=10000000)
        year, week_no, _ = monday.isocalendar()

        call_command('populate_school_live_weekly_metrics', '-layer_id={0}'.format(layer.id),
                     '-year={0}'.format(year), '-week_no={0}'.format(week_no))

        metric = SchoolRealTimeWeeklyMetric.objects.get(school=self.school, year=year, week=week_no)
        self.assertEqual(metric.metric_name, 'connectivity_speed_p90')
        self.assertEqual(metric.agg_value, 9000000)
        self.assertEqual(metric.sample_count, 2)
