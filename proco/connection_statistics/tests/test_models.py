from datetime import datetime, timedelta

from django.test import TestCase

from proco.connection_statistics.models import CountryWeeklyStatus
from proco.connection_statistics.tests.factories import (
    CountryDailyStatusFactory,
    CountryWeeklyStatusFactory,
    RealTimeConnectivityFactory,
    SchoolDailyStatusFactory,
    SchoolWeeklyStatusFactory,
)
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory


class TestCountryWeeklyStatusModel(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()
        cls.school = SchoolFactory(country=cls.country, location__country=cls.country)

        cls.country_weekly = CountryWeeklyStatusFactory(
            country=cls.country, integration_status=CountryWeeklyStatus.REALTIME_MAPPED, year=datetime.now().year + 1,
            schools_connectivity_no=1,
        )

    def test_country_weekly_status_str(self):
        country_weekly_print = (
            f'{self.country_weekly.year} {self.country_weekly.country.name} Week {self.country_weekly.week} '
            f'Speed - {self.country_weekly.connectivity_speed}{self.country_weekly.schools_connected} '
            f'{self.country_weekly.schools_total} {self.country_weekly.schools_connectivity_unknown} '
            f'{self.country_weekly.schools_connectivity_no}{self.country_weekly.schools_connectivity_moderate} '
            f'{self.country_weekly.schools_connectivity_good} {self.country_weekly.schools_coverage_unknown} '
            f'{self.country_weekly.schools_coverage_no} {self.country_weekly.schools_coverage_moderate}'
            f'{self.country_weekly.schools_coverage_good} {self.country_weekly.global_schools_connectivity_good}'
        )

        self.assertEqual(str(self.country_weekly), country_weekly_print)


class TestSchoolWeeklyStatusModel(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school = SchoolFactory(country=cls.country, location__country=cls.country)

        cls.school_weekly = SchoolWeeklyStatusFactory(
            school=cls.school,
            connectivity=True, connectivity_speed=3 * (10 ** 6),
            coverage_availability=True, coverage_type='3g',
        )

        cls.school.last_weekly_status = cls.school_weekly
        cls.school.save()

    def test_school_weekly_status_str(self):
        school_weekly_print = (f'{self.school_weekly.year} {self.school_weekly.school.name} '
                               f'Week {self.school_weekly.week} Speed - {self.school_weekly.connectivity_speed}')

        self.assertEqual(str(self.school_weekly), school_weekly_print)

    def test_get_coverage_status(self):
        no_coverage = self.school_weekly.get_coverage_status('no_coverage')
        coverage_availability = self.school_weekly.get_coverage_status('coverage_availability')
        coverage_type = self.school_weekly.get_coverage_status('coverage_type')

        self.assertIsNone(no_coverage)
        self.assertEqual(coverage_availability, 'good')
        self.assertEqual(coverage_type, 'good')


class TestCountryDailyStatusModel(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school = SchoolFactory(country=cls.country, location__country=cls.country)

        cls.country_daily = CountryDailyStatusFactory(country=cls.country)

    def test_country_daily_status_str(self):
        year, week, weekday = self.country_daily.date.isocalendar()
        country_daily_print = (f'{year} {self.country_daily.country.name} Week {week} Day {weekday} '
                               f'Speed - {self.country_daily.connectivity_speed}')

        self.assertEqual(str(self.country_daily), country_daily_print)


class TestSchoolDailyStatusModel(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school = SchoolFactory(country=cls.country, location__country=cls.country)

        cls.school_daily = SchoolDailyStatusFactory(school=cls.school,
                                                    date=datetime.now().date() - timedelta(days=1),
                                                    connectivity_speed=4000000)

    def test_school_daily_status_str(self):
        year, week, weekday = self.school_daily.date.isocalendar()
        school_daily_print = (f'{year} {self.school_daily.school.name} Week {week} Day {weekday} '
                              f'Speed - {self.school_daily.connectivity_speed}')

        self.assertEqual(str(self.school_daily), school_daily_print)


class TestRealTimeConnectivityModel(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school = SchoolFactory(country=cls.country, location__country=cls.country)

        cls.school_real_time_connectivity = RealTimeConnectivityFactory(school=cls.school, connectivity_speed=4000000)

    def test_school_daily_status_str(self):
        school_rtc_print = (f'{self.school_real_time_connectivity.created} '
                            f'{self.school_real_time_connectivity.school.name} '
                            f'Speed - {self.school_real_time_connectivity.connectivity_speed}')

        self.assertEqual(str(self.school_real_time_connectivity), school_rtc_print)
