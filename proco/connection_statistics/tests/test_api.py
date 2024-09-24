import random
from datetime import datetime, timedelta

from django.core.cache import cache
from django.core.management import call_command
from django.test import TestCase
from django.urls import resolve, reverse
from isoweek import Week
from rest_framework import exceptions as rest_exceptions
from rest_framework import status

from proco.accounts import models as accounts_models
from proco.connection_statistics.models import CountryWeeklyStatus
from proco.connection_statistics.tests.factories import (
    CountryDailyStatusFactory,
    CountryWeeklyStatusFactory,
    SchoolDailyStatusFactory,
    SchoolWeeklyStatusFactory,
)
from proco.custom_auth.tests import test_utils as test_utilities
from proco.locations.tests.factories import CountryFactory, Admin1Factory
from proco.schools.tests.factories import SchoolFactory
from proco.utils.dates import format_date, get_first_date_of_month, get_last_date_of_month
from proco.utils.tests import TestAPIViewSetMixin


def statistics_url(url_params, query_param, view_name='global-stat'):
    url = reverse('connection_statistics:' + view_name, args=url_params)
    view = resolve(url)
    view_info = view.func

    if len(query_param) > 0:
        query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
        url += query_params
    return url, view, view_info


def accounts_url(url_params, query_param, view_name='list-or-create-api-keys'):
    url = reverse('accounts:' + view_name, args=url_params)
    view = resolve(url)
    view_info = view.func

    if len(query_param) > 0:
        query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
        url += query_params
    return url, view, view_info


class GlobalStatisticsApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()

        cls.school_one = SchoolFactory(country=cls.country_one, location__country=cls.country_one, geopoint=None)
        cls.school_two = SchoolFactory(country=cls.country_one, location__country=cls.country_one)

        SchoolWeeklyStatusFactory(school=cls.school_one, connectivity=True)
        SchoolWeeklyStatusFactory(school=cls.school_two, connectivity=False)
        CountryWeeklyStatusFactory(country=cls.country_one, integration_status=CountryWeeklyStatus.REALTIME_MAPPED,
                                   year=datetime.now().year + 1, schools_connectivity_no=1)

        cls.cws = CountryWeeklyStatusFactory(integration_status=CountryWeeklyStatus.STATIC_MAPPED,
                                             schools_connectivity_no=0,
                                             year=datetime.now().year + 2)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_global_stats(self):
        url, _, view = statistics_url((), {})

        response = self.forced_auth_req(
            'get',
            url,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['no_of_countries'], 1)
        self.assertEqual(response.data['schools_connected'], 2)
        self.assertEqual(list(response.data['connected_schools'].keys()),
                         ['connected', 'not_connected', 'unknown'])

    def test_global_stats_queries(self):
        url, _, view = statistics_url((), {})

        with self.assertNumQueries(2):
            self.forced_auth_req(
                'get',
                url,
            )


class CountryDailyStatsApiTestCase(TestAPIViewSetMixin, TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()

        cls.country_one_stats_number = random.SystemRandom().randint(a=5, b=25)
        for _i in range(cls.country_one_stats_number):
            CountryDailyStatusFactory(country=cls.country_one)

        CountryDailyStatusFactory(country=cls.country_two)

    def test_country_daily_stats(self):
        url, _, view = statistics_url((self.country_one.code.lower(),), {}, view_name='country-daily-stat')
        response = self.forced_auth_req('get', url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], self.country_one_stats_number)

        url, _, view = statistics_url((self.country_two.code.lower(),), {}, view_name='country-daily-stat')
        response = self.forced_auth_req('get', url)

        self.assertEqual(response.data['count'], 1)

    def test_country_daily_stats_queries(self):
        url, _, view = statistics_url((self.country_one.code.lower(),), {}, view_name='country-daily-stat')
        with self.assertNumQueries(2):
            self.forced_auth_req('get', url)


class SchoolDailyStatsApiTestCase(TestAPIViewSetMixin, TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school_one = SchoolFactory()
        cls.school_two = SchoolFactory()

        cls.school_one_stats_number = random.SystemRandom().randint(a=5, b=25)
        for _i in range(cls.school_one_stats_number):
            SchoolDailyStatusFactory(school=cls.school_one)

        SchoolDailyStatusFactory(school=cls.school_two)

    def test_school_daily_stats(self):
        url, _, view = statistics_url((self.school_one.id,), {}, view_name='school-daily-stat')
        response = self.forced_auth_req('get', url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], self.school_one_stats_number)

        url, _, view = statistics_url((self.school_two.id,), {}, view_name='school-daily-stat')
        response = self.forced_auth_req('get', url)

        self.assertEqual(response.data['count'], 1)

    def test_school_daily_stats_queries(self):
        url, _, view = statistics_url((self.school_one.id,), {}, view_name='school-daily-stat')
        with self.assertNumQueries(1):
            self.forced_auth_req('get', url)


class SchoolCoverageStatApiTestCase(TestAPIViewSetMixin, TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school_one = SchoolFactory(country=cls.country, location__country=cls.country)
        cls.school_two = SchoolFactory(country=cls.country, location__country=cls.country)
        cls.school_three = SchoolFactory(country=cls.country, location__country=cls.country)

        cls.school_weekly_one = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=True, connectivity_speed=3 * (10 ** 6),
            coverage_availability=True, coverage_type='3g',
        )
        cls.school_weekly_two = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=False, connectivity_speed=None,
            coverage_availability=False, coverage_type='no',
        )
        cls.school_weekly_three = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=None, connectivity_speed=None,
            coverage_availability=None, coverage_type='unknown',
        )

        cls.school_one.last_weekly_status = cls.school_weekly_one
        cls.school_one.save()
        cls.school_two.last_weekly_status = cls.school_weekly_two
        cls.school_two.save()
        cls.school_three.last_weekly_status = cls.school_weekly_three
        cls.school_three.save()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_school_coverage_stat_school_list(self):
        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        }, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

    def test_school_coverage_stat_without_school_id(self):
        url, _, view = statistics_url((), {'country_id': self.country.id}, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # TODO: Change it once that hard coded school id is removed
        self.assertEqual(len(response.data), 0)

    def test_school_coverage_stat_without_country_id(self):
        url, _, view = statistics_url((), {}, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_school_coverage_stat_for_one_school(self):
        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': str(self.school_one.id),
        }, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        school_data = response.data[0]
        self.assertIn('coverage_type', school_data)
        self.assertIn('name', school_data)
        self.assertIn('country_name', school_data)

        self.assertEqual(school_data['statistics']['coverage_type'], '3g')
        self.assertEqual(school_data['name'], self.school_one.name)
        self.assertEqual(school_data['country_name'], self.country.name)

    def test_school_coverage_stat_for_one_school_statistics(self):
        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': str(self.school_one.id),
        }, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        school_data = response.data[0]
        self.assertIn('statistics', school_data)

        school_statistics_data = school_data['statistics']

        self.assertEqual(school_statistics_data['coverage_type'], '3g')
        self.assertEqual(school_statistics_data['connectivity_speed'], round(3 * (10 ** 6) / 1000000, 2))

    def test_school_coverage_stat_for_coverage_type_choices(self):
        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        }, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertIn('coverage_type', response.data[0])
        self.assertIn('coverage_type', response.data[1])
        self.assertIn('coverage_type', response.data[2])
        self.assertEqual(response.data[0]['statistics']['coverage_type'], '3g')
        self.assertEqual(response.data[1]['statistics']['coverage_type'], 'no')
        self.assertEqual(response.data[2]['statistics']['coverage_type'], 'unknown')

    def test_school_coverage_stat_for_one_school_when_school_weekly_status_not_available(self):
        """
        test_school_coverage_stat_for_one_school_when_school_weekly_status_not_available
            Positive test case to test the coverage_type field for weekly data for 1 school when
            School Weekly Status records are not available.

        Expected: HTTP_200_OK - 1 school data with empty statistics json.
        coverage_type == unknown
        """
        school_four = SchoolFactory(country=self.country, location__country=self.country)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': school_four.id,
        }, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        school_data = response.data[0]
        self.assertIn('statistics', school_data)
        self.assertEqual(len(school_data['statistics']), 0)

        self.assertEqual(school_data['coverage_type'], 'unknown')

    def test_school_coverage_stat_for_connectivity_status(self):
        connectivity_availability = CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.connectivity
        self.country.last_weekly_status.connectivity_availability = connectivity_availability
        self.country.last_weekly_status.save()

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        }, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[1]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[2]['statistics']['connectivity_status'], 'unknown')

    def test_school_coverage_stat_for_connectivity_status_when_connectivity_availability_static_speed(self):
        connectivity_availability = CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.static_speed
        self.country.last_weekly_status.connectivity_availability = connectivity_availability
        self.country.last_weekly_status.save()

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        }, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[1]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[2]['statistics']['connectivity_status'], 'unknown')

    def test_school_coverage_stat_for_connectivity_status_when_country_weekly_status_not_available(self):
        self.country.last_weekly_status = None
        self.country.save()

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        }, view_name='school-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertEqual(response.data[0]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[1]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[2]['statistics']['connectivity_status'], 'unknown')


class ConnectivityStatApiTestCase(TestAPIViewSetMixin, TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()
        cls.country_two = CountryFactory()

        cls.admin1_one = Admin1Factory(country=cls.country, layer_name='adm1')

        cls.school_one = SchoolFactory(country=cls.country, location__country=cls.country, admin1=cls.admin1_one)
        cls.school_two = SchoolFactory(country=cls.country, location__country=cls.country, admin1=cls.admin1_one)
        cls.school_three = SchoolFactory(country=cls.country, location__country=cls.country, admin1=cls.admin1_one)

        cls.school_weekly_one = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=True, connectivity_speed=3 * (10 ** 6),
            coverage_availability=True, coverage_type='3g',
        )
        cls.school_weekly_two = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=False, connectivity_speed=None,
            coverage_availability=False, coverage_type='no',
        )
        cls.school_weekly_three = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=None, connectivity_speed=None,
            coverage_availability=None, coverage_type='unknown',
        )
        cls.school_one.last_weekly_status = cls.school_weekly_one
        cls.school_one.save()
        cls.school_two.last_weekly_status = cls.school_weekly_two
        cls.school_two.save()
        cls.school_three.last_weekly_status = cls.school_weekly_three
        cls.school_three.save()

        cls.school_daily_two = SchoolDailyStatusFactory(school=cls.school_two,
                                                        date=datetime.now().date() - timedelta(days=1),
                                                        connectivity_speed=4000000)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_country_download_connectivity_stat(self):
        """
        test_country_download_connectivity_stat
            Positive test case for weekly data.

        Expected: HTTP_200_OK - List of data for all 3 schools
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='country-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertIn('live_avg', response_data)
        self.assertIn('no_of_schools_measure', response_data)
        self.assertIn('school_with_realtime_data', response_data)
        self.assertIn('is_data_synced', response_data)
        self.assertIn('graph_data', response_data)
        self.assertIn('real_time_connected_schools', response_data)

    def test_admin1_download_connectivity_stat_monthly(self):
        """
        test_admin1_download_connectivity_stat_monthly
            Positive test case for monthly data.

        Expected: HTTP_200_OK - List of data for all 3 schools
        """
        today = datetime.now().date()
        start_date = get_first_date_of_month(today.year, today.month)
        end_date = get_last_date_of_month(today.year, today.month)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'admin1_id': self.admin1_one.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'false',
        }, view_name='global-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertIn('live_avg', response_data)
        self.assertIn('no_of_schools_measure', response_data)
        self.assertIn('school_with_realtime_data', response_data)
        self.assertIn('is_data_synced', response_data)
        self.assertIn('graph_data', response_data)
        self.assertIn('real_time_connected_schools', response_data)

    def test_country_download_connectivity_stat_without_country_id(self):
        """
        test_school_download_connectivity_stat_without_school_id
            Negative test case for weekly data without passing the school id in url query parameters.

        Expected: HTTP_200_OK - As of now it will return no data as we have hard coded the school id as 34554
        in API View. But with changes in API it will return the list of schools from the country.
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='country-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertIn('live_avg', response_data)
        self.assertIn('no_of_schools_measure', response_data)
        self.assertIn('school_with_realtime_data', response_data)
        self.assertIn('is_data_synced', response_data)
        self.assertIn('graph_data', response_data)
        self.assertIn('real_time_connected_schools', response_data)

    def test_country_download_connectivity_stat_for_one_country_without_daily(self):
        """
        test_school_download_connectivity_stat_for_one_school_without_daily
            Positive test case for weekly data for 1 school and School Daily records are also not available.

        Expected: HTTP_200_OK - 1 school data with graph_data json with value as null.
        Connectivity_speed == 0, as for download speed is calculated based on graph data aggregation.
        connectivity_status == unknown, as country.last_weekly_status.connectivity_availability == no_connectivity
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country_two.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='country-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['no_of_schools_measure'], 0)

    def test_country_download_connectivity_stat_for_one_country_statistics(self):
        """
        test_school_download_connectivity_stat_for_one_school_statistics
            Positive test case to test the statistics JSON for weekly data for 1 school and
            School Daily records are also not available.

        Expected: HTTP_200_OK - 1 school data with filled statistics json and graph_data json with value as null.
        Connectivity_speed == 0, as for download speed is calculated based on graph data aggregation.
        connectivity_status == unknown, as country.last_weekly_status.connectivity_availability == no_connectivity

        statistics.connectivity_speed == round(3 * (10 ** 6) / 1000000, 2), as speed is picked from SchoolWeeklyStatus
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='country-connectivity-stat')

        with self.assertNumQueries(6):
            response = self.forced_auth_req('get', url, view=view)

            self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_country_download_connectivity_stat_for_one_school_graph_data_when_school_daily_status_not_available(self):
        """
        test_school_download_connectivity_stat_for_one_school_graph_data_when_school_daily_status_not_available
            Positive test case to test the graph_data JSON for weekly data for 1 school when
            School Daily records are not available.

        Expected: HTTP_200_OK - 1 school data with filled statistics json and filled graph_data json with null values.
        Connectivity_speed == 0, as for download speed is calculated based on graph data aggregation.
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='country-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertIn('graph_data', response_data)
        self.assertEqual(len(response_data['graph_data']), 7)

        graph_data = response_data['graph_data']
        for data in graph_data:
            self.assertIsNone(data['value'])

    def test_country_download_connectivity_stat_for_global_benchmark(self):
        """
        test_country_download_connectivity_stat
            Positive test case for country weekly data.

        Expected: HTTP_200_OK - List of data for given country id
        """
        date = Week(self.school_weekly_one.year, self.school_weekly_one.week).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'global'
        }, view_name='country-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)

        self.assertIn('live_avg', response_data)
        self.assertIn('school_with_realtime_data', response_data)
        self.assertIn('is_data_synced', response_data)
        self.assertIn('graph_data', response_data)
        self.assertIn('real_time_connected_schools', response_data)

    def test_country_download_connectivity_stat_for_invalid_date_range(self):
        date = Week(2023, 56).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'global'
        }, view_name='country-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_country_download_connectivity_stat_for_missing_country_id(self):
        date = Week(self.school_weekly_one.year, self.school_weekly_one.week).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, _, view = statistics_url((), {
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'global'
        }, view_name='country-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_country_download_connectivity_stat_for_national_benchmark(self):
        date = Week(self.school_weekly_one.year, self.school_weekly_one.week).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'national',
        }, view_name='country-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)

        self.assertIn('live_avg', response_data)
        self.assertIn('school_with_realtime_data', response_data)
        self.assertIn('is_data_synced', response_data)
        self.assertIn('graph_data', response_data)
        self.assertIn('real_time_connected_schools', response_data)


class SchoolConnectivityStatApiTestCase(TestAPIViewSetMixin, TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school_one = SchoolFactory(country=cls.country, location__country=cls.country)
        cls.school_two = SchoolFactory(country=cls.country, location__country=cls.country)
        cls.school_three = SchoolFactory(country=cls.country, location__country=cls.country)

        cls.school_weekly_one = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=True, connectivity_speed=3 * (10 ** 6),
            coverage_availability=True, coverage_type='3g',
        )
        cls.school_weekly_two = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=False, connectivity_speed=None,
            coverage_availability=False, coverage_type='no',
        )
        cls.school_weekly_three = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=None, connectivity_speed=None,
            coverage_availability=None, coverage_type='unknown',
        )
        cls.school_one.last_weekly_status = cls.school_weekly_one
        cls.school_one.save()
        cls.school_two.last_weekly_status = cls.school_weekly_two
        cls.school_two.save()
        cls.school_three.last_weekly_status = cls.school_weekly_three
        cls.school_three.save()

        cls.school_daily_two = SchoolDailyStatusFactory(school=cls.school_two,
                                                        date=datetime.now().date() - timedelta(days=1),
                                                        connectivity_speed=4000000)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_school_download_connectivity_stat_school_list(self):
        """
        test_school_download_connectivity_stat_school_list
            Positive test case for weekly data.

        Expected: HTTP_200_OK - List of data for all 3 schools
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

    def test_school_download_connectivity_stat_school_list_for_month(self):
        """
        test_school_download_connectivity_stat_school_list
            Positive test case for weekly data.

        Expected: HTTP_200_OK - List of data for all 3 schools
        """
        today = datetime.now().date()
        start_date = get_first_date_of_month(today.year, today.month)
        end_date = get_last_date_of_month(today.year, today.month)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'false',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

    def test_school_download_connectivity_stat_without_school_id(self):
        """
        test_school_download_connectivity_stat_without_school_id
            Negative test case for weekly data without passing the school id in url query parameters.

        Expected: HTTP_200_OK - As of now it will return no data as we have hard coded the school id as 34554
        in API View. But with changes in API it will return the list of schools from the country.
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # TODO: Change it once that hard coded school id is removed
        self.assertEqual(len(response.data), 0)

    def test_school_download_connectivity_stat_without_country_id(self):
        """
        test_school_download_connectivity_stat_without_country_id
            Negative test case for weekly data without passing the country id in url query parameters.

        Expected: HTTP_404_NOT_FOUND - Country ID is a mandatory field.
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(response.data.get('detail').code, rest_exceptions.NotFound.default_code)

    def test_school_download_connectivity_stat_for_one_school_without_daily(self):
        """
        test_school_download_connectivity_stat_for_one_school_without_daily
            Positive test case for weekly data for 1 school and School Daily records are also not available.

        Expected: HTTP_200_OK - 1 school data with graph_data json with value as null.
        Connectivity_speed == 0, as for download speed is calculated based on graph data aggregation.
        connectivity_status == unknown, as country.last_weekly_status.connectivity_availability == no_connectivity
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': self.school_one.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        school_data = response.data[0]

        self.assertIn('name', school_data)
        self.assertIn('connectivity_speed', school_data['statistics'])
        self.assertIn('connectivity_status', school_data['statistics'])

        self.assertEqual(school_data['name'], self.school_one.name)
        # INFO: As we have not created any School daily data, it will return 0 for download
        self.assertIsNone(school_data['statistics']['connectivity_speed'])
        self.assertEqual(school_data['statistics']['connectivity_status'], 'unknown')

        self.assertEqual(len(school_data['graph_data']), 7)

    def test_school_download_connectivity_stat_for_one_school_statistics(self):
        """
        test_school_download_connectivity_stat_for_one_school_statistics
            Positive test case to test the statistics JSON for weekly data for 1 school and
            School Daily records are also not available.

        Expected: HTTP_200_OK - 1 school data with filled statistics json and graph_data json with value as null.
        Connectivity_speed == 0, as for download speed is calculated based on graph data aggregation.
        connectivity_status == unknown, as country.last_weekly_status.connectivity_availability == no_connectivity

        statistics.connectivity_speed == round(3 * (10 ** 6) / 1000000, 2), as speed is picked from SchoolWeeklyStatus
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': self.school_one.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        school_data = response.data[0]
        self.assertIn('statistics', school_data)
        self.assertIsNone(school_data['statistics']['connectivity_speed'])

        self.assertEqual(school_data['statistics']['connectivity_status'], 'unknown')
        self.assertIsNone(school_data['statistics']['connectivity_speed'])

    def test_school_download_connectivity_stat_for_connectivity_status_choices(self):
        """
        test_school_download_connectivity_stat_for_connectivity_status_choices
            Positive test case to test the connectivity_status field for weekly data for 3 schools and
            School Daily records only present for school_two.

        Expected: HTTP_200_OK - 3 school data with filled statistics json and graph_data json.
        1. school_one.connectivity_status == connected, as country.last_weekly_status.connectivity_availability ==
        connectivity and school_one.last_weekly_status.connectivity=True

        2. school_two.connectivity_status == not_connected, as country.last_weekly_status.connectivity_availability ==
        connectivity and school_one.last_weekly_status.connectivity=False

        3. school_three.connectivity_status == unknown, as country.last_weekly_status.connectivity_availability ==
        connectivity and school_one.last_weekly_status.connectivity=None
        """

        connectivity_availability = CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.connectivity
        self.country.last_weekly_status.connectivity_availability = connectivity_availability
        self.country.last_weekly_status.save()

        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertIn('connectivity_status', response.data[0])
        self.assertIn('connectivity_status', response.data[1])
        self.assertIn('connectivity_status', response.data[2])

        # self.assertEqual(response.data[0]['connectivity_status'], 'connected')
        # self.assertEqual(response.data[1]['connectivity_status'], 'not_connected')
        # self.assertEqual(response.data[2]['connectivity_status'], 'unknown')

    def test_school_download_connectivity_stat_for_one_school_graph_data_when_school_daily_status_not_available(self):
        """
        test_school_download_connectivity_stat_for_one_school_graph_data_when_school_daily_status_not_available
            Positive test case to test the graph_data JSON for weekly data for 1 school when
            School Daily records are not available.

        Expected: HTTP_200_OK - 1 school data with filled statistics json and filled graph_data json with null values.
        Connectivity_speed == 0, as for download speed is calculated based on graph data aggregation.
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': self.school_one.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        school_data = response.data[0]
        self.assertIn('graph_data', school_data)
        self.assertEqual(len(school_data['graph_data']), 7)
        self.assertIsNone(school_data['statistics']['connectivity_speed'])

        graph_data = school_data['graph_data']
        for data in graph_data:
            self.assertIsNone(data['value'])

    def test_school_download_connectivity_stat_for_one_school_graph_data_when_school_daily_status_available(self):
        """
        test_school_download_connectivity_stat_for_one_school_graph_data_when_school_daily_status_available
            Positive test case to test the graph_data JSON for weekly data for 1 school when
            School Daily records are available.

        Expected: HTTP_200_OK - 1 school data with filled statistics json and filled graph_data json.
        Connectivity_speed == 4, as for download speed is calculated based on graph data aggregation.
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': self.school_two.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        school_data = response.data[0]
        self.assertIn('graph_data', school_data)
        self.assertEqual(len(school_data['graph_data']), 7)
        self.assertEqual(school_data['statistics']['connectivity_speed'], 4)

        graph_data = school_data['graph_data']
        for data in graph_data:
            if data['key'] == format_date((today - timedelta(days=1))):
                self.assertEqual(data['value'], 4)
            else:
                self.assertIsNone(data['value'])

    def test_school_download_connectivity_stat_for_one_school_when_school_weekly_status_not_available(self):
        """
        test_school_download_connectivity_stat_for_one_school_when_school_weekly_status_not_available
            Positive test case to test the connectivity_status field for weekly data for 1 school when
            School Weekly Status records are not available.

        Expected: HTTP_200_OK - 1 school data with empty statistics json and filled graph_data json.
        Connectivity_speed == 0, as for download speed is calculated based on graph data aggregation.
        """
        school_four = SchoolFactory(country=self.country, location__country=self.country)

        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': school_four.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        school_data = response.data[0]
        self.assertIn('graph_data', school_data)
        self.assertEqual(len(school_data['graph_data']), 7)
        # self.assertEqual(school_data['statistics']['connectivity_speed'], 0)
        self.assertEqual(school_data['connectivity_status'], 'unknown')

        self.assertEqual(len(school_data['statistics']), 0)

        for data in school_data['graph_data']:
            self.assertIsNone(data['value'])

    def test_school_latency_connectivity_stat_school_list(self):
        """
        test_school_latency_connectivity_stat_school_list
            Positive test case for weekly data for latency.

        Expected: HTTP_200_OK - List of data for all 3 schools
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, _, view = statistics_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        }, view_name='school-connectivity-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

        # self.assertEqual(response.data[0]['statistics']['connectivity_speed'], 0)
        # self.assertEqual(response.data[1]['statistics']['connectivity_speed'], 0)
        # self.assertEqual(response.data[2]['statistics']['connectivity_speed'], 0)


class CountryCoverageStatsAPITestCase(TestAPIViewSetMixin, TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()

        cls.stat_one = CountryWeeklyStatusFactory(country=cls.country_one)
        cls.stat_two = CountryWeeklyStatusFactory(country=cls.country_two)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_get_country_coverage_stats(self):
        url, _, view = statistics_url((), {'country_id': self.country_one.id}, view_name='country-coverage-stat')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['total_schools'], self.stat_one.schools_total)
        # self.assertEqual(response.data['connected_schools']['5g_4g'], self.stat_one.schools_coverage_good)
        # self.assertEqual(response.data['connected_schools']['3g_2g'], self.stat_one.schools_coverage_moderate)
        # self.assertEqual(response.data['connected_schools']['no_coverage'], self.stat_one.schools_coverage_no)
        # self.assertEqual(response.data['connected_schools']['unknown'], self.stat_one.schools_coverage_unknown)

    def test_get_country_coverage_stats_no_data(self):
        url, _, view = statistics_url((), {'country_id': 999}, view_name='country-coverage-stat')
        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_get_country_coverage_stats_cached(self):
        url, _, view = statistics_url((), {'country_id': self.country_one.id}, view_name='country-coverage-stat')

        # Call the API to cache the data
        with self.assertNumQueries(4):
            self.forced_auth_req('get', url, view=view)

        with self.assertNumQueries(0):
            self.forced_auth_req('get', url, view=view)

    def test_get_country_coverage_stats_no_cache(self):
        url = reverse('connection_statistics:country-coverage-stat')
        query_params = {'country_id': self.country_one.id}
        # Call the API without caching
        with self.assertNumQueries(5):
            response = self.client.get(url, query_params, HTTP_CACHE_CONTROL='no-cache')
            self.assertEqual(response.status_code, status.HTTP_200_OK)


class ConnectivityConfigurationsAPITestCase(TestAPIViewSetMixin, TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()

        cls.admin1_one = Admin1Factory(country=cls.country_one, layer_name='adm1')

        cls.school_one = SchoolFactory(country=cls.country_one, admin1=cls.admin1_one)
        cls.school_two = SchoolFactory(country=cls.country_one)
        cls.school_three = SchoolFactory(country=cls.country_one)

        cls.stat_one = SchoolDailyStatusFactory(school=cls.school_one, live_data_source='DAILY_CHECK_APP_MLAB')
        cls.stat_two = SchoolDailyStatusFactory(school=cls.school_two, live_data_source='QOS')

        args = ['--delete_data_sources', '--update_data_sources', '--update_data_layers']
        call_command('load_system_data_layers', *args)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_global_latest_configurations(self):
        url, _, view = statistics_url((), {}, view_name='get-latest-week-and-month')

        with self.assertNumQueries(2):
            response = self.forced_auth_req('get', url, view=view)
            self.assertEqual(response.status_code, status.HTTP_200_OK)

            response_data = response.data
            self.assertIn('week', response_data)
            self.assertIn('month', response_data)
            self.assertIn('years', response_data)

        with self.assertNumQueries(0):
            self.forced_auth_req('get', url, view=view)

    def test_country_with_schools_latest_configurations(self):
        url, _, view = statistics_url((), {'country_id': self.country_one.id}, view_name='get-latest-week-and-month')

        with self.assertNumQueries(2):
            response = self.forced_auth_req('get', url, view=view)
            self.assertEqual(response.status_code, status.HTTP_200_OK)

            response_data = response.data
            self.assertIn('week', response_data)
            self.assertIn('month', response_data)
            self.assertIn('years', response_data)

        with self.assertNumQueries(0):
            self.forced_auth_req('get', url, view=view)

    def test_country_with_schools_latest_configurations_for_live_layer(self):
        layer = accounts_models.DataLayer.objects.filter(
            type=accounts_models.DataLayer.LAYER_TYPE_LIVE,
            category=accounts_models.DataLayer.LAYER_CATEGORY_CONNECTIVITY,
            status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
            created_by__isnull=True,
        ).first()
        url, _, view = statistics_url((), {'country_id': self.country_one.id, 'layer_id': layer.id},
                                      view_name='get-latest-week-and-month')

        with self.assertNumQueries(6):
            response = self.forced_auth_req('get', url, view=view)
            self.assertEqual(response.status_code, status.HTTP_200_OK)

            response_data = response.data
            self.assertIn('week', response_data)
            self.assertIn('month', response_data)
            self.assertIn('years', response_data)

    def test_country_without_schools_latest_configurations(self):
        url, _, view = statistics_url((), {'country_id': self.country_two.id}, view_name='get-latest-week-and-month')

        with self.assertNumQueries(1):
            response = self.forced_auth_req('get', url, view=view)
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(len(response.data), 0)

        with self.assertNumQueries(1):
            self.forced_auth_req('get', url, view=view)

    def test_admin1_latest_configurations(self):
        url, _, view = statistics_url((), {'admin1_id': self.admin1_one.id}, view_name='get-latest-week-and-month')

        with self.assertNumQueries(2):
            response = self.forced_auth_req('get', url, view=view)
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            response_data = response.data
            self.assertIn('week', response_data)
            self.assertIn('month', response_data)
            self.assertIn('years', response_data)

        with self.assertNumQueries(0):
            self.forced_auth_req('get', url, view=view)

    def test_school_latest_configurations(self):
        url, _, view = statistics_url((), {'school_id': self.school_one.id}, view_name='get-latest-week-and-month')

        with self.assertNumQueries(2):
            response = self.forced_auth_req('get', url, view=view)
            self.assertEqual(response.status_code, status.HTTP_200_OK)

            response_data = response.data
            self.assertIn('week', response_data)
            self.assertIn('month', response_data)
            self.assertIn('years', response_data)

        with self.assertNumQueries(0):
            self.forced_auth_req('get', url, view=view)

    def test_schools_latest_configurations(self):
        url, _, view = statistics_url((), {
            'school_ids': ','.join([str(s) for s in [self.school_one.id, self.school_two.id]])
        }, view_name='get-latest-week-and-month')

        with self.assertNumQueries(2):
            response = self.forced_auth_req('get', url, view=view)
            self.assertEqual(response.status_code, status.HTTP_200_OK)

            response_data = response.data
            self.assertIn('week', response_data)
            self.assertIn('month', response_data)
            self.assertIn('years', response_data)

        with self.assertNumQueries(0):
            self.forced_auth_req('get', url, view=view)


class CountrySummaryAPIViewSetAPITestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()

        cls.stat_one = CountryWeeklyStatusFactory(
            country=cls.country_one,
            integration_status=CountryWeeklyStatus.REALTIME_MAPPED,
            year=datetime.now().year - 1,
            week=12,
            schools_connectivity_no=1
        )
        cls.stat_two = CountryWeeklyStatusFactory(
            country=cls.country_one,
            integration_status=CountryWeeklyStatus.REALTIME_MAPPED,
            year=datetime.now().year - 1,
            week=13,
            schools_connectivity_no=1
        )

        cls.stat_three = CountryWeeklyStatusFactory(
            country=cls.country_two,
            integration_status=CountryWeeklyStatus.REALTIME_MAPPED,
            year=datetime.now().year - 1,
            week=12,
            schools_connectivity_no=1
        )

        cls.user = test_utilities.setup_admin_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-countryweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 3 records as we created manually in setup, 2 for each country with latest year and latest week
        self.assertEqual(response_data['count'], 5)
        self.assertEqual(len(response_data['results']), 5)

    def test_country_id_filter(self):
        url, _, view = statistics_url((), {'country_id': self.country_one.id},
                                      view_name='list-create-destroy-countryweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup, 1 for country with latest year and latest week
        self.assertEqual(response_data['count'], 3)
        self.assertEqual(len(response_data['results']), 3)

    def test_year_week_filter(self):
        url, _, view = statistics_url((), {'year': datetime.now().year - 1, 'week': 12},
                                      view_name='list-create-destroy-countryweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_search(self):
        url, _, view = statistics_url((), {'search': self.country_one.name},
                                      view_name='list-create-destroy-countryweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup, 1 for country with latest year and latest week
        self.assertEqual(response_data['count'], 3)
        self.assertEqual(len(response_data['results']), 3)

    def test_retrieve(self):
        url, view, view_info = statistics_url((self.stat_one.id,), {},
                                              view_name='update-retrieve-countryweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(response_data['id'], self.stat_one.id)
        self.assertEqual(response_data['connectivity_speed'], self.stat_one.connectivity_speed)
        self.assertEqual(response_data['year'], self.stat_one.year)
        self.assertEqual(response_data['week'], self.stat_one.week)
        self.assertEqual(response_data['integration_status'], self.stat_one.integration_status)

    def test_retrieve_wrong_id(self):
        url, view, view_info = statistics_url((1234546,), {},
                                              view_name='update-retrieve-countryweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update(self):
        url, _, view = statistics_url((self.stat_two.id,), {},
                                      view_name='update-retrieve-countryweeklystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": self.stat_two.connectivity_speed,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "year": self.stat_two.year,
                "week": self.stat_two.week,
                "date": self.stat_two.date,
                "integration_status": CountryWeeklyStatus.STATIC_MAPPED,
                "country": self.stat_two.country.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_update_wrong_id(self):
        url, _, view = statistics_url((123434567,), {},
                                      view_name='update-retrieve-countryweeklystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": self.stat_two.connectivity_speed,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "year": self.stat_two.year,
                "week": self.stat_two.week,
                "date": self.stat_two.date,
                "integration_status": CountryWeeklyStatus.STATIC_MAPPED,
                "country": self.stat_two.country.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update_invalid_data(self):
        url, _, view = statistics_url((self.stat_two.id,), {},
                                      view_name='update-retrieve-countryweeklystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": self.stat_two.connectivity_speed,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "year": self.stat_two.year,
                "week": self.stat_two.week,
                "date": self.stat_two.date,
                "integration_status": 8,
                "country": self.stat_two.country.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_delete(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-countryweeklystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [self.stat_two.id]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_delete_without_ids(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-countryweeklystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_delete_wrong_ids(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-countryweeklystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [12345432]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)


class CountryDailyConnectivitySummaryAPIViewSetAPITestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()

        today = datetime.now().date()

        cls.stat_one = CountryDailyStatusFactory(
            country=cls.country_one,
            date=today,
            live_data_source='DAILY_CHECK_APP_MLAB'
        )
        cls.stat_two = CountryDailyStatusFactory(
            country=cls.country_one,
            date=today,
            live_data_source='QOS'
        )

        cls.stat_three = CountryDailyStatusFactory(
            country=cls.country_two,
            date=today,
            live_data_source='DAILY_CHECK_APP_MLAB'
        )

        cls.user = test_utilities.setup_admin_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-countrydailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 3 records as we created manually in setup
        self.assertEqual(response_data['count'], 3)
        self.assertEqual(len(response_data['results']), 3)

    def test_country_id_filter(self):
        url, _, view = statistics_url((), {'country_id': self.country_one.id},
                                      view_name='list-create-destroy-countrydailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_search(self):
        url, _, view = statistics_url((), {'search': self.country_one.name},
                                      view_name='list-create-destroy-countrydailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_retrieve(self):
        url, view, view_info = statistics_url((self.stat_one.id,), {},
                                              view_name='update-retrieve-countrydailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(response_data['id'], self.stat_one.id)
        self.assertEqual(response_data['connectivity_speed'], self.stat_one.connectivity_speed)
        self.assertEqual(response_data['date'], format_date(self.stat_one.date))

    def test_retrieve_wrong_id(self):
        url, view, view_info = statistics_url((1234546,), {},
                                              view_name='update-retrieve-countrydailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update(self):
        url, _, view = statistics_url((self.stat_two.id,), {},
                                      view_name='update-retrieve-countrydailystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": 10000000,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "date": self.stat_two.date,
                "country": self.stat_two.country.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_update_wrong_id(self):
        url, _, view = statistics_url((123434567,), {},
                                      view_name='update-retrieve-countrydailystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": self.stat_two.connectivity_speed,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "date": self.stat_two.date,
                "country": self.stat_two.country.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update_invalid_data(self):
        url, _, view = statistics_url((self.stat_two.id,), {},
                                      view_name='update-retrieve-countrydailystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": 234.123,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "date": self.stat_two.date,
                "country": self.stat_two.country.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_delete(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-countrydailystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [self.stat_two.id]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_delete_without_ids(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-countrydailystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_delete_wrong_ids(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-countrydailystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [12345432]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)


class SchoolSummaryAPIViewSetAPITestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school_one = SchoolFactory(country=cls.country, location__country=cls.country, geopoint=None)
        cls.school_two = SchoolFactory(country=cls.country, location__country=cls.country)

        cls.stat_one = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=True,
            year=datetime.now().year - 1,
            week=12,
        )
        cls.stat_two = SchoolWeeklyStatusFactory(
            school=cls.school_one,
            connectivity=False,
            year=datetime.now().year - 1,
            week=13,
        )
        cls.stat_three = SchoolWeeklyStatusFactory(
            school=cls.school_two,
            connectivity=True,
            year=datetime.now().year - 1,
            week=12,
        )

        cls.user = test_utilities.setup_admin_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-schoolweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 3 records as we created manually in setup
        self.assertEqual(response_data['count'], 3)
        self.assertEqual(len(response_data['results']), 3)

    def test_school_id_filter(self):
        url, _, view = statistics_url((), {'school_id': self.school_one.id},
                                      view_name='list-create-destroy-schoolweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_year_week_filter(self):
        url, _, view = statistics_url((), {'year': datetime.now().year - 1, 'week': 12},
                                      view_name='list-create-destroy-schoolweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_search(self):
        url, _, view = statistics_url((), {'search': self.school_one.name},
                                      view_name='list-create-destroy-schoolweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_retrieve(self):
        url, view, view_info = statistics_url((self.stat_one.id,), {},
                                              view_name='update-retrieve-schoolweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(response_data['id'], self.stat_one.id)
        self.assertEqual(response_data['connectivity_speed'], self.stat_one.connectivity_speed)
        self.assertEqual(response_data['year'], self.stat_one.year)
        self.assertEqual(response_data['week'], self.stat_one.week)

    def test_retrieve_wrong_id(self):
        url, view, view_info = statistics_url((1234546,), {},
                                              view_name='update-retrieve-schoolweeklystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update(self):
        url, _, view = statistics_url((self.stat_two.id,), {},
                                      view_name='update-retrieve-schoolweeklystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": self.stat_two.connectivity_speed,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "year": self.stat_two.year,
                "week": self.stat_two.week,
                "date": self.stat_two.date,
                "school": self.school_one.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_update_wrong_id(self):
        url, _, view = statistics_url((123434567,), {},
                                      view_name='update-retrieve-schoolweeklystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": self.stat_two.connectivity_speed,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "year": self.stat_two.year,
                "week": self.stat_two.week,
                "date": self.stat_two.date,
                "school": self.school_two.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update_invalid_data(self):
        url, _, view = statistics_url((self.stat_two.id,), {},
                                      view_name='update-retrieve-schoolweeklystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": self.stat_two.connectivity_speed,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "year": self.stat_two.year,
                "week": self.stat_two.week,
                "date": self.stat_two.date,
                'coverage_type': '7g',
                "school": self.school_one.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_delete(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-schoolweeklystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [self.stat_two.id]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_delete_without_ids(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-schoolweeklystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_delete_wrong_ids(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-schoolweeklystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [12345432]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)


class SchoolDailyConnectivitySummaryAPIViewSetAPITestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.school_one = SchoolFactory(country=cls.country, location__country=cls.country, geopoint=None)
        cls.school_two = SchoolFactory(country=cls.country, location__country=cls.country)

        today = datetime.now().date()

        cls.stat_one = SchoolDailyStatusFactory(
            school=cls.school_one,
            date=today,
            live_data_source='DAILY_CHECK_APP_MLAB'
        )
        cls.stat_two = SchoolDailyStatusFactory(
            school=cls.school_one,
            date=today,
            live_data_source='QOS'
        )

        cls.stat_three = SchoolDailyStatusFactory(
            school=cls.school_two,
            date=today,
            live_data_source='DAILY_CHECK_APP_MLAB'
        )

        cls.user = test_utilities.setup_admin_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-schooldailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 3 records as we created manually in setup
        self.assertEqual(response_data['count'], 3)
        self.assertEqual(len(response_data['results']), 3)

    def test_school_id_filter(self):
        url, _, view = statistics_url((), {'school_id': self.school_one.id},
                                      view_name='list-create-destroy-schooldailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_search(self):
        url, _, view = statistics_url((), {'search': self.school_one.name},
                                      view_name='list-create-destroy-schooldailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)
        # 2 records as we created manually in setup
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_retrieve(self):
        url, view, view_info = statistics_url((self.stat_one.id,), {},
                                              view_name='update-retrieve-schooldailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(response_data['id'], self.stat_one.id)
        self.assertEqual(response_data['connectivity_speed'], self.stat_one.connectivity_speed)
        self.assertEqual(response_data['date'], format_date(self.stat_one.date))

    def test_retrieve_wrong_id(self):
        url, view, view_info = statistics_url((1234546,), {},
                                              view_name='update-retrieve-schooldailystatus')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update(self):
        url, _, view = statistics_url((self.stat_two.id,), {},
                                      view_name='update-retrieve-schooldailystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": 10000000,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "date": self.stat_two.date,
                "school": self.stat_two.school.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_update_wrong_id(self):
        url, _, view = statistics_url((123434567,), {},
                                      view_name='update-retrieve-schooldailystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": self.stat_two.connectivity_speed,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "date": self.stat_two.date,
                "school": self.stat_two.school.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update_invalid_data(self):
        url, _, view = statistics_url((self.stat_two.id,), {},
                                      view_name='update-retrieve-schooldailystatus')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                "id": self.stat_two.id,
                "created": self.stat_two.created,
                "modified": self.stat_two.modified,
                "connectivity_speed": 234.123,
                "connectivity_upload_speed": self.stat_two.connectivity_upload_speed,
                "date": self.stat_two.date,
                "school": self.stat_two.school.id
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_delete(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-schooldailystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [self.stat_two.id]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_delete_without_ids(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-schooldailystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_delete_wrong_ids(self):
        url, _, view = statistics_url((), {}, view_name='list-create-destroy-schooldailystatus')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [12345432]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)


class TimePlayerApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        args = ['--delete_data_sources', '--update_data_sources', '--update_data_layers']
        call_command('load_system_data_layers', *args)

        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

    def test_get_invalid_layer_id(self):
        url, _, view = statistics_url((), {
            'layer_id': 123,
            'country_id': 123,
        }, view_name='get-time-player-data')

        response = self.forced_auth_req('get', url, _, view=view)

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_for_live_layer(self):
        pcdc_data_source = accounts_models.DataSource.objects.filter(
            data_source_type=accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP,
        ).first()

        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'icon': '<icon>',
                'name': 'Test data layer 3',
                'description': 'Test data layer 3 description',
                'version': '1.0.0',
                'type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'data_sources_list': [pcdc_data_source.id, ],
                'data_source_column': pcdc_data_source.column_config[0],
                'global_benchmark': {
                    'value': '20000000',
                    'unit': 'bps',
                    'convert_unit': 'mbps'
                },
                'is_reverse': False,
                'legend_configs': {
                    'good': {
                        'values': [],
                        'labels': 'Good'
                    },
                    'moderate': {
                        'values': [],
                        'labels': 'Moderate'
                    },
                    'bad': {
                        'values': [],
                        'labels': 'Bad'
                    },
                    'unknown': {
                        'values': [],
                        'labels': 'Unknown'
                    }
                }
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        layer_id = response_data['id']

        url, _, view = accounts_url((layer_id,), {},
                                    view_name='update-or-delete-data-layer')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'status': accounts_models.DataLayer.LAYER_STATUS_READY_TO_PUBLISH,
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

        url, _, view = accounts_url((layer_id,), {},
                                    view_name='publish-data-layer')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'status': accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

        url, _, view = statistics_url((), {
            'layer_id': layer_id,
            'country_id': 123,
            'z': '2',
            'x': '1',
            'y': '2.mvt',
        }, view_name='get-time-player-data')

        response = self.forced_auth_req('get', url, _, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
