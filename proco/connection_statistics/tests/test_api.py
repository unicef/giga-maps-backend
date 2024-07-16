import random
from datetime import datetime, timedelta

from django.core.cache import cache
from django.test import TestCase
from django.urls import resolve, reverse
from isoweek import Week
from rest_framework import exceptions as rest_exceptions
from rest_framework import status
from rest_framework.test import APITestCase

from proco.connection_statistics.models import CountryWeeklyStatus
from proco.connection_statistics.tests.factories import (
    CountryDailyStatusFactory,
    CountryWeeklyStatusFactory,
    SchoolDailyStatusFactory,
    SchoolWeeklyStatusFactory,
)
from proco.custom_auth import models as auth_models
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory
from proco.utils.dates import format_date, get_first_date_of_month, get_last_date_of_month
from proco.utils.tests import TestAPIViewSetMixin


class GlobalStatisticsApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', 'read_only_database']

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
        response = self.forced_auth_req(
            'get',
            reverse('connection_statistics:global-stat'),
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['no_of_countries'], 1)
        self.assertEqual(response.data['schools_connected'], 2)
        self.assertEqual(list(response.data['connected_schools'].keys()),
                         ['connected', 'not_connected', 'unknown'])

    def test_global_stats_queries(self):
        with self.assertNumQueries(7):
            self.forced_auth_req(
                'get',
                reverse('connection_statistics:global-stat'),
            )
        # TODO: Test only when caching is enabled
        # with self.assertNumQueries(0):
        #     self.forced_auth_req(
        #         'get',
        #         reverse('connection_statistics:global-stat'),
        #     )


#
# class CountryWeekStatsApiTestCase(TestAPIViewSetMixin, TestCase):
#     @classmethod
#     def setUpTestData(cls):
#         cls.country_one = CountryFactory()
#         cls.country_two = CountryFactory()
#         cls.stat_one = CountryWeeklyStatusFactory(country=cls.country_one)
#         cls.stat_two = CountryWeeklyStatusFactory(country=cls.country_two)
#
#     def test_country_weekly_stats(self):
#         response = self.forced_auth_req(
#             'get',
#             reverse('connection_statistics:country-weekly-stat', kwargs={
#                 'country_code': self.stat_one.country.code.lower(),
#                 'year': self.stat_one.year,
#                 'week': self.stat_one.week,
#             }),
#         )
#         self.assertEqual(response.status_code, status.HTTP_200_OK)
#         self.assertEqual(response.data['schools_total'], self.stat_one.schools_total)
#         self.assertEqual(response.data['avg_distance_school'], self.stat_one.avg_distance_school)
#         self.assertEqual(response.data['schools_connected'], self.stat_one.schools_connected)
#         self.assertEqual(response.data['schools_connectivity_unknown'], self.stat_one.schools_connectivity_unknown)
#         self.assertEqual(response.data['schools_connectivity_moderate'], self.stat_one.schools_connectivity_moderate)
#         self.assertEqual(response.data['schools_connectivity_good'], self.stat_one.schools_connectivity_good)
#         self.assertEqual(response.data['schools_connectivity_no'], self.stat_one.schools_connectivity_no)
#         self.assertEqual(response.data['integration_status'], self.stat_one.integration_status)
#
#     def test_country_weekly_stats_queries(self):
#         code = self.stat_one.country.code.lower()
#         with self.assertNumQueries(2):
#             self.forced_auth_req(
#                 'get',
#                 reverse('connection_statistics:country-weekly-stat', kwargs={
#                     'country_code': code,
#                     'year': self.stat_one.year,
#                     'week': self.stat_one.week,
#                 }),
#             )


class CountryDailyStatsApiTestCase(TestAPIViewSetMixin, TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()

        cls.country_one_stats_number = random.SystemRandom().randint(a=5, b=25)
        for _i in range(cls.country_one_stats_number):
            CountryDailyStatusFactory(country=cls.country_one)

        CountryDailyStatusFactory(country=cls.country_two)

    def test_country_weekly_stats(self):
        response = self.forced_auth_req(
            'get',
            reverse('connection_statistics:country-daily-stat', kwargs={
                'country_code': self.country_one.code.lower(),
            }),
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], self.country_one_stats_number)

        response = self.forced_auth_req(
            'get',
            reverse('connection_statistics:country-daily-stat', kwargs={
                'country_code': self.country_two.code.lower(),
            }),
        )
        self.assertEqual(response.data['count'], 1)

    def test_country_weekly_stats_queries(self):
        code = self.country_one.code.lower()
        with self.assertNumQueries(2):
            self.forced_auth_req(
                'get',
                reverse('connection_statistics:country-daily-stat', kwargs={
                    'country_code': code,
                }),
            )


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

    def list_school_coverage_url(self, url_params, query_param):
        view_name = 'connection_statistics:school-coverage-stat'
        url = reverse(view_name, args=url_params)
        view_info = resolve(url).func

        if len(query_param) > 0:
            query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
            url += query_params
        return url, view_info

    def test_school_coverage_stat_school_list(self):
        url, view = self.list_school_coverage_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

    def test_school_coverage_stat_without_school_id(self):
        url, view = self.list_school_coverage_url((), {
            'country_id': self.country.id,
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # TODO: Change it once that hard coded school id is removed
        self.assertEqual(len(response.data), 0)

    def test_school_coverage_stat_without_country_id(self):
        url, view = self.list_school_coverage_url((), {})

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_school_coverage_stat_for_one_school(self):
        url, view = self.list_school_coverage_url((), {
            'country_id': self.country.id,
            'school_ids': str(self.school_one.id),
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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
        url, view = self.list_school_coverage_url((), {
            'country_id': self.country.id,
            'school_ids': str(self.school_one.id),
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        school_data = response.data[0]
        self.assertIn('statistics', school_data)

        school_statistics_data = school_data['statistics']

        self.assertEqual(school_statistics_data['coverage_type'], '3g')
        self.assertEqual(school_statistics_data['connectivity_speed'], round(3 * (10 ** 6) / 1000000, 2))

    def test_school_coverage_stat_for_coverage_type_choices(self):
        url, view = self.list_school_coverage_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_coverage_url((), {
            'country_id': self.country.id,
            'school_ids': school_four.id,
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_coverage_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[1]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[2]['statistics']['connectivity_status'], 'unknown')

    def test_school_coverage_stat_for_connectivity_status_when_connectivity_availability_static_speed(self):
        connectivity_availability = CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.static_speed
        self.country.last_weekly_status.connectivity_availability = connectivity_availability
        self.country.last_weekly_status.save()

        url, view = self.list_school_coverage_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[1]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[2]['statistics']['connectivity_status'], 'unknown')

    def test_school_coverage_stat_for_connectivity_status_when_country_weekly_status_not_available(self):
        self.country.last_weekly_status = None
        self.country.save()

        url, view = self.list_school_coverage_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertEqual(response.data[0]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[1]['statistics']['connectivity_status'], 'unknown')
        self.assertEqual(response.data[2]['statistics']['connectivity_status'], 'unknown')


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

    def list_school_connectivity_stat_url(self, url_params, query_param):
        view_name = 'connection_statistics:school-connectivity-stat'
        url = reverse(view_name, args=url_params)
        view_info = resolve(url).func

        if len(query_param) > 0:
            query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
            url += query_params
        return url, view_info

    def test_school_download_connectivity_stat_school_list(self):
        """
        test_school_download_connectivity_stat_school_list
            Positive test case for weekly data.

        Expected: HTTP_200_OK - List of data for all 3 schools
        """
        today = datetime.now().date()
        date_7_days_back = today - timedelta(days=6)

        url, view = self.list_school_connectivity_stat_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_connectivity_stat_url((), {
            'country_id': self.country.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_connectivity_stat_url((), {
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_connectivity_stat_url((), {
            'country_id': self.country.id,
            'school_ids': self.school_one.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_connectivity_stat_url((), {
            'country_id': self.country.id,
            'school_ids': self.school_one.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_connectivity_stat_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_connectivity_stat_url((), {
            'country_id': self.country.id,
            'school_ids': self.school_one.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_connectivity_stat_url((), {
            'country_id': self.country.id,
            'school_ids': self.school_two.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_connectivity_stat_url((), {
            'country_id': self.country.id,
            'school_ids': school_four.id,
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

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

        url, view = self.list_school_connectivity_stat_url((), {
            'country_id': self.country.id,
            'school_ids': ','.join([str(self.school_one.id), str(self.school_two.id), str(self.school_three.id)]),
            'start_date': format_date(date_7_days_back),
            'end_date': format_date(today),
            'is_weekly': 'true',
        })

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

        # self.assertEqual(response.data[0]['statistics']['connectivity_speed'], 0)
        # self.assertEqual(response.data[1]['statistics']['connectivity_speed'], 0)
        # self.assertEqual(response.data[2]['statistics']['connectivity_speed'], 0)


class CountryWeekStatsApiTestCase(TestAPIViewSetMixin, TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()

        cls.stat_one = CountryWeeklyStatusFactory(country=cls.country_one)
        cls.stat_two = CountryWeeklyStatusFactory(country=cls.country_two)

        cls.country_one_daily = CountryDailyStatusFactory(country=cls.country_one,
                                                          date=Week(cls.stat_one.year, cls.stat_one.week).monday())

        cls.email = 'test@test.com'
        cls.password = 'SomeRandomPass96'
        cls.user = auth_models.ApplicationUser.objects.create_user(username=cls.email, password=cls.password)

        cls.role = auth_models.Role.objects.create(name='Admin', category='system')
        cls.role_permission = auth_models.UserRoleRelationship.objects.create(user=cls.user, role=cls.role)

    def setUp(self):
        cache.clear()
        super().setUp()

    def country_connectivity_stat_url(self, url_params, query_param):
        view_name = 'connection_statistics:list_or_create_destroy_countryweeklystatus'
        url = reverse(view_name, args=url_params)
        view_info = resolve(url).func

        if len(query_param) > 0:
            query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
            url += query_params
        return url, view_info

    def test_country_download_connectivity_stat(self):
        """
        test_country_download_connectivity_stat
            Positive test case for country weekly data.

        Expected: HTTP_200_OK - List of data for given country id
        """
        date = Week(self.stat_one.year, self.stat_one.week).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, view = self.country_connectivity_stat_url((), {
            'country_id': self.country_one.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'global'
        })

        response = self.forced_auth_req('get', url, user=self.user, view=view)
        # print(response.data)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)

        # self.assertIn('live_avg', response_data)
        # self.assertIn('schools_total', response_data['results'])
        # self.assertIn('school_with_realtime_data', response_data)
        # self.assertIn('is_data_synced', response_data)
        # self.assertIn('graph_data', response_data)
        # self.assertIn('real_time_connected_schools', response_data)

    def test_country_download_connectivity_stat_data(self):
        date = Week(self.stat_one.year, self.stat_one.week).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, view = self.country_connectivity_stat_url((), {
            'country_id': self.country_one.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'global'
        })

        response = self.forced_auth_req('get', url, user=self.user, view=view)
        # print(response.data)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # self.assertEqual(response.data[0]['schools_total'], self.stat_one.schools_total)
        # self.assertEqual(response.data[0]['school_with_realtime_data'], self.stat_one.schools_connected)

    def test_country_download_connectivity_stat_for_invalid_country_id(self):
        date = Week(self.stat_one.year, self.stat_one.week).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, view = self.country_connectivity_stat_url((), {
            'country_id': 123456,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'global'
        })

        response = self.forced_auth_req('get', url, user=self.user, view=view)
        # print(response.data)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_country_download_connectivity_stat_for_invalid_date_range(self):
        date = Week(2023, 56).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, view = self.country_connectivity_stat_url((), {
            'country_id': self.country_one.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'global'
        })

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_country_download_connectivity_stat_for_missing_country_id(self):
        date = Week(self.stat_one.year, self.stat_one.week).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, view = self.country_connectivity_stat_url((), {
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'global'
        })

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_country_download_connectivity_stat_for_national_benchmark(self):
        date = Week(self.stat_one.year, self.stat_one.week).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, view = self.country_connectivity_stat_url((), {
            'country_id': self.country_one.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'national',
        })

        response = self.forced_auth_req('get', url, user=self.user, view=view)
        # print(response.data)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # self.assertEqual(response.data['schools_total'], self.stat_one.schools_total)

        # self.assertEqual(response.data['real_time_connected_schools']['good'],
        #                  self.stat_one.schools_connectivity_good)
        # self.assertEqual(response.data['real_time_connected_schools']['moderate'],
        #                  self.stat_one.schools_connectivity_moderate)
        # self.assertEqual(response.data['real_time_connected_schools']['no_internet'],
        #                  self.stat_one.schools_connectivity_no)
        # self.assertEqual(response.data['real_time_connected_schools']['unknown'],
        #                  self.stat_one.schools_connectivity_unknown)

    def test_country_uptime_connectivity_stat(self):
        date = Week(self.stat_one.year, self.stat_one.week).monday()
        start_date = date - timedelta(days=1)
        end_date = start_date + timedelta(days=6)

        url, view = self.country_connectivity_stat_url((), {
            'country_id': self.country_one.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'true',
            'benchmark': 'global'
        })

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        # self.assertIn('live_avg', response_data)
        # self.assertIn('schools_total', response_data['results'])
        # self.assertIn('school_with_realtime_data', response_data)
        # self.assertIn('is_data_synced', response_data)
        # self.assertIn('graph_data', response_data)
        # self.assertIn('real_time_connected_schools', response_data)

    def test_country_download_connectivity_stat_monthly(self):
        """
        test_country_download_connectivity_stat_monthly
            Positive test case for country weekly data.

        Expected: HTTP_200_OK - List of data for given country id
        """
        date = Week(self.stat_one.year, self.stat_one.week).monday()
        start_date = get_first_date_of_month(date.year, date.month)
        end_date = get_last_date_of_month(date.year, date.month)

        url, view = self.country_connectivity_stat_url((), {
            'country_id': self.country_one.id,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'false',
            'benchmark': 'global'
        })

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(type(response_data), dict)

        # self.assertIn('live_avg', response_data)
        # self.assertIn('schools_total', response_data['results'])
        # self.assertIn('school_with_realtime_data', response_data)
        # self.assertIn('is_data_synced', response_data)
        # self.assertIn('graph_data', response_data)
        # self.assertIn('real_time_connected_schools', response_data)

    def test_country_download_connectivity_stat_monthly_invalid_country_id(self):
        """
        test_country_download_connectivity_stat
            Positive test case for country weekly data.

        Expected: HTTP_200_OK - List of data for given country id
        """
        date = Week(self.stat_one.year, self.stat_one.week).monday()
        start_date = get_first_date_of_month(date.year, date.month)
        end_date = get_last_date_of_month(date.year, date.month)

        url, view = self.country_connectivity_stat_url((), {
            'country_id': 123456,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'is_weekly': 'false',
            'benchmark': 'global'
        })

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class CountryCoverageStatsAPITestCase(APITestCase):
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
        url = reverse('connection_statistics:country-coverage-stat')
        query_params = {'country_id': self.country_one.id}
        response = self.client.get(url, query_params)
        # print(response.data)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['total_schools'], self.stat_one.schools_total)
        # self.assertEqual(response.data['connected_schools']['5g_4g'], self.stat_one.schools_coverage_good)
        # self.assertEqual(response.data['connected_schools']['3g_2g'], self.stat_one.schools_coverage_moderate)
        # self.assertEqual(response.data['connected_schools']['no_coverage'], self.stat_one.schools_coverage_no)
        # self.assertEqual(response.data['connected_schools']['unknown'], self.stat_one.schools_coverage_unknown)

    def test_get_country_coverage_stats_no_data(self):
        url = reverse('connection_statistics:country-coverage-stat')
        query_params = {'country_id': 999}  # Assuming this country ID does not exist
        response = self.client.get(url, query_params)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_get_country_coverage_stats_cached(self):
        url = reverse('connection_statistics:country-coverage-stat')
        query_params = {'country_id': self.country_one.id}
        # Call the API to cache the data
        with self.assertNumQueries(6):
            self.client.get(url, query_params)

        with self.assertNumQueries(0):
            self.client.get(url, query_params)

    def test_get_country_coverage_stats_no_cache(self):
        url = reverse('connection_statistics:country-coverage-stat')
        query_params = {'country_id': self.country_one.id}
        # Call the API without caching
        with self.assertNumQueries(6):
            response = self.client.get(url, query_params, HTTP_CACHE_CONTROL='no-cache')
            self.assertEqual(response.status_code, status.HTTP_200_OK)
