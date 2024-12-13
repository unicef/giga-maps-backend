from django.contrib.gis.geos import GEOSGeometry
from django.core.cache import cache
from django.test import TestCase
from django.urls import resolve, reverse
from rest_framework import status

from proco.connection_statistics.tests.factories import CountryWeeklyStatusFactory
from proco.custom_auth.tests import test_utils as test_utilities
from proco.locations.tests.factories import Admin1Factory, CountryFactory
from proco.schools.tests.factories import SchoolFactory
from proco.utils.tests import TestAPIViewSetMixin


def locations_url(url_params, query_param, view_name='countries-list'):
    url = reverse('locations:' + view_name, args=url_params)
    view = resolve(url)
    view_info = view.func

    if len(query_param) > 0:
        query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
        url += query_params
    return url, view, view_info


class CountryApiTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'locations:countries'

    def get_detail_args(self, instance):
        return self.get_list_args() + [instance.code.lower()]

    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()
        cls.country_three = CountryFactory()

        cls.admin1_one = Admin1Factory(country=cls.country_one)

        SchoolFactory(country=cls.country_one, location__country=cls.country_one, admin1=cls.admin1_one)
        SchoolFactory(country=cls.country_one, location__country=cls.country_one, admin1=cls.admin1_one)

        CountryWeeklyStatusFactory(country=cls.country_one)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_countries_list(self):
        with self.assertNumQueries(4):
            response = self._test_list(
                user=None, expected_objects=[self.country_one, self.country_two, self.country_three],
            )
        self.assertIn('integration_status', response.data[0])

    def test_list_countries_with_schools(self):
        url, _, view = locations_url((), {'has_schools': 'true'})

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertIn('integration_status', response.data[0])

    def test_list_countries_without_schools(self):
        url, _, view = locations_url((), {'has_schools': 'false'})

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        self.assertIn('integration_status', response.data[0])

    def test_list_countries_with_school_master_records(self):
        url, _, view = locations_url((), {'has_school_master_records': 'true'})

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

    def test_list_countries_without_school_master_records(self):
        url, _, view = locations_url((), {'has_school_master_records': 'false'})

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        self.assertIn('integration_status', response.data[0])

    def test_country_detail(self):
        # 1. Get the country details
        # 2. Get country admin0 details
        # 3. Get all admin1 of country
        # 4. Active data layers
        # 5. Active filters
        with self.assertNumQueries(5):
            response = self._test_retrieve(
                user=None, instance=self.country_one,
            )
        self.assertIn('statistics', response.data)

    def test_country_list_cached(self):
        with self.assertNumQueries(4):
            self._test_list(
                user=None, expected_objects=[self.country_one, self.country_two, self.country_three],
            )

        with self.assertNumQueries(0):
            self._test_list(
                user=None, expected_objects=[self.country_one, self.country_two, self.country_three],
            )


class CountryBoundaryApiTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'locations:countries-list'

    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()

        cls.admin1_one = Admin1Factory(country=cls.country_one)

        SchoolFactory(country=cls.country_one, location__country=cls.country_one, admin1=cls.admin1_one)
        SchoolFactory(country=cls.country_one, location__country=cls.country_one, admin1=cls.admin1_one)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_countries_list(self):
        with self.assertNumQueries(3):
            response = self.forced_auth_req('get', reverse(self.base_view))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_country_list_cached(self):
        with self.assertNumQueries(3):
            response = self.forced_auth_req('get', reverse(self.base_view))
            self.assertEqual(response.status_code, status.HTTP_200_OK)

        with self.assertNumQueries(0):
            response = self.forced_auth_req('get', reverse(self.base_view))
            self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_empty_countries_hidden(self):
        CountryFactory(geometry=GEOSGeometry('{"type": "MultiPolygon", "coordinates": []}'))
        response = self.forced_auth_req('get', reverse(self.base_view))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # self.assertCountEqual([r['id'] for r in response.data], [self.country_one.id, self.country_two.id])


class CountryDataViewSetTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()
        cls.country_three = CountryFactory()

        cls.admin1_one = Admin1Factory(country=cls.country_one)

        cls.school_one = SchoolFactory(country=cls.country_one, location__country=cls.country_one,
                                       admin1=cls.admin1_one)
        cls.school_two = SchoolFactory(country=cls.country_one, location__country=cls.country_one,
                                       admin1=cls.admin1_one)

        cls.school_three = SchoolFactory(country=cls.country_two, location__country=cls.country_two, admin1=None)

        cls.user = test_utilities.setup_admin_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list(self):
        url, _, view = locations_url((), {}, view_name='list-create-destroy-country')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        # 3 records as we created manually in setup
        self.assertEqual(response_data['count'], 3)
        self.assertEqual(len(response_data['results']), 3)

    def test_country_id_filter(self):
        url, _, view = locations_url((), {'id': self.country_one.id},
                                     view_name='list-create-destroy-country')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(response_data['count'], 1)
        self.assertEqual(len(response_data['results']), 1)

    def test_search(self):
        url, _, view = locations_url((), {'search': self.country_one.name},
                                     view_name='list-create-destroy-country')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(response_data['count'], 1)
        self.assertEqual(len(response_data['results']), 1)

    def test_retrieve(self):
        url, view, view_info = locations_url((self.country_one.id,), {},
                                             view_name='update-retrieve-country')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(response_data['id'], self.country_one.id)
        self.assertEqual(response_data['name'], self.country_one.name)

    def test_retrieve_wrong_id(self):
        url, view, view_info = locations_url((1234546,), {},
                                             view_name='update-retrieve-country')

        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_update_wrong_id(self):
        url, view, view_info = locations_url((self.country_one.id,), {},
                                             view_name='update-retrieve-country')
        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        url, _, view = locations_url((123434567,), {}, view_name='update-retrieve-country')
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data=response_data
        )

        self.assertEqual(put_response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update_invalid_data(self):
        url, view, view_info = locations_url((self.country_one.id,), {},
                                             view_name='update-retrieve-country')
        response = self.forced_auth_req('get', url, user=self.user, view=view, view_info=view_info, )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        response_data['date_of_join'] = '2024-13-01'
        response_data['flag'] = b'abd'
        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data=response_data
        )

        self.assertEqual(put_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_delete(self):
        url, _, view = locations_url((), {}, view_name='list-create-destroy-country')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [self.country_three.id]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_delete_single_country(self):
        url, view, view_info = locations_url((self.country_three.id, ), {}, view_name='update-retrieve-country')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.user,
            view=view, view_info=view_info,
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

    def test_delete_without_ids(self):
        url, _, view = locations_url((), {}, view_name='list-create-destroy-country')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_delete_wrong_ids(self):
        url, _, view = locations_url((), {}, view_name='list-create-destroy-country')

        response = self.forced_auth_req(
            'delete',
            url,
            data={'id': [12345432]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_download_country_data_without_api_key(self):
        url, view, view_info = locations_url((), {
            'page': '1',
            'page_size': '10',
            'ordering': 'name',
        }, view_name='download-countries')

        response = self.forced_auth_req(
            'get',
            url,
            user=self.user,
            view_info=view_info,
            view=view,
            request_format='text/csv'
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_list_searchable_details_from_db(self):
        url, _, view = locations_url((), {}, view_name='search-countries-admin-schools')

        with self.assertNumQueries(1):
            response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        # 2 records as we created manually in setup and only 2 countries has schools
        self.assertEqual(len(response_data), 3)

        with self.assertNumQueries(0):
            response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

