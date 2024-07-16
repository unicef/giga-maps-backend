from django.core.cache import cache
from django.test import TestCase
from django.urls import resolve, reverse
from rest_framework import status

from proco.connection_statistics.models import CountryWeeklyStatus
from proco.connection_statistics.tests.factories import SchoolWeeklyStatusFactory
from proco.custom_auth.tests import test_utils as test_utilities
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory, FileImportFactory
from proco.utils.tests import TestAPIViewSetMixin


def schools_url(url_params, query_param, view_name='schools-list'):
    url = reverse('schools:' + view_name, args=url_params)
    view = resolve(url)
    view_info = view.func

    if len(query_param) > 0:
        query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
        url += query_params
    return url, view, view_info


class SchoolsApiTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'schools:schools'

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

        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

        cls.imported_file_one = FileImportFactory(country=cls.country)
        cls.imported_file_one.uploaded_by = cls.admin_user
        cls.imported_file_one.save()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_schools_list(self):
        with self.assertNumQueries(2):
            response = self.forced_auth_req(
                'get',
                reverse('schools:schools-list', args=[self.country.code.lower()]),
                user=None,
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertIn('connectivity_status', response.data[0])
            self.assertIn('coverage_status', response.data[0])
            self.assertIn('is_verified', response.data[0])

    def test_schools_list_with_part_availability(self):
        connectivity_availability = CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.connectivity
        self.country.last_weekly_status.connectivity_availability = connectivity_availability
        coverage_availability = CountryWeeklyStatus.COVERAGE_TYPES_AVAILABILITY.coverage_availability
        self.country.last_weekly_status.coverage_availability = coverage_availability
        self.country.last_weekly_status.save()
        response = self.forced_auth_req(
            'get',
            reverse('schools:schools-list', args=[self.country.code.lower()]),
            user=None,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('connectivity_status', response.data[0])
        self.assertIn('coverage_status', response.data[0])
        self.assertEqual(response.data[0]['connectivity_status'], 'unknown')
        self.assertEqual(response.data[0]['coverage_status'], 'unknown')
        self.assertEqual(response.data[1]['connectivity_status'], 'unknown')
        self.assertEqual(response.data[1]['coverage_status'], 'unknown')
        self.assertEqual(response.data[2]['connectivity_status'], 'unknown')
        self.assertEqual(response.data[2]['coverage_status'], 'unknown')

    def test_schools_list_with_full_availability(self):
        connectivity_availability = CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.realtime_speed
        self.country.last_weekly_status.connectivity_availability = connectivity_availability
        coverage_availability = CountryWeeklyStatus.COVERAGE_TYPES_AVAILABILITY.coverage_type
        self.country.last_weekly_status.coverage_availability = coverage_availability

        self.country.last_weekly_status.save()
        response = self.forced_auth_req(
            'get',
            reverse('schools:schools-list', args=[self.country.code.lower()]),
            user=None,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('connectivity_status', response.data[0])
        self.assertIn('coverage_status', response.data[0])
        self.assertEqual(response.data[0]['connectivity_status'], 'unknown')
        self.assertEqual(response.data[0]['coverage_status'], 'unknown')
        self.assertEqual(response.data[1]['connectivity_status'], 'unknown')
        self.assertEqual(response.data[1]['coverage_status'], 'unknown')
        self.assertEqual(response.data[2]['connectivity_status'], 'unknown')
        self.assertEqual(response.data[2]['coverage_status'], 'unknown')

    def test_authorization_user(self):
        response = self.forced_auth_req(
            'get',
            reverse('schools:schools-list', args=[self.country.code.lower()]),
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_schools_detail(self):
        with self.assertNumQueries(2):
            response = self.forced_auth_req(
                'get',
                reverse('schools:schools-detail', args=[self.country.code.lower(), self.school_one.id]),
                user=None,
            )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['id'], self.school_one.id)

    def test_update_keys(self):
        # todo: move me to proper place
        from proco.locations.models import Country
        from proco.utils.tasks import update_cached_value
        for country in Country.objects.all():
            update_cached_value(url=reverse('locations:countries-detail', kwargs={'pk': country.code.lower()}))
            update_cached_value(url=reverse('schools:schools-list', kwargs={'country_code': country.code.lower()}))

        self.assertListEqual(
            list(sorted(cache.keys('*'))),  # noqa: C413
            list(sorted([  # noqa: C413
                f'SOFT_CACHE_COUNTRY_INFO_pk_{self.country.code.lower()}',
                f'SOFT_CACHE_SCHOOLS_{self.country.code.lower()}_',
            ])),
        )

    def test_random_schools_list(self):
        with self.assertNumQueries(2):
            response = self.forced_auth_req(
                'get',
                reverse('schools:random-schools'),
                user=None,
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertIn('geopoint', response.data[0])
            self.assertIn('country_integration_status', response.data[0])
            self.assertIn('country_id', response.data[0])

    def test_default_coverage_layer_school_tiles_country_view(self):
        url, _, view = schools_url((), {
            'country_id': self.country.id,
            'z': '2',
            'x': '1',
            'y': '2.mvt',
        }, view_name='tiles-view')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_default_coverage_layer_school_tiles_admin_view(self):
        url, _, view = schools_url((), {
            'country_id': self.country.id,
            'admin1_id': '12345678',
            'z': '2',
            'x': '1',
            'y': '2.mvt',
        }, view_name='tiles-view')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_default_coverage_layer_school_tiles_global_view(self):
        url, _, view = schools_url((), {
            'z': '2',
            'x': '1',
            'y': '2.mvt',
        }, view_name='tiles-view')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_default_download_layer_school_tiles_country_view(self):
        url, _, view = schools_url((), {
            'country_id': self.country.id,
            'indicator': 'download',
            'benchmark': 'global',
            'start_date': '24-06-2024',
            'end_date': '30-06-2024',
            'is_weekly': 'true',
            'z': '2',
            'x': '1',
            'y': '2.mvt',
        }, view_name='tiles-connectivity-view')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_default_download_layer_school_tiles_admin_view(self):
        url, _, view = schools_url((), {
            'country_id': self.country.id,
            'admin1_id': '1234543',
            'indicator': 'download',
            'benchmark': 'global',
            'start_date': '24-06-2024',
            'end_date': '30-06-2024',
            'is_weekly': 'true',
            'z': '2',
            'x': '1',
            'y': '2.mvt',
        }, view_name='tiles-connectivity-view')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_default_download_layer_school_tiles_global_view(self):
        url, _, view = schools_url((), {
            'indicator': 'download',
            'benchmark': 'global',
            'start_date': '24-06-2024',
            'end_date': '30-06-2024',
            'is_weekly': 'true',
            'z': '2',
            'x': '1',
            'y': '2.mvt',
        }, view_name='tiles-connectivity-view')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_default_download_layer_school_tiles_country_view_month_filter(self):
        url, _, view = schools_url((), {
            'country_id': self.country.id,
            'indicator': 'download',
            'benchmark': 'global',
            'start_date': '01-06-2024',
            'end_date': '30-06-2024',
            'is_weekly': 'false',
            'z': '2',
            'x': '1',
            'y': '2.mvt',
        }, view_name='tiles-connectivity-view')

        response = self.forced_auth_req('get', url, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_list_file_imports_on_admin_view(self):
        url, _, view = schools_url((), {}, view_name='file-import')

        response = self.forced_auth_req('get', url, view=view, user=self.admin_user)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_update_school(self):
        url, _, view = schools_url((self.school_one.id,), {},
                                   view_name='update-or-retrieve-school')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'name': self.school_one.name + ' Test',
                'timezone': 'UTC',
                'country': self.country.id,
                'giga_id_school': self.school_one.giga_id_school + '-test',
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_update_school_giga_id_to_duplicate_value(self):
        url, _, view = schools_url((self.school_one.id,), {},
                                   view_name='update-or-retrieve-school')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'name': self.school_one.name + ' Test',
                'timezone': 'UTC',
                'country': self.country.id,
                'giga_id_school': self.school_two.giga_id_school,
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update_school_giga_id_to_invalid_regex(self):
        url, _, view = schools_url((self.school_one.id,), {},
                                   view_name='update-or-retrieve-school')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'name': self.school_one.name + ' Test',
                'timezone': 'UTC',
                'country': self.country.id,
                'giga_id_school': self.school_one.giga_id_school + '$!@#',
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update_school_to_invalid_id(self):
        url, _, view = schools_url((12345678,), {},
                                   view_name='update-or-retrieve-school')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'name': self.school_one.name + ' Test',
                'timezone': 'UTC',
                'country': self.country.id,
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_list_schools(self):
        url, _, view = schools_url((), {}, view_name='list-create-destroy-school')

        response = self.forced_auth_req('get', url, view=view, user=self.admin_user)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_create_school_by_admin(self):
        url, _, view = schools_url((), {}, view_name='list-create-destroy-school')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'name': 'New School',
                'giga_id_school': 'ac65543e-cdba-4f5c-891a-448bzdcfge099',
                'external_id': '25805591031323454',
                'country': self.country.id,
                'geopoint': {
                    'type': 'Point',
                    'coordinates': [
                        76.92044830322266,
                        9.022849082946777
                    ]
                },
                'gps_confidence': 1.0,
                'altitude': 0,
                'timezone': 'Africa/Conakry'
            }
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_restore_school_by_admin(self):
        url, _, view = schools_url((), {}, view_name='list-create-destroy-school')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'name': 'New School 2',
                'giga_id_school': 'ac65543e-cdba-4f5c-891a-448bzdc12e099',
                'external_id': '258055910313231',
                'country': self.country.id,
                'geopoint': {
                    'type': 'Point',
                    'coordinates': [
                        76.92044830322266,
                        9.022849082946777
                    ]
                },
                'gps_confidence': 1.0,
                'altitude': 0,
                'timezone': 'Africa/Conakry'
            }
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        school_id = response.data['id']

        put_response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
            data={
                'id': [school_id, ]
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'name': 'New School 3',
                'giga_id_school': 'ac65543e-cdba-4f5c-891a-448bzdc12e099',
                'external_id': '258055910313231',
                'country': self.country.id,
                'geopoint': {
                    'type': 'Point',
                    'coordinates': [
                        76.92044830322266,
                        9.022849082946777
                    ]
                },
                'gps_confidence': 1.0,
                'altitude': 0,
                'timezone': 'Africa/Conakry'
            }
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertEqual(school_id, response.data['id'])

    def test_retrieve_school(self):
        url, view, view_info = schools_url((self.school_one.id,), {}, view_name='update-or-retrieve-school')

        response = self.forced_auth_req('get', url, view=view, user=self.admin_user, view_info=view_info)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_retrieve_school_to_invalid_id(self):
        url, view, view_info = schools_url((1234,), {}, view_name='update-or-retrieve-school')

        response = self.forced_auth_req('get', url, view=view, user=self.admin_user, view_info=view_info)

        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_delete_school(self):
        url, _, view = schools_url((), {}, view_name='list-create-destroy-school')

        put_response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
            data={
                'id': [self.school_one.id, ]
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_delete_school_to_invalid_id(self):
        url, _, view = schools_url((), {}, view_name='list-create-destroy-school')

        put_response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
            data={
                'id': [54321, ]
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_download_school_data_without_api_key(self):
        url, view, view_info = schools_url((), {
            'page': '1',
            'page_size': '10',
            'ordering': 'name',
            'expand': 'country,last_weekly_status,admin1,admin2',
        }, view_name='download-schools')

        response = self.forced_auth_req(
            'get',
            url,
            user=self.admin_user,
            view_info=view_info,
            view=view,
            request_format='text/csv'
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
