from django.core.cache import cache
from django.test import TestCase
from django.urls import resolve, reverse
from rest_framework import status

from proco.custom_auth.tests import test_utils as test_utilities
from proco.data_sources.models import SchoolMasterData
from proco.data_sources.tests.factories import SchoolMasterDataFactory
from proco.locations.tests.factories import CountryFactory
from proco.utils.tests import TestAPIViewSetMixin


def sources_url(url_params, query_param, view_name='list-school-master-rows'):
    url = reverse('sources:' + view_name, args=url_params)
    view_info = resolve(url).func

    if len(query_param) > 0:
        query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
        url += query_params
    return url, view_info


class SchoolMasterApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default',]

    @classmethod
    def setUpTestData(cls):
        cls.country = CountryFactory()

        cls.row_one = SchoolMasterDataFactory(country=cls.country, status=SchoolMasterData.ROW_STATUS_DRAFT)
        cls.row_two = SchoolMasterDataFactory(country=cls.country, status=SchoolMasterData.ROW_STATUS_DRAFT)
        cls.row_three = SchoolMasterDataFactory(country=cls.country, school_id_giga=cls.row_two.school_id_giga,
                                                status=SchoolMasterData.ROW_STATUS_PUBLISHED)

        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_rows_list_for_admin(self):
        url, view = sources_url((), {})

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_rows_list_for_read_only_user(self):
        url, view = sources_url((), {})

        response = self.forced_auth_req('get', url, user=self.read_only_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_update_multiple_rows(self):
        row_id = self.row_one.id

        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': row_id,
                'school_name': 'New School Name'
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_200_OK)

        url, view = sources_url((row_id,), {}, view_name='retrieve-update-delete-school-master-data-row')

        response = self.forced_auth_req('get', url, user=self.admin_user, )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(response_data['id'], row_id)
        self.assertEqual(response_data['school_name'], 'New School Name')

    def test_invalid_update_multiple_rows(self):
        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'school_name': 'New School Name'
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_invalid_update_multiple_rows_with_wrong_id(self):
        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': 123456789,
                'school_name': 'New School Name'
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_invalid_multiple_updates_to_same_id_rows(self):
        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': self.row_one.id,
                'school_name': 'New School Name'
            }, {
                'id': self.row_one.id,
                'school_name': 'New School Name1'
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_update_to_single_id_rows(self):
        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'id': self.row_one.id,
                'school_name': 'New School Name 2'
            },
        )

        self.assertEqual(update_response.status_code, status.HTTP_200_OK)

    def test_invalid_update_multiple_rows_with_missing_data(self):
        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_update_single_row(self):
        row_id = self.row_one.id

        url, view = sources_url((row_id,), {}, view_name='retrieve-update-delete-school-master-data-row')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'school_name': 'New School Name 1'
            },
        )

        self.assertEqual(update_response.status_code, status.HTTP_200_OK)

        url, view = sources_url((row_id,), {}, view_name='retrieve-update-delete-school-master-data-row')

        response = self.forced_auth_req('get', url, user=self.admin_user, )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data
        self.assertEqual(response_data['id'], row_id)
        self.assertEqual(response_data['school_name'], 'New School Name 1')

    def test_publish_data_list(self):
        row_id = self.row_one.id

        url, view = sources_url((), {}, view_name='publish-school-master-data-rows')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': row_id,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_200_OK)

        url, view = sources_url((row_id,), {}, view_name='retrieve-update-delete-school-master-data-row')

        response = self.forced_auth_req('get', url, user=self.admin_user, )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_publish_data_list_invalid_id(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': 123456789,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_data_list_id_missing(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'name': '123456789',
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_without_data_list(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_data_list_same_id(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': self.row_one.id,
            }, {
                'id': self.row_one.id,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_row(self):
        row_id = self.row_two.id

        url, view = sources_url((row_id,), {}, view_name='publish-school-master-data-row')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
        )

        self.assertEqual(update_response.status_code, status.HTTP_200_OK)

        url, view = sources_url((row_id,), {}, view_name='retrieve-update-delete-school-master-data-row')

        response = self.forced_auth_req('get', url, user=self.admin_user, )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_publish_by_country(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows-for-country')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'country_id': self.row_one.country_id,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_200_OK)

        url, view = sources_url((self.row_one.id,), {}, view_name='retrieve-update-delete-school-master-data-row')

        response = self.forced_auth_req('get', url, user=self.admin_user, )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_publish_by_invalid_country_id(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows-for-country')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'country_id': 123456543,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_by_missing_country_id(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows-for-country')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': 123456543,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_by_missing_payload(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows-for-country')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_by_empty_payload(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows-for-country')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_to_same_country_id(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows-for-country')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'country_id': self.country.id,
            }, {
                'country_id': self.country.id,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_to_single_country_id(self):
        url, view = sources_url((), {}, view_name='publish-school-master-data-rows-for-country')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'country_id': self.country.id,
            },
        )

        self.assertEqual(update_response.status_code, status.HTTP_200_OK)

        url, view = sources_url((self.row_one.id,), {}, view_name='retrieve-update-delete-school-master-data-row')

        response = self.forced_auth_req('get', url, user=self.admin_user, )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_publish_data_list_to_published_row(self):
        row_id = self.row_three.id

        url, view = sources_url((), {}, view_name='publish-school-master-data-rows')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': row_id,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_update_multiple_rows_to_published_row(self):
        row_id = self.row_three.id

        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': row_id,
                'school_name': 'New School Name',
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publish_in_row_update_api_call(self):
        row_id = self.row_one.id

        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': row_id,
                'school_name': 'New School Name',
                'status': SchoolMasterData.ROW_STATUS_PUBLISHED,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_update_to_draft_from_draft(self):
        row_id = self.row_one.id

        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': row_id,
                'school_name': 'New School Name',
                'status': SchoolMasterData.ROW_STATUS_DRAFT,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publisher_update_to_UPDATED_IN_DRAFT(self):
        row_id = self.row_one.id

        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': row_id,
                'school_name': 'New School Name',
                'status': SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_publisher_update_to_ROW_STATUS_DRAFT_LOCKED(self):
        row_id = self.row_one.id

        url, view = sources_url((), {})

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'id': row_id,
                'school_name': 'New School Name',
                'status': SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
            }],
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

