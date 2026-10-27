from unittest.mock import patch

from django.core.cache import cache
from django.test import TestCase
from django.urls import resolve, reverse
from rest_framework import status

from proco.custom_auth.tests import test_utils as test_utilities
from proco.data_sources import api as sources_api
from proco.data_sources import tasks as sources_tasks
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

    def _setup_country_publish_rows(self):
        # One row per publishable status plus one UPDATED_IN_DRAFT row, which country publish must never touch.
        # Names and ids are fixed so that each search term matches a known subset of rows.
        country = CountryFactory()
        rows_data = (
            ('draft_primaria', SchoolMasterData.ROW_STATUS_DRAFT, 'Escola Primaria A', 'WRONGGOVT-A'),
            ('draft_secundaria', SchoolMasterData.ROW_STATUS_DRAFT, 'Escola Secundaria B', 'GOVT-B'),
            ('locked_primaria', SchoolMasterData.ROW_STATUS_DRAFT_LOCKED, 'Escola Primaria C', 'WRONGGOVT-C'),
            ('updated_locked_secundaria', SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
             'Escola Secundaria D', 'GOVT-D'),
            ('deleted_primaria', SchoolMasterData.ROW_STATUS_DELETED, 'Escola Primaria E', 'WRONGGOVT-E'),
            ('updated_in_draft_primaria', SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
             'Escola Primaria F', 'WRONGGOVT-F'),
        )
        rows = {}
        for key, row_status, school_name, school_id_govt in rows_data:
            rows[key] = SchoolMasterDataFactory(
                country=country,
                status=row_status,
                school_name=school_name,
                school_id_govt=school_id_govt,
                school_id_giga='giga-' + key,
            )
        return country, rows

    def _publish_by_country(self, country, query_params):
        url, view = sources_url((), query_params, view_name='publish-school-master-data-rows-for-country')
        return self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data=[{
                'country_id': country.id,
            }],
            view=view,
        )

    def _get_row_statuses(self, rows):
        return {key: SchoolMasterData.objects.get(id=row.id).status for key, row in rows.items()}

    def _assert_publish_counts(self, response, published_count, deleted_count):
        self.assertEqual(response.data['message'],
                         'Country record publish started. Application will be updated in minutes.')
        self.assertEqual(response.data['published_count'], published_count)
        self.assertEqual(response.data['deleted_count'], deleted_count)

    def test_publish_by_country_with_status_filter(self):
        country, rows = self._setup_country_publish_rows()

        response = self._publish_by_country(country, {
            'status__in': ','.join([
                SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
                SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
            ]),
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self._assert_publish_counts(response, published_count=2, deleted_count=0)
        self.assertEqual(self._get_row_statuses(rows), {
            'draft_primaria': SchoolMasterData.ROW_STATUS_DRAFT,
            'draft_secundaria': SchoolMasterData.ROW_STATUS_DRAFT,
            'locked_primaria': SchoolMasterData.ROW_STATUS_PUBLISHED,
            'updated_locked_secundaria': SchoolMasterData.ROW_STATUS_PUBLISHED,
            'deleted_primaria': SchoolMasterData.ROW_STATUS_DELETED,
            'updated_in_draft_primaria': SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
        })

    def test_publish_by_country_with_search(self):
        country, rows = self._setup_country_publish_rows()

        response = self._publish_by_country(country, {'search': 'WRONGGOVT'})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self._assert_publish_counts(response, published_count=2, deleted_count=1)
        self.assertEqual(self._get_row_statuses(rows), {
            'draft_primaria': SchoolMasterData.ROW_STATUS_PUBLISHED,
            'draft_secundaria': SchoolMasterData.ROW_STATUS_DRAFT,
            'locked_primaria': SchoolMasterData.ROW_STATUS_PUBLISHED,
            'updated_locked_secundaria': SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
            'deleted_primaria': SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
            'updated_in_draft_primaria': SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
        })

    def test_publish_by_country_without_filters_publishes_all_pending_rows(self):
        country, rows = self._setup_country_publish_rows()

        response = self._publish_by_country(country, {})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self._assert_publish_counts(response, published_count=4, deleted_count=1)
        self.assertEqual(self._get_row_statuses(rows), {
            'draft_primaria': SchoolMasterData.ROW_STATUS_PUBLISHED,
            'draft_secundaria': SchoolMasterData.ROW_STATUS_PUBLISHED,
            'locked_primaria': SchoolMasterData.ROW_STATUS_PUBLISHED,
            'updated_locked_secundaria': SchoolMasterData.ROW_STATUS_PUBLISHED,
            'deleted_primaria': SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
            'updated_in_draft_primaria': SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
        })

    def test_publish_by_country_with_non_publishable_status(self):
        country, rows = self._setup_country_publish_rows()
        statuses_before = self._get_row_statuses(rows)

        response = self._publish_by_country(country, {'status__in': SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self._assert_publish_counts(response, published_count=0, deleted_count=0)
        self.assertEqual(self._get_row_statuses(rows), statuses_before)

    def test_publish_by_country_with_status_and_search(self):
        country, rows = self._setup_country_publish_rows()

        response = self._publish_by_country(country, {
            'status__in': ','.join([SchoolMasterData.ROW_STATUS_DRAFT, SchoolMasterData.ROW_STATUS_DELETED]),
            'search': 'Primaria',
        })

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self._assert_publish_counts(response, published_count=1, deleted_count=1)
        self.assertEqual(self._get_row_statuses(rows), {
            'draft_primaria': SchoolMasterData.ROW_STATUS_PUBLISHED,
            'draft_secundaria': SchoolMasterData.ROW_STATUS_DRAFT,
            'locked_primaria': SchoolMasterData.ROW_STATUS_DRAFT_LOCKED,
            'updated_locked_secundaria': SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT_LOCKED,
            'deleted_primaria': SchoolMasterData.ROW_STATUS_DELETED_PUBLISHED,
            'updated_in_draft_primaria': SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT,
        })

    def test_publish_by_country_with_unsupported_query_params(self):
        # Filters the list accepts but country publish does not, and typos, must not fall back to publishing the
        # whole country
        country, rows = self._setup_country_publish_rows()
        statuses_before = self._get_row_statuses(rows)

        for query_params in (
            {'status': SchoolMasterData.ROW_STATUS_DRAFT_LOCKED},
            {'status__exact': SchoolMasterData.ROW_STATUS_DRAFT_LOCKED},
            {'status_in': SchoolMasterData.ROW_STATUS_DRAFT_LOCKED},
            {'status__in[]': SchoolMasterData.ROW_STATUS_DRAFT_LOCKED},
            {'status__in': SchoolMasterData.ROW_STATUS_DRAFT_LOCKED, 'school_name': 'Escola Primaria C'},
        ):
            with self.subTest(query_params=query_params):
                response = self._publish_by_country(country, query_params)

                self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
                self.assertEqual(self._get_row_statuses(rows), statuses_before)

    def test_publish_by_country_with_empty_query_params_matches_the_list(self):
        # The list ignores empty filter values (e.g. a search made only of spaces, which the FE sends untrimmed),
        # so country publish ignores them too and publishes exactly what the list counts
        for query_params in (
            {'search': '%20%20%20'},
            {'search': ''},
            {'status__in': ''},
        ):
            with self.subTest(query_params=query_params):
                country, rows = self._setup_country_publish_rows()

                list_url, list_view = sources_url((), dict(query_params, country_id=country.id))
                list_response = self.forced_auth_req('get', list_url, user=self.admin_user, view=list_view)
                self.assertEqual(list_response.status_code, status.HTTP_200_OK)
                self.assertEqual(list_response.data['count'], 5)

                response = self._publish_by_country(country, query_params)

                self.assertEqual(response.status_code, status.HTTP_200_OK)
                self._assert_publish_counts(response, published_count=4, deleted_count=1)
                self.assertEqual(self._get_row_statuses(rows)['updated_in_draft_primaria'],
                                 SchoolMasterData.ROW_STATUS_UPDATED_IN_DRAFT)

    def test_publish_by_country_queues_tasks_limited_to_its_rows(self):
        country, rows = self._setup_country_publish_rows()

        with patch.object(sources_tasks.handle_published_school_master_data_row, 'delay') as published_delay, \
                patch.object(sources_tasks.handle_deleted_school_master_data_row, 'delay') as deleted_delay:
            response = self._publish_by_country(country, {'search': 'WRONGGOVT'})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        published_delay.assert_called_once()
        deleted_delay.assert_called_once()

        published_kwargs = published_delay.call_args.kwargs
        self.assertEqual(published_kwargs['country_ids'], [country.id])
        self.assertEqual(published_kwargs['publish_source'], 'admin_portal_country')
        # Same timestamp for both tasks and for every row of the request
        self.assertEqual(deleted_delay.call_args.kwargs, published_kwargs)
        published_at = sources_tasks.parse_published_at(published_kwargs['published_at'])
        self.assertEqual(
            set(SchoolMasterData.objects.filter(published_at=published_at).values_list('id', flat=True)),
            {rows['draft_primaria'].id, rows['locked_primaria'].id, rows['deleted_primaria'].id},
        )

        # Running the queued tasks with those kwargs handles exactly the rows of this request
        sources_tasks.handle_published_school_master_data_row(**published_kwargs)
        sources_tasks.handle_deleted_school_master_data_row(**deleted_delay.call_args.kwargs)
        self.assertEqual(
            set(SchoolMasterData.objects.filter(country=country, is_read=True).values_list('id', flat=True)),
            {rows['draft_primaria'].id, rows['locked_primaria'].id, rows['deleted_primaria'].id},
        )

    def test_publish_by_multiple_countries_with_filters(self):
        country_one, rows_one = self._setup_country_publish_rows()
        country_two, rows_two = self._setup_country_publish_rows()
        statuses_before_one = self._get_row_statuses(rows_one)
        statuses_before_two = self._get_row_statuses(rows_two)

        url, view = sources_url(
            (), {'status__in': SchoolMasterData.ROW_STATUS_DRAFT, 'search': 'Primaria'},
            view_name='publish-school-master-data-rows-for-country')
        with patch.object(sources_tasks.handle_published_school_master_data_row, 'delay') as published_delay, \
                patch.object(sources_tasks.handle_deleted_school_master_data_row, 'delay') as deleted_delay:
            response = self.forced_auth_req(
                'put',
                url,
                user=self.admin_user,
                data=[{'country_id': country_one.id}, {'country_id': country_two.id}],
                view=view,
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self._assert_publish_counts(response, published_count=2, deleted_count=0)
        self.assertEqual(self._get_row_statuses(rows_one),
                         dict(statuses_before_one, draft_primaria=SchoolMasterData.ROW_STATUS_PUBLISHED))
        self.assertEqual(self._get_row_statuses(rows_two),
                         dict(statuses_before_two, draft_primaria=SchoolMasterData.ROW_STATUS_PUBLISHED))
        self.assertEqual(published_delay.call_args.kwargs['country_ids'], sorted([country_one.id, country_two.id]))
        deleted_delay.assert_not_called()

    def test_publish_by_country_with_dict_body_and_filters(self):
        country, rows = self._setup_country_publish_rows()
        statuses_before = self._get_row_statuses(rows)

        url, view = sources_url(
            (), {'status__in': SchoolMasterData.ROW_STATUS_DRAFT_LOCKED},
            view_name='publish-school-master-data-rows-for-country')
        response = self.forced_auth_req('put', url, user=self.admin_user, data={'country_id': country.id}, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self._assert_publish_counts(response, published_count=1, deleted_count=0)
        self.assertEqual(self._get_row_statuses(rows),
                         dict(statuses_before, locked_primaria=SchoolMasterData.ROW_STATUS_PUBLISHED))

    def test_country_publish_filters_are_a_subset_of_list_filters(self):
        # A filter the list applies but country publish does not would make "Publish all" publish more rows
        # than the publisher sees
        list_view = sources_api.SchoolMasterDataViewSet
        publish_view = sources_api.SchoolMasterDataPublishByCountryViewSet

        self.assertEqual(publish_view.search_fields, list_view.search_fields)
        self.assertTrue(set(publish_view.filter_backends).issubset(set(list_view.filter_backends)))
        for field_name, lookups in publish_view.filterset_fields.items():
            with self.subTest(field_name=field_name):
                self.assertTrue(set(lookups).issubset(set(list_view.filterset_fields.get(field_name, []))))

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

