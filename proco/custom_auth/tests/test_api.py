from django.core.cache import cache
from django.test import TestCase
from django.urls import resolve, reverse
from rest_framework import status

from proco.custom_auth import models as auth_models
from proco.custom_auth.tests import test_utils as test_utilities
from proco.utils.tests import TestAPIViewSetMixin


def custom_auth_url(url_params, query_param, view_name='create-and-list-users'):
    url = reverse('custom_auth:' + view_name, args=url_params)
    view_info = resolve(url).func

    if len(query_param) > 0:
        query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
        url += query_params
    return url, view_info


class UserApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default']

    @classmethod
    def setUpTestData(cls):
        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list_for_admin_user(self):
        url, view = custom_auth_url((), {})

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_list_for_read_only_user(self):
        url, view = custom_auth_url((), {})

        response = self.forced_auth_req('get', url, user=self.read_only_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_list_for_non_logged_in_user(self):
        url, view = custom_auth_url((), {})

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_get_for_admin_user(self):
        url, view = custom_auth_url((), {}, view_name='get-and-create-self-user')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertIn('id', response_data)
        self.assertEqual(response_data['id'], self.admin_user.id)

    def test_create_user(self):
        user_payload = {
            'first_name': 'UserFName',
            'last_name': 'UserLName',
            'username': 'test.read_only1@test.com',
            'email': 'test.read_only1@test.com',
            'role': auth_models.Role.objects.filter(name=auth_models.Role.SYSTEM_ROLE_NAME_READ_ONLY).first().id,
        }

        url, view = custom_auth_url((), {}, view_name='get-and-create-self-user')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=user_payload
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_create_user_with_existing_email(self):
        user_payload = {
            'first_name': 'UserFName',
            'last_name': 'UserLName',
            'username': 'test.read_only1@test.com',
            'email': 'test.read_only1@test.com',
            'role': auth_models.Role.objects.filter(name=auth_models.Role.SYSTEM_ROLE_NAME_READ_ONLY).first().id,
        }

        url, view = custom_auth_url((), {}, view_name='get-and-create-self-user')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=user_payload
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=user_payload
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_create_user_with_name_as_single_string(self):
        user_payload = {
            'name': 'UserFName1 UserLName1',
            'username': 'test.read_only11@test.com',
            'email': 'test.read_only11@test.com',
            'role': auth_models.Role.objects.filter(name=auth_models.Role.SYSTEM_ROLE_NAME_READ_ONLY).first().id,
        }

        url, view = custom_auth_url((), {}, view_name='get-and-create-self-user')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=user_payload
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_create_user_with_emails(self):
        user_payload = {
            'name': 'UserFName1 UserLName1',
            'username': 'test.read_only11@test.com',
            'emails': ['test.read_only11@test.com', ],
            'role': auth_models.Role.objects.filter(name=auth_models.Role.SYSTEM_ROLE_NAME_READ_ONLY).first().id,
        }

        url, view = custom_auth_url((), {}, view_name='get-and-create-self-user')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=user_payload
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_update_user(self):
        user_payload = {
            'first_name': 'UserFName',
            'last_name': 'UserLName',
            'username': 'test.read_only1@test.com',
            'email': 'test.read_only1@test.com',
            'role': auth_models.Role.objects.filter(name=auth_models.Role.SYSTEM_ROLE_NAME_READ_ONLY).first().id,
        }

        url, view = custom_auth_url((), {}, view_name='get-and-create-self-user')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=user_payload
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        user_id = response.data['id']

        url, view = custom_auth_url((user_id,), {},
                                    view_name='user-details')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'first_name': 'UserFNameUpdated',
                'last_name': 'UserLNameUpdated',
                'role': auth_models.Role.objects.filter(name=auth_models.Role.SYSTEM_ROLE_NAME_ADMIN).first().id,
            },
        )

        self.assertEqual(update_response.status_code, status.HTTP_200_OK)

    def test_update_user_to_same_role(self):
        user_payload = {
            'first_name': 'UserFName',
            'last_name': 'UserLName',
            'username': 'test.read_only1@test.com',
            'email': 'test.read_only1@test.com',
            'role': auth_models.Role.objects.filter(name=auth_models.Role.SYSTEM_ROLE_NAME_READ_ONLY).first().id,
        }

        url, view = custom_auth_url((), {}, view_name='get-and-create-self-user')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=user_payload
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        user_id = response.data['id']

        url, view = custom_auth_url((user_id,), {},
                                    view_name='user-details')

        update_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'role': auth_models.Role.objects.filter(name=auth_models.Role.SYSTEM_ROLE_NAME_READ_ONLY).first().id,
            },
        )

        self.assertEqual(update_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_delete_custom_role_by_admin(self):
        url, view = custom_auth_url((), {}, view_name='create-and-list-roles')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'name': 'CUSTOM_ROLE_FOR_DELETE',
                'category': auth_models.Role.ROLE_CATEGORY_CUSTOM,
                'description': 'Dummy role description',
                'permission_slugs': [auth_models.RolePermission.CAN_VIEW_COUNTRY, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        role_id = response.data['id']

        url, view = custom_auth_url((role_id,), {},
                                    view_name='get-update-and-delete-role')

        delete_response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
        )

        self.assertEqual(delete_response.status_code, status.HTTP_204_NO_CONTENT)

    def test_delete_system_role_by_admin(self):
        url, view = custom_auth_url((self.read_only_user.get_roles().id,), {},
                                    view_name='get-update-and-delete-role')

        delete_response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
        )

        self.assertEqual(delete_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_update_custom_role_by_admin(self):
        url, view = custom_auth_url((), {}, view_name='create-and-list-roles')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'name': 'CUSTOM_ROLE_FOR_UPDATE',
                'category': auth_models.Role.ROLE_CATEGORY_CUSTOM,
                'description': 'Dummy role description',
                'permission_slugs': [auth_models.RolePermission.CAN_VIEW_COUNTRY, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        role_id = response.data['id']

        url, view = custom_auth_url((role_id,), {},
                                    view_name='get-update-and-delete-role')

        delete_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'name': 'CUSTOM_ROLE_FOR_UPDATE_1',
                'description': 'Dummy role description updated',
                'permission_slugs': [auth_models.RolePermission.CAN_VIEW_COUNTRY,
                                     auth_models.RolePermission.CAN_VIEW_SCHOOL]
            }
        )

        self.assertEqual(delete_response.status_code, status.HTTP_200_OK)

    def test_update_custom_role_by_admin_with_duplicate_name(self):
        url, view = custom_auth_url((), {}, view_name='create-and-list-roles')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'name': 'CUSTOM_ROLE_FOR_UPDATE',
                'category': auth_models.Role.ROLE_CATEGORY_CUSTOM,
                'description': 'Dummy role description',
                'permission_slugs': [auth_models.RolePermission.CAN_VIEW_COUNTRY, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        role_id = response.data['id']

        url, view = custom_auth_url((role_id,), {},
                                    view_name='get-update-and-delete-role')

        delete_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'name': self.read_only_user.role(),
                'description': 'Dummy role description updated',
                'permission_slugs': [auth_models.RolePermission.CAN_VIEW_COUNTRY,
                                     auth_models.RolePermission.CAN_VIEW_SCHOOL]
            }
        )

        self.assertEqual(delete_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_update_custom_role_by_admin_with_system_role_name(self):
        url, view = custom_auth_url((), {}, view_name='create-and-list-roles')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'name': 'CUSTOM_ROLE_FOR_UPDATE',
                'category': auth_models.Role.ROLE_CATEGORY_CUSTOM,
                'description': 'Dummy role description',
                'permission_slugs': [auth_models.RolePermission.CAN_VIEW_COUNTRY, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        role_id = response.data['id']

        url, view = custom_auth_url((role_id,), {},
                                    view_name='get-update-and-delete-role')

        delete_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'name': auth_models.Role.SYSTEM_ROLE_NAME_ADMIN,
                'description': 'Dummy role description updated',
                'permission_slugs': [auth_models.RolePermission.CAN_VIEW_COUNTRY,
                                     auth_models.RolePermission.CAN_VIEW_SCHOOL]
            }
        )

        self.assertEqual(delete_response.status_code, status.HTTP_400_BAD_REQUEST)
