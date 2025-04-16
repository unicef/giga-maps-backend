import os
from collections import OrderedDict
from datetime import timedelta

from django.conf import settings
from django.core.cache import cache
from django.core.management import call_command
from django.test import TestCase
from django.urls import resolve, reverse
from rest_framework import status

from proco.accounts import models as accounts_models
from proco.accounts.tests import test_utils as accounts_test_utilities
from proco.core import utils as core_utilities
from proco.custom_auth.tests import test_utils as test_utilities
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory
from proco.utils.tests import TestAPIViewSetMixin


def accounts_url(url_params, query_param, view_name='list-or-create-api-keys'):
    url = reverse('accounts:' + view_name, args=url_params)
    view = resolve(url)
    view_info = view.func

    if len(query_param) > 0:
        query_params = '?' + '&'.join([key + '=' + str(val) for key, val in query_param.items()])
        url += query_params
    return url, view, view_info


class APIsApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        api_file = os.path.join(
            settings.BASE_DIR,
            'proco/core/resources/all_apis.tsv'
        )
        args = ['--api-file', api_file]
        call_command('load_api_data', *args)

        cls.user = test_utilities.setup_admin_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list_apis_all(self):
        url, _, view = accounts_url((), {}, view_name='list-apis')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), OrderedDict)
        self.assertEqual(response_data['count'], 5)
        self.assertEqual(len(response_data['results']), 5)

    def test_list_apis_filter_on_code(self):
        url, _, view = accounts_url((), {
            'code': 'DAILY_CHECK_APP'
        }, view_name='list-apis')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), OrderedDict)
        self.assertEqual(response_data['count'], 1)
        self.assertEqual(len(response_data['results']), 1)

    def test_list_apis_filter_on_category_public(self):
        url, _, view = accounts_url((), {
            'category': 'public'
        }, view_name='list-apis')

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), OrderedDict)
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_list_apis_filter_on_category_private(self):
        url, _, view = accounts_url((), {
            'category': 'private'
        }, view_name='list-apis')

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), OrderedDict)
        self.assertEqual(response_data['count'], 3)
        self.assertEqual(len(response_data['results']), 3)


class APIKeysApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        api_file = os.path.join(
            settings.BASE_DIR,
            'proco/core/resources/all_apis.tsv'
        )
        args = ['--api-file', api_file]
        call_command('load_api_data', *args)

        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

        cls.country = CountryFactory()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list_api_keys_all_for_logged_in_user(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 0)
        self.assertEqual(len(response_data['results']), 0)

    def test_list_api_keys_all_for_read_only_user(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req('get', url, user=self.read_only_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 0)
        self.assertEqual(len(response_data['results']), 0)

    def test_list_api_keys_all_for_non_logged_in_user(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_create_api_keys_for_admin_for_public_api(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='COUNTRY').id,
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_validate_api_keys_for_admin_for_public_api(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='COUNTRY').id,
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data
        api_key = response_data['api_key']

        url, _, view = accounts_url((), {}, view_name='validate-an-api-key')

        get_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'api_id': accounts_models.API.objects.get(code='COUNTRY').id,
                'api_key': api_key,
            }
        )

        self.assertEqual(get_response.status_code, status.HTTP_200_OK)

    def test_validate_invalid_api_key_for_admin_for_public_api(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='COUNTRY').id,
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data
        api_key = response_data['api_key']

        url, _, view = accounts_url((), {}, view_name='validate-an-api-key')

        get_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'api_id': accounts_models.API.objects.get(code='COUNTRY').id,
                'api_key': api_key + 'abc',
            }
        )

        self.assertEqual(get_response.status_code, status.HTTP_404_NOT_FOUND)

    def test_create_api_keys_for_admin_for_private_api(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                'active_countries_list': [self.country.id, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_create_api_keys_for_read_only_user_for_public_api(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.read_only_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='COUNTRY').id,
                'active_countries_list': [self.country.id, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_create_api_keys_for_read_only_user_for_private_api(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.read_only_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                'active_countries_list': [self.country.id, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_approve_api_key_for_read_only_user_for_private_api_by_admin(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.read_only_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                'active_countries_list': [self.country.id, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        api_key_id = response_data['id']

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='update-and-delete-api-key')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'status': accounts_models.APIKey.APPROVED,
                'valid_to': core_utilities.get_current_datetime_object().date() + timedelta(days=30),
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_create_api_key_extension_request_for_private_api(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.read_only_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                'active_countries_list': [self.country.id, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        api_key_id = response_data['id']

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='update-and-delete-api-key')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'status': accounts_models.APIKey.APPROVED,
                'valid_to': core_utilities.get_current_datetime_object().date() + timedelta(days=30),
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='request-api-key-extension')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.read_only_user,
            data={
                'extension_valid_to': core_utilities.get_current_datetime_object().date() + timedelta(days=60),
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_approve_api_key_extension_request_for_private_api_by_admin(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.read_only_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                'active_countries_list': [self.country.id, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        api_key_id = response_data['id']

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='update-and-delete-api-key')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'status': accounts_models.APIKey.APPROVED,
                'valid_to': core_utilities.get_current_datetime_object().date() + timedelta(days=30),
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='request-api-key-extension')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.read_only_user,
            data={
                'extension_valid_to': core_utilities.get_current_datetime_object().date() + timedelta(days=60),
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='update-and-delete-api-key')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'extension_status': accounts_models.APIKey.APPROVED,
                'extension_valid_to': core_utilities.get_current_datetime_object().date() + timedelta(days=100),
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_delete_api_key_by_admin(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.read_only_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='COUNTRY').id,
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        api_key_id = response_data['id']

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='update-and-delete-api-key')

        delete_response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
        )

        self.assertEqual(delete_response.status_code, status.HTTP_204_NO_CONTENT)

    def test_delete_api_key_by_read_only_user(self):
        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.read_only_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='COUNTRY').id,
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        api_key_id = response_data['id']

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='update-and-delete-api-key')

        delete_response = self.forced_auth_req(
            'delete',
            url,
            user=self.read_only_user,
        )

        self.assertEqual(delete_response.status_code, status.HTTP_204_NO_CONTENT)


class APIKeyAPICategoryApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        api_file = os.path.join(
            settings.BASE_DIR,
            'proco/core/resources/all_apis.tsv'
        )
        args = ['--api-file', api_file]
        call_command('load_api_data', *args)

        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

        cls.country = CountryFactory()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list_api_categories_all_for_admin_user(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-api-categories')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 0)
        self.assertEqual(len(response_data['results']), 0)

    def test_list_api_categories_for_read_only_user(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-api-categories')

        response = self.forced_auth_req('get', url, user=self.read_only_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


    def test_list_api_categories_all_for_non_logged_in_user(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-api-categories')

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_create_api_categories_for_admin_for_pcdc(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-api-categories')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                "api": accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                "code": "GOVT",
                "name": "Govt",
                "is_default": True,
                "description": "To access the Govt endpoints"
            },
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_update_api_categories_for_admin_for_pcdc(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-api-categories')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                "api": accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                "code": "GOVT",
                "name": "Govt",
                "is_default": True,
                "description": "To access the Govt endpoints"
            },
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        category_id = response_data['id']

        url, _, view = accounts_url((category_id,), {},
                                    view_name='update-and-delete-api-category')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                "description": "To access the Govt endpoints"
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_create_api_categories_for_read_only_user_for_pcdc(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-api-categories')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.read_only_user,
            view=view,
            data={
                "api": accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                "code": "GOVT",
                "name": "Govt",
                "is_default": True,
                "description": "To access the Govt endpoints"
            },
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_delete_api_category_by_admin(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-api-categories')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                "api": accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                "code": "GOVT",
                "name": "Govt",
                "is_default": True,
                "description": "To access the Govt endpoints"
            },
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        api_category_id = response_data['id']

        url, _, view = accounts_url((api_category_id,), {},
                                    view_name='update-and-delete-api-category')

        delete_response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
        )

        self.assertEqual(delete_response.status_code, status.HTTP_204_NO_CONTENT)

    def test_delete_api_category_by_read_only_user(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-api-categories')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                "api": accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                "code": "GOVT",
                "name": "Govt",
                "is_default": True,
                "description": "To access the Govt endpoints"
            },
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        api_category_id = response_data['id']

        url, _, view = accounts_url((api_category_id,), {},
                                    view_name='update-and-delete-api-category')

        delete_response = self.forced_auth_req(
            'delete',
            url,
            user=self.read_only_user,
        )

        self.assertEqual(delete_response.status_code, status.HTTP_403_FORBIDDEN)

    def test_assign_api_key_category_for_pcdc_api(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-api-categories')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                "api": accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                "code": "GOVT",
                "name": "Govt",
                "is_default": True,
                "description": "To access the Govt endpoints"
            },
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        api_category_id = response_data['id']

        url, _, view = accounts_url((), {})

        response = self.forced_auth_req(
            'post',
            url,
            user=self.read_only_user,
            view=view,
            data={
                'api': accounts_models.API.objects.get(code='DAILY_CHECK_APP').id,
                'active_countries_list': [self.country.id, ]
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        api_key_id = response_data['id']

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='update-and-delete-api-key')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'status': accounts_models.APIKey.APPROVED,
                'valid_to': core_utilities.get_current_datetime_object().date() + timedelta(days=30),
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

        url, _, view = accounts_url((api_key_id,), {},
                                    view_name='api-key-api-categories-crud')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                "active_api_categories_list": [api_category_id,],
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

class NotificationsApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list_for_admin_user(self):
        url, _, view = accounts_url((), {}, view_name='list-send-notifications')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 0)
        self.assertEqual(len(response_data['results']), 0)

    def test_list_for_real_only_user(self):
        url, _, view = accounts_url((), {}, view_name='list-send-notifications')

        response = self.forced_auth_req('get', url, user=self.read_only_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_list_for_non_logged_in_user(self):
        url, _, view = accounts_url((), {}, view_name='list-send-notifications')

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_create_notification_by_admin(self):
        url, _, view = accounts_url((), {}, view_name='list-send-notifications')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'type': accounts_models.Message.TYPE_NOTIFICATION,
                'recipient': [self.read_only_user.id, ],
                'subject_text': 'Notification subject',
                'message_text': 'Notification Message',
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_create_notification_by_admin_single_recipient(self):
        url, _, view = accounts_url((), {}, view_name='list-send-notifications')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'type': accounts_models.Message.TYPE_NOTIFICATION,
                'recipient': self.read_only_user.id,
                'subject_text': 'Notification subject',
                'message_text': 'Notification Message',
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_create_notification_by_admin_invalid_recipient(self):
        url, _, view = accounts_url((), {}, view_name='list-send-notifications')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'type': accounts_models.Message.TYPE_NOTIFICATION,
                'recipient': '123456',
                'subject_text': 'Notification subject',
                'message_text': 'Notification Message',
            }
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class AppStaticConfigurationsApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    def test_get(self):
        url, _, view = accounts_url((), {}, view_name='get-app-static-configurations')

        response = self.forced_auth_req('get', url, _, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertIn('API_CATEGORY_CHOICES', response_data)


class TimePlayerApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = {'default', settings.READ_ONLY_DB_KEY,}

    @classmethod
    def setUpTestData(cls):
        args = ['--delete_data_sources', '--update_data_sources', '--update_data_layers']
        call_command('load_system_data_layers', *args)

        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

    def test_get_invalid_layer_id(self):
        url, _, view = accounts_url((), {
            'layer_id': 123,
            'country_id': 123,
        }, view_name='get-time-player-data-v2')

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
                'code': 'TEST_DATA_LAYER_{0}'.format(pcdc_data_source.id),
                'description': 'Test data layer 3 description',
                'version': '1.0.0',
                'type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'data_sources_list': [pcdc_data_source.id, ],
                'data_source_column': pcdc_data_source.column_config[0],
                'data_source_column_function': {},
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

        url, _, view = accounts_url((), {
            'layer_id': layer_id,
            'country_id': 123,
            'z': '2',
            'x': '1',
            'y': '2.mvt',
        }, view_name='get-time-player-data-v2')

        response = self.forced_auth_req('get', url, _, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)


class DataSourceApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        args = ['--delete_data_sources', '--update_data_sources', '--update_data_layers']
        call_command('load_system_data_layers', *args)

        cls.user = test_utilities.setup_admin_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list_data_sources_all(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-sources')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), OrderedDict)
        self.assertEqual(response_data['count'], 3)
        self.assertEqual(len(response_data['results']), 3)

    def test_list_data_sources_filter_on_status_published(self):
        url, _, view = accounts_url((), {
            'status': 'PUBLISHED'
        }, view_name='list-or-create-data-sources')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), OrderedDict)
        self.assertEqual(response_data['count'], 3)
        self.assertEqual(len(response_data['results']), 3)

    def test_list_data_sources_filter_on_status_draft(self):
        url, _, view = accounts_url((), {
            'status': 'DRAFT'
        }, view_name='list-or-create-data-sources')

        response = self.forced_auth_req('get', url, user=self.user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), OrderedDict)
        self.assertEqual(response_data['count'], 0)
        self.assertEqual(len(response_data['results']), 0)

    def test_list_data_sources_filter_on_status_published_without_auth(self):
        url, _, view = accounts_url((), {
            'status': 'PUBLISHED'
        }, view_name='list-or-create-data-sources')

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_delete_data_source_by_admin(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-sources')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.user,
            view=view,
            data={
                'name': 'Testing data source name',
                'description': 'Testing data source description',
                'version': '1.0.0',
                'data_source_type': 'SCHOOL_MASTER',
                'request_config': {},
                'column_config': {},
                'status': 'DRAFT',
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        source_id = response_data['id']

        url, _, view = accounts_url((source_id,), {},
                                    view_name='update-or-delete-data-source')

        delete_response = self.forced_auth_req(
            'delete',
            url,
            user=self.user,
        )

        self.assertEqual(delete_response.status_code, status.HTTP_204_NO_CONTENT)

    def test_publish_data_source_by_admin(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-sources')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.user,
            view=view,
            data={
                'name': 'Testing data source name for publish',
                'description': 'Testing data source description for publish',
                'version': '1.0.0',
                'data_source_type': 'SCHOOL_MASTER',
                'request_config': {},
                'column_config': {},
                'status': 'READY_TO_PUBLISH',
            }
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        source_id = response_data['id']

        url, _, view = accounts_url((source_id,), {},
                                    view_name='publish-data-source')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.user,
            data={
                'status': 'PUBLISHED',
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)


class DataLayerApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        args = ['--delete_data_sources', '--update_data_sources', '--update_data_layers']
        call_command('load_system_data_layers', *args)

        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_list_data_layers_all(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_list_data_layers_filter_on_status_published(self):
        url, _, view = accounts_url((), {
            'status': 'PUBLISHED'
        }, view_name='list-or-create-data-layers')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_list_published_data_layers_for_admin(self):
        url, _, view = accounts_url(('PUBLISHED',), {
        }, view_name='list-published-data-layers')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_list_published_data_layers_without_auth(self):
        url, _, view = accounts_url(('PUBLISHED',), {
        }, view_name='list-published-data-layers')

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 2)
        self.assertEqual(len(response_data['results']), 2)

    def test_list_published_data_layers_for_country(self):
        url, _, view = accounts_url(('PUBLISHED',), {
            'country_id': 123456789,
        }, view_name='list-published-data-layers')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 0)
        self.assertEqual(len(response_data['results']), 0)

    def test_list_data_layers_filter_on_status_draft(self):
        url, _, view = accounts_url((), {
            'status': 'DRAFT'
        }, view_name='list-or-create-data-layers')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 0)
        self.assertEqual(len(response_data['results']), 0)

    def test_list_data_layers_filter_on_status_published_without_auth(self):
        url, _, view = accounts_url((), {
            'status': 'PUBLISHED'
        }, view_name='list-or-create-data-layers')

        response = self.forced_auth_req('get', url, user=None, view=view)

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_create_data_layer_by_admin(self):
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
                'name': 'Test data layer',
                'code': 'TEST_DATA_LAYER_{0}'.format(pcdc_data_source.id),
                'description': 'Test data layer description',
                'version': '1.0.0',
                'type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'data_sources_list': [pcdc_data_source.id, ],
                'data_source_column': pcdc_data_source.column_config[0],
                'data_source_column_function': {},
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

        print(response.__dict__)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_publish_in_draft_data_layer_by_admin(self):
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
                'name': 'Test data layer 2',
                'code': 'TEST_DATA_LAYER_{0}'.format(pcdc_data_source.id),
                'description': 'Test data layer 2 description',
                'version': '1.0.0',
                'type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'data_sources_list': [pcdc_data_source.id, ],
                'data_source_column': pcdc_data_source.column_config[0],
                'data_source_column_function': {},
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
                                    view_name='publish-data-layer')

        put_response = self.forced_auth_req(
            'put',
            url,
            user=self.admin_user,
            data={
                'status': 'PUBLISHED',
            }
        )

        self.assertEqual(put_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_delete_in_draft_data_layer_by_admin(self):
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
                'name': 'Test data layer 2',
                'code': 'TEST_DATA_LAYER_{0}'.format(pcdc_data_source.id),
                'description': 'Test data layer 2 description',
                'version': '1.0.0',
                'type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'data_sources_list': [pcdc_data_source.id, ],
                'data_source_column': pcdc_data_source.column_config[0],
                'data_source_column_function': {},
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
            'delete',
            url,
            user=self.admin_user,
        )

        self.assertEqual(put_response.status_code, status.HTTP_204_NO_CONTENT)

    def test_publish_in_ready_data_layer_by_admin(self):
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
                'code': 'TEST_DATA_LAYER_{0}'.format(pcdc_data_source.id),
                'description': 'Test data layer 3 description',
                'version': '1.0.0',
                'type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'data_sources_list': [pcdc_data_source.id, ],
                'data_source_column': pcdc_data_source.column_config[0],
                'data_source_column_function': {},
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

    def test_preview_pcdc_data_layer_by_admin(self):
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
                'name': 'Test data layer',
                'code': 'TEST_DATA_LAYER_{0}'.format(pcdc_data_source.id),
                'description': 'Test data layer description',
                'version': '1.0.0',
                'type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'data_sources_list': [pcdc_data_source.id, ],
                'data_source_column': pcdc_data_source.column_config[0],
                'data_source_column_function': {},
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
                                    view_name='preview-data-layer')

        put_response = self.forced_auth_req(
            'get',
            url,
            user=self.admin_user,
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_preview_qos_data_layer_by_admin(self):
        qos_data_source = accounts_models.DataSource.objects.filter(
            data_source_type=accounts_models.DataSource.DATA_SOURCE_TYPE_QOS,
        ).first()

        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data={
                'icon': '<icon>',
                'name': 'Test data layer',
                'code': 'TEST_DATA_LAYER_{0}'.format(qos_data_source.id),
                'description': 'Test data layer description',
                'version': '1.0.0',
                'type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'data_sources_list': [qos_data_source.id, ],
                'data_source_column': qos_data_source.column_config[0],
                'data_source_column_function': {},
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
                                    view_name='preview-data-layer')

        put_response = self.forced_auth_req(
            'get',
            url,
            user=self.admin_user,
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)

    def test_preview_static_data_layer_by_admin(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=accounts_test_utilities.static_coverage_layer_data()
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response_data = response.data

        layer_id = response_data['id']

        url, _, view = accounts_url((layer_id,), {},
                                    view_name='preview-data-layer')

        put_response = self.forced_auth_req(
            'get',
            url,
            user=self.admin_user,
        )

        self.assertEqual(put_response.status_code, status.HTTP_200_OK)


class LogActionApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['default', ]

    @classmethod
    def setUpTestData(cls):
        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

    def test_list_for_admin_user(self):
        url, _, view = accounts_url((), {}, view_name='list-recent-action-log')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response_data = response.data

        self.assertEqual(type(response_data), dict)
        self.assertEqual(response_data['count'], 0)
        self.assertEqual(len(response_data['results']), 0)

    def test_list_for_readonly_user(self):
        url, _, view = accounts_url((), {}, view_name='list-recent-action-log')

        response = self.forced_auth_req('get', url, user=self.read_only_user, view=view)

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


class DataLayerMapApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = {'default', settings.READ_ONLY_DB_KEY,}

    @classmethod
    def setUpTestData(cls):
        args = ['--delete_data_sources', '--update_data_sources', '--update_data_layers']
        call_command('load_system_data_layers', *args)

        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

        cls.country = CountryFactory()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_static_data_layer_map_country_view(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=accounts_test_utilities.static_coverage_layer_data()
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

        url, view, view_info = accounts_url(
            (layer_id,),
            {
                'country_id': self.country.id,
                'z': '8',
                'x': '82',
                'y': '114.mvt'
            },
            view_name='map-data-layer'
        )

        response = self.forced_auth_req(
            'get',
            url,
            view=view,
            view_info=view_info,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_static_data_layer_map_school_view(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=accounts_test_utilities.static_coverage_layer_data()
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

        url, view, view_info = accounts_url(
            (layer_id,),
            {
                'country_id': self.country.id,
                'school_id': '1234567',
                'z': '8',
                'x': '82',
                'y': '114.mvt'
            },
            view_name='map-data-layer'
        )

        response = self.forced_auth_req(
            'get',
            url,
            view=view,
            view_info=view_info,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_live_data_layer_map_country_view(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=accounts_test_utilities.live_download_layer_data_pcdc(),
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

        url, view, view_info = accounts_url(
            (layer_id,),
            {
                'country_id': self.country.id,
                'benchmark': 'global',
                'start_date': '24-06-2024',
                'end_date': '30-06-2024',
                'is_weekly': 'true',
                'z': '8',
                'x': '82',
                'y': '114.mvt'
            },
            view_name='map-data-layer'
        )

        response = self.forced_auth_req(
            'get',
            url,
            view=view,
            view_info=view_info,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_live_data_layer_map_school_view(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=accounts_test_utilities.live_download_layer_data_pcdc(),
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

        url, view, view_info = accounts_url(
            (layer_id,),
            {
                'country_id': self.country.id,
                'school_id': '1234568',
                'benchmark': 'global',
                'start_date': '24-06-2024',
                'end_date': '30-06-2024',
                'is_weekly': 'true',
                'z': '8',
                'x': '82',
                'y': '114.mvt'
            },
            view_name='map-data-layer'
        )

        response = self.forced_auth_req(
            'get',
            url,
            view=view,
            view_info=view_info,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)


class DataLayerInfoApiTestCase(TestAPIViewSetMixin, TestCase):
    databases = {'default', settings.READ_ONLY_DB_KEY,}

    @classmethod
    def setUpTestData(cls):
        args = ['--delete_data_sources', '--update_data_sources', '--update_data_layers']
        call_command('load_system_data_layers', *args)

        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

        cls.country = CountryFactory()
        cls.school = SchoolFactory()

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_static_data_layer_map_country_view(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=accounts_test_utilities.static_coverage_layer_data()
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

        url, view, view_info = accounts_url(
            (layer_id,),
            {
                'country_id': self.country.id,
            },
            view_name='info-data-layer'
        )

        response = self.forced_auth_req(
            'get',
            url,
            view=view,
            view_info=view_info,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_static_data_layer_map_school_view(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=accounts_test_utilities.static_coverage_layer_data()
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

        url, view, view_info = accounts_url(
            (layer_id,),
            {
                'country_id': self.country.id,
                'school_id': '1234567',
            },
            view_name='info-data-layer'
        )

        response = self.forced_auth_req(
            'get',
            url,
            view=view,
            view_info=view_info,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_live_data_layer_map_country_view(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=accounts_test_utilities.live_download_layer_data_pcdc(),
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

        url, view, view_info = accounts_url(
            (layer_id,),
            {
                'country_id': self.country.id,
                'benchmark': 'global',
                'start_date': '24-06-2024',
                'end_date': '30-06-2024',
                'is_weekly': 'true',
            },
            view_name='info-data-layer'
        )

        response = self.forced_auth_req(
            'get',
            url,
            view=view,
            view_info=view_info,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_live_data_layer_map_school_view(self):
        url, _, view = accounts_url((), {}, view_name='list-or-create-data-layers')

        response = self.forced_auth_req(
            'post',
            url,
            user=self.admin_user,
            view=view,
            data=accounts_test_utilities.live_download_layer_data_pcdc(),
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

        url, view, view_info = accounts_url(
            (layer_id,),
            {
                'country_id': self.country.id,
                'school_id': '123456',
                'benchmark': 'global',
                'start_date': '24-06-2024',
                'end_date': '30-06-2024',
                'is_weekly': 'true',
            },
            view_name='info-data-layer'
        )

        response = self.forced_auth_req(
            'get',
            url,
            view=view,
            view_info=view_info,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)


class InvalidateCacheApiTestCase(TestAPIViewSetMixin, TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.admin_user = test_utilities.setup_admin_user_by_role()
        cls.read_only_user = test_utilities.setup_read_only_user_by_role()

    def test_hard_cache_clean_for_admin(self):
        url, view, view_info = accounts_url((), {'hard': 'true'}, view_name='admin-invalidate-cache')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view, view_info=view_info)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_soft_cache_clean_for_admin(self):
        url, view, view_info = accounts_url((), {'hard': 'false'}, view_name='admin-invalidate-cache')

        response = self.forced_auth_req('get', url, user=self.admin_user, view=view, view_info=view_info)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_hard_cache_clean_for_all_pattern_for_admin(self):
        url, view, view_info = accounts_url((), {'hard': 'true'},
                                            view_name='admin-invalidate-cache-based-on-patterns')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
            view=view,
            view_info=view_info,
            data={
                'key': 'all'
            }
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_soft_cache_clean_for_all_pattern_admin(self):
        url, view, view_info = accounts_url((), {'hard': 'false'},
                                            view_name='admin-invalidate-cache-based-on-patterns')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
            view=view,
            view_info=view_info,
            data={
                'key': 'all'
            }
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_hard_cache_clean_for_country_for_admin(self):
        url, view, view_info = accounts_url((), {'hard': 'true'},
                                            view_name='admin-invalidate-cache-based-on-patterns')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
            view=view,
            view_info=view_info,
            data={
                'key': 'country',
                'id': 12345,
                'code': 'C_CODE'
            }
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_hard_cache_clean_for_layer_for_admin(self):
        url, view, view_info = accounts_url((), {'hard': 'true'},
                                            view_name='admin-invalidate-cache-based-on-patterns')

        response = self.forced_auth_req(
            'delete',
            url,
            user=self.admin_user,
            view=view,
            view_info=view_info,
            data={
                'key': 'layer',
                'id': 12345,
            }
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
