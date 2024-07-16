import traceback

from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from django.core.cache import cache
from datetime import datetime
import pytz, uuid

from proco.background.models import BackgroundTask
from proco.custom_auth import models as auth_models
from proco.utils.tests import TestAPIViewSetMixin


class BackgroundTaskTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'background:'
    databases = {'read_only_database', 'default'}

    @classmethod
    def setUpTestData(cls):
        # self.databases = 'default'
        cls.email = 'test@test.com'
        cls.password = 'SomeRandomPass96'
        cls.user = auth_models.ApplicationUser.objects.create_user(username=cls.email, password=cls.password)

        cls.role = auth_models.Role.objects.create(name='Admin', category='system')
        cls.role_permission = auth_models.UserRoleRelationship.objects.create(user=cls.user, role=cls.role)

        cls.data = {'task_id': str(uuid.uuid4()), 'status': 'running', 'log': "",
                    'completed_at': datetime.now(pytz.timezone('Africa/Lagos'))}

        cls.task = BackgroundTask.objects.create(**cls.data)
        cls.delete_data = {"task_id": [cls.task.task_id]}

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_retrive(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + "update_or_retrieve_backgroundtask", args=(self.task.task_id,)),
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_destroy(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + "list_or_destroy_backgroundtask"),
            data=self.delete_data,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)


class BackgroundTaskHistoryTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'background:'
    databases = {'default', 'read_only_database'}

    def setUp(self):
        self.email = 'test@test.com'
        self.password = 'SomeRandomPass96'
        self.user = auth_models.ApplicationUser.objects.create_user(username=self.email, password=self.password)

        self.data = {'task_id': '8303395e-e8c0-4e72-afb8-35f3a53d01d7', 'status': 'running', 'log': "",
                     'completed_at': '2024-03-11 06:50:12'}
        self.task = BackgroundTask.objects.create(**self.data)
        return super().setUp()

    def test_list(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + "background_task_history", args=(self.task.task_id,)),
            user=self.user)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)
