import uuid
from datetime import datetime

import pytz
from django.core.cache import cache
from django.test import TestCase
from django.urls import reverse
from rest_framework import status

from proco.background.models import BackgroundTask
from proco.custom_auth.tests import test_utils as test_utilities
from proco.utils.tests import TestAPIViewSetMixin


class BackgroundTaskTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'background:'
    databases = {'default', }

    @classmethod
    def setUpTestData(cls):
        cls.user = test_utilities.setup_admin_user_by_role()

        cls.data = {
            'task_id': str(uuid.uuid4()),
            'status': 'running',
            'log': '',
            'completed_at': datetime.now(pytz.timezone('Africa/Lagos'))
        }

        cls.task = BackgroundTask.objects.create(**cls.data)
        cls.delete_data = {'task_id': [cls.task.task_id]}

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_retrieve(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + 'update-retrieve-backgroundtask', args=(self.task.task_id,)),
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_destroy(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + 'list-destroy-backgroundtask'),
            data=self.delete_data,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)


class BackgroundTaskHistoryTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'background:'
    databases = {'default', }

    def setUp(self):
        self.user = test_utilities.setup_admin_user_by_role()

        self.data = {
            'task_id': '8303395e-e8c0-4e72-afb8-35f3a53d01d7',
            'status': 'running',
            'log': '',
            'completed_at': '2024-03-11 06:50:12'
        }
        self.task = BackgroundTask.objects.create(**self.data)
        return super().setUp()

    def test_list(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + 'background-task-history', args=(self.task.task_id,)),
            user=self.user)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
