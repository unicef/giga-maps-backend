from django.core.cache import cache
from django.test import TestCase
from django.urls import reverse
from rest_framework import status

from proco.background.models import BackgroundTask
from proco.background.tests.factories import BackgroundTaskFactory
from proco.custom_auth.tests import test_utils as test_utilities
from proco.utils.tests import TestAPIViewSetMixin


class BackgroundTaskTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'background:'
    databases = {'default', }

    @classmethod
    def setUpTestData(cls):
        cls.user = test_utilities.setup_admin_user_by_role()
        cls.task = BackgroundTaskFactory(status=BackgroundTask.STATUSES.running)

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
            data={'task_id': [self.task.task_id]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)


class BackgroundTaskHistoryTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'background:'
    databases = {'default', }

    def setUp(self):
        self.user = test_utilities.setup_admin_user_by_role()
        self.task = BackgroundTaskFactory(status=BackgroundTask.STATUSES.running)
        return super().setUp()

    def test_list(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + 'list-destroy-backgroundtask'),
            data={'task_id': [self.task.task_id]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + 'background-task-history', args=(self.task.task_id,)),
            user=self.user)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
