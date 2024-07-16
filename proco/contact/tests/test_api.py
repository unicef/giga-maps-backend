from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from django.core.cache import cache

from proco.contact.models import ContactMessage
from proco.utils.tests import TestAPIViewSetMixin
from proco.custom_auth import models as auth_models


class ContactAPITestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'contact:'
    databases = {'default', }

    @classmethod
    def setUpTestData(cls):
        cls.email = 'test@test.com'
        cls.password = 'SomeRandomPass96'
        cls.user = auth_models.ApplicationUser.objects.create_user(username=cls.email, password=cls.password)

        cls.role = auth_models.Role.objects.create(name='Admin', category='system')
        cls.role_permission = auth_models.UserRoleRelationship.objects.create(user=cls.user, role=cls.role)

        cls.data = {
            'full_name': 'Test Contact Full Name',
            'organisation': 'Test Contact Organisation',
            'purpose': 'Test Contact Purpose',
            'message': 'Test Contact Message',
        }
        if ContactMessage.objects.filter(full_name=cls.data['full_name']).exists():
            cls.contact = ContactMessage.objects.get(full_name=cls.data['full_name'])
        else:
            cls.contact = ContactMessage.objects.create(**cls.data)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_retrieve(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + 'retrieve-contact', args=(self.contact.id,)),
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_retrieve_invalid_id(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + 'retrieve-contact', args=(123456789,)),
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_retrieve_invalid_string_id(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + 'retrieve-contact', args=('12347654',)),
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_destroy(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + 'list-or-delete-contact'),
            data={'id': [self.contact.id]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_destroy_invalid_id(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + 'list-or-delete-contact'),
            data={'id': [123456789]},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_destroy_empty_id_list(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + 'list-or-delete-contact'),
            data={'id': ['']},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_destroy_empty_id(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + 'list-or-delete-contact'),
            data={'id': []},
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_list(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + 'list-or-delete-contact'),
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_list_unauthorized_user(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + 'list-or-delete-contact'),
            user=None,
        )
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_create(self):
        response = self.forced_auth_req(
            'post',
            reverse(self.base_view + 'create-contact'),
            user=self.user,
            data=self.data
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_create_unauthorized_user(self):
        response = self.forced_auth_req(
            'post',
            reverse(self.base_view + 'create-contact'),
            user=None,
            data=self.data
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
