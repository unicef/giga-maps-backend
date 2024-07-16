from django.core.cache import cache
from django.test import TestCase
from django.urls import reverse
from rest_framework import status

from proco.about_us.models import AboutUs, SliderImage
from proco.custom_auth.tests import test_utils as test_utilities
from proco.utils.tests import TestAPIViewSetMixin


class SlideImageAPITestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'about_us:'
    databases = {'read_only_database', 'default'}

    @classmethod
    def setUpTestData(cls):
        # self.databases = 'default'
        cls.email = 'test@test.com'
        cls.password = 'SomeRandomPass96'
        cls.user = test_utilities.setup_admin_user_by_role()

        cls.data = {'name': 'abc'}
        cls.slide = SliderImage.objects.create(**cls.data)
        cls.delete_data = {"id": [cls.slide.id]}
        cls.update_data = {'name': 'abcxyz'}

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_slide_add(self):
        response = self.forced_auth_req(
            'post',
            reverse(self.base_view + "list_or_delete_image"),
            data=self.data,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_slide_update(self):
        response = self.forced_auth_req(
            'put',
            reverse(self.base_view + "retrieve_and_update_image", args=(self.slide.id,)),
            data=self.update_data,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_slide_retrieve(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + "retrieve_and_update_image", args=(self.slide.id,)),
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_slide_destroy(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + "list_or_delete_image"),
            data=self.delete_data,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)


class AboutUsAPITestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'about_us:'
    databases = {'default', 'read_only_database'}

    def setUp(self):
        self.email = 'test@test.com'
        self.password = 'SomeRandomPass96'
        self.user = test_utilities.setup_admin_user_by_role()

        self.about_us_data = {
            "text": [],
            "cta": [],
            "content": [
                {
                    "text": [
                        "Request access to school location and real-time connectivity data."
                    ],
                    "image": "images/63c814e0-7925-4f81-b696-7c4f9807345b.png",
                    "title": "Data downloads & API"
                },
                {
                    "text": [
                        "Explore and contribute the open-source code of GigaMaps webapp."
                    ],
                    "image": "images/3012b06d-b278-4d6b-ad65-f0efc09d6530.png",
                    "title": "Open-source code"
                }
            ],
            "title": None,
            "image": None,
            "type": "resources",
            "status": True,
            "order": 1
        }
        self.about_us_id = AboutUs.objects.create(**self.about_us_data)
        self.delete_data = {"id": [self.about_us_id.id]}

        self.about_data = [self.about_us_data]
        self.about_update_data = [
            {
                "id": self.about_us_id.id,
                "text": ['xyz'],
                "cta": [],
                "content": [
                    {
                        "text": [
                            "Request access to school location and real-time connectivity data."
                        ],
                        "image": "images/63c814e0-7925-4f81-b696-7c4f9807345b.png",
                        "title": "Data downloads & API"
                    },
                    {
                        "text": [
                            "Explore and contribute the open-source code of GigaMaps webapp."
                        ],
                        "image": "images/3012b06d-b278-4d6b-ad65-f0efc09d6530.png",
                        "title": "Open-source code"
                    }
                ],
                "title": None,
                "image": None,
                "type": "resources",
                "status": True,
                "order": 1
            }]

        return super().setUp()

    def test_list(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + "list_about_us"),
            user=self.user)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_create(self):
        response = self.forced_auth_req(
            'post',
            reverse(self.base_view + "retrieve_delete_create_update_about_us"),
            data=self.about_data,
            user=self.user)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_update(self):
        response = self.forced_auth_req(
            'put',
            reverse(self.base_view + "retrieve_delete_create_update_about_us"),
            data=self.about_update_data,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_retrieve(self):
        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + "retrieve_delete_create_update_about_us"),
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_destroy(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + "retrieve_delete_create_update_about_us"),
            data=self.delete_data,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)
