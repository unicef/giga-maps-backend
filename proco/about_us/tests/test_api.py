import shutil
import tempfile

from django.core.cache import cache
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.urls import reverse

from rest_framework import status

from proco.about_us.models import AboutUs, SliderImage
from proco.custom_auth.tests import test_utils as test_utilities
from proco.utils.tests import TestAPIViewSetMixin

# Smallest payloads that carry a recognisable container signature. The uploader validates on
# extension rather than content, so these only need to be non-empty and correctly named.
MP4_BYTES = b'\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2avc1mp41' + b'\x00' * 32
WEBM_BYTES = b'\x1a\x45\xdf\xa3' + b'\x00' * 32


class SlideImageAPITestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'about_us:'
    databases = {'default', }

    @classmethod
    def setUpTestData(cls):
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


class SlideImageUploadTestCase(TestAPIViewSetMixin, TestCase):
    """
    SlideImageUploadTestCase
        Covers the media types accepted by the slide uploader. The landing page carries short
        videos alongside stills, so mp4/webm must upload through the same endpoint as images.
    """

    base_view = 'about_us:'
    databases = {'default', }

    @classmethod
    def setUpTestData(cls):
        cls.user = test_utilities.setup_admin_user_by_role()

    def setUp(self):
        cache.clear()
        self.media_root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.media_root, True)
        super().setUp()

    def _upload(self, filename, content, content_type):
        upload = SimpleUploadedFile(filename, content, content_type=content_type)
        with override_settings(MEDIA_ROOT=self.media_root):
            return self.forced_auth_req(
                'post',
                reverse(self.base_view + 'list_or_delete_image'),
                data={'name': filename, 'image': upload},
                user=self.user,
                request_format='multipart',
            )

    def test_slide_add_mp4(self):
        response = self._upload('promo.mp4', MP4_BYTES, 'video/mp4')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['image'].endswith('.mp4'))

    def test_slide_add_webm(self):
        response = self._upload('promo.webm', WEBM_BYTES, 'video/webm')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['image'].endswith('.webm'))

    def test_slide_add_rejects_disallowed_extension(self):
        response = self._upload('payload.exe', b'MZ\x90\x00', 'application/octet-stream')
        self.assertEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)
        self.assertIn('image', response.data)
        self.assertEqual(response.data['image'][0].code, 'invalid_extension')

    def test_slide_list_returns_video_alongside_images(self):
        self._upload('promo.mp4', MP4_BYTES, 'video/mp4')
        self._upload('still.png', b'\x89PNG\r\n\x1a\n' + b'\x00' * 32, 'image/png')

        response = self.forced_auth_req(
            'get',
            reverse(self.base_view + 'list_or_delete_image'),
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        stored = [row['image'] for row in response.data['results']]
        self.assertEqual(len(stored), 2)
        self.assertTrue(any(name.endswith('.mp4') for name in stored))
        self.assertTrue(any(name.endswith('.png') for name in stored))


class AboutUsAPITestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'about_us:'
    databases = {'default', }

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
