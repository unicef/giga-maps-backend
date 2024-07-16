from django.contrib.gis.geos import GEOSGeometry
from django.core.cache import cache
from django.test import TestCase
from django.urls import reverse

from rest_framework import status
import string, random
from proco.connection_statistics.tests.factories import CountryWeeklyStatusFactory
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory
from proco.utils.tests import TestAPIViewSetMixin
from proco.custom_auth import models as auth_models
from proco.locations.models import Country


class CountryApiTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'locations:countries'

    def get_detail_args(self, instance):
        return self.get_list_args() + [instance.code.lower()]

    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()
        SchoolFactory(country=cls.country_one, location__country=cls.country_one)
        SchoolFactory(country=cls.country_one, location__country=cls.country_one)
        CountryWeeklyStatusFactory(country=cls.country_one)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_countries_list(self):
        with self.assertNumQueries(3):
            response = self._test_list(
                user=None, expected_objects=[self.country_one, self.country_two],
            )
        self.assertIn('integration_status', response.data[0])

    def test_country_detail(self):
        with self.assertNumQueries(4):
            response = self._test_retrieve(
                user=None, instance=self.country_one,
            )
        self.assertIn('statistics', response.data)

    def test_country_list_cached(self):
        with self.assertNumQueries(3):
            self._test_list(
                user=None, expected_objects=[self.country_one, self.country_two],
            )

        with self.assertNumQueries(0):
            self._test_list(
                user=None, expected_objects=[self.country_one, self.country_two],
            )

    # def test_empty_countries_hidden(self):
    #     CountryFactory(geometry=GEOSGeometry('{"type": "MultiPolygon", "coordinates": []}'))
    #     self._test_list(
    #         user=None, expected_objects=[self.country_one, self.country_two],
    #     )


class CountryBoundaryApiTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'locations:countries-list'

    @classmethod
    def setUpTestData(cls):
        cls.country_one = CountryFactory()
        cls.country_two = CountryFactory()
        SchoolFactory(country=cls.country_one, location__country=cls.country_one)
        SchoolFactory(country=cls.country_one, location__country=cls.country_one)

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_countries_list(self):
        with self.assertNumQueries(3):
            response = self.forced_auth_req('get', reverse(self.base_view))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # self.assertIn('geometry_simplified', response.data[0])

    def test_country_list_cached(self):
        with self.assertNumQueries(3):
            response = self.forced_auth_req('get', reverse(self.base_view))
            self.assertEqual(response.status_code, status.HTTP_200_OK)

        with self.assertNumQueries(0):
            response = self.forced_auth_req('get', reverse(self.base_view))
            self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_empty_countries_hidden(self):
        CountryFactory(geometry=GEOSGeometry('{"type": "MultiPolygon", "coordinates": []}'))
        response = self.forced_auth_req('get', reverse(self.base_view))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # self.assertCountEqual([r['id'] for r in response.data], [self.country_one.id, self.country_two.id])


class CountryDataTestCase(TestAPIViewSetMixin, TestCase):
    base_view = 'locations:'
    databases = {'default', 'read_only_database'}

    def setUp(self):
        self.email = 'test@test.com'
        self.password = 'SomeRandomPass96'
        self.user = auth_models.ApplicationUser.objects.create_user(username=self.email, password=self.password)

        self.role = auth_models.Role.objects.create(name='Admin', category='system')
        self.role_permission = auth_models.UserRoleRelationship.objects.create(user=self.user, role=self.role)

        self.data = {"name": ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)),
                     # ===str(uuid.uuid4())[0:10],
                     "code": ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(2)),
                     "last_weekly_status_id": 2091,
                     "flag": "images/7962e7d2-ea1f-4571-a031-bb830fd575c6.png"}

        self.country_id = Country.objects.create(**self.data).id
        self.delete_data = {"id": [self.country_id]}

        self.country_one = CountryFactory()
        return super().setUp()

    # def test_create(self):
    #     self.data = {"name": ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)),#===str(uuid.uuid4())[0:10],
    #                  "code": ''.join(random.choice(string.ascii_uppercase) for _ in range(2)),
    #                  "last_weekly_status_id": 2091,"benchmark_metadata":{}}
    #     headers = {'Content-Type': 'multipart/form-data'}
    #
    #     response = self.forced_auth_req(
    #         'post',
    #         reverse(self.base_view + "list_or_create_or_destroy_country"),
    #         data=self.data,
    #         headers=headers,
    #         user=self.user)

    # self.assertEqual(response.status_code, status.HTTP_200_OK)
    # self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    # def test_update(self):
    #     self.data = {"name": ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)),#===str(uuid.uuid4())[0:10],
    #                  "code": ''.join(random.choice(string.ascii_uppercase) for _ in range(2)),
    #                  "last_weekly_status_id": 2091,"benchmark_metadata":{}}
    #     self.country_one = CountryFactory()
    #     from django.core import serializers
    #     tmpJson = serializers.serialize("json", self.country_one[0])
    #     tmpObj = json.loads(tmpJson)
    #     # import json
    #     # print(json.dumps(self.country_one.__dict__))
    #     response = self.forced_auth_req(
    #         'put',
    #         reverse(self.base_view + "update_or_retrieve_country", args=(self.country_id,)),
    #         data=tmpObj,
    #         user=self.user,
    #     )
    # self.assertEqual(response.status_code, status.HTTP_200_OK)
    # self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)

    def test_destroy(self):
        response = self.forced_auth_req(
            'delete',
            reverse(self.base_view + "list_or_create_or_destroy_country"),
            data=self.delete_data,
            user=self.user,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.status_code, status.HTTP_502_BAD_GATEWAY)
