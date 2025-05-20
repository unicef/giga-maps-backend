from django.core.cache import cache
from django.test import TestCase

from proco.giga_meter import models as giga_meter_models
from proco.giga_meter.tests import factories as giga_meter_test_models
from proco.utils.tests import TestAPIViewSetMixin


class GigaMeterModelsTestCase(TestAPIViewSetMixin, TestCase):
    databases = ['gigameter_database',]

    @classmethod
    def setUpTestData(cls):
        cls.country = giga_meter_test_models.GigaMeter_CountryFactory()

        cls.row_one = giga_meter_test_models.GigaMeter_SchoolMasterDataFactory(
            country=cls.country,
            status=giga_meter_models.GigaMeter_SchoolMasterData.ROW_STATUS_PUBLISHED,
        )
        cls.row_two = giga_meter_test_models.GigaMeter_SchoolMasterDataFactory(
            country=cls.country,
            status=giga_meter_models.GigaMeter_SchoolMasterData.ROW_STATUS_PUBLISHED,
        )

    def setUp(self):
        cache.clear()
        super().setUp()

    def test_country_model(self):
        self.assertEqual(giga_meter_models.GigaMeter_Country.objects.all().count(), 1)
        self.assertEqual(str(self.country), f'{self.country.code} - {self.country.name} - {self.country.iso3_format}')

    def test_school_intermediate_model(self):
        self.assertEqual(giga_meter_models.GigaMeter_SchoolMasterData.objects.all().count(), 2)
        self.assertEqual(
            giga_meter_models.GigaMeter_SchoolMasterData.get_last_version(self.country.iso3_format),
            giga_meter_models.GigaMeter_Country.objects.get(
                iso3_format=self.country.iso3_format,
            ).latest_school_master_data_version
        )

        self.assertEqual(
            giga_meter_models.GigaMeter_SchoolMasterData.set_last_version(5, self.country.iso3_format),
            None
        )

        self.assertEqual(
            giga_meter_models.GigaMeter_SchoolMasterData.get_last_version(self.country.iso3_format),
            5
        )

        self.assertEqual(
            giga_meter_models.GigaMeter_Country.objects.get(
                iso3_format=self.country.iso3_format,
            ).latest_school_master_data_version,
            5
        )
