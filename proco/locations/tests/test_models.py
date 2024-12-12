from django.contrib.gis.geos import GEOSGeometry
from django.test import TestCase

from proco.connection_statistics.models import CountryWeeklyStatus
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory


class TestCountryModel(TestCase):

    def test_optimization_for_empty_geometry(self):
        country = CountryFactory(geometry=GEOSGeometry('{"type": "MultiPolygon", "coordinates": []}'))

        self.assertTrue(country.geometry.empty)

    def test_geometry_null(self):
        country = CountryFactory(geometry=None)

        self.assertIsNone(country.geometry)

    def test_clear_data_function(self):
        country = CountryFactory()
        country.last_weekly_status.update_country_status_to_joined()
        [SchoolFactory(country=country) for _ in range(3)]

        self.assertEqual(country.schools.count(), 3)
        self.assertEqual(country.last_weekly_status.integration_status, CountryWeeklyStatus.JOINED)

        country._clear_data_country()

        self.assertEqual(country.schools.count(), 0)
        self.assertEqual(country.last_weekly_status.integration_status, CountryWeeklyStatus.COUNTRY_CREATED)
