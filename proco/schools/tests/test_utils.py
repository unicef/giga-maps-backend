from django.test import TestCase

from proco.locations.tests.factories import CountryFactory
from proco.schools import utils as schools_utilities
from proco.schools.tests.factories import SchoolFactory
from proco.utils.tests import TestAPIViewSetMixin


class UtilsUtilitiesTestCase(TestAPIViewSetMixin, TestCase):

    def test_get_imported_file_path_utility(self):
        self.assertEqual(type(schools_utilities.get_imported_file_path(None, 'TestImportFile.csv')), str)

    def test_get_coverage_type_utility(self):
        country = CountryFactory()
        school_one = SchoolFactory(country=country)
        self.assertEqual(type(schools_utilities.get_coverage_type(school_one)), str)

    def test_get_connectivity_status_utility(self):
        country = CountryFactory()
        school_one = SchoolFactory(country=country)
        self.assertEqual(type(schools_utilities.get_connectivity_status(school_one)), str)

    def test_get_connectivity_status_by_master_api_utility(self):
        country = CountryFactory()
        school_one = SchoolFactory(country=country)
        self.assertEqual(type(schools_utilities.get_connectivity_status_by_master_api(school_one)), str)

    def test_get_get_coverage_status_utility(self):
        country = CountryFactory()
        school_one = SchoolFactory(country=country)
        self.assertEqual(type(schools_utilities.get_coverage_status(school_one)), str)

    def test_update_school_from_country_or_school_weekly_update_utility(self):
        self.assertEqual(schools_utilities.update_school_from_country_or_school_weekly_update(), None)
