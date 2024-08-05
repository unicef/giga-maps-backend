from django.contrib.gis.geos import GEOSGeometry
from factory import SubFactory
from factory import django as django_factory
from factory import fuzzy

from proco.locations.tests.factories import Admin1Factory, CountryFactory, LocationFactory
from proco.schools.models import FileImport, School


class SchoolFactory(django_factory.DjangoModelFactory):
    name = fuzzy.FuzzyText(length=20)
    giga_id_school = fuzzy.FuzzyText(length=20)
    external_id = fuzzy.FuzzyText(length=20)
    country = SubFactory(CountryFactory)
    location = SubFactory(LocationFactory)
    admin1 = SubFactory(Admin1Factory)
    geopoint = GEOSGeometry('Point(1 1)')
    gps_confidence = fuzzy.FuzzyDecimal(low=0.0)
    altitude = fuzzy.FuzzyInteger(0, 10000)

    class Meta:
        model = School


class FileImportFactory(django_factory.DjangoModelFactory):
    country = SubFactory(CountryFactory)
    uploaded_file = fuzzy.FuzzyText(length=20)

    class Meta:
        model = FileImport
