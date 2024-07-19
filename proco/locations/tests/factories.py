from django.contrib.gis.geos import GEOSGeometry

from factory import SubFactory
from factory import django as django_factory
from factory import fuzzy

from proco.locations.models import Country, CountryAdminMetadata, Location


class CountryFactory(django_factory.DjangoModelFactory):
    name = fuzzy.FuzzyText(length=20)
    code = fuzzy.FuzzyText(length=20)
    iso3_format = fuzzy.FuzzyText(length=20)
    description = fuzzy.FuzzyText(length=40)
    data_source = fuzzy.FuzzyText(length=40)

    geometry = GEOSGeometry('MultiPolygon(((0 0, 0 1, 1 1, 1 0, 0 0)), ((1 1, 1 2, 2 2, 2 1, 1 1)))')

    class Meta:
        model = Country


class LocationFactory(django_factory.DjangoModelFactory):
    name = fuzzy.FuzzyText(length=20)
    country = SubFactory(CountryFactory)

    geometry = GEOSGeometry('MultiPolygon(((0 0, 0 1, 1 1, 1 0, 0 0)), ((1 1, 1 2, 2 2, 2 1, 1 1)))')

    class Meta:
        model = Location


class Admin1Factory(django_factory.DjangoModelFactory):
    name = fuzzy.FuzzyText(length=20)
    giga_id_admin = fuzzy.FuzzyText(length=20)
    description = fuzzy.FuzzyText(length=40)
    layer_name = fuzzy.FuzzyChoice(dict(CountryAdminMetadata.LAYER_NAME_CHOICES).keys())

    country = SubFactory(CountryFactory)

    class Meta:
        model = CountryAdminMetadata
