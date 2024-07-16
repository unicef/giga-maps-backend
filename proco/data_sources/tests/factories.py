from factory import SubFactory
from factory import django as django_factory
from factory import fuzzy

from proco.data_sources.models import SchoolMasterData
from proco.locations.tests.factories import CountryFactory
from proco.schools.tests.factories import SchoolFactory


class SchoolMasterDataFactory(django_factory.DjangoModelFactory):

    school_id_giga = fuzzy.FuzzyText(length=50)
    school_id_govt = fuzzy.FuzzyText(length=50)
    school_name = fuzzy.FuzzyText(length=1000)

    admin1_id_giga = fuzzy.FuzzyText(length=50)

    admin2_id_giga = fuzzy.FuzzyText(length=50)
    education_level = fuzzy.FuzzyText(length=50)
    school_area_type = fuzzy.FuzzyText(length=50)
    school_funding_type = fuzzy.FuzzyText(length=50)
    school_establishment_year = fuzzy.FuzzyInteger(1900, high=2024)
    status = fuzzy.FuzzyChoice(SchoolMasterData.ROW_STATUS_CHOICES)

    country = SubFactory(CountryFactory)
    school = SubFactory(SchoolFactory)

    class Meta:
        model = SchoolMasterData
