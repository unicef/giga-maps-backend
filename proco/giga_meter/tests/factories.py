from factory import SubFactory
from factory import django as django_factory
from factory import fuzzy

from proco.giga_meter import models as giga_meter_models


class GigaMeter_CountryFactory(django_factory.DjangoModelFactory):
    name = fuzzy.FuzzyText(length=20)
    code = fuzzy.FuzzyText(length=20)
    iso3_format = fuzzy.FuzzyText(length=20)
    latest_school_master_data_version = fuzzy.FuzzyInteger(0, 100)

    class Meta:
        model = giga_meter_models.GigaMeter_Country


class GigaMeter_SchoolFactory(django_factory.DjangoModelFactory):
    country = SubFactory(GigaMeter_CountryFactory)

    name = fuzzy.FuzzyText(length=20)
    giga_id_school = fuzzy.FuzzyText(length=20)
    external_id = fuzzy.FuzzyText(length=20)
    geopoint = fuzzy.FuzzyText(length=20)
    gps_confidence = fuzzy.FuzzyDecimal(low=0.0)
    altitude = fuzzy.FuzzyInteger(0, 10000)

    admin_1_name = fuzzy.FuzzyText(length=20)
    admin_2_name = fuzzy.FuzzyText(length=20)
    admin_3_name = fuzzy.FuzzyText(length=20)
    admin_4_name = fuzzy.FuzzyText(length=20)

    class Meta:
        model = giga_meter_models.GigaMeter_School


class GigaMeter_SchoolMasterDataFactory(django_factory.DjangoModelFactory):
    country = SubFactory(GigaMeter_CountryFactory)

    school_id_giga = fuzzy.FuzzyText(length=50)
    school_id_govt = fuzzy.FuzzyText(length=50)
    school_name = fuzzy.FuzzyText(length=20)
    admin1 =  fuzzy.FuzzyText(length=20)
    admin1_id_giga = fuzzy.FuzzyText(length=20)
    admin2 = fuzzy.FuzzyText(length=20)
    admin2_id_giga = fuzzy.FuzzyText(length=20)
    education_level = fuzzy.FuzzyText(length=20)
    school_area_type = fuzzy.FuzzyText(length=20)
    school_funding_type = fuzzy.FuzzyText(length=20)
    school_establishment_year = fuzzy.FuzzyInteger(1900, high=2024)
    status = fuzzy.FuzzyChoice(giga_meter_models.GigaMeter_SchoolMasterData.ROW_STATUS_CHOICES)

    class Meta:
        model = giga_meter_models.GigaMeter_SchoolMasterData


class GigaMeter_SchoolStaticFactory(django_factory.DjangoModelFactory):
    school = SubFactory(GigaMeter_SchoolFactory)

    admin1_id_giga = fuzzy.FuzzyText(length=50)
    admin2_id_giga = fuzzy.FuzzyText(length=50)

    latitude = fuzzy.FuzzyDecimal(low=10.0, high=20.0)
    longitude = fuzzy.FuzzyDecimal(low=50.0, high=80.0)
    num_computers_desired = fuzzy.FuzzyInteger(0, 1000)

    school_establishment_year = fuzzy.FuzzyInteger(1900, high=2024)

    class Meta:
        model = giga_meter_models.GigaMeter_SchoolMasterData
