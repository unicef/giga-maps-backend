import logging
import re
from collections import OrderedDict
from datetime import timedelta

from django.db.models.functions import Lower
from rest_flex_fields.serializers import FlexFieldsModelSerializer
from rest_framework import serializers

from proco.connection_statistics.models import (
    CountryWeeklyStatus,
    SchoolWeeklyStatus,
    SchoolDailyStatus,
    SchoolRealTimeRegistration,
)
from proco.connection_statistics.serializers import SchoolWeeklyStatusSerializer
from proco.core.mixins import DownloadSerializerMixin
from proco.core.utils import is_blank_string
from proco.locations.fields import GeoPointCSVField
from proco.locations.models import Country, CountryAdminMetadata
from proco.schools import exceptions as schools_exceptions
from proco.schools.models import School, FileImport
from proco.utils import dates as date_utilities

logger = logging.getLogger('gigamaps.' + __name__)


class BaseSchoolSerializer(serializers.ModelSerializer):
    class Meta:
        model = School
        fields = (
            'id', 'name', 'geopoint',
        )
        read_only_fields = fields


class SchoolPointSerializer(BaseSchoolSerializer):
    country_integration_status = serializers.SerializerMethodField()

    class Meta(BaseSchoolSerializer.Meta):
        fields = ('geopoint', 'country_id', 'country_integration_status')

    def __init__(self, *args, **kwargs):
        self.countries_statuses = kwargs.pop('countries_statuses', None)
        super(SchoolPointSerializer, self).__init__(*args, **kwargs)

    def get_country_integration_status(self, obj):
        return self.countries_statuses[obj.country_id]


class CountryToSerializerMixin(object):
    def __init__(self, *args, **kwargs):
        self.country = kwargs.pop('country', None)
        super(CountryToSerializerMixin, self).__init__(*args, **kwargs)


class ListSchoolSerializer(CountryToSerializerMixin, BaseSchoolSerializer):
    is_verified = serializers.SerializerMethodField()

    class Meta(BaseSchoolSerializer.Meta):
        fields = BaseSchoolSerializer.Meta.fields + (
            'connectivity_status',
            'coverage_status',
            'is_verified',
        )

    def get_is_verified(self, obj):
        if not self.country.last_weekly_status:
            return None
        return self.country.last_weekly_status.integration_status not in [
            CountryWeeklyStatus.COUNTRY_CREATED, CountryWeeklyStatus.SCHOOL_OSM_MAPPED,
        ]


class CSVSchoolsListSerializer(ListSchoolSerializer):
    geopoint = GeoPointCSVField()

    class Meta(BaseSchoolSerializer.Meta):
        fields = (
            'name',
            'geopoint',
            'connectivity_status',
        )


class ExtendedSchoolSerializer(BaseSchoolSerializer):
    class Meta(BaseSchoolSerializer.Meta):
        fields = BaseSchoolSerializer.Meta.fields + (
            'connectivity_status',
            'coverage_type',
        )


class ExpandCountrySerializer(FlexFieldsModelSerializer):
    """
    ExpandCountrySerializer
        This serializer is used for expandable feature.
    """

    class Meta:
        model = Country
        fields = (
            'id',
            'name',
            'code',
            'iso3_format',
        )


class ExpandCountryAdminSerializer(FlexFieldsModelSerializer):
    """
    ExpandCountryAdminSerializer
        This serializer is used for expandable feature.
    """

    class Meta:
        model = CountryAdminMetadata
        fields = (
            'id',
            'name',
            'name_en',
            'layer_name',
        )


class ExpandSchoolSerializer(FlexFieldsModelSerializer):
    """
    ExpandSchoolSerializer
        This serializer is used for expandable feature.
    """
    admin1_name = serializers.ReadOnlyField(source='admin1.name', default='')
    admin2_name = serializers.ReadOnlyField(source='admin2.name', default='')

    class Meta:
        model = School
        fields = (
            'id',
            'name',
            'name_lower',
            'admin1_name',
            'admin2_name',
            'giga_id_school',
        )


class ExpandSchoolWeeklyStatusSerializer(FlexFieldsModelSerializer):
    """
    ExpandSchoolWeeklyStatusSerializer
        This serializer is used for expandable feature.
    """

    class Meta:
        model = SchoolWeeklyStatus
        fields = (
            'school_data_source',
        )


class SchoolStatusSerializer(FlexFieldsModelSerializer):
    class Meta:
        model = School
        read_only_fields = fields = (
            'name',
            'geopoint',
            'education_level',
            'country_id',
            'external_id',
            'last_weekly_status_id',
            'giga_id_school',
            'education_level_regional',
        )

        expandable_fields = {
            'country': (ExpandCountrySerializer, {'source': 'country'}),
            'last_weekly_status': (ExpandSchoolWeeklyStatusSerializer, {'source': 'last_weekly_status'}),
        }


class SchoolCSVSerializer(SchoolStatusSerializer, DownloadSerializerMixin):

    class Meta(SchoolStatusSerializer.Meta):

        report_fields = OrderedDict([
            ('giga_id_school', 'School Giga ID'),
            ('name', 'School Name'),
            ('longitude', {'name': 'Longitude', 'is_computed': True}),
            ('latitude', {'name': 'Latitude', 'is_computed': True}),
            ('education_level', 'Education Level'),
            ('country_iso3_format', {'name': 'Country ISO3 Code', 'is_computed': True}),
            ('country_name', {'name': 'Country Name', 'is_computed': True}),
            ('school_data_source', {'name': 'School Data Source', 'is_computed': True}),
        ])

    def get_country_name(self, data):
        return data.get('country', {}).get('name')

    def get_country_iso3_format(self, data):
        return data.get('country', {}).get('iso3_format')

    def get_longitude(self, data):
        point_coordinates = data.get('geopoint', {}).get('coordinates', [])
        if len(point_coordinates) > 0:
            return point_coordinates[0]

    def get_latitude(self, data):
        point_coordinates = data.get('geopoint', {}).get('coordinates', [])
        if len(point_coordinates) > 1:
            return point_coordinates[1]

    def get_school_data_source(self, data):
        return data.get('last_weekly_status', {}).get('school_data_source')

    def to_representation(self, data):
        data = super().to_representation(data)
        return self.to_record_representation(data)


class SchoolUpdateRetriveSerializer(serializers.ModelSerializer):
    """
    CountryUpdateRetriveSerializer
        Serializer to create Country.
    """
    timezone = serializers.CharField()

    class Meta:
        model = School
        fields = '__all__'

    def validate_giga_id_school(self, giga_id_school):
        if re.match(r'[a-zA-Z0-9-\'_()]*$', giga_id_school):
            school_qs = School.objects.annotate(giga_id_school_lower=Lower('giga_id_school')).filter(
                giga_id_school_lower=str(giga_id_school).lower())
            if self.instance:
                school_qs = school_qs.exclude(pk=self.instance.id)

            if school_qs.exists():
                raise schools_exceptions.DuplicateSchoolFieldValueError(
                    message_kwargs={'value': giga_id_school, 'name': 'giga_id_school'}
                )
            return str(giga_id_school).lower()
        raise schools_exceptions.InvalidSchoolFieldValueError(message_kwargs={'name': 'giga_id_school'})

    def create(self, validated_data):
        giga_id_school = str(validated_data.get('giga_id_school', '')).lower()

        deleted_school_with_same_giga_id = School.objects.all_deleted().annotate(
            giga_id_school_lower=Lower('giga_id_school'),
        ).filter(
            giga_id_school_lower=giga_id_school,
        ).order_by('-id').first()

        if deleted_school_with_same_giga_id:
            validated_data['deleted'] = None
            school_instance = super().update(deleted_school_with_same_giga_id, validated_data)
            logger.debug('School restored')

            SchoolDailyStatus.objects.all_deleted().filter(school=school_instance).update(deleted=None)
            logger.debug('School Daily restored')

            SchoolWeeklyStatus.objects.all_deleted().filter(school=school_instance).update(deleted=None)
            logger.debug('School Weekly restored')

            SchoolRealTimeRegistration.objects.all_deleted().filter(school=school_instance).update(deleted=None)
            logger.debug('School Real Time Registration restored')

            return school_instance
        else:
            return super().create(validated_data)


class SchoolListSerializer(BaseSchoolSerializer):
    country_name = serializers.CharField(source='country.name')
    timezone = serializers.CharField()

    class Meta(BaseSchoolSerializer.Meta):
        model = School
        fields = (
            'id',
            'name',
            'country_name',
            'giga_id_school',
            'address',
            'education_level',
            'school_type',
            'education_level_regional',
            'timezone',
            'environment',
            'external_id',
        )


class DetailSchoolSerializer(BaseSchoolSerializer):
    class Meta(BaseSchoolSerializer.Meta):
        fields = SchoolUpdateRetriveSerializer.Meta.fields


class ImportCSVSerializer(serializers.ModelSerializer):
    country_name = serializers.SerializerMethodField()
    uploaded_by_name = serializers.SerializerMethodField()

    class Meta:
        model = FileImport
        fields = '__all__'

    def get_country_name(self, instance):
        return instance.country.name if instance.country else instance.country

    def get_uploaded_by_name(self, instance):
        user_name = None
        if instance.uploaded_by:
            user_name = instance.uploaded_by.first_name
            if not is_blank_string(instance.uploaded_by.last_name):
                user_name += ' ' + instance.uploaded_by.last_name
        return user_name
