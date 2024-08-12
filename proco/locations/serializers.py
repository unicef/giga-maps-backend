import logging
import re
from collections import OrderedDict

from django.db.models.functions.text import Lower
from rest_flex_fields.serializers import FlexFieldsModelSerializer
from rest_framework import serializers

from proco.accounts.models import (
    AdvanceFilter,
    APIKeyCountryRelationship,
    AdvanceFilterCountryRelationship,
    DataLayerCountryRelationship,
)
from proco.accounts.serializers import (
    AdvanceFilterCountryRelationshipSerializer,
    DataLayerCountryRelationshipSerializer,
)
from proco.connection_statistics.models import (
    CountryDailyStatus,
    CountryWeeklyStatus,
    SchoolDailyStatus,
    SchoolWeeklyStatus,
    SchoolRealTimeRegistration,
)
from proco.connection_statistics.serializers import CountryWeeklyStatusSerializer
from proco.core import utils as core_utilities
from proco.core.mixins import DownloadSerializerMixin
from proco.custom_auth.serializers import ExpandUserSerializer
from proco.locations import exceptions as locations_exceptions
from proco.locations.models import Country, CountryAdminMetadata
from proco.schools.models import School
from proco.schools.serializers import ExpandCountrySerializer


logger = logging.getLogger('gigamaps.' + __name__)


class ExpandCountryAdminMetadataSerializer(FlexFieldsModelSerializer):
    """
    ExpandCountryAdminMetadataSerializer
        This serializer is used for expandable feature.
    """

    centroid = serializers.JSONField()
    bbox = serializers.JSONField()

    class Meta:
        model = CountryAdminMetadata
        fields = (
            'id',
            'layer_name',
            'name',
            'name_en',
            'description',
            'description_ui_label',
            'giga_id_admin',
            'mapbox_id',
            'centroid',
            'bbox',
        )

        expandable_fields = {
            'country': (ExpandCountrySerializer, {'source': 'country'}),
            'last_modified_by': (ExpandUserSerializer, {'source': 'last_modified_by'}),
            'created_by': (ExpandUserSerializer, {'source': 'created_by'}),
            'parent': ('proco.locations.ExpandCountryAdminMetadataSerializer', {'source': 'parent'}),
        }


class BaseCountrySerializer(serializers.ModelSerializer):
    map_preview = serializers.SerializerMethodField()

    admin_metadata = serializers.SerializerMethodField()

    benchmark_metadata = serializers.JSONField()

    class Meta:
        model = Country
        fields = (
            'id',
            'name',
            'code',
            'iso3_format',
            'flag',
            'map_preview',
            'description',
            'data_source',
            'data_source_description',
            'date_schools_mapped',
            'admin_metadata',
            'benchmark_metadata',
        )
        read_only_fields = fields

    def get_map_preview(self, instance):
        if not instance.map_preview:
            return ''

        request = self.context.get('request')
        photo_url = instance.map_preview.url
        return request.build_absolute_uri(photo_url)

    def get_admin_metadata(self, instance):
        return ExpandCountryAdminMetadataSerializer(
            CountryAdminMetadata.objects.filter(
                country=instance, layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN0).first(),
        ).data


class CountrySerializer(BaseCountrySerializer):
    pass


class CountryUpdateRetrieveSerializer(serializers.ModelSerializer):
    """
    CountryUpdateRetrieveSerializer
        Serializer to create Country.
    """

    benchmark_metadata = serializers.JSONField()

    active_layers_list = serializers.JSONField()
    active_filters_list = serializers.JSONField()

    class Meta:
        model = Country
        fields = '__all__'

    def validate_name(self, name):
        country_qs = Country.objects.annotate(name_lower=Lower('name')).filter(name_lower=str(name).lower())
        if self.instance:
            country_qs = country_qs.exclude(pk=self.instance.id)

        if country_qs.exists():
            raise locations_exceptions.DuplicateCountryFieldValueError(
                message_kwargs={'value': name, 'name': 'name'},
            )
        return name

    def validate_code(self, code):
        if re.match(r'[a-zA-Z0-9-\'_]*$', code):
            country_qs = Country.objects.annotate(code_lower=Lower('code')).filter(code_lower=str(code).lower())
            if self.instance:
                country_qs = country_qs.exclude(pk=self.instance.id)

            if country_qs.exists():
                raise locations_exceptions.DuplicateCountryFieldValueError(
                    message_kwargs={'value': code, 'name': 'code'},
                )
            return code
        raise locations_exceptions.InvalidCountryFieldValueError(message_kwargs={'name': 'code'})

    def validate_iso3_format(self, iso3_format):
        if re.match(r'[a-zA-Z0-9-\'_]*$', iso3_format):
            country_qs = Country.objects.annotate(code_lower=Lower('iso3_format')).filter(
                code_lower=str(iso3_format).lower())
            if self.instance:
                country_qs = country_qs.exclude(pk=self.instance.id)

            if country_qs.exists():
                raise locations_exceptions.DuplicateCountryFieldValueError(
                    message_kwargs={'value': iso3_format, 'name': 'iso3_format'},
                )
            return iso3_format
        raise locations_exceptions.InvalidCountryFieldValueError(message_kwargs={'name': 'iso3_format'})

    def create(self, validated_data):
        country_code = str(validated_data.get('code', '')).lower()
        country_iso3_format = str(validated_data.get('iso3_format')).lower()
        deleted_country_with_same_code_iso3_format = Country.objects.all_deleted().annotate(
            code_lower=Lower('code'),
            iso3_format_lower=Lower('iso3_format'),
        ).filter(
            code_lower=country_code,
            iso3_format_lower=country_iso3_format,
        ).order_by('-id').first()

        active_layers_list = validated_data.pop('active_layers_list', [])
        active_filters_list = validated_data.pop('active_filters_list', [])

        if deleted_country_with_same_code_iso3_format:
            validated_data['deleted'] = None
            country_instance = super().update(deleted_country_with_same_code_iso3_format, validated_data)
            logger.info('Country restored.')

            CountryDailyStatus.objects.all_deleted().filter(country=country_instance).update(deleted=None)
            logger.info('Country daily restored.')

            CountryWeeklyStatus.objects.all_deleted().filter(country=country_instance).update(deleted=None)
            logger.info('Country weekly restored.')

            School.objects.all_deleted().filter(country=country_instance).update(deleted=None)
            logger.info('Schools restored.')

            SchoolDailyStatus.objects.all_deleted().filter(school__country=country_instance).update(deleted=None)
            logger.info('School daily restored.')

            SchoolWeeklyStatus.objects.all_deleted().filter(school__country=country_instance).update(deleted=None)
            logger.info('School weekly restored.')

            SchoolRealTimeRegistration.objects.all_deleted().filter(school__country=country_instance).update(
                deleted=None)
            logger.info('School real time registration restored.')

            request_user = core_utilities.get_current_user(context=self.context)
            DataLayerCountryRelationship.objects.filter(country=country_instance).update(
                deleted=core_utilities.get_current_datetime_object(),
                last_modified_by=request_user,
            )

            AdvanceFilterCountryRelationship.objects.filter(country=country_instance).update(
                deleted=core_utilities.get_current_datetime_object(),
                last_modified_by=request_user,
            )

            for country_api_key_relationship_obj in APIKeyCountryRelationship.objects.all_records().filter(
                country=country_instance,
                api_key__valid_to__gte=core_utilities.get_current_datetime_object().date(),
                deleted__isnull=False,
            ).order_by('-last_modified_at'):
                if APIKeyCountryRelationship.objects.all_records().filter(
                    country=country_api_key_relationship_obj.country,
                    api_key=country_api_key_relationship_obj.api_key,
                    api_key__valid_to__gte=core_utilities.get_current_datetime_object().date(),
                    deleted__isnull=True,
                ).exists():
                    logger.debug(
                        'Warning: api key for country ({0}) already exists.'.format(country_instance.iso3_format))
                else:
                    country_api_key_relationship_obj.update(deleted=None, last_modified_by=request_user)
                    logger.info('Api key restored.')

        else:
            country_instance = super().create(validated_data)

        for data_layer_dict in active_layers_list:
            data_layer_country_data = {
                'country': country_instance.id,
                'data_layer': data_layer_dict['data_layer_id'],
                'is_default': data_layer_dict['is_default'],
                'data_sources': data_layer_dict.get('data_sources', {}),
            }

            data_layer_country_relationships = DataLayerCountryRelationshipSerializer(
                data=data_layer_country_data,
                context=self.context,
            )
            data_layer_country_relationships.is_valid(raise_exception=True)
            data_layer_country_relationships.save()

        for filter_dict in active_filters_list:
            filter_country_data = {
                'country': country_instance.id,
                'advance_filter': filter_dict['advance_filter_id'],
            }

            filter_country_relationships = AdvanceFilterCountryRelationshipSerializer(
                data=filter_country_data,
                context=self.context,
            )
            filter_country_relationships.is_valid(raise_exception=True)
            filter_country_relationships.save()

        return country_instance

    def update(self, instance, validated_data):
        country_instance = super().update(instance, validated_data)

        active_layers_list = validated_data.pop('active_layers_list', [])
        active_filters_list = validated_data.pop('active_filters_list', [])

        request_user = core_utilities.get_current_user(context=self.context)

        DataLayerCountryRelationship.objects.filter(country=country_instance).update(
            deleted=core_utilities.get_current_datetime_object(),
            last_modified_by=request_user,
        )

        for data_layer_dict in active_layers_list:
            data_layer_country_data = {
                'country': country_instance.id,
                'data_layer': data_layer_dict['data_layer_id'],
                'is_default': data_layer_dict['is_default'],
                'data_sources': data_layer_dict.get('data_sources', {}),
            }

            data_layer_country_relationships = DataLayerCountryRelationshipSerializer(
                data=data_layer_country_data,
                context=self.context,
            )
            data_layer_country_relationships.is_valid(raise_exception=True)
            data_layer_country_relationships.save()

        AdvanceFilterCountryRelationship.objects.filter(country=country_instance).update(
            deleted=core_utilities.get_current_datetime_object(),
            last_modified_by=request_user,
        )
        for filter_dict in active_filters_list:
            filter_country_data = {
                'country': country_instance.id,
                'advance_filter': filter_dict['advance_filter_id'],
            }

            filter_country_relationships = AdvanceFilterCountryRelationshipSerializer(
                data=filter_country_data,
                context=self.context,
            )
            filter_country_relationships.is_valid(raise_exception=True)
            filter_country_relationships.save()

        return country_instance

    def to_representation(self, instance):
        active_layers_list = []
        active_filters_list = []

        linked_layers = instance.active_layers.all()
        for relationship_instance in linked_layers:
            active_layers_list.append({
                'data_layer_id': relationship_instance.data_layer_id,
                'is_default': relationship_instance.is_default,
                'data_sources': relationship_instance.data_sources,
            })

        linked_filters = instance.active_filters.all()
        for relationship_instance in linked_filters:
            active_filters_list.append({
                'advance_filter_id': relationship_instance.advance_filter_id,
            })

        setattr(instance, 'active_layers_list', active_layers_list)
        setattr(instance, 'active_filters_list', active_filters_list)

        return super().to_representation(instance)


class ListCountrySerializer(BaseCountrySerializer):
    integration_status = serializers.SerializerMethodField()
    schools_with_data_percentage = serializers.SerializerMethodField()
    schools_total = serializers.SerializerMethodField()
    connectivity_availability = serializers.SerializerMethodField()
    coverage_availability = serializers.SerializerMethodField()

    data_source = serializers.SerializerMethodField()

    class Meta(BaseCountrySerializer.Meta):
        fields = BaseCountrySerializer.Meta.fields + (
            'integration_status',
            'date_of_join',
            'schools_with_data_percentage',
            'schools_total',
            'connectivity_availability',
            'coverage_availability',
        )

    def get_integration_status(self, instance):
        if instance.last_weekly_status:
            return instance.last_weekly_status.integration_status

    def get_schools_total(self, instance):
        if instance.last_weekly_status:
            return instance.last_weekly_status.schools_total

    def get_schools_with_data_percentage(self, instance):
        if instance.last_weekly_status:
            return instance.last_weekly_status.schools_with_data_percentage

    def get_connectivity_availability(self, instance):
        if instance.last_weekly_status:
            return instance.last_weekly_status.connectivity_availability

    def get_coverage_availability(self, instance):
        if instance.last_weekly_status:
            return instance.last_weekly_status.coverage_availability

    def get_data_source(self, instance):
        data_source = instance.data_source
        if core_utilities.is_blank_string(data_source):
            return data_source
        # \r = CR (Carriage Return) → Used as a new line character in Mac OS before X
        # \n = LF (Line Feed) → Used as a new line character in Unix/Mac OS X
        # \r\n = CR + LF → Used as a new line character in Windows
        escape_chars = ['\r\n', '\n', '\r']
        for escape_char in escape_chars:
            if escape_char in data_source:
                data_source = ', '.join([value.strip() for value in data_source.split(escape_char)])
        return data_source


class DetailCountrySerializer(BaseCountrySerializer):
    statistics = serializers.SerializerMethodField()

    admin1_metadata = serializers.SerializerMethodField()

    active_layers_list = serializers.SerializerMethodField()
    active_filters_list = serializers.SerializerMethodField()

    data_source = serializers.SerializerMethodField()

    class Meta(BaseCountrySerializer.Meta):
        fields = BaseCountrySerializer.Meta.fields + (
            'date_of_join',
            'statistics',
            'admin1_metadata',
            'last_weekly_status_id',
            'active_layers_list',
            'active_filters_list',
        )

    def get_statistics(self, instance):
        return CountryWeeklyStatusSerializer(instance.last_weekly_status if instance.last_weekly_status else None).data

    def get_admin1_metadata(self, instance):
        admin1_list = []
        qs = CountryAdminMetadata.objects.filter(country=instance, layer_name=CountryAdminMetadata.LAYER_NAME_ADMIN1)
        for qry_instance in qs:
            admin1_list.append(ExpandCountryAdminMetadataSerializer(qry_instance).data)
        return admin1_list

    def get_active_layers_list(self, instance):
        active_layers_list = []
        linked_layers = instance.active_layers.all()
        for relationship_instance in linked_layers:
            active_layers_list.append({
                'data_layer_id': relationship_instance.data_layer_id,
                'is_default': relationship_instance.is_default,
                'data_sources': relationship_instance.data_sources,
            })

        return active_layers_list

    def get_active_filters_list(self, instance):
        active_filters_list = []
        linked_filters = instance.active_filters.all().filter(advance_filter__status=AdvanceFilter.FILTER_STATUS_PUBLISHED)
        for relationship_instance in linked_filters:
            active_filters_list.append({
                'advance_filter_id': relationship_instance.advance_filter_id,
            })

        return active_filters_list

    def get_data_source(self, instance):
        data_source = instance.data_source
        if core_utilities.is_blank_string(data_source):
            return data_source
        # \r = CR (Carriage Return) → Used as a new line character in Mac OS before X
        # \n = LF (Line Feed) → Used as a new line character in Unix/Mac OS X
        # \r\n = CR + LF → Used as a new line character in Windows
        escape_chars = ['\r\n', '\n', '\r']
        for escape_char in escape_chars:
            if escape_char in data_source:
                data_source = ', '.join([value.strip() for value in data_source.split(escape_char)])
        return data_source


class ExpandCountryWeeklyStatusSerializer(FlexFieldsModelSerializer):
    """
    ExpandCountryWeeklyStatusSerializer
        This serializer is used for expandable feature.
    """

    class Meta:
        model = CountryWeeklyStatus
        fields = (
            # 'id',
            'schools_total',
            'schools_connected',
            'schools_connectivity_unknown',
            'schools_connectivity_no',
            'schools_connectivity_moderate',
            'schools_connectivity_good',
            'integration_status',
            'avg_distance_school',
            'schools_with_data_percentage',
            'connectivity_speed',
            'connectivity_latency',
            'connectivity_availability',
            'coverage_availability',
            'schools_coverage_good',
            'schools_coverage_moderate',
            'schools_coverage_no',
            'schools_coverage_unknown',
            'global_schools_connectivity_good',
            'global_schools_connectivity_moderate',
            'global_schools_connectivity_no',
            'global_schools_connectivity_unknown',
        )


class CountryStatusSerializer(FlexFieldsModelSerializer):
    class Meta:
        model = Country
        read_only_fields = fields = (
            'id',
            'name',
            'iso3_format',
        )

        expandable_fields = {
            'last_weekly_status': (ExpandCountryWeeklyStatusSerializer, {'source': 'last_weekly_status'}),
        }


class CountryCSVSerializer(CountryStatusSerializer, DownloadSerializerMixin):
    class Meta(CountryStatusSerializer.Meta):
        report_fields = OrderedDict([
            ('id', 'ID'),
            ('name', 'Name'),
            ('iso3_format', 'Country ISO3 Code'),
        ])

    def to_representation(self, data):
        data = super().to_representation(data)
        return self.to_record_representation(data)


class SearchListSerializer(FlexFieldsModelSerializer):
    """
    SearchListSerializer
        Serializer to list all APIs.
    """
    admin1_name = serializers.SerializerMethodField()
    admin2_name = serializers.SerializerMethodField()

    country_id = serializers.ReadOnlyField(source='country.id')
    country_name = serializers.ReadOnlyField(source='country.name')
    country_code = serializers.ReadOnlyField(source='country.code')

    class Meta:
        model = School
        read_only_fields = fields = (
            'id',
            'name',
            'name_lower',
            'admin1_name',
            'admin2_name',
            'giga_id_school',
            'country_id',
            'country_name',
            'country_code',
        )

    def get_admin1_name(self, data):
        admin_val = 'Unknown'
        if isinstance(data, dict):
            if not core_utilities.is_blank_string(data.get('admin1_name')):
                admin_val = data['admin1_name']
        elif isinstance(data, self.Meta.model) and not core_utilities.is_blank_string(data.admin1_name):
            admin_val = data.admin1_name
        return admin_val

    def get_admin2_name(self, data):
        admin_val = 'Unknown'
        if isinstance(data, dict):
            if not core_utilities.is_blank_string(data.get('admin2_name')):
                admin_val = data['admin2_name']
        elif isinstance(data, self.Meta.model) and not core_utilities.is_blank_string(data.admin2_name):
            admin_val = data.admin2_name
        return admin_val
