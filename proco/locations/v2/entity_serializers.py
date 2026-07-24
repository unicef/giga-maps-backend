import logging
from collections import OrderedDict

from django.db.models import Count, Q
from rest_flex_fields.serializers import FlexFieldsModelSerializer
from rest_framework import serializers

from proco.accounts import models as accounts_models
from proco.core import utils as core_utilities
from proco.entities.models import Entity, EntityType
from proco.locations.models import Country
from proco.locations.serializers import BaseCountrySerializer, DetailCountrySerializer
from proco.schools.models import School


logger = logging.getLogger('gigamaps.' + __name__)


class EntityAwareListCountrySerializer(BaseCountrySerializer):
    """
    Entity-aware country list serializer.
    """
    integration_status = serializers.SerializerMethodField()
    schools_with_data_percentage = serializers.SerializerMethodField()
    entity_counts = serializers.SerializerMethodField()
    connectivity_availability = serializers.SerializerMethodField()
    coverage_availability = serializers.SerializerMethodField()

    data_source = serializers.SerializerMethodField()

    class Meta(BaseCountrySerializer.Meta):
        fields = BaseCountrySerializer.Meta.fields + (
            'integration_status',
            'date_of_join',
            'schools_with_data_percentage',
            'entity_counts',
            'connectivity_availability',
            'coverage_availability',
        )

    def get_integration_status(self, instance):
        if instance.last_weekly_status:
            return instance.last_weekly_status.integration_status

    def get_schools_with_data_percentage(self, instance):
        if instance.last_weekly_status:
            return instance.last_weekly_status.schools_with_data_percentage

    def get_connectivity_availability(self, instance):
        if instance.last_weekly_status:
            return instance.last_weekly_status.connectivity_availability

    def get_coverage_availability(self, instance):
        if instance.last_weekly_status:
            return instance.last_weekly_status.coverage_availability

    def get_entity_counts(self, instance):
        """
        Returns entity counts as a dictionary with entity_code as key.
        Example: {"school": 1500, "health": 250, "library": 50}
        """
        entity_counts = {}

        # Get all active entity types
        active_entity_types = EntityType.objects.filter(
            deleted__isnull=True,
            is_active=True
        ).order_by('display_order', 'code')

        for entity_type in active_entity_types:
            if entity_type.is_legacy:
                # Legacy schools - get from School model
                count = School.objects.filter(
                    country=instance,
                    deleted__isnull=True
                ).count()
            else:
                # New entities - get from Entity model
                count = Entity.objects.filter(
                    country=instance,
                    entity_type=entity_type,
                    deleted__isnull=True
                ).count()

            entity_counts[entity_type.code] = count

        return entity_counts

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


class EntityAwareDetailCountrySerializer(DetailCountrySerializer):
    """
    Entity-aware country detail serializer.
    """
    entity_counts = serializers.SerializerMethodField()

    class Meta(DetailCountrySerializer.Meta):
        fields = DetailCountrySerializer.Meta.fields + ('entity_counts',)

    def get_entity_counts(self, instance):
        """
        Returns entity counts as a dictionary with entity_code as key.
        Example: {"school": 1500, "health": 250, "library": 50}
        """
        entity_counts = {}

        # Get all active entity types
        active_entity_types = EntityType.objects.filter(
            deleted__isnull=True,
            is_active=True
        ).order_by('display_order', 'code')

        for entity_type in active_entity_types:
            if entity_type.is_legacy:
                # Legacy schools - get from School model
                count = School.objects.filter(
                    country=instance,
                    deleted__isnull=True
                ).count()
            else:
                # New entities - get from Entity model
                count = Entity.objects.filter(
                    country=instance,
                    entity_type=entity_type,
                    deleted__isnull=True
                ).count()

            entity_counts[entity_type.code] = count

        return entity_counts

    def get_active_layers_list(self, instance):
        active_layers_list = []
        linked_layers = instance.active_layers.select_related('data_layer__entity_type').filter(
            deleted__isnull=True,
            data_layer__deleted__isnull=True,
            data_layer__status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
        )
        for relationship_instance in linked_layers:
            entity_type = relationship_instance.data_layer.entity_type
            active_layers_list.append({
                'data_layer_id': relationship_instance.data_layer_id,
                'is_default': relationship_instance.is_default,
                'data_sources': relationship_instance.data_sources,
                'is_applicable': relationship_instance.is_applicable,
                'legend_configs': relationship_instance.legend_configs,
                'entity_type': entity_type.code if entity_type else None,
            })

        return active_layers_list

    def get_active_filters_list(self, instance):
        active_filters_list = []
        linked_filters = instance.active_filters.select_related('advance_filter__entity_type').filter(
            deleted__isnull=True,
            advance_filter__deleted__isnull=True,
            advance_filter__status=accounts_models.AdvanceFilter.FILTER_STATUS_PUBLISHED,
        )
        for relationship_instance in linked_filters:
            entity_type = relationship_instance.advance_filter.entity_type
            active_filters_list.append({
                'advance_filter_id': relationship_instance.advance_filter_id,
                'is_default': relationship_instance.is_default,
                'default_filter_values': relationship_instance.default_filter_values,
                'entity_type': entity_type.code if entity_type else None,
            })

        return active_filters_list
