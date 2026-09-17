import logging

from rest_framework import serializers

from proco.entities.models import Entity

logger = logging.getLogger('gigamaps.' + __name__)


class BaseEntitySerializer(serializers.ModelSerializer):
    entity_type_code = serializers.SerializerMethodField()

    class Meta:
        model = Entity
        fields = (
            'id', 'name', 'geopoint', 'entity_type_code',
        )
        read_only_fields = fields

    def get_entity_type_code(self, obj):
        return obj.entity_type.code if obj.entity_type else None


class CountryToSerializerMixin(object):
    def __init__(self, *args, **kwargs):
        self.country = kwargs.pop('country', None)
        super(CountryToSerializerMixin, self).__init__(*args, **kwargs)


class ListEntitySerializer(CountryToSerializerMixin, BaseEntitySerializer):
    is_verified = serializers.SerializerMethodField()

    class Meta(BaseEntitySerializer.Meta):
        fields = BaseEntitySerializer.Meta.fields + (
            'connectivity_status',
            'coverage_status',
            'is_verified',
        )

    def get_is_verified(self, obj):
        # TODO: Get this logic
        return False


class CommaSeparatedIntegerField(serializers.CharField):
    def to_internal_value(self, data):
        data = super().to_internal_value(data)
        if not data or not data.strip():
            raise serializers.ValidationError("This field may not be blank.")
        parts = [p.strip() for p in data.split(",") if p.strip()]
        if not parts:
            raise serializers.ValidationError("This field may not be blank.")
        for p in parts:
            if not p.isdigit():
                raise serializers.ValidationError("Must be a comma-separated list of valid integers.")
        return parts


class TileQuerySerializer(serializers.Serializer):
    country_id = serializers.IntegerField(required=False)
    country_id__in = CommaSeparatedIntegerField(required=False)
    admin1_id = serializers.IntegerField(required=False)
    admin1_id__in = CommaSeparatedIntegerField(required=False)
    school_id = serializers.IntegerField(required=False)
    school_id__in = CommaSeparatedIntegerField(required=False)
    entity_id = serializers.IntegerField(required=False)
    entity_id__in = CommaSeparatedIntegerField(required=False)
    exclude_schools_same_coords_except_id = serializers.IntegerField(required=False)
    exclude_entities_same_coords_except_id = serializers.IntegerField(required=False)
    limit = serializers.IntegerField(required=False, min_value=1)

