import logging

from rest_framework import serializers

from proco.entities.models import Entity

logger = logging.getLogger('gigamaps.' + __name__)


class BaseEntitySerializer(serializers.ModelSerializer):
    class Meta:
        model = Entity
        fields = (
            'id', 'name', 'geopoint',
        )
        read_only_fields = fields


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
