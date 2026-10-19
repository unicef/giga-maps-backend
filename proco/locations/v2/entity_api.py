import logging

from django.conf import settings
from django.db.models.functions.text import Lower
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_control
from rest_framework import mixins, viewsets
from rest_framework.filters import SearchFilter

from proco.entities.models import Entity, EntityType
from proco.locations.models import Country
from proco.locations.v2.entity_serializers import (
    EntityAwareDetailCountrySerializer,
    EntityAwareListCountrySerializer,
)
from proco.schools.models import School
from proco.utils.filters import NullsAlwaysLastOrderingFilter
from proco.utils.mixins import CachedListMixin, CachedRetrieveMixin


logger = logging.getLogger('gigamaps.' + __name__)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class EntityCountryViewSet(
    CachedListMixin,
    CachedRetrieveMixin,
    mixins.RetrieveModelMixin,
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    LIST_CACHE_KEY_PREFIX = 'V2_ENTITY_COUNTRIES_LIST'
    RETRIEVE_CACHE_KEY_PREFIX = 'V2_ENTITY_COUNTRY_DETAIL'

    pagination_class = None
    queryset = Country.objects.all().select_related('last_weekly_status')
    serializer_class = EntityAwareListCountrySerializer
    filter_backends = (
        NullsAlwaysLastOrderingFilter, SearchFilter,
    )
    ordering = ('name',)
    ordering_fields = ('name',)
    search_fields = ('name',)

    def get_serializer_class(self):
        return (
            EntityAwareListCountrySerializer
            if self.action == 'list'
            else EntityAwareDetailCountrySerializer
        )

    def get_object(self):
        return get_object_or_404(
            self.queryset.annotate(code_lower=Lower('code')),
            code_lower=self.kwargs.get('pk').lower(),
        )

    def get_queryset(self):
        qs = super().get_queryset()
        return qs.defer('geometry')

    def filter_queryset(self, queryset):
        queryset = super().filter_queryset(queryset)

        has_entities = self.request.query_params.get('has_entities', '').lower().strip()

        if has_entities:
            entity_type = EntityType.get_by_code(has_entities)
            if entity_type:
                if entity_type.is_legacy:
                    country_ids = list(
                        School.objects.all().values_list(
                            'country_id', flat=True,
                        ).order_by('country_id').distinct('country_id')
                    )
                else:
                    country_ids = list(
                        Entity.objects.filter(
                            entity_type=entity_type,
                        ).values_list(
                            'country_id', flat=True,
                        ).order_by('country_id').distinct('country_id')
                    )
                queryset = queryset.filter(id__in=country_ids)

        return queryset
