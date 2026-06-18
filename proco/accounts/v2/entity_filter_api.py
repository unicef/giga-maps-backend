from django.db.models import Q
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import permissions
from rest_framework.filters import SearchFilter

from proco.accounts import exceptions as accounts_exceptions
from proco.accounts import models as accounts_models
from proco.accounts.v2 import entity_filter_serializers as serializers
from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.entities.constants import LEGACY_MODEL
from proco.entities.mixins import EntityTypeCodeMixin
from proco.utils.mixins import CachedListMixin
from proco.utils.filters import NullsAlwaysLastOrderingFilter


class EntityColumnConfigurationViewSet(BaseModelViewSet):
    model = accounts_models.ColumnConfiguration
    serializer_class = serializers.EntityColumnConfigurationListSerializer

    base_auth_permissions = (
        core_permissions.IsUserAuthenticated,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
    )

    ordering_field_names = ['-last_modified_at', 'name']
    apply_query_pagination = True

    filterset_fields = {
        'id': ['exact', 'in'],
        'is_filter_applicable': ['exact'],
        'entity_type__code': ['iexact', 'in', 'exact'],
    }

    def apply_queryset_filters(self, queryset):
        return super().apply_queryset_filters(queryset)


class EntityAdvanceFiltersViewSet(BaseModelViewSet):
    model = accounts_models.AdvanceFilter
    serializer_class = serializers.EntityAdvanceFiltersListSerializer

    action_serializers = {
        'create': serializers.CreateEntityAdvanceFilterSerializer,
        'partial_update': serializers.UpdateEntityAdvanceFilterSerializer,
    }

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanViewAdvanceFilters,
        core_permissions.CanAddAdvanceFilter,
        core_permissions.CanUpdateAdvanceFilter,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
        SearchFilter,
    )

    ordering_field_names = ['-last_modified_at', 'name']
    apply_query_pagination = True
    search_fields = ('=code', '=status', 'name', 'description', 'type')

    filterset_fields = {
        'id': ['exact', 'in'],
        'status': ['iexact', 'in', 'exact'],
        'published_by_id': ['exact', 'in'],
        'name': ['iexact', 'in', 'exact'],
        'entity_type__code': ['iexact', 'in', 'exact'],
    }

    permit_list_expands = ['created_by', 'published_by', 'last_modified_by', 'column_configuration']

    def apply_queryset_filters(self, queryset):
        query_params = self.request.query_params.dict()
        query_param_keys = query_params.keys()

        if 'country_id' in query_param_keys:
            queryset = queryset.filter(
                active_countries__country=query_params['country_id'],
                active_countries__deleted__isnull=True,
            )
        elif 'country_id__in' in query_param_keys:
            queryset = queryset.filter(
                active_countries__country_id__in=[c_id.strip() for c_id in query_params['country_id__in'].split(',')],
                active_countries__deleted__isnull=True,
            )

        return super().apply_queryset_filters(queryset)

    def perform_destroy(self, instance):
        if instance.status in [accounts_models.AdvanceFilter.FILTER_STATUS_DRAFT,
                               accounts_models.AdvanceFilter.FILTER_STATUS_DISABLED]:
            instance.deleted = core_utilities.get_current_datetime_object()
            instance.last_modified_at = core_utilities.get_current_datetime_object()
            instance.last_modified_by = core_utilities.get_current_user(request=self.request)
            return super().perform_destroy(instance)
        raise accounts_exceptions.InvalidAdvanceFilterDeleteError(
            message_kwargs={'filter': instance.name, 'status': instance.status},
        )


class EntityAdvanceFiltersPublishViewSet(BaseModelViewSet):
    model = accounts_models.AdvanceFilter
    serializer_class = serializers.PublishEntityAdvanceFilterSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPublishAdvanceFilter,
    )

    def apply_queryset_filters(self, queryset):
        queryset = queryset.filter(
            status__in=[accounts_models.AdvanceFilter.FILTER_STATUS_DRAFT,
                        accounts_models.AdvanceFilter.FILTER_STATUS_DISABLED],
        )
        return super().apply_queryset_filters(queryset)


class PublishedEntityAdvanceFiltersViewSet(EntityTypeCodeMixin, CachedListMixin, BaseModelViewSet):
    """
    PublishedEntityAdvanceFiltersViewSet
    Cache Attr:
        Auto Cache: Not required
        Call Cache: Yes
    """
    LIST_CACHE_KEY_PREFIX = 'PUBLISHED_ENTITY_FILTERS_LIST'

    model = accounts_models.AdvanceFilter
    serializer_class = serializers.PublishedEntityAdvanceFiltersListSerializer

    base_auth_permissions = (
        permissions.AllowAny,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
    )

    ordering_field_names = ['-last_modified_at', 'name']
    apply_query_pagination = True

    filterset_fields = {
        'id': ['exact', 'in'],
        'published_by_id': ['exact', 'in'],
        'name': ['iexact', 'in', 'exact'],
    }

    permit_list_expands = ['column_configuration', ]

    def get_list_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}_{2}'.format(
            self.LIST_CACHE_KEY_PREFIX,
            '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(self.kwargs.items()))),
            '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))),
        )

    def apply_queryset_filters(self, queryset):
        country_id = self.kwargs.get('country_id')
        status = self.kwargs.get('status', 'PUBLISHED')
        entity_type_codes = self.get_entity_type_code_params()

        queryset = queryset.filter(
            status=status,
            active_countries__country=country_id,
            active_countries__deleted__isnull=True,
        )

        if entity_type_codes is not None:
            entity_type_query = Q()
            non_legacy_entity_type_codes = [
                entity_type_code
                for entity_type_code in entity_type_codes
                if entity_type_code != LEGACY_MODEL
            ]

            if LEGACY_MODEL in entity_type_codes:
                entity_type_query |= Q(entity_type__isnull=True) | Q(entity_type__code=LEGACY_MODEL)

            if non_legacy_entity_type_codes:
                entity_type_query |= Q(entity_type__code__in=non_legacy_entity_type_codes)

            queryset = queryset.filter(entity_type_query)

        return super().apply_queryset_filters(queryset)

    def update_serializer_context(self, context):
        context['country_id'] = self.kwargs.get('country_id')
        return context


class EntityColumnConfigurationChoicesViewSet(BaseModelViewSet):
    """
    EntityColumnConfigurationChoicesViewSet
    Cache Attr:
        Auto Cache: Not required
        Call Cache: Yes
    """

    model = accounts_models.ColumnConfiguration
    serializer_class = serializers.EntityColumnConfigurationChoicesSerializer

    base_auth_permissions = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPublishAdvanceFilter,
    )

    def apply_queryset_filters(self, queryset):
        return super().apply_queryset_filters(queryset)
