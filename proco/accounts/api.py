import copy
import json
import logging
from datetime import timedelta

import requests
import uuid
from django.conf import settings
from django.contrib.admin.models import LogEntry
from django.db.models import Case, IntegerField, Value, When
from django.db.models import Q, F
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_control
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import permissions
from rest_framework import status as rest_status
from rest_framework.filters import SearchFilter
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param
from rest_framework.views import APIView

from proco.accounts import exceptions as accounts_exceptions
from proco.accounts import models as accounts_models
from proco.accounts import serializers
from proco.accounts import utils as account_utilities
from proco.accounts.config import app_config as account_config
from proco.connection_statistics import models as statistics_models
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import SchoolWeeklyStatus
from proco.contact.models import ContactMessage
from proco.core import db_utils as db_utilities
from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.custom_auth import models as auth_models
from proco.locations.models import Country
from proco.utils import dates as date_utilities
from proco.utils.cache import cache_manager, custom_cache_control, no_expiry_cache_manager
from proco.utils.filters import NullsAlwaysLastOrderingFilter
from proco.utils.mixins import CachedListMixin
from proco.utils.tasks import update_all_cached_values


logger = logging.getLogger('gigamaps.' + __name__)


class APIsListAPIView(BaseModelViewSet):
    """
    APIsListAPIView
        This class is used to list all Download APIs.
        Inherits: ListAPIView
    """
    model = accounts_models.API
    serializer_class = serializers.APIsListSerializer

    base_auth_permissions = (
        permissions.AllowAny,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
    )

    ordering_field_names = ['-category', 'name']

    filterset_fields = {
        'category': ['iexact', 'in', 'exact'],
        'code': ['iexact', 'in', 'exact'],
        'name': ['iexact', 'in', 'exact'],
    }


class APICategoriesViewSet(BaseModelViewSet):
    """
    APICategoriesViewSet
        This class is used to do CRUD on API Categories.
        Inherits: BaseModelViewSet
    """
    model = accounts_models.APICategory
    serializer_class = serializers.APICategoriesCRUDSerializer

    action_serializers = {
        'create': serializers.APICategoriesCRUDSerializer,
        'partial_update': serializers.APICategoriesCRUDSerializer,
    }

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanDoCRUDonAPICategories,
    )

    filter_backends = (
        DjangoFilterBackend,
        SearchFilter,
    )

    ordering_fields = ( 'api__name', 'name', 'last_modified_at')
    apply_query_pagination = True

    filterset_fields = {
        'name': ['iexact', 'in', 'exact'],
        'api_id': ['exact', 'in'],
    }

    search_fields = ['api__name', 'name', 'description']
    permit_list_expands = ['created_by', 'api', 'last_modified_by']

    def update_serializer_context(self, context):
        api_instance = None

        if self.kwargs.get('pk'):
            api_instance = accounts_models.APICategory.objects.filter(id=self.kwargs.get('pk')).first().api
        elif self.request.data.get('api'):
            api_instance = accounts_models.API.objects.filter(id=self.request.data.get('api')).first()

        if api_instance is not None:
            context['api_instance'] = api_instance
        return context


class APIKeysViewSet(BaseModelViewSet):
    """
    APIKeysViewSet
        This class is used to list all API keys.
        Inherits: BaseModelViewSet
    """
    model = accounts_models.APIKey
    serializer_class = serializers.APIKeysListSerializer

    action_serializers = {
        'create': serializers.CreateAPIKeysSerializer,
        'partial_update': serializers.UpdateAPIKeysSerializer,
    }

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanApproveRejectAPIKeyorAPIKeyExtension,
        core_permissions.CanDeleteAPIKey,
    )

    filter_backends = (
        DjangoFilterBackend,
        SearchFilter,
        # NullsAlwaysLastOrderingFilter,
    )

    ordering_fields = ('valid_to', 'status', 'last_modified_at', 'extension_status',)
    apply_query_pagination = True

    filterset_fields = {
        'status': ['iexact', 'in', 'exact'],
        'user_id': ['exact', 'in'],
    }

    search_fields = ['api__name', 'user__first_name', 'user__last_name', 'user__email']

    permit_list_expands = ['user', 'api', 'status_updated_by']

    def apply_queryset_filters(self, queryset):
        """ If user not superuser then return only own Keys. For superuser return all API Keys"""
        request_user = self.request.user
        has_approval_permission = request_user.permissions.get(
            auth_models.RolePermission.CAN_APPROVE_REJECT_API_KEY, False)

        queryset = queryset.filter(api__deleted__isnull=True, has_write_access=False)
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

        if 'api_category_id' in query_param_keys:
            queryset = queryset.filter(
                active_categories__api_category_id=query_params['api_category_id'],
                active_categories__deleted__isnull=True,
            )
        elif 'api_category__in' in query_param_keys:
            queryset = queryset.filter(
                active_categories__api_category_id__in=[
                    c_id.strip() for c_id in query_params['api_category__in'].split(',')
                ],
                active_categories__deleted__isnull=True,
            )

        if not core_utilities.is_superuser(request_user) and not has_approval_permission:
            queryset = queryset.filter(user=request_user)

            queryset = queryset.annotate(
                custom_order=Case(
                    When(status='APPROVED', then=Value(1)),
                    When(status='APPROVED', extension_status='APPROVED', then=Value(2)),
                    When(status='APPROVED', extension_status='INITIATED', then=Value(3)),
                    When(status='INITIATED', then=Value(4)),
                    output_field=IntegerField(),
                )
            ).order_by('custom_order', '-valid_to', '-id')
        else:
            queryset = queryset.annotate(
                custom_order=Case(
                    When(status='INITIATED', then=Value(1)),
                    When(status='APPROVED', extension_status='INITIATED', then=Value(2)),
                    When(status='APPROVED', extension_status='APPROVED', then=Value(3)),
                    When(status='APPROVED', then=Value(4)),
                    output_field=IntegerField(),
                )
            ).order_by('custom_order', '-valid_to', '-id')

        return super().apply_queryset_filters(queryset)

    def update_serializer_context(self, context):
        api_instance = None

        if self.kwargs.get('pk'):
            api_instance = accounts_models.APIKey.objects.filter(id=self.kwargs.get('pk')).first().api
        elif self.request.data.get('api'):
            api_instance = accounts_models.API.objects.filter(id=self.request.data.get('api')).first()

        if api_instance is not None:
            context['api_instance'] = api_instance
        return context

    def perform_destroy(self, instance):
        """
        perform_destroy
        :param instance:
        :return:
        """
        request_user = core_utilities.get_current_user(request=self.request)

        instance.deleted = core_utilities.get_current_datetime_object()
        instance.last_modified_at = core_utilities.get_current_datetime_object()
        instance.last_modified_by = request_user

        status = super().perform_destroy(instance)

        api_key_user = instance.user

        # Once API Key is deleted by Admin, send the status email to the user
        if api_key_user is not None and request_user.id != api_key_user.id:
            email_subject = account_config.api_key_deletion_email_subject_format % (
                core_utilities.get_project_title(), instance.api.name,
            )
            email_message = account_config.api_key_deletion_email_message_format
            email_content = {'subject': email_subject, 'message': email_message}
            account_utilities.send_standard_email(api_key_user, email_content)
        return status


class APIKeysRequestExtensionViewSet(BaseModelViewSet):
    """
    APIKeysRequestExtensionViewSet
        This class is used to update the API key valid_to date value.
        Inherits: BaseModelViewSet
    """
    model = accounts_models.APIKey
    serializer_class = serializers.UpdateAPIKeysForExtensionSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
    )


class APIKeysAPICategoriesViewSet(BaseModelViewSet):
    """
    APIKeysAPICategoriesViewSet
        This class is used to assign an API category to the API key.
        Inherits: BaseModelViewSet
    """
    model = accounts_models.APIKey
    serializer_class = serializers.UpdateAPIKeysForAPICategoriesSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanApproveRejectAPIKeyorAPIKeyExtension,
    )

    def update_serializer_context(self, context):
        api_instance = None

        if self.kwargs.get('pk'):
            api_instance = accounts_models.APIKey.objects.filter(id=self.kwargs.get('pk')).first().api
        elif self.request.data.get('api'):
            api_instance = accounts_models.API.objects.filter(id=self.request.data.get('api')).first()

        if api_instance is not None:
            context['api_instance'] = api_instance
        return context


class ValidateAPIKeyViewSet(APIView):
    permission_classes = (
        core_permissions.IsUserAuthenticated,
    )

    def put(self, request, *args, **kwargs):
        request_user = self.request.user

        queryset = accounts_models.APIKey.objects.all().filter(
            Q(user=request_user) | Q(has_write_access=True),
            api__deleted__isnull=True,
            api_id=request.data.get('api_id'),
            api_key=request.data.get('api_key'),
            status=accounts_models.APIKey.APPROVED,
            valid_to__gte=core_utilities.get_current_datetime_object().date(),
        )

        if queryset.exists():
            all_categories = list(queryset.filter(
                active_categories__deleted__isnull=True,
                active_categories__api_category__deleted__isnull=True,
            ).annotate(
                api_category_id=F('active_categories__api_category__id'),
                api_category_name=F('active_categories__api_category__name'),
                api_category_code=F('active_categories__api_category__code'),
                api_category_is_default=F('active_categories__api_category__is_default'),
            ).values('api_category_id', 'api_category_name', 'api_category_code', 'api_category_is_default'))
            return Response(status=rest_status.HTTP_200_OK, data=all_categories)
        return Response(status=rest_status.HTTP_404_NOT_FOUND, data={'detail': 'Please enter valid api key.'})


class TranslateTextFromEnViewSet(APIView):
    permission_classes = (
        permissions.AllowAny,
    )
    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'TRANSLATED_TEXT'

    def get_cache_key(self, request):
        pk = self.kwargs.get('target')

        payload = str(request.data)
        if payload:
            if len(payload) <= settings.AI_TRANSLATION_CACHE_KEY_LIMIT:
                return '{0}_{1}_{2}'.format(self.CACHE_KEY_PREFIX, pk, payload)
            else:
                return '{0}_{1}_{2}_{3}'.format(self.CACHE_KEY_PREFIX, pk, payload[:int(settings.AI_TRANSLATION_CACHE_KEY_LIMIT / 2)], payload[-int(settings.AI_TRANSLATION_CACHE_KEY_LIMIT / 2):])

    def prepare_azure_request(self, request, *args, **kwargs):
        # Add your key and endpoint
        key = settings.AI_TRANSLATION_KEY
        endpoint = settings.AI_TRANSLATION_ENDPOINT

        if not key or not endpoint:
            logger.error('Required environment variables are missing for Azure AI translation. AI_TRANSLATION_ENDPOINT, AI_TRANSLATION_KEY')
            return Response({'error': 'An error occurred while processing the request'}, status=500)

        if len(settings.AI_TRANSLATION_SUPPORTED_TARGETS) > 0 and self.kwargs.get('target') not in settings.AI_TRANSLATION_SUPPORTED_TARGETS:
            return Response({'error': 'Requested language target is not supported by the application.'}, status=500)

        payload = request.data
        if not payload:
            return Response({'error': 'Empty text can not be translated.'}, status=500)
        elif isinstance(payload, dict):
            text = request.data.get('text')
            if not text:
                return Response({'error': 'Empty text can not be translated.'}, status=500)
            payload = [payload,]

        path = 'translate'
        constructed_url = str(endpoint if str(endpoint).endswith('/') else endpoint + '/') + path

        params = {
            'api-version': '3.0',
            'from': 'en',
            'to': [self.kwargs.get('target', 'fr'),]
        }

        headers = {
            'Ocp-Apim-Subscription-Key': key,
            'Content-type': 'application/json',
            'X-ClientTraceId': str(uuid.uuid4())
        }

        # location, also known as region.
        # required if you're using a multi service or regional (not global) resource.
        # It can be found in the Azure portal on the Keys and Endpoint page.
        if settings.AI_TRANSLATION_REGION:
            headers['Ocp-Apim-Subscription-Region'] = settings.AI_TRANSLATION_REGION

        return requests.post(constructed_url, params=params, headers=headers, json=payload)

    def put(self, request, *args, **kwargs):
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key(request)

        response = None
        if use_cached_data and cache_key:
            response = no_expiry_cache_manager.get(cache_key)

        if not response:
            response = self.prepare_azure_request(request, *args, **kwargs)
            if response.status_code == rest_status.HTTP_200_OK:
                try:
                    response_json = response.json()
                    no_expiry_cache_manager.set(cache_key, response_json, request_path=request_path,
                                      soft_timeout=None)
                    response  = Response(data=response_json, status=rest_status.HTTP_200_OK)
                except requests.exceptions.InvalidJSONError as ex:
                    response = Response(data=ex.strerror, status=rest_status.HTTP_400_BAD_REQUEST)
        else:
            response = Response(data=response)
        return response


class NotificationViewSet(BaseModelViewSet):
    """
    NotificationViewSet
        This class is used to list all Messages/Notifications, send new notification.
        Inherits: BaseModelViewSet
    """
    model = accounts_models.Message

    serializer_class = serializers.MessageListSerializer

    action_serializers = {
        'create': serializers.SendNotificationSerializer,
    }

    base_auth_permissions = (
        permissions.AllowAny,
    )

    permission_classes = (
        core_permissions.CanViewMessages,
        core_permissions.CanDeleteMessages,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
    )

    ordering_fields = ('last_modified_at', 'type', 'severity',)
    apply_query_pagination = True

    filterset_fields = {
        'type': ['iexact', 'in', 'exact'],
        'severity': ['iexact', 'in', 'exact'],
    }

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        message_type = self.request.data.get('type')
        if message_type == accounts_models.Message.TYPE_NOTIFICATION:
            self.permission_classes = self.permission_classes + (core_permissions.CanSendMessages,)

        return super().get_permissions()


class InvalidateCache(APIView):
    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanCleanCache,
    )

    def get(self, request, *args, **kwargs):
        if request.query_params.get('hard', settings.INVALIDATE_CACHE_HARD).lower() == 'true':
            cache_manager.invalidate(hard=True)
            message = 'Cache cleared. Map is updated in real time.'
        else:
            cache_manager.invalidate()
            message = 'Cache invalidation started. Maps will be updated in a few minutes.'

        update_all_cached_values.delay()
        return Response(data={'message': message})


class InvalidateCacheByPattern(APIView):
    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanCleanCache,
    )

    def delete(self, request, *args, **kwargs):
        hard_delete = request.query_params.get('hard', settings.INVALIDATE_CACHE_HARD).lower() == 'true'
        payload = request.data
        if payload:
            cache_key_name = payload.get('key', 'all')
        else:
            cache_key_name = 'all'

        if cache_key_name == 'all':
            if hard_delete:
                cache_manager.invalidate(hard=True)
                message = 'Cache cleared. Map is updated in real time.'
            else:
                cache_manager.invalidate()
                message = 'Cache invalidation started. Maps will be updated in a few minutes.'

            update_all_cached_values.delay()
        else:
            keys = []

            if cache_key_name == 'country':
                country_id = payload.get('id', None)
                country_code = payload.get('code', None)
                keys = [
                    "*COUNTRIES_LIST_",
                    "*PUBLISHED_LAYERS_LIST_*",
                    "*GLOBAL_COUNTRY_SEARCH_MAPPING_",
                    "*country_id_\['{0}'\]*".format(country_id),
                    "*country_id_{0}*".format(country_id),
                    "*COUNTRY_INFO_pk_{0}".format(country_code),
                ]
            elif cache_key_name == 'layer':
                layer_id = payload.get('id', None)
                keys = [
                    "*PUBLISHED_LAYERS_LIST_*",
                    "*DATA_LAYER_INFO_{0}*".format(layer_id),
                    "*DATA_LAYER_MAP_{0}*".format(layer_id),
                    "*layer_id_\['{0}'\]*".format(layer_id),
                    "*layer_id_{0}*".format(layer_id),
                ]

            if hard_delete:
                cache_manager.invalidate_many(keys=keys, hard=True)
                message = 'Cache cleared. Map is updated in real time.'
            else:
                cache_manager.invalidate_many(keys=keys)
                message = 'Cache invalidation started. Maps will be updated in a few minutes.'

        return Response(data={'message': message})


class AppStaticConfigurationsViewSet(APIView):
    base_auth_permissions = (
        permissions.AllowAny,
    )

    def get(self, request, *args, **kwargs):
        static_data = {
            'API_CATEGORY_CHOICES': dict(accounts_models.API.API_CATEGORY_CHOICES),
            'API_KEY_STATUS_CHOICES': dict(accounts_models.APIKey.STATUS_CHOICES),
            'DATA_SOURCE_TYPE_CHOICES': dict(accounts_models.DataSource.DATA_SOURCE_TYPE_CHOICES),
            'DATA_SOURCE_STATUS_CHOICES': dict(accounts_models.DataSource.DATA_SOURCE_STATUS_CHOICES),
            'LAYER_TYPE_CHOICES': dict(accounts_models.DataLayer.LAYER_TYPE_CHOICES),
            'LAYER_CATEGORY_CHOICES': dict(accounts_models.DataLayer.LAYER_CATEGORY_CHOICES),
            'LAYER_STATUS_CHOICES': dict(accounts_models.DataLayer.STATUS_CHOICES),
            'MESSAGE_SEVERITY_TYPE_CHOICES': dict(accounts_models.Message.MESSAGE_SEVERITY_TYPE_CHOICES),
            'MESSAGE_MODE_CHOICES': dict(accounts_models.Message.MESSAGE_MODE_CHOICES),
            'PERMISSION_CHOICES': dict(auth_models.RolePermission.PERMISSION_CHOICES),
            'COVERAGE_TYPES': dict(statistics_models.SchoolWeeklyStatus.COVERAGE_TYPES),
            'FILTER_TYPE_CHOICES': dict(accounts_models.AdvanceFilter.FILTER_TYPE_CHOICES),
            'FILTER_QUERY_PARAM_CHOICES': dict(accounts_models.AdvanceFilter.FILTER_QUERY_PARAM_CHOICES),
            'FILTER_STATUS_CHOICES': dict(accounts_models.AdvanceFilter.STATUS_CHOICES),
            'CONTACT_MESSAGE_CATEGORY_CHOICES': dict(ContactMessage.CATEGORY_CHOICES),
        }

        return Response(data=static_data)


class DataSourceViewSet(BaseModelViewSet):
    model = accounts_models.DataSource
    serializer_class = serializers.DataSourceListSerializer

    action_serializers = {
        'create': serializers.CreateDataSourceSerializer,
        'partial_update': serializers.UpdateDataSourceSerializer,
    }

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanViewDataLayer,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
    )

    ordering_fields = ('status', 'last_modified_at', 'data_source_type', 'name',)
    filterset_fields = {
        'id': ['exact', 'in'],
        'status': ['iexact', 'in', 'exact'],
        'published_by_id': ['exact', 'in'],
        'data_source_type': ['iexact', 'in', 'exact'],
        'name': ['iexact', 'in', 'exact'],
    }

    permit_list_expands = ['created_by', 'published_by', 'last_modified_by']

    def perform_destroy(self, instance):
        """
        perform_destroy
        :param instance:
        :return:
        """
        instance.deleted = core_utilities.get_current_datetime_object()
        instance.last_modified_at = core_utilities.get_current_datetime_object()
        instance.last_modified_by = core_utilities.get_current_user(request=self.request)
        return super().perform_destroy(instance)


class DataSourcePublishViewSet(BaseModelViewSet):
    model = accounts_models.DataSource
    serializer_class = serializers.PublishDataSourceSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPublishDataLayer,
    )


class DataLayersViewSet(BaseModelViewSet):
    model = accounts_models.DataLayer
    serializer_class = serializers.DataLayersListSerializer

    action_serializers = {
        'create': serializers.CreateDataLayersSerializer,
        'partial_update': serializers.UpdateDataLayerSerializer,
    }

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanViewDataLayer,
        core_permissions.CanAddDataLayer,
        core_permissions.CanUpdateDataLayer,
        core_permissions.CanDeleteDataLayer,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
        SearchFilter,
    )

    ordering_field_names = ['-last_modified_at', 'name']
    apply_query_pagination = True

    filterset_fields = {
        'id': ['exact', 'in'],
        'status': ['iexact', 'in', 'exact'],
        'published_by_id': ['exact', 'in'],
        'name': ['iexact', 'in', 'exact'],
    }

    search_fields = ('name', 'code', 'type',)

    permit_list_expands = ['created_by', 'published_by', 'last_modified_by']

    def update_serializer_context(self, context):
        data_source_instances = []
        if self.request.data.get('data_sources_list'):
            data_source_instances = list(accounts_models.DataSource.objects.filter(
                id__in=self.request.data.get('data_sources_list')
            ))

        if len(data_source_instances) > 0:
            context['data_sources_list'] = data_source_instances
        return context

    def apply_queryset_filters(self, queryset):
        """
        Override if applying more complex filters to queryset.
        :param queryset:
        :return queryset:
        """

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

        if 'is_default' in query_param_keys:
            is_default = str(query_params['is_default']).lower() == 'true'
            queryset = queryset.filter(
                active_countries__is_default=is_default,
                active_countries__deleted__isnull=True,
            )

        return super().apply_queryset_filters(queryset)

    def perform_destroy(self, instance):
        """
        perform_destroy
        :param instance:
        :return:
        """
        instance.deleted = core_utilities.get_current_datetime_object()
        instance.last_modified_at = core_utilities.get_current_datetime_object()
        instance.last_modified_by = core_utilities.get_current_user(request=self.request)
        return super().perform_destroy(instance)


class DataLayerPublishViewSet(BaseModelViewSet):
    model = accounts_models.DataLayer
    serializer_class = serializers.PublishDataLayerSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPublishDataLayer,
    )


class DataLayerPreviewViewSet(APIView):
    model = accounts_models.DataLayer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPreviewDataLayer,
    )

    def get_column_function_sql(self, parameter_col_function):
        if isinstance(parameter_col_function, dict) and len(parameter_col_function) > 0:
            return parameter_col_function.get('sql').format(col_name='t."{col_name}"')
        return 'AVG(t."{col_name}")'

    def get_map_query(self, kwargs):
        query = """
        SELECT schools_school.id,
            CASE WHEN rt_status.rt_registered = True AND rt_status.rt_registration_date <= '{end_date}' THEN True
                    ELSE False
            END AS is_rt_connected,
            {case_conditions}
            CASE WHEN schools_school.connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN schools_school.connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END AS connectivity_status,
            ST_AsGeoJSON(ST_Transform(schools_school.geopoint, 4326)) AS geopoint
        FROM schools_school
        INNER JOIN connection_statistics_schoolweeklystatus sws ON schools_school.last_weekly_status_id = sws.id
        INNER JOIN connection_statistics_schoolrealtimeregistration rt_status ON rt_status.school_id = schools_school.id
        LEFT JOIN (
            SELECT "schools_school"."id" AS school_id,
                {col_function} AS "{col_name}"
            FROM "schools_school"
            INNER JOIN "connection_statistics_schooldailystatus" t ON "schools_school"."id" = t."school_id"
            WHERE (
                {country_condition}
                "schools_school"."deleted" IS NULL
                AND t."deleted" IS NULL
                AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                AND t."live_data_source" IN ({live_source_types})
            )
            GROUP BY "schools_school"."id"
            ORDER BY "schools_school"."id" ASC
        ) AS sds ON sds.school_id = schools_school.id
        WHERE schools_school."deleted" IS NULL
            AND rt_status."deleted" IS NULL
            AND rt_status."rt_registered" = True
            AND rt_status."rt_registration_date"::date <= '{end_date}'
        {country_condition_outer}
        ORDER BY random()
        LIMIT 1000
        """

        legend_configs = kwargs['legend_configs']
        if len(legend_configs) > 0 and 'SQL:' in str(legend_configs):
            label_cases = []
            for title, values_and_label in legend_configs.items():
                values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                     values_and_label.get('values', [])))

                if len(values) > 0:
                    is_sql_value = 'SQL:' in values[0]
                    if is_sql_value:
                        sql_statement = str(','.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    label_cases.append("ELSE '{label}'".format(label=title))

            kwargs['case_conditions'] = 'CASE ' + ' '.join(label_cases) + 'END AS connectivity,'
        else:
            kwargs['case_conditions'] = """
                        CASE WHEN sds.{col_name} > {benchmark_value} THEN 'good'
                            WHEN sds.{col_name} <= {benchmark_value} AND sds.{col_name} >= {base_benchmark} THEN 'moderate'
                            WHEN sds.{col_name} < {base_benchmark}  THEN 'bad'
                            ELSE 'unknown'
                        END AS connectivity,
                    """.format(**kwargs)

            if kwargs['is_reverse'] is True:
                kwargs['case_conditions'] = """
                            CASE WHEN sds.{col_name} < {benchmark_value}  THEN 'good'
                                WHEN sds.{col_name} >= {benchmark_value} AND sds.{col_name} <= {base_benchmark} THEN 'moderate'
                                WHEN sds.{col_name} > {base_benchmark} THEN 'bad'
                                ELSE 'unknown'
                            END AS connectivity,
                        """.format(**kwargs)

        if len(kwargs['country_ids']) > 0:
            kwargs['country_condition'] = '"schools_school"."country_id" IN ({0}) AND'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )
            kwargs['country_condition_outer'] = 'AND schools_school."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )
        else:
            kwargs['country_condition'] = ''
            kwargs['country_condition_outer'] = ''

        kwargs['col_function'] = kwargs['parameter_col_function_sql'].format(**kwargs)

        return query.format(**kwargs)


    def get_static_map_query(self, kwargs):
        query = """
            SELECT
                schools_school.id,
                schools_school.name,
                {table_name}."{col_name}",
                ST_AsGeoJSON(ST_Transform(schools_school.geopoint, 4326)) as geopoint,
                {label_case_statements}
            FROM schools_school
            INNER JOIN connection_statistics_schoolweeklystatus sws ON schools_school.last_weekly_status_id = sws.id
            WHERE schools_school."deleted" IS NULL {country_condition}
            ORDER BY random()
            LIMIT 1000
            """

        kwargs['country_condition'] = ''

        if len(kwargs['country_ids']) > 0:
            kwargs['country_condition'] = 'AND schools_school.country_id IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
        kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'sws')

        for title, values_and_label in legend_configs.items():
            values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                 values_and_label.get('values', [])))

            if len(values) > 0:
                is_sql_value = 'SQL:' in values[0]
                if is_sql_value:
                    sql_statement = str(','.join(values)).replace('SQL:', '').format(
                        table_name=kwargs['table_name'],
                        col_name=kwargs['col_name'],
                    )
                    label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    values_l.extend(values)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            """WHEN LOWER({table_name}."{col_name}") IN ({value}) THEN '{label}'""".format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            """WHEN {table_name}."{col_name}" IN ({value}) THEN '{label}'""".format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join([str(v) for v in values])
                            ))
            else:
                label_cases.append("ELSE '{label}'".format(label=title))

        kwargs['label_case_statements'] = 'CASE ' + ' '.join(label_cases) + 'END AS field_status'

        return query.format(**kwargs)


    def get(self, request, *args, **kwargs):
        data_layer_instance = get_object_or_404(accounts_models.DataLayer.objects.all(), pk=self.kwargs.get('pk'))
        data_sources = data_layer_instance.data_sources.all()

        country_ids = data_layer_instance.applicable_countries
        parameter_col = data_sources.first().data_source_column
        column_function_sql = self.get_column_function_sql(data_sources.first().data_source_column_function)

        parameter_column_name = str(parameter_col['name'])
        legend_configs = data_layer_instance.legend_configs

        if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            live_data_sources = ['UNKNOWN']
            for d in data_sources:
                source_type = d.data_source.data_source_type
                if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                    live_data_sources.append(statistics_configs.QOS_SOURCE)
                elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                    live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

            global_benchmark = data_layer_instance.global_benchmark.get('value')
            benchmark_base = str(parameter_col.get('base_benchmark', 1))

            data_layer_qs = statistics_models.SchoolDailyStatus.objects.all()
            if len(country_ids) > 0:
                data_layer_qs = data_layer_qs.filter(school__country__in=country_ids)

            date = core_utilities.get_current_datetime_object().date() - timedelta(days=6)

            latest_school_daily_instance = data_layer_qs.order_by('-date').first()
            if latest_school_daily_instance:
                date = latest_school_daily_instance.date

            start_date = date - timedelta(days=date.weekday())
            end_date = start_date + timedelta(days=6)
            query_kwargs = {
                'col_name': parameter_column_name,
                'benchmark_value': global_benchmark,
                'global_benchmark': global_benchmark,
                'base_benchmark': benchmark_base,
                'country_ids': country_ids,
                'start_date': start_date,
                'end_date': end_date,
                'live_source_types': ','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                'parameter_col': parameter_col,
                'parameter_col_function_sql': column_function_sql,
                'is_reverse': data_layer_instance.is_reverse,
                'legend_configs': legend_configs,
            }

            map_points = db_utilities.sql_to_response(self.get_map_query(query_kwargs), label=self.__class__.__name__)
        else:
            query_kwargs = {
                'col_name': parameter_column_name,
                'legend_configs': legend_configs,
                'country_ids': country_ids,
                'parameter_col': parameter_col,
            }

            map_points = db_utilities.sql_to_response(self.get_static_map_query(query_kwargs),
                                                      label=self.__class__.__name__)

        if map_points:
            for map_point in map_points:
                map_point['geopoint'] = json.loads(map_point['geopoint'])
        return Response(data={'map': map_points})


class PublishedDataLayersViewSet(CachedListMixin, BaseModelViewSet):
    """
    PublishedDataLayersViewSet
    Cache Attr:
        Auto Cache: Not required
        Call Cache: Yes
    """
    LIST_CACHE_KEY_PREFIX = 'PUBLISHED_LAYERS_LIST'

    model = accounts_models.DataLayer
    serializer_class = serializers.DataLayersListSerializer

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

    permit_list_expands = ['created_by', 'published_by', 'last_modified_by']

    def apply_queryset_filters(self, queryset):
        """
        Override if applying more complex filters to queryset.
        :param queryset:
        :return queryset:
        """
        queryset = queryset.filter(status=self.kwargs.get('status', 'PUBLISHED'))

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

        if 'is_default' in query_param_keys:
            is_default = str(query_params['is_default']).lower() == 'true'
            queryset = queryset.filter(
                active_countries__is_default=is_default,
                active_countries__deleted__isnull=True,
            )

        return super().apply_queryset_filters(queryset)


class DataLayerMetadataViewSet(BaseModelViewSet):
    model = accounts_models.DataLayer

    serializer_class = serializers.DataLayersListSerializer

    base_auth_permissions = (
        permissions.AllowAny,
    )

    permit_list_expands = ['created_by', 'published_by', 'last_modified_by']

    def get_object(self):
        return get_object_or_404(
            accounts_models.DataLayer.objects.all(),
            pk=self.kwargs.get('pk'),
            status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
        )


class BaseDataLayerAPIViewSet(APIView):
    model = accounts_models.DataLayer

    permission_classes = (
        permissions.AllowAny,
    )

    def update_kwargs(self, country_ids, layer_instance):
        query_params = self.request.query_params.dict()
        query_param_keys = query_params.keys()

        if 'start_date' in query_param_keys:
            self.kwargs['start_date'] = date_utilities.to_date(query_params['start_date']).date()
        elif layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            date = core_utilities.get_current_datetime_object() - timedelta(days=7)
            self.kwargs['start_date'] = (date - timedelta(days=date.weekday())).date()

        if 'end_date' in query_param_keys:
            self.kwargs['end_date'] = date_utilities.to_date(query_params['end_date']).date()
        elif layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            date = core_utilities.get_current_datetime_object() - timedelta(days=7)
            self.kwargs['end_date'] = ((date - timedelta(days=date.weekday())) + timedelta(days=6)).date()

        if 'country_id' in query_param_keys:
            self.kwargs['country_ids'] = [query_params['country_id']]
        elif 'country_id__in' in query_param_keys:
            self.kwargs['country_ids'] = [c_id.strip() for c_id in query_params['country_id__in'].split(',')]
        elif len(country_ids) > 0:
            self.kwargs['country_ids'] = country_ids

        if 'admin1_id' in query_param_keys:
            self.kwargs['admin1_ids'] = [query_params['admin1_id']]
        elif 'admin1_id__in' in query_param_keys:
            self.kwargs['admin1_ids'] = [a_id.strip() for a_id in query_params['admin1_id__in'].split(',')]

        if 'school_id' in query_param_keys:
            self.kwargs['school_ids'] = [str(query_params['school_id']).strip()]
        elif 'school_id__in' in query_param_keys:
            self.kwargs['school_ids'] = [s_id.strip() for s_id in query_params['school_id__in'].split(',')]

        self.kwargs['is_weekly'] = False if query_params.get('is_weekly', 'true') == 'false' else True
        self.kwargs['benchmark'] = 'national' if query_params.get('benchmark', 'global') == 'national' else 'global'

        self.kwargs['convert_unit'] = layer_instance.global_benchmark.get('convert_unit', 'mbps')
        self.kwargs['is_reverse'] = layer_instance.is_reverse

        self.kwargs['school_filters'] = core_utilities.get_filter_sql(
            self.request, 'schools', 'schools_school')
        self.kwargs['school_static_filters'] = core_utilities.get_filter_sql(
            self.request, 'school_static', 'connection_statistics_schoolweeklystatus')

    def get_benchmark_value(self, data_layer_instance):
        benchmark_val = data_layer_instance.global_benchmark.get('value')
        benchmark_unit = data_layer_instance.global_benchmark.get('unit')

        if self.kwargs['benchmark'] == 'national':
            country_ids = self.kwargs.get('country_ids', [])
            if len(country_ids) > 0:
                benchmark_metadata = Country.objects.all().filter(
                    id__in=country_ids,
                    benchmark_metadata__isnull=False,
                ).order_by('id').values_list('benchmark_metadata', flat=True).first()

                if benchmark_metadata and len(benchmark_metadata) > 0:
                    benchmark_metadata = json.loads(benchmark_metadata)
                    data_layer_type = data_layer_instance.type
                    if data_layer_type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
                        all_live_layers = benchmark_metadata.get('live_layer', {})
                        if len(all_live_layers) > 0 and str(data_layer_instance.id) in (all_live_layers.keys()):
                            benchmark_val = all_live_layers[str(data_layer_instance.id)]
                    else:
                        all_static_layers = benchmark_metadata.get('static_layer', {})
                        if len(all_static_layers) > 0 and str(data_layer_instance.id) in (all_static_layers.keys()):
                            benchmark_val = all_static_layers[str(data_layer_instance.id)]

        return benchmark_val, benchmark_unit

    def get_legend_configs(self, data_layer_instance):
        legend_configs = data_layer_instance.legend_configs

        if self.kwargs['benchmark'] == 'national':
            country_ids = self.kwargs.get('country_ids', [])
            if len(country_ids) > 0:
                legend_configurations = Country.objects.all().filter(
                    id__in=country_ids,
                    active_layers__deleted__isnull=True,
                    active_layers__data_layer_id=data_layer_instance.id,
                ).order_by('id').values_list('active_layers__legend_configs', flat=True).first()
                if legend_configurations and len(legend_configurations) > 0:
                    legend_configs = json.loads(legend_configurations)

        return legend_configs

    def get_column_function_sql(self, parameter_col_function):
        if isinstance(parameter_col_function, dict) and len(parameter_col_function) > 0:
            return parameter_col_function.get('sql').format(col_name='t."{col_name}"')
        return 'AVG(t."{col_name}")'


@method_decorator([
    custom_cache_control(
        public=True,
        max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE,
        cache_status_codes=[rest_status.HTTP_200_OK,],
    )
], name='dispatch')
class DataLayerInfoViewSet(BaseDataLayerAPIViewSet):
    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'DATA_LAYER_INFO'

    def get_cache_key(self):
        pk = self.kwargs.get('pk')
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}_{2}'.format(
            self.CACHE_KEY_PREFIX,
            pk,
            '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))),
        )

    def get_info_query(self):
        query = """
        SELECT {case_conditions}
            COUNT(DISTINCT CASE WHEN sds.{col_name} IS NOT NULL THEN sds.school_id ELSE NULL END)
                AS "school_with_realtime_data",
            {benchmark_value_sql}
            COUNT(DISTINCT sds.school_id) AS "no_of_schools_measure"
        FROM (
            SELECT "schools_school"."id" AS school_id,
                "schools_school"."last_weekly_status_id",
                {col_function} AS "{col_name}"
            FROM "schools_school"
            INNER JOIN "connection_statistics_schoolrealtimeregistration"
                ON ("schools_school"."id" = "connection_statistics_schoolrealtimeregistration"."school_id")
            {school_weekly_join}
            LEFT OUTER JOIN "connection_statistics_schooldailystatus" t
                ON (
                    "schools_school"."id" = t."school_id"
                    AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                    AND t."live_data_source" IN ({live_source_types})
                    AND t."deleted" IS NULL
                )
            WHERE (
                "schools_school"."deleted" IS NULL
                AND "connection_statistics_schoolrealtimeregistration"."deleted" IS NULL
                {country_condition}
                {admin1_condition}
                {school_condition}
                {school_weekly_condition}
                AND "connection_statistics_schoolrealtimeregistration"."rt_registered" = True
                AND "connection_statistics_schoolrealtimeregistration"."rt_registration_date"::date <= '{end_date}')
            GROUP BY "schools_school"."id"
            ORDER BY "schools_school"."id" ASC
        ) AS sds
        {school_weekly_outer_join}
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['school_condition'] = ''
        kwargs['school_weekly_join'] = ''
        kwargs['school_weekly_condition'] = ''
        kwargs['school_weekly_outer_join'] = ''
        kwargs['benchmark_value_sql'] = ''

        benchmark_value = kwargs['benchmark_value']
        if benchmark_value and 'SQL:' in benchmark_value:
            kwargs['benchmark_value_sql'] = benchmark_value.replace('SQL:', '').format(**kwargs) + ' AS benchmark_sql_value,'

        legend_configs = kwargs['legend_configs']
        if len(legend_configs) > 0 and 'SQL:' in str(legend_configs):
            label_cases = []
            for title, values_and_label in legend_configs.items():
                values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                     values_and_label.get('values', [])))

                if len(values) > 0:
                    is_sql_value = 'SQL:' in values[0]
                    if is_sql_value:
                        sql_statement = str(','.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN {sql} THEN sds.school_id ELSE NULL END) AS "{label}",'.format(
                                sql=sql_statement, label=title))
                else:
                    label_cases.append(
                        'COUNT(DISTINCT CASE WHEN sds.{col_name} IS NULL '
                        'THEN sds.school_id ELSE NULL END) AS "{label}",'.format(
                            col_name=kwargs['col_name'],label=title))

            kwargs['case_conditions'] = ' '.join(label_cases)

            kwargs['school_weekly_outer_join'] = """
            INNER JOIN "connection_statistics_schoolweeklystatus" sws ON sds."last_weekly_status_id" = sws."id"
            """
        else:
            kwargs['case_conditions'] = """
            COUNT(DISTINCT CASE WHEN sds.{col_name} > {benchmark_value} THEN sds.school_id ELSE NULL END) AS "good",
            COUNT(DISTINCT CASE WHEN (sds.{col_name} >= {base_benchmark} AND sds.{col_name} <= {benchmark_value})
                THEN sds.school_id ELSE NULL END) AS "moderate",
            COUNT(DISTINCT CASE WHEN sds.{col_name} < {base_benchmark} THEN sds.school_id ELSE NULL END) AS "bad",
            COUNT(DISTINCT CASE WHEN sds.{col_name} IS NULL THEN sds.school_id ELSE NULL END) AS "unknown",
            """.format(**kwargs)

            if kwargs['is_reverse'] is True:
                kwargs['case_conditions'] = """
                COUNT(DISTINCT CASE WHEN sds.{col_name} < {benchmark_value} THEN sds.school_id ELSE NULL END) AS "good",
                COUNT(DISTINCT CASE WHEN (sds.{col_name} >= {benchmark_value} AND sds.{col_name} <= {base_benchmark})
                    THEN sds.school_id ELSE NULL END) AS "moderate",
                COUNT(DISTINCT CASE WHEN sds.{col_name} > {base_benchmark} THEN sds.school_id ELSE NULL END) AS "bad",
                COUNT(DISTINCT CASE WHEN sds.{col_name} IS NULL THEN sds.school_id ELSE NULL END) AS "unknown",
                """.format(**kwargs)

        if len(kwargs.get('admin1_ids', [])) > 0:
            kwargs['admin1_condition'] = 'AND "schools_school"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
        elif len(kwargs.get('country_ids', [])) > 0:
            kwargs['country_condition'] = 'AND "schools_school"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] = ' AND ' + kwargs['school_filters']

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            INNER JOIN "connection_statistics_schoolweeklystatus"
                ON "schools_school"."last_weekly_status_id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = ' AND ' + kwargs['school_static_filters']

        kwargs['col_function'] = kwargs['parameter_col_function_sql'].format(**kwargs)

        return query.format(**kwargs)

    def get_school_view_info_query(self):
        query = """
        SELECT DISTINCT schools_school."id",
            schools_school."name",
            schools_school."external_id",
            schools_school."giga_id_school",
            CASE WHEN srr."rt_registered" = True THEN true ELSE false END AS is_data_synced,
            schools_school."admin1_id",
            adm1_metadata."name" AS admin1_name,
            adm1_metadata."giga_id_admin" AS admin1_code,
            adm1_metadata."description_ui_label" AS admin1_description_ui_label,
            schools_school."admin2_id",
            adm2_metadata."name" AS admin2_name,
            adm2_metadata."giga_id_admin" AS admin2_code,
            adm2_metadata."description_ui_label" AS admin2_description_ui_label,
            schools_school."country_id",
            c."name" AS country_name,
            ST_AsGeoJSON(ST_Transform(schools_school."geopoint", 4326)) AS geopoint,
            schools_school."environment",
            schools_school."education_level",
            ROUND(sds."{col_name}"::numeric, 2) AS "live_avg",
            sws."download_speed_benchmark",
            CASE WHEN schools_school.connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN schools_school.connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END AS connectivity_status,
            CASE WHEN srr."rt_registered" = True AND srr."rt_registration_date"::date <= '{end_date}' THEN true
            ELSE false END AS is_rt_connected,
            {benchmark_value_sql}
            {case_conditions}
        FROM "schools_school" schools_school
        INNER JOIN public.locations_country c ON c."id" = schools_school."country_id"
        INNER JOIN "connection_statistics_schoolweeklystatus" sws ON schools_school."last_weekly_status_id" = sws."id"
        LEFT JOIN public.locations_countryadminmetadata AS adm1_metadata
            ON adm1_metadata."id" = schools_school.admin1_id
            AND adm1_metadata."layer_name" = 'adm1'
            AND adm1_metadata."deleted" IS NULL
        LEFT JOIN public.locations_countryadminmetadata AS adm2_metadata
            ON adm2_metadata."id" = schools_school.admin2_id
            AND adm2_metadata."layer_name" = 'adm2'
            AND adm2_metadata."deleted" IS NULL
        LEFT JOIN "connection_statistics_schoolrealtimeregistration" AS srr
            ON schools_school."id" = srr."school_id"
            AND srr."deleted" IS NULL
        LEFT JOIN (
            SELECT "schools_school"."id" AS school_id,
                {col_function} AS "{col_name}"
            FROM "schools_school"
            LEFT OUTER JOIN "connection_statistics_schooldailystatus" t
                ON (
                    "schools_school"."id" = t."school_id"
                    AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                    AND t."live_data_source" IN ({live_source_types})
                    AND t."deleted" IS NULL
                )
            WHERE ("schools_school"."id" IN ({ids})
                AND "schools_school"."deleted" IS NULL)
            GROUP BY "schools_school"."id"
            ORDER BY "schools_school"."id" ASC
        ) AS sds ON sds.school_id = schools_school.id
        WHERE "schools_school"."id" IN ({ids})
            AND c."deleted" IS NULL
            AND schools_school."deleted" IS NULL
        GROUP BY schools_school."id", srr."rt_registered", srr."rt_registration_date",
            adm1_metadata."name", adm1_metadata."description_ui_label",
            adm2_metadata."name", adm2_metadata."description_ui_label",
            c."name", adm1_metadata."giga_id_admin", adm2_metadata."giga_id_admin",
            sds."{col_name}", sws."download_speed_benchmark"
        ORDER BY schools_school."id" ASC
        """

        kwargs = copy.deepcopy(self.kwargs)
        kwargs['ids'] = ','.join(kwargs['school_ids'])

        kwargs['benchmark_value_sql'] = ''
        benchmark_value = kwargs['benchmark_value']
        if benchmark_value and 'SQL:' in benchmark_value:
            kwargs['benchmark_value_sql'] = benchmark_value.replace('SQL:', '').format(
                **kwargs) + ' AS benchmark_sql_value,'

        legend_configs = kwargs['legend_configs']
        if len(legend_configs) > 0 and 'SQL:' in str(legend_configs):
            label_cases = []
            for title, values_and_label in legend_configs.items():
                values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                     values_and_label.get('values', [])))

                if len(values) > 0:
                    is_sql_value = 'SQL:' in values[0]
                    if is_sql_value:
                        sql_statement = str(','.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    label_cases.append("ELSE '{label}'".format(label=title))

            kwargs['case_conditions'] = 'CASE ' + ' '.join(label_cases) + 'END AS live_avg_connectivity'
        else:
            kwargs['case_conditions'] = """
            CASE
                WHEN sds."{col_name}" > {benchmark_value} THEN 'good'
                WHEN (sds."{col_name}" >= {base_benchmark} AND sds."{col_name}" <= {benchmark_value})
                    THEN 'moderate'
                WHEN sds."{col_name}" < {base_benchmark} THEN 'bad'
                ELSE 'unknown' END AS live_avg_connectivity
            """.format(**kwargs)

            if kwargs['is_reverse'] is True:
                kwargs['case_conditions'] = """
                CASE
                    WHEN sds."{col_name}" < {benchmark_value} THEN 'good'
                    WHEN (sds."{col_name}" >= {benchmark_value} AND sds."{col_name}" <= {base_benchmark})
                        THEN 'moderate'
                    WHEN sds."{col_name}" > {base_benchmark} THEN 'bad'
                    ELSE 'unknown' END AS live_avg_connectivity
                """.format(**kwargs)

        kwargs['col_function'] = kwargs['parameter_col_function_sql'].format(**kwargs)

        return query.format(**kwargs)

    def get_school_view_statistics_info_query(self):
        query = """
        SELECT sws.*
        FROM "schools_school"
        INNER JOIN connection_statistics_schoolweeklystatus sws
            ON "schools_school"."last_weekly_status_id" = sws."id"
        WHERE "schools_school"."deleted" IS NULL
            AND "schools_school"."id" IN ({ids})
        """.format(ids=','.join(self.kwargs['school_ids']))

        return query

    def get_live_avg(self, function_name, positive_speeds):
        live_avg = 0

        if len(positive_speeds) == 0:
            return live_avg

        if function_name == 'avg':
            live_avg = round(sum(positive_speeds) / len(positive_speeds), 2)
        elif function_name == 'min':
            live_avg = round(min(positive_speeds), 2)
        elif function_name == 'max':
            live_avg = round(max(positive_speeds), 2)
        elif function_name == 'sum':
            live_avg = round(sum(positive_speeds), 2)
        elif str(function_name).startswith('median'):
            import numpy as np

            positive_speeds = list(sorted(positive_speeds))

            percentile_val = (str(function_name.split('|')[-1])).strip()
            if percentile_val:
                live_avg = round(np.percentile(positive_speeds, int(percentile_val)), 2)
            else:
                live_avg = np.median(positive_speeds)

        return live_avg

    def get_avg_query(self, **kwargs):
        query = """
        SELECT {school_selection}t."date" AS date,
            {col_function} AS "field_avg"
        FROM "schools_school"
        INNER JOIN "connection_statistics_schoolrealtimeregistration" ON
            "connection_statistics_schoolrealtimeregistration"."school_id" = "schools_school"."id"
        INNER JOIN "connection_statistics_schooldailystatus" t ON "schools_school"."id" = t."school_id"
        {school_weekly_join}
        WHERE (
            {country_condition}
            {admin1_condition}
            {school_condition}
            {school_weekly_condition}
            "connection_statistics_schoolrealtimeregistration"."deleted" IS NULL
            AND "connection_statistics_schoolrealtimeregistration"."rt_registered" = True
            AND "connection_statistics_schoolrealtimeregistration"."rt_registration_date"::date <= '{end_date}'
            AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
            AND t."live_data_source" IN ({live_source_types})
            AND t."deleted" IS NULL
            AND t."{col_name}" IS NOT NULL
        )
        GROUP BY t."date"{school_group_by}
        ORDER BY t."date" ASC
        """

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['school_condition'] = ''
        kwargs['school_selection'] = ''
        kwargs['school_group_by'] = ''
        kwargs['school_weekly_join'] = ''
        kwargs['school_weekly_condition'] = ''

        if len(kwargs.get('school_ids', [])) > 0:
            kwargs['school_condition'] = '"schools_school"."id" IN ({0}) AND '.format(','.join(kwargs['school_ids']))
            kwargs['school_selection'] = '"schools_school"."id", '
            kwargs['school_group_by'] = ', "schools_school"."id"'
        elif len(kwargs.get('admin1_ids', [])) > 0:
            kwargs['admin1_condition'] = '"schools_school"."admin1_id" IN ({0}) AND'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
        elif len(kwargs.get('country_ids', [])) > 0:
            kwargs['country_condition'] = '"schools_school"."country_id" IN ({0}) AND'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] += kwargs['school_filters'] + ' AND '

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            INNER JOIN "connection_statistics_schoolweeklystatus"
                ON "schools_school"."last_weekly_status_id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = kwargs['school_static_filters'] + ' AND '

        kwargs['col_function'] = kwargs['parameter_col_function_sql'].format(**kwargs)

        return query.format(**kwargs)

    def generate_graph_data(self):
        kwargs = copy.deepcopy(self.kwargs)

        # Get the daily connectivity_speed for the given country from SchoolDailyStatus model
        data = db_utilities.sql_to_response(self.get_avg_query(**kwargs), label=self.__class__.__name__, db_var=settings.READ_ONLY_DB_KEY)

        # Generate the graph data in the desired format
        graph_data = []
        current_date = kwargs['start_date']

        while current_date <= kwargs['end_date']:
            graph_data.append({
                'group': 'Speed',
                'key': date_utilities.format_date(current_date),
                'value': None  # Default value, will be updated later if data exists for the date
            })
            current_date += timedelta(days=1)

        round_unit_value = kwargs['round_unit_value']

        if len(kwargs.get('school_ids', [])) > 0:
            graph_data_per_school = {}
            all_positive_speeds_per_school = {}

            for school_id in kwargs.get('school_ids', []):
                graph_data_per_school[school_id] = copy.deepcopy(graph_data)
                all_positive_speeds_per_school[school_id] = []

            # Update the graph_data with actual values if they exist
            for daily_avg_data in data:
                school_id = str(daily_avg_data['id'])
                formatted_date = date_utilities.format_date(daily_avg_data['date'])
                school_graph_data = graph_data_per_school[school_id]
                school_all_positive_speeds = all_positive_speeds_per_school[school_id]
                for entry in school_graph_data:
                    if entry['key'] == formatted_date:
                        try:
                            rounded_speed = 0
                            if daily_avg_data['field_avg'] is not None:
                                rounded_speed = round(eval(round_unit_value.format(val=daily_avg_data['field_avg'])), 2)
                            entry['value'] = rounded_speed
                            school_all_positive_speeds.append(rounded_speed)
                        except (KeyError, TypeError):
                            pass
                graph_data_per_school[school_id] = school_graph_data
                all_positive_speeds_per_school[school_id] = school_all_positive_speeds
            return graph_data_per_school, all_positive_speeds_per_school

        all_positive_speeds = []
        # Update the graph_data with actual values if they exist
        for daily_avg_data in data:
            formatted_date = date_utilities.format_date(daily_avg_data['date'])
            for entry in graph_data:
                if entry['key'] == formatted_date:
                    try:
                        rounded_speed = 0
                        if daily_avg_data['field_avg'] is not None:
                            rounded_speed = round(eval(round_unit_value.format(val=daily_avg_data['field_avg'])), 2)
                        entry['value'] = rounded_speed
                        all_positive_speeds.append(rounded_speed)
                    except (KeyError, TypeError):
                        pass
        return graph_data, all_positive_speeds

    def get_static_info_query(self, query_labels):
        query = """
        SELECT {label_case_statements}
            COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" IS NOT NULL THEN "schools_school"."id" ELSE NULL END)
            AS "total_schools"
        FROM "schools_school"
        INNER JOIN connection_statistics_schoolweeklystatus sws ON "schools_school"."last_weekly_status_id" = sws."id"
        {school_weekly_join}
        WHERE "schools_school"."deleted" IS NULL
        {country_condition}
        {admin1_condition}
        {school_condition}
        {school_weekly_condition}
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['school_condition'] = ''
        kwargs['school_weekly_join'] = ''
        kwargs['school_weekly_condition'] = ''

        if len(kwargs.get('admin1_ids', [])) > 0:
            kwargs['admin1_condition'] = ' AND "schools_school"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
        elif len(kwargs.get('country_ids', [])) > 0:
            kwargs['country_condition'] = ' AND "schools_school"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] = ' AND ' + kwargs['school_filters']

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            INNER JOIN "connection_statistics_schoolweeklystatus"
                ON sws."id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = ' AND ' + kwargs['school_static_filters']

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
        kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'sws')
        is_sql_value = False

        for title, values_and_label in legend_configs.items():
            values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                 values_and_label.get('values', [])))
            label = values_and_label.get('labels', title).strip()
            query_labels.append(label)

            if len(values) > 0:
                is_sql_value = 'SQL:' in values[0]
                if is_sql_value:
                    sql_statement = str(','.join(values)).replace('SQL:', '').format(
                        table_name=kwargs['table_name'],
                        col_name=kwargs['col_name'],
                    )
                    label_cases.append(
                        'COUNT(DISTINCT CASE WHEN {sql} THEN schools_school."id" ELSE NULL END) AS "{label}",'.format(
                            sql=sql_statement,
                            label=label,
                        ))
                else:
                    values_l.extend(values)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN LOWER({table_name}."{col_name}") IN ({value}) '
                            'THEN schools_school."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" IN ({value}) '
                            'THEN schools_school."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join([str(v) for v in values])
                            ))
            else:
                if is_sql_value:
                    label_cases.append(
                        'COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" IS NULL THEN schools_school."id" ELSE NULL END) '
                        'AS "{label}",'.format(
                            table_name=kwargs['table_name'],
                            col_name=kwargs['col_name'],
                            label=label,
                        ))
                else:
                    values = set(values_l)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN LOWER({table_name}."{col_name}") NOT IN ({value}) '
                            'THEN schools_school."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" NOT IN ({value}) '
                            'THEN schools_school."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join([str(v) for v in values])
                            ))

        kwargs['label_case_statements'] = ' '.join(label_cases)

        return query.format(**kwargs)

    def get_static_school_view_info_query(self):
        query = """
        SELECT schools_school."id",
            schools_school."name",
            schools_school."external_id",
            schools_school."giga_id_school",
            schools_school."country_id",
            c."name" AS country_name,
            schools_school."admin1_id",
            adm1_metadata."name" AS admin1_name,
            adm1_metadata."giga_id_admin" AS admin1_code,
            adm1_metadata."description_ui_label" AS admin1_description_ui_label,
            schools_school."admin2_id",
            adm2_metadata."name" AS admin2_name,
            adm2_metadata."giga_id_admin" AS admin2_code,
            adm2_metadata."description_ui_label" AS admin2_description_ui_label,
            schools_school."environment",
            schools_school."education_level",
            {table_name}."{col_name}" AS field_value,
            {label_case_statements}
            ST_AsGeoJSON(ST_Transform(schools_school."geopoint", 4326)) AS geopoint,
            CASE WHEN schools_school.connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN schools_school.connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END as connectivity_status
        FROM "schools_school"
        INNER JOIN locations_country c ON c.id = schools_school.country_id
        INNER JOIN connection_statistics_schoolweeklystatus sws ON schools_school.last_weekly_status_id = sws.id
        LEFT JOIN locations_countryadminmetadata AS adm1_metadata
            ON adm1_metadata."id" = schools_school.admin1_id
            AND adm1_metadata."layer_name" = 'adm1'
            AND adm1_metadata."deleted" IS NULL
        LEFT JOIN locations_countryadminmetadata AS adm2_metadata
            ON adm2_metadata."id" = schools_school.admin2_id
            AND adm2_metadata."layer_name" = 'adm2'
            AND adm2_metadata."deleted" IS NULL
        WHERE "schools_school"."id" IN ({ids})
            AND c."deleted" IS NULL
        """

        kwargs = copy.deepcopy(self.kwargs)
        kwargs['ids'] = ','.join(kwargs['school_ids'])

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
        kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'sws')

        for title, values_and_label in legend_configs.items():
            values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                 values_and_label.get('values', [])))

            if len(values) > 0:
                is_sql_value = 'SQL:' in values[0]
                if is_sql_value:
                    sql_statement = str(','.join(values)).replace('SQL:', '').format(
                        table_name=kwargs['table_name'],
                        col_name=kwargs['col_name'],
                    )
                    label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    values_l.extend(values)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            """WHEN LOWER({table_name}."{col_name}") IN ({value}) THEN '{label}'""".format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            """WHEN {table_name}."{col_name}" IN ({value}) THEN '{label}'""".format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join([str(v) for v in values])
                            ))
            else:
                label_cases.append("ELSE '{label}'".format(label=title))

        kwargs['label_case_statements'] = 'CASE ' + ' '.join(label_cases) + 'END AS field_status,'

        return query.format(**kwargs)

    def get(self, request, *args, **kwargs):
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key()

        response = None
        if use_cached_data:
            response = cache_manager.get(cache_key)

        if not response:
            data_layer_instance = get_object_or_404(
                accounts_models.DataLayer.objects.all(),
                pk=self.kwargs.get('pk'),
                status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
            )

            data_sources = data_layer_instance.data_sources.all()

            live_data_sources = ['UNKNOWN']

            for d in data_sources:
                source_type = d.data_source.data_source_type
                if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                    live_data_sources.append(statistics_configs.QOS_SOURCE)
                elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                    live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

            country_ids = data_layer_instance.applicable_countries
            parameter_col = data_sources.first().data_source_column
            parameter_col_function = data_sources.first().data_source_column_function
            column_function_sql = self.get_column_function_sql(parameter_col_function)

            parameter_column_name = str(parameter_col['name'])
            parameter_column_unit = str(parameter_col.get('unit', '')).lower()
            base_benchmark = str(parameter_col.get('base_benchmark', 1))
            display_unit = parameter_col.get('display_unit', '')

            self.update_kwargs(country_ids, data_layer_instance)
            benchmark_value, benchmark_unit = self.get_benchmark_value(data_layer_instance)
            global_benchmark = data_layer_instance.global_benchmark.get('value')

            legend_configs = self.get_legend_configs(data_layer_instance)

            unit_agg_str = '{val}'

            if (
                self.kwargs['convert_unit'] and
                not core_utilities.is_blank_string(parameter_column_unit) and
                self.kwargs['convert_unit'].lower() != parameter_column_unit
            ):
                convert_unit = self.kwargs['convert_unit'].lower()

                if convert_unit == 'mbps' and parameter_column_unit == 'bps':
                    unit_agg_str = '{val} / (1000 * 1000)'
                elif convert_unit == 'mbps' and parameter_column_unit == 'kbps':
                    unit_agg_str = '{val} / 1000'
                elif convert_unit == 'kbps' and parameter_column_unit == 'bps':
                    unit_agg_str = '{val} / 1000'
                elif convert_unit == 'kbps' and parameter_column_unit == 'mbps':
                    unit_agg_str = '{val} * 1000'
                elif convert_unit == 'bps' and parameter_column_unit == 'kbps':
                    unit_agg_str = '{val} * 1000'
                elif convert_unit == 'bps' and parameter_column_unit == 'mbps':
                    unit_agg_str = '{val} * 1000 * 1000'

            self.kwargs['round_unit_value'] = unit_agg_str

            if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:

                self.kwargs.update({
                    'col_name': parameter_column_name,
                    'benchmark_value': benchmark_value,
                    'global_benchmark': global_benchmark,
                    'national_benchmark': benchmark_value,
                    'base_benchmark': base_benchmark,
                    'live_source_types': ','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                    'parameter_col': parameter_col,
                    'parameter_col_function_sql': column_function_sql,
                    'is_reverse': data_layer_instance.is_reverse,
                    'legend_configs': legend_configs,
                })

                if len(self.kwargs.get('school_ids', [])) > 0:
                    info_panel_school_list = db_utilities.sql_to_response(self.get_school_view_info_query(),
                                                                          label=self.__class__.__name__,
                                                                          db_var=settings.READ_ONLY_DB_KEY)
                    statistics = db_utilities.sql_to_response(self.get_school_view_statistics_info_query(),
                                                              label=self.__class__.__name__,
                                                              db_var=settings.READ_ONLY_DB_KEY)
                    graph_data, positive_speeds = self.generate_graph_data()

                    if len(info_panel_school_list) > 0:
                        for info_panel_school in info_panel_school_list:
                            info_panel_school['geopoint'] = json.loads(info_panel_school['geopoint'])
                            info_panel_school['statistics'] = list(filter(
                                lambda s: s['school_id'] == info_panel_school['id'], statistics))[-1]

                            # live_avg = (round(sum(positive_speeds[str(info_panel_school['id'])]) / len(
                            #     positive_speeds[str(info_panel_school['id'])]), 2) if len(
                            #     positive_speeds[str(info_panel_school['id'])]) > 0 else 0)

                            info_panel_school['live_avg'] = self.get_live_avg(
                                parameter_col_function.get('name', 'avg'),
                                positive_speeds[str(info_panel_school['id'])]
                            )
                            info_panel_school['graph_data'] = graph_data[str(info_panel_school['id'])]

                            benchmark_value_from_sql = info_panel_school.get('benchmark_sql_value', None)
                            if benchmark_value_from_sql:
                                rounded_benchmark_value_int = round(
                                    eval(unit_agg_str.format(
                                        val=core_utilities.convert_to_int(benchmark_value_from_sql))), 2)
                                benchmark_value = str(benchmark_value_from_sql)
                            else:
                                rounded_benchmark_value_int = round(
                                    eval(unit_agg_str.format(val=core_utilities.convert_to_int(benchmark_value))), 2)

                            info_panel_school['benchmark_metadata'] = {
                                'benchmark_value': benchmark_value,
                                'rounded_benchmark_value': rounded_benchmark_value_int,
                                'benchmark_unit': benchmark_unit,
                                'base_benchmark': base_benchmark,
                                'parameter_column_unit': parameter_column_unit,
                                'round_unit_value': unit_agg_str,
                                'convert_unit': self.kwargs.get('convert_unit'),
                                'display_unit': display_unit,
                            }

                    response = info_panel_school_list
                else:
                    is_data_synced_qs = SchoolWeeklyStatus.objects.filter(
                        school__realtime_registration_status__rt_registered=True,
                    )

                    if len(self.kwargs['school_filters']) > 0:
                        is_data_synced_qs = is_data_synced_qs.extra(where=[self.kwargs['school_filters']])

                    if len(self.kwargs['school_static_filters']) > 0:
                        is_data_synced_qs = is_data_synced_qs.extra(where=[self.kwargs['school_static_filters']])

                    if len(self.kwargs.get('admin1_ids', [])) > 0:
                        is_data_synced_qs = is_data_synced_qs.filter(school__admin1_id__in=self.kwargs['admin1_ids'])
                    elif len(self.kwargs.get('country_ids', [])) > 0:
                        is_data_synced_qs = is_data_synced_qs.filter(school__country_id__in=self.kwargs['country_ids'])

                    query_response = db_utilities.sql_to_response(self.get_info_query(),
                                                                  label=self.__class__.__name__,
                                                                  db_var=settings.READ_ONLY_DB_KEY)[-1]

                    graph_data, positive_speeds = self.generate_graph_data()
                    live_avg = self.get_live_avg(
                        parameter_col_function.get('name', 'avg'),
                        positive_speeds
                    )

                    live_avg_connectivity = 'unknown'

                    benchmark_value_from_sql = query_response.get('benchmark_sql_value', None)
                    if benchmark_value_from_sql:
                        rounded_benchmark_value_int = round(
                                eval(unit_agg_str.format(val=core_utilities.convert_to_int(benchmark_value_from_sql))), 2)
                        benchmark_value = str(benchmark_value_from_sql)
                    else:
                        rounded_benchmark_value_int = round(
                            eval(unit_agg_str.format(val=core_utilities.convert_to_int(benchmark_value))), 2)

                    rounded_base_benchmark_int = round(
                        eval(unit_agg_str.format(val=core_utilities.convert_to_int(base_benchmark))), 2)

                    if data_layer_instance.is_reverse:
                        if live_avg < rounded_benchmark_value_int:
                            live_avg_connectivity = 'good'
                        elif rounded_benchmark_value_int <= live_avg <= rounded_base_benchmark_int:
                            live_avg_connectivity = 'moderate'
                        elif live_avg > rounded_base_benchmark_int:
                            live_avg_connectivity = 'bad'
                    else:
                        if live_avg > rounded_benchmark_value_int:
                            live_avg_connectivity = 'good'
                        elif rounded_base_benchmark_int <= live_avg <= rounded_benchmark_value_int:
                            live_avg_connectivity = 'moderate'
                        elif live_avg < rounded_base_benchmark_int:
                            live_avg_connectivity = 'bad'

                    response = {
                        'no_of_schools_measure': query_response['no_of_schools_measure'],
                        'school_with_realtime_data': query_response['school_with_realtime_data'],
                        'real_time_connected_schools': {
                            'good': query_response['good'],
                            'moderate': query_response['moderate'],
                            'no_internet': query_response['bad'],
                            'unknown': query_response['unknown'],
                        },
                        'is_data_synced': is_data_synced_qs.exists(),
                        'live_avg': live_avg,
                        'live_avg_connectivity': live_avg_connectivity,
                        'graph_data': graph_data,
                        'benchmark_metadata': {
                            'benchmark_value': benchmark_value,
                            'rounded_benchmark_value': rounded_benchmark_value_int,
                            'benchmark_unit': benchmark_unit,
                            'base_benchmark': base_benchmark,
                            'parameter_column_unit': parameter_column_unit,
                            'round_unit_value': unit_agg_str,
                            'convert_unit': self.kwargs.get('convert_unit'),
                            'display_unit': display_unit,
                        },
                    }
            else:
                self.kwargs.update({
                    'col_name': parameter_column_name,
                    'legend_configs': legend_configs,
                    'parameter_col': parameter_col,
                })

                if len(self.kwargs.get('school_ids', [])) > 0:
                    info_panel_school_list = db_utilities.sql_to_response(self.get_static_school_view_info_query(),
                                                                          label=self.__class__.__name__,
                                                                          db_var=settings.READ_ONLY_DB_KEY)
                    statistics = db_utilities.sql_to_response(self.get_school_view_statistics_info_query(),
                                                              label=self.__class__.__name__,
                                                              db_var=settings.READ_ONLY_DB_KEY)

                    if len(info_panel_school_list) > 0:
                        for info_panel_school in info_panel_school_list:
                            info_panel_school['geopoint'] = json.loads(info_panel_school['geopoint'])
                            info_panel_school['statistics'] = list(filter(
                                lambda s: s['school_id'] == info_panel_school['id'], statistics))[-1]

                    response = info_panel_school_list
                else:
                    query_labels = []
                    query_response = db_utilities.sql_to_response(self.get_static_info_query(query_labels),
                                                                  label=self.__class__.__name__,
                                                                  db_var=settings.READ_ONLY_DB_KEY)[-1]
                    response = {
                        'total_schools': query_response['total_schools'],
                        'connected_schools': {label: query_response[label] for label in query_labels},
                        'legend_configs': legend_configs,
                        'benchmark_metadata': {
                            'parameter_column_unit': parameter_column_unit,
                            'display_unit': display_unit,
                        },
                    }

            cache_manager.set(cache_key, response, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=response)


@method_decorator([
    custom_cache_control(
        public=True,
        max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE,
        cache_status_codes=[rest_status.HTTP_200_OK,],
    )
], name='dispatch')
class DataLayerMapViewSet(BaseDataLayerAPIViewSet, account_utilities.BaseTileGenerator):
    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'DATA_LAYER_MAP'

    def get_cache_key(self):
        pk = self.kwargs.get('pk')
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}_{2}'.format(
            self.CACHE_KEY_PREFIX,
            pk,
            '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))),
        )

    def get_live_map_query(self, env, request):
        query = """
        WITH bounds AS (
                SELECT {env} AS geom,
                {env}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT DISTINCT ST_AsMVTGeom(ST_Transform("schools_school".geopoint, 3857), bounds.b2d) AS geom,
                    {random_select_list}
                    "schools_school".id,
                    True AS is_rt_connected,
                    sds.{col_name} AS field_avg,
                    {case_conditions}
                    'connected' AS connectivity_status
                FROM schools_school
                INNER JOIN bounds ON ST_Intersects("schools_school".geopoint, ST_Transform(bounds.geom, 4326))
                INNER JOIN (
                    SELECT "schools_school"."id" AS school_id,
                        "schools_school"."last_weekly_status_id",
                        {col_function} AS "{col_name}"
                    FROM "schools_school"
                    INNER JOIN connection_statistics_schoolrealtimeregistration rt_status ON
                        rt_status."school_id" = "schools_school".id
                    {school_weekly_join}
                    LEFT OUTER JOIN "connection_statistics_schooldailystatus" t ON (
                        "schools_school"."id" = t."school_id"
                        AND t."deleted" IS NULL
                        AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                        AND t."live_data_source" IN ({live_source_types})
                    )
                    WHERE (
                        "schools_school"."deleted" IS NULL
                        AND rt_status."deleted" IS NULL
                        {country_condition}
                        {admin1_condition}
                        {school_condition}
                        {school_weekly_condition}
                        AND rt_status."rt_registered" = True
                        AND rt_status."rt_registration_date"::date <= '{end_date}'
                    )
                    GROUP BY "schools_school"."id"
                ) AS sds ON sds.school_id = "schools_school".id
                {school_weekly_outer_join}
                WHERE "schools_school"."deleted" IS NULL
                    {random_order}
                    {limit_condition}
            )
            SELECT ST_AsMVT(DISTINCT mvtgeom.*) FROM mvtgeom;
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['school_condition'] = ''

        # kwargs['country_outer_condition'] = ''
        # kwargs['admin1_outer_condition'] = ''
        # kwargs['school_outer_condition'] = ''

        kwargs['school_weekly_join'] = ''
        kwargs['school_weekly_condition'] = ''
        kwargs['school_weekly_outer_join'] = ''

        kwargs['env'] = self.envelope_to_bounds_sql(env)

        kwargs['limit_condition'] = ''
        kwargs['random_order'] = ''
        kwargs['random_select_list'] = ''

        add_random_condition = True

        legend_configs = kwargs['legend_configs']
        if len(legend_configs) > 0 and 'SQL:' in str(legend_configs):
            label_cases = []
            for title, values_and_label in legend_configs.items():
                values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                     values_and_label.get('values', [])))

                if len(values) > 0:
                    is_sql_value = 'SQL:' in values[0]
                    if is_sql_value:
                        sql_statement = str(','.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    label_cases.append("ELSE '{label}'".format(label=title))

            kwargs['case_conditions'] = 'CASE ' + ' '.join(label_cases) + 'END AS field_status,'
            kwargs['school_weekly_outer_join'] = """
            INNER JOIN "connection_statistics_schoolweeklystatus" sws ON sds."last_weekly_status_id" = sws."id"
            """
        else:
            kwargs['case_conditions'] = """
                CASE WHEN sds.{col_name} >  {benchmark_value} THEN 'good'
                    WHEN sds.{col_name} < {benchmark_value} AND sds.{col_name} >= {base_benchmark} THEN 'moderate'
                    WHEN sds.{col_name} < {base_benchmark}  THEN 'bad'
                    ELSE 'unknown'
                END AS field_status,
            """.format(**kwargs)

            if kwargs['is_reverse'] is True:
                kwargs['case_conditions'] = """
                CASE WHEN sds.{col_name} < {benchmark_value}  THEN 'good'
                    WHEN sds.{col_name} >= {benchmark_value} AND sds.{col_name} <= {base_benchmark} THEN 'moderate'
                    WHEN sds.{col_name} > {base_benchmark} THEN 'bad'
                    ELSE 'unknown'
                END AS field_status,
                """.format(**kwargs)

        if len(kwargs.get('school_ids', [])) > 0:
            add_random_condition = False
            kwargs['school_condition'] = 'AND "schools_school"."id" IN ({0})'.format(
                ','.join([str(school_id) for school_id in kwargs['school_ids']])
            )

            # kwargs['school_outer_condition'] = 'AND "schools_school"."id" IN ({0})'.format(
            #     ','.join([str(school_id) for school_id in kwargs['school_ids']])
            # )
        elif len(kwargs.get('admin1_ids', [])) > 0:
            if settings.ADMIN_MAP_API_SAMPLING_LIMIT is not None:
                kwargs['MAP_API_SAMPLING_LIMIT'] = settings.ADMIN_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            kwargs['admin1_condition'] = 'AND "schools_school"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
            # kwargs['admin1_outer_condition'] = 'AND "schools_school"."admin1_id" IN ({0})'.format(
            #     ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            # )
        elif len(kwargs.get('country_ids', [])) > 0:
            if settings.COUNTRY_MAP_API_SAMPLING_LIMIT:
                kwargs['MAP_API_SAMPLING_LIMIT'] = settings.COUNTRY_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            kwargs['country_condition'] = 'AND "schools_school"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )
            # kwargs['country_outer_condition'] = 'AND "schools_school"."country_id" IN ({0})'.format(
            #     ','.join([str(country_id) for country_id in kwargs['country_ids']])
            # )

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] += ' AND ' + kwargs['school_filters']
            # kwargs['school_outer_condition'] += ' AND ' + kwargs['school_filters']

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            INNER JOIN "connection_statistics_schoolweeklystatus"
                ON "schools_school"."last_weekly_status_id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = ' AND ' + kwargs['school_static_filters']

        if add_random_condition:
            if 'limit' in request.query_params:
                limit = request.query_params['limit']
                kwargs['random_order'] = 'ORDER BY random()' if int(request.query_params.get('z', '0')) == 2 else ''
            elif kwargs.get('MAP_API_SAMPLING_LIMIT'):
                limit = kwargs['MAP_API_SAMPLING_LIMIT']
                kwargs['random_order'] = 'ORDER BY random()'
            else:
                limit = '50000'
                kwargs['random_order'] = 'ORDER BY random()' if int(request.query_params.get('z', '0')) == 2 else ''

            kwargs['limit_condition'] = 'LIMIT ' + str(limit)
            kwargs['random_select_list'] = 'random(),'

        kwargs['col_function'] = kwargs['parameter_col_function_sql'].format(**kwargs)

        return query.format(**kwargs)

    def envelope_to_sql(self, env, request):
        if self.kwargs['layer_type'] == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            return self.get_live_map_query(env, request)
        return self.get_static_map_query(env, request)

    def get_static_map_query(self, env, request):
        query = """
        WITH
        bounds AS (
            SELECT {env} AS geom,
                   {env}::box2d AS b2d
        ),
        mvtgeom AS (
            SELECT DISTINCT ST_AsMVTGeom(ST_Transform(schools_school.geopoint, 3857), bounds.b2d) AS geom,
                {random_select_list}
                schools_school.id,
                {table_name}."{col_name}" AS field_value,
                'connected' AS connectivity_status,
                {label_case_statements}
            FROM schools_school
            INNER JOIN bounds ON ST_Intersects(schools_school.geopoint, ST_Transform(bounds.geom, 4326))
            INNER JOIN connection_statistics_schoolweeklystatus sws ON schools_school.last_weekly_status_id = sws.id
            {school_weekly_join}
            WHERE schools_school."deleted" IS NULL
            {country_condition}
            {admin1_condition}
            {school_condition}
            {school_weekly_condition}
            {random_order}
            {limit_condition}
        )
        SELECT ST_AsMVT(DISTINCT mvtgeom.*) FROM mvtgeom;
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['school_condition'] = ''

        kwargs['school_weekly_join'] = ''
        kwargs['school_weekly_condition'] = ''

        kwargs['env'] = self.envelope_to_bounds_sql(env)

        kwargs['limit_condition'] = ''
        kwargs['random_order'] = ''
        kwargs['random_select_list'] = ''

        add_random_condition = True

        if len(kwargs.get('school_ids', [])) > 0:
            add_random_condition = False
            kwargs['school_condition'] = 'AND schools_school."id" IN ({0})'.format(
                ','.join([str(school_id) for school_id in kwargs['school_ids']])
            )
        elif len(kwargs.get('admin1_ids', [])) > 0:
            if settings.ADMIN_MAP_API_SAMPLING_LIMIT:
                kwargs['MAP_API_SAMPLING_LIMIT'] = settings.ADMIN_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            kwargs['admin1_condition'] = 'AND schools_school."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
        elif len(kwargs.get('country_ids', [])) > 0:
            if settings.COUNTRY_MAP_API_SAMPLING_LIMIT:
                kwargs['MAP_API_SAMPLING_LIMIT'] = settings.COUNTRY_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            kwargs['country_condition'] = 'AND schools_school."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] += ' AND ' + kwargs['school_filters']

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            INNER JOIN "connection_statistics_schoolweeklystatus"
                ON sws."id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = ' AND ' + kwargs['school_static_filters']

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
        kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'sws')

        for title, values_and_label in legend_configs.items():
            values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                 values_and_label.get('values', [])))

            if len(values) > 0:
                is_sql_value = 'SQL:' in values[0]
                if is_sql_value:
                    sql_statement = str(','.join(values)).replace('SQL:', '').format(
                        table_name=kwargs['table_name'],
                        col_name=kwargs['col_name'],
                    )
                    label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    values_l.extend(values)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            """WHEN LOWER({table_name}."{col_name}") IN ({value}) THEN '{label}'""".format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            """WHEN {table_name}."{col_name}" IN ({value}) THEN '{label}'""".format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join([str(v) for v in values])
                            ))
            else:
                label_cases.append("ELSE '{label}'".format(label=title))

        kwargs['label_case_statements'] = 'CASE ' + ' '.join(label_cases) + 'END AS field_status'

        if add_random_condition:
            if 'limit' in request.query_params:
                limit = request.query_params['limit']
                kwargs['random_order'] = 'ORDER BY random()' if int(request.query_params.get('z', '0')) == 2 else ''
            elif kwargs.get('MAP_API_SAMPLING_LIMIT'):
                limit = kwargs['MAP_API_SAMPLING_LIMIT']
                kwargs['random_order'] = 'ORDER BY random()'
            else:
                limit = '50000'
                kwargs['random_order'] = 'ORDER BY random()' if int(request.query_params.get('z', '0')) == 2 else ''

            kwargs['limit_condition'] = 'LIMIT ' + str(limit)
            kwargs['random_select_list'] = 'random(),'

        return query.format(**kwargs)

    def cache_enabled(self, data_layer_instance):
        # Cache static layer Map data
        if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_STATIC:
            return True

        # Check if list of country ids provided and passed country id present in it
        if len(settings.LIVE_LAYER_CACHE_FOR_COUNTRY_IDS) > 0:
            if (
                'country_ids' in self.kwargs and
                len(list(set(self.kwargs['country_ids']) & set(settings.LIVE_LAYER_CACHE_FOR_COUNTRY_IDS))) == 0
            ):
                return False

        date = core_utilities.get_current_datetime_object().date() - timedelta(weeks=settings.LIVE_LAYER_CACHE_FOR_WEEKS)
        if self.kwargs['start_date'] >= date:
            return True

        return False

    def get(self, request, *args, **kwargs):
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key()

        response = None
        if use_cached_data:
            response = cache_manager.get(cache_key)

        if not response:
            data_layer_instance = get_object_or_404(
                accounts_models.DataLayer.objects.all(),
                pk=self.kwargs.get('pk'),
                status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
            )

            data_sources = data_layer_instance.data_sources.all()

            live_data_sources = ['UNKNOWN']

            for d in data_sources:
                source_type = d.data_source.data_source_type
                if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                    live_data_sources.append(statistics_configs.QOS_SOURCE)
                elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                    live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

            country_ids = data_layer_instance.applicable_countries
            parameter_col = data_sources.first().data_source_column
            column_function_sql = self.get_column_function_sql(data_sources.first().data_source_column_function)

            parameter_column_name = str(parameter_col['name'])
            base_benchmark = str(parameter_col.get('base_benchmark', 1))

            self.update_kwargs(country_ids, data_layer_instance)
            benchmark_value, _ = self.get_benchmark_value(data_layer_instance)
            global_benchmark = data_layer_instance.global_benchmark.get('value')

            legend_configs = self.get_legend_configs(data_layer_instance)

            if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
                self.kwargs.update({
                    'col_name': parameter_column_name,
                    'benchmark_value': benchmark_value,
                    'global_benchmark': global_benchmark,
                    'national_benchmark': benchmark_value,
                    'base_benchmark': base_benchmark,
                    'live_source_types': ','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                    'parameter_col': parameter_col,
                    'parameter_col_function_sql': column_function_sql,
                    'layer_type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                    'legend_configs': legend_configs,
                })
            else:
                self.kwargs.update({
                    'col_name': parameter_column_name,
                    'legend_configs': legend_configs,
                    'parameter_col': parameter_col,
                    'layer_type': accounts_models.DataLayer.LAYER_TYPE_STATIC,
                })

            try:
                response = self.generate_tile(request)
                if self.cache_enabled(data_layer_instance) and response.status_code == rest_status.HTTP_200_OK:
                    cache_manager.set(cache_key, response, request_path=request_path,
                                      soft_timeout=settings.CACHE_CONTROL_MAX_AGE)
            except Exception as ex:
                logger.error('Exception occurred for school connectivity tiles endpoint: {}'.format(ex))
                response = Response({'error': 'An error occurred while processing the request'}, status=500)

        return response


class LogActionViewSet(BaseModelViewSet):
    """
    LogActionViewSet
        This class is used to list all logs recorded.
        Inherits: BaseModelViewSet
    """
    model = LogEntry
    serializer_class = serializers.LogActionSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanViewRecentAction,
    )

    filter_backends = (
        DjangoFilterBackend,
    )

    ordering_field_names = ['-action_time']
    apply_query_pagination = True

    filterset_fields = {
        'user_id': ['exact', 'in'],
    }

    paginator = None

    def apply_queryset_filters(self, queryset):
        """ If user not superuser then return only own Keys. For superuser return all API Keys"""
        request_user = self.request.user
        if not core_utilities.is_superuser(request_user):
            queryset = queryset.filter(user=request_user)
        return super().apply_queryset_filters(queryset)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class TimePlayerViewSet(BaseDataLayerAPIViewSet, account_utilities.BaseTileGenerator):
    permission_classes = (permissions.AllowAny,)

    def get_live_map_query(self, env, request):
        query = """
        WITH bounds AS (
                SELECT {env} AS geom,
                {env}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT DISTINCT ST_AsMVTGeom(ST_Transform(sds.geopoint, 3857), bounds.b2d) AS geom,
                   sds.school_id,
                   sds.year,
                   CASE
                       WHEN rt_status.rt_registered = True
                            AND EXTRACT(YEAR FROM CAST(rt_status.rt_registration_date AS DATE)) <= sds.year THEN True
                       ELSE False
                   END AS is_rt_connected,
                   {case_conditions}
                FROM bounds
                INNER JOIN (
                    SELECT schools_school.id AS school_id,
                        schools_school.geopoint,
                        EXTRACT(YEAR FROM CAST(t.date AS DATE)) AS year,
                        schools_school."last_weekly_status_id",
                        {col_function} AS "{col_name}"
                    FROM "schools_school"
                    INNER JOIN "connection_statistics_schooldailystatus" t ON schools_school."id" = t."school_id"
                    WHERE schools_school."deleted" IS NULL
                        AND t."deleted" IS NULL
                        AND schools_school."country_id" = {country_id}
                        AND EXTRACT(YEAR FROM CAST(t.date AS DATE)) >= {start_year}
                        AND t."live_data_source" IN ({live_source_types})
                    GROUP BY schools_school."id", EXTRACT(YEAR FROM CAST(t.date AS DATE))
                ) AS sds ON ST_Intersects(sds.geopoint, ST_Transform(bounds.geom, 4326))
            INNER JOIN "connection_statistics_schoolweeklystatus" sws ON sds."last_weekly_status_id" = sws."id"
            LEFT JOIN connection_statistics_schoolrealtimeregistration rt_status ON rt_status.school_id = sds.school_id
            WHERE rt_status."deleted" IS NULL
        )
        SELECT ST_AsMVT(DISTINCT mvtgeom.*) FROM mvtgeom;
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['env'] = self.envelope_to_bounds_sql(env)

        legend_configs = kwargs['legend_configs']
        if len(legend_configs) > 0 and 'SQL:' in str(legend_configs):
            label_cases = []
            for title, values_and_label in legend_configs.items():
                values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                     values_and_label.get('values', [])))

                if len(values) > 0:
                    is_sql_value = 'SQL:' in values[0]
                    if is_sql_value:
                        sql_statement = str(','.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    label_cases.append("ELSE '{label}'".format(label=title))

            kwargs['case_conditions'] = 'CASE ' + ' '.join(label_cases) + 'END AS field_status'
        else:
            kwargs['case_conditions'] = """
                        CASE WHEN sds.{col_name} >  {benchmark_value} THEN 'good'
                            WHEN sds.{col_name} < {benchmark_value} AND sds.{col_name} >= {base_benchmark} THEN 'moderate'
                            WHEN sds.{col_name} < {base_benchmark}  THEN 'bad'
                            ELSE 'unknown'
                        END AS field_status
                    """.format(**kwargs)

            if kwargs['is_reverse'] is True:
                kwargs['case_conditions'] = """
                        CASE WHEN sds.{col_name} < {benchmark_value}  THEN 'good'
                            WHEN sds.{col_name} >= {benchmark_value} AND sds.{col_name} <= {base_benchmark} THEN 'moderate'
                            WHEN sds.{col_name} > {base_benchmark} THEN 'bad'
                            ELSE 'unknown'
                        END AS field_status
                        """.format(**kwargs)

        kwargs['col_function'] = kwargs['parameter_col_function_sql'].format(**kwargs)

        return query.format(**kwargs)

    def envelope_to_sql(self, env, request):
        return self.get_live_map_query(env, request)

    def get(self, request, *args, **kwargs):
        layer_id = request.query_params.get('layer_id')
        country_id = request.query_params.get('country_id')

        data_layer_instance = get_object_or_404(
            accounts_models.DataLayer.objects.all(),
            pk=layer_id,
            status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
        )

        data_sources = data_layer_instance.data_sources.all()

        live_data_sources = ['UNKNOWN']

        for d in data_sources:
            source_type = d.data_source.data_source_type
            if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                live_data_sources.append(statistics_configs.QOS_SOURCE)
            elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

        parameter_col = data_sources.first().data_source_column
        column_function_sql = self.get_column_function_sql(data_sources.first().data_source_column_function)

        parameter_column_name = str(parameter_col['name'])
        base_benchmark = str(parameter_col.get('base_benchmark', 1))

        self.kwargs['benchmark'] = 'national' if request.query_params.get('benchmark', 'global') == 'national' else 'global'
        self.kwargs['country_id'] = country_id
        self.kwargs['col_name'] = parameter_column_name

        benchmark_value, _ = self.get_benchmark_value(data_layer_instance)
        legend_configs = self.get_legend_configs(data_layer_instance)

        if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            self.kwargs.update({
                'benchmark_value': benchmark_value,
                'base_benchmark': base_benchmark,
                'live_source_types': ','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                'parameter_col': parameter_col,
                'parameter_col_function_sql': column_function_sql,
                'layer_type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'start_year': request.query_params.get('start_year', date_utilities.get_current_year() - 4),
                'is_reverse': data_layer_instance.is_reverse,
                'legend_configs': legend_configs,
            })

        try:
            return self.generate_tile(request)
        except Exception as ex:
            logger.error('Exception occurred for school connectivity tiles endpoint: {0}'.format(ex))
            return Response({'error': 'An error occurred while processing the request'}, status=500)


class ColumnConfigurationViewSet(BaseModelViewSet):
    model = accounts_models.ColumnConfiguration
    serializer_class = serializers.ColumnConfigurationListSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanViewColumnConfigurations,
    )

    filter_backends = (
        DjangoFilterBackend,
        NullsAlwaysLastOrderingFilter,
    )

    ordering_field_names = ['label', 'name']
    apply_query_pagination = True

    filterset_fields = {
        'id': ['exact', 'in'],
        'type': ['iexact', 'in', 'exact'],
        'name': ['iexact', 'in', 'exact'],
        'table_name': ['iexact', 'in', 'exact'],
        'is_filter_applicable': ['exact'],
    }


class AdvanceFiltersViewSet(BaseModelViewSet):
    model = accounts_models.AdvanceFilter
    serializer_class = serializers.AdvanceFiltersListSerializer

    action_serializers = {
        'create': serializers.CreateAdvanceFilterSerializer,
        'partial_update': serializers.UpdateAdvanceFilterSerializer,
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
    }

    permit_list_expands = ['created_by', 'published_by', 'last_modified_by', 'column_configuration']

    def apply_queryset_filters(self, queryset):
        """
        Override if applying more complex filters to queryset.
        :param queryset:
        :return queryset:
        """

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
        """
        Delete the filter from Admin portal listing only if its in Draft or Disabled mode.
        Published filter can not be deleted.
        """
        if instance.status in [accounts_models.AdvanceFilter.FILTER_STATUS_DRAFT,
                               accounts_models.AdvanceFilter.FILTER_STATUS_DISABLED]:
            instance.deleted = core_utilities.get_current_datetime_object()
            instance.last_modified_at = core_utilities.get_current_datetime_object()
            instance.last_modified_by = core_utilities.get_current_user(request=self.request)
            return super().perform_destroy(instance)
        raise accounts_exceptions.InvalidAdvanceFilterDeleteError(
            message_kwargs={'filter': instance.name, 'status': instance.status},
        )


class AdvanceFiltersPublishViewSet(BaseModelViewSet):
    model = accounts_models.AdvanceFilter
    serializer_class = serializers.PublishAdvanceFilterSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPublishAdvanceFilter,
    )

    def apply_queryset_filters(self, queryset):
        """
        Filter only in Draft or Disabled status can be Published.
        """
        queryset = queryset.filter(
            status__in=[accounts_models.AdvanceFilter.FILTER_STATUS_DRAFT,
                        accounts_models.AdvanceFilter.FILTER_STATUS_DISABLED],
        )
        return super().apply_queryset_filters(queryset)


class PublishedAdvanceFiltersViewSet(CachedListMixin, BaseModelViewSet):
    """
    PublishedAdvanceFiltersViewSet
    Cache Attr:
        Auto Cache: Not required
        Call Cache: Yes
    """
    LIST_CACHE_KEY_PREFIX = 'PUBLISHED_FILTERS_LIST'

    model = accounts_models.AdvanceFilter
    serializer_class = serializers.PublishedAdvanceFiltersListSerializer

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
        """
        Override if applying more complex filters to queryset.
        :param queryset:
        :return queryset:
        """

        country_id = self.kwargs.get('country_id')
        status = self.kwargs.get('status', 'PUBLISHED')

        queryset = queryset.filter(
            status=status,
            active_countries__country=country_id,
            active_countries__deleted__isnull=True,
        )

        return super().apply_queryset_filters(queryset)

    def update_serializer_context(self, context):
        context['country_id'] = self.kwargs.get('country_id')
        return context
