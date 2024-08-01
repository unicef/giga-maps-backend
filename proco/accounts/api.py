import copy
import json
import logging
from datetime import timedelta
from math import floor, ceil

from django.conf import settings
from django.contrib.admin.models import LogEntry
from django.db.models import Case, F, IntegerField, Value, When, Min, Max
from django.db.models import Q
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

from proco.accounts import models as accounts_models
from proco.accounts import serializers
from proco.accounts import utils as account_utilities
from proco.accounts.config import app_config as account_config
from proco.connection_statistics import models as statistics_models
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import SchoolWeeklyStatus
from proco.core import db_utils as db_utilities
from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.custom_auth import models as auth_models
from proco.locations.models import Country
from proco.schools.models import School
from proco.utils import dates as date_utilities
from proco.utils.cache import cache_manager
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
        # NullsAlwaysLastOrderingFilter,
    )

    ordering_fields = ('valid_to', 'status', 'last_modified_at', 'extension_status',)
    apply_query_pagination = True

    filterset_fields = {
        'status': ['iexact', 'in', 'exact'],
        'user_id': ['exact', 'in'],
    }

    permit_list_expands = ['user', 'api', 'status_updated_by']

    def apply_queryset_filters(self, queryset):
        """ If user not superuser then return only own Keys. For superuser return all API Keys"""
        request_user = self.request.user
        has_approval_permission = request_user.permissions.get(
            auth_models.RolePermission.CAN_APPROVE_REJECT_API_KEY, False)

        queryset = queryset.filter(api__deleted__isnull=True, has_write_access=False)

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
        This class is used to list all API keys.
        Inherits: BaseModelViewSet
    """
    model = accounts_models.APIKey
    serializer_class = serializers.UpdateAPIKeysForExtensionSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
    )


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
            return Response(status=rest_status.HTTP_200_OK)
        return Response(status=rest_status.HTTP_404_NOT_FOUND, data={'detail': 'Please enter valid api key.'})


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
        }

        return Response(data=static_data)


class AdvancedFiltersViewSet(APIView):
    base_auth_permissions = (
        permissions.AllowAny,
    )

    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'ADVANCE_FILTERS_JSON'

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}'.format(self.CACHE_KEY_PREFIX,
                                '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))), )

    def get(self, request, *args, **kwargs):
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        cache_key = self.get_cache_key()

        response_data = None
        if use_cached_data:
            response_data = cache_manager.get(cache_key)

        if not response_data:
            filters = copy.deepcopy(settings.FILTERS_DATA)

            for filter_json in filters:
                parameter_table = filter_json['parameter']['table']
                parameter_field = filter_json['parameter']['field']

                last_weekly_status_field = 'last_weekly_status__{}'.format(parameter_field)

                active_countries_list = []

                # Populate the active countries list
                active_countries_sql_filter = filter_json.get('active_countries_filter', None)
                if active_countries_sql_filter:
                    country_qs = School.objects.all()
                    if parameter_table == 'school_static':
                        country_qs = country_qs.select_related('last_weekly_status').annotate(**{
                            parameter_table + '_' + parameter_field: F(last_weekly_status_field)
                        })

                    active_countries_list = list(country_qs.extra(
                        where=[active_countries_sql_filter],
                    ).order_by('country_id').values_list('country_id', flat=True).distinct('country_id'))

                    if len(active_countries_list) > 0:
                        filter_json['active_countries_list'] = active_countries_list

                    del filter_json['active_countries_filter']

                if filter_json['type'] == 'range':
                    select_qs = School.objects.all()
                    if len(active_countries_list) > 0:
                        select_qs = select_qs.filter(country_id__in=active_countries_list)

                    if parameter_table == 'school_static':
                        select_qs = select_qs.select_related('last_weekly_status').values('country_id').annotate(
                            min_value=Min(F(last_weekly_status_field)),
                            max_value=Max(F(last_weekly_status_field)),
                        )
                    else:
                        select_qs = select_qs.values('country_id').annotate(
                            min_value=Min(parameter_field),
                            max_value=Max(parameter_field),
                        )

                    min_max_result_country_wise = list(
                        select_qs.values('country_id', 'min_value', 'max_value').order_by('country_id').distinct())

                    active_countries_range = filter_json['active_countries_range']

                    for min_max_result in min_max_result_country_wise:
                        country_id = min_max_result.pop('country_id')
                        country_range_json = active_countries_range.get(country_id, copy.deepcopy(
                            active_countries_range['default']))
                        min_max_result['min_value'] = floor(min_max_result['min_value'])
                        min_max_result['max_value'] = ceil(min_max_result['max_value'])

                        if 'downcast_aggr_str' in filter_json:
                            downcast_eval = filter_json['downcast_aggr_str']
                            min_max_result['min_value'] = floor(
                                eval(downcast_eval.format(val=min_max_result['min_value'])))
                            min_max_result['max_value'] = ceil(
                                eval(downcast_eval.format(val=min_max_result['max_value'])))

                        country_range_json.update(**min_max_result)

                        country_range_json['min_place_holder'] = 'Min ({})'.format(min_max_result['min_value'])
                        country_range_json['max_place_holder'] = 'Max ({})'.format(min_max_result['max_value'])
                        active_countries_range[country_id] = country_range_json

                    filter_json['active_countries_range'] = active_countries_range

            response_data = {
                'count': len(settings.FILTERS_DATA),
                'results': filters,
            }
            request_path = remove_query_param(request.get_full_path(), 'cache')
            cache_manager.set(cache_key, response_data, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=response_data)


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
    )

    ordering_field_names = ['-last_modified_at', 'name']
    apply_query_pagination = True

    filterset_fields = {
        'id': ['exact', 'in'],
        'status': ['iexact', 'in', 'exact'],
        'published_by_id': ['exact', 'in'],
        'name': ['iexact', 'in', 'exact'],
    }

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

    def get_map_query(self, kwargs):
        query = """
        SELECT "schools_school".id, "schools_school".name,
            CASE WHEN rt_status.rt_registered = True AND rt_status.rt_registration_date <= '{end_date}' THEN True
                    ELSE False
            END as is_rt_connected,
            {case_conditions}
            CASE WHEN "schools_school".connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN "schools_school".connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END as connectivity_status,
            ST_AsGeoJSON(ST_Transform("schools_school".geopoint, 4326)) as geopoint
        FROM schools_school
        LEFT JOIN (
            SELECT "schools_school"."id" AS school_id,
                AVG(t."{col_name}") AS "field_avg"
            FROM "schools_school"
            INNER JOIN "connection_statistics_schoolrealtimeregistration"
                ON ("schools_school"."id" = "connection_statistics_schoolrealtimeregistration"."school_id")
            LEFT OUTER JOIN "connection_statistics_schooldailystatus" t
                ON (
                    "schools_school"."id" = t."school_id"
                    AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                    AND t."live_data_source" IN ({live_source_types})
                )
            WHERE (
                {country_condition}
                "connection_statistics_schoolrealtimeregistration"."rt_registered" = True
                AND "connection_statistics_schoolrealtimeregistration"."rt_registration_date"::date <= '{end_date}'
                AND "schools_school"."deleted" IS NULL
                AND "connection_statistics_schoolrealtimeregistration"."deleted" IS NULL
                AND t."deleted" IS NULL)
            GROUP BY "schools_school"."id"
            ORDER BY "schools_school"."id" ASC
        ) as t
            ON t.school_id = "schools_school".id
        LEFT JOIN connection_statistics_schoolrealtimeregistration rt_status
            ON rt_status.school_id = "schools_school".id
        WHERE "schools_school"."deleted" IS NULL
        AND rt_status."deleted" IS NULL
        {country_condition_outer}
        ORDER BY random()
        LIMIT 1000
        """

        kwargs['case_conditions'] = """
            CASE WHEN t.field_avg >  {benchmark_value} THEN 'good'
                WHEN t.field_avg <= {benchmark_value} and t.field_avg >= {base_benchmark} THEN 'moderate'
                WHEN t.field_avg < {base_benchmark}  THEN 'bad'
                ELSE 'unknown'
            END as connectivity,
        """.format(**kwargs)

        if kwargs['is_reverse'] is True:
            kwargs['case_conditions'] = """
                CASE WHEN t.field_avg < {benchmark_value}  THEN 'good'
                    WHEN t.field_avg >= {benchmark_value} AND t.field_avg <= {base_benchmark} THEN 'moderate'
                    WHEN t.field_avg > {base_benchmark} THEN 'bad'
                    ELSE 'unknown'
                END as connectivity,
            """.format(**kwargs)

        if len(kwargs['country_ids']) > 0:
            kwargs['country_condition'] = '"schools_school"."country_id" IN ({0}) AND'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )
            kwargs['country_condition_outer'] = 'AND "schools_school"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )
        else:
            kwargs['country_condition'] = ''
            kwargs['country_condition_outer'] = ''

        return query.format(**kwargs)

    def get_static_map_query(self, kwargs):
        query = """
        SELECT
            s.id,
            s.name,
            sws."{col_name}",
            CASE WHEN s.connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN s.connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END as connectivity_status,
            ST_AsGeoJSON(ST_Transform(s.geopoint, 4326)) as geopoint,
            {label_case_statements}
        FROM schools_school as s
        LEFT JOIN connection_statistics_schoolweeklystatus sws ON s.last_weekly_status_id = sws.id
        WHERE s."deleted" IS NULL AND sws."deleted" IS NULL {country_condition}
        ORDER BY random()
        LIMIT 1000
        """

        kwargs['country_condition'] = ''

        if len(kwargs['country_ids']) > 0:
            kwargs['country_condition'] = 'AND s.country_id IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
        for title, values_and_label in legend_configs.items():
            values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                 values_and_label.get('values', [])))

            if len(values) > 0:
                is_sql_value = 'SQL:' in values[0]
                if is_sql_value:
                    sql_statement = str(','.join(values)).replace('SQL:', '').format(
                        col_name=kwargs['col_name'],
                    )
                    label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    values_l.extend(values)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            """WHEN LOWER(sws."{col_name}") IN ({value}) THEN '{label}'""".format(
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            """WHEN sws."{col_name}" IN ({value}) THEN '{label}'""".format(
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

        live_data_sources = ['UNKNOWN']

        for d in data_sources:
            source_type = d.data_source.data_source_type
            if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                live_data_sources.append(statistics_configs.QOS_SOURCE)
            elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

        country_ids = data_layer_instance.applicable_countries
        parameter_col = data_sources.first().data_source_column

        parameter_column_name = str(parameter_col['name'])

        if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            response = {
                'map': None,
            }

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
                'base_benchmark': benchmark_base,
                'country_ids': country_ids,
                'start_date': start_date,
                'end_date': end_date,
                'live_source_types': ','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                'parameter_col': parameter_col,
                'is_reverse': data_layer_instance.is_reverse,
            }

            map_points = db_utilities.sql_to_response(self.get_map_query(query_kwargs), label=self.__class__.__name__)
            if map_points:
                for map_point in map_points:
                    map_point['geopoint'] = json.loads(map_point['geopoint'])
            response['map'] = map_points
        else:
            response = {
                'map': None,
            }

            legend_configs = data_layer_instance.legend_configs

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
            response['map'] = map_points

        return Response(data=response)


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


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
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
            COUNT(DISTINCT CASE WHEN t.field_avg IS NOT NULL THEN t.school_id ELSE NULL END)
                AS "school_with_realtime_data",
            COUNT(DISTINCT t.school_id) AS "no_of_schools_measure"
        FROM (
            SELECT "schools_school"."id" AS school_id,
                AVG(t."{col_name}") AS "field_avg"
            FROM "schools_school"
            INNER JOIN "connection_statistics_schoolrealtimeregistration"
                ON ("schools_school"."id" = "connection_statistics_schoolrealtimeregistration"."school_id")
            LEFT OUTER JOIN "connection_statistics_schooldailystatus" t
                ON (
                    "schools_school"."id" = t."school_id"
                    AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                    AND t."live_data_source" IN ({live_source_types})
                )
            {school_weekly_join}
            WHERE (
                "schools_school"."deleted" IS NULL
                AND "connection_statistics_schoolrealtimeregistration"."deleted" IS NULL
                AND t."deleted" IS NULL
                {country_condition}
                {admin1_condition}
                {school_condition}
                {school_weekly_condition}
                AND "connection_statistics_schoolrealtimeregistration"."rt_registered" = True
                AND "connection_statistics_schoolrealtimeregistration"."rt_registration_date"::date <= '{end_date}')
            GROUP BY "schools_school"."id"
            ORDER BY "schools_school"."id" ASC
        ) as t
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['school_condition'] = ''
        kwargs['school_weekly_join'] = ''
        kwargs['school_weekly_condition'] = ''

        kwargs['case_conditions'] = """
        COUNT(DISTINCT CASE WHEN t.field_avg > {benchmark_value} THEN t.school_id ELSE NULL END) AS "good",
        COUNT(DISTINCT CASE WHEN (t.field_avg >= {base_benchmark} AND t.field_avg <= {benchmark_value})
            THEN t.school_id ELSE NULL END) AS "moderate",
        COUNT(DISTINCT CASE WHEN t.field_avg < {base_benchmark} THEN t.school_id ELSE NULL END) AS "bad",
        COUNT(DISTINCT CASE WHEN t.field_avg IS NULL THEN t.school_id ELSE NULL END) AS "unknown",
        """.format(**kwargs)

        if kwargs['is_reverse'] is True:
            kwargs['case_conditions'] = """
            COUNT(DISTINCT CASE WHEN t.field_avg < {benchmark_value} THEN t.school_id ELSE NULL END) AS "good",
            COUNT(DISTINCT CASE WHEN (t.field_avg >= {benchmark_value} AND t.field_avg <= {base_benchmark})
                THEN t.school_id ELSE NULL END) AS "moderate",
            COUNT(DISTINCT CASE WHEN t.field_avg > {base_benchmark} THEN t.school_id ELSE NULL END) AS "bad",
            COUNT(DISTINCT CASE WHEN t.field_avg IS NULL THEN t.school_id ELSE NULL END) AS "unknown",
            """.format(**kwargs)

        if len(kwargs.get('country_ids', [])) > 0:
            kwargs['country_condition'] = 'AND "schools_school"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs.get('admin1_ids', [])) > 0:
            kwargs['admin1_condition'] = 'AND "schools_school"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] = ' AND ' + kwargs['school_filters']

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            LEFT OUTER JOIN "connection_statistics_schoolweeklystatus"
                ON "schools_school"."last_weekly_status_id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = ' AND ' + kwargs['school_static_filters']

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
            ROUND(AVG(sds."{col_name}"::numeric), 2) AS "live_avg",
            CASE WHEN schools_school.connectivity_status IN ('good', 'moderate') THEN 'connected'
                   WHEN schools_school.connectivity_status = 'no' THEN 'not_connected' ELSE 'unknown' END as connectivity_status,
            CASE WHEN srr."rt_registered" = True AND srr."rt_registration_date"::date <= '{end_date}' THEN true
            ELSE false END AS is_rt_connected,
            {case_conditions}
        FROM "schools_school" schools_school
        INNER JOIN public.locations_country c ON c."id" = schools_school."country_id"
            AND c."deleted" IS NULL
            AND schools_school."deleted" IS NULL
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
        LEFT OUTER JOIN "connection_statistics_schooldailystatus" sds
            ON schools_school."id" = sds."school_id"
            AND sds."deleted" IS NULL
            AND (sds."date" BETWEEN '{start_date}' AND '{end_date}')
            AND sds."live_data_source" IN ({live_source_types})
        WHERE "schools_school"."id" IN ({ids})
        GROUP BY schools_school."id", srr."rt_registered", srr."rt_registration_date",
            adm1_metadata."name", adm1_metadata."description_ui_label",
            adm2_metadata."name", adm2_metadata."description_ui_label",
            c."name", adm1_metadata."giga_id_admin", adm2_metadata."giga_id_admin"
        ORDER BY schools_school."id" ASC
        """

        kwargs = copy.deepcopy(self.kwargs)
        kwargs['ids'] = ','.join(kwargs['school_ids'])

        kwargs['case_conditions'] = """
        CASE
            WHEN AVG(sds."{col_name}") > {benchmark_value} THEN 'good'
            WHEN (AVG(sds."{col_name}") >= {base_benchmark} AND AVG(sds."{col_name}") <= {benchmark_value})
                THEN 'moderate'
            WHEN AVG(sds."{col_name}") < {base_benchmark} THEN 'bad'
            ELSE 'unknown' END AS live_avg_connectivity
        """.format(**kwargs)

        if kwargs['is_reverse'] is True:
            kwargs['case_conditions'] = """
            CASE
                WHEN AVG(sds."{col_name}") < {benchmark_value} THEN 'good'
                WHEN (AVG(sds."{col_name}") >= {benchmark_value} AND AVG(sds."{col_name}") <= {base_benchmark})
                    THEN 'moderate'
                WHEN AVG(sds."{col_name}") > {base_benchmark} THEN 'bad'
                ELSE 'unknown' END AS live_avg_connectivity
            """.format(**kwargs)

        return query.format(**kwargs)

    def get_school_view_statistics_info_query(self):
        query = """
        SELECT sws.*
        FROM "schools_school"
        INNER JOIN connection_statistics_schoolweeklystatus sws
            ON sws."id" = "schools_school"."last_weekly_status_id"
        WHERE
            "schools_school"."deleted" IS NULL
            AND sws."deleted" IS NULL
            AND "schools_school"."id" IN ({ids})
        """.format(ids=','.join(self.kwargs['school_ids']))

        return query

    def get_avg_query(self, **kwargs):
        query = """
        SELECT {school_selection}t."date" AS date,
            AVG(t."{col_name}") AS "field_avg"
        FROM "schools_school"
        INNER JOIN "connection_statistics_schoolrealtimeregistration"
            ON (
                "schools_school"."id" = "connection_statistics_schoolrealtimeregistration"."school_id"
                AND "connection_statistics_schoolrealtimeregistration"."deleted" IS NULL
            )
        INNER JOIN "connection_statistics_schooldailystatus" t
            ON (
                "schools_school"."id" = t."school_id"
                AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                AND t."live_data_source" IN ({live_source_types})
                AND t."deleted" IS NULL
            )
        {school_weekly_join}
        WHERE (
            {country_condition}
            {admin1_condition}
            {school_condition}
            {school_weekly_condition}
            "connection_statistics_schoolrealtimeregistration"."rt_registered" = True
            AND "connection_statistics_schoolrealtimeregistration"."rt_registration_date"::date <= '{end_date}'
            AND t."{col_name}" IS NOT NULL)
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

        if len(kwargs.get('country_ids', [])) > 0:
            kwargs['country_condition'] = '"schools_school"."country_id" IN ({0}) AND'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs.get('admin1_ids', [])) > 0:
            kwargs['admin1_condition'] = '"schools_school"."admin1_id" IN ({0}) AND'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )

        if len(kwargs.get('school_ids', [])) > 0:
            kwargs['school_condition'] = '"schools_school"."id" IN ({0}) AND '.format(','.join(kwargs['school_ids']))
            kwargs['school_selection'] = '"schools_school"."id", '
            kwargs['school_group_by'] = ', "schools_school"."id"'

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] += kwargs['school_filters'] + ' AND '

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            LEFT OUTER JOIN "connection_statistics_schoolweeklystatus"
                ON "schools_school"."last_weekly_status_id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = kwargs['school_static_filters'] + ' AND '

        return query.format(**kwargs)

    def generate_graph_data(self):
        kwargs = copy.deepcopy(self.kwargs)

        # Get the daily connectivity_speed for the given country from SchoolDailyStatus model
        data = db_utilities.sql_to_response(self.get_avg_query(**kwargs), label=self.__class__.__name__)

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
            COUNT(DISTINCT CASE WHEN sws."{col_name}" IS NOT NULL THEN "schools_school"."id" ELSE NULL END)
            AS "total_schools"
        FROM "schools_school"
        {school_weekly_join}
        LEFT JOIN connection_statistics_schoolweeklystatus sws ON "schools_school"."last_weekly_status_id" = sws."id"
        WHERE "schools_school"."deleted" IS NULL AND sws."deleted" IS NULL
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

        if len(kwargs.get('country_ids', [])) > 0:
            kwargs['country_condition'] = ' AND "schools_school"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs.get('admin1_ids', [])) > 0:
            kwargs['admin1_condition'] = ' AND "schools_school"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] = ' AND ' + kwargs['school_filters']

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            LEFT OUTER JOIN "connection_statistics_schoolweeklystatus"
                ON "schools_school"."last_weekly_status_id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = ' AND ' + kwargs['school_static_filters']

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
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
                            'COUNT(DISTINCT CASE WHEN LOWER(sws."{col_name}") IN ({value}) THEN schools_school."id" ELSE NULL END) '
                            'AS "{label}",'.format(
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN sws."{col_name}" IN ({value}) THEN schools_school."id" ELSE NULL END) '
                            'AS "{label}",'.format(
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join([str(v) for v in values])
                            ))
            else:
                if is_sql_value:
                    label_cases.append(
                        'COUNT(DISTINCT CASE WHEN sws."{col_name}" IS NULL THEN schools_school."id" ELSE NULL END) AS "{label}",'.format(
                            col_name=kwargs['col_name'],
                            label=label,
                        ))
                else:
                    values = set(values_l)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN LOWER(sws."{col_name}") NOT IN ({value}) THEN schools_school."id" ELSE NULL END) '
                            'AS "{label}",'.format(
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN sws."{col_name}" NOT IN ({value}) THEN schools_school."id" ELSE NULL END) '
                            'AS "{label}",'.format(
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
            sws."{col_name}" AS field_value,
            {label_case_statements}
            ST_AsGeoJSON(ST_Transform(schools_school."geopoint", 4326)) AS geopoint,
            CASE WHEN schools_school.connectivity_status IN ('good', 'moderate') THEN 'connected'
                   WHEN schools_school.connectivity_status = 'no' THEN 'not_connected' ELSE 'unknown' END as connectivity_status
        FROM "schools_school"
        INNER JOIN locations_country c ON c.id = schools_school.country_id
            AND c."deleted" IS NULL
        LEFT JOIN locations_countryadminmetadata AS adm1_metadata
            ON adm1_metadata."id" = schools_school.admin1_id
            AND adm1_metadata."layer_name" = 'adm1'
            AND adm1_metadata."deleted" IS NULL
        LEFT JOIN locations_countryadminmetadata AS adm2_metadata
            ON adm2_metadata."id" = schools_school.admin2_id
            AND adm2_metadata."layer_name" = 'adm2'
            AND adm2_metadata."deleted" IS NULL
        LEFT JOIN connection_statistics_schoolweeklystatus sws ON schools_school.last_weekly_status_id = sws.id
        WHERE "schools_school"."id" IN ({ids})
        """

        kwargs = copy.deepcopy(self.kwargs)
        kwargs['ids'] = ','.join(kwargs['school_ids'])

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
        for title, values_and_label in legend_configs.items():
            values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                 values_and_label.get('values', [])))

            if len(values) > 0:
                is_sql_value = 'SQL:' in values[0]
                if is_sql_value:
                    sql_statement = str(','.join(values)).replace('SQL:', '').format(
                        col_name=kwargs['col_name'],
                    )
                    label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    values_l.extend(values)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            """WHEN LOWER(sws."{col_name}") IN ({value}) THEN '{label}'""".format(
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            """WHEN sws."{col_name}" IN ({value}) THEN '{label}'""".format(
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

            parameter_column_name = str(parameter_col['name'])
            parameter_column_unit = str(parameter_col.get('unit', '')).lower()
            base_benchmark = str(parameter_col.get('base_benchmark', 1))

            self.update_kwargs(country_ids, data_layer_instance)
            benchmark_value, benchmark_unit = self.get_benchmark_value(data_layer_instance)

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
                    'base_benchmark': base_benchmark,
                    'live_source_types': ','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                    'parameter_col': parameter_col,
                    'is_reverse': data_layer_instance.is_reverse,
                })

                if len(self.kwargs.get('school_ids', [])) > 0:
                    info_panel_school_list = db_utilities.sql_to_response(self.get_school_view_info_query(),
                                                                          label=self.__class__.__name__)
                    statistics = db_utilities.sql_to_response(self.get_school_view_statistics_info_query(),
                                                              label=self.__class__.__name__)
                    graph_data, positive_speeds = self.generate_graph_data()

                    if len(info_panel_school_list) > 0:
                        for info_panel_school in info_panel_school_list:
                            info_panel_school['geopoint'] = json.loads(info_panel_school['geopoint'])
                            info_panel_school['statistics'] = list(filter(
                                lambda s: s['school_id'] == info_panel_school['id'], statistics))[-1]

                            live_avg = (round(sum(positive_speeds[str(info_panel_school['id'])]) / len(
                                positive_speeds[str(info_panel_school['id'])]), 2) if len(
                                positive_speeds[str(info_panel_school['id'])]) > 0 else 0)

                            info_panel_school['live_avg'] = live_avg
                            info_panel_school['graph_data'] = graph_data[str(info_panel_school['id'])]

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

                    query_response = db_utilities.sql_to_response(self.get_info_query(), label=self.__class__.__name__)[
                        -1]

                    graph_data, positive_speeds = self.generate_graph_data()
                    live_avg = round(sum(positive_speeds) / len(positive_speeds), 2) if len(positive_speeds) > 0 else 0

                    live_avg_connectivity = 'unknown'
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
                            'benchmark_unit': benchmark_unit,
                            'base_benchmark': base_benchmark,
                            'parameter_column_unit': parameter_column_unit,
                            'round_unit_value': unit_agg_str,
                        },
                    }
            else:
                legend_configs = data_layer_instance.legend_configs

                self.kwargs.update({
                    'col_name': parameter_column_name,
                    'legend_configs': legend_configs,
                    'parameter_col': parameter_col,
                })

                if len(self.kwargs.get('school_ids', [])) > 0:
                    info_panel_school_list = db_utilities.sql_to_response(self.get_static_school_view_info_query(),
                                                                          label=self.__class__.__name__)
                    statistics = db_utilities.sql_to_response(self.get_school_view_statistics_info_query(),
                                                              label=self.__class__.__name__)

                    if len(info_panel_school_list) > 0:
                        for info_panel_school in info_panel_school_list:
                            info_panel_school['geopoint'] = json.loads(info_panel_school['geopoint'])
                            info_panel_school['statistics'] = list(filter(
                                lambda s: s['school_id'] == info_panel_school['id'], statistics))[-1]

                    response = info_panel_school_list
                else:
                    query_labels = []
                    query_response = db_utilities.sql_to_response(self.get_static_info_query(query_labels),
                                                                  label=self.__class__.__name__)[-1]
                    response = {
                        'total_schools': query_response['total_schools'],
                        'connected_schools': {label: query_response[label] for label in query_labels},
                        'legend_configs': legend_configs,
                    }

            cache_manager.set(cache_key, response, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=response)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class DataLayerMapViewSet(BaseDataLayerAPIViewSet, account_utilities.BaseTileGenerator):

    def get_live_map_query(self, env, request):
        query = """
        WITH bounds AS (
                SELECT {env} AS geom,
                {env}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT DISTINCT ST_AsMVTGeom(ST_Transform("schools_school".geopoint, 3857), bounds.b2d) AS geom,
                    "schools_school".id,
                    CASE WHEN rt_status.rt_registered = True AND rt_status.rt_registration_date::date <= '{end_date}'
                        THEN True ELSE False
                    END as is_rt_connected,
                    t.field_avg AS field_avg,
                    {case_conditions}
                    CASE WHEN "schools_school".connectivity_status IN ('good', 'moderate') THEN 'connected'
                        WHEN "schools_school".connectivity_status = 'no' THEN 'not_connected'
                        ELSE 'unknown'
                    END as connectivity_status
                FROM schools_school
                INNER JOIN bounds ON ST_Intersects("schools_school".geopoint, ST_Transform(bounds.geom, 4326))
                {school_weekly_join}
                LEFT JOIN (
                    SELECT "schools_school"."id" AS school_id,
                        AVG(t."{col_name}") AS "field_avg"
                    FROM "schools_school"
                    INNER JOIN "connection_statistics_schoolrealtimeregistration"
                        ON ("schools_school"."id" = "connection_statistics_schoolrealtimeregistration"."school_id")
                    LEFT OUTER JOIN "connection_statistics_schooldailystatus" t
                        ON (
                            "schools_school"."id" = t."school_id"
                            AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                            AND t."live_data_source" IN ({live_source_types})
                        )
                    {school_weekly_join}
                    WHERE (
                        "schools_school"."deleted" IS NULL
                        AND "connection_statistics_schoolrealtimeregistration"."deleted" IS NULL
                        AND t."deleted" IS NULL
                        {country_condition}
                        {admin1_condition}
                        {school_condition}
                        {school_weekly_condition}
                        AND "connection_statistics_schoolrealtimeregistration"."rt_registered" = True
                        AND "connection_statistics_schoolrealtimeregistration"."rt_registration_date"::date <= '{end_date}')
                    GROUP BY "schools_school"."id"
                    ORDER BY "schools_school"."id" ASC
                ) as t
                    ON t.school_id = "schools_school".id
                LEFT JOIN connection_statistics_schoolrealtimeregistration rt_status
                    ON rt_status.school_id = "schools_school".id
                WHERE "schools_school"."deleted" IS NULL
                AND rt_status."deleted" IS NULL
                {country_outer_condition}
                {admin1_outer_condition}
                {school_outer_condition}
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

        kwargs['country_outer_condition'] = ''
        kwargs['admin1_outer_condition'] = ''
        kwargs['school_outer_condition'] = ''

        kwargs['school_weekly_join'] = ''
        kwargs['school_weekly_condition'] = ''

        kwargs['env'] = self.envelope_to_bounds_sql(env)

        kwargs['limit_condition'] = ''
        kwargs['random_order'] = ''

        add_random_condition = True

        kwargs['case_conditions'] = """
            CASE WHEN t.field_avg >  {benchmark_value} THEN 'good'
                WHEN t.field_avg < {benchmark_value} and t.field_avg >= {base_benchmark} THEN 'moderate'
                WHEN t.field_avg < {base_benchmark}  THEN 'bad'
                ELSE 'unknown'
            END AS field_status,
        """.format(**kwargs)

        if kwargs['is_reverse'] is True:
            kwargs['case_conditions'] = """
            CASE WHEN t.field_avg < {benchmark_value}  THEN 'good'
                WHEN t.field_avg >= {benchmark_value} and t.field_avg <= {base_benchmark} THEN 'moderate'
                WHEN t.field_avg > {base_benchmark} THEN 'bad'
                ELSE 'unknown'
            END AS field_status,
            """.format(**kwargs)

        if len(kwargs.get('country_ids', [])) > 0:
            add_random_condition = False
            kwargs['country_condition'] = 'AND "schools_school"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )
            kwargs['country_outer_condition'] = 'AND "schools_school"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs.get('admin1_ids', [])) > 0:
            add_random_condition = False
            kwargs['admin1_condition'] = 'AND "schools_school"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
            kwargs['admin1_outer_condition'] = 'AND "schools_school"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )

        if len(kwargs.get('school_ids', [])) > 0:
            add_random_condition = False
            kwargs['school_condition'] = 'AND "schools_school"."id" IN ({0})'.format(
                ','.join([str(school_id) for school_id in kwargs['school_ids']])
            )

            kwargs['school_outer_condition'] = 'AND "schools_school"."id" IN ({0})'.format(
                ','.join([str(school_id) for school_id in kwargs['school_ids']])
            )

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] += ' AND ' + kwargs['school_filters']
            kwargs['school_outer_condition'] += ' AND ' + kwargs['school_filters']

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            LEFT OUTER JOIN "connection_statistics_schoolweeklystatus"
                ON "schools_school"."last_weekly_status_id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = ' AND ' + kwargs['school_static_filters']

        if add_random_condition:
            kwargs['limit_condition'] = 'LIMIT ' + request.query_params.get('limit', '50000')
            kwargs['random_order'] = 'ORDER BY random()' if int(request.query_params.get('z', '0')) == 2 else ''

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
                schools_school.id,
                sws."{col_name}" AS field_value,
                CASE WHEN schools_school.connectivity_status IN ('good', 'moderate') THEN 'connected'
                    WHEN schools_school.connectivity_status = 'no' THEN 'not_connected'
                    ELSE 'unknown'
                END as connectivity_status,
                {label_case_statements}
            FROM schools_school
            INNER JOIN bounds ON ST_Intersects(schools_school.geopoint, ST_Transform(bounds.geom, 4326))
            {school_weekly_join}
            LEFT JOIN connection_statistics_schoolweeklystatus sws ON schools_school.last_weekly_status_id = sws.id
            WHERE schools_school."deleted" IS NULL
            AND sws."deleted" IS NULL
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

        add_random_condition = True

        if len(kwargs.get('country_ids', [])) > 0:
            add_random_condition = False
            kwargs['country_condition'] = 'AND schools_school."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs.get('admin1_ids', [])) > 0:
            add_random_condition = False
            kwargs['admin1_condition'] = 'AND schools_school."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )

        if len(kwargs.get('school_ids', [])) > 0:
            add_random_condition = False
            kwargs['school_condition'] = 'AND schools_school."id" IN ({0})'.format(
                ','.join([str(school_id) for school_id in kwargs['school_ids']])
            )

        if len(kwargs['school_filters']) > 0:
            kwargs['school_condition'] += ' AND ' + kwargs['school_filters']

        if len(kwargs['school_static_filters']) > 0:
            kwargs['school_weekly_join'] = """
            LEFT OUTER JOIN "connection_statistics_schoolweeklystatus"
                ON "schools_school"."last_weekly_status_id" = "connection_statistics_schoolweeklystatus"."id"
            """
            kwargs['school_weekly_condition'] = ' AND ' + kwargs['school_static_filters']

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
        for title, values_and_label in legend_configs.items():
            values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                 values_and_label.get('values', [])))

            if len(values) > 0:
                is_sql_value = 'SQL:' in values[0]
                if is_sql_value:
                    sql_statement = str(','.join(values)).replace('SQL:', '').format(
                        col_name=kwargs['col_name'],
                    )
                    label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    values_l.extend(values)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            """WHEN LOWER(sws."{col_name}") IN ({value}) THEN '{label}'""".format(
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            """WHEN sws."{col_name}" IN ({value}) THEN '{label}'""".format(
                                col_name=kwargs['col_name'],
                                label=title,
                                value=','.join([str(v) for v in values])
                            ))
            else:
                label_cases.append("ELSE '{label}'".format(label=title))

        kwargs['label_case_statements'] = 'CASE ' + ' '.join(label_cases) + 'END AS field_status'

        if add_random_condition:
            kwargs['limit_condition'] = 'LIMIT ' + request.query_params.get('limit', '50000')
            kwargs['random_order'] = 'ORDER BY random()' if int(request.query_params.get('z', '0')) == 2 else ''

        return query.format(**kwargs)

    def get(self, request, *args, **kwargs):
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

        parameter_column_name = str(parameter_col['name'])
        base_benchmark = str(parameter_col.get('base_benchmark', 1))

        self.update_kwargs(country_ids, data_layer_instance)
        benchmark_value, _ = self.get_benchmark_value(data_layer_instance)

        if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            self.kwargs.update({
                'col_name': parameter_column_name,
                'benchmark_value': benchmark_value,
                'base_benchmark': base_benchmark,
                'live_source_types': ','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                'parameter_col': parameter_col,
                'layer_type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
            })
        else:
            legend_configs = data_layer_instance.legend_configs

            self.kwargs.update({
                'col_name': parameter_column_name,
                'legend_configs': legend_configs,
                'parameter_col': parameter_col,
                'layer_type': accounts_models.DataLayer.LAYER_TYPE_STATIC,
            })

        try:
            return self.generate_tile(request)
        except Exception as ex:
            logger.error('Exception occurred for school connectivity tiles endpoint: {}'.format(ex))
            return Response({'error': 'An error occurred while processing the request'}, status=500)


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
                SELECT DISTINCT ST_AsMVTGeom(ST_Transform(t1.geopoint, 3857), bounds.b2d) AS geom,
                   t1.school_id,
                   t1.year,
                   CASE
                       WHEN rt_status.rt_registered = True
                            AND EXTRACT(YEAR FROM CAST(rt_status.rt_registration_date AS DATE)) <= t1.year THEN True
                       ELSE False
                   END as is_rt_connected,
                   t1.field_status
                FROM bounds
                INNER JOIN (
                    SELECT s.id AS school_id,
                        s.geopoint,
                        EXTRACT(YEAR FROM CAST(t.date AS DATE)) AS year,
                        {case_conditions}
                    FROM "schools_school" AS s
                    INNER JOIN "connection_statistics_schooldailystatus" t ON (
                        s."id" = t."school_id"
                        AND EXTRACT(YEAR FROM CAST(t.date AS DATE)) >= {start_year}
                        AND t."live_data_source" IN ({live_source_types}))
                    WHERE s."deleted" IS NULL
                        AND t."deleted" IS NULL
                        AND s."country_id" = {country_id}
                    GROUP BY s."id", year
                    ORDER BY s.id ASC, year ASC
                ) AS t1 ON ST_Intersects(t1.geopoint, ST_Transform(bounds.geom, 4326))
            LEFT JOIN connection_statistics_schoolrealtimeregistration rt_status ON rt_status.school_id = t1.school_id
            WHERE rt_status."deleted" IS NULL
            ORDER BY t1.school_id ASC, t1.year ASC
        )
        SELECT ST_AsMVT(DISTINCT mvtgeom.*) FROM mvtgeom;
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['env'] = self.envelope_to_bounds_sql(env)

        kwargs['case_conditions'] = """
            CASE
              WHEN AVG(t."{col_name}") > {benchmark_value} THEN 'good'
              WHEN AVG(t."{col_name}") < {benchmark_value}
                   and AVG(t."{col_name}") >= {base_benchmark} THEN 'moderate'
              WHEN AVG(t."{col_name}") < {base_benchmark} THEN 'bad'
              ELSE 'unknown'
            END AS field_status
        """.format(**kwargs)

        if kwargs['is_reverse'] is True:
            kwargs['case_conditions'] = """
            CASE
                WHEN AVG(t."{col_name}") < {benchmark_value} THEN 'good'
                WHEN AVG(t."{col_name}") >= {benchmark_value} AND AVG(t."{col_name}") <= {base_benchmark}
                    THEN 'moderate'
                WHEN AVG(t."{col_name}") > {base_benchmark} THEN 'bad'
              ELSE 'unknown'
            END AS field_status
            """.format(**kwargs)

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

        parameter_column_name = str(parameter_col['name'])
        base_benchmark = str(parameter_col.get('base_benchmark', 1))

        benchmark_val = data_layer_instance.global_benchmark.get('value')

        if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            self.kwargs.update({
                'country_id': country_id,
                'col_name': parameter_column_name,
                'benchmark_value': benchmark_val,
                'base_benchmark': base_benchmark,
                'live_source_types': ','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                'parameter_col': parameter_col,
                'layer_type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                'start_year': request.query_params.get('start_year', date_utilities.get_current_year() - 4),
                'is_reverse': data_layer_instance.is_reverse,
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

    permit_list_expands = ['created_by', 'last_modified_by']


class AdvanceFiltersViewSet(BaseModelViewSet):
    model = accounts_models.AdvanceFilter
    serializer_class = serializers.AdvanceFiltersListSerializer

    action_serializers = {
        'create': serializers.CreateAdvanceFilterSerializer,
        # 'partial_update': serializers.UpdateAdvanceFilterSerializer,
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
        perform_destroy
        :param instance:
        :return:
        """
        instance.deleted = core_utilities.get_current_datetime_object()
        instance.last_modified_at = core_utilities.get_current_datetime_object()
        instance.last_modified_by = core_utilities.get_current_user(request=self.request)
        return super().perform_destroy(instance)


class AdvanceFiltersPublishViewSet(BaseModelViewSet):
    model = accounts_models.AdvanceFilter
    serializer_class = serializers.PublishDataLayerSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPublishAdvanceFilter,
    )


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
