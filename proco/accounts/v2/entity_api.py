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
from proco.accounts.api import DataLayerInfoViewSet, DataLayerMapViewSet
from proco.accounts.config import app_config as account_config
from proco.accounts.v2 import entity_serializers
from proco.connection_statistics import models as statistics_models
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import SchoolWeeklyStatus, EntityWeeklyStatus
from proco.contact.models import ContactMessage
from proco.core import db_utils as db_utilities
from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.custom_auth import models as auth_models
from proco.entities.config import build_parameter_config, get_entity_type_config
from proco.entities.constants import LEGACY_MODEL
from proco.locations.models import Country
from proco.utils import dates as date_utilities
from proco.utils.cache import cache_manager, custom_cache_control, no_expiry_cache_manager
from proco.utils.filters import NullsAlwaysLastOrderingFilter
from proco.utils.mixins import CachedListMixin
from proco.utils.tasks import update_all_cached_values

logger = logging.getLogger('gigamaps.' + __name__)


class BaseEntityDataLayerAPIViewSet(APIView):
    model = accounts_models.DataLayer

    permission_classes = (
        permissions.AllowAny,
    )

    def get_column_function_sql(self, parameter_col_function):
        if isinstance(parameter_col_function, dict) and len(parameter_col_function) > 0:
            return parameter_col_function.get('sql').format(col_name='t."{col_name}"')
        return 'AVG(t."{col_name}")'

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

        if 'entity_id' in query_param_keys:
            self.kwargs['entity_ids'] = [str(query_params['entity_id']).strip()]
        elif 'entity_id__in' in query_param_keys:
            self.kwargs['entity_ids'] = [s_id.strip() for s_id in query_params['entity_id__in'].split(',')]

        self.kwargs['is_weekly'] = False if query_params.get('is_weekly', 'true') == 'false' else True
        self.kwargs['benchmark'] = 'national' if query_params.get('benchmark', 'global') == 'national' else 'global'

        self.kwargs['convert_unit'] = layer_instance.global_benchmark.get('convert_unit', 'mbps')
        self.kwargs['is_reverse'] = layer_instance.is_reverse

        self.kwargs['school_filters'] = core_utilities.get_filter_sql(
            self.request, 'schools', 'schools_school')
        self.kwargs['school_static_filters'] = core_utilities.get_filter_sql(
            self.request, 'school_static', 'connection_statistics_schoolweeklystatus')

        if layer_instance.entity_type is not None and not layer_instance.entity_type.is_legacy:
            entity_type = layer_instance.entity_type.code
            entity_static_table = f"entities_{entity_type}_entity"

            self.kwargs['entity_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity', 'entities_entity')
            self.kwargs['entity_static_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity_static', entity_static_table)
            self.kwargs['entity_real_time_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity_real_time', 'connection_statistics_entityweeklystatus')
        else:
            self.kwargs['entity_filters'] = ''
            self.kwargs['entity_static_filters'] = ''
            self.kwargs['entity_real_time_filters'] = ''

    @staticmethod
    def _parse_date(value, param_name):
        """Parse a date string using DATE_FORMAT; raise ValueError on failure."""
        parsed = date_utilities.to_date(value)
        if parsed is None:
            from django.conf import settings
            raise ValueError(
                f"Invalid date format for '{param_name}': '{value}'. Expected format: {settings.DATE_FORMAT}"
            )
        return parsed.date()

    def update_kwargs_from_dict(self, country_ids, layer_instance, query_params):
        """Update kwargs from a query_params dict instead of request.query_params."""
        self.kwargs.setdefault('country_ids', [])
        self.kwargs.setdefault('admin1_ids', [])
        self.kwargs.setdefault('entity_ids', [])
        self.kwargs.setdefault('school_ids', [])
        self.kwargs.setdefault('entity_filters', '')
        self.kwargs.setdefault('entity_static_filters', '')
        self.kwargs.setdefault('entity_real_time_filters', '')
        self.kwargs.setdefault('school_filters', '')
        self.kwargs.setdefault('school_static_filters', '')
        self.kwargs.setdefault('convert_unit', 'mbps')
        self.kwargs.setdefault('is_weekly', True)
        self.kwargs.setdefault('benchmark', 'global')

        query_param_keys = query_params.keys()

        if 'start_date' in query_param_keys:
            self.kwargs['start_date'] = self._parse_date(query_params['start_date'], 'start_date')
        elif layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            date = core_utilities.get_current_datetime_object() - timedelta(days=7)
            self.kwargs['start_date'] = (date - timedelta(days=date.weekday())).date()

        if 'end_date' in query_param_keys:
            self.kwargs['end_date'] = self._parse_date(query_params['end_date'], 'end_date')
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

        if 'entity_id' in query_param_keys:
            self.kwargs['entity_ids'] = [str(query_params['entity_id']).strip()]
        elif 'entity_id__in' in query_param_keys:
            self.kwargs['entity_ids'] = [s_id.strip() for s_id in query_params['entity_id__in'].split(',')]

        self.kwargs['is_weekly'] = False if query_params.get('is_weekly', 'true') == 'false' else True
        self.kwargs['benchmark'] = 'national' if query_params.get('benchmark', 'global') == 'national' else 'global'

        self.kwargs['convert_unit'] = layer_instance.global_benchmark.get('convert_unit', 'mbps')
        self.kwargs['is_reverse'] = layer_instance.is_reverse

        self.kwargs['school_filters'] = core_utilities.get_filter_sql(
            self.request, 'schools', 'schools_school')
        self.kwargs['school_static_filters'] = core_utilities.get_filter_sql(
            self.request, 'school_static', 'connection_statistics_schoolweeklystatus')

        if layer_instance.entity_type is not None and not layer_instance.entity_type.is_legacy:
            entity_type = layer_instance.entity_type.code
            entity_static_table = f"entities_{entity_type}_entity"

            self.kwargs['entity_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity', 'entities_entity')
            self.kwargs['entity_static_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity_static', entity_static_table)
            self.kwargs['entity_real_time_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity_real_time', 'connection_statistics_entityweeklystatus')

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


@method_decorator([
    custom_cache_control(
        public=True,
        max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE,
        cache_status_codes=[rest_status.HTTP_200_OK, ],
    )
], name='dispatch')
class EntityDataLayerMapViewSet(BaseEntityDataLayerAPIViewSet, account_utilities.BaseTileGenerator):
    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'ENTITY_DATA_LAYER_MAP'

    def get_cache_key(self):
        pk = self.kwargs.get('pk')
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}_{2}'.format(
            self.CACHE_KEY_PREFIX,
            pk,
            '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))),
        )

    def envelope_to_sql(self, env, request):
        if self.kwargs['layer_type'] == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            return self.get_live_entity_map_query(env, request)
        return self.get_static_entity_map_query(env, request)

    def get_live_entity_map_query(self, env, request):
        query = """
        WITH bounds AS (
                SELECT {env} AS geom,
                {env}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT DISTINCT ST_AsMVTGeom(ST_Transform("entities_entity".geopoint, 3857), bounds.b2d) AS geom,
                    {random_select_list}
                    "entities_entity".id,
                    True AS is_rt_connected,
                    eds.{col_name} AS field_avg,
                    {case_conditions}
                    'connected' AS connectivity_status
                FROM entities_entity
                INNER JOIN bounds ON ST_Intersects("entities_entity".geopoint, ST_Transform(bounds.geom, 4326))
                INNER JOIN (
                    SELECT "entities_entity"."id" AS entity_id,
                        "entities_entity"."last_weekly_status_id",
                        AVG(t."{col_name}") AS "{col_name}"
                    FROM "entities_entity"
                    INNER JOIN connection_statistics_entityrealtimeregistration rt_status ON
                        rt_status."entity_id" = "entities_entity".id
                    {entity_weekly_join}
                    LEFT OUTER JOIN "connection_statistics_entitydailystatus" t ON (
                        "entities_entity"."id" = t."entity_id"
                        AND t."deleted" IS NULL
                        AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                        AND t."live_data_source" IN ({live_source_types})
                    )
                    WHERE (
                        "entities_entity"."deleted" IS NULL
                        AND rt_status."deleted" IS NULL
                        AND entities_entity.entity_type_id = (SELECT id FROM entities_entity_type WHERE code = '{entity_name}' AND deleted IS NULL)
                        {country_condition}
                        {admin1_condition}
                        {entity_condition}
                        {entity_weekly_condition}
                        {entity_master_table_condition}
                        AND rt_status."rt_registered" = True
                        AND rt_status."rt_registration_date"::date <= '{end_date}'
                    )
                    GROUP BY "entities_entity"."id"
                ) AS eds ON eds.entity_id = "entities_entity".id
                {entity_weekly_outer_join}
                WHERE "entities_entity"."deleted" IS NULL
                    {random_order}
                    {limit_condition}
            )
            SELECT ST_AsMVT(DISTINCT mvtgeom.*) FROM mvtgeom;
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['entity_condition'] = ''

        kwargs['entity_weekly_join'] = ''
        kwargs['entity_weekly_condition'] = ''
        kwargs['entity_weekly_outer_join'] = ''
        kwargs['entity_master_table_join'] = ''
        kwargs['entity_master_table_condition'] = ''

        kwargs['env'] = self.envelope_to_bounds_sql(env)

        kwargs['limit_condition'] = ''
        kwargs['random_order'] = ''
        kwargs['random_select_list'] = ''

        add_random_condition = True

        legend_configs = kwargs['legend_configs']
        entity_type_obj = get_entity_type_config(kwargs['entity_name'])
        kwargs['parameter_col'] = build_parameter_config(
            entity_type_obj,
            kwargs['col_name'],
            kwargs['entity_name']
        )
        kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'entities_entity')
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
            kwargs['entity_weekly_outer_join'] = """
            INNER JOIN "connection_statistics_entityweeklystatus" ews ON eds."last_weekly_status_id" = ews."id"
            """
        else:
            kwargs['case_conditions'] = """
                CASE WHEN eds.{col_name} >  {benchmark_value} THEN 'good'
                    WHEN eds.{col_name} < {benchmark_value} AND eds.{col_name} >= {base_benchmark} THEN 'moderate'
                    WHEN eds.{col_name} < {base_benchmark}  THEN 'bad'
                    ELSE 'unknown'
                END AS field_status,
            """.format(**kwargs)

            if kwargs['is_reverse'] is True:
                kwargs['case_conditions'] = """
                CASE WHEN eds.{col_name} < {benchmark_value}  THEN 'good'
                    WHEN eds.{col_name} >= {benchmark_value} AND eds.{col_name} <= {base_benchmark} THEN 'moderate'
                    WHEN eds.{col_name} > {base_benchmark} THEN 'bad'
                    ELSE 'unknown'
                END AS field_status,
                """.format(**kwargs)

        if len(kwargs.get('entity_ids', [])) > 0:
            add_random_condition = False
            kwargs['entity_condition'] = 'AND "entities_entity"."id" IN ({0})'.format(
                ','.join([str(entity_id) for entity_id in kwargs['entity_ids']])
            )
        elif len(kwargs.get('admin1_ids', [])) > 0:
            if settings.ADMIN_MAP_API_SAMPLING_LIMIT is not None:
                kwargs['MAP_API_SAMPLING_LIMIT'] = settings.ADMIN_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            kwargs['admin1_condition'] = 'AND "entities_entity"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
        elif len(kwargs.get('country_ids', [])) > 0:
            if settings.COUNTRY_MAP_API_SAMPLING_LIMIT:
                kwargs['MAP_API_SAMPLING_LIMIT'] = settings.COUNTRY_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            kwargs['country_condition'] = 'AND "entities_entity"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs['entity_filters']) > 0:
            kwargs['entity_condition'] += ' AND ' + kwargs['entity_filters']

        if len(kwargs['entity_real_time_filters']) > 0:
            kwargs['entity_weekly_join'] = """
            INNER JOIN "connection_statistics_entityweeklystatus"
                ON "entities_entity"."last_weekly_status_id" = "connection_statistics_entityweeklystatus"."id"
            """
            kwargs['entity_weekly_condition'] = ' AND ' + kwargs['entity_real_time_filters']

        # if len(kwargs['entity_static_filters']) > 0:
        #     kwargs['entity_master_table_join'] = """
        #     INNER JOIN "entities_{entity_name}_entity"
        #         ON "entities_entity"."last_master_status_id" = "entities_{entity_name}_entity"."id"
        #     """.format(entity_name=kwargs['entity_name'])

        # kwargs['entity_master_table_condition'] = ' AND ' + kwargs['entity_static_filters']

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

    def get_static_entity_map_query(self, env, request):
        query = """
        WITH
        bounds AS (
            SELECT {env} AS geom,
                   {env}::box2d AS b2d
        ),
        mvtgeom AS (
            SELECT DISTINCT ST_AsMVTGeom(ST_Transform(entities_entity.geopoint, 3857), bounds.b2d) AS geom,
                {random_select_list}
                entities_entity.id,
                {table_name}."{col_name}" AS field_value,
                'connected' AS connectivity_status,
                {label_case_statements}
            FROM entities_entity
            INNER JOIN bounds ON ST_Intersects(entities_entity.geopoint, ST_Transform(bounds.geom, 4326))
            INNER JOIN entities_{entity_name}_entity ews ON "entities_entity"."id" = ews."entity_id"
            {entity_master_table_condition}
            WHERE entities_entity."deleted" IS NULL
            AND entities_entity.entity_type_id = (SELECT id FROM entities_entity_type WHERE code = '{entity_name}' AND deleted IS NULL)
            {country_condition}
            {admin1_condition}
            {entity_condition}
            {random_order}
            {limit_condition}
        )
        SELECT ST_AsMVT(DISTINCT mvtgeom.*) FROM mvtgeom;
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['entity_condition'] = ''

        kwargs['entity_master_table_join'] = ''
        kwargs['entity_master_table_condition'] = ''

        kwargs['env'] = self.envelope_to_bounds_sql(env)

        kwargs['limit_condition'] = ''
        kwargs['random_order'] = ''
        kwargs['random_select_list'] = ''

        add_random_condition = True

        if len(kwargs.get('entity_ids', [])) > 0:
            add_random_condition = False
            kwargs['entity_condition'] = 'AND entities_entity."id" IN ({0})'.format(
                ','.join([str(entity_id) for entity_id in kwargs['entity_ids']])
            )
        elif len(kwargs.get('admin1_ids', [])) > 0:
            if settings.ADMIN_MAP_API_SAMPLING_LIMIT:
                kwargs['MAP_API_SAMPLING_LIMIT'] = settings.ADMIN_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            kwargs['admin1_condition'] = 'AND entities_entity."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
        elif len(kwargs.get('country_ids', [])) > 0:
            if settings.COUNTRY_MAP_API_SAMPLING_LIMIT:
                kwargs['MAP_API_SAMPLING_LIMIT'] = settings.COUNTRY_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            kwargs['country_condition'] = 'AND entities_entity."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs['entity_filters']) > 0:
            kwargs['entity_condition'] += ' AND ' + kwargs['entity_filters']

        # if len(kwargs['entity_real_time_filters']) > 0:
        #     kwargs['entity_weekly_join'] = """
        #     INNER JOIN "connection_statistics_entityweeklystatus"
        #         ON ews."id" = "connection_statistics_entityweeklystatus"."id"
        #     """
        #     kwargs['entity_weekly_condition'] = ' AND ' + kwargs['entity_real_time_filters']

        if len(kwargs['entity_static_filters']) > 0:
            kwargs['entity_master_table_join'] = """
            INNER JOIN "entities_{entity_name}_entity"
                ON "entities_entity"."id" = ews."entity_id"
            """.format(entity_name=kwargs['entity_name'])

            kwargs['entity_master_table_condition'] = ' AND ' + kwargs['entity_static_filters']

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        entity_type_obj = get_entity_type_config(kwargs['entity_name'])
        kwargs['parameter_col'] = build_parameter_config(
            entity_type_obj,
            kwargs['col_name'],
            kwargs['entity_name']
        )
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
        kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'ews')

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

        date = core_utilities.get_current_datetime_object().date() - timedelta(
            weeks=settings.LIVE_LAYER_CACHE_FOR_WEEKS)
        if self.kwargs['start_date'] >= date:
            return True

        return False

    def get(self, request, *args, **kwargs):
        entity_type = self.request.query_params.get('entity_type__code')
        if entity_type == LEGACY_MODEL:
            school_view = DataLayerMapViewSet.as_view()
            return school_view(request._request, *args, **kwargs)

        # If entity_type__code is not provided, we will extract entity using data layer :
        # NEED TO CONFIRM WITH FE ON THIS
        # data_layer_instance = get_object_or_404(
        #     accounts_models.DataLayer.objects.all(),
        #     pk=self.kwargs.get('pk'),
        #     status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
        #
        # )
        # if data_layer_instance.entity_type.code == LEGACY_MODEL:
        #     school_view = DataLayerMapViewSet.as_view()
        #     return school_view(request._request, *args, **kwargs)

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
                    'layer_type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                    'legend_configs': legend_configs,
                    'entity_name': data_layer_instance.entity_name,
                })
            else:
                self.kwargs.update({
                    'col_name': parameter_column_name,
                    'legend_configs': legend_configs,
                    'parameter_col': parameter_col,
                    'layer_type': accounts_models.DataLayer.LAYER_TYPE_STATIC,
                    'entity_name': data_layer_instance.entity_name,
                })

            try:
                response = self.generate_tile(request)
                if self.cache_enabled(data_layer_instance) and response.status_code == rest_status.HTTP_200_OK:
                    cache_manager.set(cache_key, response, request_path=request_path,
                                      soft_timeout=settings.CACHE_CONTROL_MAX_AGE)
            except Exception as ex:
                logger.error('Exception occurred for entity connectivity tiles endpoint: {}'.format(ex))
                response = Response({'error': 'An error occurred while processing the request'}, status=500)

        return response


@method_decorator([
    custom_cache_control(
        public=True,
        max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE,
        cache_status_codes=[rest_status.HTTP_200_OK, ],
    )
], name='dispatch')
class EntityDataLayerInfoViewSet(BaseEntityDataLayerAPIViewSet):
    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'ENTITY_DATA_LAYER_INFO'

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        # Build cache key from entity layer IDs since pk is no longer in URL
        entity_params = self.extract_entity_params(self.request)
        entity_keys = []
        for entity_code, entity_config in sorted(entity_params.items()):
            if 'layer_id' in entity_config:
                entity_keys.append(f"{entity_code}_{entity_config['layer_id']}")

        entity_part = '_'.join(entity_keys) if entity_keys else 'no_entities'

        return '{0}_{1}_{2}'.format(
            self.CACHE_KEY_PREFIX,
            entity_part,
            '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))),
        )

    def get_info_query(self, query_labels=None):
        if query_labels is None:
            query_labels = []
        query = """
        SELECT {case_conditions}
            COUNT(DISTINCT CASE WHEN eds.{col_name} IS NOT NULL THEN eds.entity_id ELSE NULL END)
                AS "entity_with_realtime_data",
            {benchmark_value_sql}
            COUNT(DISTINCT eds.entity_id) AS "no_of_entities_measure"
        FROM (
            SELECT "entities_entity"."id" AS entity_id,
                "entities_entity"."last_weekly_status_id",
                AVG(t."{col_name}") AS "{col_name}"
            FROM "entities_entity"
            INNER JOIN "connection_statistics_entityrealtimeregistration"
                ON ("entities_entity"."id" = "connection_statistics_entityrealtimeregistration"."entity_id")
            {entity_weekly_join}
            LEFT OUTER JOIN "connection_statistics_entitydailystatus" t
                ON (
                    "entities_entity"."id" = t."entity_id"
                    AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                    AND t."live_data_source" IN ({live_source_types})
                    AND t."deleted" IS NULL
                )
            WHERE (
                "entities_entity"."deleted" IS NULL
                AND entities_entity.entity_type_id = (SELECT id FROM entities_entity_type WHERE code = '{entity_name}' AND deleted IS NULL)
                AND "connection_statistics_entityrealtimeregistration"."deleted" IS NULL
                {country_condition}
                {admin1_condition}
                {entity_condition}
                {entity_weekly_condition}
                AND "connection_statistics_entityrealtimeregistration"."rt_registered" = True
                AND "connection_statistics_entityrealtimeregistration"."rt_registration_date"::date <= '{end_date}')
            GROUP BY "entities_entity"."id"
            ORDER BY "entities_entity"."id" ASC
        ) AS eds
        {entity_weekly_outer_join}
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['entity_condition'] = ''
        kwargs['entity_weekly_join'] = ''
        kwargs['entity_weekly_condition'] = ''
        kwargs['entity_weekly_outer_join'] = ''
        kwargs['benchmark_value_sql'] = ''

        benchmark_value = kwargs['benchmark_value']
        if benchmark_value and 'SQL:' in str(benchmark_value):
            kwargs['benchmark_value_sql'] = benchmark_value.replace('SQL:', '').format(
                **kwargs) + ' AS benchmark_sql_value,'

        legend_configs = kwargs['legend_configs']
        entity_type_obj = get_entity_type_config(kwargs['entity_name'])
        try:
            kwargs['parameter_col'] = build_parameter_config(
                entity_type_obj,
                kwargs['col_name'],
                kwargs['entity_name']
            )
        except Exception as e:
            raise ValueError(
                f"Column '{kwargs['col_name']}' not found in any model for entity '{kwargs['entity_name']}'. "
                f"Check the DataLayer's data source column configuration."
            ) from e
        kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'entities_entity')
        if len(legend_configs) > 0 and 'SQL:' in str(legend_configs):
            label_cases = []
            for title, values_and_label in legend_configs.items():
                label = values_and_label.get('labels', title).strip()
                query_labels.append(label)
                values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                     values_and_label.get('values', [])))

                if len(values) > 0:
                    is_sql_value = 'SQL:' in values[0]
                    if is_sql_value:
                        sql_statement = str(','.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN {sql} THEN eds.entity_id ELSE NULL END) AS "{label}",'.format(
                                sql=sql_statement, label=label))
                else:
                    label_cases.append(
                        'COUNT(DISTINCT CASE WHEN eds.{col_name} IS NULL '
                        'THEN eds.entity_id ELSE NULL END) AS "{label}",'.format(
                            col_name=kwargs['col_name'], label=label))

            kwargs['case_conditions'] = ' '.join(label_cases)

            kwargs['entity_weekly_outer_join'] = """
            INNER JOIN "connection_statistics_entityweeklystatus" ews ON eds."last_weekly_status_id" = ews."id"
            """
        else:
            kwargs['case_conditions'] = """
            COUNT(DISTINCT CASE WHEN eds.{col_name} > {benchmark_value} THEN eds.entity_id ELSE NULL END) AS "good",
            COUNT(DISTINCT CASE WHEN (eds.{col_name} >= {base_benchmark} AND eds.{col_name} <= {benchmark_value})
                THEN eds.entity_id ELSE NULL END) AS "moderate",
            COUNT(DISTINCT CASE WHEN eds.{col_name} < {base_benchmark} THEN eds.entity_id ELSE NULL END) AS "bad",
            COUNT(DISTINCT CASE WHEN eds.{col_name} IS NULL THEN eds.entity_id ELSE NULL END) AS "unknown",
            """.format(**kwargs)

            if kwargs['is_reverse'] is True:
                kwargs['case_conditions'] = """
                COUNT(DISTINCT CASE WHEN eds.{col_name} < {benchmark_value} THEN eds.entity_id ELSE NULL END) AS "good",
                COUNT(DISTINCT CASE WHEN (eds.{col_name} >= {benchmark_value} AND eds.{col_name} <= {base_benchmark})
                    THEN eds.entity_id ELSE NULL END) AS "moderate",
                COUNT(DISTINCT CASE WHEN eds.{col_name} > {base_benchmark} THEN eds.entity_id ELSE NULL END) AS "bad",
                COUNT(DISTINCT CASE WHEN eds.{col_name} IS NULL THEN eds.entity_id ELSE NULL END) AS "unknown",
                """.format(**kwargs)

        if len(kwargs.get('admin1_ids', [])) > 0:
            kwargs['admin1_condition'] = 'AND "entities_entity"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
        elif len(kwargs.get('country_ids', [])) > 0:
            kwargs['country_condition'] = 'AND "entities_entity"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs['entity_filters']) > 0:
            kwargs['entity_condition'] = ' AND ' + kwargs['entity_filters']

        if len(kwargs['entity_static_filters']) > 0:
            kwargs['entity_weekly_join'] = """
            INNER JOIN "connection_statistics_entityweeklystatus"
                ON "entities_entity"."last_weekly_status_id" = "connection_statistics_entityweeklystatus"."id"
            """
            kwargs['entity_weekly_condition'] = ' AND ' + kwargs['entity_static_filters']

        return query.format(**kwargs)

    def get_entity_view_info_query(self):
        query = """
        SELECT DISTINCT entities_entity."id",
            entities_entity."name",
            entities_entity."external_id",
            entities_entity."giga_id",
            CASE WHEN err."rt_registered" = True THEN true ELSE false END AS is_data_synced,
            entities_entity."admin1_id",
            adm1_metadata."name" AS admin1_name,
            adm1_metadata."giga_id_admin" AS admin1_code,
            adm1_metadata."description_ui_label" AS admin1_description_ui_label,
            entities_entity."admin2_id",
            adm2_metadata."name" AS admin2_name,
            adm2_metadata."giga_id_admin" AS admin2_code,
            adm2_metadata."description_ui_label" AS admin2_description_ui_label,
            entities_entity."country_id",
            c."name" AS country_name,
            ST_AsGeoJSON(ST_Transform(entities_entity."geopoint", 4326)) AS geopoint,
            entities_entity."environment",
            ROUND(eds."{col_name}"::numeric, 2) AS "live_avg",
            ews."download_speed_benchmark",
            CASE WHEN entities_entity.connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN entities_entity.connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END AS connectivity_status,
            CASE WHEN err."rt_registered" = True AND err."rt_registration_date"::date <= '{end_date}' THEN true
            ELSE false END AS is_rt_connected,
            {benchmark_value_sql}
            {case_conditions}
        FROM "entities_entity" entities_entity
        INNER JOIN public.locations_country c ON c."id" = entities_entity."country_id"
        INNER JOIN "connection_statistics_entityweeklystatus" ews ON entities_entity."last_weekly_status_id" =
        ews."id"
        LEFT JOIN public.locations_countryadminmetadata AS adm1_metadata
            ON adm1_metadata."id" = entities_entity.admin1_id
            AND adm1_metadata."layer_name" = 'adm1'
            AND adm1_metadata."deleted" IS NULL
        LEFT JOIN public.locations_countryadminmetadata AS adm2_metadata
            ON adm2_metadata."id" = entities_entity.admin2_id
            AND adm2_metadata."layer_name" = 'adm2'
            AND adm2_metadata."deleted" IS NULL
        LEFT JOIN "connection_statistics_entityrealtimeregistration" AS err
            ON entities_entity."id" = err."entity_id"
            AND err."deleted" IS NULL
        LEFT JOIN (
            SELECT "entities_entity"."id" AS entity_id,
                AVG(t."{col_name}") AS "{col_name}"
            FROM "entities_entity"
            LEFT OUTER JOIN "connection_statistics_entitydailystatus" t
                ON (
                    "entities_entity"."id" = t."entity_id"
                    AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                    AND t."live_data_source" IN ({live_source_types})
                    AND t."deleted" IS NULL
                )
            WHERE ("entities_entity"."id" IN ({ids})
                AND "entities_entity"."deleted" IS NULL
                AND entities_entity.entity_type_id = (SELECT id FROM entities_entity_type WHERE code = '{entity_name}' AND deleted IS NULL))
            GROUP BY "entities_entity"."id"
            ORDER BY "entities_entity"."id" ASC
        ) AS eds ON eds.entity_id = entities_entity.id
        WHERE "entities_entity"."id" IN ({ids})
            AND c."deleted" IS NULL
            AND entities_entity."deleted" IS NULL
            AND entities_entity.entity_type_id = (SELECT id FROM entities_entity_type WHERE code = '{entity_name}' AND deleted IS NULL)
        GROUP BY entities_entity."id", err."rt_registered", err."rt_registration_date",
            adm1_metadata."name", adm1_metadata."description_ui_label",
            adm2_metadata."name", adm2_metadata."description_ui_label",
            c."name", adm1_metadata."giga_id_admin", adm2_metadata."giga_id_admin",
            eds."{col_name}", ews."download_speed_benchmark"
        ORDER BY entities_entity."id" ASC
        """

        kwargs = copy.deepcopy(self.kwargs)
        kwargs['ids'] = ','.join(kwargs['entity_ids'])

        kwargs['benchmark_value_sql'] = ''
        benchmark_value = kwargs['benchmark_value']
        if benchmark_value and 'SQL:' in str(benchmark_value):
            kwargs['benchmark_value_sql'] = benchmark_value.replace('SQL:', '').format(
                **kwargs) + ' AS benchmark_sql_value,'

        legend_configs = kwargs['legend_configs']
        entity_type_obj = get_entity_type_config(kwargs['entity_name'])
        try:
            kwargs['parameter_col'] = build_parameter_config(
                entity_type_obj,
                kwargs['col_name'],
                kwargs['entity_name']
            )
        except Exception as e:
            raise ValueError(
                f"Column '{kwargs['col_name']}' not found in any model for entity '{kwargs['entity_name']}'. "
                f"Check the DataLayer's data source column configuration."
            ) from e
        kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'entities_entity')
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
                WHEN eds."{col_name}" > {benchmark_value} THEN 'good'
                WHEN (eds."{col_name}" >= {base_benchmark} AND eds."{col_name}" <= {benchmark_value})
                    THEN 'moderate'
                WHEN eds."{col_name}" < {base_benchmark} THEN 'bad'
                ELSE 'unknown' END AS live_avg_connectivity
            """.format(**kwargs)

            if kwargs['is_reverse'] is True:
                kwargs['case_conditions'] = """
                CASE
                    WHEN eds."{col_name}" < {benchmark_value} THEN 'good'
                    WHEN (eds."{col_name}" >= {benchmark_value} AND eds."{col_name}" <= {base_benchmark})
                        THEN 'moderate'
                    WHEN eds."{col_name}" > {base_benchmark} THEN 'bad'
                    ELSE 'unknown' END AS live_avg_connectivity
                """.format(**kwargs)

        return query.format(**kwargs)

    def get_entity_view_statistics_info_query(self, layer_type):
        if layer_type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            join_condition = """
            INNER JOIN connection_statistics_entityweeklystatus ews
                ON "entities_entity"."last_weekly_status_id" = ews."id"
            """
        else:
            join_condition = """
            INNER JOIN entities_{entity_name}_entity ews
                ON "entities_entity"."id" = ews."entity_id"
            """.format(entity_name=self.kwargs['entity_name'])

        query = """
        SELECT ews.*
        FROM "entities_entity"
        {join_condition}
        WHERE "entities_entity"."deleted" IS NULL
            AND "entities_entity"."id" IN ({ids})
        """.format(ids=','.join(self.kwargs['entity_ids']), join_condition=join_condition)

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
        SELECT {entity_selection}t."date" AS date,
            AVG(t."{col_name}") AS "field_avg"
        FROM "entities_entity"
        INNER JOIN "connection_statistics_entityrealtimeregistration" ON
            "connection_statistics_entityrealtimeregistration"."entity_id" = "entities_entity"."id"
        INNER JOIN "connection_statistics_entitydailystatus" t ON "entities_entity"."id" = t."entity_id"
        {entity_weekly_join}
        WHERE (
            {country_condition}
            {admin1_condition}
            {entity_condition}
            {entity_weekly_condition}
            "connection_statistics_entityrealtimeregistration"."deleted" IS NULL
            AND "connection_statistics_entityrealtimeregistration"."rt_registered" = True
            AND "connection_statistics_entityrealtimeregistration"."rt_registration_date"::date <= '{end_date}'
            AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
            AND t."live_data_source" IN ({live_source_types})
            AND t."deleted" IS NULL
            AND t."{col_name}" IS NOT NULL
        )
        GROUP BY t."date"{entity_group_by}
        ORDER BY t."date" ASC
        """

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['entity_condition'] = ''
        kwargs['entity_selection'] = ''
        kwargs['entity_group_by'] = ''
        kwargs['entity_weekly_join'] = ''
        kwargs['entity_weekly_condition'] = ''

        if len(kwargs.get('entity_ids', [])) > 0:
            kwargs['entity_condition'] = '"entities_entity"."id" IN ({0}) AND '.format(','.join(kwargs['entity_ids']))
            kwargs['entity_selection'] = '"entities_entity"."id", '
            kwargs['entity_group_by'] = ', "entities_entity"."id"'
        elif len(kwargs.get('admin1_ids', [])) > 0:
            kwargs['admin1_condition'] = '"entities_entity"."admin1_id" IN ({0}) AND'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
        elif len(kwargs.get('country_ids', [])) > 0:
            kwargs['country_condition'] = '"entities_entity"."country_id" IN ({0}) AND'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs['entity_filters']) > 0:
            kwargs['entity_condition'] += kwargs['entity_filters'] + ' AND '

        if len(kwargs['entity_static_filters']) > 0:
            kwargs['entity_weekly_join'] = """
            INNER JOIN "connection_statistics_entityweeklystatus"
                ON "entities_entity"."last_weekly_status_id" = "connection_statistics_entityweeklystatus"."id"
            """
            kwargs['entity_weekly_condition'] = kwargs['entity_static_filters'] + ' AND '

        return query.format(**kwargs)

    def _generate_school_graph_data(self, school_viewset_cls):
        """
        Generate graph data using the legacy school daily status tables.
        Delegates avg query generation to school_viewset_cls.get_avg_query.
        """
        kwargs = copy.deepcopy(self.kwargs)

        data = db_utilities.sql_to_response(
            school_viewset_cls.get_avg_query(self, **kwargs),
            label=self.__class__.__name__,
            db_var=settings.READ_ONLY_DB_KEY,
        )

        graph_data = []
        current_date = kwargs['start_date']
        while current_date <= kwargs['end_date']:
            graph_data.append({
                'group': 'Speed',
                'key': date_utilities.format_date(current_date),
                'value': None,
            })
            current_date += timedelta(days=1)

        round_unit_value = kwargs['round_unit_value']
        all_positive_speeds = []

        for daily_avg_data in (data or []):
            formatted_date = date_utilities.format_date(daily_avg_data['date'])
            for entry in graph_data:
                if entry['key'] == formatted_date:
                    try:
                        rounded_speed = 0
                        if daily_avg_data['field_avg'] is not None:
                            rounded_speed = round(
                                eval(round_unit_value.format(val=daily_avg_data['field_avg'])), 2)
                        entry['value'] = rounded_speed
                        all_positive_speeds.append(rounded_speed)
                    except (KeyError, TypeError):
                        pass

        return graph_data, all_positive_speeds

    def generate_graph_data(self):
        kwargs = copy.deepcopy(self.kwargs)

        # Get the daily connectivity_speed for the given country from SchoolDailyStatus model
        data = db_utilities.sql_to_response(self.get_avg_query(**kwargs), label=self.__class__.__name__,
                                            db_var=settings.READ_ONLY_DB_KEY)

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

        if len(kwargs.get('entity_ids', [])) > 0:
            graph_data_per_entity = {}
            all_positive_speeds_per_entity = {}

            for entity_id in kwargs.get('entity_ids', []):
                graph_data_per_entity[entity_id] = copy.deepcopy(graph_data)
                all_positive_speeds_per_entity[entity_id] = []

            # Update the graph_data with actual values if they exist
            for daily_avg_data in data:
                entity_id = str(daily_avg_data['id'])
                formatted_date = date_utilities.format_date(daily_avg_data['date'])
                entity_graph_data = graph_data_per_entity[entity_id]
                entity_all_positive_speeds = all_positive_speeds_per_entity[entity_id]
                for entry in entity_graph_data:
                    if entry['key'] == formatted_date:
                        try:
                            rounded_speed = 0
                            if daily_avg_data['field_avg'] is not None:
                                rounded_speed = round(eval(round_unit_value.format(val=daily_avg_data['field_avg'])), 2)
                            entry['value'] = rounded_speed
                            entity_all_positive_speeds.append(rounded_speed)
                        except (KeyError, TypeError):
                            pass
                graph_data_per_entity[entity_id] = entity_graph_data
                all_positive_speeds_per_entity[entity_id] = entity_all_positive_speeds
            return graph_data_per_entity, all_positive_speeds_per_entity

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
            COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" IS NOT NULL THEN "entities_entity"."id" ELSE NULL END)
            AS "total_entities"
        FROM "entities_entity"
        INNER JOIN entities_{entity_name}_entity ews ON "entities_entity"."id" = ews."entity_id"
        {entity_weekly_join}
        WHERE "entities_entity"."deleted" IS NULL
        -- AND entities_entity.entity_type_id = (SELECT id FROM entities_entity_type WHERE code = '{entity_name}' AND
        -- deleted IS NULL)
        {country_condition}
        {admin1_condition}
        {entity_condition}
        {entity_weekly_condition}
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['entity_condition'] = ''
        kwargs['entity_weekly_join'] = ''
        kwargs['entity_weekly_condition'] = ''

        if len(kwargs.get('admin1_ids', [])) > 0:
            kwargs['admin1_condition'] = ' AND "entities_entity"."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in kwargs['admin1_ids']])
            )
        elif len(kwargs.get('country_ids', [])) > 0:
            kwargs['country_condition'] = ' AND "entities_entity"."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )

        if len(kwargs['entity_filters']) > 0:
            kwargs['entity_condition'] = ' AND ' + kwargs['entity_filters']

        if len(kwargs['entity_static_filters']) > 0:
            kwargs['entity_weekly_condition'] = ' AND ' + kwargs['entity_static_filters']

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        entity_type_obj = get_entity_type_config(kwargs['entity_name'])
        try:
            kwargs['parameter_col'] = build_parameter_config(
                entity_type_obj,
                kwargs['col_name'],
                kwargs['entity_name']
            )
        except Exception as e:
            raise ValueError(
                f"Column '{kwargs['col_name']}' not found in any model for entity '{kwargs['entity_name']}'. "
                f"Check the DataLayer's data source column configuration."
            ) from e
        parameter_col_type = kwargs['parameter_col'].get('type', 'str').lower()
        kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'entities_entity')
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
                        'COUNT(DISTINCT CASE WHEN {sql} THEN entities_entity."id" ELSE NULL END) AS "{label}",'.format(
                            sql=sql_statement,
                            label=label,
                        ))
                else:
                    values_l.extend(values)
                    if parameter_col_type == 'str':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN LOWER({table_name}."{col_name}") IN ({value}) '
                            'THEN entities_entity."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" IN ({value}) '
                            'THEN entities_entity."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join([str(v) for v in values])
                            ))
            else:
                if is_sql_value:
                    label_cases.append(
                        'COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" IS NULL THEN entities_entity."id" ELSE NULL END) '
                        'AS "{label}",'.format(
                            table_name=kwargs['table_name'],
                            col_name=kwargs['col_name'],
                            label=label,
                        ))
                else:
                    values = set(values_l)
                    if not values:
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" IS NOT NULL '
                            'THEN entities_entity."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                            ))
                    elif parameter_col_type == 'str':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN LOWER({table_name}."{col_name}") NOT IN ({value}) '
                            'THEN entities_entity."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" NOT IN ({value}) '
                            'THEN entities_entity."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join([str(v) for v in values])
                            ))

        kwargs['label_case_statements'] = ' '.join(label_cases)
        return query.format(**kwargs)

    def get_static_entity_view_info_query(self):
        query = """
        SELECT entities_entity."id",
            entities_entity."name",
            entities_entity."external_id",
            entities_entity."giga_id",
            entities_entity."country_id",
            c."name" AS country_name,
            entities_entity."admin1_id",
            adm1_metadata."name" AS admin1_name,
            adm1_metadata."giga_id_admin" AS admin1_code,
            adm1_metadata."description_ui_label" AS admin1_description_ui_label,
            entities_entity."admin2_id",
            adm2_metadata."name" AS admin2_name,
            adm2_metadata."giga_id_admin" AS admin2_code,
            adm2_metadata."description_ui_label" AS admin2_description_ui_label,
            entities_entity."environment",
            {table_name}."{col_name}" AS field_value,
            {label_case_statements}
            ST_AsGeoJSON(ST_Transform(entities_entity."geopoint", 4326)) AS geopoint,
            CASE WHEN entities_entity.connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN entities_entity.connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END as connectivity_status
        FROM "entities_entity"
        INNER JOIN locations_country c ON c.id = entities_entity.country_id
        INNER JOIN entities_{entity_name}_entity ews ON entities_entity.id = ews.entity_id
        LEFT JOIN locations_countryadminmetadata AS adm1_metadata
            ON adm1_metadata."id" = entities_entity.admin1_id
            AND adm1_metadata."layer_name" = 'adm1'
            AND adm1_metadata."deleted" IS NULL
        LEFT JOIN locations_countryadminmetadata AS adm2_metadata
            ON adm2_metadata."id" = entities_entity.admin2_id
            AND adm2_metadata."layer_name" = 'adm2'
            AND adm2_metadata."deleted" IS NULL
        WHERE "entities_entity"."id" IN ({ids})
            AND c."deleted" IS NULL
        """

        kwargs = copy.deepcopy(self.kwargs)
        kwargs['ids'] = ','.join(kwargs['entity_ids'])

        legend_configs = kwargs['legend_configs']
        label_cases = []
        values_l = []
        entity_type_obj = get_entity_type_config(kwargs['entity_name'])
        kwargs['parameter_col'] = build_parameter_config(
            entity_type_obj,
            kwargs['col_name'],
            kwargs['entity_name']
        )
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

    @staticmethod
    def extract_entity_params(request):
        """
        Extract entity-specific parameters from query params.
        Looks for patterns like: {entity_code}_{param_name}
        e.g., school_layer_id, health_start_date, school_benchmark

        Returns dict: {entity_code: {param_name: value, ...}}
        """
        entity_params = {}
        param_suffixes = ['layer_id', 'start_date', 'end_date', 'is_weekly', 'benchmark',
                          'include_same_location', 'country_id', 'admin1_id']

        for param_name, param_value in request.query_params.items():
            if param_value:
                for suffix in param_suffixes:
                    if param_name.endswith(f'_{suffix}'):
                        entity_code = param_name.replace(f'_{suffix}', '')
                        if entity_code not in entity_params:
                            entity_params[entity_code] = {}
                        entity_params[entity_code][suffix] = param_value
                        break

        return entity_params

    def _process_entity_layer(self, request, entity_code, params):
        """
        Process a single entity layer and return its info data.
        """
        layer_id = params.get('layer_id')
        if not layer_id:
            return None

        try:
            data_layer_instance = accounts_models.DataLayer.objects.get(
                pk=layer_id,
                status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
            )
        except accounts_models.DataLayer.DoesNotExist:
            return {'error': f'No published DataLayer found with id={layer_id}.'}

        data_sources = data_layer_instance.data_sources.all()
        live_data_sources = ['UNKNOWN']

        for d in data_sources:
            source_type = d.data_source.data_source_type
            if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                live_data_sources.append(statistics_configs.QOS_SOURCE)
            elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

        country_ids = data_layer_instance.applicable_countries
        first_source = data_sources.first()
        if first_source is None:
            return {'error': f'DataLayer (id={layer_id}) has no data sources configured.'}
        parameter_col = first_source.data_source_column or {}
        if not parameter_col.get('name'):
            return {'error': f'DataLayer (id={layer_id}) has no data source column name configured.'}

        parameter_column_name = str(parameter_col['name'])
        parameter_column_unit = str(parameter_col.get('unit', '')).lower()
        base_benchmark = str(parameter_col.get('base_benchmark', 1))
        display_unit = parameter_col.get('display_unit', '')

        # Build entity-specific query params dict (don't modify request)
        entity_query_params = request.query_params.copy()

        # Override with entity-specific params if provided
        if 'start_date' in params:
            entity_query_params['start_date'] = params['start_date']
        if 'end_date' in params:
            entity_query_params['end_date'] = params['end_date']
        if 'is_weekly' in params:
            entity_query_params['is_weekly'] = params['is_weekly']
        if 'benchmark' in params:
            entity_query_params['benchmark'] = params['benchmark']
        if 'include_same_location' in params:
            entity_query_params['include_same_location'] = params['include_same_location']

        # Handle country_id from base or entity-specific
        country_id = params.get('country_id', entity_query_params.get('country_id'))
        if country_id:
            entity_query_params['country_id'] = country_id

        # Handle admin1_id from base or entity-specific
        admin1_id = params.get('admin1_id', entity_query_params.get('admin1_id'))
        if admin1_id:
            entity_query_params['admin1_id'] = admin1_id

        try:
            self.update_kwargs_from_dict(country_ids, data_layer_instance, entity_query_params)
        except ValueError as e:
            return {'error': str(e)}

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

        response_data = None

        is_legacy = (data_layer_instance.entity_type is None) or data_layer_instance.entity_type.is_legacy

        if data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            parameter_col_function = first_source.data_source_column_function or {}
            column_function_sql = self.get_column_function_sql(parameter_col_function)

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
                'entity_name': data_layer_instance.entity_name,
                'school_filters': self.kwargs.get('entity_filters', ''),
                'school_static_filters': self.kwargs.get('entity_static_filters', ''),
                'school_ids': self.kwargs.get('entity_ids', []),
            })

            if len(self.kwargs.get('entity_ids', [])) > 0 and not is_legacy:
                info_panel_entity_list = db_utilities.sql_to_response(self.get_entity_view_info_query(),
                                                                      label=self.__class__.__name__,
                                                                      db_var=settings.READ_ONLY_DB_KEY) or []
                statistics = db_utilities.sql_to_response(
                    self.get_entity_view_statistics_info_query(data_layer_instance.type),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY) or []
                graph_data, positive_speeds = self.generate_graph_data()
                sorted_info_panel_entity_list = []

                if len(info_panel_entity_list) > 0:
                    for entity_id in self.kwargs.get('entity_ids', []):
                        for entity_details in info_panel_entity_list:
                            if str(entity_details['id']) == str(entity_id):
                                sorted_info_panel_entity_list.append(entity_details)

                    for info_panel_entity in sorted_info_panel_entity_list:
                        info_panel_entity['geopoint'] = json.loads(info_panel_entity['geopoint'])
                        matched = list(filter(
                            lambda s: s['entity_id'] == info_panel_entity['id'], statistics))
                        info_panel_entity['statistics'] = matched[-1] if matched else {}

                        _speeds = positive_speeds.get(str(info_panel_entity['id']), [])
                        live_avg = round(sum(_speeds) / len(_speeds), 2) if _speeds else 0

                        info_panel_entity['live_avg'] = live_avg
                        info_panel_entity['graph_data'] = graph_data.get(str(info_panel_entity['id']), [])

                        benchmark_value_from_sql = info_panel_entity.get('benchmark_sql_value', None)
                        if benchmark_value_from_sql:
                            rounded_benchmark_value_int = round(
                                eval(unit_agg_str.format(
                                    val=core_utilities.convert_to_int(benchmark_value_from_sql))), 2)
                            benchmark_value = str(benchmark_value_from_sql)
                        else:
                            rounded_benchmark_value_int = round(
                                eval(unit_agg_str.format(val=core_utilities.convert_to_int(benchmark_value))), 2)

                        info_panel_entity['benchmark_metadata'] = {
                            'benchmark_value': benchmark_value,
                            'rounded_benchmark_value': rounded_benchmark_value_int,
                            'benchmark_unit': benchmark_unit,
                            'base_benchmark': base_benchmark,
                            'parameter_column_unit': parameter_column_unit,
                            'round_unit_value': unit_agg_str,
                            'convert_unit': self.kwargs.get('convert_unit'),
                            'display_unit': display_unit,
                        }

                response_data = sorted_info_panel_entity_list
            elif is_legacy:
                from proco.connection_statistics.models import SchoolWeeklyStatus
                from proco.accounts.api import DataLayerInfoViewSet

                is_data_synced_qs = SchoolWeeklyStatus.objects.filter(
                    school__realtime_registration_status__rt_registered=True,
                )
                if len(self.kwargs.get('school_filters', '')) > 0:
                    is_data_synced_qs = is_data_synced_qs.extra(where=[self.kwargs['school_filters']])
                if len(self.kwargs.get('school_static_filters', '')) > 0:
                    is_data_synced_qs = is_data_synced_qs.extra(where=[self.kwargs['school_static_filters']])
                if len(self.kwargs.get('admin1_ids', [])) > 0:
                    is_data_synced_qs = is_data_synced_qs.filter(school__admin1_id__in=self.kwargs['admin1_ids'])
                elif len(self.kwargs.get('country_ids', [])) > 0:
                    is_data_synced_qs = is_data_synced_qs.filter(school__country_id__in=self.kwargs['country_ids'])

                query_result = db_utilities.sql_to_response(
                    DataLayerInfoViewSet.get_info_query(self),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY)
                if not query_result:
                    return {
                        'error': f'Failed to fetch data for {entity_code} layer. The configured column may not exist.'
                    }
                query_response = query_result[-1]

                graph_data, positive_speeds = self._generate_school_graph_data(
                    DataLayerInfoViewSet
                )
                live_avg = self.get_live_avg(
                    parameter_col_function.get('name', 'avg'), positive_speeds
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

                response_data = {
                    'no_of_entities_measure': query_response.get('no_of_schools_measure', 0),
                    'entity_with_realtime_data': query_response.get('school_with_realtime_data', 0),
                    'real_time_connected_entities': {
                        'good': query_response.get('good', 0),
                        'moderate': query_response.get('moderate', 0),
                        'no_internet': query_response.get('bad', 0),
                        'unknown': query_response.get('unknown', 0),
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
                is_data_synced_qs = EntityWeeklyStatus.objects.filter(
                    entity__realtime_registration_status__rt_registered=True,
                )

                if len(self.kwargs['entity_filters']) > 0:
                    is_data_synced_qs = is_data_synced_qs.extra(where=[self.kwargs['entity_filters']])

                if len(self.kwargs['entity_static_filters']) > 0:
                    is_data_synced_qs = is_data_synced_qs.extra(where=[self.kwargs['entity_static_filters']])

                if len(self.kwargs.get('admin1_ids', [])) > 0:
                    is_data_synced_qs = is_data_synced_qs.filter(entity__admin1_id__in=self.kwargs['admin1_ids'])
                elif len(self.kwargs.get('country_ids', [])) > 0:
                    is_data_synced_qs = is_data_synced_qs.filter(entity__country_id__in=self.kwargs['country_ids'])

                query_labels = []
                query_result = db_utilities.sql_to_response(self.get_info_query(query_labels),
                                                            label=self.__class__.__name__,
                                                            db_var=settings.READ_ONLY_DB_KEY)
                if not query_result:
                    return {
                        'error': f'Failed to fetch data for {entity_code} layer. The configured column may not exist.'
                    }
                query_response = query_result[-1]

                graph_data, positive_speeds = self.generate_graph_data()
                live_avg = round(sum(positive_speeds) / len(positive_speeds), 2) if len(positive_speeds) > 0 else 0

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

                if query_labels:
                    connected_entities = {label: query_response.get(label, 0) for label in query_labels}
                else:
                    connected_entities = {
                        'good': query_response.get('good', 0),
                        'moderate': query_response.get('moderate', 0),
                        'no_internet': query_response.get('bad', 0),
                        'unknown': query_response.get('unknown', 0),
                    }

                response_data = {
                    'no_of_entities_measure': query_response.get('no_of_entities_measure', 0),
                    'entity_with_realtime_data': query_response.get('entity_with_realtime_data', 0),
                    'real_time_connected_entities': connected_entities,
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
                'entity_name': data_layer_instance.entity_name,
            })

            if len(self.kwargs.get('entity_ids', [])) > 0:
                info_panel_entity_list = db_utilities.sql_to_response(self.get_static_entity_view_info_query(),
                                                                      label=self.__class__.__name__,
                                                                      db_var=settings.READ_ONLY_DB_KEY) or []

                statistics = db_utilities.sql_to_response(
                    self.get_entity_view_statistics_info_query(data_layer_instance.type),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY) or []

                sorted_info_panel_entity_list = []
                if len(info_panel_entity_list) > 0:
                    for entity_id in self.kwargs.get('entity_ids', []):
                        for entity_details in info_panel_entity_list:
                            if str(entity_details['id']) == str(entity_id):
                                sorted_info_panel_entity_list.append(entity_details)

                    for info_panel_entity in sorted_info_panel_entity_list:
                        info_panel_entity['geopoint'] = json.loads(info_panel_entity['geopoint'])
                        matched = list(filter(
                            lambda s: s['entity_id'] == info_panel_entity['id'], statistics))
                        info_panel_entity['statistics'] = matched[-1] if matched else {}

                response_data = sorted_info_panel_entity_list
            else:
                query_labels = []
                query_result = db_utilities.sql_to_response(self.get_static_info_query(query_labels),
                                                            label=self.__class__.__name__,
                                                            db_var=settings.READ_ONLY_DB_KEY)
                if not query_result:
                    return {
                        'error': f'Failed to fetch static data for {entity_code} layer. The configured column may not exist.'
                    }
                query_response = query_result[-1]
                response_data = {
                    'total_entities': query_response['total_entities'],
                    'connected_entities': {label: query_response[label] for label in query_labels},
                    'legend_configs': legend_configs,
                    'benchmark_metadata': {
                        'parameter_column_unit': parameter_column_unit,
                        'display_unit': display_unit,
                    },
                }

        return response_data

    def get(self, request, *args, **kwargs):
        """
        Get layer info for multiple entity types.

        API endpoint:
            /api/v2/entities/layers/info/

        Query Parameters (all prefixed with entity_code):
            school_layer_id=30&health_layer_id=40
            school_start_date=11-05-2026&health_start_date=11-05-2026
            school_end_date=17-05-2026&health_end_date=17-05-2026
            school_is_weekly=true&health_is_weekly=true
            school_benchmark=global&health_benchmark=global
            school_include_same_location=false&health_include_same_location=false
            country_id=144

        Returns:
            {"school": {...}, "health": {...}}
        """
        # Check for legacy entity_type__code (backward compatibility)
        entity_type = request.query_params.get('entity_type__code')
        if entity_type == LEGACY_MODEL:
            school_view = DataLayerInfoViewSet.as_view()
            return school_view(request._request, *args, **kwargs)

        use_cached_data = request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key()

        response = None
        if use_cached_data:
            response = cache_manager.get(cache_key)

        if not response:
            # Initialize kwargs for this request
            self.kwargs = {}

            # Extract all entity-specific parameters from query params
            entity_params = self.extract_entity_params(request)

            # If no entity-specific params found, return error
            if not entity_params:
                return Response(
                    {'error': 'No entity parameters found. Please provide at least one entity_layer_id parameter.'},
                    status=400
                )

            response = {}

            # Process each entity type
            for entity_code, params in entity_params.items():
                if 'layer_id' in params:
                    try:
                        entity_response = self._process_entity_layer(request, entity_code, params)
                    except Exception as exc:
                        entity_response = {'error': str(exc)}
                    if entity_response is not None:
                        response[entity_code] = entity_response

            cache_manager.set(cache_key, response, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=response)


class EntityDataLayersViewSet(BaseModelViewSet):
    model = accounts_models.DataLayer
    serializer_class = entity_serializers.EntityDataLayersListSerializer

    action_serializers = {
        'create': entity_serializers.CreateEntityDataLayersSerializer,
        'partial_update': entity_serializers.UpdateEntityDataLayerSerializer,
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
        'entity_type__code': ['iexact', 'in', 'exact'],
    }

    search_fields = ('name', 'code', 'type', 'entity_type__code',)

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


class EntityDataLayerPublishViewSet(BaseModelViewSet):
    model = accounts_models.DataLayer
    serializer_class = entity_serializers.PublishEntityDataLayerSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPublishDataLayer,
    )


class EntityDataLayerPreviewViewSet(APIView):
    model = accounts_models.DataLayer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanPreviewDataLayer,
    )

    def get_map_query(self, kwargs):
        query = """
        SELECT entities_entity.id,
            CASE WHEN rt_status.rt_registered = True AND rt_status.rt_registration_date <= '{end_date}' THEN True
                    ELSE False
            END AS is_rt_connected,
            {case_conditions}
            CASE WHEN entities_entity.connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN entities_entity.connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END AS connectivity_status,
            ST_AsGeoJSON(ST_Transform(entities_entity.geopoint, 4326)) AS geopoint
        FROM entities_entity
        INNER JOIN connection_statistics_entityweeklystatus sws ON entities_entity.last_weekly_status_id = sws.id
        INNER JOIN connection_statistics_entityrealtimeregistration rt_status ON rt_status.entity_id =
        entities_entity.id
        LEFT JOIN (
            SELECT "entities_entity"."id" AS entity_id,
                AVG(t."{col_name}") AS "{col_name}"
            FROM "entities_entity"
            INNER JOIN "connection_statistics_entitydailystatus" t ON "entities_entity"."id" = t."entity_id"
            WHERE (
                {country_condition}
                "entities_entity"."deleted" IS NULL
                AND t."deleted" IS NULL
                AND (t."date" BETWEEN '{start_date}' AND '{end_date}')
                AND t."live_data_source" IN ({live_source_types})
            )
            GROUP BY "entities_entity"."id"
            ORDER BY "entities_entity"."id" ASC
        ) AS sds ON sds.entity_id = entities_entity.id
        WHERE entities_entity."deleted" IS NULL
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
                            WHEN sds.{col_name} <= {benchmark_value} and sds.{col_name} >= {base_benchmark} THEN 'moderate'
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
            kwargs['country_condition'] = '"entities_entity"."country_id" IN ({0}) AND'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )
            kwargs['country_condition_outer'] = 'AND entities_entity."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in kwargs['country_ids']])
            )
        else:
            kwargs['country_condition'] = ''
            kwargs['country_condition_outer'] = ''

        return query.format(**kwargs)

    def get_static_map_query(self, kwargs):
        query = """
            SELECT
                entities_entity.id,
                entities_entity.name,
                {table_name}."{col_name}",
                ST_AsGeoJSON(ST_Transform(entities_entity.geopoint, 4326)) as geopoint,
                {label_case_statements}
            FROM entities_entity
            INNER JOIN connection_statistics_schoolweeklystatus sws ON schools_school.last_weekly_status_id = sws.id
            WHERE entities_entity."deleted" IS NULL {country_condition}
            ORDER BY random()
            LIMIT 1000
            """

        kwargs['country_condition'] = ''

        if len(kwargs['country_ids']) > 0:
            kwargs['country_condition'] = 'AND entities_entity.country_id IN ({0})'.format(
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


class PublishedEntityDataLayersViewSet(CachedListMixin, BaseModelViewSet):
    """
    PublishedEntityDataLayersViewSet
    Cache Attr:
        Auto Cache: Not required
        Call Cache: Yes
    """
    LIST_CACHE_KEY_PREFIX = 'PUBLISHED_LAYERS_LIST_ENTITIES'

    model = accounts_models.DataLayer
    serializer_class = entity_serializers.EntityDataLayersListSerializer

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
        'entity_type__code': ['iexact', 'in', 'exact'],
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


class EntityDataLayerMetadataViewSet(BaseModelViewSet):
    model = accounts_models.DataLayer

    serializer_class = entity_serializers.EntityDataLayersListSerializer

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
