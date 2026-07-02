import copy
import json
import logging
from datetime import timedelta

from django.conf import settings
from django.core.exceptions import FieldDoesNotExist
from django.db.models import Q
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import permissions
from rest_framework import status as rest_status
from rest_framework.filters import SearchFilter
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param
from rest_framework.views import APIView

from proco.accounts import models as accounts_models
from proco.accounts import utils as account_utilities
from proco.accounts.api import DataLayerInfoViewSet, DataLayerMapViewSet
from proco.accounts.v2 import entity_serializers
from proco.connection_statistics import models as statistics_models
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import SchoolWeeklyStatus, EntityWeeklyStatus
from proco.core import db_utils as db_utilities
from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.entities.config import build_parameter_config, get_entity_type_config
from proco.entities.constants import LEGACY_MODEL
from proco.entities.mixins import EntityDetailFilterMixin, EntityTypeCodeMixin
from proco.locations.models import Country
from proco.utils import dates as date_utilities
from proco.utils.cache import cache_manager, custom_cache_control
from proco.utils.filters import NullsAlwaysLastOrderingFilter
from proco.utils.mixins import CachedListMixin

logger = logging.getLogger('gigamaps.' + __name__)


class BaseEntityDataLayerAPIViewSet(EntityDetailFilterMixin, APIView):
    model = accounts_models.DataLayer
    ENTITY_PARAM_SUFFIXES = (
        'layer_id',
        'start_date',
        'end_date',
        'is_weekly',
        'benchmark',
        'include_same_location',
        'country_id',
        'admin1_id',
    )

    permission_classes = (
        permissions.AllowAny,
    )

    def get_column_function_sql(self, parameter_col_function):
        if isinstance(parameter_col_function, dict) and len(parameter_col_function) > 0:
            return parameter_col_function.get('sql').format(col_name='t."{col_name}"')
        return 'AVG(t."{col_name}")'

    def get_entity_detail_filter_sql(self, entity_type_obj, base_table_ref='"entities_entity"'):
        if entity_type_obj is None or entity_type_obj.is_legacy:
            return '', ''

        detail_table_name = self.get_entity_detail_table_name(entity_type_obj)
        if not detail_table_name:
            return '', ''

        conditions = []
        for table_alias in self.get_entity_detail_filter_aliases(entity_type_obj):
            detail_filters = core_utilities.get_filter_sql(
                self.request,
                table_alias,
                detail_table_name,
                entity_type_obj.code,
            )
            if len(detail_filters) > 0:
                conditions.append(detail_filters)

        if not conditions:
            return '', ''

        join_sql = """
                INNER JOIN "{detail_table_name}"
                    ON {base_table_ref}."id" = "{detail_table_name}"."entity_id"
                    AND "{detail_table_name}"."deleted" IS NULL
        """.format(
            base_table_ref=base_table_ref,
            detail_table_name=detail_table_name,
        )
        return join_sql, ' AND ' + ' AND '.join(conditions)

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
        self.kwargs['layer_type'] = layer_instance.type

        self.kwargs['school_filters'] = core_utilities.get_filter_sql(
            self.request, 'schools', 'schools_school', LEGACY_MODEL)
        self.kwargs['school_static_filters'] = core_utilities.get_filter_sql(
            self.request, 'school_static', 'connection_statistics_schoolweeklystatus', LEGACY_MODEL)

        if layer_instance.entity_type is not None and not layer_instance.entity_type.is_legacy:
            entity_type = layer_instance.entity_type.code
            entity_static_table = f"entities_{entity_type}_entity"

            self.kwargs['entity_filters'] = core_utilities.get_filter_sql(
                self.request, 'entities', 'entities_entity', entity_type)
            self.kwargs['entity_static_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity_static', entity_static_table, entity_type)
            self.kwargs['entity_real_time_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity_real_time', 'connection_statistics_entityweeklystatus', entity_type)
            (
                self.kwargs['entity_detail_join'],
                self.kwargs['entity_detail_condition'],
            ) = self.get_entity_detail_filter_sql(layer_instance.entity_type)
        else:
            self.kwargs['entity_filters'] = ''
            self.kwargs['entity_static_filters'] = ''
            self.kwargs['entity_real_time_filters'] = ''
            self.kwargs['entity_detail_join'] = ''
            self.kwargs['entity_detail_condition'] = ''

    @staticmethod
    def parse_date(value, param_name):
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
        self.kwargs.setdefault('entity_detail_join', '')
        self.kwargs.setdefault('entity_detail_condition', '')
        self.kwargs.setdefault('school_filters', '')
        self.kwargs.setdefault('school_static_filters', '')
        self.kwargs.setdefault('convert_unit', 'mbps')
        self.kwargs.setdefault('is_weekly', True)
        self.kwargs.setdefault('benchmark', 'global')

        query_param_keys = query_params.keys()

        if 'start_date' in query_param_keys:
            self.kwargs['start_date'] = self.parse_date(query_params['start_date'], 'start_date')
        elif layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            date = core_utilities.get_current_datetime_object() - timedelta(days=7)
            self.kwargs['start_date'] = (date - timedelta(days=date.weekday())).date()

        if 'end_date' in query_param_keys:
            self.kwargs['end_date'] = self.parse_date(query_params['end_date'], 'end_date')
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
        self.kwargs['layer_type'] = layer_instance.type

        self.kwargs['school_filters'] = core_utilities.get_filter_sql(
            self.request, 'schools', 'schools_school', LEGACY_MODEL)
        self.kwargs['school_static_filters'] = core_utilities.get_filter_sql(
            self.request, 'school_static', 'connection_statistics_schoolweeklystatus', LEGACY_MODEL)

        if layer_instance.entity_type is not None and not layer_instance.entity_type.is_legacy:
            entity_type = layer_instance.entity_type.code
            entity_static_table = f"entities_{entity_type}_entity"

            self.kwargs['entity_filters'] = core_utilities.get_filter_sql(
                self.request, 'entities', 'entities_entity', entity_type)
            self.kwargs['entity_static_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity_static', entity_static_table, entity_type)
            self.kwargs['entity_real_time_filters'] = core_utilities.get_filter_sql(
                self.request, 'entity_real_time', 'connection_statistics_entityweeklystatus', entity_type)
            (
                self.kwargs['entity_detail_join'],
                self.kwargs['entity_detail_condition'],
            ) = self.get_entity_detail_filter_sql(layer_instance.entity_type)

    @classmethod
    def extract_entity_params(cls, request):
        """
        Extract entity-specific parameters from query params.
        Looks for patterns like: {entity_code}_{param_name}
        e.g., school_layer_id, health_start_date, school_benchmark

        Returns dict: {entity_code: {param_name: value, ...}}
        """
        entity_params = {}
        for param_name, param_value in request.query_params.items():
            if not param_value:
                continue

            for suffix in cls.ENTITY_PARAM_SUFFIXES:
                entity_suffix = f'_{suffix}'
                if param_name.endswith(entity_suffix):
                    entity_code = param_name[:-len(entity_suffix)]
                    entity_params.setdefault(entity_code, {})[suffix] = param_value
                    break

        return entity_params

    @staticmethod
    def set_query_param(query_params, key, value):
        if value in (None, ''):
            query_params.pop(key, None)
            return
        query_params[key] = value

    def build_scoped_query_params(self, request, entity_code, params, is_legacy):
        query_params = request.query_params.copy()

        for key in self.ENTITY_PARAM_SUFFIXES:
            if key in params:
                query_params[key] = params[key]

        if is_legacy:
            scoped_single = (
                request.query_params.get(f'{entity_code}_school_id')
                or request.query_params.get(f'{entity_code}_entity_id')
                or request.query_params.get('school_id')
            )
            scoped_multi = (
                request.query_params.get(f'{entity_code}_school_id__in')
                or request.query_params.get(f'{entity_code}_entity_id__in')
                or request.query_params.get('school_id__in')
            )
            self.set_query_param(query_params, 'school_id', scoped_single)
            self.set_query_param(query_params, 'school_id__in', scoped_multi)
            self.set_query_param(
                query_params,
                'include_same_location_schools',
                query_params.get('include_same_location'),
            )
        else:
            scoped_single = (
                request.query_params.get(f'{entity_code}_entity_id')
                or request.query_params.get('entity_id')
            )
            scoped_multi = (
                request.query_params.get(f'{entity_code}_entity_id__in')
                or request.query_params.get('entity_id__in')
            )
            self.set_query_param(query_params, 'entity_id', scoped_single)
            self.set_query_param(query_params, 'entity_id__in', scoped_multi)

        return query_params

    def get_benchmark_value(self, data_layer_instance):
        benchmark_val = data_layer_instance.global_benchmark.get('value')
        benchmark_unit = data_layer_instance.global_benchmark.get('unit')

        if self.kwargs['benchmark'] == 'national':
            country_ids = self.kwargs.get('country_ids', [])
            if len(country_ids) > 0:
                country_key = tuple(country_ids)
                if not hasattr(self, '_benchmark_metadata_cache'):
                    self._benchmark_metadata_cache = {}

                if country_key not in self._benchmark_metadata_cache:
                    self._benchmark_metadata_cache[country_key] = Country.objects.all().filter(
                        id__in=country_ids,
                        benchmark_metadata__isnull=False,
                    ).order_by('id').values_list('benchmark_metadata', flat=True).first()

                benchmark_metadata = self._benchmark_metadata_cache[country_key]

                if benchmark_metadata and len(benchmark_metadata) > 0:
                    if isinstance(benchmark_metadata, str):
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
                cache_key = (tuple(country_ids), data_layer_instance.id)
                if not hasattr(self, '_legend_configs_cache'):
                    self._legend_configs_cache = {}

                if cache_key not in self._legend_configs_cache:
                    self._legend_configs_cache[cache_key] = Country.objects.all().filter(
                        id__in=country_ids,
                        active_layers__deleted__isnull=True,
                        active_layers__data_layer_id=data_layer_instance.id,
                    ).order_by('id').values_list('active_layers__legend_configs', flat=True).first()

                legend_configurations = self._legend_configs_cache[cache_key]
                if legend_configurations and len(legend_configurations) > 0:
                    if isinstance(legend_configurations, str):
                        legend_configs = json.loads(legend_configurations)
                    else:
                        legend_configs = legend_configurations

        # Clean legacy legend configurations that hardcoded "sws." instead of "{table_name}."
        if isinstance(legend_configs, dict):
            for title, config_dict in legend_configs.items():
                if isinstance(config_dict, dict) and 'values' in config_dict:
                    config_dict['values'] = [
                        val.replace('sws.', '{table_name}.').replace('sws"', '{table_name}"')
                        if isinstance(val, str) else val
                        for val in config_dict['values']
                    ]

        return legend_configs

    @staticmethod
    def build_case_expression(label_cases, alias, trailing_comma=False, default_label='unknown'):
        when_cases = []
        fallback_label = None

        for label_case in label_cases:
            label_case = label_case.strip()
            if label_case.upper().startswith('ELSE '):
                if fallback_label is None:
                    fallback_label = label_case[5:].strip()
            elif label_case:
                when_cases.append(label_case)

        if fallback_label is None:
            fallback_label = "'{0}'".format(str(default_label).replace("'", "''"))

        if when_cases:
            expression = 'CASE {when_cases} ELSE {fallback_label} END AS {alias}'.format(
                when_cases=' '.join(when_cases),
                fallback_label=fallback_label,
                alias=alias,
            )
        else:
            expression = '{fallback_label} AS {alias}'.format(
                fallback_label=fallback_label,
                alias=alias,
            )

        if trailing_comma:
            expression += ','
        return expression


@method_decorator([
    custom_cache_control(
        public=True,
        max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE,
        cache_status_codes=[rest_status.HTTP_200_OK, ],
    )
], name='dispatch')
class EntityDataLayerMapViewSet(EntityTypeCodeMixin, BaseEntityDataLayerAPIViewSet, account_utilities.BaseTileGenerator):
    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'ENTITY_DATA_LAYER_MAP'
    LAYER_ID_QUERY_PARAM = 'layer_id'

    def get_cache_key(self):
        layer_id = self.get_layer_ids_cache_part()
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}_{2}'.format(
            self.CACHE_KEY_PREFIX,
            layer_id,
            '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))),
        )

    def get_layer_ids_cache_part(self):
        layer_entity_params = self.get_prefixed_layer_params()
        if layer_entity_params:
            return '_'.join([
                f'{entity_code}_{params[self.LAYER_ID_QUERY_PARAM]}'
                for entity_code, params in sorted(layer_entity_params.items())
            ])
        return self.get_layer_id()

    def get_layer_id(self):
        _, params = self.get_entity_layer_params()
        if params:
            return params.get(self.LAYER_ID_QUERY_PARAM)
        return None

    def get_prefixed_layer_params(self):
        requested_entity_type_codes = self.get_entity_type_code_params()
        layer_entity_params = {
            entity_code: params
            for entity_code, params in self.extract_entity_params(self.request).items()
            if params.get(self.LAYER_ID_QUERY_PARAM)
        }
        if requested_entity_type_codes is None:
            return layer_entity_params
        return {
            entity_code: params
            for entity_code, params in layer_entity_params.items()
            if entity_code in requested_entity_type_codes
        }

    def get_entity_layer_params(self):
        entity_params = self.extract_entity_params(self.request)
        requested_entity_type_codes = self.get_entity_type_code_params()

        if requested_entity_type_codes:
            if len(requested_entity_type_codes) > 1:
                return None, None

            entity_code = requested_entity_type_codes[0]
            params = entity_params.get(entity_code)
            if params and params.get(self.LAYER_ID_QUERY_PARAM):
                return entity_code, params

        layer_entity_params = {
            entity_code: params
            for entity_code, params in entity_params.items()
            if params.get(self.LAYER_ID_QUERY_PARAM)
        }
        if len(layer_entity_params) == 1:
            entity_code, params = next(iter(layer_entity_params.items()))
            return entity_code, params
        if len(layer_entity_params) > 1:
            return None, None

        layer_id = self.request.query_params.get(self.LAYER_ID_QUERY_PARAM)
        if layer_id:
            return None, {self.LAYER_ID_QUERY_PARAM: layer_id}

        return None, None

    def envelope_to_sql(self, env, request):
        if getattr(self, 'layer_sql_contexts', None):
            sql_parts = []
            original_kwargs = self.kwargs
            try:
                for layer_context in self.layer_sql_contexts:
                    self.kwargs = copy.deepcopy(layer_context['kwargs'])
                    if layer_context['is_legacy']:
                        school_map_view = DataLayerMapViewSet()
                        school_map_view.kwargs = self.kwargs
                        if self.kwargs['layer_type'] == accounts_models.DataLayer.LAYER_TYPE_LIVE:
                            sql = school_map_view.get_live_map_query(env, request)
                        else:
                            sql = school_map_view.get_static_map_query(env, request)
                    elif self.kwargs['layer_type'] == accounts_models.DataLayer.LAYER_TYPE_LIVE:
                        sql = self.get_live_entity_map_query(env, request)
                    else:
                        sql = self.get_static_entity_map_query(env, request)

                    sql_parts.append(sql.rstrip(';\n '))
            finally:
                self.kwargs = original_kwargs

            return "SELECT {0};".format(
                " || ".join([
                    "COALESCE(({0}), ''::bytea)".format(sql)
                    for sql in sql_parts
                ])
            )

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
                    '{entity_name}' AS entity_type,
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
                    INNER JOIN bounds ON ST_Intersects("entities_entity".geopoint, ST_Transform(bounds.geom, 4326))
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
                {entity_detail_join}
                WHERE "entities_entity"."deleted" IS NULL
                    {entity_detail_condition}
                    {random_order}
                    {limit_condition}
            )
            SELECT COALESCE(NULLIF(tile.mvt, ''::bytea), {empty_mvt_layer})
            FROM (
                SELECT ST_AsMVT(DISTINCT mvtgeom.*, '{mvt_layer}') AS mvt FROM mvtgeom
            ) tile;
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['mvt_layer'] = kwargs.get('mvt_layer', 'default')
        kwargs['empty_mvt_layer'] = account_utilities.get_empty_mvt_layer_sql(kwargs['mvt_layer'])
        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['entity_condition'] = ''

        kwargs['entity_weekly_join'] = ''
        kwargs['entity_weekly_condition'] = ''
        kwargs['entity_weekly_outer_join'] = ''
        kwargs['entity_master_table_join'] = ''
        kwargs['entity_master_table_condition'] = ''
        kwargs.setdefault('entity_detail_join', '')
        kwargs.setdefault('entity_detail_condition', '')

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
        if kwargs.get('layer_type') == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            kwargs['table_name'] = 'eds'
        else:
            kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'entities_entity')

        if len(legend_configs) > 0 and 'SQL:' in str(legend_configs):
            label_cases = []
            for title, values_and_label in legend_configs.items():
                values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                     values_and_label.get('values', [])))

                if len(values) > 0:
                    is_sql_value = 'SQL:' in values[0]
                    if is_sql_value:
                        sql_statement = str(' AND '.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    label_cases.append("ELSE '{label}'".format(label=title))

            kwargs['case_conditions'] = self.build_case_expression(
                label_cases,
                alias='field_status',
                trailing_comma=True,
            )
            if kwargs.get('layer_type') == accounts_models.DataLayer.LAYER_TYPE_LIVE:
                kwargs['entity_weekly_outer_join'] = ''
            else:
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
                '{entity_name}' AS entity_type,
                {table_name}."{col_name}" AS field_value,
                'connected' AS connectivity_status,
                {label_case_statements}
            FROM entities_entity
            INNER JOIN bounds ON ST_Intersects(entities_entity.geopoint, ST_Transform(bounds.geom, 4326))
            INNER JOIN entities_{entity_name}_entity ews ON "entities_entity"."id" = ews."entity_id"
            {entity_weekly_join}
            {entity_detail_join}
            {entity_master_table_condition}
            WHERE entities_entity."deleted" IS NULL
            AND entities_entity.entity_type_id = (SELECT id FROM entities_entity_type WHERE code = '{entity_name}' AND deleted IS NULL)
            {entity_detail_condition}
            {country_condition}
            {admin1_condition}
            {entity_condition}
            {random_order}
            {limit_condition}
        )
        SELECT COALESCE(NULLIF(tile.mvt, ''::bytea), {empty_mvt_layer})
        FROM (
            SELECT ST_AsMVT(DISTINCT mvtgeom.*, '{mvt_layer}') AS mvt FROM mvtgeom
        ) tile;
        """

        kwargs = copy.deepcopy(self.kwargs)

        kwargs['mvt_layer'] = kwargs.get('mvt_layer', 'default')
        kwargs['empty_mvt_layer'] = account_utilities.get_empty_mvt_layer_sql(kwargs['mvt_layer'])
        kwargs['country_condition'] = ''
        kwargs['admin1_condition'] = ''
        kwargs['entity_condition'] = ''

        kwargs['entity_master_table_join'] = ''
        kwargs['entity_master_table_condition'] = ''
        kwargs.setdefault('entity_detail_join', '')
        kwargs.setdefault('entity_detail_condition', '')

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
                    sql_statement = str(' AND '.join(values)).replace('SQL:', '').format(
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

        kwargs['label_case_statements'] = self.build_case_expression(label_cases, alias='field_status')

        # If legend config SQL or table_name references 'sws' (entity weekly status), add the JOIN
        kwargs['entity_weekly_join'] = ''
        if 'sws' in str(legend_configs) or kwargs.get('table_name') == 'sws':
            kwargs['entity_weekly_join'] = """
                INNER JOIN "connection_statistics_entityweeklystatus" sws
                    ON "entities_entity"."last_weekly_status_id" = sws."id"
            """

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

    def get_layer_entity_type_code(self, data_layer_instance):
        if data_layer_instance.entity_type and data_layer_instance.entity_type.is_legacy:
            return LEGACY_MODEL
        return data_layer_instance.entity_name

    def validate_requested_entity_type(self, data_layer_instance):
        requested_entity_type_codes = self.get_entity_type_code_params()
        if requested_entity_type_codes is None:
            return None

        layer_entity_type_code = self.get_layer_entity_type_code(data_layer_instance)
        if layer_entity_type_code not in requested_entity_type_codes:
            return Response(
                {
                    'error': (
                        f"entity_type__code does not match data layer entity type: "
                        f"{layer_entity_type_code}"
                    )
                },
                status=400,
            )
        return None

    def build_layer_sql_context(self, request, entity_code, entity_params):
        layer_id = entity_params.get(self.LAYER_ID_QUERY_PARAM)
        data_layer_instance = get_object_or_404(
            accounts_models.DataLayer.objects.select_related('entity_type'),
            pk=layer_id,
            status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
        )

        layer_entity_type_code = self.get_layer_entity_type_code(data_layer_instance)
        if entity_code and entity_code != layer_entity_type_code:
            return Response(
                {
                    'error': (
                        f"{entity_code}_layer_id does not match data layer entity type: "
                        f"{layer_entity_type_code}"
                    )
                },
                status=400,
            )

        is_legacy = layer_entity_type_code == LEGACY_MODEL
        data_sources = data_layer_instance.data_sources.all()
        first_data_source = data_sources.first()
        if first_data_source is None:
            return Response({'error': f'DataLayer (id={layer_id}) has no data sources configured.'}, status=400)

        live_data_sources = ['UNKNOWN']
        for data_source_relationship in data_sources:
            source_type = (data_source_relationship.data_source.data_source_type or '').upper()
            if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                live_data_sources.append(statistics_configs.QOS_SOURCE)
            elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

        country_ids = data_layer_instance.applicable_countries
        parameter_col = first_data_source.data_source_column
        parameter_column_name = str(parameter_col['name'])
        base_benchmark = str(parameter_col.get('base_benchmark', 1))
        scoped_query_params = self.build_scoped_query_params(
            request,
            entity_code,
            entity_params,
            is_legacy=is_legacy,
        )

        original_kwargs = self.kwargs
        self.kwargs = {}
        try:
            self.update_kwargs_from_dict(country_ids, data_layer_instance, scoped_query_params)
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
                    'parameter_col_function_sql': self.get_column_function_sql(
                        first_data_source.data_source_column_function
                    ),
                    'layer_type': accounts_models.DataLayer.LAYER_TYPE_LIVE,
                    'legend_configs': legend_configs,
                    'entity_name': data_layer_instance.entity_name,
                    'mvt_layer': LEGACY_MODEL if is_legacy else 'entities',
                })
            else:
                self.kwargs.update({
                    'col_name': parameter_column_name,
                    'legend_configs': legend_configs,
                    'parameter_col': parameter_col,
                    'layer_type': accounts_models.DataLayer.LAYER_TYPE_STATIC,
                    'entity_name': data_layer_instance.entity_name,
                    'mvt_layer': LEGACY_MODEL if is_legacy else 'entities',
                })

            return {
                'data_layer_instance': data_layer_instance,
                'is_legacy': is_legacy,
                'kwargs': copy.deepcopy(self.kwargs),
            }
        finally:
            self.kwargs = original_kwargs

    def get(self, request, *args, **kwargs):
        layer_entity_params = self.get_prefixed_layer_params()
        if len(layer_entity_params) > 1:
            use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
            request_path = remove_query_param(request.get_full_path(), 'cache')
            cache_key = self.get_cache_key()

            response = cache_manager.get(cache_key) if use_cached_data else None
            if not response:
                layer_sql_contexts = []
                for entity_code, entity_params in layer_entity_params.items():
                    layer_context = self.build_layer_sql_context(request, entity_code, entity_params)
                    if isinstance(layer_context, Response):
                        return layer_context
                    layer_sql_contexts.append(layer_context)

                self.layer_sql_contexts = layer_sql_contexts
                try:
                    response = self.generate_tile(request)
                finally:
                    self.layer_sql_contexts = []

                if response.status_code == rest_status.HTTP_200_OK:
                    cache_manager.set(cache_key, response, request_path=request_path,
                                      soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

            return response

        entity_code, entity_params = self.get_entity_layer_params()
        layer_id = entity_params.get(self.LAYER_ID_QUERY_PARAM) if entity_params else None
        if not layer_id:
            return Response(
                {
                    'error': (
                        'Provide a layer_id query parameter or exactly one '
                        '{entity_code}_layer_id query parameter.'
                    )
                },
                status=400,
            )

        self.kwargs[self.LAYER_ID_QUERY_PARAM] = layer_id

        data_layer_instance = get_object_or_404(
            accounts_models.DataLayer.objects.select_related('entity_type'),
            pk=layer_id,
            status=accounts_models.DataLayer.LAYER_STATUS_PUBLISHED,
        )

        entity_type_validation_error = self.validate_requested_entity_type(data_layer_instance)
        if entity_type_validation_error:
            return entity_type_validation_error

        if self.get_layer_entity_type_code(data_layer_instance) == LEGACY_MODEL:
            if entity_code:
                scoped_query_params = self.build_scoped_query_params(
                    request,
                    entity_code,
                    entity_params,
                    is_legacy=True,
                )
                request._request.GET = scoped_query_params
            kwargs.update({
                'pk': layer_id,
                'mvt_layer': LEGACY_MODEL,
            })
            school_view = DataLayerMapViewSet.as_view()
            return school_view(request._request, *args, **kwargs)

        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key()

        response = None
        if use_cached_data:
            response = cache_manager.get(cache_key)

        if not response:
            data_sources = data_layer_instance.data_sources.all()

            live_data_sources = ['UNKNOWN']

            for d in data_sources:
                source_type = (d.data_source.data_source_type or '').upper()
                if source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_QOS:
                    live_data_sources.append(statistics_configs.QOS_SOURCE)
                elif source_type == accounts_models.DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                    live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

            country_ids = data_layer_instance.applicable_countries
            parameter_col = data_sources.first().data_source_column

            parameter_column_name = str(parameter_col['name'])
            base_benchmark = str(parameter_col.get('base_benchmark', 1))

            if entity_code:
                scoped_query_params = self.build_scoped_query_params(
                    request,
                    entity_code,
                    entity_params,
                    is_legacy=False,
                )
                self.update_kwargs_from_dict(country_ids, data_layer_instance, scoped_query_params)
            else:
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
                    'mvt_layer': 'entities',
                })
            else:
                self.kwargs.update({
                    'col_name': parameter_column_name,
                    'legend_configs': legend_configs,
                    'parameter_col': parameter_col,
                    'layer_type': accounts_models.DataLayer.LAYER_TYPE_STATIC,
                    'entity_name': data_layer_instance.entity_name,
                    'mvt_layer': 'entities',
                })

            try:
                response = self.generate_tile(request)
                if self.cache_enabled(data_layer_instance) and response.status_code == rest_status.HTTP_200_OK:
                    cache_manager.set(cache_key, response, request_path=request_path,
                                      soft_timeout=settings.CACHE_CONTROL_MAX_AGE)
            except FieldDoesNotExist as ex:
                logger.error('Layer configuration error: {}'.format(ex))
                response = Response(status=rest_status.HTTP_204_NO_CONTENT)
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
                {col_function} AS "{col_name}"
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
        if benchmark_value is not None and isinstance(benchmark_value, str) and 'SQL:' in benchmark_value:
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
        if kwargs.get('layer_type') == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            kwargs['table_name'] = 'eds'
        else:
            kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'entities_entity')

        if len(legend_configs) > 0 and 'SQL:' in str(legend_configs):
            label_cases = []
            for title, values_and_label in legend_configs.items():
                label = values_and_label.get('labels', title).strip()
                if not label:
                    continue  # Skip empty labels to prevent SQL syntax errors
                query_labels.append(label)
                values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                     values_and_label.get('values', [])))

                if len(values) > 0:
                    is_sql_value = 'SQL:' in values[0]
                    if is_sql_value:
                        sql_statement = str(' AND '.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN {sql} THEN eds.entity_id ELSE NULL END) AS "{label}",'.format(
                                sql=sql_statement, label=label))
                else:
                    label_cases.append(
                        'COUNT(DISTINCT CASE WHEN eds.{col_name} IS NULL '
                        'THEN eds.entity_id ELSE NULL END) AS "{label}",'.format(
                            col_name=kwargs['col_name'], label=label))

            kwargs['case_conditions'] = ' '.join(label_cases)

            if kwargs.get('layer_type') == accounts_models.DataLayer.LAYER_TYPE_LIVE:
                kwargs['entity_weekly_outer_join'] = ''
            else:
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

        kwargs['col_function'] = kwargs['parameter_col_function_sql'].format(**kwargs)

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
        LEFT JOIN "connection_statistics_entityweeklystatus" ews ON entities_entity."last_weekly_status_id" =
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
                (
                    SELECT {col_function}
                    FROM "connection_statistics_entitydailystatus" t
                    WHERE t."entity_id" = "entities_entity"."id"
                        AND t."date" BETWEEN '{start_date}' AND '{end_date}'
                        AND t."live_data_source" IN ({live_source_types})
                        AND t."deleted" IS NULL
                ) AS "{col_name}"
            FROM "entities_entity"
            WHERE ("entities_entity"."id" IN ({ids})
                AND "entities_entity"."deleted" IS NULL
                AND entities_entity.entity_type_id = (SELECT id FROM entities_entity_type WHERE code = '{entity_name}' AND deleted IS NULL))
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
        if benchmark_value is not None and isinstance(benchmark_value, str) and 'SQL:' in benchmark_value:
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
        if kwargs.get('layer_type') == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            kwargs['table_name'] = 'eds'
        else:
            kwargs['table_name'] = kwargs['parameter_col'].get('table_name', 'entities_entity')
        if len(legend_configs) > 0 and 'SQL:' in str(legend_configs):
            label_cases = []
            for title, values_and_label in legend_configs.items():
                values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                     values_and_label.get('values', [])))

                if len(values) > 0:
                    is_sql_value = 'SQL:' in values[0]
                    if is_sql_value:
                        sql_statement = str(' AND '.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    label_cases.append("ELSE '{label}'".format(label=title))

            kwargs['case_conditions'] = self.build_case_expression(label_cases, alias='live_avg_connectivity')

            if 'ews' in str(legend_configs):
                kwargs['entity_weekly_outer_join'] = """
                LEFT JOIN "connection_statistics_entityweeklystatus" ews 
                    ON eds."last_weekly_status_id" = ews."id"
                """
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

        kwargs['col_function'] = kwargs['parameter_col_function_sql'].format(**kwargs)

        return query.format(**kwargs)

    def get_entity_view_statistics_info_query(self, layer_type):
        if layer_type == accounts_models.DataLayer.LAYER_TYPE_LIVE:
            join_condition = """
            LEFT JOIN connection_statistics_entityweeklystatus ews
                ON "entities_entity"."last_weekly_status_id" = ews."id"
            """
        else:
            join_condition = """
            LEFT JOIN entities_{entity_name}_entity ews
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
            {col_function} AS "field_avg"
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

        kwargs['col_function'] = kwargs['parameter_col_function_sql'].format(**kwargs)

        return query.format(**kwargs)

    def generate_school_graph_data(self, school_viewset_cls):
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
        entry_idx = {entry['key']: entry for entry in graph_data}

        for daily_avg_data in (data or []):
            formatted_date = date_utilities.format_date(daily_avg_data['date'])
            entry = entry_idx.get(formatted_date)
            if entry is not None:
                try:
                    rounded_speed = self.apply_unit_conversion(daily_avg_data['field_avg'], round_unit_value)
                    entry['value'] = rounded_speed
                    all_positive_speeds.append(rounded_speed)
                except (KeyError, TypeError):
                    pass

        return graph_data, all_positive_speeds

    @staticmethod
    def apply_unit_conversion(val, round_unit_value):
        if val is None:
            return 0
        try:
            val = float(val)
            if '/ (1000 * 1000)' in round_unit_value:
                return round(val / 1000000.0, 2)
            elif '/ 1000' in round_unit_value:
                return round(val / 1000.0, 2)
            elif '* 1000 * 1000' in round_unit_value:
                return round(val * 1000000.0, 2)
            elif '* 1000' in round_unit_value:
                return round(val * 1000.0, 2)
            return round(val, 2)
        except (ValueError, TypeError):
            return 0

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
            entry_idx_per_entity = {}

            for entity_id in kwargs.get('entity_ids', []):
                entity_id_str = str(entity_id)
                graph_data_per_entity[entity_id_str] = copy.deepcopy(graph_data)
                all_positive_speeds_per_entity[entity_id_str] = []
                entry_idx_per_entity[entity_id_str] = {
                    entry['key']: entry for entry in graph_data_per_entity[entity_id_str]
                }

            # Update the graph_data with actual values if they exist
            for daily_avg_data in (data or []):
                entity_id = str(daily_avg_data['id'])
                if entity_id not in entry_idx_per_entity:
                    continue
                formatted_date = date_utilities.format_date(daily_avg_data['date'])

                entry = entry_idx_per_entity[entity_id].get(formatted_date)
                if entry is not None:
                    try:
                        rounded_speed = self.apply_unit_conversion(daily_avg_data['field_avg'], round_unit_value)
                        entry['value'] = rounded_speed
                        all_positive_speeds_per_entity[entity_id].append(rounded_speed)
                    except (KeyError, TypeError):
                        pass
            return graph_data_per_entity, all_positive_speeds_per_entity

        all_positive_speeds = []
        entry_idx = {entry['key']: entry for entry in graph_data}

        for daily_avg_data in (data or []):
            formatted_date = date_utilities.format_date(daily_avg_data['date'])
            entry = entry_idx.get(formatted_date)
            if entry is not None:
                try:
                    rounded_speed = self.apply_unit_conversion(daily_avg_data['field_avg'], round_unit_value)
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
        has_remainder = False

        for title, values_and_label in legend_configs.items():
            values = list(filter(lambda val: val if not core_utilities.is_blank_string(val) else None,
                                 values_and_label.get('values', [])))
            label = values_and_label.get('labels', title).strip()
            if not label:
                continue
            query_labels.append(label)

            if len(values) > 0:
                is_sql_value = 'SQL:' in values[0]
                if is_sql_value:
                    sql_statement = str(' AND '.join(values)).replace('SQL:', '').format(
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
                        label_cases.append('0 AS "{label}",'.format(label=label))
                    elif has_remainder:
                        label_cases.append('0 AS "{label}",'.format(label=label))
                    elif parameter_col_type == 'str':
                        has_remainder = True
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN LOWER({table_name}."{col_name}") NOT IN ({value}) '
                            'THEN entities_entity."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join(["'" + str(v).lower() + "'" for v in values])
                            ))
                    elif parameter_col_type == 'int':
                        has_remainder = True
                        label_cases.append(
                            'COUNT(DISTINCT CASE WHEN {table_name}."{col_name}" NOT IN ({value}) '
                            'THEN entities_entity."id" ELSE NULL END) AS "{label}",'.format(
                                table_name=kwargs['table_name'],
                                col_name=kwargs['col_name'],
                                label=label,
                                value=','.join([str(v) for v in values])
                            ))

        kwargs['label_case_statements'] = ' '.join(label_cases)

        # If legend config SQL references 'sws' (entity weekly status), add the JOIN
        if 'sws' in str(legend_configs):
            kwargs['entity_weekly_join'] = """
                INNER JOIN "connection_statistics_entityweeklystatus" sws
                    ON "entities_entity"."last_weekly_status_id" = sws."id"
            """

        return query.format(**kwargs)

    def get_school_basic_info_query(self):
        query = """
        SELECT schools_school."id",
            schools_school."name",
            schools_school."external_id",
            schools_school."giga_id_school" AS giga_id,
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
            ST_AsGeoJSON(ST_Transform(schools_school."geopoint", 4326)) AS geopoint,
            CASE WHEN schools_school.connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN schools_school.connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END as connectivity_status
        FROM "schools_school"
        INNER JOIN locations_country c ON c.id = schools_school.country_id
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
            AND schools_school."deleted" IS NULL
        ORDER BY schools_school."id" ASC
        """

        kwargs = copy.deepcopy(self.kwargs)
        kwargs['ids'] = ','.join(kwargs['entity_ids'])
        return query.format(**kwargs)

    def get_entity_basic_info_query(self):
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
            ST_AsGeoJSON(ST_Transform(entities_entity."geopoint", 4326)) AS geopoint,
            CASE WHEN entities_entity.connectivity_status IN ('good', 'moderate') THEN 'connected'
                WHEN entities_entity.connectivity_status = 'no' THEN 'not_connected'
                ELSE 'unknown'
            END as connectivity_status
        FROM "entities_entity"
        INNER JOIN locations_country c ON c.id = entities_entity.country_id
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
        {entity_weekly_join}
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
                    sql_statement = str(' AND '.join(values)).replace('SQL:', '').format(
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

        kwargs['label_case_statements'] = self.build_case_expression(
            label_cases,
            alias='field_status',
            trailing_comma=True,
        )

        # If legend config SQL or table_name references 'sws' (entity weekly status), add the JOIN
        kwargs['entity_weekly_join'] = ''
        if 'sws' in str(legend_configs) or kwargs.get('table_name') == 'sws':
            kwargs['entity_weekly_join'] = """
                INNER JOIN "connection_statistics_entityweeklystatus" sws
                    ON "entities_entity"."last_weekly_status_id" = sws."id"
            """

        return query.format(**kwargs)

    @staticmethod
    def get_selected_ids(kwargs, is_legacy):
        return kwargs.get('school_ids', []) if is_legacy else kwargs.get('entity_ids', [])

    def build_benchmark_metadata(
        self,
        benchmark_value,
        benchmark_unit,
        base_benchmark,
        parameter_column_unit,
        round_unit_value,
        display_unit,
        benchmark_value_from_sql=None,
    ):
        if benchmark_value_from_sql:
            rounded_benchmark_value = round(
                eval(round_unit_value.format(val=core_utilities.convert_to_int(benchmark_value_from_sql))), 2)
            benchmark_value = str(benchmark_value_from_sql)
        else:
            rounded_benchmark_value = round(
                eval(round_unit_value.format(val=core_utilities.convert_to_int(benchmark_value))), 2)

        return {
            'benchmark_value': benchmark_value,
            'rounded_benchmark_value': rounded_benchmark_value,
            'benchmark_unit': benchmark_unit,
            'base_benchmark': base_benchmark,
            'parameter_column_unit': parameter_column_unit,
            'round_unit_value': round_unit_value,
            'convert_unit': self.kwargs.get('convert_unit'),
            'display_unit': display_unit,
        }

    @staticmethod
    def resolve_connectivity_bucket(live_avg, rounded_benchmark_value, rounded_base_benchmark, is_reverse):
        if is_reverse:
            if live_avg < rounded_benchmark_value:
                return 'good'
            if rounded_benchmark_value <= live_avg <= rounded_base_benchmark:
                return 'moderate'
            if live_avg > rounded_base_benchmark:
                return 'bad'
            return 'unknown'

        if live_avg > rounded_benchmark_value:
            return 'good'
        if rounded_base_benchmark <= live_avg <= rounded_benchmark_value:
            return 'moderate'
        if live_avg < rounded_base_benchmark:
            return 'bad'
        return 'unknown'

    @staticmethod
    def sort_info_panel_rows(selected_ids, info_panel_rows, statistics, statistics_key):
        rows_by_id = {str(row['id']): row for row in info_panel_rows}
        statistics_by_id = {str(item[statistics_key]): item for item in statistics}
        sorted_rows = []

        for entity_id in selected_ids:
            row = rows_by_id.get(str(entity_id))
            if row is None:
                continue
            if isinstance(row.get('geopoint'), str):
                row['geopoint'] = json.loads(row['geopoint'])
            row['statistics'] = statistics_by_id.get(str(entity_id), {})
            sorted_rows.append(row)

        return sorted_rows

    def generate_school_detail_graph_data(self, school_viewset_cls):
        kwargs = copy.deepcopy(self.kwargs)
        data = db_utilities.sql_to_response(
            school_viewset_cls.get_avg_query(self, **kwargs),
            label=self.__class__.__name__,
            db_var=settings.READ_ONLY_DB_KEY,
        ) or []

        graph_data = []
        current_date = kwargs['start_date']
        while current_date <= kwargs['end_date']:
            graph_data.append({
                'group': 'Speed',
                'key': date_utilities.format_date(current_date),
                'value': None,
            })
            current_date += timedelta(days=1)

        graph_data_per_school = {}
        all_positive_speeds_per_school = {}
        entry_idx_per_school = {}

        for school_id in kwargs.get('school_ids', []):
            school_id_str = str(school_id)
            graph_data_per_school[school_id_str] = copy.deepcopy(graph_data)
            all_positive_speeds_per_school[school_id_str] = []
            entry_idx_per_school[school_id_str] = {
                entry['key']: entry for entry in graph_data_per_school[school_id_str]
            }

        round_unit_value = kwargs['round_unit_value']
        for daily_avg_data in data:
            school_id = str(daily_avg_data['id'])
            if school_id not in entry_idx_per_school:
                continue

            formatted_date = date_utilities.format_date(daily_avg_data['date'])
            entry = entry_idx_per_school[school_id].get(formatted_date)
            if entry is not None:
                try:
                    rounded_speed = self.apply_unit_conversion(daily_avg_data['field_avg'], round_unit_value)
                    entry['value'] = rounded_speed
                    all_positive_speeds_per_school[school_id].append(rounded_speed)
                except (KeyError, TypeError):
                    pass

        return graph_data_per_school, all_positive_speeds_per_school

    def build_school_detail_response(
        self,
        request,
        school_viewset_cls,
        is_live_layer,
        parameter_col_function,
        benchmark_value,
        benchmark_unit,
        base_benchmark,
        parameter_column_unit,
        unit_agg_str,
        display_unit,
        scoped_query_params,
    ):
        selected_ids = self.kwargs.get('school_ids', [])
        try:
            statistics = db_utilities.sql_to_response(
                school_viewset_cls.get_school_view_statistics_info_query(self),
                label=self.__class__.__name__,
                db_var=settings.READ_ONLY_DB_KEY,
                raise_exception=True) or []

            if is_live_layer:
                info_panel_rows = db_utilities.sql_to_response(
                    school_viewset_cls.get_school_view_info_query(self),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY,
                    raise_exception=True) or []
                graph_data, positive_speeds = self.generate_school_detail_graph_data(school_viewset_cls)
            else:
                info_panel_rows = db_utilities.sql_to_response(
                    school_viewset_cls.get_static_school_view_info_query(self),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY,
                    raise_exception=True) or []
                graph_data, positive_speeds = {}, {}
        except Exception as sql_exc:
            return {'error': str(sql_exc)}

        import sys; sys.stderr.write(f"INFO_PANEL_ROWS: {info_panel_rows}\nKWARGS: {self.kwargs}\n")
        sorted_rows = self.sort_info_panel_rows(selected_ids, info_panel_rows, statistics, 'school_id')

        include_same_location = scoped_query_params.get('include_same_location_schools') == 'true'
        for school_row in sorted_rows:
            if is_live_layer:
                school_speeds = positive_speeds.get(str(school_row['id']), [])
                school_row['live_avg'] = self.get_live_avg(
                    parameter_col_function.get('name', 'avg'),
                    school_speeds,
                )
                school_row['graph_data'] = graph_data.get(str(school_row['id']), [])

            school_row['benchmark_metadata'] = self.build_benchmark_metadata(
                benchmark_value=benchmark_value,
                benchmark_unit=benchmark_unit,
                base_benchmark=base_benchmark,
                parameter_column_unit=parameter_column_unit,
                round_unit_value=unit_agg_str,
                display_unit=display_unit,
                benchmark_value_from_sql=school_row.get('benchmark_sql_value'),
            )

            if include_same_location and sorted_rows:
                is_legacy = self.kwargs.get('entity_name', '') == 'school'
                same_loc_map = self.get_entities_at_same_location_batched(request, sorted_rows, is_legacy)
                for school_row in sorted_rows:
                    school_row['schools_at_same_location'] = same_loc_map.get(school_row.get('id'), {"count": 0, "school_ids": []})

        return sorted_rows

    def get_entities_at_same_location_batched(self, request, rows, is_legacy):
        response_map = {}
        if not rows:
            return response_map

        ids_str = ','.join(str(r['id']) for r in rows if r.get('id'))
        if not ids_str:
            return response_map

        table_name = "schools_school" if is_legacy else "entities_entity"
        rt_table_name = "connection_statistics_schoolrealtimeregistration" if is_legacy else "connection_statistics_entityrealtimeregistration"
        entity_col = "school_id" if is_legacy else "entity_id"
        end_date_str = self.kwargs.get('end_date', '2099-01-01')

        try:
            limit = int(request.query_params.get(f"limit_same_location_schools", 300))
            limit = limit if limit > 0 else 300
        except ValueError:
            limit = 300
        try:
            offset = int(request.query_params.get(f"offset_same_location_schools", 0))
            offset = max(offset, 0)
        except ValueError:
            offset = 0

        batch_query = f"""
            SELECT e1.id AS original_id, e2.id AS related_id, srr.id AS srr_id
            FROM {table_name} e1
            JOIN {table_name} e2 ON e1.geopoint = e2.geopoint AND e1.id != e2.id
            LEFT JOIN {rt_table_name} srr
                ON e2.id = srr.{entity_col}
                AND srr.deleted IS NULL
                AND srr.rt_registration_date <= '{{end_date_str}}'
            WHERE e1.id IN ({{ids_str}})
              AND e1.deleted IS NULL
              AND e2.deleted IS NULL
        """

        from collections import defaultdict
        grouped = defaultdict(list)

        batch_query_formatted = batch_query.format(ids_str=ids_str, end_date_str=end_date_str)
        batch_rows = db_utilities.sql_to_response(batch_query_formatted, label=self.__class__.__name__, db_var=settings.READ_ONLY_DB_KEY) or []

        for row in batch_rows:
            grouped[row['original_id']].append((row['srr_id'] is not None, row['related_id']))

        for orig, items in grouped.items():
            items.sort(key=lambda x: (0 if x[0] else 1, x[1]))
            response_map[orig] = {
                "count": len(items),
                "school_ids": [x[1] for x in items[offset:offset+limit]]
            }

        return response_map

    def compute_unit_conversion(self, parameter_column_unit):
        unit_agg_str = '{val}'
        if (
            self.kwargs.get('convert_unit') and
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
        return unit_agg_str

    def build_is_data_synced_qs(self, is_legacy):
        if is_legacy:
            is_data_synced_qs = SchoolWeeklyStatus.objects.filter(
                school__realtime_registration_status__rt_registered=True,
            )
            entity_prefix = 'school'
        else:
            is_data_synced_qs = EntityWeeklyStatus.objects.filter(
                entity__realtime_registration_status__rt_registered=True,
            )
            entity_prefix = 'entity'

        if len(self.kwargs.get(f'{entity_prefix}_filters', '')) > 0:
            is_data_synced_qs = is_data_synced_qs.extra(where=[self.kwargs[f'{entity_prefix}_filters']])
        if len(self.kwargs.get(f'{entity_prefix}_static_filters', '')) > 0:
            is_data_synced_qs = is_data_synced_qs.extra(where=[self.kwargs[f'{entity_prefix}_static_filters']])

        if len(self.kwargs.get('admin1_ids', [])) > 0:
            is_data_synced_qs = is_data_synced_qs.filter(**{f"{entity_prefix}__admin1_id__in": self.kwargs['admin1_ids']})
        elif len(self.kwargs.get('country_ids', [])) > 0:
            is_data_synced_qs = is_data_synced_qs.filter(**{f"{entity_prefix}__country_id__in": self.kwargs['country_ids']})

        return is_data_synced_qs

    def build_entity_detail_response(
        self,
        request,
        is_live_layer,
        parameter_col_function,
        benchmark_value,
        benchmark_unit,
        base_benchmark,
        parameter_column_unit,
        unit_agg_str,
        display_unit,
        scoped_query_params,
        data_layer_instance,
    ):
        selected_ids = self.kwargs.get('entity_ids', [])

        try:
            statistics = db_utilities.sql_to_response(
                self.get_entity_view_statistics_info_query(data_layer_instance.type),
                label=self.__class__.__name__,
                db_var=settings.READ_ONLY_DB_KEY,
                raise_exception=True) or []

            if is_live_layer:
                info_panel_rows = db_utilities.sql_to_response(
                    self.get_entity_view_info_query(),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY,
                    raise_exception=True) or []
                graph_data, positive_speeds = self.generate_graph_data()
            else:
                info_panel_rows = db_utilities.sql_to_response(
                    self.get_static_entity_view_info_query(),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY,
                    raise_exception=True) or []
                graph_data, positive_speeds = {}, {}
        except Exception as sql_exc:
            return {'error': str(sql_exc)}

        print("INFO_PANEL_ROWS:", info_panel_rows)
        print("KWARGS:", self.kwargs)
        sorted_rows = self.sort_info_panel_rows(selected_ids, info_panel_rows, statistics, 'entity_id')

        for entity_row in sorted_rows:
            if is_live_layer:
                entity_speeds = positive_speeds.get(str(entity_row['id']), [])
                entity_row['live_avg'] = self.get_live_avg(
                    parameter_col_function.get('name', 'avg'),
                    entity_speeds,
                )
                entity_row['graph_data'] = graph_data.get(str(entity_row['id']), [])

            entity_row['benchmark_metadata'] = self.build_benchmark_metadata(
                benchmark_value=benchmark_value,
                benchmark_unit=benchmark_unit,
                base_benchmark=base_benchmark,
                parameter_column_unit=parameter_column_unit,
                round_unit_value=unit_agg_str,
                display_unit=display_unit,
                benchmark_value_from_sql=entity_row.get('benchmark_sql_value'),
            )

        return sorted_rows

    def build_entity_summary_response(
        self,
        is_live_layer,
        parameter_col_function,
        benchmark_value,
        benchmark_unit,
        base_benchmark,
        parameter_column_unit,
        unit_agg_str,
        display_unit,
        data_layer_instance,
        entity_code,
        layer_id,
        parameter_column_name,
        legend_configs,
    ):
        is_data_synced_qs = self.build_is_data_synced_qs(is_legacy=False)
        query_labels = []

        try:
            if is_live_layer:
                query_result = db_utilities.sql_to_response(
                    self.get_info_query(query_labels),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY,
                    raise_exception=True)
            else:
                query_result = db_utilities.sql_to_response(
                    self.get_static_info_query(query_labels),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY,
                    raise_exception=True)
        except Exception as sql_exc:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(
                f'Failed to fetch {"live" if is_live_layer else "static"} data for {entity_code} layer '
                f'(layer_id={layer_id}, column="{parameter_column_name}", '
                f'entity="{data_layer_instance.entity_name}"). '
                f'DB error: {sql_exc}'
            )
            query_result = None

        if not query_result:
            if is_live_layer:
                return {
                    'no_of_entities_measure': 0,
                    'entity_with_realtime_data': 0,
                    'real_time_connected_entities': {},
                    'is_data_synced': is_data_synced_qs.exists(),
                    'live_avg': 0,
                    'live_avg_connectivity': 'unknown',
                    'graph_data': [],
                    'benchmark_metadata': {'parameter_column_unit': parameter_column_unit, 'display_unit': display_unit},
                }
            else:
                return {
                    'total_entities': 0,
                    'connected_entities': {},
                    'legend_configs': legend_configs,
                    'benchmark_metadata': {'parameter_column_unit': parameter_column_unit, 'display_unit': display_unit},
                }

        query_response = query_result[-1]

        benchmark_metadata = self.build_benchmark_metadata(
            benchmark_value=benchmark_value,
            benchmark_unit=benchmark_unit,
            base_benchmark=base_benchmark,
            parameter_column_unit=parameter_column_unit,
            round_unit_value=unit_agg_str,
            display_unit=display_unit,
            benchmark_value_from_sql=query_response.get('benchmark_sql_value'),
        )

        if is_live_layer:
            no_of_entities_measure = query_response.get('no_of_entities_measure', 0)
            if no_of_entities_measure == 0:
                graph_data = []
                positive_speeds = []
                live_avg = 0
            else:
                graph_data, positive_speeds = self.generate_graph_data()
                live_avg = self.get_live_avg(parameter_col_function.get('name', 'avg'), positive_speeds)

            rounded_base_benchmark_int = round(
                eval(unit_agg_str.format(val=core_utilities.convert_to_int(base_benchmark))), 2)

            if no_of_entities_measure == 0:
                live_avg_connectivity = 'unknown'
            else:
                live_avg_connectivity = self.resolve_connectivity_bucket(
                    live_avg,
                    benchmark_metadata['rounded_benchmark_value'],
                    rounded_base_benchmark_int,
                    data_layer_instance.is_reverse,
                )

            if query_labels:
                connected_entities = {label: query_response.get(label, 0) for label in query_labels}
            else:
                connected_entities = {
                    'good': query_response.get('good', 0),
                    'moderate': query_response.get('moderate', 0),
                    'no_internet': query_response.get('bad', 0),
                    'unknown': query_response.get('unknown', 0),
                }

            return {
                'no_of_entities_measure': query_response.get('no_of_entities_measure', 0),
                'entity_with_realtime_data': query_response.get('entity_with_realtime_data', 0),
                'real_time_connected_entities': connected_entities,
                'is_data_synced': is_data_synced_qs.exists(),
                'live_avg': live_avg,
                'live_avg_connectivity': live_avg_connectivity,
                'graph_data': graph_data,
                'benchmark_metadata': benchmark_metadata,
            }
        else:
            return {
                'total_entities': query_response.get('total_entities', 0),
                'connected_entities': {label: query_response.get(label, 0) for label in query_labels},
                'legend_configs': legend_configs,
                'benchmark_metadata': {
                    'parameter_column_unit': parameter_column_unit,
                    'display_unit': display_unit,
                },
            }

    def build_legacy_summary_response(
        self,
        is_live_layer,
        parameter_col_function,
        benchmark_value,
        benchmark_unit,
        base_benchmark,
        parameter_column_unit,
        unit_agg_str,
        display_unit,
        data_layer_instance,
        entity_code,
        layer_id,
        parameter_column_name,
        legend_configs,
    ):
        is_data_synced_qs = self.build_is_data_synced_qs(is_legacy=True)
        query_labels = []

        try:
            if is_live_layer:
                query_result = db_utilities.sql_to_response(
                    DataLayerInfoViewSet.get_info_query(self),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY,
                    raise_exception=True)
            else:
                query_result = db_utilities.sql_to_response(
                    DataLayerInfoViewSet.get_static_info_query(self, query_labels),
                    label=self.__class__.__name__,
                    db_var=settings.READ_ONLY_DB_KEY,
                    raise_exception=True)
        except Exception as sql_exc:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(
                f'Failed to fetch {"live" if is_live_layer else "static"} data for {entity_code} layer '
                f'(layer_id={layer_id}, column="{parameter_column_name}", '
                f'entity="{data_layer_instance.entity_name}", path=legacy). '
                f'DB error: {sql_exc}'
            )
            query_result = None

        if not query_result:
            if is_live_layer:
                return {
                    'no_of_entities_measure': 0,
                    'entity_with_realtime_data': 0,
                    'real_time_connected_entities': {},
                    'is_data_synced': is_data_synced_qs.exists(),
                    'live_avg': 0,
                    'live_avg_connectivity': 'unknown',
                    'graph_data': [],
                    'benchmark_metadata': {'parameter_column_unit': parameter_column_unit, 'display_unit': display_unit},
                }
            else:
                return {
                    'total_entities': 0,
                    'connected_entities': {},
                    'legend_configs': legend_configs,
                    'benchmark_metadata': {'parameter_column_unit': parameter_column_unit, 'display_unit': display_unit},
                }

        query_response = query_result[-1]

        benchmark_metadata = self.build_benchmark_metadata(
            benchmark_value=benchmark_value,
            benchmark_unit=benchmark_unit,
            base_benchmark=base_benchmark,
            parameter_column_unit=parameter_column_unit,
            round_unit_value=unit_agg_str,
            display_unit=display_unit,
            benchmark_value_from_sql=query_response.get('benchmark_sql_value'),
        )

        if is_live_layer:
            no_of_entities_measure = query_response.get('no_of_entities_measure', 0)
            if no_of_entities_measure == 0:
                graph_data = []
                live_avg = 0
            else:
                graph_data, positive_speeds = self.generate_school_graph_data(DataLayerInfoViewSet)
                live_avg = self.get_live_avg(parameter_col_function.get('name', 'avg'), positive_speeds)

            rounded_base_benchmark_int = round(
                eval(unit_agg_str.format(val=core_utilities.convert_to_int(base_benchmark))), 2)

            if no_of_entities_measure == 0:
                live_avg_connectivity = 'unknown'
            else:
                live_avg_connectivity = self.resolve_connectivity_bucket(
                    live_avg,
                    benchmark_metadata['rounded_benchmark_value'],
                    rounded_base_benchmark_int,
                    data_layer_instance.is_reverse,
                )

            return {
                'no_of_entities_measure': query_response.get('no_of_entities_measure', 0),
                'entity_with_realtime_data': query_response.get('entity_with_realtime_data', 0),
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
                'benchmark_metadata': benchmark_metadata,
            }
        else:
            print(f"QUERY_RESPONSE STATIC: {query_response}")
            return {
                'total_entities': query_response.get('total_entities', 0),
                'connected_entities': {label: query_response.get(label, 0) for label in query_labels},
                'legend_configs': legend_configs,
                'benchmark_metadata': {
                    'parameter_column_unit': parameter_column_unit,
                    'display_unit': display_unit,
                },
            }

    def process_entity_layer(self, request, entity_code, params):
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

        is_legacy = (data_layer_instance.entity_type is None) or data_layer_instance.entity_type.is_legacy

        data_sources = data_layer_instance.data_sources.all()
        live_data_sources = ['UNKNOWN']

        for d in data_sources:
            source_type = (d.data_source.data_source_type or '').upper()
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

        entity_query_params = self.build_scoped_query_params(request, entity_code, params, is_legacy)

        try:
            self.update_kwargs_from_dict(country_ids, data_layer_instance, entity_query_params)
        except ValueError as e:
            return {'error': str(e)}

        benchmark_value, benchmark_unit = self.get_benchmark_value(data_layer_instance)
        global_benchmark = data_layer_instance.global_benchmark.get('value')
        legend_configs = self.get_legend_configs(data_layer_instance)

        unit_agg_str = self.compute_unit_conversion(parameter_column_unit)
        self.kwargs['round_unit_value'] = unit_agg_str

        selected_ids = self.get_selected_ids(self.kwargs, is_legacy)
        is_live_layer = data_layer_instance.type == accounts_models.DataLayer.LAYER_TYPE_LIVE

        parameter_col_function = {}
        if is_live_layer:
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
            })
        else:
            self.kwargs.update({
                'col_name': parameter_column_name,
                'legend_configs': legend_configs,
                'parameter_col': parameter_col,
                'entity_name': data_layer_instance.entity_name,
                'school_filters': self.kwargs.get('entity_filters', ''),
                'school_static_filters': self.kwargs.get('entity_static_filters', ''),
            })

        if selected_ids:
            if is_legacy:
                return self.build_school_detail_response(
                    request=request,
                    school_viewset_cls=DataLayerInfoViewSet,
                    is_live_layer=is_live_layer,
                    parameter_col_function=parameter_col_function,
                    benchmark_value=benchmark_value,
                    benchmark_unit=benchmark_unit,
                    base_benchmark=base_benchmark,
                    parameter_column_unit=parameter_column_unit,
                    unit_agg_str=unit_agg_str,
                    display_unit=display_unit,
                    scoped_query_params=entity_query_params,
                )
            else:
                return self.build_entity_detail_response(
                    request=request,
                    is_live_layer=is_live_layer,
                    parameter_col_function=parameter_col_function,
                    benchmark_value=benchmark_value,
                    benchmark_unit=benchmark_unit,
                    base_benchmark=base_benchmark,
                    parameter_column_unit=parameter_column_unit,
                    unit_agg_str=unit_agg_str,
                    display_unit=display_unit,
                    scoped_query_params=entity_query_params,
                    data_layer_instance=data_layer_instance,
                )
        else:
            if is_legacy:
                return self.build_legacy_summary_response(
                    is_live_layer=is_live_layer,
                    parameter_col_function=parameter_col_function,
                    benchmark_value=benchmark_value,
                    benchmark_unit=benchmark_unit,
                    base_benchmark=base_benchmark,
                    parameter_column_unit=parameter_column_unit,
                    unit_agg_str=unit_agg_str,
                    display_unit=display_unit,
                    data_layer_instance=data_layer_instance,
                    entity_code=entity_code,
                    layer_id=layer_id,
                    parameter_column_name=parameter_column_name,
                    legend_configs=legend_configs,
                )
            else:
                return self.build_entity_summary_response(
                    is_live_layer=is_live_layer,
                    parameter_col_function=parameter_col_function,
                    benchmark_value=benchmark_value,
                    benchmark_unit=benchmark_unit,
                    base_benchmark=base_benchmark,
                    parameter_column_unit=parameter_column_unit,
                    unit_agg_str=unit_agg_str,
                    display_unit=display_unit,
                    data_layer_instance=data_layer_instance,
                    entity_code=entity_code,
                    layer_id=layer_id,
                    parameter_column_name=parameter_column_name,
                    legend_configs=legend_configs,
                )

    def process_entity_without_layer(self, request, entity_code, params):
        """
        Process a single entity type and return its basic info when no layer_id is provided.
        Only valid for detail views (when entity_ids are provided).
        """
        entity_config = get_entity_type_config(entity_code)
        is_legacy = getattr(entity_config, 'is_legacy', False) if entity_config else False

        entity_query_params = self.build_scoped_query_params(request, entity_code, params, is_legacy)

        # Populate kwargs from query params manually for basic usage
        if 'entity_id__in' in entity_query_params:
            self.kwargs['entity_ids'] = [s_id.strip() for s_id in entity_query_params['entity_id__in'].split(',')]
        elif 'school_id__in' in entity_query_params:
            self.kwargs['entity_ids'] = [s_id.strip() for s_id in entity_query_params['school_id__in'].split(',')]
        elif 'entity_id' in entity_query_params:
            self.kwargs['entity_ids'] = [entity_query_params['entity_id'].strip()]
        elif 'school_id' in entity_query_params:
            self.kwargs['entity_ids'] = [entity_query_params['school_id'].strip()]

        if 'include_same_location' in entity_query_params:
            self.kwargs['include_same_location_schools'] = entity_query_params['include_same_location']
        elif 'include_same_location_schools' in entity_query_params:
            self.kwargs['include_same_location_schools'] = entity_query_params['include_same_location_schools']

        selected_ids = self.kwargs.get('entity_ids', [])

        # If no selected IDs, we cannot return summary statistics without a layer configuration
        if not selected_ids:
            return None

        # Get basic entity details
        if is_legacy:
            info_panel_rows = db_utilities.sql_to_response(
                self.get_school_basic_info_query(),
                label=self.__class__.__name__,
                db_var=settings.READ_ONLY_DB_KEY) or []
        else:
            info_panel_rows = db_utilities.sql_to_response(
                self.get_entity_basic_info_query(),
                label=self.__class__.__name__,
                db_var=settings.READ_ONLY_DB_KEY) or []

        sorted_rows = self.sort_info_panel_rows(selected_ids, info_panel_rows, [], 'entity_id')

        # Determine if we should include same location entities
        include_same_location = self.kwargs.get('include_same_location_schools') == 'true'
        if include_same_location and sorted_rows:
            same_loc_map = self.get_entities_at_same_location_batched(request, sorted_rows, is_legacy)
            for row in sorted_rows:
                row['schools_at_same_location'] = same_loc_map.get(row.get('id'), {"count": 0, "school_ids": []})

        return sorted_rows

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

            response = {}

            # Find entity codes that don't have layer_id but have entity_ids directly
            for param_name, param_value in request.query_params.items():
                if param_name.endswith('_entity_id__in') or param_name.endswith('_entity_id'):
                    if param_name.endswith('_entity_id__in'):
                        entity_code = param_name[:-len('_entity_id__in')]
                    else:
                        entity_code = param_name[:-len('_entity_id')]

                    if entity_code not in entity_params:
                        entity_params[entity_code] = {}

            # Also catch the generic entity_id__in case if an entity_type_code is specified
            generic_entity_id = request.query_params.get('entity_id__in') or request.query_params.get('entity_id')
            generic_entity_type = request.query_params.get('entity_type__code')
            if generic_entity_id and generic_entity_type and generic_entity_type not in entity_params:
                entity_params[generic_entity_type] = {}

            # If no entity-specific params found, return error
            if not entity_params:
                return Response(
                    {'error': 'No entity parameters found. Please provide at least one entity_layer_id parameter.'},
                    status=400
                )

            # Process each entity type
            for entity_code, params in entity_params.items():
                if 'layer_id' in params:
                    try:
                        entity_response = self.process_entity_layer(request, entity_code, params)
                        if isinstance(entity_response, dict) and 'error' in entity_response:
                            import logging
                            logger = logging.getLogger(__name__)
                            logger.error(f"Failed to process layer for {entity_code}: {entity_response['error']}")
                            raise ValueError(entity_response['error'])
                    except Exception as exc:
                        try:
                            entity_response = self.process_entity_without_layer(request, entity_code, params)
                        except Exception:
                            entity_response = None
                        
                        if entity_response is None:
                            entity_response = {
                                'total_entities': 0,
                                'connected_entities': {},
                                'legend_configs': {},
                                'no_of_entities_measure': 0,
                                'entity_with_realtime_data': 0,
                                'real_time_connected_entities': {},
                                'is_data_synced': False,
                                'live_avg': 0,
                                'live_avg_connectivity': 'unknown',
                                'graph_data': [],
                                'benchmark_metadata': {'parameter_column_unit': '', 'display_unit': ''},
                            }
                    if entity_response is not None:
                        response[entity_code] = entity_response
                elif params or f'{entity_code}_entity_id__in' in request.query_params or f'{entity_code}_entity_id' in request.query_params or (generic_entity_type == entity_code and generic_entity_id):
                    # Has params or explicit ID but no layer_id - try processing basic info
                    try:
                        entity_response = self.process_entity_without_layer(request, entity_code, params)
                    except Exception as exc:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.error(f"Failed to process basic info for {entity_code}: {exc}")
                        entity_response = []
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
                        sql_statement = str(' AND '.join(values)).replace('SQL:', '').format(**kwargs)
                        label_cases.append("""WHEN {sql} THEN '{label}'""".format(sql=sql_statement, label=title))
                else:
                    label_cases.append("ELSE '{label}'".format(label=title))

            kwargs['case_conditions'] = BaseEntityDataLayerAPIViewSet.build_case_expression(
                label_cases,
                alias='connectivity',
                trailing_comma=True,
            )
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
                    sql_statement = str(' AND '.join(values)).replace('SQL:', '').format(
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

        kwargs['label_case_statements'] = BaseEntityDataLayerAPIViewSet.build_case_expression(
            label_cases,
            alias='field_status',
        )

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
                source_type = (d.data_source.data_source_type or '').upper()
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
                if isinstance(map_point.get('geopoint'), str):
                    map_point['geopoint'] = json.loads(map_point['geopoint'])
        return Response(data={'map': map_points})


class PublishedEntityDataLayersViewSet(EntityTypeCodeMixin, CachedListMixin, BaseModelViewSet):
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
    }

    permit_list_expands = ['created_by', 'published_by', 'last_modified_by']

    def apply_queryset_filters(self, queryset):
        """
        Override if applying more complex filters to queryset.
        :param queryset:
        :return queryset:
        """
        queryset = queryset.filter(status=self.kwargs.get('status', 'PUBLISHED'))
        entity_type_codes = self.get_entity_type_code_params()

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
