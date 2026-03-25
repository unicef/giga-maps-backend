import logging
from collections import OrderedDict
from datetime import datetime, time

from django.conf import settings
from django.db import models
from django.http import JsonResponse
from django.db.models.functions.text import Lower
from azure.search.documents.indexes.models import SearchFieldDataType
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_control
from rest_framework.generics import ListAPIView
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import mixins, permissions, viewsets, status as rest_status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.utils.urls import remove_query_param

from proco.utils import dates as date_utilities
from proco.connection_statistics.models import SchoolWeeklyStatus
from proco.connection_statistics.utils import get_benchmark_value_for_default_download_layer
from proco.entities.constants import LEGACY_MODEL, ALL_ENTITIES
from proco.locations.api import BaseSearchMixin
from proco.locations.search_indexes import EntityIndex, SchoolIndex
from proco.utils.cache import cache_manager, custom_cache_control

from proco.entities.models import Entity, EntityType
from proco.entities.serializers import ListEntitySerializer
from proco.locations.models import Country
from proco.schools.api import ConnectivityTileRequestHandler, BaseTileGenerator, ConnectivityTileGenerator, \
    SchoolStatusConnectivityTileGenerator
from proco.utils.cache import custom_cache_control
from proco.utils.mixins import CachedListMixin
from proco.core import utils as core_utilities

logger = logging.getLogger('gigamaps.' + __name__)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class EntitiesViewSet(
    CachedListMixin,
    mixins.RetrieveModelMixin,
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    LIST_CACHE_KEY_PREFIX = 'ENTITIES'

    queryset = Entity.objects.all().select_related('last_weekly_status')
    pagination_class = None
    serializer_class = ListEntitySerializer
    filter_backends = (
        DjangoFilterBackend,
    )
    related_model = Country

    def get_serializer(self, *args, **kwargs):
        kwargs['country'] = self.get_country()
        return super(EntitiesViewSet, self).get_serializer(*args, **kwargs)

    def get_list_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}_{2}'.format(
            getattr(self.__class__, 'LIST_CACHE_KEY_PREFIX', self.__class__.__name__) or self.__class__.__name__,
            self.kwargs['country_code'].lower(),
            '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))),
        )

    def get_country(self):
        if not hasattr(self, '_country'):
            self._country = get_object_or_404(
                Country.objects.defer('geometry').select_related('last_weekly_status').annotate(
                    code_lower=Lower('code'),
                ),
                code_lower=self.kwargs.get('country_code').lower(),
            )
        return self._country

    def get_queryset(self):
        return super().get_queryset().filter(country=self.get_country())


class EntityStatusConnectivityTileGenerator(BaseTileGenerator):
    def __init__(self, table_config):
        super().__init__()
        self.table_config = table_config

    def update_kwargs(self, request, table_configs):
        query_params = request.query_params.dict()
        query_param_keys = query_params.keys()

        if 'country_id' in query_param_keys:
            table_configs['country_ids'] = [query_params['country_id'], ]
        elif 'country_id__in' in query_param_keys:
            table_configs['country_ids'] = [c_id.strip() for c_id in query_params['country_id__in'].split(',')]

        if 'admin1_id' in query_param_keys:
            table_configs['admin1_ids'] = [query_params['admin1_id']]
        elif 'admin1_id__in' in query_param_keys:
            table_configs['admin1_ids'] = [a_id.strip() for a_id in query_params['admin1_id__in'].split(',')]

        if 'entity_id' in query_param_keys:
            table_configs['entity_ids'] = [str(query_params['entity_id']).strip()]
        elif 'entity_id__in' in query_param_keys:
            table_configs['entity_ids'] = [s_id.strip() for s_id in query_params['entity_id__in'].split(',')]

        if 'entity_type' in query_param_keys:
            table_configs['entity_types'] = [query_params['entity_type'], ]
        elif 'entity_type__in' in query_param_keys:
            table_configs['entity_types'] = [c_id.strip() for c_id in query_params['entity_type__in'].split(',')]

        table_configs['entity_filters'] = core_utilities.get_filter_sql(
            request, 'entity', 'entities_entity')
        table_configs['entity_static_filters'] = core_utilities.get_filter_sql(
            request, 'entity_static', table_configs['master_data_table'])
        table_configs['entity_real_time_filters'] = core_utilities.get_filter_sql(
            request, 'entity_real_time', 'connection_statistics_entityweeklystatus')

    def envelope_to_sql(self, env, request):
        tbl = self.table_config.copy()
        tbl['env'] = self.envelope_to_bounds_sql(env)

        tbl['limit_condition'] = ''
        tbl['country_condition'] = ''
        tbl['admin1_condition'] = ''
        tbl['entity_condition'] = ''
        tbl['random_order'] = ''

        self.update_kwargs(request, tbl)

        """sql with join and connectivity_speed"""
        sql_tmpl = """
            WITH bounds AS (
                SELECT {env} AS geom,
                {env}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT ST_AsMVTGeom(ST_Transform(entities_entity.geopoint, 3857), bounds.b2d) AS geom,
                entities_entity.id,
                CASE WHEN entities_entity.connectivity_status IN ('good', 'moderate') THEN 'connected'
                    WHEN entities_entity.connectivity_status = 'no' THEN 'not_connected'
                    ELSE 'unknown'
                END AS connectivity_status
                FROM entities_entity
                INNER JOIN entities_entity_type ON entities_entity.entity_type_id = entities_entity_type.id
                INNER JOIN bounds ON ST_Intersects(entities_entity.geopoint, ST_Transform(bounds.geom, {srid}))
                {entity_weekly_join}
                {entity_master_join}
                WHERE entities_entity."deleted" IS NULL
                    AND entities_entity_type.code = '{entity}'
                    {country_condition}
                    {admin1_condition}
                    {entity_condition}
                    {entity_weekly_condition}
                    {entity_master_condition}
                    {random_order}
                    {limit_condition}
            )
            SELECT ST_AsMVT(DISTINCT mvtgeom.*) FROM mvtgeom;
        """

        tbl['entity_weekly_join'] = ''
        tbl['entity_weekly_condition'] = ''
        tbl['entity_master_join'] = ''
        tbl['entity_master_condition'] = ''

        add_random_condition = True

        if len(tbl.get('entity_ids', [])) > 0:
            add_random_condition = False
            tbl['entity_condition'] = 'AND entities_entity."id" IN ({0})'.format(
            ','.join([str(entity_id) for entity_id in tbl['entity_ids']])
            )

        elif len(tbl.get('admin1_ids', [])) > 0:
            if settings.ADMIN_MAP_API_SAMPLING_LIMIT:
                tbl['MAP_API_SAMPLING_LIMIT'] = settings.ADMIN_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            tbl['admin1_condition'] = 'AND entities_entity."admin1_id" IN ({0})'.format(
                ','.join([str(admin1_id) for admin1_id in tbl['admin1_ids']])
            )

        elif len(tbl.get('country_ids', [])) > 0:
            if settings.COUNTRY_MAP_API_SAMPLING_LIMIT:
                tbl['MAP_API_SAMPLING_LIMIT'] = settings.COUNTRY_MAP_API_SAMPLING_LIMIT
                add_random_condition = True
            else:
                add_random_condition = False

            tbl['country_condition'] = 'AND entities_entity."country_id" IN ({0})'.format(
                ','.join([str(country_id) for country_id in tbl['country_ids']])
            )

        if len(tbl['entity_filters']) > 0:
            tbl['entity_condition'] += ' AND ' + tbl['entity_filters']

        if len(tbl['entity_real_time_filters']) > 0:
            tbl['entity_weekly_join'] = """
            INNER JOIN "connection_statistics_entityweeklystatus"
                ON entities_entity."last_weekly_status_id" = connection_statistics_entityweeklystatus."id"
            """

            tbl['entity_weekly_condition'] = ' AND ' + tbl['entity_real_time_filters']

        if len(tbl['entity_static_filters']) > 0:
            tbl['entity_master_join'] = """
            INNER JOIN "{master_data_table}"
                ON entities_entity."last_master_status_id" = {master_data_table}."id"
            """.format(master_data_table=tbl['master_data_table'])

            tbl['entity_master_condition'] = ' AND ' + tbl['entity_static_filters']

        if add_random_condition:
            if 'limit' in request.query_params:
                limit = request.query_params['limit']
                tbl['random_order'] = 'ORDER BY random()' if int(request.query_params.get('z', '0')) == 2 else ''
            elif tbl.get('MAP_API_SAMPLING_LIMIT'):
                limit = tbl['MAP_API_SAMPLING_LIMIT']
                tbl['random_order'] = 'ORDER BY random()'
            else:
                limit = '50000'
                tbl['random_order'] = 'ORDER BY random()' if int(request.query_params.get('z', '0')) == 2 else ''

            tbl['limit_condition'] = 'LIMIT ' + str(limit)
        return sql_tmpl.format(**tbl)


@method_decorator([
    custom_cache_control(
        public=True,
        max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE,
        cache_status_codes=[rest_status.HTTP_200_OK,],
    )
], name='dispatch')
class EntityConnectivityTileRequestHandler(APIView):
    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'ENTITY_CONNECTIVITY_TILES_MAP'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        table_config = {
            'table': 'entities_entity',
            'srid': '4326',
            'geomColumn': 'geopoint',
            'attrColumns': 'id',
        }
        self.tile_generator = EntityStatusConnectivityTileGenerator(table_config)

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)

        entity = params.get('entity_type')
        param_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))

        return f"{self.CACHE_KEY_PREFIX}_{entity}_tiles_{param_string}"

    def get(self, request, *args, **kwargs):
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key()

        response = None
        if use_cached_data:
            response = cache_manager.get(cache_key)

        if not response:
            try:
                response = self.tile_generator.generate_tile(request)

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
        cache_status_codes=[rest_status.HTTP_200_OK],
    )
], name='dispatch')
class EntityConnectivityStatusTileRequestHandler(EntityConnectivityTileRequestHandler):

    def dispatch(self, request, *args, **kwargs):
        entity = request.GET.get("entity_type__code")
        if not entity:
            return JsonResponse({"error": "Missing required query param: entity_type"}, status=400)
        try:
            entity_type = EntityType.objects.get(code=entity)
        except EntityType.DoesNotExist:
            return JsonResponse(
                {"error": f"Invalid entity_type__code: {entity}"},
                status=400
            )

        extra_config = entity_type.extra_config or {}

        table_config = {
            "entity": entity_type.code,
            "table": extra_config.get("main_table"),
            "srid": extra_config.get("srid"),
            "master_data_table": entity_type.detail_related_name
        }

        if entity_type.code == LEGACY_MODEL:
            self.tile_generator = SchoolStatusConnectivityTileGenerator(table_config)
        else:
            self.tile_generator = EntityStatusConnectivityTileGenerator(table_config)

        self.CACHE_KEY_PREFIX = extra_config.get("tile_cache_prefix")

        return super().dispatch(request, *args, **kwargs)

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)

        entity = params.get('entity_type__code')
        param_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))

        return f"{self.CACHE_KEY_PREFIX}_{entity}_tiles_{param_string}"


class EntityTypeListAPIView(APIView):
    def get(self, request):
        active_types = EntityType.get_all_active()

        response_data = {}

        for et in active_types:
            response_data[et.code] = {
                'id': et.id,
                'code': et.code,
                'name': et.name,
                'description': et.description,
                'display_order': et.display_order,
                'is_legacy': et.is_legacy,
                'extra_config': et.extra_config or {},
            }

        return Response(response_data, status=200)


class AggregateSearchEntityViewSet(BaseSearchMixin, ListAPIView):
    """
    AggregateSearchViewSet
        Endpoint to use the Cognitive search index.
        Inherits: BaseSearchMixin, ListAPIView
    """

    base_auth_permissions = (
        permissions.AllowAny,
    )

    default_index_class = EntityIndex
    school_index_class = SchoolIndex

    filterset_fields = {
        'id': ['exact', 'in', 'notexact'],
        'country_id': ['exact', 'in', 'notexact'],
        'admin1_id': ['exact', 'in', 'notexact'],
        'admin2_id': ['exact', 'in', 'notexact'],
        'admin1_name': ['exact', 'in', 'notexact'],
        'admin2_name': ['exact', 'in', 'notexact'],
        'entity_type_code':['exact', 'in']
    }

    filter_field_type = {
        'id': SearchFieldDataType.Int64,
        'country_id': SearchFieldDataType.Int64,
        'admin1_id': SearchFieldDataType.Int64,
        'admin2_id': SearchFieldDataType.Int64,
        'entity_type_code': SearchFieldDataType.String,
    }

    def get_index_class(self):
        entity_type = self.request.query_params.get("entity_type__code")

        if entity_type == LEGACY_MODEL:
            return self.school_index_class

        return self.default_index_class

    def sanitize_fields(self, request):
        """
        Remove fields not supported by selected index
        """
        fields = request.query_params.get("fields")

        if not fields:
            return

        fields_list = fields.split(",")

        # If SchoolIndex → remove entity_type_code
        if self.index_class == self.school_index_class:
            fields_list = [
                f for f in fields_list
                if f != "entity_type_code"
            ]

        mutable_querydict = request.query_params.copy()
        mutable_querydict["fields"] = ",".join(fields_list)
        request._request.GET = mutable_querydict

    def sanitize_search_fields(self, request):
        search_fields = request.query_params.get("search_fields")

        if not search_fields:
            return

        fields_list = search_fields.split(",")

        if self.index_class == self.school_index_class:
            fields_list = [
                "giga_id_school" if f == "giga_id" else f
                for f in fields_list
            ]

        mutable_querydict = request.query_params.copy()
        mutable_querydict["search_fields"] = ",".join(fields_list)
        request._request.GET = mutable_querydict

    def list(self, request, *args, **kwargs):
        resp_data = OrderedDict()

        entity_type = request.query_params.get("entity_type__code")

        results = []
        total_count = 0
        original_params = request.query_params.copy()
        if entity_type == ALL_ENTITIES:
            index_classes = [self.school_index_class, self.default_index_class]
        else:
            index_classes = [self.get_index_class()]

        for index in index_classes:
            request._request.GET = original_params.copy()

            self.index_class = index

            self.sanitize_fields(request)
            self.sanitize_search_fields(request)

            data = self.index_search(request, *args, **kwargs)

            result_list = list(data)

            if index == self.school_index_class:
                for r in result_list:
                    r["entity_type_code"] = LEGACY_MODEL

            results.extend(result_list)
            total_count += data.get_count()

        resp_data["count"] = total_count
        resp_data["results"] = results

        return Response(resp_data)


class EntityConnectivityTileGenerator(BaseTileGenerator):
    def __init__(self, table_config):
        super().__init__()
        self.table_config = table_config

    def query_filters(self, request, table_configs):
        table_configs['limit_condition'] = 'LIMIT ' + request.query_params.get('limit', '50000')

        if (
            'country_id' in request.query_params or
            'country_id__in' in request.query_params or
            'admin1_id' in request.query_params or
            'admin1_id__in' in request.query_params or
            'school_id' in request.query_params or
            'school_id__in' in request.query_params
        ):
            if 'school_id' in request.query_params:
                table_configs['school_condition'] = f" AND schools_school.id = {request.query_params['school_id']}"
            elif 'school_id__in' in request.query_params:
                school_ids = ','.join([c.strip() for c in request.query_params['school_id__in'].split(',')])
                table_configs['school_condition'] = f" AND schools_school.id IN ({school_ids})"

            elif 'admin1_id' in request.query_params:
                table_configs[
                    'admin1_condition'] = f" AND schools_school.admin1_id = {request.query_params['admin1_id']}"
            elif 'admin1_id__in' in request.query_params:
                admin1_ids = ','.join([c.strip() for c in request.query_params['admin1_id__in'].split(',')])
                table_configs['admin1_condition'] = f" AND schools_school.admin1_id IN ({admin1_ids})"

            elif 'country_id' in request.query_params:
                table_configs[
                    'country_condition'] = f" AND schools_school.country_id = {request.query_params['country_id']}"
            elif 'country_id__in' in request.query_params:
                country_ids = ','.join([c.strip() for c in request.query_params['country_id__in'].split(',')])
                table_configs['country_condition'] = f" AND schools_school.country_id IN ({country_ids})"

        else:
            zoom_level = int(request.query_params.get('z', '0'))
            if zoom_level == 0:
                table_configs['limit_condition'] = 'LIMIT ' + '90000'
            elif zoom_level == 1:
                table_configs['limit_condition'] = 'LIMIT ' + '30000'

            table_configs['random_order'] = 'ORDER BY schools_school.giga_id_school ASC'
            table_configs['entity_random_order'] = 'ORDER BY entities_entity.giga_id ASC'

        if 'is_weekly' in request.query_params:
            is_weekly = request.query_params.get('is_weekly', 'true') == 'true'
            start_date = date_utilities.to_date(request.query_params.get('start_date'),
                                                default=datetime.combine(datetime.now(), time.min))

            end_date = date_utilities.to_date(request.query_params.get('end_date'),
                                              default=datetime.combine(datetime.now(), time.min))
            table_configs['rt_date_condition'] = f" AND rt_status.rt_registration_date <= '{end_date}'"

            month_number = date_utilities.get_month_from_date(start_date)
            year_number = date_utilities.get_year_from_date(start_date)

            if is_weekly:
                # If is_weekly == True, then pick the week number based on start_date
                week_number = date_utilities.get_week_from_date(start_date)
            else:
                # If is_weekly == False, then:
                # 1. Collect dates on all sundays of the given month and year
                # 2. Get the week numbers for all sundays and look into SchoolWeeklyStatus table for which
                # last week number data was created in the given month of the year. And pick this week number
                dates_on_all_sundays = date_utilities.all_days_of_a_month(year_number, month_number,
                                                                          day_name='sunday').keys()
                week_numbers_for_month = [date_utilities.get_week_from_date(date) for date in dates_on_all_sundays]
                week_number = SchoolWeeklyStatus.objects.all().filter(
                    year=year_number, week__in=week_numbers_for_month, ).order_by('-week').values_list(
                    'week', flat=True).first()

                if not week_number:
                    # If for any week of the month data is not available then pick last week number
                    week_number = week_numbers_for_month[-1]

            table_configs['weekly_lookup_condition'] = (f'ON schools_school.id = c.school_id AND c.week={week_number} '
                                                        f'AND c.year={year_number}')

        table_configs['benchmark'], table_configs['benchmark_unit'] = get_benchmark_value_for_default_download_layer(
            request.query_params.get('benchmark', 'global'),
            request.query_params.get('country_id', None)
        )

    def envelope_to_sql(self, env, request):
        tbl = self.table_config.copy()
        tbl['env'] = self.envelope_to_bounds_sql(env)

        tbl['limit_condition'] = ''
        tbl['country_condition'] = ''
        tbl['admin1_condition'] = ''
        tbl['school_condition'] = ''
        tbl['weekly_lookup_condition'] = 'ON schools_school.last_weekly_status_id = c.id'
        tbl['random_order'] = ''
        tbl['entity_random_order'] = ''
        tbl['rt_date_condition'] = ''
        tbl['entity_condition'] = ''
        tbl['entity_weekly_lookup_condition'] = 'ON entities_entity.last_weekly_status_id = c.id'

        self.query_filters(request, tbl)

        """sql with join and connectivity_speed"""
        sql_tmpl = """
            WITH bounds AS (
                SELECT {env} AS geom,
                {env}::box2d AS b2d
            ),
            school_mvtgeom  AS (
                SELECT ST_AsMVTGeom(ST_Transform(schools_school.geopoint, 3857), bounds.b2d) AS geom,
                schools_school.id,
                CASE WHEN c.id is NULL AND rt_status.rt_registered = True {rt_date_condition} THEN 'unknown'
                    WHEN c.id is NULL THEN NULL
                    WHEN c.connectivity_speed >  {benchmark} THEN 'good'
                    WHEN c.connectivity_speed <= {benchmark} and c.connectivity_speed >= 1000000 THEN 'moderate'
                    WHEN c.connectivity_speed < 1000000  THEN 'bad'
                    ELSE 'unknown'
                END AS connectivity,
                CASE WHEN schools_school.connectivity_status IN ('good', 'moderate') THEN 'connected'
                    WHEN schools_school.connectivity_status = 'no' THEN 'not_connected'
                    ELSE 'unknown'
                END AS connectivity_status,
                CASE WHEN rt_status.rt_registered = True {rt_date_condition} THEN True
                    ELSE False
                END AS is_rt_connected
                FROM schools_school
                INNER JOIN bounds ON ST_Intersects(schools_school.geopoint, ST_Transform(bounds.geom, {srid}))
                {school_weekly_join}
                LEFT JOIN connection_statistics_schoolweeklystatus c {weekly_lookup_condition}
                    AND c."deleted" IS NULL
                LEFT JOIN connection_statistics_schoolrealtimeregistration rt_status
                    ON rt_status.school_id = schools_school.id AND rt_status."deleted" IS NULL
                WHERE schools_school."deleted" IS NULL
                    {country_condition}
                    {admin1_condition}
                    {school_condition}
                    {school_weekly_condition}
                {random_order}
                {limit_condition}
            ),
            entity_mvtgeom  AS (
                SELECT ST_AsMVTGeom(ST_Transform(entities_entity.geopoint, 3857), bounds.b2d) AS geom,
                entities_entity.id,
                CASE WHEN c.id is NULL AND rt_status.rt_registered = True {rt_date_condition} THEN 'unknown'
                    WHEN c.id is NULL THEN NULL
                    WHEN c.connectivity_speed >  {benchmark} THEN 'good'
                    WHEN c.connectivity_speed <= {benchmark} and c.connectivity_speed >= 1000000 THEN 'moderate'
                    WHEN c.connectivity_speed < 1000000  THEN 'bad'
                    ELSE 'unknown'
                END AS connectivity,
                CASE WHEN entities_entity.connectivity_status IN ('good', 'moderate') THEN 'connected'
                    WHEN entities_entity.connectivity_status = 'no' THEN 'not_connected'
                    ELSE 'unknown'
                END AS connectivity_status,
                CASE WHEN rt_status.rt_registered = True {rt_date_condition} THEN True
                    ELSE False
                END AS is_rt_connected
                FROM entities_entity
                INNER JOIN bounds ON ST_Intersects(entities_entity.geopoint, ST_Transform(bounds.geom, {srid}))
                {school_weekly_join}
                LEFT JOIN connection_statistics_entityweeklystatus c {entity_weekly_lookup_condition}
                    AND c."deleted" IS NULL
                LEFT JOIN connection_statistics_entityrealtimeregistration rt_status
                    ON rt_status.entity_id = entities_entity.id AND rt_status."deleted" IS NULL
                WHERE entities_entity."deleted" IS NULL
                    {country_condition}
                    {admin1_condition}
                    {entity_condition}
                    {entity_weekly_condition}
                {entity_random_order}
                {limit_condition}
            )

            SELECT
                (
                    SELECT ST_AsMVT(school_mvtgeom, 'schools')
                    FROM school_mvtgeom
                )
                ||
                (
                    SELECT ST_AsMVT(entity_mvtgeom, 'entities')
                    FROM entity_mvtgeom
                );
        """

        tbl['school_weekly_join'] = ''
        tbl['school_weekly_condition'] = ''
        tbl['entity_weekly_join'] = ''
        tbl['entity_weekly_condition'] = ''

        school_filters = core_utilities.get_filter_sql(request, 'schools', 'schools_school')
        entity_filters = core_utilities.get_filter_sql(request, 'entities', 'entities_entity')
        if len(school_filters) > 0:
            tbl['school_condition'] += ' AND ' + school_filters

        if len(entity_filters) > 0:
            tbl['entity_condition'] += ' AND ' + entity_filters

        school_static_filters = core_utilities.get_filter_sql(request, 'school_static',
                                                              'connection_statistics_schoolweeklystatus')
        entity_static_filters = core_utilities.get_filter_sql(request, 'entity_static',
                                                              'connection_statistics_entityweeklystatus')
        if len(school_static_filters) > 0:
            tbl['school_weekly_join'] = """
            LEFT OUTER JOIN connection_statistics_schoolweeklystatus
                ON schools_school."last_weekly_status_id" = connection_statistics_schoolweeklystatus."id"
            """
            tbl['school_weekly_condition'] = 'AND ' + school_static_filters

        if len(entity_static_filters) > 0:
            tbl['entity_weekly_join'] = """
            LEFT OUTER JOIN connection_statistics_entityweeklystatus
                ON entities_entity."last_weekly_status_id" = connection_statistics_entityweeklystatus."id"
            """
            tbl['entity_weekly_condition'] = 'AND ' + entity_static_filters

        return sql_tmpl.format(**tbl)


@method_decorator([
    custom_cache_control(
        public=True,
        max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE,
        cache_status_codes=[rest_status.HTTP_200_OK,],
    )
], name='dispatch')
class EntityGlobalConnectivityTileRequestHandler(APIView):
    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'CONNECTIVITY_GLOBAL_TILES_MAP'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        table_config = {
            'table': 'schools_school',
            'srid': '4326',
            'geomColumn': 'geopoint',
            'attrColumns': 'id',
        }
        self.tile_generator = EntityConnectivityTileGenerator(table_config)


    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}'.format(
            self.CACHE_KEY_PREFIX,
            '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))),
        )

    def get(self, request):
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key()

        response = None
        if use_cached_data:
            response = cache_manager.get(cache_key)

        if not response:
            try:
                response = self.tile_generator.generate_tile(request)

                cache_manager.set(cache_key, response, request_path=request_path,
                                  soft_timeout=settings.CACHE_CONTROL_MAX_AGE)
            except Exception as ex:
                logger.error('Exception occurred for school connectivity tiles endpoint: {}'.format(ex))
                response = Response({'error': 'An error occurred while processing the request'}, status=500)

        return response
