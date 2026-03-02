import logging
from collections import OrderedDict

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

from proco.locations.api import BaseSearchMixin
from proco.locations.search_indexes import EntityIndex
from proco.utils.cache import cache_manager, custom_cache_control

from proco.entities.models import Entity, EntityType
from proco.entities.serializers import ListEntitySerializer
from proco.locations.models import Country
from proco.schools.api import ConnectivityTileRequestHandler, BaseTileGenerator, ConnectivityTileGenerator
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
            request, 'entity_static', 'entities_health_entity')
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

    ENTITY_CONFIG = {
        "health": {
            "table": "entities_entity",
            "srid": "4326",
            "geomColumn": "geopoint",
            "attrColumns": "id",
            "cache_prefix": "HEALTH_STATUS_CONNECTIVITY_TILES_MAP",
            "master_data_table": "entities_health_entity"
        },
        "school": {
            "table": "entities_entity",
            "srid": "4326",
            "geomColumn": "geopoint",
            "attrColumns": "id",
            "cache_prefix": "SCHOOL_STATUS_CONNECTIVITY_TILES_MAP",
            "master_data_table": "entities_school_entity"
        }
    }

    def dispatch(self, request, *args, **kwargs):
        entity = request.GET.get("entity_type")
        if not entity:
            return JsonResponse({"error": "Missing required query param: entity_type"}, status=400)
        entity_config = self.ENTITY_CONFIG.get(entity)
        if not entity_config:
            return JsonResponse({"error": f"Invalid entity_type: {entity}. Allowed: {', '.join(self.ENTITY_CONFIG)}"}, status=400)

        table_config = {
            "entity": entity,
            "table": entity_config["table"],
            "srid": entity_config["srid"],
            "geomColumn": entity_config["geomColumn"],
            "attrColumns": entity_config["attrColumns"],
            "master_data_table": entity_config["master_data_table"]
        }

        self.tile_generator = EntityStatusConnectivityTileGenerator(table_config)

        self.CACHE_KEY_PREFIX = entity_config["cache_prefix"]

        return super().dispatch(request, *args, **kwargs)

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)

        entity = params.get('entity_type')
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
    index_class = EntityIndex

    base_auth_permissions = (
        permissions.AllowAny,
    )

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

    def list(self, request, *args, **kwargs):
        resp_data = OrderedDict()
        data = self.index_search(request, *args, **kwargs)
        counts = data.get_count()
        resp_data['count'] = counts
        resp_data['results'] = list(data)
        return Response(resp_data)
