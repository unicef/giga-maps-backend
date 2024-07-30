import re
import logging
from datetime import datetime, time

from django.conf import settings
from django.contrib import messages
from django.core.exceptions import PermissionDenied
from django.db import connection
from django.db.models.functions.text import Lower
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_control
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import mixins, viewsets, status as rest_status
from rest_framework.decorators import action
from rest_framework.filters import SearchFilter
from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.views import APIView

from proco.connection_statistics.models import SchoolWeeklyStatus, SchoolDailyStatus, SchoolRealTimeRegistration
from proco.connection_statistics.utils import get_benchmark_value_for_default_download_layer
from proco.core import mixins as core_mixins
from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.locations.backends.csv import SchoolsCSVWriterBackend
from proco.locations.models import Country
from proco.schools.models import School, FileImport
from proco.schools.serializers import (
    CSVSchoolsListSerializer,
    SchoolPointSerializer,
    SchoolListSerializer,
    SchoolStatusSerializer,
    SchoolCSVSerializer,
    SchoolUpdateRetrieveSerializer,
    ListSchoolSerializer,
    DetailSchoolSerializer,
    ImportCSVSerializer,
)
from proco.schools.tasks import process_loaded_file
from proco.utils import dates as date_utilities
from proco.utils.error_message import id_missing_error_mess, delete_succ_mess, \
    error_mess
from proco.utils.log import action_log, changed_fields
from proco.utils.mixins import CachedListMixin

logger = logging.getLogger('gigamaps.' + __name__)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class SchoolsViewSet(
    CachedListMixin,
    mixins.RetrieveModelMixin,
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    LIST_CACHE_KEY_PREFIX = 'SCHOOLS'

    queryset = School.objects.all().select_related('last_weekly_status')
    pagination_class = None
    serializer_class = ListSchoolSerializer
    filter_backends = (
        DjangoFilterBackend,
    )
    related_model = Country

    def get_serializer(self, *args, **kwargs):
        kwargs['country'] = self.get_country()
        return super(SchoolsViewSet, self).get_serializer(*args, **kwargs)

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
                Country.objects.defer(
                    'geometry', 'geometry_simplified',
                ).select_related('last_weekly_status').annotate(code_lower=Lower('code')),
                code_lower=self.kwargs.get('country_code').lower(),
            )
        return self._country

    def get_queryset(self):
        return super().get_queryset().filter(country=self.get_country())

    def get_serializer_class(self):
        serializer_class = self.serializer_class
        if self.action == 'list':
            serializer_class = ListSchoolSerializer
        if self.action == 'export_csv_schools':
            serializer_class = CSVSchoolsListSerializer
        return serializer_class

    @action(methods=['get'], detail=False, url_path='export-csv-schools', url_name='export_csv_schools')
    def export_csv_schools(self, request, *args, **kwargs):
        country = self.get_country()
        serializer = self.get_serializer(self.get_queryset(), many=True)
        csvwriter = SchoolsCSVWriterBackend(serializer, country)
        response = csvwriter.write()
        return response


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class RandomSchoolsListAPIView(CachedListMixin, ListAPIView):
    LIST_CACHE_KEY_PREFIX = 'RANDOM_SCHOOLS'

    queryset = School.objects.order_by('?')[:settings.RANDOM_SCHOOLS_DEFAULT_AMOUNT]
    serializer_class = SchoolPointSerializer
    pagination_class = None

    def get_serializer(self, *args, **kwargs):
        countries_statuses = Country.objects.all().defer('geometry', 'geometry_simplified').select_related(
            'last_weekly_status',
        ).values_list(
            'id', 'last_weekly_status__integration_status',
        )
        kwargs['countries_statuses'] = dict(countries_statuses)
        return super(RandomSchoolsListAPIView, self).get_serializer(*args, **kwargs)


class BaseTileGenerator:
    def path_to_tile(self, request):
        path = "/" + request.query_params.get('z') + "/" + request.query_params.get(
            'x') + "/" + request.query_params.get('y')

        if m := re.search(r'^\/(\d+)\/(\d+)\/(\d+)\.(\w+)', path):
            return {'zoom': int(m[1]), 'x': int(m[2]), 'y': int(m[3]), 'format': m[4]}
        return None

    def tile_is_valid(self, tile):
        if 'x' not in tile or 'y' not in tile or 'zoom' not in tile:
            return False
        if 'format' not in tile or tile['format'] not in ['pbf', 'mvt']:
            return False

        size = 2 ** tile['zoom']
        if tile['x'] >= size or tile['y'] >= size:
            return False
        return tile['x'] >= 0 and tile['y'] >= 0

    def tile_to_envelope(self, tile):
        # Width of world in EPSG:3857
        worldMercMax = 20037508.3427892
        worldMercMin = -1 * worldMercMax
        worldMercSize = worldMercMax - worldMercMin
        # Width in tiles
        worldTileSize = 2 ** tile['zoom']
        # Tile width in EPSG:3857
        tileMercSize = worldMercSize / worldTileSize
        # Calculate geographic bounds from tile coordinates
        # XYZ tile coordinates are in "image space" so origin is
        # top-left, not bottom right
        return {
            'xmin': worldMercMin + tileMercSize * tile['x'],
            'xmax': worldMercMin + tileMercSize * (tile['x'] + 1),
            'ymin': worldMercMax - tileMercSize * (tile['y'] + 1),
            'ymax': worldMercMax - tileMercSize * (tile['y']),
        }

    def envelope_to_bounds_sql(self, env):
        DENSIFY_FACTOR = 4
        env['segSize'] = (env['xmax'] - env['xmin']) / DENSIFY_FACTOR
        sql_tmpl = 'ST_Segmentize(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 3857),{segSize})'
        return sql_tmpl.format(**env)

    def envelope_to_sql(self, env, request):
        raise NotImplementedError("envelope_to_sql must be implemented in the subclass.")

    def sql_to_pbf(self, sql):
        try:
            with connection.cursor() as cur:
                cur.execute(sql)
                if not cur:
                    return Response({"error": f"sql query failed: {sql}"}, status=404)
                return cur.fetchone()[0]
        except Exception:
            return Response({"error": "An error occurred while executing SQL query"}, status=500)

    def generate_tile(self, request):
        tile = self.path_to_tile(request)
        if not (tile and self.tile_is_valid(tile)):
            return Response({"error": "Invalid tile path"}, status=400)

        env = self.tile_to_envelope(tile)

        sql = self.envelope_to_sql(env, request)

        logger.debug(sql.replace('\n', ''))

        pbf = self.sql_to_pbf(sql)
        if isinstance(pbf, memoryview):
            response = HttpResponse(pbf.tobytes(), content_type="application/vnd.mapbox-vector-tile")
            response["Access-Control-Allow-Origin"] = "*"
            return response
        return pbf


class SchoolTileGenerator(BaseTileGenerator):
    def __init__(self, table_config):
        super().__init__()
        self.table_config = table_config

    def envelope_to_sql(self, env, request):
        country_id = request.query_params.get('country_id', None)
        admin1_id = request.query_params.get('admin1_id', None)

        tbl = self.table_config.copy()
        tbl['env'] = self.envelope_to_bounds_sql(env)

        tbl['limit_condition'] = 'LIMIT ' + str(int(request.query_params.get('limit', '50000')))
        tbl['country_condition'] = ''
        tbl['admin1_condition'] = ''
        tbl['random_order'] = ''

        if country_id or admin1_id:
            if admin1_id:
                tbl['admin1_condition'] = f"AND schools_school.admin1_id = {admin1_id}"

            if country_id:
                tbl['country_condition'] = f"AND schools_school.country_id = {country_id}"
        else:
            tbl['random_order'] = 'ORDER BY random()' if int(request.query_params.get('z', 0)) == 2 else ''

        """In order to cater school requirements, {school_condition} can be added to id before/after country_condition
         in the query"""

        sql_tmpl = """WITH
            bounds AS (
            SELECT {env} AS geom,
                   {env}::box2d AS b2d
            ),
            mvtgeom AS (
            SELECT ST_AsMVTGeom(ST_Transform(schools_school."geopoint", 3857), bounds.b2d) AS geom,
                schools_school."id",
                schools_school."coverage_type",
                CASE WHEN LOWER(schools_school."coverage_type") IN ('5g', '4g') THEN 'good'
                    WHEN LOWER(schools_school."coverage_type") IN ('3g', '2g') THEN 'moderate'
                    WHEN LOWER(schools_school."coverage_type") = 'no' THEN 'bad'
                    ELSE 'unknown'
                END AS coverage_status,
                CASE WHEN schools_school."connectivity_status" IN ('good', 'moderate') THEN 'connected'
                    WHEN schools_school."connectivity_status" = 'no' THEN 'not_connected' ELSE 'unknown'
                END AS connectivity_status
            FROM schools_school
            INNER JOIN bounds ON ST_Intersects(schools_school."geopoint", ST_Transform(bounds.geom, 4326))
            {school_weekly_join}
            WHERE schools_school."deleted" IS NULL
             {country_condition}
             {admin1_condition}
             {school_condition}
             {school_weekly_condition}
             {random_order}
             {limit_condition}
            )
            SELECT ST_AsMVT(DISTINCT mvtgeom.*) FROM mvtgeom
        """

        tbl['school_condition'] = ''
        tbl['school_weekly_join'] = ''
        tbl['school_weekly_condition'] = ''

        school_filters = core_utilities.get_filter_sql(request, 'schools', 'schools_school')
        if len(school_filters) > 0:
            tbl['school_condition'] = 'AND ' + school_filters

        school_static_filters = core_utilities.get_filter_sql(request, 'school_static',
                                                              'connection_statistics_schoolweeklystatus')
        if len(school_static_filters) > 0:
            tbl['school_weekly_join'] = """
            LEFT OUTER JOIN connection_statistics_schoolweeklystatus
                ON schools_school."last_weekly_status_id" = connection_statistics_schoolweeklystatus."id"
            """
            tbl['school_weekly_condition'] = 'AND ' + school_static_filters

        return sql_tmpl.format(**tbl)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class SchoolTileRequestHandler(APIView):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        table_config = {
            'table': 'schools_school',
            'srid': '4326',
            'geomColumn': 'geopoint',
            'attrColumns': 'id',
        }
        self.tile_generator = SchoolTileGenerator(table_config)

    def get(self, request):
        try:
            return self.tile_generator.generate_tile(request)
        except Exception as ex:
            logger.error('Exception occurred for school tiles endpoint: {}'.format(ex))
            return Response({"error": "An error occurred while processing the request"}, status=500)


class ConnectivityTileGenerator(BaseTileGenerator):
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
            if 'country_id' in request.query_params:
                table_configs[
                    'country_condition'] = f" AND schools_school.country_id = {request.query_params['country_id']}"
            elif 'country_id__in' in request.query_params:
                country_ids = ','.join([c.strip() for c in request.query_params['country_id__in'].split(',')])
                table_configs['country_condition'] = f" AND schools_school.country_id IN ({country_ids})"

            if 'admin1_id' in request.query_params:
                table_configs[
                    'admin1_condition'] = f" AND schools_school.admin1_id = {request.query_params['admin1_id']}"
            elif 'admin1_id__in' in request.query_params:
                admin1_ids = ','.join([c.strip() for c in request.query_params['admin1_id__in'].split(',')])
                table_configs['admin1_condition'] = f" AND schools_school.admin1_id IN ({admin1_ids})"

            if 'school_id' in request.query_params:
                table_configs['school_condition'] = f" AND schools_school.id = {request.query_params['school_id']}"
            elif 'school_id__in' in request.query_params:
                school_ids = ','.join([c.strip() for c in request.query_params['school_id__in'].split(',')])
                table_configs['school_condition'] = f" AND schools_school.id IN ({school_ids})"
        else:
            zoom_level = int(request.query_params.get('z', '0'))
            if zoom_level == 0:
                table_configs['limit_condition'] = 'LIMIT ' + '90000'
            elif zoom_level == 1:
                table_configs['limit_condition'] = 'LIMIT ' + '30000'

            table_configs['random_order'] = 'ORDER BY random()'

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
        tbl['rt_date_condition'] = ''

        self.query_filters(request, tbl)

        """sql with join and connectivity_speed"""
        sql_tmpl = """
            WITH bounds AS (
                SELECT {env} AS geom,
                {env}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT ST_AsMVTGeom(ST_Transform(schools_school.geopoint, 3857), bounds.b2d) AS geom,
                schools_school.id,
                CASE WHEN c.id is NULL AND rt_status.rt_registered = True {rt_date_condition} THEN 'unknown'
                    WHEN c.id is NULL THEN NULL
                    WHEN c.connectivity_speed >  {benchmark} THEN 'good'
                    WHEN c.connectivity_speed <= {benchmark} and c.connectivity_speed >= 1000000 THEN 'moderate'
                    WHEN c.connectivity_speed < 1000000  THEN 'bad'
                    ELSE 'unknown'
                END as connectivity,
                CASE WHEN schools_school.connectivity_status IN ('good', 'moderate') THEN 'connected'
                    WHEN schools_school.connectivity_status = 'no' THEN 'not_connected'
                    ELSE 'unknown'
                END as connectivity_status,
                CASE WHEN rt_status.rt_registered = True {rt_date_condition} THEN True
                    ELSE False
                END as is_rt_connected
                FROM schools_school
                INNER JOIN bounds ON ST_Intersects(schools_school.geopoint, ST_Transform(bounds.geom, {srid}))
                    AND schools_school."deleted" IS NULL {country_condition}{admin1_condition}{school_condition}
                {school_weekly_join}
                LEFT JOIN connection_statistics_schoolweeklystatus c {weekly_lookup_condition}
                    AND c."deleted" IS NULL
                LEFT JOIN connection_statistics_schoolrealtimeregistration rt_status ON rt_status.school_id = schools_school.id
                    AND rt_status."deleted" IS NULL
                {school_weekly_condition}
                {random_order}
                {limit_condition}
            )
            SELECT ST_AsMVT(DISTINCT mvtgeom.*) FROM mvtgeom;
        """

        tbl['school_weekly_join'] = ''
        tbl['school_weekly_condition'] = ''

        school_filters = core_utilities.get_filter_sql(request, 'schools', 'schools_school')
        if len(school_filters) > 0:
            tbl['school_condition'] += ' AND ' + school_filters

        school_static_filters = core_utilities.get_filter_sql(request, 'school_static',
                                                              'connection_statistics_schoolweeklystatus')
        if len(school_static_filters) > 0:
            tbl['school_weekly_join'] = """
                    LEFT OUTER JOIN connection_statistics_schoolweeklystatus
                        ON schools_school."last_weekly_status_id" = connection_statistics_schoolweeklystatus."id"
                    """
            tbl['school_weekly_condition'] = 'WHERE ' + school_static_filters

        return sql_tmpl.format(**tbl)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class ConnectivityTileRequestHandler(APIView):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        table_config = {
            'table': 'schools_school',
            'srid': '4326',
            'geomColumn': 'geopoint',
            'attrColumns': 'id',
        }
        self.tile_generator = ConnectivityTileGenerator(table_config)

    def get(self, request):
        try:
            return self.tile_generator.generate_tile(request)
        except Exception as ex:
            logger.error('Exception occurred for school connectivity tiles endpoint: {}'.format(ex))
            return Response({'error': 'An error occurred while processing the request'}, status=500)


class DownloadSchoolsViewSet(BaseModelViewSet, core_mixins.DownloadAPIDataToCSVMixin):
    model = School
    queryset = School.objects.all().select_related('last_weekly_status')
    serializer_class = SchoolCSVSerializer

    base_auth_permissions = (
        core_permissions.IsUserAuthenticated,
    )
    related_model = Country

    filter_backends = (
        DjangoFilterBackend,
        # NullsAlwaysLastOrderingFilter,
    )

    ordering_field_names = ['name']
    apply_query_pagination = True

    filterset_fields = {
        'id': ['exact', 'in'],
        'country_id': ['exact', 'in'],
        'admin1_id': ['exact', 'in'],
        'giga_id_school': ['exact', 'in'],
        'external_id': ['exact', 'in'],
    }

    permit_list_expands = ['country', 'last_weekly_status', 'admin1', 'admin2']

    def list(self, request, *args, **kwargs):
        if core_utilities.is_export(request, self.action):
            return self.list_export(request, *args, **kwargs)
        else:
            self.perform_pre_checks(request, *args, **kwargs)
            self.serializer_class = SchoolStatusSerializer
            return super().list(request, *args, **kwargs)


class AdminViewSchoolAPIViewSet(BaseModelViewSet):
    model = School
    serializer_class = SchoolPointSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanAddSchool,
        core_permissions.CanUpdateSchool,
        core_permissions.CanViewSchool,
        core_permissions.CanDeleteSchool,
    )

    filter_backends = (
        SearchFilter, DjangoFilterBackend,
    )

    ordering_field_names = ['country', 'name']
    apply_query_pagination = True
    search_fields = ('=school_type', '=environment', 'name', 'country__name', '=giga_id_school', '=external_id')

    filterset_fields = {
        'country_id': ['exact', 'in'],
        'name': ['exact', 'in'],
        'location__name': ['exact', 'in'],
    }

    def get_serializer_class(self):
        if self.action == 'list':
            ser_class = SchoolListSerializer
        elif self.action in ['create', 'update', 'destroy']:
            ser_class = SchoolUpdateRetrieveSerializer
        else:
            ser_class = DetailSchoolSerializer
        return (ser_class)

    def get_object(self):
        if self.action == 'list':
            return get_object_or_404(
                self.queryset.filter(pk=self.kwargs.get('pk').lower())
            )

    def get_queryset(self):
        """
        Return active records
        :return queryset:
        """
        qs = super().get_queryset()
        return qs.prefetch_related('country').defer('location')

    def create(self, request, *args, **kwargs):
        try:
            data = SchoolUpdateRetrieveSerializer(data=request.data)
            if data.is_valid():
                data.save()
                action_log(request, [data.data], 1, '', self.model, field_name='name')
                return Response(data.data)
            return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
        except School.DoesNotExist:
            return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def update(self, request, pk):
        if pk is not None:
            try:
                school = School.objects.get(id=pk)
                data = SchoolUpdateRetrieveSerializer(instance=school, data=request.data)
                if data.is_valid():
                    change_message = changed_fields(school, request.data)
                    action_log(request, [school], 2, change_message, self.model, field_name='name')
                    data.save()
                    return Response(data.data)
                return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
            except School.DoesNotExist:
                return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)
        return Response(data=id_missing_error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def retrieve(self, request, pk):
        if pk is not None:
            try:
                school = School.objects.get(id=pk)
                if school:
                    serializer = SchoolUpdateRetrieveSerializer(school, partial=True,
                                                               context={'request': request}, )
                    return Response(serializer.data)
                return Response(status=rest_status.HTTP_404_NOT_FOUND, data=error_mess)
            except School.DoesNotExist:
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
        return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)

    def destroy(self, request, *args, **kwargs):
        try:
            pk_ids = request.data.get('id', [])
            if len(pk_ids) > 0:
                if School.objects.filter(id__in=pk_ids).exists():
                    schools = School.objects.filter(id__in=pk_ids)
                    action_log(request, schools, 3, "Country deleted", self.model, field_name='name')

                    for school in schools:
                        school.delete()

                        SchoolWeeklyStatus.objects.filter(school=school).update(
                            deleted=core_utilities.get_current_datetime_object())

                        SchoolDailyStatus.objects.filter(school=school).update(
                            deleted=core_utilities.get_current_datetime_object())

                        SchoolRealTimeRegistration.objects.filter(school=school).update(
                            deleted=core_utilities.get_current_datetime_object())
                    return Response(status=rest_status.HTTP_200_OK, data=delete_succ_mess)
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)
        except:
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)


class ImportCSVViewSet(BaseModelViewSet):
    model = FileImport
    serializer_class = ImportCSVSerializer

    search_fields = ('=country__id', 'status',)
    filter_backends = (DjangoFilterBackend,)

    filterset_fields = {
        'id': ['exact', 'in'],
        'country_id': ['exact', 'in'],
        'status': ['exact', 'in'],
    }

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanImportCSV,
        core_permissions.CanDeleteCSV,
        core_permissions.CanViewCSV,
    )

    ordering_field_names = ['-id']
    apply_query_pagination = True

    def get_queryset(self):
        from django.db.models import Q
        queryset = super(ImportCSVViewSet, self).get_queryset()
        if self.action == 'list' and self.request.query_params.get('search'):
            queryset = queryset.filter(
                Q(country__name=self.request.query_params.get('search')) |
                Q(status__icontains=self.request.query_params.get('search'))
            )
        return queryset

    @action(methods=['post'], detail=False, url_path='import_csv_data', url_name='import_csv_data')
    def fileimport(self, request, *args, **kwargs):
        if request.method == 'POST':
            imported_file = FileImport.objects.create(
                uploaded_file=request.FILES['uploaded_file'], uploaded_by=request.user,
            )
            action_log(request, [imported_file], 1, '', self.model, field_name='uploaded_file')
            process_loaded_file.delay(imported_file.id, force=request.POST['force'])

            messages.success(request, 'Your file was uploaded and will be processed soon.')
            return HttpResponse(imported_file)

        raise PermissionDenied()
