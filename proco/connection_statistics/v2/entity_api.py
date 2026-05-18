from datetime import timedelta, datetime, time

from django.conf import settings
from django.db import connection
from django.db.models import (
    Avg, Case, FilteredRelation, Q, Value, When
)
from django.db.models import Count
from django.db.models import (
    IntegerField
)
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_control
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param
from rest_framework.views import APIView

from proco.accounts.models import DataLayer, DataSource, AdvanceFilter
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.models import SchoolWeeklyStatus, EntityWeeklyStatus, EntityDailyStatus, \
    SchoolDailyStatus
from proco.connection_statistics.utils import get_benchmark_value_for_default_download_layer
from proco.core import utils as core_utilities
from proco.entities.constants import LEGACY_MODEL, LEGACY_MODEL_NAME
from proco.entities.models import EntityType, Entity
from proco.schools.models import School
from proco.utils import dates as date_utilities
from proco.utils.cache import cache_manager


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class EntityGlobalStatsAPIView(APIView):
    permission_classes = (AllowAny,)

    model = LEGACY_MODEL_NAME
    queryset = model.objects.all()

    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'GLOBAL_STATS_ALL_ENTITIES'

    filter_backends = (
        DjangoFilterBackend,
    )

    filterset_fields = {
        'country_id': ['exact', 'in'],
        'admin1_id': ['exact', 'in'],
        'id': ['exact', 'in'],
    }

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}'.format(self.CACHE_KEY_PREFIX,
                                '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))), )

    def filter_queryset(self, queryset):
        """
        Given a queryset, filter it with whichever filter backend is in use.

        You are unlikely to want to override this method, although you may need
        to call it either from a list view, or from a custom `get_object`
        method if you want to apply the configured filtering backend to the
        default queryset.
        """

        for backend in list(self.filter_backends):
            queryset = backend().filter_queryset(self.request, queryset, self)
        return queryset

    def get(self, request, *args, **kwargs):
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key()

        data = None
        if use_cached_data:
            data = cache_manager.get(cache_key)

        if not data:
            data = self.calculate_global_statistic()
            cache_manager.set(cache_key, data, request_path=request_path, soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=data)

    def calculate_school_global_statistic(self):
        # Count the number of schools with known connectivity status (connected, not_connected, or unknown)
        queryset = self.filter_queryset(self.queryset)
        school_connectivity_status_qry = queryset.annotate(
            dummy_group_by=Value(1)).values('dummy_group_by').annotate(
            connected=Count(Case(When(connectivity_status__in=['good', 'moderate'], then='id')), distinct=True),
            not_connected=Count(Case(When(connectivity_status='no', then='id')), distinct=True),
            unknown=Count(Case(When(connectivity_status='unknown', then='id')), distinct=True),
            total_schools=Count('id', distinct=True),
            all_countries=Count('country_id', distinct=True),
            schools_with_connectivity_status_mapped=Count(Case(
                When(connectivity_status__in=['good', 'moderate', 'no'], then='id')), distinct=True),
            countries_with_connectivity_status_mapped=Count(Case(
                When(connectivity_status__in=['good', 'moderate', 'no'], then='country_id')), distinct=True),
        ).values('connected', 'not_connected', 'unknown', 'total_schools',
                 'all_countries', 'schools_with_connectivity_status_mapped',
                 'countries_with_connectivity_status_mapped').order_by()

        school_filters = core_utilities.get_filter_sql(self.request, 'schools', 'schools_school')
        if len(school_filters) > 0:
            school_connectivity_status_qry = school_connectivity_status_qry.extra(where=[school_filters])

        school_static_filters = core_utilities.get_filter_sql(self.request, 'school_static',
                                                              'connection_statistics_schoolweeklystatus')

        if len(school_static_filters) > 0:
            school_connectivity_status_qry = school_connectivity_status_qry.annotate(
                total_weekly_schools=Count('last_weekly_status__school_id', distinct=True),
            ).values('connected', 'not_connected', 'unknown', 'total_schools',
                     'all_countries', 'schools_with_connectivity_status_mapped',
                     'countries_with_connectivity_status_mapped', 'total_weekly_schools')
            school_connectivity_status_qry = school_connectivity_status_qry.extra(where=[school_static_filters])

        giga_connectivity_benchmark, giga_connectivity_benchmark_unit = get_benchmark_value_for_default_download_layer(
            'global', None)

        school_connectivity_status = list(school_connectivity_status_qry)[0]

        return {
            'no_of_countries': school_connectivity_status['all_countries'],
            'countries_with_connectivity_status_mapped': school_connectivity_status[
                'countries_with_connectivity_status_mapped'],
            'entities_connected': school_connectivity_status['total_schools'],
            'entities_with_connectivity_status_mapped': school_connectivity_status[
                'schools_with_connectivity_status_mapped'],
            'connectivity_global_benchmark': {
                'value': giga_connectivity_benchmark,
                'unit': giga_connectivity_benchmark_unit,
            },
            'connected_entities': {
                'connected': school_connectivity_status['connected'],
                'not_connected': school_connectivity_status['not_connected'],
                'unknown': school_connectivity_status['unknown'],
            }
        }

    def calculate_entity_global_statistic(self, entity_type):
        entity_type_obj = EntityType.objects.get(code=entity_type)

        queryset = Entity.objects.filter(entity_type=entity_type_obj)
        queryset = self.filter_queryset(queryset)

        stats_qs = (
            queryset
            .annotate(dummy_group_by=Value(1, output_field=IntegerField()))
            .values('dummy_group_by')
            .annotate(
                connected=Count(
                    Case(
                        When(connectivity_status__in=['good', 'moderate'], then='id')
                    ),
                    distinct=True
                ),
                not_connected=Count(
                    Case(
                        When(connectivity_status='no', then='id')
                    ),
                    distinct=True
                ),
                unknown=Count(
                    Case(
                        When(connectivity_status='unknown', then='id')
                    ),
                    distinct=True
                ),
                total_entities=Count('id', distinct=True),
                all_countries=Count('country_id', distinct=True),
                entities_with_connectivity_status_mapped=Count(
                    Case(
                        When(
                            connectivity_status__in=['good', 'moderate', 'no'],
                            then='id'
                        )
                    ),
                    distinct=True
                ),
                countries_with_connectivity_status_mapped=Count(
                    Case(
                        When(
                            connectivity_status__in=['good', 'moderate', 'no'],
                            then='country_id'
                        )
                    ),
                    distinct=True
                ),
            )
            .order_by()
        )

        entity_filters = core_utilities.get_filter_sql(
            self.request, 'entities', 'entities_entity'
        )
        if len(entity_filters) > 0:
            stats_qs = stats_qs.extra(where=[entity_filters])

        entity_static_filters = core_utilities.get_filter_sql(self.request, 'entity_static',
                                                              'connection_statistics_entityweeklystatus')

        if len(entity_static_filters) > 0:
            stats_qs = stats_qs.annotate(
                total_weekly_schools=Count('last_weekly_status__entity_id', distinct=True),
            ).values('connected', 'not_connected', 'unknown', 'total_entities',
                     'all_countries', 'entities_with_connectivity_status_mapped',
                     'countries_with_connectivity_status_mapped', 'total_weekly_entities')
            stats_qs = stats_qs.extra(where=[entity_static_filters])

        benchmark, unit = get_benchmark_value_for_default_download_layer(
            entity_type, None
        )

        stats = list(stats_qs)[0]

        return {
            'no_of_countries': stats['all_countries'],
            'countries_with_connectivity_status_mapped':
                stats['countries_with_connectivity_status_mapped'],
            'entities_total': stats['total_entities'],
            'entities_with_connectivity_status_mapped':
                stats['entities_with_connectivity_status_mapped'],
            'connectivity_global_benchmark': {
                'value': benchmark,
                'unit': unit,
            },
            'connected_entities': {
                'connected': stats['connected'],
                'not_connected': stats['not_connected'],
                'unknown': stats['unknown'],
            }
        }

    def calculate_global_statistic(self):
        response = {}

        # ✅ School → OLD code
        response[LEGACY_MODEL] = self.calculate_school_global_statistic()

        # ✅ Other entities → NEW code
        entity_types = EntityType.get_all_active().exclude(is_legacy=True)

        for entity_type in entity_types:
            response[entity_type.code] = self.calculate_entity_global_statistic(
                entity_type.code
            )

        return response


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class EntityConnectivityAPIView(APIView):
    permission_classes = (AllowAny,)

    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'CONNECTIVITY_STATS_ALL_ENTITIES'

    school_filters = []
    school_static_filters = []

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}'.format(self.CACHE_KEY_PREFIX,
                                '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))), )

    def get(self, request, *args, **kwargs):
        entity_type = request.query_params.get("entity_type__code")

        if entity_type == LEGACY_MODEL:
            self.queryset = School.objects.all()
            return Response(self.get_school_data(request))
        if entity_type:
            self.queryset = Entity.objects.all()
            return Response(self.get_entity_data(request, entity_type))

        response = {}

        # School - utilising old code
        self.queryset = School.objects.all()
        response["school"] = self.get_school_data(request)

        # For the entities other than school
        entity_types = EntityType.get_all_active().exclude(is_legacy=True)
        self.queryset = Entity.objects.all()
        for et in entity_types:
            response[et.code] = self.get_entity_data(request, et.code)

        return Response(response)

    def calculate_country_download_data(self, start_date, end_date, week_number, year_number):
        benchmark = self.request.query_params.get('benchmark', 'global')
        country_id = self.request.query_params.get('country_id', None)

        speed_benchmark, _ = get_benchmark_value_for_default_download_layer(benchmark, country_id)

        weekly_queryset = self.queryset.annotate(
            t=FilteredRelation(
                'weekly_status',
                condition=Q(weekly_status__week=week_number)
                          & Q(weekly_status__year=year_number)
                          & Q(weekly_status__deleted__isnull=True),
            )
        ).filter(
            realtime_registration_status__rt_registered=True,
            realtime_registration_status__rt_registration_date__date__lte=end_date,
            realtime_registration_status__deleted__isnull=True,
        ).annotate(
            dummy_group_by=Value(1)).values('dummy_group_by').annotate(
            good=Count(Case(When(t__connectivity_speed__gt=speed_benchmark, then='id')), distinct=True),
            moderate=Count(Case(When(t__connectivity_speed__lte=speed_benchmark, t__connectivity_speed__gte=1000000,
                                     then='id')), distinct=True),
            bad=Count(Case(When(t__connectivity_speed__lt=1000000, then='id')), distinct=True),
            unknown=Count(Case(When(t__connectivity_speed__isnull=True, then='id')), distinct=True),
            school_with_realtime_data=Count(Case(When(t__connectivity_speed__isnull=False, then='id')), distinct=True),
            no_of_schools_measure=Count('id', distinct=True),
            countries_with_realtime_data=Count('country_id', distinct=True),
        ).values('good', 'moderate', 'bad', 'unknown', 'school_with_realtime_data',
                 'no_of_schools_measure', 'countries_with_realtime_data').order_by()

        if len(self.school_filters) > 0:
            weekly_queryset = weekly_queryset.extra(where=[self.school_filters])

        if len(self.school_static_filters) > 0:
            school_static_filters = core_utilities.get_filter_sql(self.request, 'school_static', 'T5')
            weekly_queryset = weekly_queryset.annotate(
                total_weekly_schools=Count('last_weekly_status__school_id', distinct=True),
            ).values(
                'good', 'moderate', 'bad', 'unknown', 'school_with_realtime_data',
                'no_of_schools_measure', 'countries_with_realtime_data', 'total_weekly_schools'
            ).extra(where=[school_static_filters])

        weekly_status = list(weekly_queryset)[0]
        real_time_connected_schools = {
            'good': weekly_status['good'],
            'moderate': weekly_status['moderate'],
            'no_internet': weekly_status['bad'],
            'unknown': weekly_status['unknown'],
        }

        graph_data, positive_speeds = self.generate_country_graph_data(start_date, end_date)

        live_avg = round(sum(positive_speeds) / len(positive_speeds), 2) if len(positive_speeds) > 0 else 0

        live_avg_connectivity = 'unknown'
        rounded_benchmark_value_int = round(speed_benchmark / 1000000, 2)
        rounded_base_benchmark_int = 1

        if live_avg > rounded_benchmark_value_int:
            live_avg_connectivity = 'good'
        elif rounded_base_benchmark_int <= live_avg <= rounded_benchmark_value_int:
            live_avg_connectivity = 'moderate'
        elif live_avg < rounded_base_benchmark_int:
            live_avg_connectivity = 'bad'

        country_id = self.request.query_params.get('country_id', None)
        admin1_id = self.request.query_params.get('admin1_id', None)

        is_data_synced_qs = SchoolWeeklyStatus.objects.filter(
            school__realtime_registration_status__rt_registered=True,
        )

        if len(self.school_filters) > 0:
            is_data_synced_qs = is_data_synced_qs.extra(where=[self.school_filters])

        if len(self.school_static_filters) > 0:
            is_data_synced_qs = is_data_synced_qs.extra(where=[self.school_static_filters])

        if admin1_id:
            is_data_synced_qs = is_data_synced_qs.filter(school__admin1_id=admin1_id)
        if country_id:
            is_data_synced_qs = is_data_synced_qs.filter(school__country_id=country_id)

        return {
            'live_avg': live_avg,
            'live_avg_connectivity': live_avg_connectivity,
            'no_of_entities_measure': weekly_status['no_of_schools_measure'],
            'entity_with_realtime_data': weekly_status['school_with_realtime_data'],
            'countries_with_realtime_data': weekly_status['countries_with_realtime_data'],
            'real_time_connected_entities': real_time_connected_schools,
            'graph_data': graph_data,
            'is_data_synced': is_data_synced_qs.exists(),
            'benchmark_metadata': {
                'benchmark_value': str(speed_benchmark),
                'benchmark_unit': "bps",
                'base_benchmark': "1000000",
                'parameter_column_unit': "bps",
                'round_unit_value': "{val} / (1000 * 1000)",
                'rounded_benchmark_value': rounded_benchmark_value_int,
                'convert_unit': 'mbps',
                'display_unit': 'Mbps',
            },
        }

    def generate_country_graph_data(self, start_date, end_date):
        # Get the daily connectivity_speed for the given country from SchoolDailyStatus model
        avg_daily_connectivity_speed = self.queryset.filter(
            realtime_registration_status__rt_registered=True,
            realtime_registration_status__rt_registration_date__date__lte=end_date,
            realtime_registration_status__deleted__isnull=True,
            daily_status__date__range=[start_date, end_date],
            daily_status__connectivity_speed__isnull=False,
            daily_status__deleted__isnull=True,
        ).values('daily_status__date').annotate(
            avg_speed=Avg('daily_status__connectivity_speed'),
        ).order_by('daily_status__date')

        if len(self.school_filters) > 0:
            avg_daily_connectivity_speed = avg_daily_connectivity_speed.extra(where=[self.school_filters])

        if len(self.school_static_filters) > 0:
            avg_daily_connectivity_speed = avg_daily_connectivity_speed.annotate(
                total_weekly_schools=Count('last_weekly_status__school_id', distinct=True),
            )
            avg_daily_connectivity_speed = avg_daily_connectivity_speed.extra(where=[self.school_static_filters])

        # Generate the graph data in the desired format
        graph_data = []
        current_date = start_date

        while current_date <= end_date:
            graph_data.append({
                'group': 'Download speed',
                'key': date_utilities.format_date(current_date),
                'value': None  # Default value, will be updated later if data exists for the date
            })
            current_date += timedelta(days=1)

        all_positive_speeds = []
        # Update the graph_data with actual values if they exist
        for daily_avg_data in avg_daily_connectivity_speed:
            formatted_date = date_utilities.format_date(daily_avg_data['daily_status__date'])
            for entry in graph_data:
                if entry['key'] == formatted_date:
                    try:
                        rounded_speed = 0
                        if daily_avg_data['avg_speed'] is not None:
                            rounded_speed = round(daily_avg_data['avg_speed'] / 1000000, 2)
                        entry['value'] = rounded_speed
                        all_positive_speeds.append(rounded_speed)
                    except (KeyError, TypeError):
                        pass

        return graph_data, all_positive_speeds

    def get_school_data(self, request):
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key()

        data = None
        if use_cached_data:
            data = cache_manager.get(cache_key)

        if not data:
            self.school_filters = core_utilities.get_filter_sql(self.request, 'schools', 'schools_school')
            self.school_static_filters = core_utilities.get_filter_sql(self.request, 'school_static',
                                                                       'connection_statistics_schoolweeklystatus')

            country_id = self.request.query_params.get('country_id', None)
            if country_id:
                self.queryset = self.queryset.filter(country_id=country_id)

            admin1_id = self.request.query_params.get('admin1_id', None)
            if admin1_id:
                self.queryset = self.queryset.filter(admin1_id=admin1_id)

            is_weekly = self.request.query_params.get('is_weekly', 'true') == 'true'
            start_date = date_utilities.to_date(self.request.query_params.get('start_date'),
                                                default=datetime.combine(datetime.now(), time.min))
            end_date = date_utilities.to_date(self.request.query_params.get('end_date'),
                                              default=datetime.combine(datetime.now(), time.min))

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
                week_number = SchoolWeeklyStatus.objects.filter(
                    year=year_number, week__in=week_numbers_for_month,
                ).order_by('-week').values_list('week', flat=True).first()

                if not week_number:
                    # If for any week of the month data is not available then pick last week number
                    week_number = week_numbers_for_month[-1]

            data = self.calculate_country_download_data(start_date, end_date, week_number, year_number)
            cache_manager.set(cache_key, data, request_path=request_path, soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return data

    def calculate_country_download_entity_data(self, start_date, end_date, week_number, year_number):
        benchmark = self.request.query_params.get('benchmark', 'global')
        country_id = self.request.query_params.get('country_id', None)

        speed_benchmark, _ = get_benchmark_value_for_default_download_layer(benchmark, country_id)

        weekly_queryset = self.queryset.annotate(
            t=FilteredRelation(
                'weekly_status',
                condition=Q(weekly_status__week=week_number)
                          & Q(weekly_status__year=year_number)
                          & Q(weekly_status__deleted__isnull=True),
            )
        ).filter(
            realtime_registration_status__rt_registered=True,
            realtime_registration_status__rt_registration_date__date__lte=end_date,
            realtime_registration_status__deleted__isnull=True,
        ).annotate(
            dummy_group_by=Value(1)).values('dummy_group_by').annotate(
            good=Count(Case(When(t__connectivity_speed__gt=speed_benchmark, then='id')), distinct=True),
            moderate=Count(Case(When(t__connectivity_speed__lte=speed_benchmark, t__connectivity_speed__gte=1000000,
                                     then='id')), distinct=True),
            bad=Count(Case(When(t__connectivity_speed__lt=1000000, then='id')), distinct=True),
            unknown=Count(Case(When(t__connectivity_speed__isnull=True, then='id')), distinct=True),
            entity_with_realtime_data=Count(Case(When(t__connectivity_speed__isnull=False, then='id')), distinct=True),
            no_of_entities_measure=Count('id', distinct=True),
            countries_with_realtime_data=Count('country_id', distinct=True),
        ).values('good', 'moderate', 'bad', 'unknown', 'entity_with_realtime_data',
                 'no_of_entities_measure', 'countries_with_realtime_data').order_by()

        if len(self.entity_filters) > 0:
            weekly_queryset = weekly_queryset.extra(where=[self.entity_filters])

        if len(self.entity_static_filters) > 0:
            entity_static_filters = core_utilities.get_filter_sql(self.request, 'entity_static', 'T5')
            weekly_queryset = weekly_queryset.annotate(
                total_weekly_entities=Count('last_weekly_status__entity_id', distinct=True),
            ).values(
                'good', 'moderate', 'bad', 'unknown', 'entity_with_realtime_data',
                'no_of_entities_measure', 'countries_with_realtime_data', 'total_weekly_entities'
            ).extra(where=[entity_static_filters])

        weekly_status = list(weekly_queryset)[0]
        real_time_connected_entities = {
            'good': weekly_status['good'],
            'moderate': weekly_status['moderate'],
            'no_internet': weekly_status['bad'],
            'unknown': weekly_status['unknown'],
        }

        graph_data, positive_speeds = self.generate_country_graph_entity_data(start_date, end_date)

        live_avg = round(sum(positive_speeds) / len(positive_speeds), 2) if len(positive_speeds) > 0 else 0

        live_avg_connectivity = 'unknown'
        rounded_benchmark_value_int = round(speed_benchmark / 1000000, 2)
        rounded_base_benchmark_int = 1

        if live_avg > rounded_benchmark_value_int:
            live_avg_connectivity = 'good'
        elif rounded_base_benchmark_int <= live_avg <= rounded_benchmark_value_int:
            live_avg_connectivity = 'moderate'
        elif live_avg < rounded_base_benchmark_int:
            live_avg_connectivity = 'bad'

        country_id = self.request.query_params.get('country_id', None)
        admin1_id = self.request.query_params.get('admin1_id', None)

        is_data_synced_qs = EntityWeeklyStatus.objects.filter(
            entity__realtime_registration_status__rt_registered=True,
        )

        if len(self.entity_filters) > 0:
            is_data_synced_qs = is_data_synced_qs.extra(where=[self.entity_filters])

        if len(self.entity_static_filters) > 0:
            is_data_synced_qs = is_data_synced_qs.extra(where=[self.entity_static_filters])

        if admin1_id:
            is_data_synced_qs = is_data_synced_qs.filter(entity__admin1_id=admin1_id)
        if country_id:
            is_data_synced_qs = is_data_synced_qs.filter(entity__country_id=country_id)

        return {
            'live_avg': live_avg,
            'live_avg_connectivity': live_avg_connectivity,
            'no_of_entities_measure': weekly_status['no_of_entities_measure'],
            'entity_with_realtime_data': weekly_status['entity_with_realtime_data'],
            'countries_with_realtime_data': weekly_status['countries_with_realtime_data'],
            'real_time_connected_entities': real_time_connected_entities,
            'graph_data': graph_data,
            'is_data_synced': is_data_synced_qs.exists(),
            'benchmark_metadata': {
                'benchmark_value': str(speed_benchmark),
                'benchmark_unit': "bps",
                'base_benchmark': "1000000",
                'parameter_column_unit': "bps",
                'round_unit_value': "{val} / (1000 * 1000)",
                'rounded_benchmark_value': rounded_benchmark_value_int,
                'convert_unit': 'mbps',
                'display_unit': 'Mbps',
            },
        }

    def generate_country_graph_entity_data(self, start_date, end_date):
        # Get the daily connectivity_speed for the given country from SchoolDailyStatus model
        avg_daily_connectivity_speed = self.queryset.filter(
            realtime_registration_status__rt_registered=True,
            realtime_registration_status__rt_registration_date__date__lte=end_date,
            realtime_registration_status__deleted__isnull=True,
            daily_status__date__range=[start_date, end_date],
            daily_status__connectivity_speed__isnull=False,
            daily_status__deleted__isnull=True,
        ).values('daily_status__date').annotate(
            avg_speed=Avg('daily_status__connectivity_speed'),
        ).order_by('daily_status__date')

        if len(self.entity_filters) > 0:
            avg_daily_connectivity_speed = avg_daily_connectivity_speed.extra(where=[self.entity_filters])

        if len(self.entity_static_filters) > 0:
            avg_daily_connectivity_speed = avg_daily_connectivity_speed.annotate(
                total_weekly_entities=Count('last_weekly_status__entity_id', distinct=True),
            )
            avg_daily_connectivity_speed = avg_daily_connectivity_speed.extra(where=[self.entity_static_filters])

        # Generate the graph data in the desired format
        graph_data = []
        current_date = start_date

        while current_date <= end_date:
            graph_data.append({
                'group': 'Download speed',
                'key': date_utilities.format_date(current_date),
                'value': None  # Default value, will be updated later if data exists for the date
            })
            current_date += timedelta(days=1)

        all_positive_speeds = []
        # Update the graph_data with actual values if they exist
        for daily_avg_data in avg_daily_connectivity_speed:
            formatted_date = date_utilities.format_date(daily_avg_data['daily_status__date'])
            for entry in graph_data:
                if entry['key'] == formatted_date:
                    try:
                        rounded_speed = 0
                        if daily_avg_data['avg_speed'] is not None:
                            rounded_speed = round(daily_avg_data['avg_speed'] / 1000000, 2)
                        entry['value'] = rounded_speed
                        all_positive_speeds.append(rounded_speed)
                    except (KeyError, TypeError):
                        pass

        return graph_data, all_positive_speeds

    def get_entity_data(self, request, entity_type):
        use_cached_data = self.request.query_params.get(
            self.CACHE_KEY, 'on'
        ).lower() in ['on', 'true']

        request_path = remove_query_param(
            self.request.get_full_path(), 'cache'
        )
        cache_key = f"{self.get_cache_key()}_{entity_type}"

        data = None
        if use_cached_data:
            data = cache_manager.get(cache_key)

        if not data:
            # -------- Filters
            self.entity_filters = []
            self.entity_static_filters = core_utilities.get_filter_sql(
                self.request,
                'entity_static',
                'connection_statistics_entityweeklystatus'
            )

            # -------- Apply entity_type filter
            self.queryset = self.queryset.filter(
                entity_type__code=entity_type
            )

            # -------- Country / admin filters
            country_id = self.request.query_params.get('country_id')
            if country_id:
                self.queryset = self.queryset.filter(country_id=country_id)

            admin1_id = self.request.query_params.get('admin1_id')
            if admin1_id:
                self.queryset = self.queryset.filter(admin1_id=admin1_id)

            # -------- Date logic
            is_weekly = self.request.query_params.get(
                'is_weekly', 'true'
            ) == 'true'

            start_date = date_utilities.to_date(
                self.request.query_params.get('start_date'),
                default=datetime.combine(datetime.now(), time.min)
            )
            end_date = date_utilities.to_date(
                self.request.query_params.get('end_date'),
                default=datetime.combine(datetime.now(), time.min)
            )

            month_number = date_utilities.get_month_from_date(start_date)
            year_number = date_utilities.get_year_from_date(start_date)

            if is_weekly:
                week_number = date_utilities.get_week_from_date(start_date)
            else:
                dates_on_all_sundays = date_utilities.all_days_of_a_month(
                    year_number,
                    month_number,
                    day_name='sunday'
                ).keys()

                week_numbers_for_month = [
                    date_utilities.get_week_from_date(d)
                    for d in dates_on_all_sundays
                ]

                week_number = (
                    EntityWeeklyStatus.objects
                    .filter(
                        year=year_number,
                        week__in=week_numbers_for_month,
                        entity__entity_type__code=entity_type
                    )
                    .order_by('-week')
                    .values_list('week', flat=True)
                    .first()
                )

                if not week_number:
                    week_number = week_numbers_for_month[-1]

            # -------- Compute stats
            data = self.calculate_country_download_entity_data(
                start_date,
                end_date,
                week_number,
                year_number
            )

            cache_manager.set(
                cache_key,
                data,
                request_path=request_path,
                soft_timeout=settings.CACHE_CONTROL_MAX_AGE
            )

        return data


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class EntityConnectivityConfigurationsViewSet(APIView):
    base_auth_permissions = (
        AllowAny,
    )
    model = EntityDailyStatus
    queryset = model.objects.filter(entity__deleted__isnull=True)

    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'ENTITY_CONNECTIVITY_CONFIGURATIONS_V2_STATS'

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}'.format(self.CACHE_KEY_PREFIX,
                                '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))), )

    @staticmethod
    def find_most_recent_week(table_name, last_week_start, last_week_end, monday_date, max_weeks=9):
        """
        Find the most recent week with data by checking week-by-week backward.
        Returns (monday_on_entry_date, sunday_on_entry_date) tuple.
        """
        monday_on_entry_date = None
        sunday_on_entry_date = None

        with connection.cursor() as cursor:
            for weeks_back in range(max_weeks):
                check_start = last_week_start - timedelta(days=7 * weeks_back)
                check_end = last_week_end - timedelta(days=7 * weeks_back)

                cursor.execute(f"""
                    SELECT 1 FROM {table_name}
                    WHERE deleted IS NULL
                    AND date BETWEEN %s AND %s
                    LIMIT 1
                """, [check_start, check_end])

                if cursor.fetchone() is not None:
                    monday_on_entry_date = check_start
                    sunday_on_entry_date = check_end
                    break

        # If no data found in checked weeks, fallback to latest available date
        if not monday_on_entry_date:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT date FROM {table_name}
                    WHERE deleted IS NULL
                    ORDER BY date DESC
                    LIMIT 1
                """)
                row = cursor.fetchone()
                if row:
                    latest_daily_entry = row[0]
                    monday_on_entry_date = latest_daily_entry - timedelta(days=latest_daily_entry.weekday())
                    sunday_on_entry_date = monday_on_entry_date + timedelta(days=6)
                else:
                    monday_on_entry_date = monday_date
                    sunday_on_entry_date = monday_date + timedelta(days=6)

        return monday_on_entry_date, sunday_on_entry_date

    @staticmethod
    def get_available_years(table_name, default_year):
        """
        Get list of available years from the data using raw SQL.
        Returns list of years as integers.
        """
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT DISTINCT EXTRACT(YEAR FROM date)::int as year
                FROM {table_name}
                WHERE deleted IS NULL
                ORDER BY year
            """)
            if cursor.rowcount > 0:
                return [row[0] for row in cursor.fetchall()]
            return [default_year]

    @staticmethod
    def extract_entity_layer_params(request):
        """
        Extract all entity layer parameters from the request.
        Looks for parameters matching pattern: {entity_code}_layer_id
        e.g., school_layer_id, health_layer_id, hospital_layer_id

        Returns a dict mapping entity_code -> layer_id
        """
        entity_layers = {}
        for param_name, param_value in request.query_params.items():
            if param_name.endswith('_layer_id') and param_value:
                entity_code = param_name.replace('_layer_id', '')
                entity_layers[entity_code] = param_value
        return entity_layers

    @staticmethod
    def extract_entity_filter_params(request):
        """
        Extract all entity filter parameters from the request.
        Looks for parameters matching pattern: {entity_code}_filter_id
        e.g., school_filter_id, health_filter_id

        Returns a dict mapping entity_code -> filter_id
        """
        entity_filters = {}
        for param_name, param_value in request.query_params.items():
            if param_name.endswith('_filter_id') and param_value:
                entity_code = param_name.replace('_filter_id', '')
                entity_filters[entity_code] = param_value
        return entity_filters

    @staticmethod
    def build_layers_and_filters_list_for_entity(request, entity_code, layer_id=None, filter_id=None):
        """
        Helper method to build layers_list and filters_list for a specific entity type.
        Only includes layers/filters that belong to the specified entity type.
        Returns a tuple (layers_list, filters_list).
        """
        layers_list = []
        filters_list = []

        # Use explicitly passed layer_id only (do NOT fall back to old layer_id param here)
        # The calling method should pass the correct layer_id for this entity type
        effective_layer_id = layer_id
        if effective_layer_id:
            try:
                data_layer = DataLayer.objects.select_related('entity_type').get(
                    pk=effective_layer_id,
                    status=DataLayer.LAYER_STATUS_PUBLISHED,
                )
                # Validate that the layer belongs to the expected entity type
                layer_entity_code = data_layer.entity_type.code if data_layer.entity_type else LEGACY_MODEL
                if layer_entity_code == entity_code:
                    layers_list.append({
                        'data_layer_id': data_layer.id,
                        'name': data_layer.name,
                        'entity_type': layer_entity_code,
                    })
            except DataLayer.DoesNotExist:
                pass

        # Use explicitly passed filter_id only
        effective_filter_id = filter_id
        if effective_filter_id:
            try:
                advance_filter = AdvanceFilter.objects.select_related('entity_type').get(
                    pk=effective_filter_id,
                    status=AdvanceFilter.FILTER_STATUS_PUBLISHED,
                )
                # Validate that the filter belongs to the expected entity type
                filter_entity_code = advance_filter.entity_type.code if advance_filter.entity_type else LEGACY_MODEL
                if filter_entity_code == entity_code:
                    filters_list.append({
                        'advance_filter_id': advance_filter.id,
                        'name': advance_filter.name,
                        'entity_type': filter_entity_code,
                    })
            except AdvanceFilter.DoesNotExist:
                pass

        return layers_list, filters_list

    def get_school_configs(self, request, layer_id=None, filter_id=None, **kwargs):
        """
        Build connectivity configuration for school entity type.

        Args:
            request: The HTTP request
            layer_id: Optional explicit layer ID (overrides request.query_params.get('layer_id'))
            filter_id: Optional explicit filter ID (overrides request.query_params.get('filter_id'))
        """
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        cache_key = self.get_cache_key()

        static_data = None
        if use_cached_data:
            static_data = cache_manager.get(cache_key)

        if not static_data:
            static_data = {}

            country_id = self.request.query_params.get('country_id', None)
            if country_id:
                self.queryset = self.queryset.filter(school__country_id=country_id)

            admin1_id = self.request.query_params.get('admin1_id', None)
            if admin1_id:
                self.queryset = self.queryset.filter(school__admin1_id=admin1_id)

            school_id = self.request.query_params.get('school_id', None)
            if school_id:
                self.queryset = self.queryset.filter(school=school_id)

            school_ids = self.request.query_params.get('school_ids', '')
            if not core_utilities.is_blank_string(school_ids):
                school_ids = [int(school_id.strip()) for school_id in school_ids.split(',')]
                self.queryset = self.queryset.filter(school__in=school_ids)

            # Use explicit layer_id if provided, otherwise fall back to request param
            effective_layer_id = layer_id or request.query_params.get('layer_id')

            if effective_layer_id:
                data_layer_instance = get_object_or_404(
                    DataLayer.objects.all(),
                    pk=effective_layer_id,
                    status=DataLayer.LAYER_STATUS_PUBLISHED,
                    type=DataLayer.LAYER_TYPE_LIVE,
                )

                data_sources = data_layer_instance.data_sources.all()

                live_data_sources = ['UNKNOWN']

                for d in data_sources:
                    source_type = d.data_source.data_source_type
                    if source_type == DataSource.DATA_SOURCE_TYPE_QOS:
                        live_data_sources.append(statistics_configs.QOS_SOURCE)
                    elif source_type == DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                        live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

                parameter_col = data_sources.first().data_source_column
                parameter_column_name = str(parameter_col['name'])

                self.queryset = self.queryset.filter(
                    live_data_source__in=live_data_sources,
                ).filter(**{parameter_column_name + '__isnull': False})

            today_date = core_utilities.get_current_datetime_object().date()
            monday_date = today_date - timedelta(days=today_date.weekday())

            last_week_start = monday_date - timedelta(days=7)
            last_week_end = monday_date - timedelta(days=1)

            table_name = self.queryset.model._meta.db_table
            monday_on_entry_date, sunday_on_entry_date = self.find_most_recent_week(
                table_name, last_week_start, last_week_end, monday_date, max_weeks=9
            )

            # Use explicit filter_id if provided
            effective_filter_id = filter_id or request.query_params.get('filter_id')
            layers_list, filters_list = self.build_layers_and_filters_list_for_entity(
                request, LEGACY_MODEL, effective_layer_id, effective_filter_id
            )

            if monday_on_entry_date:
                years = self.get_available_years(table_name, today_date.year)

                static_data = {
                    'week': {
                        'start_date': date_utilities.format_date(monday_on_entry_date),
                        'end_date': date_utilities.format_date(sunday_on_entry_date)
                    },
                    'month': {
                        'start_date': date_utilities.format_date(date_utilities.get_first_date_of_month(
                            monday_on_entry_date.year, monday_on_entry_date.month)),
                        'end_date': date_utilities.format_date(date_utilities.get_last_date_of_month(
                            monday_on_entry_date.year, monday_on_entry_date.month))
                    },
                    'years': years,
                    'layers_list': layers_list,
                    'filters_list': filters_list,
                }

            request_path = remove_query_param(request.get_full_path(), 'cache')
            cache_manager.set(cache_key, static_data, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return static_data

    @staticmethod
    def resolve_entity_type(request):
        """
        Determine the canonical entity type code from the request.
        Priority: layer_id's entity_type > explicit entity_type__code param > default (school).
        """
        layer_id = request.query_params.get('layer_id')
        explicit_entity_type = request.query_params.get('entity_type__code')

        if layer_id:
            try:
                data_layer = DataLayer.objects.select_related('entity_type').get(
                    pk=layer_id,
                    status=DataLayer.LAYER_STATUS_PUBLISHED,
                )
                if data_layer.entity_type:
                    layer_entity_type = LEGACY_MODEL if data_layer.entity_type.is_legacy else data_layer.entity_type.code

                    if explicit_entity_type and explicit_entity_type != layer_entity_type:
                        from rest_framework.exceptions import ValidationError
                        raise ValidationError(
                            {"error": "Requested entity_type__code does not match the layer's entity type."})

                    return layer_entity_type
            except DataLayer.DoesNotExist:
                pass

        if explicit_entity_type:
            return explicit_entity_type

        return LEGACY_MODEL

    def get(self, request, *args, **kwargs):
        """
        Get connectivity configurations for entity types specified via query parameters.

        Query Parameters:
            {entity_code}_layer_id: Layer ID for a specific entity type (e.g., school_layer_id, health_layer_id)
            {entity_code}_filter_id: Filter ID for a specific entity type (e.g., school_filter_id, health_filter_id)
            country_id: Country ID to filter by (applies to all entities)
            admin1_id: Admin1 ID to filter by (applies to all entities)
            cache: Set to 'off' to bypass cache

        Returns:
            Response with entity type codes as keys:
            {
                "school": {"week": {...}, "month": {...}, "years": [...], "layers_list": [...], "filters_list": [...]},
                "health": {"week": {...}, "month": {...}, "years": [...], "layers_list": [...], "filters_list": [...]},
                ...
            }

        Future-proof: Adding a new entity type (e.g., hospital) only requires adding
        hospital_layer_id parameter - no code changes needed.
        """
        use_cached_data = request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        cache_key = self.get_cache_key()

        data = None
        if use_cached_data:
            data = cache_manager.get(cache_key)

        if not data:
            # Extract entity-specific layer and filter parameters
            entity_layers = self.extract_entity_layer_params(request)
            entity_filters = self.extract_entity_filter_params(request)

            # Build the response - only query entity types that have explicit parameters
            data = {}

            # Get entity codes to process (union of layer params and filter params)
            entity_codes_to_process = set(entity_layers.keys()) | set(entity_filters.keys())

            # Handle backward compatibility: if old-style layer_id is provided without entity-specific params
            if not entity_codes_to_process and request.query_params.get('layer_id'):
                resolved_type = self.resolve_entity_type(request)
                entity_codes_to_process.add(resolved_type)

            # Process each entity type that has parameters
            for entity_code in entity_codes_to_process:
                # Get layer_id and filter_id for this entity type
                layer_id = entity_layers.get(entity_code)
                filter_id = entity_filters.get(entity_code)

                # Get the entity type object
                try:
                    entity_type = EntityType.objects.get(code=entity_code, deleted__isnull=True, is_active=True)
                except EntityType.DoesNotExist:
                    # Skip if entity type doesn't exist or is inactive
                    continue

                # Build configs for this entity type
                if entity_type.is_legacy:
                    # School (legacy) - uses SchoolDailyStatus
                    self.queryset = SchoolDailyStatus.objects.filter(school__deleted__isnull=True)
                    entity_data = self.get_school_configs(request, layer_id=layer_id, filter_id=filter_id)
                    data[LEGACY_MODEL] = entity_data
                else:
                    # Non-legacy entity (health, etc.) - uses EntityDailyStatus
                    self.queryset = EntityDailyStatus.objects.filter(
                        entity__deleted__isnull=True,
                        entity__entity_type=entity_type,
                    )
                    entity_data = self.get_entity_configs(request, entity_code, layer_id=layer_id, filter_id=filter_id)
                    data[entity_code] = entity_data

            request_path = remove_query_param(request.get_full_path(), 'cache')
            cache_manager.set(cache_key, data, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=data)

    def get_entity_configs(self, request, entity_type_code, layer_id=None, filter_id=None, **kwargs):
        """
        Build connectivity configuration response for non-legacy (non-school) entity types.
        Mirrors get_school_configs() but operates on EntityDailyStatus.

        Args:
            request: The HTTP request
            entity_type_code: The entity type code (e.g., 'health', 'library')
            layer_id: Optional explicit layer ID
            filter_id: Optional explicit filter ID
        """
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        cache_key = self.get_cache_key()

        static_data = None
        if use_cached_data:
            static_data = cache_manager.get(cache_key)

        if not static_data:
            static_data = {}

            country_id = self.request.query_params.get('country_id', None)
            if country_id:
                self.queryset = self.queryset.filter(entity__country_id=country_id)

            admin1_id = self.request.query_params.get('admin1_id', None)
            if admin1_id:
                self.queryset = self.queryset.filter(entity__admin1_id=admin1_id)

            entity_id = self.request.query_params.get('entity_id', None)
            if entity_id:
                self.queryset = self.queryset.filter(entity=entity_id)

            entity_ids = self.request.query_params.get('entity_ids', '')
            if not core_utilities.is_blank_string(entity_ids):
                entity_ids = [int(eid.strip()) for eid in entity_ids.split(',')]
                self.queryset = self.queryset.filter(entity__in=entity_ids)

            # Use explicit layer_id if provided
            effective_layer_id = layer_id or request.query_params.get('layer_id')
            if effective_layer_id:
                data_layer_instance = get_object_or_404(
                    DataLayer.objects.all(),
                    pk=effective_layer_id,
                    status=DataLayer.LAYER_STATUS_PUBLISHED,
                    type=DataLayer.LAYER_TYPE_LIVE,
                )

                data_sources = data_layer_instance.data_sources.all()

                live_data_sources = ['UNKNOWN']

                for d in data_sources:
                    source_type = d.data_source.data_source_type
                    if source_type == DataSource.DATA_SOURCE_TYPE_QOS:
                        live_data_sources.append(statistics_configs.QOS_SOURCE)
                    elif source_type == DataSource.DATA_SOURCE_TYPE_DAILY_CHECK_APP:
                        live_data_sources.append(statistics_configs.DAILY_CHECK_APP_MLAB_SOURCE)

                parameter_col = data_sources.first().data_source_column
                parameter_column_name = str(parameter_col['name'])

                self.queryset = self.queryset.filter(
                    live_data_source__in=live_data_sources,
                ).filter(**{parameter_column_name + '__isnull': False})

            today_date = core_utilities.get_current_datetime_object().date()
            monday_date = today_date - timedelta(days=today_date.weekday())

            last_week_start = monday_date - timedelta(days=7)
            last_week_end = monday_date - timedelta(days=1)

            table_name = self.queryset.model._meta.db_table
            monday_on_entry_date, sunday_on_entry_date = self.find_most_recent_week(
                table_name, last_week_start, last_week_end, monday_date, max_weeks=9
            )

            # Use explicit filter_id if provided
            effective_filter_id = filter_id or request.query_params.get('filter_id')
            layers_list, filters_list = self.build_layers_and_filters_list_for_entity(
                request, entity_type_code, effective_layer_id, effective_filter_id
            )

            if monday_on_entry_date:
                years = self.get_available_years(table_name, today_date.year)

                static_data = {
                    'week': {
                        'start_date': date_utilities.format_date(monday_on_entry_date),
                        'end_date': date_utilities.format_date(sunday_on_entry_date)
                    },
                    'month': {
                        'start_date': date_utilities.format_date(date_utilities.get_first_date_of_month(
                            monday_on_entry_date.year, monday_on_entry_date.month)),
                        'end_date': date_utilities.format_date(date_utilities.get_last_date_of_month(
                            monday_on_entry_date.year, monday_on_entry_date.month))
                    },
                    'years': years,
                    'layers_list': layers_list,
                    'filters_list': filters_list,
                }

            request_path = remove_query_param(request.get_full_path(), 'cache')
            cache_manager.set(cache_key, static_data, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return static_data
