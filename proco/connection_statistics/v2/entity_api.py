from datetime import timedelta, datetime, time

from django.conf import settings
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
    CACHE_KEY_PREFIX = 'ENTITY_CONNECTIVITY_CONFIGURATIONS_STATS'

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}'.format(self.CACHE_KEY_PREFIX,
                                '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))), )
    @staticmethod
    def build_layers_and_filters_list(request):
        """
        Helper method to build layers_list and filters_list based on layer_id and filter_id.
        Returns a tuple (layers_list, filters_list).
        """
        layers_list = []
        filters_list = []

        layer_id = request.query_params.get('layer_id')
        if layer_id:
            try:
                data_layer = DataLayer.objects.select_related('entity_type').get(
                    pk=layer_id,
                    status=DataLayer.LAYER_STATUS_PUBLISHED,
                )
                entity_type = data_layer.entity_type
                layers_list.append({
                    'data_layer_id': data_layer.id,
                    'name': data_layer.name,
                    'entity_type': entity_type.code if entity_type else None,
                })
            except DataLayer.DoesNotExist:
                pass

        filter_id = request.query_params.get('filter_id')
        if filter_id:
            try:
                advance_filter = AdvanceFilter.objects.select_related('entity_type').get(
                    pk=filter_id,
                    status=AdvanceFilter.FILTER_STATUS_PUBLISHED,
                )
                entity_type = advance_filter.entity_type
                filters_list.append({
                    'advance_filter_id': advance_filter.id,
                    'name': advance_filter.name,
                    'entity_type': entity_type.code if entity_type else None,
                })
            except AdvanceFilter.DoesNotExist:
                pass

        return layers_list, filters_list

    def get_school_configs(self, request, *args, **kwargs):
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

            layer_id = request.query_params.get('layer_id')

            if layer_id:
                data_layer_instance = get_object_or_404(
                    DataLayer.objects.all(),
                    pk=layer_id,
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

            monday_on_entry_date = None
            sunday_on_entry_date = None

            today_date = core_utilities.get_current_datetime_object().date()
            monday_date = today_date - timedelta(days=today_date.weekday())

            last_week_start = monday_date - timedelta(days=7)
            last_week_end = monday_date - timedelta(days=1)

            last_week_entry = self.queryset.filter(
                date__range=(last_week_start, last_week_end)
            ).values_list('date', flat=True).order_by('-date').first()

            if last_week_entry:
                # TECH-7453: 1. If last week's data is present use it as default.
                monday_on_entry_date = last_week_start
                sunday_on_entry_date = last_week_end
            else:
                # TECH-7453: 2. If last week data is not present then fallback to the latest available week including the current week as well.
                latest_daily_entry = self.queryset.values_list('date', flat=True).order_by('-date').first()

                if latest_daily_entry:
                    monday_on_entry_date = latest_daily_entry - timedelta(days=latest_daily_entry.weekday())
                    sunday_on_entry_date = monday_on_entry_date + timedelta(days=6)

            layers_list, filters_list = self.build_layers_and_filters_list(request)

            if monday_on_entry_date:
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
                    'years': list(self.queryset.values_list('date__year', flat=True).order_by('date__year').distinct()),
                    'layers_list': layers_list,
                    'filters_list': filters_list,
                }

            request_path = remove_query_param(request.get_full_path(), 'cache')
            cache_manager.set(cache_key, static_data, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=static_data)


    def get(self, request, *args, **kwargs):
        entity_type = self.request.query_params.get('entity_type__code')
        if entity_type == LEGACY_MODEL:
            self.queryset = SchoolDailyStatus.objects.filter(school__deleted__isnull=True)
            return self.get_school_configs(request, *args, **kwargs)
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
                self.queryset = self.queryset.filter(school=entity_id)

            entity_ids = self.request.query_params.get('entity_ids', '')
            if not core_utilities.is_blank_string(entity_ids):
                entity_ids = [int(entity_id.strip()) for school_id in entity_ids.split(',')]
                self.queryset = self.queryset.filter(entity__in=entity_ids)

            layer_id = request.query_params.get('layer_id')
            if layer_id:
                data_layer_instance = get_object_or_404(
                    DataLayer.objects.all(),
                    pk=layer_id,
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

            monday_on_entry_date = None
            sunday_on_entry_date = None

            today_date = core_utilities.get_current_datetime_object().date()
            monday_date = today_date - timedelta(days=today_date.weekday())

            last_week_start = monday_date - timedelta(days=7)
            last_week_end = monday_date - timedelta(days=1)

            last_week_entry = self.queryset.filter(
                date__range=(last_week_start, last_week_end)
            ).values_list('date', flat=True).order_by('-date').first()

            if last_week_entry:
                # TECH-7453: 1. If last week's data is present use it as default.
                monday_on_entry_date = last_week_start
                sunday_on_entry_date = last_week_end
            else:
                # TECH-7453: 2. If last week data is not present then fallback to the latest available week including the current week as well.
                latest_daily_entry = self.queryset.values_list('date', flat=True).order_by('-date').first()

                if latest_daily_entry:
                    monday_on_entry_date = latest_daily_entry - timedelta(days=latest_daily_entry.weekday())
                    sunday_on_entry_date = monday_on_entry_date + timedelta(days=6)

            layers_list, filters_list = self.build_layers_and_filters_list(request)

            if monday_on_entry_date:
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
                    'years': list(self.queryset.values_list('date__year', flat=True).order_by('date__year').distinct()),
                    'layers_list': layers_list,
                    'filters_list': filters_list,
                }

            request_path = remove_query_param(request.get_full_path(), 'cache')
            cache_manager.set(cache_key, static_data, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=static_data)


