from collections import OrderedDict
from datetime import datetime, time, timedelta

from django.conf import settings
from django.db.models import (
    Avg, Case, CharField, FilteredRelation, OuterRef, Q, Subquery, Value, When
)
from django.db.models import BooleanField, Count
from django.db.models.functions.text import Lower
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_control
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import status as rest_status
from rest_framework.filters import SearchFilter
from rest_framework.generics import ListAPIView
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param
from rest_framework.views import APIView

from proco.accounts.models import DataLayer, DataSource
from proco.connection_statistics import serializers as statistics_serializers
from proco.connection_statistics.config import app_config as statistics_configs
from proco.connection_statistics.filters import DateMonthFilter, DateWeekNumberFilter, DateYearFilter
from proco.connection_statistics.models import (
    CountryDailyStatus,
    SchoolDailyStatus,
    SchoolWeeklyStatus,
    CountryWeeklyStatus,
    SchoolRealTimeRegistration,
)
from proco.connection_statistics.utils import get_benchmark_value_for_default_download_layer
from proco.core import db_utils as db_utilities
from proco.core import permissions as core_permissions
from proco.core import utils as core_utilities
from proco.core.viewsets import BaseModelViewSet
from proco.locations.models import Country
from proco.schools.models import School
from proco.utils import dates as date_utilities
from proco.utils.cache import cache_manager
from proco.utils.error_message import id_missing_error_mess, delete_succ_mess, error_mess
from proco.utils.filters import NullsAlwaysLastOrderingFilter
from proco.utils.log import action_log, changed_fields


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class GlobalStatsAPIView(APIView):
    permission_classes = (AllowAny,)

    model = School
    queryset = model.objects.all()

    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'GLOBAL_STATS'

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

    def calculate_global_statistic(self):
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
            'schools_connected': school_connectivity_status['total_schools'],
            'schools_with_connectivity_status_mapped': school_connectivity_status[
                'schools_with_connectivity_status_mapped'],
            'connectivity_global_benchmark': {
                'value': giga_connectivity_benchmark,
                'unit': giga_connectivity_benchmark_unit,
            },
            'connected_schools': {
                'connected': school_connectivity_status['connected'],
                'not_connected': school_connectivity_status['not_connected'],
                'unknown': school_connectivity_status['unknown'],
            }
        }


class CountryDailyStatsListAPIView(ListAPIView):
    model = CountryDailyStatus
    queryset = model.objects.all()
    serializer_class = statistics_serializers.CountryDailyStatusSerializer
    filter_backends = (
        DjangoFilterBackend,
        DateYearFilter,
        DateWeekNumberFilter,
    )
    filterset_fields = {
        'date': ['lte', 'gte'],
    }

    def get_queryset(self):
        queryset = super(CountryDailyStatsListAPIView, self).get_queryset()
        country = get_object_or_404(
            Country.objects.annotate(code_lower=Lower('code')),
            code_lower=self.kwargs.get('country_code').lower(),
        )
        return queryset.filter(country=country)


class SchoolDailyStatsListAPIView(ListAPIView):
    model = SchoolDailyStatus
    queryset = model.objects.all()
    serializer_class = statistics_serializers.SchoolDailyStatusSerializer
    filter_backends = (
        DjangoFilterBackend,
        DateYearFilter,
        DateWeekNumberFilter,
        DateMonthFilter,
    )
    filterset_fields = {
        'date': ['lte', 'gte'],
    }

    def get_queryset(self):
        queryset = super(SchoolDailyStatsListAPIView, self).get_queryset()
        return queryset.filter(school_id=self.kwargs['school_id'])


class SchoolConnectivityStatsListAPIView(ListAPIView):
    model = School
    queryset = model.objects.all().select_related('last_weekly_status')
    serializer_class = statistics_serializers.SchoolConnectivityStatusSerializer
    related_model = Country

    schools_daily_status_qs = SchoolDailyStatus.objects.all()

    def get_serializer(self, *args, **kwargs):
        kwargs['country'] = self.get_country()
        kwargs['graph_data'] = self.generate_graph_data()
        kwargs['speed_benchmark'] = self.kwargs['speed_benchmark']

        return super(SchoolConnectivityStatsListAPIView, self).get_serializer(*args, **kwargs)

    def get_country(self):
        if not hasattr(self, '_country'):
            self._country = get_object_or_404(
                Country.objects.defer('geometry').select_related('last_weekly_status'),
                id=self.kwargs.get('country_id'),
            )
        return self._country

    def update_kwargs(self):
        start_date = date_utilities.to_date(self.request.query_params.get('start_date'),
                                            default=datetime.combine(datetime.now(),
                                                                     time.min))
        end_date = date_utilities.to_date(self.request.query_params.get('end_date'),
                                          default=datetime.combine(datetime.now(),
                                                                   time.min))

        school_ids = self.request.query_params.get('school_ids', '')
        if not core_utilities.is_blank_string(school_ids):
            school_ids = [int(school_id.strip()) for school_id in school_ids.split(',')]
        else:
            school_ids = [34554]

        self.kwargs.update({
            'start_date': start_date,
            'end_date': end_date,
            'school_ids': school_ids,
            'country_id': self.request.query_params.get('country_id'),
            'is_weekly': False if self.request.query_params.get('is_weekly', 'true') == 'false' else True,
            'group_name': 'Download speed',
            'group_value': 'connectivity_speed',
        })

    def get_queryset(self):
        queryset = super(SchoolConnectivityStatsListAPIView, self).get_queryset()

        rt_connected_schools = list(SchoolRealTimeRegistration.objects.filter(
            school__in=self.kwargs['school_ids'],
            rt_registered=True,
            rt_registration_date__lte=self.kwargs['end_date'],
        ).values_list('school', flat=True).order_by('school_id').distinct('school_id'))

        queryset = queryset.filter(
            id__in=self.kwargs['school_ids'],
            country__id=self.kwargs['country_id'],
        ).annotate(
            is_rt_connected=Case(
                When(
                    id__in=rt_connected_schools,
                    then=Value(True)
                ),
                default=Value(False),
                output_field=BooleanField(),
            ),
        )

        is_weekly = self.request.query_params.get('is_weekly', 'true') == 'true'
        year_number = date_utilities.get_year_from_date(self.kwargs['start_date'])

        if is_weekly:
            # If is_weekly == True, then pick the week number based on start_date
            week_numbers = [date_utilities.get_week_from_date(self.kwargs['start_date']), ]
        else:
            # If is_weekly == False, then:
            # 1. Collect dates on all sundays of the given month and year
            # 2. Get the week numbers for all sundays and look into SchoolWeeklyStatus table for which
            # last week number data was created in the given month of the year. And pick this week number
            month_number = date_utilities.get_month_from_date(self.kwargs['start_date'])
            dates_on_all_sundays = date_utilities.all_days_of_a_month(year_number, month_number,
                                                                      day_name='sunday').keys()
            week_numbers = [date_utilities.get_week_from_date(date) for date in dates_on_all_sundays]

        benchmark = self.request.query_params.get('benchmark', 'global')
        country_id = self.kwargs['country_id']

        speed_benchmark, _ = get_benchmark_value_for_default_download_layer(benchmark, country_id)
        self.kwargs['speed_benchmark'] = speed_benchmark

        school_status_in_given_week_qry = SchoolWeeklyStatus.objects.filter(
            school=OuterRef('id'),
            year=year_number, week__in=week_numbers,
        ).annotate(
            live_avg_connectivity=Case(
                When(connectivity_speed__gt=speed_benchmark, then=Value('good')),
                When(connectivity_speed__lte=speed_benchmark, connectivity_speed__gte=1000000, then=Value('moderate')),
                When(connectivity_speed__lt=1000000, then=Value('bad')),
                default=Value('unknown'),
                output_field=CharField(),
            ),
        ).values('live_avg_connectivity', 'school_id', 'week').order_by('school_id', '-week').distinct('school_id')

        queryset = queryset.annotate(live_avg_connectivity=Subquery(
            school_status_in_given_week_qry.values('live_avg_connectivity')[:1],
            output_field=CharField(),
        ), )

        return queryset

    def list(self, request, *args, **kwargs):
        self.update_kwargs()

        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True)

        return Response(serializer.data)

    def generate_graph_data(self):

        school_graph_data = {}

        for school_id in self.kwargs['school_ids']:
            graph_data = []
            current_date = self.kwargs['start_date']

            while current_date <= self.kwargs['end_date']:
                graph_data.append({
                    'group': self.kwargs['group_name'],
                    'key': date_utilities.format_date(current_date),
                    'value': None,
                })
                current_date += timedelta(days=1)
            school_graph_data[school_id] = graph_data

        schools_daily_status_qs = self.schools_daily_status_qs.filter(
            school_id__in=self.kwargs['school_ids'],
            date__range=[self.kwargs['start_date'], self.kwargs['end_date']],
        )

        # Update the graph_data with actual values if they exist
        for daily_school_record in schools_daily_status_qs.values():
            school_id = daily_school_record['school_id']
            connectivity_speed = daily_school_record['connectivity_speed']
            if daily_school_record['connectivity_speed'] is not None:
                connectivity_speed = round(connectivity_speed / 1000000, 2)
                daily_school_record['connectivity_speed'] = connectivity_speed

            graph_data = school_graph_data[school_id]
            formatted_date = date_utilities.format_date(daily_school_record['date'])
            for entry in graph_data:
                if entry['key'] == formatted_date:
                    entry['value'] = daily_school_record.get(self.kwargs['group_value'], None)

        return school_graph_data


class SchoolCoverageStatsListAPIView(ListAPIView):
    model = School
    queryset = model.objects.all().select_related('last_weekly_status')
    serializer_class = statistics_serializers.SchoolCoverageStatusSerializer

    def update_kwargs(self):
        school_ids = self.request.query_params.get('school_ids', '')
        if not core_utilities.is_blank_string(school_ids):
            school_ids = [int(school_id.strip()) for school_id in school_ids.split(',')]
        else:
            school_ids = [34554]

        self.kwargs.update({
            'school_ids': school_ids,
            'country_id': self.request.query_params.get('country_id'),
        })

    def get_queryset(self):
        queryset = super(SchoolCoverageStatsListAPIView, self).get_queryset()
        queryset = queryset.filter(
            id__in=self.kwargs['school_ids'],
            country__id=self.kwargs['country_id'],
        )

        return queryset

    def list(self, request, *args, **kwargs):
        self.update_kwargs()

        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True)

        return Response(serializer.data)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class ConnectivityAPIView(APIView):
    permission_classes = (AllowAny,)

    model = School
    queryset = model.objects.all()

    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'CONNECTIVITY_STATS'

    school_filters = []
    school_static_filters = []

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}'.format(self.CACHE_KEY_PREFIX,
                                '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))), )

    def get(self, request, *args, **kwargs):
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

        return Response(data=data)

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
            'no_of_schools_measure': weekly_status['no_of_schools_measure'],
            'school_with_realtime_data': weekly_status['school_with_realtime_data'],
            'countries_with_realtime_data': weekly_status['countries_with_realtime_data'],
            'real_time_connected_schools': real_time_connected_schools,
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


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class CoverageAPIView(APIView):
    permission_classes = (AllowAny,)

    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'COVERAGE_STATS'

    model = School
    queryset = model.objects.all()

    filter_backends = (
        DjangoFilterBackend,
    )

    filterset_fields = {
        'country_id': ['exact', 'in'],
        'admin1_id': ['exact', 'in'],
        'id': ['exact', 'in'],
    }

    school_filters = []
    school_static_filters = []

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
            self.school_filters = core_utilities.get_filter_sql(self.request, 'schools', 'schools_school')
            self.school_static_filters = core_utilities.get_filter_sql(self.request, 'school_static',
                                                                       'connection_statistics_schoolweeklystatus')

            # Query the School table to get the coverage data
            # Get the total number of schools with coverage data
            # Get the count of schools falling under different coverage types
            queryset = self.filter_queryset(self.queryset)

            school_coverage_type_qry = queryset.annotate(
                dummy_group_by=Value(1)).values('dummy_group_by').annotate(
                g_4_5=Count(Case(When(coverage_type__in=['5g', '4g'], then='id')), distinct=True),
                g_2_3=Count(Case(When(coverage_type__in=['3g', '2g'], then='id')), distinct=True),
                no_coverage=Count(Case(When(coverage_type='no', then='id')), distinct=True),
                unknown=Count(Case(When(coverage_type__in=['unknown', None], then='id')), distinct=True),
                total_coverage_schools=Count(Case(When(coverage_type__isnull=False, then='id')), distinct=True),
            ).values('g_4_5', 'g_2_3', 'no_coverage', 'unknown', 'total_coverage_schools').order_by()

            if len(self.school_filters) > 0:
                school_coverage_type_qry = school_coverage_type_qry.extra(where=[self.school_filters])

            if len(self.school_static_filters) > 0:
                school_coverage_type_qry = school_coverage_type_qry.annotate(
                    total_weekly_schools=Count('last_weekly_status__school_id', distinct=True),
                ).values(
                    'g_4_5', 'g_2_3', 'no_coverage', 'unknown', 'total_coverage_schools', 'total_weekly_schools'
                ).extra(where=[self.school_static_filters])

            school_coverage_status = list(school_coverage_type_qry)[0]
            coverage_data = {
                '5g_4g': school_coverage_status['g_4_5'],
                '3g_2g': school_coverage_status['g_2_3'],
                'no_coverage': school_coverage_status['no_coverage'],
                'unknown': school_coverage_status['unknown'],
            }

            data = {
                'total_schools': school_coverage_status['total_coverage_schools'],
                'connected_schools': coverage_data,
            }

            cache_manager.set(cache_key, data, request_path=request_path, soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=data)


@method_decorator([cache_control(public=True, max_age=settings.CACHE_CONTROL_MAX_AGE_FOR_FE)], name='dispatch')
class ConnectivityConfigurationsViewSet(APIView):
    base_auth_permissions = (
        AllowAny,
    )

    model = SchoolDailyStatus
    queryset = model.objects.all()

    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'CONNECTIVITY_CONFIGURATIONS_STATS'

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}'.format(self.CACHE_KEY_PREFIX,
                                '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))), )

    def get(self, request, *args, **kwargs):
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

            today_date = core_utilities.get_current_datetime_object().date()
            monday_date = today_date - timedelta(days=today_date.weekday())

            latest_daily_entry = self.queryset.filter(
                date__lt=monday_date,
                school__deleted__isnull=True,
            ).values_list('date', flat=True).order_by('-date').first()

            if latest_daily_entry:
                monday_on_entry_date = latest_daily_entry - timedelta(days=latest_daily_entry.weekday())
                sunday_on_entry_date = monday_on_entry_date + timedelta(days=6)

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
                }

            request_path = remove_query_param(request.get_full_path(), 'cache')
            cache_manager.set(cache_key, static_data, request_path=request_path,
                              soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=static_data)


class CountrySummaryAPIViewSet(BaseModelViewSet):
    model = CountryWeeklyStatus
    serializer_class = statistics_serializers.CountryWeeklyStatusSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanAddCountry,
        core_permissions.CanUpdateCountry,
        core_permissions.CanViewCountry,
        core_permissions.CanDeleteCountry,
    )

    filter_backends = (
        DjangoFilterBackend,
        SearchFilter,
    )

    ordering_field_names = ['-year', '-week', 'country__name']
    apply_query_pagination = True
    search_fields = ('=country__id', 'country__name', 'year', 'week',)

    filterset_fields = {
        'country_id': ['exact', 'in'],
        'year': ['exact', 'in'],
        'week': ['exact', 'in'],
        'integration_status': ['exact', 'in'],
    }

    def get_serializer_class(self):
        if self.action == 'list':
            ser_class = statistics_serializers.ListCountryWeeklyStatusSerializer
        elif self.action in ['create', 'update', 'destroy']:
            ser_class = statistics_serializers.CountryWeeklyStatusUpdateRetrieveSerializer
        else:
            ser_class = statistics_serializers.DetailCountryWeeklyStatusSerializer
        return (ser_class)

    def get_object(self):
        if self.action == 'list':
            return get_object_or_404(self.queryset.filter(pk=self.kwargs.get('pk').lower()))

    def create(self, request, *args, **kwargs):
        try:
            data = statistics_serializers.CountryWeeklyStatusUpdateRetrieveSerializer(data=request.data)
            if data.is_valid():
                data.save()
                action_log(request, [data.data], 1, '', self.model, field_name='id')
                return Response(data.data)
            return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
        except Country.DoesNotExist:
            return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def update(self, request, pk):
        if pk is not None:
            try:
                country_weekly_status = CountryWeeklyStatus.objects.get(pk=pk)
                data = statistics_serializers.CountryWeeklyStatusUpdateRetrieveSerializer(
                    instance=country_weekly_status,
                    data=request.data,
                    partial=True)
                if data.is_valid(raise_exception=True):
                    change_message = changed_fields(country_weekly_status, request.data)
                    action_log(request, [country_weekly_status], 2, change_message, self.model, field_name='id')
                    data.save()
                    return Response(data.data)
                return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
            except CountryWeeklyStatus.DoesNotExist:
                return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)
        return Response(data=id_missing_error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def retrieve(self, request, pk):
        if pk is not None:
            try:
                country_weekly_status = CountryWeeklyStatus.objects.get(id=pk)
                if country_weekly_status:
                    serializer = statistics_serializers.CountryWeeklyStatusUpdateRetrieveSerializer(
                        country_weekly_status, partial=True,
                        context={'request': request}, )
                    return Response(serializer.data)
                return Response(status=rest_status.HTTP_404_NOT_FOUND, data=error_mess)
            except CountryWeeklyStatus.DoesNotExist:
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
        return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)

    def destroy(self, request, *args, **kwargs):
        try:
            pk_ids = request.data.get('id', [])
            if len(pk_ids) > 0:
                if CountryWeeklyStatus.objects.filter(id__in=pk_ids).exists():
                    country_weekly_statuses = CountryWeeklyStatus.objects.filter(id__in=pk_ids)
                    action_log(request, country_weekly_statuses, 3, "Country deleted", self.model,
                               field_name='id')

                    for country_weekly_status in list(country_weekly_statuses):
                        country_weekly_status.delete()
                    return Response(status=rest_status.HTTP_200_OK, data=delete_succ_mess)
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)
        except:
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)


class CountryDailyConnectivitySummaryAPIViewSet(BaseModelViewSet):
    model = CountryDailyStatus
    serializer_class = statistics_serializers.CountryDailyStatusSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanAddCountry,
        core_permissions.CanUpdateCountry,
        core_permissions.CanViewCountry,
        core_permissions.CanDeleteCountry,
    )

    filter_backends = (
        DjangoFilterBackend,
        SearchFilter,
    )

    ordering_field_names = ['-date', 'country__name', ]
    apply_query_pagination = True
    search_fields = ('=country__id', 'country__name',)

    filterset_fields = {
        'country_id': ['exact', 'in'],
    }

    def get_serializer_class(self):
        if self.action == 'list':
            ser_class = statistics_serializers.ListCountryDailyStatusSerializer
        elif self.action in ['create', 'update', 'destroy']:
            ser_class = statistics_serializers.CountryDailyStatusUpdateRetrieveSerializer
        else:
            ser_class = statistics_serializers.DetailCountryDailyStatusSerializer
        return (ser_class)

    def get_object(self):
        if self.action == 'list':
            return get_object_or_404(self.queryset.filter(pk=self.kwargs.get('pk').lower()))

    def create(self, request, *args, **kwargs):
        try:
            data = statistics_serializers.CountryDailyStatusUpdateRetrieveSerializer(data=request.data)
            if data.is_valid():
                data.save()
                action_log(request, [data.data], 1, '', self.model, field_name='id')
                return Response(data.data)
            return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
        except Country.DoesNotExist:
            return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def update(self, request, pk):
        if pk is not None:
            try:
                country_daily_status = CountryDailyStatus.objects.get(pk=pk)
                data = statistics_serializers.CountryDailyStatusUpdateRetrieveSerializer(instance=country_daily_status,
                                                                                         data=request.data,
                                                                                         partial=True)
                if data.is_valid(raise_exception=True):
                    change_message = changed_fields(country_daily_status, request.data)
                    action_log(request, [country_daily_status], 2, change_message, self.model, field_name='id')
                    data.save()
                    return Response(data.data)
                return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
            except CountryDailyStatus.DoesNotExist:
                return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)
        return Response(data=id_missing_error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def retrieve(self, request, pk):
        if pk is not None:
            try:
                country_daily_status = CountryDailyStatus.objects.get(id=pk)
                if country_daily_status:
                    serializer = statistics_serializers.CountryDailyStatusUpdateRetrieveSerializer(
                        country_daily_status,
                        partial=True,
                        context={'request': request},
                    )
                    return Response(serializer.data)
                return Response(status=rest_status.HTTP_404_NOT_FOUND, data=error_mess)
            except CountryDailyStatus.DoesNotExist:
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
        return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)

    def destroy(self, request, *args, **kwargs):
        try:
            pk_ids = request.data.get('id', [])
            if len(pk_ids) > 0:
                if CountryDailyStatus.objects.filter(id__in=pk_ids).exists():
                    country_daily_statuses = CountryDailyStatus.objects.filter(id__in=pk_ids)
                    action_log(request, country_daily_statuses, 3, "Country deleted", self.model,
                               field_name='id')

                    for country_daily_status in list(country_daily_statuses):
                        country_daily_status.delete()
                    return Response(status=rest_status.HTTP_200_OK, data=delete_succ_mess)
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)
        except:
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)


class SchoolSummaryAPIViewSet(BaseModelViewSet):
    model = SchoolWeeklyStatus
    serializer_class = statistics_serializers.SchoolWeeklyStatusSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanAddSchool,
        core_permissions.CanUpdateSchool,
        core_permissions.CanViewSchool,
        core_permissions.CanDeleteSchool,
    )

    filter_backends = (
        DjangoFilterBackend,
        SearchFilter,
    )

    ordering_field_names = ['-year', '-week', 'school__name_lower', ]
    apply_query_pagination = True
    search_fields = (
        '=school__id',
        'school__name_lower',
        '=school__giga_id_school',
        '=school__external_id',
        'year',
        'week',
    )
    filterset_fields = {
        'school_id': ['exact', 'in'],
        'year': ['exact', 'in'],
        'week': ['exact', 'in'],
    }

    def get_serializer_class(self):
        if self.action == 'list':
            ser_class = statistics_serializers.ListSchoolWeeklySummarySerializer
        elif self.action in ['create', 'update', 'destroy']:
            ser_class = statistics_serializers.SchoolWeeklySummaryUpdateRetrieveSerializer
        else:
            ser_class = statistics_serializers.DetailSchoolWeeklySummarySerializer
        return (ser_class)

    def get_object(self):
        if self.action == 'list':
            return get_object_or_404(self.queryset.filter(pk=self.kwargs.get('pk').lower()))

    def get_queryset(self):
        queryset = super(SchoolSummaryAPIViewSet, self).get_queryset()
        if self.action == 'list' and self.request.query_params.get('country_id'):
            queryset = queryset.filter(school__country_id__in=self.request.query_params.get('country_id').split(','))
        return queryset

    def create(self, request, *args, **kwargs):
        try:
            data = statistics_serializers.SchoolWeeklySummaryUpdateRetrieveSerializer(data=request.data)
            if data.is_valid():
                data.save()
                action_log(request, [data.data], 1, '', self.model, field_name='id')
                return Response(data.data)
            return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
        except Country.DoesNotExist:
            return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def update(self, request, pk):
        if pk is not None:
            try:
                school_weekly_status = SchoolWeeklyStatus.objects.get(pk=pk)
                data = statistics_serializers.SchoolWeeklySummaryUpdateRetrieveSerializer(instance=school_weekly_status,
                                                                                          data=request.data,
                                                                                          partial=True)
                if data.is_valid(raise_exception=True):
                    change_message = changed_fields(school_weekly_status, request.data)
                    action_log(request, [school_weekly_status], 2, change_message, self.model, field_name='id')
                    data.save()
                    return Response(data.data)
                return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
            except SchoolWeeklyStatus.DoesNotExist:
                return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)
        return Response(data=id_missing_error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def retrieve(self, request, pk):
        if pk is not None:
            try:
                school_weekly_status = SchoolWeeklyStatus.objects.get(id=pk)
                if school_weekly_status:
                    serializer = statistics_serializers.SchoolWeeklySummaryUpdateRetrieveSerializer(
                        school_weekly_status, partial=True,
                        context={'request': request}, )
                    return Response(serializer.data)
                return Response(status=rest_status.HTTP_404_NOT_FOUND, data=error_mess)
            except SchoolWeeklyStatus.DoesNotExist:
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
        return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)

    def destroy(self, request, *args, **kwargs):
        try:
            pk_ids = request.data.get('id', [])
            if len(pk_ids) > 0:
                if SchoolWeeklyStatus.objects.filter(id__in=pk_ids).exists():
                    school_weekly_statuses = SchoolWeeklyStatus.objects.filter(id__in=pk_ids)
                    action_log(request, school_weekly_statuses, 3, "Country deleted", self.model,
                               field_name='id')

                    for school_weekly_status in list(school_weekly_statuses):
                        school_weekly_status.delete()
                    return Response(status=rest_status.HTTP_200_OK, data=delete_succ_mess)
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)
        except:
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)


class SchoolDailyConnectivitySummaryAPIViewSet(BaseModelViewSet):
    model = SchoolDailyStatus
    serializer_class = statistics_serializers.SchoolDailyStatusSerializer

    permission_classes = (
        core_permissions.IsUserAuthenticated,
        core_permissions.CanAddSchool,
        core_permissions.CanUpdateSchool,
        core_permissions.CanViewSchool,
        core_permissions.CanDeleteSchool,
    )

    filter_backends = (
        DjangoFilterBackend,
        SearchFilter,
    )

    ordering_field_names = ['-date', 'school__name_lower', ]
    apply_query_pagination = True

    search_fields = (
        '=school__id',
        'school__name_lower',
        '=school__giga_id_school',
        '=school__external_id',
    )
    filterset_fields = {
        'school_id': ['exact', 'in'],
    }

    def get_serializer_class(self):
        if self.action == 'list':
            ser_class = statistics_serializers.ListSchoolDailyStatusSerializer
        elif self.action in ['create', 'update', 'destroy']:
            ser_class = statistics_serializers.SchoolDailyStatusUpdateRetrieveSerializer
        else:
            ser_class = statistics_serializers.DetailSchoolDailyStatusSerializer
        return (ser_class)

    def get_object(self):
        if self.action == 'list':
            return get_object_or_404(self.queryset.filter(pk=self.kwargs.get('pk').lower()))

    def get_queryset(self):
        queryset = super(SchoolDailyConnectivitySummaryAPIViewSet, self).get_queryset()
        if self.action == 'list' and self.request.query_params.get('country_id'):
            queryset = queryset.filter(school__country_id__in=self.request.query_params.get('country_id').split(','))
        return queryset

    def create(self, request, *args, **kwargs):
        try:
            data = statistics_serializers.SchoolDailyStatusUpdateRetrieveSerializer(data=request.data)
            if data.is_valid():
                data.save()
                action_log(request, [data.data], 1, '', self.model, field_name='id')
                return Response(data.data)
            return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
        except Country.DoesNotExist:
            return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def update(self, request, pk):
        if pk is not None:
            try:
                school_daily_status = SchoolDailyStatus.objects.get(pk=pk)
                data = statistics_serializers.SchoolDailyStatusUpdateRetrieveSerializer(instance=school_daily_status,
                                                                                        data=request.data, partial=True)
                if data.is_valid(raise_exception=True):
                    change_message = changed_fields(school_daily_status, request.data)
                    action_log(request, [school_daily_status], 2, change_message, self.model, field_name='id')
                    data.save()
                    return Response(data.data)
                return Response(data.errors, status=rest_status.HTTP_502_BAD_GATEWAY)
            except SchoolDailyStatus.DoesNotExist:
                return Response(data=error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)
        return Response(data=id_missing_error_mess, status=rest_status.HTTP_502_BAD_GATEWAY)

    def retrieve(self, request, pk):
        if pk is not None:
            try:
                school_daily_status = SchoolDailyStatus.objects.get(id=pk)
                if school_daily_status:
                    serializer = statistics_serializers.SchoolDailyStatusUpdateRetrieveSerializer(
                        school_daily_status,
                        partial=True,
                        context={'request': request},
                    )
                    return Response(serializer.data)
                return Response(status=rest_status.HTTP_404_NOT_FOUND, data=error_mess)
            except SchoolDailyStatus.DoesNotExist:
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
        return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)

    def destroy(self, request, *args, **kwargs):
        try:
            pk_ids = request.data.get('id', [])
            if len(pk_ids) > 0:
                if SchoolDailyStatus.objects.filter(id__in=pk_ids).exists():
                    school_daily_statuses = SchoolDailyStatus.objects.filter(id__in=pk_ids)
                    action_log(request, school_daily_statuses, 3, "Country deleted", self.model,
                               field_name='id')

                    for school_daily_status in list(school_daily_statuses):
                        school_daily_status.delete()
                    return Response(status=rest_status.HTTP_200_OK, data=delete_succ_mess)
                return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=id_missing_error_mess)
        except:
            return Response(status=rest_status.HTTP_502_BAD_GATEWAY, data=error_mess)


class TimePlayerViewSet(ListAPIView):
    permission_classes = (AllowAny,)

    CACHE_KEY = 'cache'
    CACHE_KEY_PREFIX = 'COUNTRY_TIME_PLAYER_DATA'

    def get_cache_key(self):
        params = dict(self.request.query_params)
        params.pop(self.CACHE_KEY, None)
        return '{0}_{1}'.format(self.CACHE_KEY_PREFIX,
                                '_'.join(map(lambda x: '{0}_{1}'.format(x[0], x[1]), sorted(params.items()))), )

    def get_live_query(self, **kwargs):
        query = """
        SELECT DISTINCT s.id AS school_id,
          s.geopoint,
          EXTRACT(YEAR FROM CAST(t.date AS DATE)) AS year,
          CASE
              WHEN AVG(t."{col_name}") > {benchmark_value} THEN 'good'
              WHEN AVG(t."{col_name}") < {benchmark_value}
                   and AVG(t."{col_name}") >= {base_benchmark} THEN 'moderate'
              WHEN AVG(t."{col_name}") < {base_benchmark} THEN 'bad'
              ELSE 'unknown'
          END AS field_status,
          CASE WHEN rt_status.rt_registered = True
            AND EXTRACT(YEAR FROM CAST(rt_status.rt_registration_date AS DATE)) <=
                EXTRACT(YEAR FROM CAST(t.date AS DATE))
            THEN True ELSE False END as is_rt_connected
        FROM schools_school AS s
        INNER JOIN connection_statistics_schooldailystatus t ON s.id = t.school_id
        LEFT JOIN connection_statistics_schoolrealtimeregistration rt_status ON rt_status.school_id = s.id
        WHERE s.deleted IS NULL
         AND t.deleted IS NULL
         AND rt_status.deleted IS NULL
         AND s.country_id = {country_id}
         AND EXTRACT(YEAR FROM CAST(t.date AS DATE)) >= {start_year}
         AND t.live_data_source IN ({live_source_types})
        GROUP BY s.id, year, is_rt_connected
        ORDER BY s.id ASC, year ASC
        """

        return query.format(**kwargs)

    def _format_result(self, qry_data):
        data = OrderedDict()
        if qry_data:
            for resp_data in qry_data:
                school_id = resp_data.get('school_id')

                school_data = data.get(school_id, {
                    'school_id': school_id,
                    'geopoint': resp_data.get('geopoint'),
                })
                school_data[int(resp_data.get('year'))] = {
                    'field_status': resp_data.get('field_status'),
                    'is_rt_connected': resp_data.get('is_rt_connected'),
                }
                data[school_id] = school_data
        return list(data.values())

    def list(self, request, *args, **kwargs):
        use_cached_data = self.request.query_params.get(self.CACHE_KEY, 'on').lower() in ['on', 'true']
        request_path = remove_query_param(request.get_full_path(), 'cache')
        cache_key = self.get_cache_key()

        data = None
        if use_cached_data:
            data = cache_manager.get(cache_key)

        if not data:
            layer_id = request.query_params.get('layer_id')
            country_id = request.query_params.get('country_id')

            data_layer_instance = get_object_or_404(
                DataLayer.objects.all(),
                pk=layer_id,
                status=DataLayer.LAYER_STATUS_PUBLISHED,
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
            base_benchmark = str(parameter_col.get('base_benchmark', 1))

            benchmark_val = data_layer_instance.global_benchmark.get('value')

            query_kwargs = {
                'country_id': country_id,
                'col_name': parameter_column_name,
                'benchmark_value': benchmark_val,
                'base_benchmark': base_benchmark,
                'live_source_types': ','.join(["'" + str(source) + "'" for source in set(live_data_sources)]),
                'start_year': request.query_params.get('start_year', date_utilities.get_current_year() - 4)
            }

            query_data = db_utilities.sql_to_response(self.get_live_query(**query_kwargs),
                                                      label=self.__class__.__name__,
                                                      db_var=settings.READ_ONLY_DB_KEY)

            data = self._format_result(query_data)
            cache_manager.set(cache_key, data, request_path=request_path, soft_timeout=settings.CACHE_CONTROL_MAX_AGE)

        return Response(data=data)
