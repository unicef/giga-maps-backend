import json
import re
from datetime import timedelta

from django.db.models import Avg, Q

from proco.accounts.models import DataLayer
from proco.connection_statistics.aggregations import (
    aggregate_connectivity_by_availability,
    aggregate_connectivity_by_speed,
    aggregate_connectivity_default,
    aggregate_coverage_by_availability,
    aggregate_coverage_by_types,
    aggregate_coverage_default,
)
from proco.connection_statistics.models import (
    CountryDailyStatus,
    CountryWeeklyStatus,
    RealTimeConnectivity,
    SchoolDailyStatus,
    SchoolWeeklyStatus,
)
from proco.core.utils import convert_to_int, get_current_datetime_object
from proco.locations.models import Country
from proco.schools.constants import ColorMapSchema
from proco.schools.constants import statuses_schema
from proco.schools.models import School
from proco.utils import dates as date_utilities


def aggregate_real_time_data_to_school_daily_status(country, date):
    schools = RealTimeConnectivity.objects.all().filter(
        created__date=date, school__country=country,
    ).order_by('school').values_list('school', flat=True).order_by('school_id').distinct('school_id')

    for school in schools:
        aggregate_by_source = RealTimeConnectivity.objects.filter(
            created__date=date, school=school,
        ).values('live_data_source').annotate(
            connectivity_speed_avg=Avg('connectivity_speed'),
            connectivity_latency_avg=Avg('connectivity_latency'),
            roundtrip_time_avg=Avg('roundtrip_time'),
            jitter_download_avg=Avg('jitter_download'),
            jitter_upload_avg=Avg('jitter_upload'),
            rtt_packet_loss_pct_avg=Avg('rtt_packet_loss_pct'),
            connectivity_speed_probe_avg=Avg('connectivity_speed_probe'),
            connectivity_upload_speed_probe_avg=Avg('connectivity_upload_speed_probe'),
            connectivity_latency_probe_avg=Avg('connectivity_latency_probe'),
            connectivity_speed_mean_avg=Avg('connectivity_speed_mean'),
            connectivity_upload_speed_mean_avg=Avg('connectivity_upload_speed_mean'),
            connectivity_upload_speed_avg=Avg('connectivity_upload_speed'),
        ).order_by()

        for source_agg in aggregate_by_source:
            SchoolDailyStatus.objects.update_or_create(
                school_id=school,
                date=date,
                live_data_source=source_agg['live_data_source'],
                defaults={
                    'connectivity_speed': source_agg['connectivity_speed_avg'],
                    'connectivity_latency': source_agg['connectivity_latency_avg'],
                    'roundtrip_time': source_agg['roundtrip_time_avg'],
                    'jitter_download': source_agg['jitter_download_avg'],
                    'jitter_upload': source_agg['jitter_upload_avg'],
                    'rtt_packet_loss_pct': source_agg['rtt_packet_loss_pct_avg'],
                    'connectivity_speed_probe': source_agg['connectivity_speed_probe_avg'],
                    'connectivity_upload_speed_probe': source_agg['connectivity_upload_speed_probe_avg'],
                    'connectivity_latency_probe': source_agg['connectivity_latency_probe_avg'],
                    'connectivity_speed_mean': source_agg['connectivity_speed_mean_avg'],
                    'connectivity_upload_speed_mean': source_agg['connectivity_upload_speed_mean_avg'],
                    'connectivity_upload_speed': source_agg['connectivity_upload_speed_avg'],
                    'deleted': None,
                },
            )


def aggregate_school_daily_to_country_daily(country, date) -> bool:
    aggregate_by_source = SchoolDailyStatus.objects.all_records().filter(
        school__country=country, date=date,
    ).values('live_data_source').annotate(
        connectivity_speed_avg=Avg('connectivity_speed'),
        connectivity_latency_avg=Avg('connectivity_latency'),
        roundtrip_time_avg=Avg('roundtrip_time'),
        jitter_download_avg=Avg('jitter_download'),
        jitter_upload_avg=Avg('jitter_upload'),
        rtt_packet_loss_pct_avg=Avg('rtt_packet_loss_pct'),
        connectivity_speed_probe_avg=Avg('connectivity_speed_probe'),
        connectivity_upload_speed_probe_avg=Avg('connectivity_upload_speed_probe'),
        connectivity_latency_probe_avg=Avg('connectivity_latency_probe'),
        connectivity_speed_mean_avg=Avg('connectivity_speed_mean'),
        connectivity_upload_speed_mean_avg=Avg('connectivity_upload_speed_mean'),
        connectivity_upload_speed_avg=Avg('connectivity_upload_speed'),
    ).order_by()

    # Adding this check because soft deletion is in place now from 2024
    if date_utilities.get_year_from_date(date) >= 2024:
        aggregate_by_source = aggregate_by_source.filter(school__deleted__isnull=True, deleted__isnull=True)

    updated = False

    for source_agg in aggregate_by_source:
        CountryDailyStatus.objects.update_or_create(
            country=country,
            date=date,
            live_data_source=source_agg['live_data_source'],
            defaults={
                'connectivity_speed': source_agg['connectivity_speed_avg'],
                'connectivity_latency': source_agg['connectivity_latency_avg'],
                'roundtrip_time': source_agg['roundtrip_time_avg'],
                'jitter_download': source_agg['jitter_download_avg'],
                'jitter_upload': source_agg['jitter_upload_avg'],
                'rtt_packet_loss_pct': source_agg['rtt_packet_loss_pct_avg'],
                'connectivity_speed_probe': source_agg['connectivity_speed_probe_avg'],
                'connectivity_upload_speed_probe': source_agg['connectivity_upload_speed_probe_avg'],
                'connectivity_latency_probe': source_agg['connectivity_latency_probe_avg'],
                'connectivity_speed_mean': source_agg['connectivity_speed_mean_avg'],
                'connectivity_upload_speed_mean': source_agg['connectivity_upload_speed_mean_avg'],
                'connectivity_upload_speed': source_agg['connectivity_upload_speed_avg'],
                'deleted': None,
            },
        )
        updated = True

    return updated


def aggregate_school_daily_status_to_school_weekly_status(country, date) -> bool:
    monday_date = date - timedelta(days=date.weekday())
    sunday_date = monday_date + timedelta(days=6)

    monday_week_no = date_utilities.get_week_from_date(monday_date)
    monday_year = date_utilities.get_year_from_date(monday_date)

    school_ids = SchoolDailyStatus.objects.all().filter(
        date__range=[monday_date, sunday_date],
        school__country=country,
        school__deleted__isnull=True
    ).values_list('school', flat=True).order_by('school_id').distinct('school_id')

    updated = False

    for school_id in school_ids:
        updated = True
        created = False

        school_weekly = SchoolWeeklyStatus.objects.filter(
            school_id=school_id, week=monday_week_no, year=monday_year,
        ).last()

        if not school_weekly:
            school_weekly = SchoolWeeklyStatus.objects.create(
                school_id=school_id,
                year=monday_year,
                week=monday_week_no,
            )
            created = True

        aggregate_qs = SchoolDailyStatus.objects.all().filter(
            school_id=school_id, date__range=[monday_date, sunday_date],
        )
        # Adding this check because soft deletion is in place now from 2024
        if date_utilities.get_year_from_date(date) >= 2024:
            aggregate_qs = aggregate_qs.filter(school__deleted__isnull=True)

        aggregate = aggregate_qs.aggregate(
            Avg('connectivity_speed'), Avg('connectivity_upload_speed'), Avg('connectivity_latency'),
            Avg('roundtrip_time'), Avg('jitter_download'), Avg('jitter_upload'), Avg('rtt_packet_loss_pct'),
            Avg('connectivity_speed_probe'), Avg('connectivity_upload_speed_probe'), Avg('connectivity_latency_probe'),
            Avg('connectivity_speed_mean'), Avg('connectivity_upload_speed_mean'),
        )

        school_weekly.modified = get_current_datetime_object()
        school_weekly.connectivity = True
        school_weekly.connectivity_speed = aggregate['connectivity_speed__avg']
        school_weekly.connectivity_latency = aggregate['connectivity_latency__avg']
        school_weekly.roundtrip_time = aggregate['roundtrip_time__avg']
        school_weekly.jitter_download = aggregate['jitter_download__avg']
        school_weekly.jitter_upload = aggregate['jitter_upload__avg']
        school_weekly.rtt_packet_loss_pct = aggregate['rtt_packet_loss_pct__avg']
        school_weekly.connectivity_speed_probe = aggregate['connectivity_speed_probe__avg']
        school_weekly.connectivity_upload_speed_probe = aggregate['connectivity_upload_speed_probe__avg']
        school_weekly.connectivity_latency_probe = aggregate['connectivity_latency_probe__avg']
        school_weekly.connectivity_speed_mean = aggregate['connectivity_speed_mean__avg']
        school_weekly.connectivity_upload_speed_mean = aggregate['connectivity_upload_speed_mean__avg']
        school_weekly.connectivity_upload_speed = aggregate['connectivity_upload_speed__avg']

        if created:
            prev_weekly = SchoolWeeklyStatus.objects.all().filter(
                school_id=school_id, date__lt=school_weekly.date,
            ).last()

            if prev_weekly:
                school_weekly.num_students = prev_weekly.num_students
                school_weekly.num_teachers = prev_weekly.num_teachers
                school_weekly.num_classroom = prev_weekly.num_classroom
                school_weekly.num_latrines = prev_weekly.num_latrines
                school_weekly.running_water = prev_weekly.running_water
                school_weekly.electricity_availability = prev_weekly.electricity_availability
                school_weekly.computer_lab = prev_weekly.computer_lab
                school_weekly.num_computers = prev_weekly.num_computers

                school_weekly.connectivity_type = prev_weekly.connectivity_type
                school_weekly.coverage_availability = prev_weekly.coverage_availability
                school_weekly.coverage_type = prev_weekly.coverage_type

                school_weekly.download_speed_contracted = prev_weekly.download_speed_contracted
                school_weekly.num_computers_desired = prev_weekly.num_computers_desired
                school_weekly.electricity_type = prev_weekly.electricity_type
                school_weekly.num_adm_personnel = prev_weekly.num_adm_personnel

                school_weekly.fiber_node_distance = prev_weekly.fiber_node_distance
                school_weekly.microwave_node_distance = prev_weekly.microwave_node_distance

                school_weekly.schools_within_1km = prev_weekly.schools_within_1km
                school_weekly.schools_within_2km = prev_weekly.schools_within_2km
                school_weekly.schools_within_3km = prev_weekly.schools_within_3km

                school_weekly.nearest_lte_distance = prev_weekly.nearest_lte_distance
                school_weekly.nearest_umts_distance = prev_weekly.nearest_umts_distance
                school_weekly.nearest_gsm_distance = prev_weekly.nearest_gsm_distance
                school_weekly.nearest_nr_distance = prev_weekly.nearest_nr_distance

                school_weekly.pop_within_1km = prev_weekly.pop_within_1km
                school_weekly.pop_within_2km = prev_weekly.pop_within_2km
                school_weekly.pop_within_3km = prev_weekly.pop_within_3km

                school_weekly.school_data_source = prev_weekly.school_data_source
                school_weekly.school_data_collection_year = prev_weekly.school_data_collection_year
                school_weekly.school_data_collection_modality = prev_weekly.school_data_collection_modality
                school_weekly.school_location_ingestion_timestamp = prev_weekly.school_location_ingestion_timestamp
                school_weekly.connectivity_govt_ingestion_timestamp = prev_weekly.connectivity_govt_ingestion_timestamp
                school_weekly.connectivity_govt_collection_year = prev_weekly.connectivity_govt_collection_year
                school_weekly.disputed_region = prev_weekly.disputed_region

                school_weekly.download_speed_benchmark = prev_weekly.download_speed_benchmark

        school_weekly.save()

    return updated


def update_country_weekly_status(country: Country, date):
    monday_date = date - timedelta(days=date.weekday())

    monday_week_no = date_utilities.get_week_from_date(monday_date)
    monday_year = date_utilities.get_year_from_date(monday_date)

    last_weekly_status_country = country.last_weekly_status
    if not last_weekly_status_country:
        last_weekly_status_country = CountryWeeklyStatus.objects.all().filter(
            country=country).order_by('-year', '-week').first()

    created = False

    country_status = CountryWeeklyStatus.objects.filter(
        country=country, year=monday_year, week=monday_week_no,
    ).last()

    if not country_status:
        country_status = CountryWeeklyStatus.objects.create(
            country=country, year=monday_year, week=monday_week_no,
        )
        created = True

    if created:
        if (
            last_weekly_status_country and
            last_weekly_status_country.year <= country_status.year and
            last_weekly_status_country.week < country_status.week
        ):
            # If CountryWeekly record created newly, then check if its latest Year+Week.
            # If it is latest, then only set in Country.last_weekly_status field.
            # Otherwise, keep the existing last_weekly_status_id as it is.
            country.last_weekly_status = country_status
            country.save()

            country.last_weekly_status.integration_status = last_weekly_status_country.integration_status
            country.last_weekly_status.save(update_fields=('integration_status',))
        elif not last_weekly_status_country:
            # If current CountryWeekly is the only record for this country, set it as last_weekly_status_id
            country.last_weekly_status = country_status
            country.save()
    elif not country.last_weekly_status:
        # If CountryWeekly record not created newly abd last_weekly_status_id is null, then set it to latest available
        # record
        country.last_weekly_status = last_weekly_status_country
        country.save()

    # calculate pie charts. first we need to understand which case is applicable for country
    latest_statuses = SchoolWeeklyStatus.objects.all_records().filter(
        school__country=country,
        year=monday_year,
        week=monday_week_no,
    )
    # Adding this check because soft deletion is in place now from 2024
    if date_utilities.get_year_from_date(monday_date) >= 2024:
        latest_statuses = latest_statuses.filter(school__deleted__isnull=True, deleted__isnull=True)

    connectivity_types = CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY
    coverage_types = CountryWeeklyStatus.COVERAGE_TYPES_AVAILABILITY

    if RealTimeConnectivity.objects.all().filter(school__country=country, school__deleted__isnull=True).exists():
        country_status.connectivity_availability = connectivity_types.realtime_speed
        connectivity_stats = aggregate_connectivity_by_speed(latest_statuses)
    elif latest_statuses.filter(connectivity_speed__gte=0).exists():
        country_status.connectivity_availability = connectivity_types.static_speed
        connectivity_stats = aggregate_connectivity_by_speed(latest_statuses)
    elif latest_statuses.filter(connectivity__isnull=False).exists():
        country_status.connectivity_availability = connectivity_types.connectivity
        connectivity_stats = aggregate_connectivity_by_availability(latest_statuses)
    else:
        country_status.connectivity_availability = connectivity_types.no_connectivity
        connectivity_stats = aggregate_connectivity_default(latest_statuses)

    if latest_statuses.exclude(coverage_type=SchoolWeeklyStatus.COVERAGE_TYPES.unknown).exists():
        country_status.coverage_availability = coverage_types.coverage_type
        coverage_stats = aggregate_coverage_by_types(latest_statuses)
    elif latest_statuses.filter(coverage_availability__isnull=False).exists():
        country_status.coverage_availability = coverage_types.coverage_availability
        coverage_stats = aggregate_coverage_by_availability(latest_statuses)
    else:
        country_status.coverage_availability = coverage_types.no_coverage
        coverage_stats = aggregate_coverage_default(latest_statuses)

    # Adding this check because soft deletion is in place now from 2024
    if date_utilities.get_year_from_date(monday_date) >= 2024:
        country_status.schools_total = School.objects.filter(country=country).count()
    else:
        country_status.schools_total = School.objects.all_records().filter(country=country).count()

    # remember connectivity pie chart
    country_status.schools_connectivity_good = connectivity_stats[ColorMapSchema.GOOD]
    country_status.schools_connectivity_moderate = connectivity_stats[ColorMapSchema.MODERATE]
    country_status.schools_connectivity_no = connectivity_stats[ColorMapSchema.NO]

    schools_with_data = country_status.schools_connectivity_moderate + country_status.schools_connectivity_good
    country_status.schools_connected = schools_with_data
    if country_status.schools_total:
        country_status.schools_with_data_percentage = 1.0 * country_status.schools_connected
        country_status.schools_with_data_percentage /= country_status.schools_total
    else:
        country_status.schools_with_data_percentage = 0

    schools_connectivity_known = schools_with_data + country_status.schools_connectivity_no
    country_status.schools_connectivity_unknown = country_status.schools_total - schools_connectivity_known

    # remember coverage pie chart
    country_status.schools_coverage_good = coverage_stats[ColorMapSchema.GOOD]
    country_status.schools_coverage_moderate = coverage_stats[ColorMapSchema.MODERATE]
    country_status.schools_coverage_no = coverage_stats[ColorMapSchema.NO]

    schools_coverage_known = (country_status.schools_coverage_good + country_status.schools_coverage_moderate +
                              country_status.schools_coverage_no)
    country_status.schools_coverage_unknown = country_status.schools_total - schools_coverage_known

    # calculate speed & latency where available
    schools_stats = latest_statuses.aggregate(
        connectivity_speed=Avg('connectivity_speed', filter=Q(connectivity_speed__gt=0)),
        connectivity_upload_speed=Avg('connectivity_upload_speed', filter=Q(connectivity_upload_speed__gt=0)),
        connectivity_latency=Avg('connectivity_latency', filter=Q(connectivity_latency__gt=0)),
        roundtrip_time=Avg('roundtrip_time', filter=Q(roundtrip_time__gt=0)),
        jitter_download=Avg('jitter_download', filter=Q(jitter_download__gt=0)),
        jitter_upload=Avg('jitter_upload', filter=Q(jitter_upload__gt=0)),
        rtt_packet_loss_pct=Avg('rtt_packet_loss_pct', filter=Q(rtt_packet_loss_pct__gt=0)),
        connectivity_speed_probe=Avg('connectivity_speed_probe',
                                     filter=Q(connectivity_speed_probe__gt=0)),
        connectivity_upload_speed_probe=Avg('connectivity_upload_speed_probe',
                                            filter=Q(connectivity_upload_speed_probe__gt=0)),
        connectivity_latency_probe=Avg('connectivity_latency_probe',
                                       filter=Q(connectivity_latency_probe__gt=0)),
        connectivity_speed_mean=Avg('connectivity_speed_mean',
                                    filter=Q(connectivity_speed_mean__gt=0)),
        connectivity_upload_speed_mean=Avg('connectivity_upload_speed_mean',
                                           filter=Q(connectivity_upload_speed_mean__gt=0)),
    )

    country_status.connectivity_speed = schools_stats['connectivity_speed']
    country_status.connectivity_latency = schools_stats['connectivity_latency']
    country_status.roundtrip_time = schools_stats['roundtrip_time']
    country_status.jitter_download = schools_stats['jitter_download']
    country_status.jitter_upload = schools_stats['jitter_upload']
    country_status.rtt_packet_loss_pct = schools_stats['rtt_packet_loss_pct']
    country_status.connectivity_speed_probe = schools_stats['connectivity_speed_probe']
    country_status.connectivity_upload_speed_probe = schools_stats['connectivity_upload_speed_probe']
    country_status.connectivity_latency_probe = schools_stats['connectivity_latency_probe']
    country_status.connectivity_speed_mean = schools_stats['connectivity_speed_mean']
    country_status.connectivity_upload_speed_mean = schools_stats['connectivity_upload_speed_mean']
    country_status.connectivity_upload_speed = schools_stats['connectivity_upload_speed']

    # move country status as far as we can
    if country_status.integration_status == CountryWeeklyStatus.COUNTRY_CREATED and country_status.schools_total:
        country_status.integration_status = CountryWeeklyStatus.SCHOOL_OSM_MAPPED

    if country_status.integration_status == CountryWeeklyStatus.JOINED and country_status.schools_total:
        country_status.integration_status = CountryWeeklyStatus.SCHOOL_MAPPED

    if country_status.integration_status == CountryWeeklyStatus.SCHOOL_MAPPED and any([
        country_status.schools_connectivity_good,
        country_status.schools_connectivity_moderate,
        country_status.schools_connectivity_no,
        country_status.schools_coverage_good,
        country_status.schools_coverage_moderate,
        country_status.schools_coverage_no,
    ]):
        country_status.integration_status = CountryWeeklyStatus.STATIC_MAPPED

    if (
        country_status.integration_status == CountryWeeklyStatus.STATIC_MAPPED and
        country_status.connectivity_availability == connectivity_types.realtime_speed
    ):
        country_status.integration_status = CountryWeeklyStatus.REALTIME_MAPPED

    country_status.avg_distance_school = country.calculate_avg_distance_school()

    country_status.save()


def update_country_data_source_by_csv_filename(imported_file):
    match = re.search(r'-(\D+)(?:-\d+)*-[^-]+\.\w+$', imported_file.filename)  # noqa: DUO138
    if match:
        source = match.group(1)
    else:
        source = '.'.join(imported_file.filename.split('.')[:-1])
    pretty_source = source.replace('_', ' ')
    if imported_file.country.data_source:
        if pretty_source.lower() not in imported_file.country.data_source.lower():
            imported_file.country.data_source += f'\n{pretty_source.capitalize()}'
    else:
        imported_file.country.data_source = pretty_source.capitalize()

    imported_file.country.save()


def get_benchmark_value_for_default_download_layer(benchmark, country_id):
    data_layer_instance = DataLayer.objects.filter(
        type=DataLayer.LAYER_TYPE_LIVE,
        category=DataLayer.LAYER_CATEGORY_CONNECTIVITY,
        status=DataLayer.LAYER_STATUS_PUBLISHED,
        created_by__isnull=True,
    ).first()

    benchmark_unit = 'bps'
    benchmark_val = 20000000
    if benchmark == 'national':
        benchmark_val = statuses_schema.CONNECTIVITY_SPEED_FOR_GOOD_CONNECTIVITY_STATUS

    if data_layer_instance:
        benchmark_val = data_layer_instance.global_benchmark.get('value')
        benchmark_unit = data_layer_instance.global_benchmark.get('unit')

        if benchmark == 'national' and country_id:
            benchmark_metadata = Country.objects.all().filter(
                id=country_id,
                benchmark_metadata__isnull=False,
            ).order_by('id').values_list('benchmark_metadata', flat=True).first()

            if benchmark_metadata and len(benchmark_metadata) > 0:
                benchmark_metadata = json.loads(benchmark_metadata)
                all_live_layers = benchmark_metadata.get('live_layer', {})
                if len(all_live_layers) > 0 and str(data_layer_instance.id) in (all_live_layers.keys()):
                    benchmark_val = all_live_layers[str(data_layer_instance.id)]

    return convert_to_int(str(benchmark_val), default='20000000'), benchmark_unit
