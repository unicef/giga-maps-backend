from datetime import timedelta

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db.models import Avg
from django.utils import timezone

from proco.connection_statistics import models as statistics_models
from proco.core.utils import get_current_datetime_object
from proco.schools.models import School
from proco.utils import dates as date_utilities


def aggregate_school_daily_status_from_old_school_to_new_school(old_school_id, new_school_id, record_date):
    aggregate_by_source = statistics_models.SchoolDailyStatus.objects.filter(
        school_id__in=[old_school_id, new_school_id],
        date=record_date,
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
        statistics_models.SchoolDailyStatus.objects.update_or_create(
            school_id=new_school_id,
            date=record_date,
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


def aggregate_school_daily_status_to_school_weekly_status(old_school_id, new_school_id, date):
    monday_date = date - timedelta(days=date.weekday())
    sunday_date = monday_date + timedelta(days=6)

    monday_week_no = date_utilities.get_week_from_date(monday_date)
    monday_year = date_utilities.get_year_from_date(monday_date)

    created = False

    school_weekly = statistics_models.SchoolWeeklyStatus.objects.filter(
        school_id=new_school_id,
        week=monday_week_no,
        year=monday_year,
    ).last()

    if not school_weekly:
        school_weekly = statistics_models.SchoolWeeklyStatus.objects.create(
            school_id=new_school_id,
            year=monday_year,
            week=monday_week_no,
        )
        created = True

    aggregate_qs = statistics_models.SchoolDailyStatus.objects.all().filter(
        school_id=new_school_id, date__range=[monday_date, sunday_date],
    )

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
        selected_school_weekly = statistics_models.SchoolWeeklyStatus.objects.all().filter(
            school_id=old_school_id,
            year=monday_year,
            week=monday_week_no,
        ).first()

        if not selected_school_weekly:
            selected_school_weekly = statistics_models.SchoolWeeklyStatus.objects.all().filter(
                school_id=new_school_id, date__lt=school_weekly.date,
            ).last()

        if not selected_school_weekly:
            selected_school_weekly = statistics_models.SchoolWeeklyStatus.objects.all().filter(
                school_id=old_school_id, date__lt=school_weekly.date,
            ).last()

        if selected_school_weekly:
            school_weekly.num_students = selected_school_weekly.num_students
            school_weekly.num_teachers = selected_school_weekly.num_teachers
            school_weekly.num_classroom = selected_school_weekly.num_classroom
            school_weekly.num_latrines = selected_school_weekly.num_latrines
            school_weekly.running_water = selected_school_weekly.running_water
            school_weekly.electricity_availability = selected_school_weekly.electricity_availability
            school_weekly.computer_lab = selected_school_weekly.computer_lab
            school_weekly.num_computers = selected_school_weekly.num_computers

            school_weekly.connectivity_type = selected_school_weekly.connectivity_type
            school_weekly.coverage_availability = selected_school_weekly.coverage_availability
            school_weekly.coverage_type = selected_school_weekly.coverage_type

            school_weekly.download_speed_contracted = selected_school_weekly.download_speed_contracted
            school_weekly.num_computers_desired = selected_school_weekly.num_computers_desired
            school_weekly.electricity_type = selected_school_weekly.electricity_type
            school_weekly.num_adm_personnel = selected_school_weekly.num_adm_personnel

            school_weekly.fiber_node_distance = selected_school_weekly.fiber_node_distance
            school_weekly.microwave_node_distance = selected_school_weekly.microwave_node_distance

            school_weekly.schools_within_1km = selected_school_weekly.schools_within_1km
            school_weekly.schools_within_2km = selected_school_weekly.schools_within_2km
            school_weekly.schools_within_3km = selected_school_weekly.schools_within_3km

            school_weekly.nearest_lte_distance = selected_school_weekly.nearest_lte_distance
            school_weekly.nearest_umts_distance = selected_school_weekly.nearest_umts_distance
            school_weekly.nearest_gsm_distance = selected_school_weekly.nearest_gsm_distance
            school_weekly.nearest_nr_distance = selected_school_weekly.nearest_nr_distance

            school_weekly.pop_within_1km = selected_school_weekly.pop_within_1km
            school_weekly.pop_within_2km = selected_school_weekly.pop_within_2km
            school_weekly.pop_within_3km = selected_school_weekly.pop_within_3km

            school_weekly.school_data_source = selected_school_weekly.school_data_source
            school_weekly.school_data_collection_year = selected_school_weekly.school_data_collection_year
            school_weekly.school_data_collection_modality = selected_school_weekly.school_data_collection_modality
            school_weekly.school_location_ingestion_timestamp = selected_school_weekly.school_location_ingestion_timestamp
            school_weekly.connectivity_govt_ingestion_timestamp = selected_school_weekly.connectivity_govt_ingestion_timestamp
            school_weekly.connectivity_govt_collection_year = selected_school_weekly.connectivity_govt_collection_year
            school_weekly.disputed_region = selected_school_weekly.disputed_region

            school_weekly.download_speed_benchmark = selected_school_weekly.download_speed_benchmark

            school_weekly.num_students_girls = selected_school_weekly.num_students_girls
            school_weekly.num_students_boys = selected_school_weekly.num_students_boys
            school_weekly.num_students_other = selected_school_weekly.num_students_other
            school_weekly.num_teachers_female = selected_school_weekly.num_teachers_female
            school_weekly.num_teachers_male = selected_school_weekly.num_teachers_male
            school_weekly.num_tablets = selected_school_weekly.num_tablets
            school_weekly.num_robotic_equipment = selected_school_weekly.num_robotic_equipment

            school_weekly.computer_availability = selected_school_weekly.computer_availability
            school_weekly.teachers_trained = selected_school_weekly.teachers_trained
            school_weekly.sustainable_business_model = selected_school_weekly.sustainable_business_model
            school_weekly.device_availability = selected_school_weekly.device_availability

            school_weekly.building_id_govt = selected_school_weekly.building_id_govt
            school_weekly.num_schools_per_building = selected_school_weekly.num_schools_per_building

    school_weekly.save()


class Command(BaseCommand):
    help = 'Merge the duplicate schools'

    def add_arguments(self, parser):

        parser.add_argument(
            '--redo_aggregations', action='store_true', dest='redo_aggregations', default=False,
            help='If provided, aggregations will be updated for new school id.'
        )

        parser.add_argument(
            '--delete_old', action='store_true', dest='delete_old_school', default=False,
            help='If provided, School for old_id will be deleted from Database.'
        )

        parser.add_argument(
            '-new_id', dest='new_school_id', required=True, type=int,
            help='id of the new school. It is database generated id, not external id.'
        )

        parser.add_argument(
            '-old_id', dest='old_school_id', required=True, type=int,
            help='id of the old school. It is database generated id, not external id.'
        )

        parser.add_argument(
            '-year', dest='year', required=False, type=int,
            help='Pass the Year to check data for a specific year.'
        )

        # TODO: Add week no logic also
        # parser.add_argument(
        #     '-week', dest='week_no', required=False, type=int,
        #     help='Pass the Country ID in case want to control the update.'
        # )

    def handle(self, **options):
        delete_old_school = options.get('delete_old_school')
        redo_aggregations = options.get('redo_aggregations')
        new_school_id = options.get('new_school_id')
        old_school_id = options.get('old_school_id')
        year = options.get('year')

        if redo_aggregations:
            rt_dates_qs = statistics_models.SchoolDailyStatus.objects.filter(school_id=old_school_id)

            if year:
                rt_dates_qs = rt_dates_qs.filter(date__year=year)

            rt_dates = list(rt_dates_qs.values_list('date', flat=True).order_by('date'))
            print('Dates picked for aggregations: ')
            print(rt_dates)

            # Do RT aggregations for new schools
            week_no_with_week_date_mapping = {}
            for aggr_date in rt_dates:
                aggregate_school_daily_status_from_old_school_to_new_school(old_school_id, new_school_id, aggr_date)
                print('Updated SchoolDailyStatus aggregations for new school')

                aggr_date_week_no = date_utilities.get_week_from_date(aggr_date)
                week_no_with_week_date_mapping[aggr_date_week_no] = aggr_date

            for week_no, any_week_date in week_no_with_week_date_mapping.items():
                year = date_utilities.get_year_from_date(any_week_date)
                print('Weekly record details. \tWeek No: {0}\tYear: {1}'.format(week_no, year))

                aggregate_school_daily_status_to_school_weekly_status(old_school_id, new_school_id, any_week_date)
                print('Updated SchoolWeeklyStatus aggregations for new school.')

            # Reset RT table for new school
            cmd_args = ['--reset', f'-school_id={new_school_id}']
            call_command('populate_school_registration_data', *cmd_args)
            print('Updated SchoolRealtimeRegistration table for new school')

        # If delete old=True
        # 1. Delete from schools_school
        # 2. TODO: Delete index if possible
        if delete_old_school:
            print('Starting deleting old school records.')
            school = School.objects.get(pk=old_school_id)

            school.deleted=timezone.now()
            school.save(update_fields=('deleted',))

            school.daily_status.all().update(deleted=timezone.now())
            school.weekly_status.all().update(deleted=timezone.now())
            school.realtime_status.all().update(deleted=timezone.now())
            school.realtime_registration_status.all().update(deleted=timezone.now())
            school.proco_schools.all().delete()

            print('Deleted old records.')
