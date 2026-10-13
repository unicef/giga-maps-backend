import logging
import os
import uuid
from datetime import timedelta

from django.db import transaction
from django.db.models import Q

from proco.core import utils as core_utilities
from proco.core.config import app_config as core_configs
from proco.schools.constants import statuses_schema

logger = logging.getLogger('gigamaps.' + __name__)


def get_imported_file_path(instance, filename):
    filename_stripped = os.path.splitext(filename)[0].split('/')[-1]
    extension = os.path.splitext(filename)[1]
    random_prefix = uuid.uuid4()
    return f'imported/{random_prefix}/{filename_stripped}{extension}'


def get_coverage_type(school_instance):
    """
    get_coverage_type
        Get the coverage_type on School object from SchoolWeeklyStatus model. If value not configured
        than save unknown as default value.
    """
    return school_instance.last_weekly_status.coverage_type if school_instance.last_weekly_status else 'unknown'


def get_connectivity_status(school_instance):
    from proco.connection_statistics.models import CountryWeeklyStatus

    availability = school_instance.country.last_weekly_status.connectivity_availability
    if not availability or not school_instance.last_weekly_status:
        return 'unknown'

    if availability in [CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.static_speed,
                        CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.realtime_speed]:
        return statuses_schema.get_connectivity_status_by_connectivity_speed(
            school_instance.last_weekly_status.connectivity_speed)

    elif availability == CountryWeeklyStatus.CONNECTIVITY_TYPES_AVAILABILITY.connectivity:
        return statuses_schema.get_status_by_availability(school_instance.last_weekly_status.connectivity)
    return 'unknown'


def get_connectivity_status_by_master_api(school_instance):
    from proco.data_sources.models import SchoolMasterData
    from proco.connection_statistics.models import SchoolRealTimeRegistration

    if SchoolRealTimeRegistration.objects.all().filter(
        school=school_instance,
        rt_registered=True,  # From School Daily or School Master connectivity_RT
        rt_registration_date__date__lte=core_utilities.get_current_datetime_object().date(),
    ).exists():
        return 'good'

    school_row = SchoolMasterData.objects.filter(
        status=SchoolMasterData.ROW_STATUS_PUBLISHED,
        country=school_instance.country,
        school_id_giga=school_instance.giga_id_school,
    ).order_by('-id').first()

    if school_row:
        status = 'unknown'
        if (core_utilities.is_blank_string(school_row.connectivity_govt) or
            str(school_row.connectivity_govt).lower() == 'unknown'):
            connectivity_govt = None
        elif str(school_row.connectivity_govt).lower() in core_configs.true_choices:
            connectivity_govt = 'yes'
        else:
            connectivity_govt = 'no'
        connectivity_rt = None
        if not core_utilities.is_blank_string(school_row.connectivity_RT):
            connectivity_rt = 'yes' if str(school_row.connectivity_RT).lower() in core_configs.true_choices else 'no'

        connectivity = None
        if not core_utilities.is_blank_string(school_row.connectivity):
            connectivity = 'yes' if str(school_row.connectivity).lower() in core_configs.true_choices else 'no'

        if connectivity_govt == 'yes' or connectivity_rt == 'yes' or connectivity == 'yes':
            status = 'good'
        elif connectivity_govt == 'no':
            status = 'no'
        return status
    return get_connectivity_status(school_instance)


def get_coverage_status(school_instance):
    availability = school_instance.country.last_weekly_status.coverage_availability
    if not availability or not school_instance.last_weekly_status:
        return 'unknown'

    from proco.connection_statistics.models import CountryWeeklyStatus

    if availability == CountryWeeklyStatus.COVERAGE_TYPES_AVAILABILITY.coverage_type:
        return statuses_schema.get_coverage_status_by_coverage_type(
            str(school_instance.last_weekly_status.coverage_type).lower())

    elif availability == CountryWeeklyStatus.COVERAGE_TYPES_AVAILABILITY.coverage_availability:
        return statuses_schema.get_status_by_availability(school_instance.last_weekly_status.coverage_availability)
    return 'unknown'


# Execution time 01:00 AM everyday
def update_school_from_country_or_school_weekly_update(start_time=None, end_time=None):
    """
    Update the school fields every 24 hours if its SchoolWeekly or CountryWeekly records updated

    Logic:
    1. Pick all schools of a country if country data updated in last 24 hours
    2. Pick all schools of a country if country's weekly status data updated in last 24 hours
    3. Pick all those schools where school weekly status updated in last 24 hours
    """

    from proco.connection_statistics.models import Country
    from proco.schools.models import School

    if start_time is None or end_time is None:
        current_time = core_utilities.get_current_datetime_object()

        start_time = (current_time - timedelta(hours=25)).replace(minute=0, second=0, microsecond=0)
        end_time = (current_time - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    # Countries modified in last 24 hours
    country_ids_updated_in_last_24_hours_qs = Country.objects.filter(
        modified__gte=start_time,
        modified__lt=end_time,
    ).values_list('id', flat=True).order_by('id').distinct('id')
    country_ids_updated_in_last_24_hours = list(country_ids_updated_in_last_24_hours_qs)

    logger.debug('Query to select countries updated between ({0} - {1}): {2}'.format(
        start_time, end_time, country_ids_updated_in_last_24_hours_qs.query))

    # CountryWeeklyStatus modified in last 24 hours
    country_status_updated_in_last_24_hours_qs = Country.objects.filter(
        last_weekly_status__modified__gte=start_time,
        last_weekly_status__modified__lt=end_time,
    ).exclude(id__in=country_ids_updated_in_last_24_hours).values_list(
        'id', flat=True).order_by('id').distinct('id')
    country_status_updated_in_last_24_hours = list(country_status_updated_in_last_24_hours_qs)

    logger.debug('Query to select countries where CountryWeeklyStatus updated between ({0} - {1}): {2}'.format(
        start_time, end_time, country_status_updated_in_last_24_hours_qs.query))

    updated_country_ids = country_ids_updated_in_last_24_hours + country_status_updated_in_last_24_hours

    # SchoolWeeklyStatus updated in last 24 hours
    school_updated_in_last_24_hours = School.objects.filter(
        Q(last_weekly_status__modified__gte=start_time, last_weekly_status__modified__lt=end_time) |
        Q(country_id__in=updated_country_ids)
    )

    logger.debug('Query to select schools where SchoolWeeklyStatus updated between ({0} - {1}): {2}'.format(
        start_time, end_time, school_updated_in_last_24_hours.query))
    school_count = school_updated_in_last_24_hours.count()
    if school_count == 0:
        return

    from proco.data_sources.models import SchoolMasterData
    from proco.connection_statistics.models import SchoolRealTimeRegistration

    school_ids_qs = school_updated_in_last_24_hours.values_list('pk', flat=True).order_by('pk')
    last_school_id = school_ids_qs.order_by('-pk').first()
    last_seen_school_id = 0
    while last_school_id and last_seen_school_id < last_school_id:
        school_ids = list(school_ids_qs.filter(pk__gt=last_seen_school_id)[:100])
        if not school_ids:
            break
        last_seen_school_id = school_ids[-1]
        data_chunk = list(School.objects.filter(pk__in=school_ids).select_related(
            'country__last_weekly_status',
            'last_weekly_status',
        ).only(
            'id',
            'country_id',
            'giga_id_school',
            'coverage_type',
            'coverage_status',
            'connectivity_status',
            'last_weekly_status_id',
            'last_weekly_status__coverage_type',
            'last_weekly_status__coverage_availability',
            'last_weekly_status__connectivity_speed',
            'last_weekly_status__connectivity',
            'country__id',
            'country__last_weekly_status_id',
            'country__last_weekly_status__connectivity_availability',
            'country__last_weekly_status__coverage_availability',
        ).order_by('pk'))
        school_ids = [s.id for s in data_chunk]

        rt_regs = set(SchoolRealTimeRegistration.objects.filter(
            school_id__in=school_ids,
            rt_registered=True,
            rt_registration_date__date__lte=core_utilities.get_current_datetime_object().date(),
        ).values_list('school_id', flat=True))

        schools_missing_rt_status = [s for s in data_chunk if s.id not in rt_regs]
        schools_with_giga_id_missing_rt_status = [
            s for s in schools_missing_rt_status if not core_utilities.is_blank_string(s.giga_id_school)
        ]
        country_ids_for_master_lookup = {s.country_id for s in schools_with_giga_id_missing_rt_status}
        giga_ids_for_master_lookup = {s.giga_id_school for s in schools_with_giga_id_missing_rt_status}

        master_map = {}
        if giga_ids_for_master_lookup:
            master_data = SchoolMasterData.objects.filter(
                status=SchoolMasterData.ROW_STATUS_PUBLISHED,
                country_id__in=country_ids_for_master_lookup,
                school_id_giga__in=giga_ids_for_master_lookup,
            ).only(
                'country_id',
                'school_id_giga',
                'connectivity_govt',
                'connectivity_RT',
                'connectivity',
            ).order_by('country_id', 'school_id_giga', '-id').distinct('country_id', 'school_id_giga')
            master_map = {(m.country_id, m.school_id_giga): m for m in master_data}

        with transaction.atomic():
            modified_at = core_utilities.get_current_datetime_object()
            for school in data_chunk:
                school.coverage_type = get_coverage_type(school)
                school.coverage_status = get_coverage_status(school)
                school.modified = modified_at

                if school.id in rt_regs:
                    school.connectivity_status = 'good'
                else:
                    school_row = master_map.get((school.country_id, school.giga_id_school))
                    if school_row:
                        status = 'unknown'
                        if (core_utilities.is_blank_string(school_row.connectivity_govt) or
                            str(school_row.connectivity_govt).lower() == 'unknown'):
                            connectivity_govt = None
                        elif str(school_row.connectivity_govt).lower() in core_configs.true_choices:
                            connectivity_govt = 'yes'
                        else:
                            connectivity_govt = 'no'

                        connectivity_rt = None
                        if not core_utilities.is_blank_string(school_row.connectivity_RT):
                            connectivity_rt = 'yes' if str(school_row.connectivity_RT).lower() in core_configs.true_choices else 'no'

                        connectivity = None
                        if not core_utilities.is_blank_string(school_row.connectivity):
                            connectivity = 'yes' if str(school_row.connectivity).lower() in core_configs.true_choices else 'no'

                        if connectivity_govt == 'yes' or connectivity_rt == 'yes' or connectivity == 'yes':
                            status = 'good'
                        elif connectivity_govt == 'no':
                            status = 'no'
                        school.connectivity_status = status
                    else:
                        school.connectivity_status = get_connectivity_status(school)

            School.objects.bulk_update(data_chunk, [
                'coverage_type',
                'coverage_status',
                'connectivity_status',
                'modified',
            ])
