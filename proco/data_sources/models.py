from datetime import datetime, timedelta

from django.conf import settings
from django.core.cache import cache
from django.db import models
from django.utils import timezone
from model_utils.models import TimeStampedModel
from simple_history.models import HistoricalRecords

from proco.core import models as core_models
from proco.locations.models import Country
from proco.schools import models as school_models
from proco.entities import models as entity_models


class SchoolMasterData(TimeStampedModel, core_models.MasterDataSourceModelMixin):
    """
    SchoolMasterData
        This class define model used to store School Master Data.
    Inherits : `BaseModel`
    """

    DATA_VERSION_CACHE_KEY = 'school_master_data_last_version_{0}'

    school = models.ForeignKey(
        school_models.School,
        blank=True,
        null=True,
        related_name='proco_schools',
        on_delete=models.DO_NOTHING,
        verbose_name='Proco School'
    )

    country = models.ForeignKey(
        Country,
        blank=True,
        null=True,
        related_name='school_master_rows',
        on_delete=models.DO_NOTHING,
    )

    # School Fields
    school_id_giga = models.CharField(max_length=50, null=False, blank=False, db_index=True)  # School.giga_id_school
    school_id_govt = models.CharField(blank=True, null=True, max_length=255, db_index=True)  # School.external_id
    school_name = models.CharField(max_length=1000, default='Name unknown')  # School.name

    education_level = models.CharField(blank=True, null=True, max_length=255)  # School.education_level
    education_level_govt = models.CharField(blank=True, null=True, max_length=255)  # School.education_level_govt
    school_area_type = models.CharField(blank=True, null=True, max_length=255)  # School.environment
    school_funding_type = models.CharField(blank=True, null=True, max_length=255)  # School.school_type
    school_establishment_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)

    # SchoolWeeklyStatus Fields
    num_computers_desired = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_computers_desired
    num_students = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_students
    num_teachers = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_teachers
    num_classrooms = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_classroom
    num_latrines = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_latrines
    computer_lab = models.CharField(blank=True, null=True, max_length=255)  # computer_lab
    cellular_coverage_availability = models.CharField(blank=True, null=True, max_length=255)  # coverage_availability
    cellular_coverage_type = models.CharField(blank=True, null=True, max_length=255)  # coverage_type
    fiber_node_distance = models.FloatField(blank=True, default=None, null=True)  # fiber_node_distance
    microwave_node_distance = models.FloatField(blank=True, default=None, null=True)  # microwave_node_distance

    schools_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)  # schools_within_1km
    schools_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)  # schools_within_2km
    schools_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)  # schools_within_3km

    nearest_LTE_distance = models.FloatField(blank=True, default=None, null=True)  # nearest_lte_distance
    nearest_UMTS_distance = models.FloatField(blank=True, default=None, null=True)  # nearest_umts_distance
    nearest_GSM_distance = models.FloatField(blank=True, default=None, null=True)  # nearest_gsm_distance
    nearest_NR_distance = models.FloatField(blank=True, default=None, null=True)  # nearest_nr_distance

    school_data_source = models.CharField(blank=True, null=True, max_length=255)  # school_data_source
    # school_data_collection_year
    school_data_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    # school_data_collection_modality
    school_data_collection_modality = models.CharField(blank=True, null=True, max_length=255)
    # school_location_ingestion_timestamp
    school_location_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)
    # connectivity_govt_ingestion_timestamp
    connectivity_govt_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)
    # connectivity_govt_collection_year
    disputed_region = models.CharField(blank=True, null=True, max_length=255)  # disputed_region

    # SchoolRealTimeRegistration
    connectivity_RT = models.CharField(blank=True, null=True, max_length=255)  # rt_registered
    connectivity_RT_datasource = models.CharField(blank=True, null=True, max_length=255)  # rt_source
    connectivity_RT_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)  # rt_registration_date

    download_speed_benchmark = models.FloatField(blank=True, default=None, null=True)  # download_speed_benchmark

    # New fields on 23rd Sept

    num_students_girls = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_students_girls
    num_students_boys = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_students_boys
    num_students_other = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_students_other
    num_teachers_female = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_teachers_female
    num_teachers_male = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_teachers_male

    teachers_trained = models.CharField(blank=True, null=True, max_length=255)  # teachers_trained

    num_robotic_equipment = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_robotic_equipment

    building_id_govt = models.CharField(blank=True, null=True, max_length=255)  # building_id_govt
    num_schools_per_building = models.PositiveIntegerField(blank=True, default=None,
                                                           null=True)  # num_schools_per_building


class HealthEntityMasterData(TimeStampedModel, core_models.MasterDataSourceModelMixin):
    """
    HealthEntityMasterData
        This class define model used to store Health Entity Master Data.
    Inherits : `DataSourceModelMixin`
    """

    DATA_VERSION_CACHE_KEY = 'health_entity_master_data_last_version_{0}'

    entity = models.ForeignKey(
        entity_models.Entity,
        blank=True,
        null=True,
        related_name='health_master_data',
        on_delete=models.DO_NOTHING,
        verbose_name='Health Entity'
    )

    country = models.ForeignKey(
        Country,
        blank=True,
        null=True,
        related_name='health_entity_master_rows',
        on_delete=models.DO_NOTHING,
    )

    # Health Entity Specific Fields
    health_id_giga = models.CharField(max_length=50, null=False, blank=False, db_index=True)  # School.giga_id_school
    dhis2_id = models.CharField(blank=True, null=True, max_length=255)  # School.external_id
    hims_id = models.CharField(blank=True, null=True, max_length=255)  # School.external_id
    hfml_id = models.CharField(blank=True, null=True, max_length=255)  # School.external_id
    facility_name = models.CharField(max_length=1000, default='Name unknown')  # School.name
    facility_type = models.CharField(blank=True, null=True, max_length=255)  # School.external_id
    facility_ownership = models.CharField(blank=True, null=True, max_length=255)  # School.external_id

    admin_3 = models.CharField(blank=True, null=True, max_length=255)
    admin_4 = models.CharField(blank=True, null=True, max_length=255)

    connectivity_type = models.CharField(blank=True, null=True, max_length=255)

    num_community_health_workers = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_community_health_workers_within_5km = models.PositiveIntegerField(blank=True, default=None, null=True)
    area_type = models.CharField(blank=True, null=True, max_length=255)

    pop_within_5km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_10km = models.PositiveIntegerField(blank=True, default=None, null=True)

    govt_pop_est = models.PositiveIntegerField(blank=True, default=None, null=True)
    hf_pop_est = models.PositiveIntegerField(blank=True, default=None, null=True)
    health_service_provider = models.CharField(blank=True, null=True, max_length=255)  # Public|Private|Charitable|Other
    facility_accessibility = models.CharField(blank=True, null=True, max_length=10)  # Yes/No/Null/Unknown
    distance_to_closest_settlement = models.PositiveIntegerField(blank=True, null=True)
    distance_to_country_boundary = models.PositiveIntegerField(blank=True, default=None, null=True)
    facility_level = models.CharField(max_length=20)  # Community|Primary|Secondary|Tertiary|Quaternary
    num_healthworkers = models.PositiveIntegerField(blank=True, default=None, null=True)  # min=0, max=6000
    num_beds_tot = models.PositiveIntegerField(blank=True, default=None, null=True)  # min = 0; max = 10000
    num_beds_icu = models.PositiveIntegerField(blank=True, default=None, null=True)  # min = 0; max = 5000
    num_theatres = models.PositiveIntegerField(blank=True, default=None, null=True)  # min = 0; max = 200
    num_toilets = models.PositiveIntegerField(blank=True, default=None, null=True)  # min = 0; max = 500
    power_backup_system = models.CharField(blank=True, null=True, max_length=10)  # yes|no
    num_outpatients = models.PositiveIntegerField(blank=True, default=None, null=True)  # <20000
    num_inpatients = models.PositiveIntegerField(blank=True, default=None, null=True)  # <20000
    licensing_status = models.CharField(max_length=50)  # licensed|provisional|expired|suspended|not_applicable
    facility_hours = models.CharField(
        max_length=50)  # 24_7 | weekdays_daytime | weekdays_extended | seasonal | unknown | other
    emergency_services_available = models.CharField(blank=True, null=True, max_length=10)  # yes|no
    staff_doctors = models.PositiveIntegerField(blank=True, default=None, null=True)  # max=2000
    staff_nurses = models.PositiveIntegerField(blank=True, default=None, null=True)  # max=4000
    staff_midwives = models.PositiveIntegerField(blank=True, default=None, null=True)  # max=500
    staff_laboratorians = models.PositiveIntegerField(blank=True, default=None, null=True)  # max=500
    staff_pharmacists = models.PositiveIntegerField(blank=True, default=None, null=True)  # max=500
    cold_chain_available = models.CharField(blank=True, null=True, max_length=10)  # yes|no
    waste_management_system = models.CharField(blank=True, null=True,
                                               max_length=50)  # incinerator|pit|contracted_service|none|other
    hmis_system = models.CharField(max_length=10)  # yes|no
    catchment_population = models.PositiveIntegerField(blank=True, default=None, null=True)
    services_offered = models.CharField(blank=True, null=True,
                                        max_length=100)  # outpatient|inpatient|maternity|surgery|laboratory|pharmacy|radiology|dialysis|mental_health|immunization|HIV|TB|NCD_clinic|pediatrics|geriatrics|physiotherapy|dental
    facility_id_govt = models.CharField(max_length=50)
    facility_id_govt_type = models.CharField(blank=True, null=True, max_length=50)
    facility_establishment_year = models.PositiveSmallIntegerField(blank=True,
                                                                   null=True)  # min = 1000; max = current year
    facility_data_source = models.CharField(blank=True, null=True, max_length=255)
    facility_data_collection_year = models.PositiveSmallIntegerField(blank=True,
                                                                     null=True)  # min = 1000; max = current year
    facility_data_collection_modality = models.CharField(blank=True, null=True,
                                                         max_length=255)  # online|in-person|phone|other
    is_facility_open = models.CharField(blank=True, null=True, max_length=10)  # yes|no

    download_speed_govt = models.FloatField(blank=True, null=True)  # min = 0; max = 200
    facility_address = models.CharField(blank=True, null=True, max_length=10)  # min = 1000; max = current year
    refugee_camp = models.CharField(blank=True, null=True, max_length=10)  # yes|no
    patients_refugees = models.CharField(blank=True, null=True, max_length=10)  # yes|no
    connectivity_start_gov = models.CharField(blank=True, null=True,
                                              max_length=10)  # MM: min =1; max=12|YYYY: min = 1000; max = current year
    connectivity_start_contract_gov = models.CharField(blank=True, null=True,
                                                       max_length=10)  # MM: min =1; max=12|YYYY: min = 1000; max = current year
    connectivity_ever_connected = models.CharField(blank=True, null=True, max_length=10)  # yes|no


class DailyCheckAppMeasurementData(core_models.DataSourceModelMixin):
    CACHE_KEY = 'last_dailycheckapp_measurement_at'

    created_at = models.DateTimeField(db_index=True)
    timestamp = models.DateTimeField()
    browserId = models.TextField(blank=True, null=True)
    school_id = models.TextField(db_index=True)
    giga_id_school = models.TextField(blank=True, null=True)
    download = models.FloatField(blank=True, null=True)
    upload = models.FloatField(blank=True, null=True)
    latency = models.BigIntegerField(blank=True, null=True)
    country_code = models.TextField(blank=True, null=True, db_index=True)
    ip_address = models.TextField(blank=True, null=True)
    app_version = models.TextField(blank=True, null=True)
    source = models.TextField()

    class Meta:
        ordering = ('timestamp',)

    @classmethod
    def get_last_dailycheckapp_measurement_date(cls) -> datetime:
        last_measurement_at = cache.get(cls.CACHE_KEY)
        if not last_measurement_at:
            latest_created_at = cls.objects.all().order_by('-created_at').first()
            if latest_created_at:
                last_measurement_at = latest_created_at.created_at
            else:
                last_measurement_at = timezone.now() - timedelta(days=1)
        return last_measurement_at

    @classmethod
    def set_last_dailycheckapp_measurement_date(cls, value: datetime):
        cache.set(cls.CACHE_KEY, value)


class QoSData(core_models.DataSourceModelMixin):
    DATA_VERSION_CACHE_KEY = 'qos_data_last_version_{0}'

    school = models.ForeignKey(
        school_models.School,
        related_name='qos_school_rows',
        on_delete=models.DO_NOTHING,
        verbose_name='QoS School'
    )

    country = models.ForeignKey(
        Country,
        related_name='qos_country_rows',
        on_delete=models.DO_NOTHING,
    )

    timestamp = models.DateTimeField(db_index=True)
    date = models.DateField(db_index=True)

    # School Fields
    school_id_giga = models.CharField(max_length=50, null=False, blank=False, db_index=True)  # School.giga_id_school
    school_id_govt = models.CharField(blank=True, null=True, max_length=255, db_index=True)  # School.external_id

    speed_download = models.FloatField(blank=True, null=True)
    speed_upload = models.FloatField(blank=True, null=True)
    roundtrip_time = models.FloatField(blank=True, null=True)

    jitter_download = models.FloatField(blank=True, null=True)
    jitter_upload = models.FloatField(blank=True, null=True)
    rtt_packet_loss_pct = models.FloatField(blank=True, null=True)

    latency = models.FloatField(blank=True, null=True)

    speed_download_probe = models.FloatField(blank=True, null=True)
    speed_upload_probe = models.FloatField(blank=True, null=True)
    latency_probe = models.FloatField(blank=True, null=True)

    speed_download_mean = models.FloatField(blank=True, null=True)
    speed_upload_mean = models.FloatField(blank=True, null=True)

    provider = models.TextField(blank=True, null=True)
    ip_family = models.IntegerField(blank=True, null=True)

    report_id = models.TextField(blank=True, null=True)
    agent_id = models.TextField(blank=True, null=True)

    speed_download_max = models.FloatField(blank=True, null=True)
    speed_upload_max = models.FloatField(blank=True, null=True)
    pe_ingress = models.FloatField(blank=True, null=True)
    pe_egress = models.FloatField(blank=True, null=True)
    inbound_traffic_sum = models.FloatField(blank=True, null=True)
    outbound_traffic_sum = models.FloatField(blank=True, null=True)
    latency_min = models.FloatField(blank=True, null=True)
    latency_mean = models.FloatField(blank=True, null=True)
    latency_max = models.FloatField(blank=True, null=True)
    signal_mean = models.FloatField(blank=True, null=True)
    signal_max = models.FloatField(blank=True, null=True)
    is_connected_all = models.PositiveIntegerField(blank=True, default=None, null=True)
    is_connected_true = models.PositiveIntegerField(blank=True, default=None, null=True)

    version = models.PositiveIntegerField(blank=True, default=None, null=True)

    class Meta:
        ordering = ('timestamp',)
        unique_together = ('school', 'timestamp')

    @classmethod
    def get_last_version(cls, iso3_format):
        last_data_version = cache.get(cls.DATA_VERSION_CACHE_KEY.format(iso3_format))
        if not last_data_version:
            latest_records = cls.objects.filter(
                country__iso3_format=iso3_format,
                version__isnull=False,
            ).order_by('-version').first()
            if latest_records:
                last_data_version = latest_records.version
        return last_data_version

    @classmethod
    def set_last_version(cls, value, iso3_format):
        cache.set(cls.DATA_VERSION_CACHE_KEY.format(iso3_format), value)
