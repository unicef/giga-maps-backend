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


class SchoolMasterData(TimeStampedModel, models.Model):
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
    admin1 = models.CharField(max_length=255, blank=True, null=True)
    admin1_id_giga = models.CharField(max_length=50, null=True, blank=True)  # School.admin1
    admin2 = models.CharField(max_length=255, blank=True, null=True)
    admin2_id_giga = models.CharField(max_length=50, null=True, blank=True)  # School.admin2
    latitude = models.FloatField(blank=True, default=None, null=True)  # School.geopoint
    longitude = models.FloatField(blank=True, default=None, null=True)  # School.geopoint
    education_level = models.CharField(blank=True, null=True, max_length=255)  # School.education_level
    school_area_type = models.CharField(blank=True, null=True, max_length=255)  # School.environment
    school_funding_type = models.CharField(blank=True, null=True, max_length=255)  # School.school_type
    school_establishment_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)

    # SchoolWeeklyStatus Fields
    download_speed_contracted = models.FloatField(blank=True, default=None, null=True)  # download_speed_contracted
    num_computers_desired = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_computers_desired
    electricity_type = models.CharField(blank=True, null=True, max_length=255)  # electricity_type
    num_adm_personnel = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_adm_personnel
    num_students = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_students
    num_teachers = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_teachers
    num_classrooms = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_classroom
    num_latrines = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_latrines
    water_availability = models.CharField(blank=True, null=True, max_length=255)  # running_water
    electricity_availability = models.CharField(blank=True, null=True, max_length=255)  # electricity_availability
    computer_lab = models.CharField(blank=True, null=True, max_length=255)  # computer_lab
    num_computers = models.PositiveIntegerField(blank=True, default=None, null=True)  # num_computers
    connectivity_govt = models.CharField(blank=True, null=True, max_length=255)  # connectivity
    connectivity_type_govt = models.CharField(blank=True, null=True, max_length=255)  # connectivity_type
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

    pop_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)  # pop_within_1km
    pop_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)  # pop_within_2km
    pop_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)  # pop_within_3km

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
    connectivity_govt_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    disputed_region = models.CharField(blank=True, null=True, max_length=255)  # disputed_region

    # SchoolRealTimeRegistration
    connectivity_RT = models.CharField(blank=True, null=True, max_length=255)  # rt_registered
    connectivity_RT_datasource = models.CharField(blank=True, null=True, max_length=255)  # rt_source
    connectivity_RT_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)  # rt_registration_date

    # No Mapping
    connectivity = models.CharField(blank=True, null=True, max_length=255)

    version = models.PositiveIntegerField(blank=True, default=None, null=True)

    # When pulled from Source API
    ROW_STATUS_DRAFT = 'DRAFT'
    # When updated by Editor after pull
    ROW_STATUS_UPDATED_IN_DRAFT = 'UPDATED_IN_DRAFT'
    # Send for the publishing by Editor to Publisher
    ROW_STATUS_DRAFT_LOCKED = 'DRAFT_LOCKED'
    # Updated by the Publisher
    ROW_STATUS_UPDATED_IN_DRAFT_LOCKED = 'UPDATED_IN_DRAFT_LOCKED'
    # Published by Publisher
    ROW_STATUS_PUBLISHED = 'PUBLISHED'
    # Deleted by Publisher
    ROW_STATUS_DELETED = 'DELETED'
    ROW_STATUS_DELETED_PUBLISHED = 'DELETED_PUBLISHED'
    ROW_STATUS_DISCARDED = 'DISCARDED'

    ROW_STATUS_CHOICES = (
        (ROW_STATUS_DRAFT, 'In Draft'),
        (ROW_STATUS_UPDATED_IN_DRAFT, 'UPDATED BY EDITOR'),
        (ROW_STATUS_DRAFT_LOCKED, 'ASSIGNED To PUBLISHER'),
        (ROW_STATUS_UPDATED_IN_DRAFT_LOCKED, 'UPDATED BY PUBLISHER'),
        (ROW_STATUS_PUBLISHED, 'Published'),
        (ROW_STATUS_DELETED, 'Deleted'),
        (ROW_STATUS_DELETED_PUBLISHED, 'Published Deleted'),
        (ROW_STATUS_DISCARDED, 'Discarded'),
    )

    status = models.CharField(max_length=50, choices=ROW_STATUS_CHOICES, default=ROW_STATUS_DRAFT, db_index=True)

    modified_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        related_name='updated_%(class)ss',
        on_delete=models.DO_NOTHING,
        verbose_name='Last Updated By'
    )

    published_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        related_name='source_published_%(class)ss',
        on_delete=models.DO_NOTHING,
        verbose_name='Published By'
    )
    published_at = core_models.CustomDateTimeField(db_index=True, null=True, blank=True)

    is_read = models.BooleanField(default=False)

    history = HistoricalRecords(inherit=True)

    objects = models.Manager()

    class Meta:
        ordering = ['created']

    @classmethod
    def get_last_version(cls, iso3_format):
        last_data_version = cache.get(cls.DATA_VERSION_CACHE_KEY.format(iso3_format))
        if not last_data_version:
            latest_records = cls.objects.filter(country__iso3_format=iso3_format).order_by('-created').first()
            if latest_records:
                last_data_version = latest_records.version
        return last_data_version

    @classmethod
    def set_last_version(cls, value, iso3_format):
        cache.set(cls.DATA_VERSION_CACHE_KEY.format(iso3_format), value)


class DailyCheckAppMeasurementData(models.Model):
    CACHE_KEY = 'last_dailycheckapp_measurement_at'

    created_at = models.DateTimeField(db_index=True)
    timestamp = models.DateTimeField()
    browserId = models.TextField(blank=True, null=True)
    school_id = models.TextField(db_index=True)
    giga_id_school = models.TextField(blank=True, null=True)
    download = models.FloatField(blank=True, null=True)
    upload = models.FloatField(blank=True, null=True)
    latency = models.IntegerField(blank=True, null=True)
    country_code = models.TextField(blank=True, null=True, db_index=True)
    ip_address = models.TextField(blank=True, null=True)
    app_version = models.TextField(blank=True, null=True)
    source = models.TextField()

    objects = models.Manager()

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


class QoSData(models.Model):
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

    provider = models.TextField(blank=True, null=True)
    ip_family = models.IntegerField(blank=True, null=True)

    report_id = models.TextField(blank=True, null=True)
    agent_id = models.TextField(blank=True, null=True)

    version = models.PositiveIntegerField(blank=True, default=None, null=True)

    objects = models.Manager()

    class Meta:
        ordering = ('timestamp',)
        unique_together = ('school', 'timestamp')

    @classmethod
    def get_last_version(cls, iso3_format):
        last_data_version = cache.get(cls.DATA_VERSION_CACHE_KEY.format(iso3_format))
        if not last_data_version:
            latest_records = cls.objects.filter(country__iso3_format=iso3_format).order_by('-version').first()
            if latest_records:
                last_data_version = latest_records.version
        return last_data_version

    @classmethod
    def set_last_version(cls, value, iso3_format):
        cache.set(cls.DATA_VERSION_CACHE_KEY.format(iso3_format), value)
