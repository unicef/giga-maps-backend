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


class SchoolMasterData(TimeStampedModel, core_models.BaseSchoolMaster):
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

    school_establishment_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True) # SchoolMasterStatus.establishment_year
    water_availability = models.CharField(blank=True, null=True, max_length=255)  # SchoolMasterStatus.running_water
    electricity_availability = models.CharField(blank=True, null=True, max_length=255)  # SchoolMasterStatus.electricity_availability
    computer_lab = models.CharField(blank=True, null=True, max_length=255)  # SchoolMasterStatus.computer_lab

    connectivity_govt = models.CharField(blank=True, null=True, max_length=255)  # SchoolWeeklyStatus.connectivity
    connectivity_type_govt = models.CharField(blank=True, null=True, max_length=255)  # SchoolWeeklyStatus.connectivity_type
    cellular_coverage_availability = models.CharField(blank=True, null=True, max_length=255)  # SchoolWeeklyStatus.coverage_availability
    cellular_coverage_type = models.CharField(blank=True, null=True, max_length=255)  # SchoolWeeklyStatus.coverage_type

    nearest_LTE_distance = models.FloatField(blank=True, default=None, null=True)  # SchoolMasterStatus.nearest_lte_distance
    nearest_UMTS_distance = models.FloatField(blank=True, default=None, null=True)  # SchoolMasterStatus.nearest_umts_distance
    nearest_GSM_distance = models.FloatField(blank=True, default=None, null=True)  # SchoolMasterStatus.nearest_gsm_distance
    nearest_NR_distance = models.FloatField(blank=True, default=None, null=True)  # SchoolMasterStatus.nearest_nr_distance

    school_data_source = models.CharField(blank=True, null=True, max_length=255)  # SchoolMasterStatus.data_source
    # SchoolMasterStatus.data_collection_year
    school_data_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    # SchoolMasterStatus.data_collection_modality
    school_data_collection_modality = models.CharField(blank=True, null=True, max_length=255)
    # SchoolMasterStatus.location_ingestion_timestamp
    school_location_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)

    disputed_region = models.CharField(blank=True, null=True, max_length=255)  # SchoolMasterStatus.disputed_region

    # SchoolRealTimeRegistration
    connectivity_RT = models.CharField(blank=True, null=True, max_length=255)  # rt_registered
    connectivity_RT_datasource = models.CharField(blank=True, null=True, max_length=255)  # rt_source
    connectivity_RT_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)  # rt_registration_date

    # New fields on 23rd Sept
    computer_availability = models.CharField(blank=True, null=True, max_length=255)  # SchoolMasterStatus.computer_availability

    teachers_trained  = models.CharField(blank=True, null=True, max_length=255)  # SchoolMasterStatus.teachers_trained
    sustainable_business_model = models.CharField(blank=True, null=True, max_length=255)  # SchoolMasterStatus.sustainable_business_model
    device_availability = models.CharField(blank=True, null=True, max_length=255)  # SchoolMasterStatus.device_availability

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
            country_object = Country.objects.get(iso3_format=iso3_format)
            last_data_version = country_object.latest_school_master_data_version
        return last_data_version

    @classmethod
    def set_last_version(cls, value, iso3_format):
        cache.set(cls.DATA_VERSION_CACHE_KEY.format(iso3_format), value)
        country_object = Country.objects.get(iso3_format=iso3_format)
        country_object.latest_school_master_data_version = value
        country_object.save(update_fields=['latest_school_master_data_version', ])


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

    speed_download_mean = models.FloatField(blank=True, null=True)
    speed_upload_mean = models.FloatField(blank=True, null=True)

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
