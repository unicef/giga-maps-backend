from django.contrib.gis.db.models import PointField
from django.core.cache import cache
from django.db import models
from django.utils.translation import ugettext as _
from model_utils.models import TimeStampedModel
from timezone_field import TimeZoneField

from proco.core import models as core_models
from proco.core.managers import BaseManager
from proco.core.models import CustomDateTimeField
from proco.daily_check_app.config import app_config


class DailyCheckApp_Country(models.Model):
    name = models.CharField(max_length=255)
    code = models.CharField(max_length=32)
    iso3_format = models.CharField(max_length=32, null=True, blank=True)
    latest_school_master_data_version = models.PositiveIntegerField(blank=True, default=None, null=True)

    objects = models.Manager()

    class Meta:
        app_label = app_config.app_name
        managed = False
        db_table = 'country'

    def __str__(self):
        return f'{self.code} - {self.name} - {self.iso3_format}'


class DailyCheckApp_School(TimeStampedModel):

    country = models.ForeignKey(
        DailyCheckApp_Country,
        related_name='schools',
        on_delete=models.CASCADE,
    )

    name = models.CharField(max_length=1000, default='Name unknown')
    country_code = models.CharField(max_length=32)
    timezone = TimeZoneField(blank=True, null=True)
    # geopoint = PointField(verbose_name=_('Point'), null=True, blank=True)
    geopoint = models.CharField(max_length=1000, verbose_name=_('Point'), null=True, blank=True)
    gps_confidence = models.FloatField(null=True, blank=True)
    altitude = models.PositiveIntegerField(blank=True, default=0)
    address = models.CharField(blank=True, max_length=255)
    postal_code = models.CharField(blank=True, max_length=128)
    email = models.EmailField(max_length=128, null=True, blank=True, default=None)
    education_level = models.CharField(blank=True, max_length=255)
    environment = models.CharField(blank=True, max_length=64)
    school_type = models.CharField(blank=True, max_length=64, db_index=True)

    admin_1_name = models.CharField(max_length=100, blank=True)
    admin_2_name = models.CharField(max_length=100, blank=True)
    admin_3_name = models.CharField(max_length=100, blank=True)
    admin_4_name = models.CharField(max_length=100, blank=True)
    external_id = models.CharField(max_length=50, blank=True, db_index=True)
    name_lower = models.CharField(max_length=1000, blank=True, editable=False, db_index=True)
    giga_id_school = models.CharField(max_length=50, blank=True, db_index=True)
    education_level_regional = models.CharField(max_length=6400007, blank=True)

    last_school_static = models.ForeignKey(
        'DailyCheckApp_SchoolStatic',
        related_name='schools',
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )

    deleted = CustomDateTimeField(db_index=True, null=True, blank=True)

    objects = BaseManager()

    class Meta:
        app_label = app_config.app_name
        managed = False
        db_table = 'school'


class DailyCheckApp_SchoolMasterData(TimeStampedModel, models.Model):
    """
    DailyCheckApp_SchoolMasterData
        This class define model used to store School Master Data.
    Inherits : `BaseModel`
    """

    DATA_VERSION_CACHE_KEY = 'daily_check_app_school_master_data_last_version_{0}'

    country = models.ForeignKey(
        DailyCheckApp_Country,
        blank=True,
        null=True,
        related_name='school_master_rows',
        on_delete=models.DO_NOTHING,
    )

    school_id_giga = models.CharField(max_length=50, null=False, blank=False, db_index=True)  # school.giga_id_school
    school_id_govt = models.CharField(blank=True, null=True, max_length=255, db_index=True)  # school.external_id
    school_name = models.CharField(max_length=1000, default='Name unknown')  # school.name
    admin1 = models.CharField(max_length=255, blank=True, null=True)  # school.admin1
    admin1_id_giga = models.CharField(max_length=50, null=True, blank=True)
    admin2 = models.CharField(max_length=255, blank=True, null=True)  # school.admin2
    admin2_id_giga = models.CharField(max_length=50, null=True, blank=True)
    latitude = models.FloatField(blank=True, default=None, null=True)  # school.geopoint
    longitude = models.FloatField(blank=True, default=None, null=True)  # school.geopoint
    education_level = models.CharField(blank=True, null=True, max_length=255)  # school.education_level
    school_area_type = models.CharField(blank=True, null=True, max_length=255)  # school.environment
    school_funding_type = models.CharField(blank=True, null=True, max_length=255)  # school.school_type
    school_establishment_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)

    download_speed_contracted = models.FloatField(blank=True, default=None, null=True)
    num_computers_desired = models.PositiveIntegerField(blank=True, default=None, null=True)
    electricity_type = models.CharField(blank=True, null=True, max_length=255)
    num_adm_personnel = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_students = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_teachers = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_classrooms = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_latrines = models.PositiveIntegerField(blank=True, default=None, null=True)
    water_availability = models.CharField(blank=True, null=True, max_length=255)
    electricity_availability = models.CharField(blank=True, null=True, max_length=255)
    computer_lab = models.CharField(blank=True, null=True, max_length=255)
    num_computers = models.PositiveIntegerField(blank=True, default=None, null=True)
    connectivity_govt = models.CharField(blank=True, null=True, max_length=255)
    connectivity_type_govt = models.CharField(blank=True, null=True, max_length=255)
    cellular_coverage_availability = models.CharField(blank=True, null=True, max_length=255)
    cellular_coverage_type = models.CharField(blank=True, null=True, max_length=255)
    connectivity_type = models.CharField(blank=True, null=True, max_length=255)
    connectivity_type_root = models.CharField(blank=True, null=True, max_length=255)
    fiber_node_distance = models.FloatField(blank=True, default=None, null=True)
    microwave_node_distance = models.FloatField(blank=True, default=None, null=True)

    schools_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)
    schools_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)
    schools_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)

    nearest_LTE_distance = models.FloatField(blank=True, default=None, null=True)  # master_sync_school_static.nearest_lte_distance
    nearest_UMTS_distance = models.FloatField(blank=True, default=None, null=True)  # master_sync_school_static.nearest_umts_distance
    nearest_GSM_distance = models.FloatField(blank=True, default=None, null=True)  # master_sync_school_static.nearest_gsm_distance
    nearest_NR_distance = models.FloatField(blank=True, default=None, null=True)  # master_sync_school_static.nearest_nr_distance

    pop_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.pop_within_1km
    pop_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.pop_within_2km
    pop_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.pop_within_3km

    school_data_source = models.CharField(blank=True, null=True, max_length=255)  # master_sync_school_static.school_data_source
    school_data_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    school_data_collection_modality = models.CharField(blank=True, null=True, max_length=255)
    school_location_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)
    connectivity_govt_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)
    connectivity_govt_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    disputed_region = models.CharField(blank=True, null=True, max_length=255)  # master_sync_school_static.disputed_region

    connectivity_RT = models.CharField(blank=True, null=True, max_length=255)  # master_sync_school_static.connectivity_rt
    connectivity_RT_datasource = models.CharField(blank=True, null=True, max_length=255)  # master_sync_school_static.connectivity_rt_datasource
    connectivity_RT_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)  # master_sync_school_static.connectivity_rt_ingestion_timestamp

    download_speed_benchmark = models.FloatField(blank=True, default=None, null=True)  # master_sync_school_static.download_speed_benchmark
    computer_availability = models.CharField(blank=True, null=True, max_length=255)  # master_sync_school_static.computer_availability

    num_students_girls = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.num_students_girls
    num_students_boys = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.num_students_boys
    num_students_other = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.num_students_other
    num_teachers_female = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.num_teachers_female
    num_teachers_male = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.num_teachers_male

    teachers_trained  = models.CharField(blank=True, null=True, max_length=255)  # master_sync_school_static.teachers_trained
    sustainable_business_model = models.CharField(blank=True, null=True, max_length=255)  # master_sync_school_static.sustainable_business_model
    device_availability = models.CharField(blank=True, null=True, max_length=255)  # master_sync_school_static.device_availability

    num_tablets = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.num_tablets
    num_robotic_equipment = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.num_robotic_equipment

    building_id_govt = models.CharField(blank=True, null=True, max_length=255)  # master_sync_school_static.building_id_govt
    num_schools_per_building = models.PositiveIntegerField(blank=True, default=None, null=True)  # master_sync_school_static.num_schools_per_building
    connectivity = models.CharField(blank=True, null=True, max_length=255)

    version = models.PositiveIntegerField(blank=True, default=None, null=True)

    ROW_STATUS_PUBLISHED = 'PUBLISHED'
    ROW_STATUS_DELETED = 'DELETED'

    ROW_STATUS_CHOICES = (
        (ROW_STATUS_PUBLISHED, 'Insert/Update'),
        (ROW_STATUS_DELETED, 'Deleted'),
    )

    status = models.CharField(max_length=50, choices=ROW_STATUS_CHOICES, default=ROW_STATUS_PUBLISHED, db_index=True)

    objects = models.Manager()

    class Meta:
        app_label = app_config.app_name
        db_table = 'master_sync_intermediate'
        ordering = ['created']

    @classmethod
    def get_last_version(cls, iso3_format):
        last_data_version = cache.get(cls.DATA_VERSION_CACHE_KEY.format(iso3_format))
        if not last_data_version:
            country_object = DailyCheckApp_Country.objects.get(iso3_format=iso3_format)
            last_data_version = country_object.latest_school_master_data_version
        return last_data_version

    @classmethod
    def set_last_version(cls, value, iso3_format):
        cache.set(cls.DATA_VERSION_CACHE_KEY.format(iso3_format), value)
        country_object = DailyCheckApp_Country.objects.get(iso3_format=iso3_format)
        country_object.latest_school_master_data_version = value
        country_object.save(update_fields=['latest_school_master_data_version',])


class DailyCheckApp_SchoolStatic(TimeStampedModel, models.Model):
    """
    DailyCheckApp_SchoolStatic
        This class define model used to store School Master Data.
    Inherits : `BaseModel`
    """

    school = models.ForeignKey(
        DailyCheckApp_School,
        blank=True,
        null=True,
        related_name='dailycheckapp_schools',
        on_delete=models.SET_NULL,
        verbose_name='Daily Check App School'
    )

    admin1_id_giga = models.CharField(max_length=50, null=True, blank=True)
    admin2_id_giga = models.CharField(max_length=50, null=True, blank=True)
    school_establishment_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)

    download_speed_contracted = models.FloatField(blank=True, default=None, null=True)
    num_computers_desired = models.PositiveIntegerField(blank=True, default=None, null=True)
    electricity_type = models.CharField(blank=True, null=True, max_length=255)
    num_adm_personnel = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_students = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_teachers = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_classrooms = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_latrines = models.PositiveIntegerField(blank=True, default=None, null=True)
    water_availability = models.NullBooleanField(default=None)
    electricity_availability = models.NullBooleanField(default=None)
    computer_lab = models.NullBooleanField(default=None)
    num_computers = models.PositiveIntegerField(blank=True, default=None, null=True)
    connectivity_govt = models.NullBooleanField(default=None)
    connectivity_type_govt = models.CharField(blank=True, null=True, max_length=255)
    connectivity_type = models.CharField(blank=True, null=True, max_length=255)
    connectivity_type_root = models.CharField(blank=True, null=True, max_length=255)
    cellular_coverage_availability = models.NullBooleanField(default=None)
    cellular_coverage_type = models.CharField(blank=True, null=True, max_length=255)
    fiber_node_distance = models.FloatField(blank=True, default=None, null=True)
    microwave_node_distance = models.FloatField(blank=True, default=None, null=True)

    schools_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)
    schools_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)
    schools_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)

    nearest_lte_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_umts_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_gsm_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_nr_distance = models.FloatField(blank=True, default=None, null=True)

    pop_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)

    school_data_source = models.CharField(blank=True, null=True, max_length=255)
    school_data_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    school_data_collection_modality = models.CharField(blank=True, null=True, max_length=255)
    school_location_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)
    connectivity_govt_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)
    connectivity_govt_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    disputed_region = models.BooleanField(default=False)

    connectivity_rt = models.NullBooleanField(default=None)
    connectivity_rt_datasource = models.CharField(blank=True, null=True, max_length=255)
    connectivity_rt_ingestion_timestamp = core_models.CustomDateTimeField(null=True, blank=True)
    connectivity = models.NullBooleanField(default=None)

    download_speed_benchmark = models.FloatField(blank=True, default=None, null=True)
    computer_availability = models.NullBooleanField(default=None)

    num_students_girls = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_students_boys = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_students_other = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_teachers_female = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_teachers_male = models.PositiveIntegerField(blank=True, default=None, null=True)

    teachers_trained  = models.NullBooleanField(default=None)
    sustainable_business_model = models.NullBooleanField(default=None)
    device_availability = models.NullBooleanField(default=None)

    num_tablets = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_robotic_equipment = models.PositiveIntegerField(blank=True, default=None, null=True)

    building_id_govt = models.CharField(blank=True, null=True, max_length=255)
    num_schools_per_building = models.PositiveIntegerField(blank=True, default=None, null=True)

    version = models.PositiveIntegerField(blank=True, default=None, null=True)

    objects = models.Manager()

    class Meta:
        app_label = app_config.app_name
        db_table = 'master_sync_school_static'
