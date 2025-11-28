import datetime

import pytz
from django.conf import settings
from django.db import models
from django.utils import timezone
from simple_history.models import HistoricalRecords

from proco.core.managers import BaseManager
from django.utils.translation import gettext_lazy as _
from django.core.cache import cache


class PositiveBigIntegerField(models.IntegerField):
    description = _("Big (8 byte) integer")
    MAX_BIGINT = 9223372036854775807

    def get_internal_type(self):
        return "BigIntegerField"

    def formfield(self, **kwargs):
        return super().formfield(**{
            'min_value': 0,
            'max_value': PositiveBigIntegerField.MAX_BIGINT,
            **kwargs,
        })


class CustomDateTimeField(models.DateTimeField):

    def _get_timezone_converted_value(self, value):
        """
        _get_timezone_converted_value
            Method to convert the timezone of the datetime field value
        :param value: DateTime instance
        :return: DateTime instance
        """
        response_timezone = pytz.timezone(settings.TIME_ZONE)
        return value.astimezone(response_timezone)

    def from_db_value(self, value, expression, connection, context):
        """
        from_db_value
             Method called at the time of getting the value from database.
        """
        if isinstance(value, datetime.datetime):
            return self._get_timezone_converted_value(value)
        return value

    def pre_save(self, model_instance, add):
        """
        pre_save
            Method to add current date time to the field which has 'auto_now' or 'auto_now_add'
            attributes.
        """
        if self.auto_now or (self.auto_now_add and add):
            value = self._get_timezone_converted_value(timezone.now())
            setattr(model_instance, self.attname, value)
            return value
        else:
            return super().pre_save(model_instance, add)

    def get_default(self):
        """
        get_default
            Method to return the default value of a DateTime field
        """
        default_value = super().get_default()
        if isinstance(default_value, datetime.datetime):
            default_value = self._get_timezone_converted_value(default_value)
        return default_value


class BaseModelMixin(models.Model):
    """
    BaseModelMixin
        This represents the BaseModel for the project without any creation, modification
        or deletion history.
    Inherits : `models.Model`
    """
    """ Project level variables """

    deleted = CustomDateTimeField(db_index=True, null=True, blank=True)
    last_modified_at = CustomDateTimeField(auto_now=True, verbose_name='Last Updated Date')
    last_modified_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        related_name='updated_%(class)ss',
        on_delete=models.DO_NOTHING,
        verbose_name='Last Updated By'
    )
    created = CustomDateTimeField(auto_now_add=True, verbose_name='Created Date')
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
        related_name='created_%(class)ss',
        on_delete=models.DO_NOTHING,
        verbose_name='Created By'
    )
    objects = BaseManager()

    """
    delete
        Method to delete BaseModel Object

    * Overrides delete method by updating deleted with current time
    """

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()

    class Meta:
        abstract = True


class BaseModel(BaseModelMixin, models.Model):
    """
    BaseModel
        This represents the BaseModel for the project with all creation, modification
        or deletion history.
    Inherits : `BaseModelMixin`
    """
    """ Project level variables """
    history = HistoricalRecords(inherit=True)

    class Meta:
        abstract = True
        ordering = ['last_modified_at']


class DataSourceModelMixin(models.Model):
    """
    DataSourceModelMixin
        This represents the common properties of the Data Source Models.
    Inherits : `models.Model`
    """

    pulled_at = CustomDateTimeField(null=True, blank=True, verbose_name='Pulled at Date')

    objects = models.Manager()

    class Meta:
        abstract = True


class MasterDataSourceModelMixin(DataSourceModelMixin):
    """
    MasterDataSourceModelMixin
        This represents the common properties of the Data Source Models.
    Inherits : `DataSourceModelMixin`
    """

    DATA_VERSION_CACHE_KEY = 'master_data_last_version_{0}'

    latitude = models.FloatField(blank=True, default=None, null=True)
    longitude = models.FloatField(blank=True, default=None, null=True)

    admin1 = models.CharField(max_length=255, blank=True, null=True)
    admin1_id_giga = models.CharField(max_length=50, null=True, blank=True)
    admin2 = models.CharField(max_length=255, blank=True, null=True)
    admin2_id_giga = models.CharField(max_length=50, null=True, blank=True)

    pop_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)  # pop_within_1km
    pop_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)  # pop_within_2km
    pop_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)  # pop_within_3km
    # No Mapping
    connectivity = models.CharField(blank=True, null=True, max_length=255)
    connectivity_govt = models.CharField(default='N/A', max_length=10)  # yes|no
    connectivity_type_govt = models.CharField(blank=True, null=True, max_length=50)  # connectivity_type
    connectivity_govt_collection_year = models.PositiveIntegerField(blank=True, null=True) #min = 2000; max = current year
    download_speed_contracted = models.FloatField(default='0.0')  # min = 0; max = 500
    num_computers = models.PositiveIntegerField(blank=True, null=True)  # min = 0; max = 500
    num_tablets = models.PositiveIntegerField(blank=True, null=True)  # min = 0; max = 200
    computer_availability = models.CharField(default='N/A', max_length=10)  # yes|no
    electricity_availability = models.CharField( default='N/A', max_length=10)  # yes|no
    electricity_type = models.CharField(blank=True, null=True, max_length=50)  # electrical grid|diesel generator|solar power station|other 
    water_availability = models.CharField(default='N/A', max_length=10)  # yes|no
    sustainable_business_model = models.CharField(blank=True, null=True, max_length=10)  # yes|no
    device_availability = models.CharField(blank=True, null=True, max_length=10)  # yes|no
    facility_establishment_year = models.PositiveIntegerField(blank=True, null=True) # min = 1000; max = current year
    facility_data_source = models.CharField(blank=True, null=True, max_length=255)
    facility_data_collection_year = models.PositiveIntegerField(blank=True, null=True) # min = 1000; max = current year
    facility_data_collection_modality = models.CharField(blank=True, null=True, max_length=50) # online|in-person|phone|other
    version = models.PositiveIntegerField(blank=True, default=None, null=True)
    is_facility_open = models.CharField(blank=True, null=True, max_length=10) # yes|no
    num_adm_personnel = models.PositiveIntegerField(blank=True, null=True)  # min=0, max=2000

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
    published_at = CustomDateTimeField(db_index=True, null=True, blank=True)

    is_read = models.BooleanField(default=False)

    history = HistoricalRecords(inherit=True)

    class Meta:
        abstract = True
        ordering = ['created']

    @classmethod
    def get_last_version(cls, iso3_format):
        last_data_version = cache.get(cls.DATA_VERSION_CACHE_KEY.format(iso3_format))
        if not last_data_version:
            latest_records = cls.objects.filter(
                country__iso3_format=iso3_format,
                version__isnull=False,
            ).order_by('-created').first()
            if latest_records:
                last_data_version = latest_records.version
        return last_data_version

    @classmethod
    def set_last_version(cls, value, iso3_format):
        cache.set(cls.DATA_VERSION_CACHE_KEY.format(iso3_format), value)


class BaseMasterStatusModel(models.Model):
    establishment_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)

    water_availability = models.NullBooleanField(default=None)
    electricity_availability = models.NullBooleanField(default=None)
    computer_lab = models.NullBooleanField(default=None)

    download_speed_benchmark = models.FloatField(blank=True, default=None, null=True)
    download_speed_contracted = models.FloatField(blank=True, default=None, null=True) #min = 0; max = 500

    electricity_type = models.CharField(blank=True, null=True, max_length=255)
    num_adm_personnel = models.PositiveIntegerField(blank=True, default=None, null=True)

    num_computers_desired = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_computers = models.PositiveIntegerField(blank=True, default=None, null=True)

    fiber_node_distance = models.FloatField(blank=True, default=None, null=True)
    microwave_node_distance = models.FloatField(blank=True, default=None, null=True)

    pop_within_1km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_2km = models.PositiveIntegerField(blank=True, default=None, null=True)
    pop_within_3km = models.PositiveIntegerField(blank=True, default=None, null=True)

    connectivity_govt_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    connectivity_govt_ingestion_timestamp = CustomDateTimeField(null=True, blank=True)

    num_tablets = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_robotic_equipment = models.PositiveIntegerField(blank=True, default=None, null=True)

    sustainable_business_model = models.NullBooleanField(default=None)
    computer_availability = models.NullBooleanField(default=None)
    device_availability = models.NullBooleanField(default=None)

    disputed_region = models.BooleanField(default=False)

    connectivity_govt = models.NullBooleanField(default=None)
    connectivity_type_govt = models.CharField(blank=True, null=True, max_length=255) #fiber|xdsl|wired|cellular|p2mp|wireless|p2p wireless|satellite|other
    connectivity_type = models.CharField(blank=True, null=True, max_length=255)
    connectivity_type_root = models.CharField(blank=True, null=True, max_length=255)

    nearest_lte_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_umts_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_gsm_distance = models.FloatField(blank=True, default=None, null=True)
    nearest_nr_distance = models.FloatField(blank=True, default=None, null=True)

    data_source = models.CharField(blank=True, null=True, max_length=255)
    data_collection_year = models.PositiveSmallIntegerField(blank=True, default=None, null=True)
    data_collection_modality = models.CharField(blank=True, null=True, max_length=255)
    location_ingestion_timestamp = CustomDateTimeField(null=True, blank=True)

    connectivity = models.NullBooleanField(default=None)

    coverage_type = models.CharField(max_length=8, default='unknown')

    version = models.PositiveIntegerField(blank=True, default=None, null=True)

    class Meta:
        abstract = True
