from django.apps.registry import apps
from django.contrib.contenttypes.models import ContentType
from django.contrib.gis.db.models import PointField
from django.db import models
from django.db.models import Q
from django.db.models.constraints import UniqueConstraint
from django.utils import timezone
from django.utils.translation import ugettext as _
from model_utils.models import TimeStampedModel

from proco.core import exceptions as core_exceptions
from proco.core.managers import BaseManager
from proco.core.models import CustomDateTimeField
from proco.locations.models import Country, CountryAdminMetadata
from proco.core import models as core_models


class Entity(TimeStampedModel):
    # School/Health/Library
    entity_name = models.CharField(max_length=20, db_index=True)

    external_id = models.CharField(max_length=50, blank=True, db_index=True)
    giga_id = models.CharField(max_length=50, db_index=True)
    name = models.CharField(max_length=1000, default='Name unknown')
    name_lower = models.CharField(max_length=1000, blank=True, editable=False, db_index=True)

    country = models.ForeignKey(Country, related_name='entities', on_delete=models.CASCADE)
    admin1 = models.ForeignKey(
        CountryAdminMetadata,
        related_name='admin1_entities',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    admin2 = models.ForeignKey(
        CountryAdminMetadata,
        related_name='admin2_entities',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    geopoint = PointField(verbose_name=_('Point'), null=True, blank=True)

    ENVIRONMENT_TYPE_RURAL = 'rural'
    ENVIRONMENT_TYPE_URBAN = 'urban'
    ENVIRONMENT_TYPE_CHOICES = (
        (ENVIRONMENT_TYPE_RURAL, 'Rural'),
        (ENVIRONMENT_TYPE_URBAN, 'Urban'),
    )

    environment = models.CharField(
        max_length=20,
        choices=ENVIRONMENT_TYPE_CHOICES,
        null=True,
        blank=True,
        db_index=True,
    )

    last_weekly_status = models.ForeignKey(
        'connection_statistics.EntityWeeklyStatus', null=True, blank=True,
        on_delete=models.SET_NULL, related_name='_entity',
    )

    connectivity_status = models.CharField(max_length=10, blank=True, default='unknown')
    coverage_status = models.CharField(max_length=10, blank=True, default='unknown')

    last_master_status_id = core_models.PositiveBigIntegerField(null=True, blank=True, default=None)

    deleted = CustomDateTimeField(db_index=True, null=True, blank=True)

    objects = BaseManager()

    class Meta:
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['entity_name', 'country', 'giga_id', 'deleted'],
                             name='entities_giga_id_unique_with_deleted'),
            UniqueConstraint(fields=['entity_name', 'country', 'giga_id'],
                             condition=Q(deleted=None),
                             name='entities_giga_id_unique_without_deleted'),
        ]

    def __str__(self):
        return f'{self.entity_name} - {self.country} - {self.admin1} - {self.name}'

    def save(self, **kwargs):
        self.name_lower = str(self.name).lower()
        self.external_id = str(self.external_id).lower()
        super().save(**kwargs)

    def delete(self, *args, **kwargs):
        force = kwargs.pop('force', False)

        if force:
            super().delete(*args, **kwargs)
        else:
            self.deleted = timezone.now()
            self.save()

        self.daily_status.all().update(deleted=timezone.now())
        self.weekly_status.all().update(deleted=timezone.now())

    def get_model_class_from_entity_name(self):
        """
        Returns model class from a name of app_label.ModelName format.
        :return Model:
        """
        model = 'connection_statistics.SchoolWeeklyStatus'

        try:
            if self.entity_name == 'health':
                model = 'entities.HealthMasterStatus'
            app_name, model_name = model.split('.')
            model = apps.get_model(app_name, model_name)
            return model
        except (ValueError, ContentType.DoesNotExist):
            raise core_exceptions.InvalidModelNameFormatError(model=model)


class HealthMasterStatus(core_models.BaseModelMixin, core_models.BaseMasterStatusModel):
    """
    SchoolMasterStatus
        This class define model used to store School Master Data.
    Inherits : `BaseModelMixin`
    """

    entity = models.ForeignKey(
        Entity,
        blank=False,
        null=False,
        related_name='master_status',
        on_delete=models.DO_NOTHING,
        verbose_name='Master Sync'
    )
    health_id_giga = models.CharField(max_length=255)
    dhis2_id = models.CharField(blank=True, null=True, max_length=255)
    hims_id = models.CharField(blank=True, null=True, max_length=255)
    hfml_id = models.CharField(blank=True, null=True, max_length=255)
    facility_name =  models.CharField(max_length=1000)
    facility_type = models.CharField(blank=True, null=True, max_length=255)
    facility_ownership = models.CharField(blank=True, null=True, max_length=255)
    num_community_health_workers = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_community_health_workers_within_5km = models.PositiveIntegerField(blank=True, default=None, null=True)
    area_type = models.CharField(blank=True, null=True, max_length=255)
    govt_pop_est = models.PositiveIntegerField(blank=True, default=None, null=True)  # govt_pop_est
    hf_pop_est = models.PositiveIntegerField(blank=True, default=None, null=True)
    is_facility_open = models.NullBooleanField(default=None)
    health_service_provider = models.CharField(blank=True, null=True, max_length=255)
    facility_accessibility = models.NullBooleanField(default=None)
    distance_to_closest_settlement = models.PositiveIntegerField(blank=True, null=True)
    distance_to_country_boundary = models.PositiveIntegerField(blank=True, default=None, null=True)
    facility_level = models.CharField(max_length=20)
    num_of_healthworkers = models.PositiveIntegerField(blank=True, default=None, null=True)
    num_beds_tot = models.PositiveIntegerField(blank=True, default=None, null=True)  # min = 0; max = 10000
    num_beds_icu = models.PositiveIntegerField(blank=True, default=None, null=True)  # min = 0; max = 5000
    num_theatres = models.PositiveIntegerField(blank=True, default=None, null=True)  # min = 0; max = 200
    num_toilets = models.PositiveIntegerField(blank=True, default=None, null=True)  # min = 0; max = 500
    power_backup_system = models.NullBooleanField(default=None)  # yes|no
    num_outpatients = models.PositiveIntegerField(blank=True, default=None, null=True)  # <20000
    num_inpatients = models.PositiveIntegerField(blank=True, default=None, null=True)  # <20000
    licensing_status = models.CharField(max_length=50)  # licensed|provisional|expired|suspended|not_applicable
    facility_hours = models.CharField(max_length=50)  # 24_7 | weekdays_daytime | weekdays_extended | seasonal | unknown | other
    emergency_services_available = models.NullBooleanField(default=None) # yes|no
    staff_doctors = models.PositiveIntegerField(blank=True, default=None, null=True) # max=2000
    staff_nurses = models.PositiveIntegerField(blank=True, default=None, null=True) # max=4000
    staff_midwives = models.PositiveIntegerField(blank=True, default=None, null=True) # max=500
    staff_laboratorians = models.PositiveIntegerField(blank=True, default=None, null=True) # max=500
    staff_pharmacists = models.PositiveIntegerField(blank=True, default=None, null=True) # max=500
    cold_chain_available = models.NullBooleanField(default=None) # yes|no
    waste_management_system = models.CharField(blank=True, null=True, max_length=50) # incinerator|pit|contracted_service|none|other
    hmis_system = models.NullBooleanField(default=None) # yes|no
    catchment_population = models.PositiveIntegerField(blank=True, default=None, null=True)
    # outpatient|inpatient|maternity|surgery|laboratory|pharmacy|radiology|dialysis|mental_health|immunization|HIV|TB|NCD_clinic|pediatrics|geriatrics|physiotherapy|dental
    services_offered = models.CharField(blank=True, null=True, max_length=100) 
    facility_id_govt = models.CharField(max_length=50)
    facility_id_govt_type = models.CharField(blank=True, null=True, max_length=50)
    facility_establishment_year = models.PositiveSmallIntegerField(blank=True, null=True)
    download_speed_govt = models.FloatField(blank=True, default=None, null=True)
    facility_address = models.CharField(blank=True, null=True, max_length=10)
    facility_data_source = models.CharField(blank=True, null=True, max_length=255)
    facility_data_collection_year = models.PositiveSmallIntegerField(blank=True, null=True)
    facility_data_collection_modality = models.NullBooleanField(default=None)
    refugee_camp = models.NullBooleanField(default=None)
    patients_refugees = models.NullBooleanField(default=None)
    connectivity_start_gov = models.CharField(blank=True, null=True, max_length=10) # MM: min =1; max=12|YYYY: min = 1000; max = current year
    connectivity_start_contract_gov = models.CharField(blank=True, null=True, max_length=10)# MM: min =1; max=12|YYYY: min = 1000; max = current year
    connectivity_ever_connected = models.NullBooleanField(default=None) # yes|no



    class Meta:
        ordering = ('id',)
        constraints = [
            UniqueConstraint(fields=['entity', 'version', 'deleted'],
                             name='entities_master_status_unique_with_deleted'),
            UniqueConstraint(fields=['entity', 'version'],
                             condition=Q(deleted=None),
                             name='entities_master_status_unique_without_deleted'),
        ]

    def __str__(self):
        return f'{self.entity} - {self.version}'
